//! CLI integration tests.
//!
//! Pattern: tests that hit external systems (Docker for Postgres or
//! RabbitMQ via `testcontainers`) are marked `#[ignore]` so they only
//! run when explicitly requested. The unmarked tests cover pure
//! argument parsing and short-circuit behaviours.
//!
//! Run the gated tests with:
//!
//! ```sh
//! cargo test -p hexeract-cli -- --ignored
//! ```

use std::io::Write;
use std::time::Duration;

use assert_cmd::Command;
use lapin::BasicProperties;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::options::BasicPublishOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use predicates::str::contains;
use tempfile::NamedTempFile;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

#[test]
fn patch_prints_canonical_schema_to_stdout() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args(["outbox", "patch", "--table", "audit_outbox"])
        .assert()
        .success()
        .stdout(contains("CREATE TABLE IF NOT EXISTS audit_outbox"));
}

#[test]
fn patch_with_invalid_table_name_fails() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args(["outbox", "patch", "--table", "bad name"])
        .assert()
        .failure();
}

#[test]
fn apply_without_confirmation_flag_refuses_with_exit_code_2() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "outbox",
            "apply",
            "--conn",
            "postgres://nobody@127.0.0.1:1/none",
            "--table",
            "audit_outbox",
        ])
        .assert()
        .failure()
        .code(2)
        .stderr(contains("--yes-i-know"));
}

const TOPOLOGY_TOML: &str = r#"
[[exchanges]]
name = "cli.orders.exchange"
kind = "topic"
durable = false
auto_delete = true

[[queues]]
name = "cli.orders.received"
durable = false
auto_delete = true

[[bindings]]
queue = "cli.orders.received"
exchange = "cli.orders.exchange"
routing_key = "cli.orders.*"
"#;

async fn start_rabbit() -> (testcontainers::ContainerAsync<RabbitMq>, String) {
    let container = RabbitMq::default()
        .start()
        .await
        .expect("rabbitmq container must start");
    let host = container
        .get_host()
        .await
        .expect("rabbitmq container must expose a host");
    let port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("rabbitmq container must expose AMQP port");
    let uri = format!("amqp://{host}:{port}");
    (container, uri)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn bus_declare_applies_topology_against_rabbitmq_container() {
    let (_container, uri) = start_rabbit().await;

    let mut file = NamedTempFile::new().expect("must create tempfile");
    file.write_all(TOPOLOGY_TOML.as_bytes())
        .expect("must write topology");
    let path = file.path().to_string_lossy().into_owned();

    Command::cargo_bin("hexeract")
        .unwrap()
        .args(["bus", "declare", "--conn", &uri, "--topology", &path])
        .assert()
        .success()
        .stdout(contains("declared 1 exchange(s)"))
        .stdout(contains("1 queue(s)"))
        .stdout(contains("1 binding(s)"));

    // Verify via a passive `queue_declare`: it fails if the queue is
    // missing, so success means `hexeract bus declare` reached the
    // broker and applied the topology.
    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .expect("probe connection must open");
    let channel = probe
        .create_channel()
        .await
        .expect("probe channel must open");
    channel
        .queue_declare(
            ShortString::from("cli.orders.received"),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue must exist after declare");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn bus_purge_then_peek_reports_empty_queue() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "cli.purge.target";

    // Declare and seed the queue with a handful of messages via
    // lapin directly, so we can observe the CLI side effects.
    let setup = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .expect("setup connection must open");
    let setup_channel = setup
        .create_channel()
        .await
        .expect("setup channel must open");
    setup_channel
        .queue_declare(
            ShortString::from(queue_name),
            QueueDeclareOptions {
                durable: false,
                auto_delete: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare must succeed");
    for index in 0..3u32 {
        setup_channel
            .basic_publish(
                ShortString::from(""),
                ShortString::from(queue_name),
                BasicPublishOptions::default(),
                format!("{{\"index\":{index}}}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .expect("publish must succeed")
            .await
            .expect("confirm must succeed");
    }

    // Give RabbitMQ a moment to settle the deliveries into the queue.
    tokio::time::sleep(Duration::from_millis(50)).await;

    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "bus",
            "purge",
            "--conn",
            &uri,
            "--queue",
            queue_name,
            "--yes-i-know",
        ])
        .assert()
        .success()
        .stdout(contains("purged 3 message(s)"));

    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "bus", "peek", "--conn", &uri, "--queue", queue_name, "--count", "5",
        ])
        .assert()
        .success()
        .stdout(contains("is empty"));
}

#[test]
fn bus_purge_without_safety_flag_short_circuits_without_connecting() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "bus",
            "purge",
            "--conn",
            "amqp://127.0.0.1:1",
            "--queue",
            "anything",
        ])
        .assert()
        .failure()
        .stderr(contains("yes-i-know"));
}
