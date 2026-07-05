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
use testcontainers_modules::postgres::Postgres;
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
fn scheduler_schema_prints_ddl_for_selected_dialect() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "scheduler",
            "schema",
            "--dialect",
            "postgres",
            "--table",
            "scheduled_messages",
        ])
        .assert()
        .success()
        .stdout(contains("CREATE TABLE IF NOT EXISTS scheduled_messages"));
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
        .code(2)
        .stderr(contains("yes-i-know"));
}

#[test]
fn bus_purge_without_safety_flag_prints_guidance_to_stderr() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "bus",
            "purge",
            "--conn",
            "amqp://127.0.0.1:1",
            "--queue",
            "orders.received",
        ])
        .assert()
        .failure()
        .code(2)
        .stderr(contains("Refusing to purge without --yes-i-know"));
}

/// Verify that `bus peek --count N` returns N distinct messages rather than
/// repeating the first one N times (regression for #224).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn bus_peek_count_n_returns_n_distinct_messages() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "cli.peek.distinct";

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

    // Publish 3 messages with clearly distinct payloads.
    for index in 1..=3u32 {
        setup_channel
            .basic_publish(
                ShortString::from(""),
                ShortString::from(queue_name),
                BasicPublishOptions::default(),
                format!("{{\"seq\":{index}}}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .expect("publish must succeed")
            .await
            .expect("confirm must succeed");
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Peek all 3: the output must contain each distinct payload exactly once.
    let output = Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "bus", "peek", "--conn", &uri, "--queue", queue_name, "--count", "3",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8_lossy(&output);

    assert!(
        stdout.contains("{\"seq\":1}"),
        "peek output must include message 1; got: {stdout}"
    );
    assert!(
        stdout.contains("{\"seq\":2}"),
        "peek output must include message 2; got: {stdout}"
    );
    assert!(
        stdout.contains("{\"seq\":3}"),
        "peek output must include message 3; got: {stdout}"
    );

    // Ensure the queue still has all 3 messages (non-destructive).
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after = Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "bus", "peek", "--conn", &uri, "--queue", queue_name, "--count", "5",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let after_out = String::from_utf8_lossy(&after);
    assert!(
        !after_out.contains("is empty"),
        "queue must not be empty after non-destructive peek"
    );
}

/// Verify that `outbox check` ignores tables in other schemas with the same
/// name, preventing false-positive validation (regression for #233).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn outbox_check_ignores_same_named_table_in_other_schema() {
    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    // Use sslmode=disable because the test container does not have TLS configured.
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    // Connect directly to set up a cross-schema scenario: create
    // `other.audit_outbox` with all required columns but leave
    // `public.audit_outbox` absent.
    let (setup_client, setup_conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("setup connect");
    tokio::spawn(async move {
        let _ = setup_conn.await;
    });

    setup_client
        .batch_execute(
            "CREATE SCHEMA other; \
             CREATE TABLE other.audit_outbox ( \
               id BIGSERIAL, event_id UUID, event_type TEXT, payload JSONB, \
               subject_id UUID, created_at TIMESTAMPTZ, attempts INT, \
               last_error TEXT, next_retry_at TIMESTAMPTZ, delivered_at TIMESTAMPTZ \
             );",
        )
        .await
        .expect("setup DDL must succeed");

    // `hexeract outbox check` must fail: public.audit_outbox does not exist.
    Command::cargo_bin("hexeract")
        .unwrap()
        .args(["outbox", "check", "--conn", &url, "--table", "audit_outbox"])
        .assert()
        .failure()
        .code(1)
        .stderr(contains("does not exist"));
}

/// Minimal event used to build a [`hexeract_scheduler::ScheduledMessage`] for
/// the `scheduler list` integration test below.
#[derive(serde::Serialize, serde::Deserialize)]
struct TestReminder;

impl hexeract_outbox::Event for TestReminder {
    const EVENT_TYPE: &'static str = "test.reminder";
}

/// Verify that `scheduler list --format json` reports a seeded pending
/// schedule (regression coverage for the B4 `scheduler list` subcommand).
#[tokio::test]
#[ignore = "requires Docker"]
async fn scheduler_list_json_reports_pending() {
    use hexeract_scheduler::{ScheduleStore, ScheduledMessage, Target};
    use hexeract_scheduler_sql::{Dialect, PgScheduleStore, schema::schema_ddl};

    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("must connect to test container");
    sqlx::raw_sql(&schema_ddl(Dialect::Postgres, "scheduled_messages").unwrap())
        .execute(&pool)
        .await
        .expect("schema DDL must apply");
    let store = PgScheduleStore::new(pool, "scheduled_messages").expect("table name must be valid");
    let message = ScheduledMessage::delay(
        Target::mediator(),
        std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
        &TestReminder,
    )
    .expect("message must build");
    store
        .insert(&message, 5)
        .await
        .expect("insert must succeed");

    Command::cargo_bin("hexeract")
        .unwrap()
        .args(["scheduler", "list", "--conn", &url, "--format", "json"])
        .assert()
        .success()
        .stdout(contains("\"status\": \"pending\""));
}

/// Verify that `scheduler inspect <id> --format json` reports the seeded
/// schedule's own id (regression coverage for the B5 `scheduler inspect`
/// subcommand).
#[tokio::test]
#[ignore = "requires Docker"]
async fn scheduler_inspect_json_reports_seeded_schedule() {
    use hexeract_scheduler::{ScheduleStore, ScheduledMessage, Target};
    use hexeract_scheduler_sql::{Dialect, PgScheduleStore, schema::schema_ddl};

    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("must connect to test container");
    sqlx::raw_sql(&schema_ddl(Dialect::Postgres, "scheduled_messages").unwrap())
        .execute(&pool)
        .await
        .expect("schema DDL must apply");
    let store = PgScheduleStore::new(pool, "scheduled_messages").expect("table name must be valid");
    let message = ScheduledMessage::delay(
        Target::mediator(),
        std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
        &TestReminder,
    )
    .expect("message must build");
    store
        .insert(&message, 5)
        .await
        .expect("insert must succeed");

    let id = message.schedule_id.to_string();
    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "scheduler",
            "inspect",
            &id,
            "--conn",
            &url,
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(contains(&id));
}

/// Verify that inspecting an id absent from the store fails with exit code 1.
#[tokio::test]
#[ignore = "requires Docker"]
async fn scheduler_inspect_unknown_id_fails_with_exit_code_1() {
    use hexeract_scheduler_sql::Dialect;
    use hexeract_scheduler_sql::schema::schema_ddl;

    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("must connect to test container");
    sqlx::raw_sql(&schema_ddl(Dialect::Postgres, "scheduled_messages").unwrap())
        .execute(&pool)
        .await
        .expect("schema DDL must apply");

    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "scheduler",
            "inspect",
            &uuid::Uuid::new_v4().to_string(),
            "--conn",
            &url,
        ])
        .assert()
        .failure()
        .code(1);
}
