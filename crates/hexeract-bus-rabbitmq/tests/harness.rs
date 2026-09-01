//! Shared testcontainers helper for the crate's Docker-backed integration
//! suite.
//!
//! Spins up a fresh RabbitMQ container per test and exposes its resolved
//! AMQP URI, plus a raw publish helper that hand-crafts the AMQP
//! properties a [`BusEnvelope`] restores from, so a test can act as a
//! foreign producer without going through [`hexeract_bus_rabbitmq::RabbitMqTransport`].

#![cfg(test)]
#![allow(
    dead_code,
    reason = "not every test binary in this crate uses every helper"
)]

use std::time::Duration;
use std::time::Instant;

use hexeract_bus::BusEnvelope;
use hexeract_bus_rabbitmq::{OwnedIdentity, OwnedTLSConfig};
use lapin::BasicProperties;
use lapin::Channel;
use lapin::options::BasicPublishOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use testcontainers::ContainerAsync;
use testcontainers::CopyTargetOptions;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

/// A running RabbitMQ test container, kept alive for the container's
/// AMQP URI to stay reachable.
pub(crate) struct RunningBroker {
    container: ContainerAsync<RabbitMq>,
    uri: String,
}

/// A RabbitMQ broker configured for TLS only, with a private CA and mandatory
/// client certificate authentication.
pub(crate) struct RunningTlsBroker {
    _container: ContainerAsync<GenericImage>,
    uri: String,
}

impl RunningTlsBroker {
    /// The AMQPS URI of the running broker.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }
}

impl RunningBroker {
    /// The AMQP URI of the running broker.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    /// Stop the underlying container immediately, simulating a broker
    /// crash so a test can assert on how a client reacts to a dropped
    /// connection.
    pub(crate) async fn stop(&self) {
        self.container
            .stop_with_timeout(Some(0))
            .await
            .expect("rabbitmq container must stop");
    }
}

/// Start a fresh RabbitMQ container and resolve its AMQP URI.
pub(crate) async fn start_rabbitmq() -> RunningBroker {
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
    RunningBroker { container, uri }
}

/// Select the rustls crypto provider for the whole test process.
///
/// `rustls` refuses to pick a provider on its own when its crate features name
/// more than one, and panics inside lapin's io loop at the first handshake
/// instead of returning an error. This test binary is exactly that case: the
/// production graph resolves `aws-lc-rs` alone through `tcp-stream`, but
/// `testcontainers` pulls `bollard`, which adds `ring`. The ambiguity is an
/// artefact of the dev-dependencies, not of the crate under test, so the tests
/// resolve it explicitly and pick the provider production would have used.
///
/// Safe to call from every test: `install_default` wins once per process and
/// the losers of the race are ignored.
fn install_test_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Start a broker that serves only AMQPS and requires mutual TLS.
pub(crate) async fn start_tls_rabbitmq() -> RunningTlsBroker {
    // `loopback_users = none` restores a default this file overwrites. The
    // official image ships its own `rabbitmq.conf` so that `guest` can log in
    // from outside the container; copying ours over it takes that away, and
    // `guest` reverts to loopback-only. The connection then arrives from the
    // Docker gateway, is refused with ACCESS_REFUSED, and the failure looks
    // like a TLS problem while TLS in fact succeeded.
    const RABBITMQ_TLS_CONFIG: &str = r"
listeners.tcp = none
listeners.ssl.default = 5671

loopback_users = none

ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true
";

    install_test_crypto_provider();

    // `WaitFor::seconds` hands the container back straight away; readiness is
    // then established by `await_broker_ready`, which reads the broker's own
    // log under a bounded budget. Delegating the wait to testcontainers would
    // give away the container, leaving nothing to inspect when a broker
    // refuses its configuration and never reports itself ready.
    let container = GenericImage::new("rabbitmq", "3.8.22-management")
        .with_exposed_port(5671.tcp())
        .with_wait_for(WaitFor::seconds(1))
        .with_copy_to(
            "/etc/rabbitmq/rabbitmq.conf",
            RABBITMQ_TLS_CONFIG.as_bytes().to_vec(),
        )
        .with_copy_to(
            "/etc/rabbitmq/tls/ca.pem",
            include_bytes!("fixtures/tls/ca.pem").to_vec(),
        )
        .with_copy_to(
            "/etc/rabbitmq/tls/server.pem",
            include_bytes!("fixtures/tls/server.pem").to_vec(),
        )
        .with_copy_to(
            CopyTargetOptions::new("/etc/rabbitmq/tls/server-key.pem").with_mode(0o644),
            include_bytes!("fixtures/tls/server-key.pem").to_vec(),
        )
        .start()
        .await
        .expect("TLS RabbitMQ container must start");
    let host = container
        .get_host()
        .await
        .expect("TLS RabbitMQ container must expose a host");
    let port = container
        .get_host_port_ipv4(5671)
        .await
        .expect("TLS RabbitMQ container must expose AMQPS port");

    await_broker_ready(&container).await;

    let uri = format!("amqps://guest:guest@{host}:{port}/%2f");

    RunningTlsBroker {
        _container: container,
        uri,
    }
}

/// How long a TLS broker is given to finish booting.
///
/// Generous enough for a cold image on a loaded CI runner, and short enough
/// that a broker which will never come up fails with a diagnosis rather than
/// hanging until the job is cancelled.
const TLS_BOOT_BUDGET: Duration = Duration::from_secs(120);

/// The line RabbitMQ prints once every listener is accepting connections.
///
/// Deliberately a prefix. The full line ends with the plugin count
/// ("Server startup complete; 4 plugins started."), and matching that count
/// couples the suite to the image: any change to the enabled plugins would
/// leave the wait hanging with no usable message.
const BROKER_READY_MARKER: &str = "Server startup complete";

/// Block until the broker reports itself ready, or fail with its own logs.
///
/// Readiness is read from the container's log, not from a TCP probe. Probing
/// the published port proves nothing under Docker: the port is bound by the
/// daemon's proxy and accepts connections while the service inside is still
/// booting, so a probe returns immediately and the first TLS handshake dies
/// against a broker that is not listening yet. That false positive is exactly
/// what made the suite fail in six seconds against a healthy broker.
///
/// Reading the log ourselves, rather than delegating to `WaitFor`, keeps the
/// container in hand: a broker that rejects its own configuration and never
/// prints the marker is reported with its output instead of hanging the job.
async fn await_broker_ready(container: &ContainerAsync<GenericImage>) {
    let deadline = Instant::now() + TLS_BOOT_BUDGET;
    loop {
        let stdout = container.stdout_to_vec().await.unwrap_or_default();
        if String::from_utf8_lossy(&stdout).contains(BROKER_READY_MARKER) {
            return;
        }
        if Instant::now() >= deadline {
            let stderr = container.stderr_to_vec().await.unwrap_or_default();
            panic!(
                "the TLS broker never reported {BROKER_READY_MARKER:?} within \
                 {TLS_BOOT_BUDGET:?}.\n--- container stdout ---\n{}\n\
                 --- container stderr ---\n{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr),
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// TLS material accepted by [`start_tls_rabbitmq`].
pub(crate) fn client_tls_config() -> OwnedTLSConfig {
    OwnedTLSConfig {
        identity: Some(OwnedIdentity::PKCS12 {
            der: include_bytes!("fixtures/tls/client.p12").to_vec(),
            password: "hexeract-test".to_owned(),
        }),
        cert_chain: Some(include_str!("fixtures/tls/ca.pem").to_owned()),
    }
}

/// Publish `envelope` to `routing_key` on the default exchange.
///
/// Stamps the AMQP `type`, `correlation_id` and header properties from
/// the envelope, following the same conventions
/// `hexeract_bus_rabbitmq::worker::delivery_to_envelope` reconstructs
/// from, so a test can publish a reply by hand and have the reply inbox
/// consumer decode it back into an equivalent [`BusEnvelope`].
pub(crate) async fn publish_to_default_exchange(
    channel: &Channel,
    routing_key: &str,
    envelope: &BusEnvelope,
) {
    let mut headers = FieldTable::default();
    for (key, value) in &envelope.headers {
        headers.insert(
            ShortString::from(key.as_str()),
            AMQPValue::LongString(value.as_str().into()),
        );
    }
    let properties = BasicProperties::default()
        .with_message_id(envelope.message_id.to_string().into())
        .with_correlation_id(envelope.correlation_id.to_string().into())
        .with_type(envelope.message_type.as_str().into())
        .with_headers(headers);

    channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(routing_key),
            BasicPublishOptions::default(),
            &envelope.payload,
            properties,
        )
        .await
        .expect("publish to inbox must succeed")
        .await
        .expect("publish confirmation must resolve");
}
