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

/// Start a broker that serves only AMQPS and requires mutual TLS.
pub(crate) async fn start_tls_rabbitmq() -> RunningTlsBroker {
    const RABBITMQ_TLS_CONFIG: &str = r"
listeners.tcp = none
listeners.ssl.default = 5671

ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true
";

    let container = GenericImage::new("rabbitmq", "3.8.22-management")
        .with_exposed_port(5671.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "Server startup complete; 4 plugins started.",
        ))
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
    let uri = format!("amqps://guest:guest@{host}:{port}/%2f");

    RunningTlsBroker {
        _container: container,
        uri,
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
