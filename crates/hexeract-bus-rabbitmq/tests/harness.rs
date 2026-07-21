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
use lapin::BasicProperties;
use lapin::Channel;
use lapin::options::BasicPublishOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

/// A running RabbitMQ test container, kept alive for the container's
/// AMQP URI to stay reachable.
pub(crate) struct RunningBroker {
    _container: ContainerAsync<RabbitMq>,
    uri: String,
}

impl RunningBroker {
    /// The AMQP URI of the running broker.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
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
    RunningBroker {
        _container: container,
        uri,
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
