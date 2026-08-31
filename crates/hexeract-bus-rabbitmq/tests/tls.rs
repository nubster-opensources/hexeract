//! Docker-backed proof that RabbitMQ clients can use a private CA and mTLS.
//!
//! Run with `cargo test -p hexeract-bus-rabbitmq -- --ignored` on a host with
//! Docker available. Each test spins up a RabbitMQ broker that serves AMQPS
//! only and demands a client certificate signed by the fixture CA, so a client
//! that fails to present its identity, or fails to trust the broker, cannot
//! connect at all.

mod harness;

use std::time::Duration;

use hexeract_bus::Message;
use hexeract_bus::Transport;
use hexeract_bus_rabbitmq::{
    RabbitMqConnection, RabbitMqConnectionConfig, RabbitMqRequestClientConfigBuilder,
    RabbitMqTransport, connect_request_client_with_config,
};
use lapin::options::BasicGetOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct OrderPlaced {
    order_id: Uuid,
}

impl Message for OrderPlaced {
    const MESSAGE_TYPE: &'static str = "orders.placed";
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn transport_and_request_client_connect_with_private_ca_and_mtls() {
    let broker = harness::start_tls_rabbitmq().await;
    let connection_config =
        RabbitMqConnectionConfig::default().with_tls_config(harness::client_tls_config());

    let transport = RabbitMqTransport::new_with_config(broker.uri(), &connection_config)
        .await
        .expect("transport must trust the private CA and present a client certificate");
    drop(transport);

    let cancel = CancellationToken::new();
    let client = connect_request_client_with_config(
        broker.uri(),
        Duration::from_secs(5),
        cancel,
        RabbitMqRequestClientConfigBuilder::new()
            .connection_config(connection_config)
            .build(),
    )
    .await
    .expect("request client must configure TLS on both owned connections");
    client.close().await;
}

/// The negative case the positive proof cannot supply.
///
/// Without the fixture CA the broker's certificate is signed by an authority
/// the platform trust store does not know, so verification must fail. It also
/// pins the classification: `rustls` reports a rejected certificate as
/// `io::ErrorKind::InvalidData`, which the connect path must treat as
/// permanent. Were it classified transient, a supervisor would rebuild this
/// connection forever against a trust chain that can never succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn connecting_without_the_private_ca_is_refused_permanently() {
    let broker = harness::start_tls_rabbitmq().await;

    let error = RabbitMqConnection::connect(broker.uri())
        .await
        .expect_err("a broker signed by an unknown authority must not be trusted");

    assert_eq!(
        error.is_retryable_connection(),
        Some(false),
        "a rejected certificate fails identically on every retry, so it must \
         classify as permanent instead of being hammered"
    );
}

/// Presenting a client certificate is not the same as having a working
/// session. This moves a real message across the mutually authenticated
/// connection, so a regression that breaks the session after the handshake
/// cannot pass unnoticed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn messages_round_trip_over_mutual_tls() {
    let broker = harness::start_tls_rabbitmq().await;
    let connection_config =
        RabbitMqConnectionConfig::default().with_tls_config(harness::client_tls_config());

    // The consumer side exercises the single-shot connect path under mTLS.
    let consumer = RabbitMqConnection::connect_with_config(broker.uri(), &connection_config)
        .await
        .expect("consumer connection must open over mutual TLS");
    let consumer_channel = consumer
        .create_channel()
        .await
        .expect("consumer channel must open");
    let queue_name = "orders.received.tls";
    consumer_channel
        .queue_declare(
            queue_name.into(),
            QueueDeclareOptions {
                durable: false,
                exclusive: false,
                auto_delete: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare must succeed over mutual TLS");

    // The publisher side exercises the recovering connect path under mTLS.
    let transport = RabbitMqTransport::new_with_config(broker.uri(), &connection_config)
        .await
        .expect("transport must connect over mutual TLS");
    let order = OrderPlaced {
        order_id: Uuid::from_u128(531),
    };
    transport
        .publish(queue_name, &order)
        .await
        .expect("publish must succeed over mutual TLS");

    let mut delivery = None;
    for _ in 0..20 {
        let candidate = consumer_channel
            .basic_get(queue_name.into(), BasicGetOptions::default())
            .await
            .expect("basic_get must succeed over mutual TLS");
        if candidate.is_some() {
            delivery = candidate;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let delivery = delivery.expect("the message must cross the mutually authenticated session");
    let body: OrderPlaced = serde_json::from_slice(&delivery.data).expect("payload must decode");
    assert_eq!(body, order);
}
