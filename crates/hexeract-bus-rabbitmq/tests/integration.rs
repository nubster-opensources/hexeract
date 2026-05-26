//! End-to-end integration test against a real RabbitMQ broker.
//!
//! Run with `cargo test -p hexeract-bus-rabbitmq -- --ignored` on a
//! host with Docker available. The test spins up a fresh RabbitMQ
//! container via `testcontainers`, declares a queue bound to the
//! default exchange, publishes a message through
//! [`RabbitMqTransport`], then consumes it back through `basic_get`.

use std::collections::HashMap;
use std::time::Duration;

use hexeract_bus::Message;
use hexeract_bus::Transport;
use hexeract_bus_rabbitmq::RabbitMqTransport;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::options::BasicGetOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use serde::Deserialize;
use serde::Serialize;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
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
async fn transport_publishes_through_default_exchange_and_consumer_reads_it_back() {
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

    // Declare a queue bound to the default exchange. The default
    // exchange routes by routing_key directly to the queue of the
    // same name.
    let consumer_conn = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .expect("consumer connection must open");
    let consumer_channel = consumer_conn
        .create_channel()
        .await
        .expect("consumer channel must open");
    let queue_name = "orders.received";
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
        .expect("queue declare must succeed");

    // Publish through the transport on the default exchange.
    let transport = RabbitMqTransport::new(&uri)
        .await
        .expect("transport must connect");
    let order = OrderPlaced {
        order_id: Uuid::from_u128(42),
    };
    let mut headers = HashMap::new();
    headers.insert("tenant".to_owned(), "acme".to_owned());
    let message_id = transport
        .publish_with_headers(queue_name, headers, &order)
        .await
        .expect("publish must succeed");
    assert_ne!(message_id, Uuid::nil());

    // Consume the message back. Retry a few times to let RabbitMQ
    // flush the publish through.
    let mut delivery = None;
    for _ in 0..20 {
        let candidate = consumer_channel
            .basic_get(queue_name.into(), BasicGetOptions::default())
            .await
            .expect("basic_get must succeed");
        if candidate.is_some() {
            delivery = candidate;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let delivery = delivery.expect("must receive at least one delivery");

    assert_eq!(delivery.routing_key.as_str(), queue_name);
    assert_eq!(delivery.exchange.as_str(), "");
    let body: OrderPlaced = serde_json::from_slice(&delivery.data).expect("payload must decode");
    assert_eq!(body, order);
    let properties = &delivery.properties;
    assert_eq!(
        properties.content_type().as_ref().map(ShortString::as_str),
        Some("application/json")
    );
    assert_eq!(
        properties.kind().as_ref().map(ShortString::as_str),
        Some("orders.placed")
    );
}
