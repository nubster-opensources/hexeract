//! End-to-end integration test against a real RabbitMQ broker.
//!
//! Run with `cargo test -p hexeract-bus-rabbitmq -- --ignored` on a
//! host with Docker available. The test spins up a fresh RabbitMQ
//! container via `testcontainers`, declares a queue bound to the
//! default exchange, publishes a message through
//! [`RabbitMqTransport`], then consumes it back through `basic_get`.

use std::collections::HashMap;
use std::time::Duration;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use hexeract_bus::Binding;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::Handler;
use hexeract_bus::Message;
use hexeract_bus::Queue;
use hexeract_bus::RoutingKey;
use hexeract_bus::Transport;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::RabbitMqTransport;
use hexeract_bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract_bus_rabbitmq::ensure_topology;
use hexeract_core::HandlerContext;
use lapin::BasicProperties;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::options::BasicGetOptions;
use lapin::options::BasicPublishOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_with_correlation_id_propagates_to_amqp_properties() {
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

    let consumer_conn = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .expect("consumer connection must open");
    let consumer_channel = consumer_conn
        .create_channel()
        .await
        .expect("consumer channel must open");
    let queue_name = "orders.correlation";
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

    let transport = RabbitMqTransport::new(&uri)
        .await
        .expect("transport must connect");
    let order = OrderPlaced {
        order_id: Uuid::from_u128(7),
    };
    let known_correlation_id = Uuid::from_u128(0x0BAD_F00D);
    let message_id = transport
        .publish_with_correlation_id(queue_name, known_correlation_id, &order)
        .await
        .expect("publish must succeed");
    assert_ne!(message_id, Uuid::nil());
    assert_ne!(message_id, known_correlation_id);

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

    let observed_correlation = delivery
        .properties
        .correlation_id()
        .as_ref()
        .map(ShortString::as_str)
        .expect("AMQP correlation_id property must be set");
    assert_eq!(observed_correlation, known_correlation_id.to_string());

    let observed_message_id = delivery
        .properties
        .message_id()
        .as_ref()
        .map(ShortString::as_str)
        .expect("AMQP message_id property must be set");
    assert_eq!(observed_message_id, message_id.to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn ensure_topology_declares_exchange_queue_and_binding() {
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

    let exchange = Exchange::new("topology.orders", ExchangeKind::Topic)
        .expect("exchange must validate")
        .durable(false)
        .auto_delete(true);
    let queue = Queue::new("topology.orders.received")
        .expect("queue must validate")
        .durable(false)
        .auto_delete(true);
    let routing_key = RoutingKey::new("orders.created").expect("routing key must validate");
    let binding = Binding::new(&queue.name, &exchange.name, routing_key.clone())
        .expect("binding must validate");

    let connection = RabbitMqConnection::connect(&uri)
        .await
        .expect("RabbitMqConnection must open");
    ensure_topology(
        &connection,
        std::slice::from_ref(&exchange),
        std::slice::from_ref(&queue),
        std::slice::from_ref(&binding),
    )
    .await
    .expect("ensure_topology must succeed");

    // Verify via passive declarations: a passive `queue_declare` /
    // `exchange_declare` fails if the entity is missing, so success
    // means the helper effectively reached the broker.
    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .expect("probe connection must open");
    let probe_channel = probe
        .create_channel()
        .await
        .expect("probe channel must open");
    probe_channel
        .queue_declare(
            ShortString::from(queue.name.as_str()),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue must exist on broker");
    probe_channel
        .exchange_declare(
            ShortString::from(exchange.name.as_str()),
            lapin::ExchangeKind::Topic,
            lapin::options::ExchangeDeclareOptions {
                passive: true,
                ..lapin::options::ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("exchange must exist on broker");

    // Publish directly to the declared exchange with a matching
    // routing key; the binding must route the message into the queue.
    probe_channel
        .basic_publish(
            ShortString::from(exchange.name.as_str()),
            ShortString::from(routing_key.as_str()),
            BasicPublishOptions::default(),
            b"{\"order_id\":\"00000000-0000-0000-0000-000000000007\"}",
            BasicProperties::default(),
        )
        .await
        .expect("publish must succeed")
        .await
        .expect("confirm must succeed");

    // basic_get must observe the routed delivery.
    let mut delivery = None;
    for _ in 0..20 {
        let candidate = probe_channel
            .basic_get(
                ShortString::from(queue.name.as_str()),
                BasicGetOptions::default(),
            )
            .await
            .expect("basic_get must succeed");
        if candidate.is_some() {
            delivery = candidate;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let delivery = delivery.expect("binding must route the message into the queue");
    assert_eq!(delivery.exchange.as_str(), exchange.name);
    assert_eq!(delivery.routing_key.as_str(), routing_key.as_str());
}

#[derive(Debug, Default)]
struct RecordingHandler {
    seen: Arc<AtomicUsize>,
}

impl Handler<OrderPlaced> for RecordingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
struct AlwaysFailingHandler {
    attempts: Arc<AtomicUsize>,
}

impl Handler<OrderPlaced> for AlwaysFailingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(hexeract_bus::BusError::Internal(
            "deliberate test failure".to_owned(),
        ))
    }
}

#[derive(Debug)]
struct FailOnceHandler {
    attempts: Arc<AtomicUsize>,
}

impl Handler<OrderPlaced> for FailOnceHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        let n = self.attempts.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            Err(hexeract_bus::BusError::Internal(
                "first attempt fails".to_owned(),
            ))
        } else {
            Ok(())
        }
    }
}

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

async fn declare_temporary_queue(uri: &str, name: &str) {
    let conn = Connection::connect(uri, ConnectionProperties::default())
        .await
        .expect("setup connection must open");
    let channel = conn
        .create_channel()
        .await
        .expect("setup channel must open");
    channel
        .queue_declare(
            name.into(),
            QueueDeclareOptions {
                durable: false,
                exclusive: false,
                auto_delete: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_dispatches_envelope_and_acks_on_success() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.happy";
    declare_temporary_queue(&uri, queue_name).await;

    let transport = RabbitMqTransport::new(&uri).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(1),
            },
        )
        .await
        .unwrap();

    let seen = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .register_handler::<OrderPlaced, _>(RecordingHandler {
            seen: Arc::clone(&seen),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    for _ in 0..40 {
        if seen.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    cancel.cancel();
    handle
        .await
        .expect("worker task joins")
        .expect("worker run returns Ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_retries_on_failure_then_succeeds() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.retry";
    declare_temporary_queue(&uri, queue_name).await;

    let transport = RabbitMqTransport::new(&uri).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(2),
            },
        )
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .max_attempts(5)
        .register_handler::<OrderPlaced, _>(FailOnceHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    for _ in 0..60 {
        if attempts.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_routes_to_dead_letter_after_exhausting_attempts() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.dlr.source";
    let dlr_queue = "worker.dlr.parked";
    declare_temporary_queue(&uri, queue_name).await;
    declare_temporary_queue(&uri, dlr_queue).await;

    let transport = RabbitMqTransport::new(&uri).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(3),
            },
        )
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .max_attempts(2)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Wait until DLR queue receives the parked message.
    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let probe_channel = probe.create_channel().await.unwrap();
    let mut parked = None;
    for _ in 0..80 {
        let candidate = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
            .unwrap();
        if candidate.is_some() {
            parked = candidate;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        parked.is_some(),
        "DLR queue must receive the parked delivery"
    );
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "handler must have been called at least max_attempts times"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_returns_ok_on_cancellation_with_idle_queue() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.cancel";
    declare_temporary_queue(&uri, queue_name).await;

    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .register_handler::<OrderPlaced, _>(RecordingHandler::default())
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("worker stops within the timeout")
        .expect("worker task joins")
        .expect("worker run returns Ok on cancellation");
}
