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
use hexeract_bus::BusError;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::Handler;
use hexeract_bus::Message;
use hexeract_bus::Queue;
use hexeract_bus::RoutingKey;
use hexeract_bus::Transport;
use hexeract_bus_rabbitmq::AckMode;
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
    assert_eq!(*properties.delivery_mode(), Some(2));
    assert!(properties.timestamp().is_some_and(|secs| secs > 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn transport_publish_to_unroutable_routing_key_fails() {
    let (_container, uri) = start_rabbit().await;
    let transport = RabbitMqTransport::new(&uri)
        .await
        .expect("transport must connect");

    let err = transport
        .publish(
            "orders.nowhere",
            &OrderPlaced {
                order_id: Uuid::from_u128(7),
            },
        )
        .await
        .expect_err("publish to a routing key with no bound queue must fail");

    assert!(
        matches!(err, BusError::Unroutable { ref routing_key, .. } if routing_key == "orders.nowhere"),
        "expected BusError::Unroutable, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn fire_and_forget_publish_to_unroutable_routing_key_returns_ok() {
    let (_container, uri) = start_rabbit().await;
    let transport = RabbitMqTransport::new(&uri)
        .await
        .expect("transport must connect")
        .fire_and_forget();

    let message_id = transport
        .publish(
            "orders.nowhere",
            &OrderPlaced {
                order_id: Uuid::from_u128(8),
            },
        )
        .await
        .expect("fire-and-forget publish must not await a broker verdict");

    assert_ne!(message_id, Uuid::nil());
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

#[derive(Debug)]
struct TimestampingFailingHandler {
    attempts: Arc<AtomicUsize>,
    seen_at: Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
}

impl Handler<OrderPlaced> for TimestampingFailingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.seen_at
            .lock()
            .expect("timestamp mutex must not be poisoned")
            .push(std::time::Instant::now());
        Err(hexeract_bus::BusError::Internal(
            "deliberate poison".to_owned(),
        ))
    }
}

struct PanickingHandler {
    seen_after: Arc<AtomicUsize>,
}

impl Handler<OrderPlaced> for PanickingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(&self, msg: OrderPlaced, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        assert!(
            msg.order_id != Uuid::from_u128(0),
            "deliberate panic in handler"
        );
        self.seen_after.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Handler that tracks how many invocations are running concurrently.
///
/// It increments a live counter on entry, records the running maximum,
/// sleeps to simulate a slow handler, then decrements on exit. The peak
/// counter lets a flow-control test assert the worker never lets more
/// than `max_buffered` deliveries run at once.
#[derive(Debug)]
struct ConcurrencyProbeHandler {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    work: Duration,
}

impl Handler<OrderPlaced> for ConcurrencyProbeHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        let current = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(self.work).await;
        self.live.fetch_sub(1, Ordering::SeqCst);
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn unacknowledged_worker_bounds_in_flight_deliveries_with_max_buffered() {
    // A no_ack consumer gets no broker-side QoS bound, so without
    // max_buffered a fast producer with a slow handler would let every
    // delivery run at once. With max_buffered = 16 the worker must cap
    // concurrent in-flight handlers at 16, even while 500 messages wait.
    const MESSAGE_COUNT: usize = 500;
    const MAX_BUFFERED: usize = 16;

    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.bounded.unack";
    declare_temporary_queue(&uri, queue_name).await;

    let transport = RabbitMqTransport::new(&uri)
        .await
        .expect("transport must connect");
    for index in 0..MESSAGE_COUNT {
        transport
            .publish(
                queue_name,
                &OrderPlaced {
                    order_id: Uuid::from_u128(index as u128),
                },
            )
            .await
            .expect("publish must succeed");
    }

    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Unacknowledged)
        .max_buffered(MAX_BUFFERED)
        .register_handler::<OrderPlaced, _>(ConcurrencyProbeHandler {
            live: Arc::clone(&live),
            peak: Arc::clone(&peak),
            completed: Arc::clone(&completed),
            work: Duration::from_millis(100),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Let several batches drain so the peak has time to reach the bound.
    for _ in 0..200 {
        if completed.load(Ordering::SeqCst) >= MESSAGE_COUNT {
            break;
        }
        assert!(
            live.load(Ordering::SeqCst) <= MAX_BUFFERED,
            "in-flight deliveries must never exceed max_buffered while running"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cancel.cancel();
    handle
        .await
        .expect("worker task joins")
        .expect("worker run returns Ok on cancellation");

    assert!(
        peak.load(Ordering::SeqCst) <= MAX_BUFFERED,
        "peak concurrent in-flight handlers ({}) must stay within the bound ({MAX_BUFFERED})",
        peak.load(Ordering::SeqCst)
    );
    assert!(
        peak.load(Ordering::SeqCst) > 1,
        "the bound must allow real concurrency, not serialize to one at a time"
    );
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
        .retry_delay(Duration::from_millis(200))
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
        .retry_delay(Duration::from_millis(200))
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Wait until the DLR queue receives the parked message. The queue
    // is declared by the worker, so early probes can race the startup:
    // a basic_get on a missing queue is a channel-closing soft error,
    // hence a fresh channel per attempt and errors treated as retries.
    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let mut parked = None;
    for _ in 0..80 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(candidate) = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
        {
            if candidate.is_some() {
                parked = candidate;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let parked = parked.expect(
        "DLR queue must receive the parked delivery even though only the worker declared it",
    );
    assert_eq!(
        *parked.delivery.properties.delivery_mode(),
        Some(2),
        "dead-letter copy must be persistent"
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
async fn worker_routes_oversize_delivery_to_dead_letter_queue() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.oversize.source";
    let dlr_queue = "worker.oversize.parked";
    declare_temporary_queue(&uri, queue_name).await;

    let publisher = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let publish_channel = publisher.create_channel().await.unwrap();
    let oversize_payload = vec![b'x'; 256];
    publish_channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(queue_name),
            BasicPublishOptions::default(),
            &oversize_payload,
            BasicProperties::default().with_type("orders.placed".into()),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .max_payload_bytes(64)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // The DLR queue is declared by the worker, so early probes can race
    // the startup: a basic_get on a missing queue is a channel-closing
    // soft error, hence a fresh channel per attempt.
    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let mut parked = None;
    for _ in 0..80 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(candidate) = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
        {
            if candidate.is_some() {
                parked = candidate;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let parked = parked.expect("oversize delivery must be routed to the dead-letter queue");
    assert_eq!(
        parked.delivery.data.len(),
        256,
        "the dead-letter copy must carry the original payload"
    );
    assert_eq!(
        *parked.delivery.properties.delivery_mode(),
        Some(2),
        "dead-letter copy must be persistent"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "handler must never run for an oversize delivery"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_routes_undecodable_delivery_to_dead_letter_queue() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.undecodable.source";
    let dlr_queue = "worker.undecodable.parked";
    declare_temporary_queue(&uri, queue_name).await;

    let publisher = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let publish_channel = publisher.create_channel().await.unwrap();
    publish_channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(queue_name),
            BasicPublishOptions::default(),
            b"{}",
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let mut parked = None;
    for _ in 0..80 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(candidate) = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
        {
            if candidate.is_some() {
                parked = candidate;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let parked = parked
        .expect("a delivery missing the `type` property must be routed to the dead-letter queue");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "handler must never run for an undecodable delivery"
    );
    drop(parked);

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_run_returns_connection_error_when_broker_stops() {
    let (container, uri) = start_rabbit().await;
    let queue_name = "worker.broker.down";
    declare_temporary_queue(&uri, queue_name).await;

    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Let the consumer subscribe before taking the broker down.
    tokio::time::sleep(Duration::from_millis(500)).await;
    container.stop().await.expect("container must stop");

    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("worker must exit after the broker stops")
        .expect("worker task must not panic");
    assert!(
        matches!(result, Err(BusError::Connection(_))),
        "a dead broker must surface as a connection error so a supervisor restarts the worker, got {result:?}"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_continues_after_handler_panic() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.panic";
    declare_temporary_queue(&uri, queue_name).await;

    let transport = RabbitMqTransport::new(&uri).await.unwrap();
    // First message triggers a panic (order_id == 0).
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(0),
            },
        )
        .await
        .unwrap();
    // Second message must be processed normally.
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(1),
            },
        )
        .await
        .unwrap();

    let seen_after = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .register_handler::<OrderPlaced, _>(PanickingHandler {
            seen_after: Arc::clone(&seen_after),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    for _ in 0..60 {
        if seen_after.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cancel.cancel();
    handle
        .await
        .expect("worker task joins")
        .expect("worker run returns Ok");

    assert_eq!(
        seen_after.load(Ordering::SeqCst),
        1,
        "worker must process the second delivery after the first handler panicked"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_delays_retries_and_retry_count_survives_restart() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.poison";
    let dlr_queue = "worker.poison.parked";
    let retry_delay = Duration::from_millis(300);
    declare_temporary_queue(&uri, queue_name).await;

    let transport = RabbitMqTransport::new(&uri).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(4),
            },
        )
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let seen_at = Arc::new(std::sync::Mutex::new(Vec::new()));

    // First worker instance: two failing attempts, spaced by the wait
    // queue TTL rather than redelivered in a tight loop.
    let first_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let first_worker = RabbitMqWorkerBuilder::new(first_conn)
        .queue(queue_name)
        .max_attempts(3)
        .retry_delay(retry_delay)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(TimestampingFailingHandler {
            attempts: Arc::clone(&attempts),
            seen_at: Arc::clone(&seen_at),
        })
        .build()
        .unwrap();
    let first_cancel = CancellationToken::new();
    let first_cancel_for_task = first_cancel.clone();
    let first_handle = tokio::spawn(async move { first_worker.run(first_cancel_for_task).await });

    for _ in 0..80 {
        if attempts.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "first worker must have seen exactly two attempts"
    );
    {
        let timestamps = seen_at.lock().expect("timestamp mutex");
        let gap = timestamps[1].duration_since(timestamps[0]);
        assert!(
            gap >= retry_delay - Duration::from_millis(50),
            "retries must be delayed by the wait queue TTL, got {gap:?}"
        );
    }

    // Restart: drop the first worker while the delivery sits in the
    // wait queue, then consume with a fresh instance. The retry count
    // travels in the broker-maintained x-death header, so the third
    // attempt must exhaust the budget instead of starting over.
    first_cancel.cancel();
    first_handle.await.unwrap().unwrap();

    let second_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let second_worker = RabbitMqWorkerBuilder::new(second_conn)
        .queue(queue_name)
        .max_attempts(3)
        .retry_delay(retry_delay)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(TimestampingFailingHandler {
            attempts: Arc::clone(&attempts),
            seen_at: Arc::clone(&seen_at),
        })
        .build()
        .unwrap();
    let second_cancel = CancellationToken::new();
    let second_cancel_for_task = second_cancel.clone();
    let second_handle =
        tokio::spawn(async move { second_worker.run(second_cancel_for_task).await });

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
        "the poison message must reach the dead-letter queue after the restart"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "the restarted worker must continue the count at attempt 3, not restart from 1"
    );

    second_cancel.cancel();
    second_handle.await.unwrap().unwrap();
}

/// Declare a durable queue for tests, as required on RabbitMQ 4 (a
/// transient non-exclusive queue is rejected). Cleans nothing up: the
/// container is torn down with the test.
async fn declare_durable_queue(uri: &str, name: &str) {
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
                durable: true,
                exclusive: false,
                auto_delete: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("durable queue declare must succeed");
}

/// Issue #212: under `AckMode::AckOnReceive` with a dead-letter routing
/// key, a flood of poison deliveries must be dead-lettered and must NOT
/// wedge the consumer. Before the fix, confirms / the DLQ were only set
/// up for `Manual`, so every poison DLQ publish failed with
/// `NotRequested`, the delivery was left unacked, and after `prefetch`
/// poison messages the consumer stalled permanently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn ack_on_receive_dead_letters_poison_flood_without_wedging() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.ackonreceive.poison.source";
    let dlr_queue = "worker.ackonreceive.poison.parked";
    declare_durable_queue(&uri, queue_name).await;

    // Publish more poison messages than the prefetch window so a wedge
    // would be observable: each lacks the AMQP `type` property.
    let publisher = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let publish_channel = publisher.create_channel().await.unwrap();
    let poison_count = 20u32;
    for _ in 0..poison_count {
        publish_channel
            .basic_publish(
                ShortString::from(""),
                ShortString::from(queue_name),
                BasicPublishOptions::default(),
                b"{}",
                BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::AckOnReceive)
        .prefetch(8)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // All poison messages must land in the DLQ. If the consumer wedged
    // after `prefetch` messages, the count would stall below poison_count.
    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let mut parked = 0u32;
    for _ in 0..200 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(Some(_msg)) = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
        {
            parked += 1;
            if parked == poison_count {
                break;
            }
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert_eq!(
        parked, poison_count,
        "every poison delivery must be dead-lettered; a lower count means the consumer wedged on prefetch"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "the handler must never run for a delivery missing its `type` property"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

/// Issue #211: in `AckMode::Manual` WITHOUT a dead-letter routing key,
/// publisher confirms must still be enabled so the retry copy's confirm
/// is awaited before the original is acked. This test exercises the
/// retry path (no DL key) end to end: a handler that fails once then
/// succeeds must see the message redelivered and finally processed,
/// proving the retry copy was durably stored (not lost) under the
/// confirm-enabled path that the fix turns on for all Manual workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn manual_without_dead_letter_enables_confirms_and_retries_safely() {
    let (_container, uri) = start_rabbit().await;
    let queue_name = "worker.manual.noconfirm.retry";
    declare_durable_queue(&uri, queue_name).await;

    let transport = RabbitMqTransport::new(&uri).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::now_v7(),
            },
        )
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(&uri).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Manual)
        .max_attempts(5)
        .retry_delay(Duration::from_millis(200))
        .register_handler::<OrderPlaced, _>(FailOnceHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    for _ in 0..100 {
        if attempts.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "the message must be redelivered after the first failure, proving the confirmed retry copy was stored, not lost"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}
