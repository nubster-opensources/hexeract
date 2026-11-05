//! End-to-end integration test against a real RabbitMQ broker.
//!
//! Run with `cargo test -p hexeract-bus-rabbitmq -- --ignored` on a
//! host with Docker available. The test spins up a fresh RabbitMQ
//! container via `testcontainers`, declares a queue bound to the
//! default exchange, publishes a message through
//! [`RabbitMqTransport`], then consumes it back through `basic_get`.

use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use ed25519_dalek::SigningKey;
use hexeract_bus::Audience;
use hexeract_bus::Binding;
use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::EnvelopeSecurityConfig;
use hexeract_bus::EnvelopeSigner;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::Handler;
use hexeract_bus::Issuer;
use hexeract_bus::KeyId;
use hexeract_bus::KeySourceError;
use hexeract_bus::Message;
use hexeract_bus::Queue;
use hexeract_bus::RawBusPublish;
use hexeract_bus::RoutingKey;
use hexeract_bus::SigningContext;
use hexeract_bus::SigningKeyHandle;
use hexeract_bus::SigningKeySource;
use hexeract_bus::StaticKeySource;
use hexeract_bus::Transport;
use hexeract_bus::VerificationKey;
use hexeract_bus::VerificationKeySource;
use hexeract_bus_rabbitmq::AckMode;
use hexeract_bus_rabbitmq::AmqpMetadataLimits;
use hexeract_bus_rabbitmq::ChannelPool;
use hexeract_bus_rabbitmq::InboundEnvelopeSecurity;
use hexeract_bus_rabbitmq::OutboundEnvelopeSecurity;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::RabbitMqConnectionConfig;
use hexeract_bus_rabbitmq::RabbitMqTransport;
use hexeract_bus_rabbitmq::RabbitMqTransportConfig;
use hexeract_bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract_bus_rabbitmq::ensure_topology;
use hexeract_core::HandlerContext;
use lapin::BasicProperties;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::options::BasicGetOptions;
use lapin::options::BasicPublishOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod harness; // reuse the crate's testcontainers helper

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
    let broker = harness::start_rabbitmq().await;

    // Declare a queue bound to the default exchange. The default
    // exchange routes by routing_key directly to the queue of the
    // same name.
    let consumer_conn = Connection::connect(broker.uri(), ConnectionProperties::default())
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
    let transport = RabbitMqTransport::new(broker.uri())
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
    let broker = harness::start_rabbitmq().await;
    let transport = RabbitMqTransport::new(broker.uri())
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
    let broker = harness::start_rabbitmq().await;
    let transport = RabbitMqTransport::new_with_config(
        broker.uri(),
        &RabbitMqTransportConfig::default().fire_and_forget(),
    )
    .await
    .expect("transport must connect");

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
    let broker = harness::start_rabbitmq().await;

    let consumer_conn = Connection::connect(broker.uri(), ConnectionProperties::default())
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

    let transport = RabbitMqTransport::new(broker.uri())
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
    let broker = harness::start_rabbitmq().await;

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

    let connection = RabbitMqConnection::connect(broker.uri())
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
    let probe = Connection::connect(broker.uri(), ConnectionProperties::default())
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
    seen: harness::CallCounter,
}

impl Handler<OrderPlaced> for RecordingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.seen.record();
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

// -------------------------------------------------- envelope security fixtures

fn security_issuer() -> Issuer {
    Issuer::new("billing-service").expect("valid issuer")
}

fn security_audience() -> Audience {
    Audience::new("ledger-service").expect("valid audience")
}

fn security_key_id() -> KeyId {
    KeyId::new("2026-09").expect("valid key id")
}

fn security_signing_key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

/// A worker-side [`InboundEnvelopeSecurity`] trusting the same key
/// [`security_signing_key`] signs with, requiring a valid signature on
/// every delivery.
fn required_inbound_security() -> InboundEnvelopeSecurity {
    let keys: Arc<dyn VerificationKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_verification_key(
                security_issuer(),
                security_key_id(),
                VerificationKey::from(security_signing_key().verifying_key()),
            )
            .build(),
    );
    let config = EnvelopeSecurityConfig::builder()
        .with_accepted_audience(security_audience())
        .build()
        .expect("valid configuration");
    InboundEnvelopeSecurity::new(keys, config)
}

/// AMQP properties and payload for a delivery signed for `destination`, then
/// tampered with after signing so its signature no longer matches: stands in
/// for a forged delivery reaching a worker configured with
/// [`required_inbound_security`].
///
/// `application_headers` is folded into both the signed canonical
/// representation and the published field table, exactly like a real
/// application header would be, so a test can prove such a header never
/// survives quarantine.
fn forged_signed_properties_and_payload(
    destination: &str,
    message_id: uuid::Uuid,
    correlation_id: uuid::Uuid,
    application_headers: &[(&str, &str)],
) -> (BasicProperties, Vec<u8>) {
    let signed_payload = b"{\"order_id\":\"00000000-0000-0000-0000-000000000001\"}".to_vec();
    let mut headers = HashMap::new();
    for (key, value) in application_headers {
        headers.insert((*key).to_owned(), (*value).to_owned());
    }
    let published_at = SystemTime::now();
    let envelope = BusEnvelope::restore_from_transport(
        message_id,
        "orders.placed".to_owned(),
        signed_payload,
        correlation_id,
        None,
        headers.clone(),
        HashMap::new(),
        published_at,
    );

    let signing_keys: Arc<dyn SigningKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_signing_key(
                security_key_id(),
                SigningKeyHandle::from(security_signing_key()),
            )
            .build(),
    );
    let signer = EnvelopeSigner::new(security_issuer(), signing_keys);
    let security_headers = signer
        .sign(
            &envelope,
            &SigningContext {
                destination,
                audience: &security_audience(),
            },
        )
        .expect("signing must succeed");

    let mut fields = FieldTable::default();
    for (key, value) in &headers {
        fields.insert(
            key.as_str().into(),
            AMQPValue::LongString(value.as_str().into()),
        );
    }
    for (name, value) in &security_headers {
        fields.insert(name.into(), AMQPValue::LongString(value.into()));
    }

    let published_at_secs = published_at
        .duration_since(UNIX_EPOCH)
        .expect("after epoch")
        .as_secs();
    let properties = BasicProperties::default()
        .with_type("orders.placed".into())
        .with_message_id(message_id.to_string().into())
        .with_correlation_id(correlation_id.to_string().into())
        .with_timestamp(published_at_secs)
        .with_headers(fields);

    // Tamper the published payload so it no longer matches what was signed:
    // the security headers and properties are otherwise entirely valid.
    let tampered_payload = b"{\"order_id\":\"11111111-1111-1111-1111-111111111111\"}".to_vec();
    (properties, tampered_payload)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_security_rejection_never_requeues() {
    let broker = harness::start_rabbitmq().await;

    for ack_mode in [
        AckMode::Manual,
        AckMode::AckOnReceive,
        AckMode::Unacknowledged,
    ] {
        let queue_name = format!("worker.security.requeue.{ack_mode:?}");
        declare_temporary_queue(broker.uri(), &queue_name).await;

        let (properties, payload) =
            forged_signed_properties_and_payload(&queue_name, Uuid::now_v7(), Uuid::now_v7(), &[]);

        let publisher = Connection::connect(broker.uri(), ConnectionProperties::default())
            .await
            .unwrap();
        let publish_channel = publisher.create_channel().await.unwrap();
        publish_channel
            .basic_publish(
                ShortString::from(""),
                ShortString::from(queue_name.as_str()),
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .unwrap()
            .await
            .unwrap();

        let attempts = Arc::new(AtomicUsize::new(0));
        let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
        let worker = RabbitMqWorkerBuilder::new(consumer_conn)
            .queue(queue_name.as_str())
            .ack_mode(ack_mode)
            .envelope_security(Arc::new(required_inbound_security()))
            .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
                attempts: Arc::clone(&attempts),
            })
            .build()
            .unwrap();

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

        tokio::time::sleep(Duration::from_millis(800)).await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "handler must never run for a security rejection ({ack_mode:?})"
        );

        let probe = Connection::connect(broker.uri(), ConnectionProperties::default())
            .await
            .unwrap();
        let probe_channel = probe.create_channel().await.unwrap();
        let remaining = probe_channel
            .basic_get(queue_name.as_str().into(), BasicGetOptions::default())
            .await
            .unwrap();
        assert!(
            remaining.is_none(),
            "a security rejection must never leave the delivery queued for redelivery \
             ({ack_mode:?})"
        );

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_security_rejection_is_dead_lettered_without_its_headers() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.security.source";
    let dlr_queue = "worker.security.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let message_id = Uuid::from_u128(0x5555_5555_5555_5555_5555_5555_5555_5555);
    let correlation_id = Uuid::from_u128(0x6666_6666_6666_6666_6666_6666_6666_6666);
    let (properties, payload) = forged_signed_properties_and_payload(
        queue_name,
        message_id,
        correlation_id,
        &[("tenant", "acme-confidential")],
    );

    let publisher = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let publish_channel = publisher.create_channel().await.unwrap();
    publish_channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(queue_name),
            BasicPublishOptions::default(),
            &payload,
            properties,
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .dead_letter_routing_key(dlr_queue)
        .envelope_security(Arc::new(required_inbound_security()))
        .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    let parked = wait_for_dead_letter(broker.uri(), dlr_queue)
        .await
        .expect("a security rejection must be routed to the dead-letter queue");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "handler must never run for a delivery whose signature fails verification"
    );

    let quarantined_headers = parked
        .delivery
        .properties
        .headers()
        .as_ref()
        .expect("the quarantine copy must carry an explicit empty table");
    assert!(
        quarantined_headers.inner().is_empty(),
        "neither the application header nor the unauthenticated security headers may be \
         republished"
    );
    assert_eq!(
        parked
            .delivery
            .properties
            .message_id()
            .as_ref()
            .map(ShortString::as_str),
        Some(message_id.to_string()).as_deref(),
        "the quarantine copy must stay diagnosable"
    );
    assert_eq!(
        parked
            .delivery
            .properties
            .correlation_id()
            .as_ref()
            .map(ShortString::as_str),
        Some(correlation_id.to_string()).as_deref(),
    );
    assert_eq!(
        parked
            .delivery
            .properties
            .kind()
            .as_ref()
            .map(ShortString::as_str),
        Some("orders.placed"),
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[derive(Debug)]
struct FailOnceHandler {
    attempts: harness::CallCounter,
}

impl Handler<OrderPlaced> for FailOnceHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        if self.attempts.record_and_count_preceding() == 0 {
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
    attempts: harness::CallCounter,
    seen_at: Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
}

impl Handler<OrderPlaced> for TimestampingFailingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        // The timestamp is pushed before the attempt is counted: a test woken
        // by `await_count(n)` must find n timestamps already recorded.
        self.seen_at
            .lock()
            .expect("timestamp mutex must not be poisoned")
            .push(std::time::Instant::now());
        self.attempts.record();
        Err(hexeract_bus::BusError::Internal(
            "deliberate poison".to_owned(),
        ))
    }
}

struct PanickingHandler {
    seen_after: harness::CallCounter,
}

impl Handler<OrderPlaced> for PanickingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(&self, msg: OrderPlaced, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        assert!(
            msg.order_id != Uuid::from_u128(0),
            "deliberate panic in handler"
        );
        self.seen_after.record();
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
    completed: harness::CallCounter,
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
        self.completed.record();
        Ok(())
    }
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn transport_config_settings_reach_the_connected_transport() {
    let broker = harness::start_rabbitmq().await;

    let limits = AmqpMetadataLimits {
        max_headers: 3,
        ..AmqpMetadataLimits::default()
    };
    let keys: Arc<dyn SigningKeySource> = Arc::new(StaticKeySource::builder().build());
    let security = Arc::new(OutboundEnvelopeSecurity::new(
        Issuer::new("billing-service").expect("valid issuer"),
        Audience::new("ledger-service").expect("valid audience"),
        keys,
    ));

    let config = RabbitMqTransportConfig::default()
        .with_metadata_limits(limits)
        .with_outbound_envelope_security(Arc::clone(&security));

    let transport = RabbitMqTransport::new_with_config(broker.uri(), &config)
        .await
        .expect("transport must connect");

    assert_eq!(
        transport.metadata_limits(),
        limits,
        "the metadata limits carried by RabbitMqTransportConfig must reach the connected transport"
    );
    assert!(
        transport.outbound_envelope_security().is_some(),
        "the outbound envelope security carried by RabbitMqTransportConfig must reach the connected transport"
    );
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

    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.bounded.unack";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri())
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
    let completed = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Unacknowledged)
        .max_buffered(MAX_BUFFERED)
        .register_handler::<OrderPlaced, _>(ConcurrencyProbeHandler {
            live: Arc::clone(&live),
            peak: Arc::clone(&peak),
            completed: completed.clone(),
            work: Duration::from_millis(100),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Let every batch drain so the peak has time to reach the bound. The
    // bound itself is proven below by `peak`, which the handler raises with
    // `fetch_max` on every entry: asserting on `live` here would observe a
    // drained queue and pass whatever the worker did.
    completed.await_count(MESSAGE_COUNT).await;

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
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.happy";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri()).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(1),
            },
        )
        .await
        .unwrap();

    let seen = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .register_handler::<OrderPlaced, _>(RecordingHandler { seen: seen.clone() })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    seen.await_count(1).await;
    assert_eq!(seen.count(), 1, "the handler must run exactly once");

    cancel.cancel();
    handle
        .await
        .expect("worker task joins")
        .expect("worker run returns Ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_retries_on_failure_then_succeeds() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.retry";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri()).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(2),
            },
        )
        .await
        .unwrap();

    let attempts = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .max_attempts(5)
        .retry_delay(Duration::from_millis(200))
        .register_handler::<OrderPlaced, _>(FailOnceHandler {
            attempts: attempts.clone(),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    attempts.await_count(2).await;
    assert_eq!(
        attempts.count(),
        2,
        "the handler must run exactly twice: one failure, one success"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_routes_to_dead_letter_after_exhausting_attempts() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.dlr.source";
    let dlr_queue = "worker.dlr.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri()).await.unwrap();
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
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
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
    let probe = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let mut parked = None;
    for _ in 0..80 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(candidate) = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
            && candidate.is_some()
        {
            parked = candidate;
            break;
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
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.oversize.source";
    let dlr_queue = "worker.oversize.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let publisher = Connection::connect(broker.uri(), ConnectionProperties::default())
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
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
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
    let probe = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let mut parked = None;
    for _ in 0..80 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(candidate) = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
            && candidate.is_some()
        {
            parked = candidate;
            break;
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

/// Poll `dead_letter_queue` until the worker parks a delivery there.
///
/// The queue is declared by the worker, so an early probe can race its
/// startup: a `basic_get` on a missing queue is a channel-closing soft error,
/// hence a fresh channel per attempt.
async fn wait_for_dead_letter(
    uri: &str,
    dead_letter_queue: &str,
) -> Option<lapin::message::BasicGetMessage> {
    let probe = Connection::connect(uri, ConnectionProperties::default())
        .await
        .unwrap();
    for _ in 0..80 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(candidate) = probe_channel
            .basic_get(dead_letter_queue.into(), BasicGetOptions::default())
            .await
            && candidate.is_some()
        {
            return candidate;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn oversized_metadata_is_quarantined_without_headers() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.metadata.source";
    let dlr_queue = "worker.metadata.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let publisher = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let publish_channel = publisher.create_channel().await.unwrap();

    // A tiny payload with one header a single byte over the configured value
    // limit: the payload cap alone would let this through.
    let mut headers = FieldTable::default();
    headers.insert("tenant".into(), AMQPValue::LongString(vec![b'x'; 9].into()));
    publish_channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(queue_name),
            BasicPublishOptions::default(),
            b"{\"order_id\":\"00000000-0000-0000-0000-000000000001\"}",
            BasicProperties::default()
                .with_type("orders.placed".into())
                .with_message_id("33333333-3333-3333-3333-333333333333".into())
                .with_correlation_id("44444444-4444-4444-4444-444444444444".into())
                .with_headers(headers),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .metadata_limits(AmqpMetadataLimits {
            max_value_bytes: 8,
            ..AmqpMetadataLimits::default()
        })
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(AlwaysFailingHandler {
            attempts: Arc::clone(&attempts),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    let parked = wait_for_dead_letter(broker.uri(), dlr_queue)
        .await
        .expect("oversized metadata must be routed to the dead-letter queue");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "handler must never run for a delivery whose metadata was refused"
    );
    let quarantined_headers = parked
        .delivery
        .properties
        .headers()
        .as_ref()
        .expect("the quarantine copy must carry an explicit empty table");
    assert!(
        quarantined_headers.inner().is_empty(),
        "the refused field table must be rebuilt empty, never republished"
    );
    assert_eq!(
        parked
            .delivery
            .properties
            .message_id()
            .as_ref()
            .map(ShortString::as_str),
        Some("33333333-3333-3333-3333-333333333333"),
        "the quarantine copy must stay diagnosable"
    );
    assert_eq!(
        parked
            .delivery
            .properties
            .correlation_id()
            .as_ref()
            .map(ShortString::as_str),
        Some("44444444-4444-4444-4444-444444444444"),
    );
    assert_eq!(
        parked
            .delivery
            .properties
            .kind()
            .as_ref()
            .map(ShortString::as_str),
        Some("orders.placed"),
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_routes_undecodable_delivery_to_dead_letter_queue() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.undecodable.source";
    let dlr_queue = "worker.undecodable.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let publisher = Connection::connect(broker.uri(), ConnectionProperties::default())
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
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
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

    let probe = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let mut parked = None;
    for _ in 0..80 {
        let probe_channel = probe.create_channel().await.unwrap();
        if let Ok(candidate) = probe_channel
            .basic_get(dlr_queue.into(), BasicGetOptions::default())
            .await
            && candidate.is_some()
        {
            parked = candidate;
            break;
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
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.broker.down";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Let the consumer subscribe before taking the broker down.
    tokio::time::sleep(Duration::from_millis(500)).await;
    broker.stop_gracefully().await;

    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("worker must exit after the broker stops")
        .expect("worker task must not panic");
    assert!(
        matches!(result, Err(BusError::Connection { .. })),
        "a dead broker must surface as a connection error so a supervisor restarts the worker, got {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_returns_ok_on_cancellation_with_idle_queue() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.cancel";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
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
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.panic";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri()).await.unwrap();
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

    let seen_after = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .register_handler::<OrderPlaced, _>(PanickingHandler {
            seen_after: seen_after.clone(),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    seen_after.await_count(1).await;

    cancel.cancel();
    handle
        .await
        .expect("worker task joins")
        .expect("worker run returns Ok");

    assert_eq!(
        seen_after.count(),
        1,
        "worker must process the second delivery after the first handler panicked"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worker_delays_retries_and_retry_count_survives_restart() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.poison";
    let dlr_queue = "worker.poison.parked";
    let retry_delay = Duration::from_millis(300);
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri()).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(4),
            },
        )
        .await
        .unwrap();

    let attempts = harness::CallCounter::new();
    let seen_at = Arc::new(std::sync::Mutex::new(Vec::new()));

    // First worker instance: two failing attempts, spaced by the wait
    // queue TTL rather than redelivered in a tight loop.
    let first_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let first_worker = RabbitMqWorkerBuilder::new(first_conn)
        .queue(queue_name)
        .max_attempts(3)
        .retry_delay(retry_delay)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(TimestampingFailingHandler {
            attempts: attempts.clone(),
            seen_at: Arc::clone(&seen_at),
        })
        .build()
        .unwrap();
    let first_cancel = CancellationToken::new();
    let first_cancel_for_task = first_cancel.clone();
    let first_handle = tokio::spawn(async move { first_worker.run(first_cancel_for_task).await });

    attempts.await_count(2).await;
    assert_eq!(
        attempts.count(),
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

    let second_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let second_worker = RabbitMqWorkerBuilder::new(second_conn)
        .queue(queue_name)
        .max_attempts(3)
        .retry_delay(retry_delay)
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(TimestampingFailingHandler {
            attempts: attempts.clone(),
            seen_at: Arc::clone(&seen_at),
        })
        .build()
        .unwrap();
    let second_cancel = CancellationToken::new();
    let second_cancel_for_task = second_cancel.clone();
    let second_handle =
        tokio::spawn(async move { second_worker.run(second_cancel_for_task).await });

    let probe = Connection::connect(broker.uri(), ConnectionProperties::default())
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
        attempts.count(),
        3,
        "the restarted worker must continue the count at attempt 3, not restart from 1"
    );

    second_cancel.cancel();
    second_handle.await.unwrap().unwrap();
}

#[derive(Debug)]
struct MessageIdRecordingFailingHandler {
    seen_message_ids: Arc<std::sync::Mutex<Vec<String>>>,
    calls: harness::CallCounter,
}

impl Handler<OrderPlaced> for MessageIdRecordingFailingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(&self, _message: OrderPlaced, ctx: &HandlerContext) -> Result<(), Self::Error> {
        self.seen_message_ids
            .lock()
            .expect("message id mutex must not be poisoned")
            .push(format!("{:?}", ctx.message_id));
        self.calls.record();
        Err(hexeract_bus::BusError::Internal(
            "deliberate poison".to_owned(),
        ))
    }
}

/// Publishes `properties` over raw AMQP (the transport always stamps a valid
/// `message_id`, which would hide the defect), lets the handler fail three
/// times, and returns the `message_id` the handler saw on each attempt.
async fn message_ids_seen_across_retries(
    queue_name: &str,
    dlr_queue: &str,
    properties: BasicProperties,
) -> Vec<String> {
    let broker = harness::start_rabbitmq().await;
    declare_temporary_queue(broker.uri(), queue_name).await;

    let publisher = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .unwrap();
    let publish_channel = publisher.create_channel().await.unwrap();
    publish_channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(queue_name),
            BasicPublishOptions::default(),
            b"{\"order_id\":\"00000000-0000-0000-0000-000000000005\"}",
            properties,
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let seen_message_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .max_attempts(3)
        .retry_delay(Duration::from_millis(200))
        .dead_letter_routing_key(dlr_queue)
        .register_handler::<OrderPlaced, _>(MessageIdRecordingFailingHandler {
            seen_message_ids: Arc::clone(&seen_message_ids),
            calls: calls.clone(),
        })
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    calls.await_count(3).await;

    cancel.cancel();
    handle.await.unwrap().unwrap();
    seen_message_ids.lock().expect("message id mutex").clone()
}

fn assert_identity_is_stable(seen_message_ids: &[String]) {
    assert!(
        seen_message_ids.len() >= 2,
        "the handler must have run at least twice, saw {seen_message_ids:?}"
    );
    assert!(
        seen_message_ids.iter().all(|id| id == &seen_message_ids[0]),
        "message_id must be identical on every attempt, saw {seen_message_ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn retry_preserves_generated_message_id_when_property_is_absent() {
    let seen = message_ids_seen_across_retries(
        "worker.identity.absent",
        "worker.identity.absent.parked",
        BasicProperties::default().with_type("orders.placed".into()),
    )
    .await;

    assert_identity_is_stable(&seen);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn retry_preserves_generated_message_id_when_property_is_unparseable() {
    let seen = message_ids_seen_across_retries(
        "worker.identity.unparseable",
        "worker.identity.unparseable.parked",
        BasicProperties::default()
            .with_type("orders.placed".into())
            .with_message_id("not-a-uuid".into()),
    )
    .await;

    assert_identity_is_stable(&seen);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_raw_round_trips_with_supplied_message_id() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "raw.publish.round_trip";

    let consumer_conn = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .expect("consumer connection must open");
    let consumer_channel = consumer_conn
        .create_channel()
        .await
        .expect("consumer channel must open");
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

    let transport = RabbitMqTransport::new(broker.uri())
        .await
        .expect("transport must connect");
    let known_id = Uuid::now_v7();
    let raw_payload = b"{\"label\":\"x\"}";
    transport
        .publish_raw(queue_name, known_id, "reminders.due", raw_payload)
        .await
        .expect("publish_raw must succeed");

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

    assert_eq!(
        delivery.data.as_slice(),
        raw_payload,
        "payload must arrive byte-for-byte unchanged"
    );
    let observed_message_id = delivery
        .properties
        .message_id()
        .as_ref()
        .map(ShortString::as_str)
        .expect("AMQP message_id property must be set");
    assert_eq!(
        observed_message_id,
        known_id.to_string(),
        "the AMQP message_id property must equal the caller-supplied id"
    );
    assert_eq!(
        delivery.properties.kind().as_ref().map(ShortString::as_str),
        Some("reminders.due"),
        "the AMQP type property must carry the caller-supplied message_type"
    );
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
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.ackonreceive.poison.source";
    let dlr_queue = "worker.ackonreceive.poison.parked";
    declare_durable_queue(broker.uri(), queue_name).await;

    // Publish more poison messages than the prefetch window so a wedge
    // would be observable: each lacks the AMQP `type` property.
    let publisher = Connection::connect(broker.uri(), ConnectionProperties::default())
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
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
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
    let probe = Connection::connect(broker.uri(), ConnectionProperties::default())
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
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.manual.noconfirm.retry";
    declare_durable_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri()).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::now_v7(),
            },
        )
        .await
        .unwrap();

    let attempts = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Manual)
        .max_attempts(5)
        .retry_delay(Duration::from_millis(200))
        .register_handler::<OrderPlaced, _>(FailOnceHandler {
            attempts: attempts.clone(),
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    attempts.await_count(2).await;
    assert_eq!(
        attempts.count(),
        2,
        "the message must be redelivered after the first failure, proving the confirmed retry copy was stored, not lost"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn channel_pool_bounds_live_channels_and_returns_them_under_contention() {
    let broker = harness::start_rabbitmq().await;
    let connection = RabbitMqConnection::connect(broker.uri())
        .await
        .expect("pool connection must open");

    let max_size = 4;
    let pool = Arc::new(ChannelPool::new(connection, max_size));

    // Drive far more concurrent acquirers than the pool's bound so the
    // semaphore is the only thing keeping live channels in check. Each
    // task holds its channel briefly to force real overlap.
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak_in_flight = Arc::new(AtomicUsize::new(0));

    let tasks: Vec<_> = (0..64)
        .map(|_| {
            let pool = Arc::clone(&pool);
            let in_flight = Arc::clone(&in_flight);
            let peak_in_flight = Arc::clone(&peak_in_flight);
            tokio::spawn(async move {
                let channel = pool.acquire().await.expect("acquire must succeed");
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak_in_flight.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(25)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                drop(channel);
            })
        })
        .collect();

    for task in tasks {
        task.await.expect("acquirer task must not panic");
    }

    // Bound: the semaphore must cap concurrently live channels at
    // max_size. Without it, acquire never blocks and a burst opens one
    // channel per task.
    let peak = peak_in_flight.load(Ordering::SeqCst);
    assert!(
        peak <= max_size,
        "live channels must stay bounded by max_size ({max_size}), observed peak {peak}"
    );

    // Reliable return: channels must be handed back to the idle cache
    // rather than dropped on a contended lock. After the burst the cache
    // is warmed to its full capacity.
    let idle = pool.idle_len();
    assert_eq!(
        idle, max_size,
        "all live channels must be returned to the pool; cache holds {idle} of {max_size}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn connect_with_config_fails_fast_on_access_refused() {
    let broker = harness::start_rabbitmq().await;
    // Same host and port as the running container, deliberately wrong
    // credentials. The default image accepts guest/guest, so guest with a
    // bad password triggers an ACCESS_REFUSED handshake rejection.
    let bad_uri = format!(
        "amqp://guest:wrong-password@{}:{}/%2f",
        broker.host(),
        broker.port()
    );

    let started = std::time::Instant::now();
    // A generous per-attempt delay: if the loop wrongly retried all five
    // attempts it would sleep ~30s (2 + 4 + 8 + 16). A permanent failure must
    // break the loop early instead of draining that budget.
    let config = RabbitMqConnectionConfig::default().with_retry_base_delay(Duration::from_secs(2));
    let result = RabbitMqConnection::connect_with_config(&bad_uri, &config).await;
    let elapsed = started.elapsed();

    let err = result.expect_err("bad credentials must fail");
    assert_eq!(
        err.is_retryable_connection(),
        Some(false),
        "ACCESS_REFUSED must be classified permanent"
    );
    // A freshly started broker consistently rejects the very first handshake
    // with a transient error before it is ready to return a clean
    // ACCESS_REFUSED, so the loop sleeps exactly one base delay (2s) and then
    // classifies the second attempt as permanent: ~2s total. The next
    // transient retry would only be reached at ~6s (2 + 4), so a 3s ceiling
    // proves the early break with a clean margin without weakening the
    // fail-fast against the ~30s full-budget drain (#340).
    assert!(
        elapsed < Duration::from_secs(3),
        "a permanent failure must not sleep through the retry budget, took {elapsed:?}"
    );
    let rendered = format!("{err:?} {err}");
    assert!(
        !rendered.contains("wrong-password"),
        "the credential must never leak into the error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn publish_recovers_after_a_broker_blip() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "publish.recovery";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri())
        .await
        .expect("transport connects");

    // Publish once while healthy.
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::now_v7(),
            },
        )
        .await
        .expect("first publish succeeds");

    // Freeze then resume the broker to force a reconnect.
    broker.pause().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    broker.unpause().await;

    // A publish issued after the blip must eventually succeed on its own,
    // without rebuilding the transport. Retry the call a few times to absorb
    // the reconnection window; the point is that it recovers rather than
    // failing forever.
    let mut published = false;
    for _ in 0..20 {
        if transport
            .publish(
                queue_name,
                &OrderPlaced {
                    order_id: Uuid::now_v7(),
                },
            )
            .await
            .is_ok()
        {
            published = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        published,
        "publisher must self-heal after a broker blip (#334)"
    );
}

/// Wraps a [`StaticKeySource`] to count every call to
/// [`SigningKeySource::current_signing_key`], so a test can prove how many
/// times a publish actually asked for a signature.
struct CountingSigningKeySource {
    inner: StaticKeySource,
    calls: Arc<AtomicUsize>,
}

impl SigningKeySource for CountingSigningKeySource {
    fn current_signing_key(&self) -> Result<(KeyId, &SigningKeyHandle), KeySourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.current_signing_key()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn signing_happens_once_per_publication() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "publish.signing-once";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let signing_calls = Arc::new(AtomicUsize::new(0));
    let keys: Arc<dyn SigningKeySource> = Arc::new(CountingSigningKeySource {
        inner: StaticKeySource::builder()
            .with_signing_key(
                KeyId::new("2026-09").expect("valid key id"),
                SigningKeyHandle::from(ed25519_dalek::SigningKey::from_bytes(&[5; 32])),
            )
            .build(),
        calls: Arc::clone(&signing_calls),
    });
    let security = Arc::new(OutboundEnvelopeSecurity::new(
        Issuer::new("billing-service").expect("valid issuer"),
        Audience::new("ledger-service").expect("valid audience"),
        keys,
    ));

    let transport = RabbitMqTransport::new_with_config(
        broker.uri(),
        &RabbitMqTransportConfig::default().with_outbound_envelope_security(security),
    )
    .await
    .expect("transport connects");

    // A healthy publish is one outer call, so exactly one signature.
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::now_v7(),
            },
        )
        .await
        .expect("first publish succeeds");
    assert_eq!(signing_calls.load(Ordering::SeqCst), 1);

    // Freeze then resume the broker to force `publish_envelope`'s internal
    // channel-recovery loop to retry, the same way as
    // `publish_recovers_after_a_broker_blip` above.
    broker.pause().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    broker.unpause().await;

    let mut attempts: usize = 0;
    loop {
        attempts += 1;
        let published = transport
            .publish(
                queue_name,
                &OrderPlaced {
                    order_id: Uuid::now_v7(),
                },
            )
            .await
            .is_ok();
        if published {
            break;
        }
        assert!(
            attempts < 20,
            "publisher must self-heal after a broker blip (#334)"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // One signature per outer call: one healthy publish plus however many
    // attempts the blip required, never more.
    //
    // What this measures, and what it does not. Pausing the broker makes the
    // internal recovery loop of `publish_with_recovery` likely to run twice
    // within one outer call, but nothing here forces it to, so a run where
    // every attempt succeeds on its first internal iteration would pass this
    // assertion without having exercised a retry at all. It is therefore a
    // check on the observable count, not the guarantee. The guarantee is that
    // `publish_with_recovery` is a free function that never receives the
    // transport or its envelope security, so a second signature cannot be
    // requested from inside the retry: that line does not compile.
    assert_eq!(signing_calls.load(Ordering::SeqCst), 1 + attempts);
}

#[derive(Debug)]
struct SlowCountingHandler {
    started: harness::CallCounter,
    done: harness::CallCounter,
}

impl Handler<OrderPlaced> for SlowCountingHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.started.record();
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.done.record();
        Ok(())
    }
}

/// Publish `count` orders to the default exchange, routed to `queue_name`.
async fn publish_orders(uri: &str, queue_name: &str, count: usize) {
    let transport = RabbitMqTransport::new(uri)
        .await
        .expect("transport connects");
    for _ in 0..count {
        transport
            .publish(
                queue_name,
                &OrderPlaced {
                    order_id: Uuid::now_v7(),
                },
            )
            .await
            .expect("publish succeeds");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn no_ack_shutdown_drains_in_flight_handlers() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "noack.drain";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let started = harness::CallCounter::new();
    let done = harness::CallCounter::new();
    let batch = 4usize;
    publish_orders(broker.uri(), queue_name, batch).await;

    let consumer_conn = RabbitMqConnection::connect(broker.uri())
        .await
        .expect("consumer connects");
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Unacknowledged)
        .max_buffered(batch)
        .register_handler::<OrderPlaced, _>(SlowCountingHandler {
            started: started.clone(),
            done: done.clone(),
        })
        .build()
        .expect("worker builds");

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Cancel only once every delivery is actually inside its (slow) handler:
    // the rendezvous is the handler reaching `batch`, never a duration.
    started.await_count(batch).await;
    cancel.cancel();

    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("worker drains and exits")
        .expect("worker task joins")
        .expect("worker run returns Ok");

    assert_eq!(
        done.count(),
        batch,
        "criterion 11: every in-flight no_ack handler must finish before shutdown (#338, #525)"
    );
}

/// A handler whose simulated work is long but which gives it up as soon as
/// `ctx.cancellation` fires.
#[derive(Debug)]
struct CooperativeHandler {
    started: harness::CallCounter,
    observed_cancellation: harness::CallCounter,
}

impl Handler<OrderPlaced> for CooperativeHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(&self, _message: OrderPlaced, ctx: &HandlerContext) -> Result<(), Self::Error> {
        self.started.record();
        tokio::select! {
            () = ctx.cancellation.cancelled() => self.observed_cancellation.record(),
            () = tokio::time::sleep(Duration::from_secs(3600)) => {}
        }
        Ok(())
    }
}

/// A handler that never returns and never looks at `ctx.cancellation`.
#[derive(Debug)]
struct DeafHandler {
    started: harness::CallCounter,
}

impl Handler<OrderPlaced> for DeafHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.started.record();
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn no_ack_handler_observes_shutdown_through_ctx_cancellation_and_run_returns_ok() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "noack.cooperative";
    declare_temporary_queue(broker.uri(), queue_name).await;
    publish_orders(broker.uri(), queue_name, 1).await;

    let started = harness::CallCounter::new();
    let observed_cancellation = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri())
        .await
        .expect("consumer connects");
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Unacknowledged)
        .max_buffered(1)
        .register_handler::<OrderPlaced, _>(CooperativeHandler {
            started: started.clone(),
            observed_cancellation: observed_cancellation.clone(),
        })
        .build()
        .expect("worker builds");

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    started.await_count(1).await;
    cancel.cancel();

    // The simulated work lasts an hour: returning inside this deadline proves
    // the handler saw the cancellation instead of being waited for.
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("criterion 12: run must not wait for the handler's simulated work")
        .expect("worker task joins")
        .expect("criterion 12: run returns Ok when every handler finished in the grace");

    assert_eq!(
        observed_cancellation.count(),
        1,
        "criterion 12: the handler must observe the shutdown through ctx.cancellation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn no_ack_handler_ignoring_cancellation_makes_run_report_the_aborted_count() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "noack.deaf";
    declare_temporary_queue(broker.uri(), queue_name).await;
    let in_flight = 2usize;
    publish_orders(broker.uri(), queue_name, in_flight).await;

    let started = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri())
        .await
        .expect("consumer connects");
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Unacknowledged)
        .max_buffered(in_flight)
        .shutdown_grace(Duration::from_millis(500))
        .register_handler::<OrderPlaced, _>(DeafHandler {
            started: started.clone(),
        })
        .build()
        .expect("worker builds");

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    started.await_count(in_flight).await;
    cancel.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("criterion 13: run must exit once the grace expires")
        .expect("worker task joins");

    match outcome {
        Err(BusError::ShutdownDrainTimedOut { abandoned, grace }) => {
            assert_eq!(
                abandoned, in_flight,
                "criterion 13: abandoned must equal the number of handlers in flight"
            );
            assert_eq!(
                grace,
                Duration::from_millis(500),
                "criterion 13: the error must carry the configured grace"
            );
        }
        other => panic!("criterion 13: expected ShutdownDrainTimedOut, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn cancellation_frees_a_worker_parked_on_a_full_in_flight_bound() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "noack.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;
    publish_orders(broker.uri(), queue_name, 2).await;

    let started = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri())
        .await
        .expect("consumer connects");
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Unacknowledged)
        .max_buffered(1)
        .shutdown_grace(Duration::from_millis(500))
        .register_handler::<OrderPlaced, _>(DeafHandler {
            started: started.clone(),
        })
        .build()
        .expect("worker builds");

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // The one permit is taken by a handler that never returns; the second
    // delivery leaves the worker parked on the permit.
    started.await_count(1).await;
    cancel.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("criterion 14: cancellation must free a worker parked on the in-flight bound")
        .expect("worker task joins");

    assert!(
        matches!(outcome, Err(BusError::ShutdownDrainTimedOut { .. })),
        "criterion 14: the stuck handler is aborted after the grace, got {outcome:?}"
    );
}

/// A handler that holds its work until the scenario releases it, so the
/// scenario decides when the handler finishes instead of a clock.
#[derive(Debug)]
struct GatedHandler {
    started: harness::CallCounter,
    done: harness::CallCounter,
    release: std::sync::Arc<tokio::sync::Semaphore>,
}

impl Handler<OrderPlaced> for GatedHandler {
    type Error = hexeract_bus::BusError;

    async fn handle(
        &self,
        _message: OrderPlaced,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.started.record();
        let _permit = self
            .release
            .acquire()
            .await
            .expect("the release semaphore is never closed");
        self.done.record();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn lost_connection_drains_in_flight_handlers_before_run_returns_the_connection_error() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "noack.connection.lost";
    declare_temporary_queue(broker.uri(), queue_name).await;
    let in_flight = 2usize;
    publish_orders(broker.uri(), queue_name, in_flight).await;

    let started = harness::CallCounter::new();
    let done = harness::CallCounter::new();
    let release = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let consumer_conn = RabbitMqConnection::connect(broker.uri())
        .await
        .expect("consumer connects");
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .ack_mode(AckMode::Unacknowledged)
        .max_buffered(in_flight)
        .register_handler::<OrderPlaced, _>(GatedHandler {
            started: started.clone(),
            done: done.clone(),
            release: std::sync::Arc::clone(&release),
        })
        .build()
        .expect("worker builds");

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let mut handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    started.await_count(in_flight).await;
    broker.stop_gracefully().await;

    // The broker is gone and the handlers are still working: run must be
    // draining them, not returning. A run that exits here abandoned them.
    assert!(
        tokio::time::timeout(Duration::from_secs(3), &mut handle)
            .await
            .is_err(),
        "criterion 20: run returned while handlers were still in flight"
    );

    release.add_permits(in_flight);
    let outcome = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("criterion 20: run must return once the handlers finished")
        .expect("worker task joins");

    assert_eq!(
        done.count(),
        in_flight,
        "criterion 20: every in-flight handler must have finished before run returned"
    );
    assert!(
        matches!(outcome, Err(BusError::Connection { .. })),
        "criterion 20: the original connection error must be returned, got {outcome:?}"
    );
}

/// Whether `name` exists on the broker, probed by a passive declaration.
///
/// A passive declaration of a missing queue closes the channel it ran on, so
/// every probe opens its own connection and channel.
async fn queue_exists(uri: &str, name: &str) -> bool {
    let Ok(connection) = Connection::connect(uri, ConnectionProperties::default()).await else {
        return false;
    };
    let Ok(channel) = connection.create_channel().await else {
        return false;
    };
    channel
        .queue_declare(
            name.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .is_ok()
}

/// Wait, bounded by [`harness::PROGRESS_TIMEOUT`], until `name` exists.
async fn await_queue(uri: &str, name: &str) {
    let wait = async {
        while !queue_exists(uri, name).await {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(harness::PROGRESS_TIMEOUT, wait)
        .await
        .unwrap_or_else(|_| panic!("queue {name} never appeared on the broker"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn transport_config_with_exchange_declares_the_exchange_and_publishes_on_it() {
    let broker = harness::start_rabbitmq().await;
    let exchange = Exchange::new("settled.direct", ExchangeKind::Direct)
        .expect("exchange must validate")
        .durable(true)
        .auto_delete(false);
    let queue_name = "settled.direct.received";
    let routing_key = "orders.placed";

    let config = RabbitMqTransportConfig::default().with_exchange(exchange.clone());
    let transport = RabbitMqTransport::new_with_config(broker.uri(), &config)
        .await
        .expect("transport must connect and declare the exchange");
    assert_eq!(transport.exchange(), exchange.name);

    let consumer_conn = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .expect("consumer connection must open");
    let consumer_channel = consumer_conn
        .create_channel()
        .await
        .expect("consumer channel must open");
    consumer_channel
        .queue_declare(
            queue_name.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare must succeed");
    consumer_channel
        .queue_bind(
            queue_name.into(),
            exchange.name.as_str().into(),
            routing_key.into(),
            lapin::options::QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("binding to the exchange the transport declared must succeed");

    transport
        .publish(
            routing_key,
            &OrderPlaced {
                order_id: Uuid::from_u128(11),
            },
        )
        .await
        .expect("a confirmed, mandatory publish through the exchange must be routed");

    let delivery = tokio::time::timeout(harness::PROGRESS_TIMEOUT, async {
        loop {
            let candidate = consumer_channel
                .basic_get(queue_name.into(), BasicGetOptions::default())
                .await
                .expect("basic_get must succeed");
            if let Some(delivery) = candidate {
                return delivery;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the message must reach the queue bound to the exchange");
    assert_eq!(
        delivery.properties.kind().as_ref().map(ShortString::as_str),
        Some("orders.placed")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn transport_config_fire_and_forget_publishes_without_confirmation() {
    let broker = harness::start_rabbitmq().await;
    let config = RabbitMqTransportConfig::default().fire_and_forget();
    let transport = RabbitMqTransport::new_with_config(broker.uri(), &config)
        .await
        .expect("transport must connect");

    let message_id = transport
        .publish(
            "orders.nowhere",
            &OrderPlaced {
                order_id: Uuid::from_u128(12),
            },
        )
        .await
        .expect("fire-and-forget publish must not await a broker verdict");

    assert_ne!(message_id, Uuid::nil());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn declare_queue_makes_the_worker_consume_a_queue_nobody_declared() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.declared";
    assert!(
        !queue_exists(broker.uri(), queue_name).await,
        "the scenario needs a queue that does not exist yet"
    );

    let seen = harness::CallCounter::new();
    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .declare_queue()
        .register_handler::<OrderPlaced, _>(RecordingHandler { seen: seen.clone() })
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    await_queue(broker.uri(), queue_name).await;
    let transport = RabbitMqTransport::new(broker.uri()).await.unwrap();
    transport
        .publish(
            queue_name,
            &OrderPlaced {
                order_id: Uuid::from_u128(13),
            },
        )
        .await
        .expect("the declared queue must route the publish");
    seen.await_count(1).await;
    assert_eq!(seen.count(), 1, "the handler must run exactly once");

    declare_durable_queue(broker.uri(), queue_name).await;

    cancel.cancel();
    handle
        .await
        .expect("worker task joins")
        .expect("worker run returns Ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_worker_without_declare_queue_does_not_create_its_queue() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.not.declared";

    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .register_handler::<OrderPlaced, _>(RecordingHandler::default())
        .build()
        .unwrap();

    let outcome = tokio::time::timeout(
        harness::PROGRESS_TIMEOUT,
        worker.run(CancellationToken::new()),
    )
    .await
    .expect("run must fail on a missing queue rather than wait for it");

    assert!(
        outcome.is_err(),
        "without declare_queue the queue belongs to the operator, and a missing one fails run"
    );
    assert!(
        !queue_exists(broker.uri(), queue_name).await,
        "run must not have created the queue"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn declare_queue_against_a_queue_with_other_arguments_fails_run_without_panic() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "worker.declared.mismatch";

    let setup_connection = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .expect("setup connection must open");
    let setup_channel = setup_connection
        .create_channel()
        .await
        .expect("setup channel must open");
    let mut arguments = FieldTable::default();
    arguments.insert("x-message-ttl".into(), AMQPValue::LongUInt(60_000));
    setup_channel
        .queue_declare(
            queue_name.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .expect("the pre-existing queue must declare");

    let consumer_conn = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let worker = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue_name)
        .declare_queue()
        .register_handler::<OrderPlaced, _>(RecordingHandler::default())
        .build()
        .unwrap();

    let outcome = tokio::time::timeout(
        harness::PROGRESS_TIMEOUT,
        worker.run(CancellationToken::new()),
    )
    .await
    .expect("run must surface the broker's refusal rather than wait");

    assert!(
        matches!(
            outcome,
            Err(BusError::Transport(_) | BusError::Connection { .. })
        ),
        "a PRECONDITION_FAILED declaration must surface as a BusError, got {outcome:?}"
    );
}
