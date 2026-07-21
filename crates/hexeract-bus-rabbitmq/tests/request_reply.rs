//! Docker-backed integration test for the reply inbox consumer
//! (request-reply v0.7.0, lot A2.1).
//!
//! This is the first broker contact for the request-reply path: an
//! aller-simple from the inbox to the [`CorrelationRegistry`], without a
//! responder on the other end. The test publishes the reply envelope by
//! hand, straight to the inbox, and asserts the waiting
//! [`hexeract_bus::PendingReply`] resolves with it.
//!
//! Run with `cargo test -p hexeract-bus-rabbitmq --test request_reply --
//! --ignored` on a host with Docker available.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::CorrelationRegistry;
use hexeract_bus::Message;
use hexeract_bus::Request;
use hexeract_bus::RequestError;
use hexeract_bus::RequestHandler;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::RabbitMqTransport;
use hexeract_bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract_bus_rabbitmq::connect_request_client;
use hexeract_bus_rabbitmq::declare_reply_inbox_for_test;
use hexeract_bus_rabbitmq::run_reply_inbox_for_test;
use hexeract_core::HandlerContext;
use lapin::options::BasicGetOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod harness; // reuse the crate's testcontainers helper

#[derive(Debug, Serialize, Deserialize)]
struct Ping {
    seq: u64,
}
impl Message for Ping {
    const MESSAGE_TYPE: &'static str = "tests.ping";
}
impl Request for Ping {
    type Reply = Pong;
}

#[derive(Debug, Serialize, Deserialize)]
struct Pong {
    seq: u64,
}
impl Message for Pong {
    const MESSAGE_TYPE: &'static str = "tests.pong";
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn reply_published_to_inbox_is_resolved() {
    let broker = harness::start_rabbitmq().await;
    let connection =
        RabbitMqConnection::connect_with_retry(broker.uri(), 5, Duration::from_millis(200))
            .await
            .unwrap();
    let consumer_channel = connection.create_channel().await.unwrap();
    let inbox = declare_reply_inbox_for_test(&consumer_channel)
        .await
        .unwrap();

    let registry = Arc::new(CorrelationRegistry::new());
    let cancel = CancellationToken::new();
    let handle = {
        let registry = Arc::clone(&registry);
        let inbox = inbox.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = run_reply_inbox_for_test(consumer_channel, inbox, registry, cancel).await;
        })
    };

    let mut pending = registry.register();
    let correlation_id = *pending.correlation_id().as_uuid();

    // publish a reply envelope straight to the inbox via a fresh channel
    let publish_channel = connection.create_channel().await.unwrap();
    let mut reply = BusEnvelope::new(correlation_id, &Pong { seq: 42 }).unwrap();
    reply
        .headers
        .insert("x-hexeract-reply-status".to_owned(), "ok".to_owned());
    harness::publish_to_default_exchange(&publish_channel, &inbox, &reply).await;

    let received = tokio::time::timeout(Duration::from_secs(5), pending.wait())
        .await
        .expect("no timeout")
        .expect("resolved");
    let pong: Pong = received.decode().unwrap();
    assert_eq!(pong.seq, 42);

    cancel.cancel();
    let _ = handle.await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn connection_drop_fails_in_flight_fast() {
    let broker = harness::start_rabbitmq().await;
    let cancel = CancellationToken::new();
    let client = hexeract_bus_rabbitmq::connect_request_client(
        broker.uri(),
        Duration::from_secs(30),
        cancel.clone(),
    )
    .await
    .unwrap();

    // no responder is running; kill the broker while a request is in flight
    let request = client.request(&Ping { seq: 1 });
    tokio::pin!(request);
    tokio::select! {
        _ = &mut request => panic!("should still be pending before the drop"),
        () = tokio::time::sleep(Duration::from_millis(200)) => {}
    }
    broker.stop().await; // testcontainers stop
    let outcome = tokio::time::timeout(Duration::from_secs(10), &mut request)
        .await
        .expect("must resolve well under the 30s timeout");
    assert!(matches!(
        outcome,
        Err(hexeract_bus::RequestError::Transport(_))
    ));
    cancel.cancel();
}

/// Responder that echoes the request's `seq` back in the reply.
struct Echo;
impl RequestHandler<Ping> for Echo {
    type Error = BusError;
    async fn handle(&self, request: Ping, _ctx: &HandlerContext) -> Result<Pong, BusError> {
        Ok(Pong { seq: request.seq })
    }
}

/// Responder that always fails, used to prove a remote error reaches the
/// caller as [`RequestError::Remote`] well before the request timeout.
struct Failing;
impl RequestHandler<Ping> for Failing {
    type Error = BusError;
    async fn handle(&self, _request: Ping, _ctx: &HandlerContext) -> Result<Pong, BusError> {
        Err(BusError::Internal("deliberate handler failure".to_owned()))
    }
}

/// Declare a durable, non-exclusive queue for a test's responder to
/// consume from. RabbitMQ 4 rejects transient non-exclusive queues, and
/// `RabbitMqWorker::run` never declares its own consume queue (only the
/// retry and dead-letter queues), so the queue must exist before the
/// worker starts consuming from it.
async fn declare_ping_queue(uri: &str, name: &str) {
    let connection = RabbitMqConnection::connect(uri)
        .await
        .expect("setup connection must open");
    let channel = connection
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
        .expect("queue declare must succeed");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn end_to_end_request_reply_round_trip() {
    let broker = harness::start_rabbitmq().await;
    let cancel = CancellationToken::new();

    declare_ping_queue(broker.uri(), "tests.ping").await;

    // responder worker
    let responder_transport = Arc::new(RabbitMqTransport::new(broker.uri()).await.unwrap());
    let worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(broker.uri(), 5, Duration::from_millis(200))
            .await
            .unwrap(),
    )
    .queue("tests.ping")
    .register_request_handler::<Ping, _>(Echo, Arc::clone(&responder_transport))
    .build()
    .unwrap();
    let worker_cancel = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    // client
    let client = connect_request_client(broker.uri(), Duration::from_secs(10), cancel.clone())
        .await
        .unwrap();

    let pong = client.request(&Ping { seq: 21 }).await.unwrap();
    assert_eq!(pong.seq, 21);

    cancel.cancel();
    let _ = worker_handle.await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn remote_error_reaches_caller_fast() {
    let broker = harness::start_rabbitmq().await;
    let cancel = CancellationToken::new();

    declare_ping_queue(broker.uri(), "tests.ping").await;

    let responder_transport = Arc::new(RabbitMqTransport::new(broker.uri()).await.unwrap());
    let worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(broker.uri(), 5, Duration::from_millis(200))
            .await
            .unwrap(),
    )
    .queue("tests.ping")
    .register_request_handler::<Ping, _>(Failing, Arc::clone(&responder_transport))
    .build()
    .unwrap();
    let worker_cancel = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    // Generous timeout: the assertion below is on elapsed wall time, not on
    // the timeout itself, so this only bounds the test's worst case.
    let client = connect_request_client(broker.uri(), Duration::from_secs(30), cancel.clone())
        .await
        .unwrap();

    let started = Instant::now();
    let err = client
        .request(&Ping { seq: 1 })
        .await
        .expect_err("a failing handler must surface as an error");
    let elapsed = started.elapsed();

    match err {
        RequestError::Remote {
            error_type,
            message,
        } => {
            assert_eq!(error_type, "Internal");
            assert!(message.contains("deliberate handler failure"));
        }
        other => panic!("expected RequestError::Remote, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(5),
        "a remote error must reach the caller fast, well under the 30s timeout, took {elapsed:?}"
    );

    cancel.cancel();
    let _ = worker_handle.await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn request_without_reply_to_is_non_fatal() {
    let broker = harness::start_rabbitmq().await;
    let cancel = CancellationToken::new();

    declare_ping_queue(broker.uri(), "tests.ping").await;

    let responder_transport = Arc::new(RabbitMqTransport::new(broker.uri()).await.unwrap());
    let worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(broker.uri(), 5, Duration::from_millis(200))
            .await
            .unwrap(),
    )
    .queue("tests.ping")
    .register_request_handler::<Ping, _>(Echo, Arc::clone(&responder_transport))
    .build()
    .unwrap();
    let worker_cancel = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    // Publish a request with no reply_to, straight to the queue: this
    // bypasses the request client, which always stamps a reply_to inbox.
    let publish_connection = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publish_channel = publish_connection.create_channel().await.unwrap();
    let envelope = BusEnvelope::new(Uuid::now_v7(), &Ping { seq: 99 }).unwrap();
    harness::publish_to_default_exchange(&publish_channel, "tests.ping", &envelope).await;

    // give the worker time to consume and settle the delivery
    tokio::time::sleep(Duration::from_millis(500)).await;

    // the delivery must be fully settled (acked), never left unsettled or
    // redelivered, proving the missing reply_to did not crash the worker
    let probe_channel = publish_connection.create_channel().await.unwrap();
    let redelivered = probe_channel
        .basic_get("tests.ping".into(), BasicGetOptions::default())
        .await
        .unwrap();
    assert!(
        redelivered.is_none(),
        "a request without reply_to must be acked normally, not left unsettled"
    );

    // the worker must still be alive and answer a normal request afterward
    let client = connect_request_client(broker.uri(), Duration::from_secs(10), cancel.clone())
        .await
        .unwrap();
    let pong = client.request(&Ping { seq: 7 }).await.unwrap();
    assert_eq!(pong.seq, 7);

    cancel.cancel();
    let _ = worker_handle.await;
}
