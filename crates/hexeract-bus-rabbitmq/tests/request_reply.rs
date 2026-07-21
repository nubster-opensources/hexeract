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

use hexeract_bus::BusEnvelope;
use hexeract_bus::CorrelationRegistry;
use hexeract_bus::Message;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::declare_reply_inbox_for_test;
use hexeract_bus_rabbitmq::run_reply_inbox_for_test;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

mod harness; // reuse the crate's testcontainers helper

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
