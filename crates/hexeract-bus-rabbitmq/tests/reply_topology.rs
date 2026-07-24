//! Broker-level properties the reply destination policy depends on.
//!
//! Lot B's reply destination policy (require the `amq.gen-` prefix on a
//! reply queue name) is only sound if a client cannot itself declare a
//! queue under the reserved `amq.` prefix. This suite proves that premise,
//! and the companion vulnerability premise (an exclusive queue still
//! accepts publishes from a connection that does not own it), against a
//! real broker. It consumes nothing from lot B: it interrogates the
//! broker, not our code.
//!
//! Run with `cargo test -p hexeract-bus-rabbitmq --test reply_topology --
//! --ignored` on a host with Docker available.

#![cfg(test)]

use lapin::BasicProperties;
use lapin::Channel;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::options::BasicPublishOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;

mod harness; // reuse the crate's testcontainers helper

use harness::RunningBroker;

/// Start a fresh broker for a single test.
async fn start_broker() -> RunningBroker {
    harness::start_rabbitmq().await
}

/// Open a fresh AMQP connection and channel against `broker`.
///
/// Returns the [`Connection`] alongside the [`Channel`] so the caller can
/// keep the connection alive for as long as the channel is in use; an
/// exclusive queue lives and dies with its declaring connection, so tests
/// that need distinct connection identities must call this once per
/// identity rather than sharing one connection across channels.
async fn open_channel(broker: &RunningBroker) -> (Connection, Channel) {
    let connection = Connection::connect(broker.uri(), ConnectionProperties::default())
        .await
        .expect("connection must be established");
    let channel = connection
        .create_channel()
        .await
        .expect("channel must open");
    (connection, channel)
}

/// Declare a server-named, exclusive reply inbox on `channel`, exactly as
/// a real requester declares its reply destination, and return the
/// broker-generated `amq.gen-*` queue name.
async fn declare_reply_inbox(channel: &Channel) -> Result<String, lapin::Error> {
    let declare_ok = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;
    Ok(declare_ok.name().to_string())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_client_cannot_declare_a_queue_under_the_reserved_prefix() {
    let broker = start_broker().await;
    let (_connection, channel) = open_channel(&broker).await;

    let outcome = channel
        .queue_declare(
            "amq.gen-attacker".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await;

    assert!(
        outcome.is_err(),
        "the reply destination policy assumes the amq. prefix is reserved to the broker; \
         if this ever succeeds, ReplyDestination::parse is not a sufficient control"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_exclusive_inbox_can_still_be_published_to_by_a_third_party() {
    let broker = start_broker().await;
    let (_owner_connection, owner) = open_channel(&broker).await;
    let inbox = declare_reply_inbox(&owner).await.expect("inbox declared");

    let (_stranger_connection, stranger) = open_channel(&broker).await;
    let outcome = stranger
        .basic_publish(
            "".into(),
            inbox.as_str().into(),
            BasicPublishOptions::default(),
            b"forged",
            BasicProperties::default(),
        )
        .await;

    assert!(
        outcome.is_ok(),
        "exclusive constrains consumption, not publication: this is the premise of the \
         threat model, and the reason validation happens before slot consumption"
    );
}
