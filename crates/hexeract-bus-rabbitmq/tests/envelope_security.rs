//! End-to-end envelope security against a real RabbitMQ broker (issue #444
//! lot B, task 6).
//!
//! Every other test in this crate's envelope security surface either signs
//! and verifies in memory, or forges AMQP properties by hand without a
//! broker. Neither can observe two properties that only exist once an
//! envelope has actually crossed the wire:
//!
//! - **Precision symmetry.** The canonical representation signs
//!   `published_at` to the second, because the AMQP `timestamp` property
//!   carries only whole seconds. A signature computed over an envelope whose
//!   `published_at` still carries sub-second precision must still verify once
//!   the broker has truncated it away on the way back.
//! - **Binding on the observed destination.** The canonical representation
//!   binds to the routing key a delivery actually arrived on, never to the
//!   `x-hexeract-destination` header the envelope announces. A worker that
//!   read that header back instead would still pass every unit test in this
//!   workspace while silently losing the one protection that stops a
//!   rerouted message from replaying as legitimate; see
//!   [`an_envelope_published_on_another_routing_key_is_rejected`].
//!
//! Run with `cargo test -p hexeract-bus-rabbitmq --test envelope_security --
//! --ignored` on a host with Docker available.

#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use hexeract_bus::Audience;
use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::EnvelopeSecurityConfig;
use hexeract_bus::EnvelopeSigner;
use hexeract_bus::Handler;
use hexeract_bus::Issuer;
use hexeract_bus::KeyId;
use hexeract_bus::KeySourceError;
use hexeract_bus::Message;
use hexeract_bus::ReplyInboxState;
use hexeract_bus::Request;
use hexeract_bus::RequestClient;
use hexeract_bus::RequestClientSupervisor;
use hexeract_bus::RequestContext;
use hexeract_bus::RequestHandler;
use hexeract_bus::RequestRegistry;
use hexeract_bus::SecurityHeaders;
use hexeract_bus::SigningContext;
use hexeract_bus::SigningKeyHandle;
use hexeract_bus::SigningKeySource;
use hexeract_bus::StaticKeySource;
use hexeract_bus::Transport;
use hexeract_bus::VerificationKey;
use hexeract_bus::VerificationKeySource;
use hexeract_bus::VerificationPolicy;
use hexeract_bus_rabbitmq::InboundEnvelopeSecurity;
use hexeract_bus_rabbitmq::OutboundEnvelopeSecurity;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::RabbitMqRequestClientConfigBuilder;
use hexeract_bus_rabbitmq::RabbitMqTransport;
use hexeract_bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract_bus_rabbitmq::connect_request_client_with_config;
use hexeract_bus_rabbitmq::declare_reply_inbox_for_test;
use hexeract_bus_rabbitmq::run_reply_inbox_for_test;
use hexeract_core::HandlerContext;
use hexeract_core::PublisherAuthentication;
use lapin::BasicProperties;
use lapin::Channel;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::options::BasicGetOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod harness; // reuse the crate's testcontainers helper

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct OrderPlaced {
    order_id: Uuid,
}

impl Message for OrderPlaced {
    const MESSAGE_TYPE: &'static str = "envelope-security.orders.placed";
}

/// The JSON body every forged or legitimate fixture in this file carries.
/// Its content is never covered by anything test-specific: what varies
/// between scenarios is the signature, the AMQP properties around it, and
/// the routing key it is published on, never this payload.
const FIXTURE_PAYLOAD: &[u8] = b"{\"order_id\":\"00000000-0000-0000-0000-000000000001\"}";

/// Handler that counts how many envelopes actually reached it, and records
/// the issuer of whichever signature the transport actually verified.
///
/// Every negative scenario in this file needs a way to observe that a
/// forged delivery never reached a typed handler at all; the counter is
/// that observation point, shared by every worker built below. The recorded
/// issuer serves a different purpose, exercised only by the tests further
/// down that prove a handler learns the publisher identity a real signature
/// established, never a value the test itself wrote on both sides.
#[derive(Debug, Default)]
struct RecordingHandler {
    seen: Arc<AtomicUsize>,
    observed_issuer: Arc<Mutex<Option<String>>>,
}

impl Handler<OrderPlaced> for RecordingHandler {
    type Error = BusError;

    async fn handle(&self, _message: OrderPlaced, ctx: &HandlerContext) -> Result<(), Self::Error> {
        if let PublisherAuthentication::Authenticated(identity) = &ctx.authentication {
            *self.observed_issuer.lock().expect("not poisoned") =
                Some(identity.issuer().to_owned());
        }
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Handler that records the exact [`PublisherAuthentication`] variant a
/// delivery carried, whichever one it turns out to be.
///
/// Distinct from [`RecordingHandler`], which only cares about the issuer
/// behind [`PublisherAuthentication::Authenticated`]: the derogation test
/// below must also tell [`PublisherAuthentication::WaivedUnsigned`] apart
/// from [`PublisherAuthentication::NotEnforced`], a distinction no
/// issuer-only observation could make.
#[derive(Debug, Default)]
struct AuthenticationRecordingHandler {
    seen: Arc<AtomicUsize>,
    observed_authentication: Arc<Mutex<Option<PublisherAuthentication>>>,
}

impl Handler<OrderPlaced> for AuthenticationRecordingHandler {
    type Error = BusError;

    async fn handle(&self, _message: OrderPlaced, ctx: &HandlerContext) -> Result<(), Self::Error> {
        *self.observed_authentication.lock().expect("not poisoned") =
            Some(ctx.authentication.clone());
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// -------------------------------------------------- envelope security fixtures

fn issuer() -> Issuer {
    Issuer::new("billing-service").expect("valid issuer")
}

fn audience() -> Audience {
    Audience::new("ledger-service").expect("valid audience")
}

fn key_id() -> KeyId {
    KeyId::new("2026-09").expect("valid key id")
}

fn signing_key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

/// Outbound security for the one scenario ([`a_signed_envelope_survives_a_real_broker_round_trip`])
/// that publishes through the real [`RabbitMqTransport`] signing path rather
/// than through a hand-forged fixture.
fn outbound_security() -> Arc<OutboundEnvelopeSecurity> {
    let keys: Arc<dyn SigningKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_signing_key(key_id(), SigningKeyHandle::from(signing_key()))
            .build(),
    );
    Arc::new(OutboundEnvelopeSecurity::new(issuer(), audience(), keys))
}

/// Worker-side verification, requiring a valid signature from [`issuer`] for
/// [`audience`], trusting the same key [`signing_key`] signs with.
fn inbound_security() -> Arc<InboundEnvelopeSecurity> {
    let keys: Arc<dyn VerificationKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_verification_key(
                issuer(),
                key_id(),
                VerificationKey::from(signing_key().verifying_key()),
            )
            .build(),
    );
    let config = EnvelopeSecurityConfig::builder()
        .with_accepted_audience(audience())
        .build()
        .expect("valid configuration");
    Arc::new(InboundEnvelopeSecurity::new(keys, config))
}

/// Same trust as [`inbound_security`], but under the named opt-out: an
/// envelope carrying no signature at all is accepted; one carrying a broken
/// signature is not.
fn inbound_security_allowing_unsigned() -> Arc<InboundEnvelopeSecurity> {
    let keys: Arc<dyn VerificationKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_verification_key(
                issuer(),
                key_id(),
                VerificationKey::from(signing_key().verifying_key()),
            )
            .build(),
    );
    let config = EnvelopeSecurityConfig::builder()
        .with_policy(VerificationPolicy::AllowInsecureUnauthenticatedEnvelopes)
        .with_accepted_audience(audience())
        .build()
        .expect("valid configuration");
    Arc::new(InboundEnvelopeSecurity::new(keys, config))
}

/// A second issuer, distinct from [`issuer`], signing with its own key.
///
/// Exists so [`a_handler_learns_the_second_issuer_that_signed_a_message`] can
/// prove the observed issuer tracks whichever signature was actually
/// verified: an implementation that always reported [`issuer`]'s name would
/// still pass every other test in this file, since none of them publish
/// under any other issuer.
fn other_issuer() -> Issuer {
    Issuer::new("shipping-service").expect("valid issuer")
}

/// Signing key for [`other_issuer`], distinct from [`signing_key`].
fn other_signing_key() -> SigningKey {
    SigningKey::from_bytes(&[9; 32])
}

/// Outbound security signing as [`other_issuer`], for the same [`audience`]
/// as [`outbound_security`].
fn other_outbound_security() -> Arc<OutboundEnvelopeSecurity> {
    let keys: Arc<dyn SigningKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_signing_key(key_id(), SigningKeyHandle::from(other_signing_key()))
            .build(),
    );
    Arc::new(OutboundEnvelopeSecurity::new(
        other_issuer(),
        audience(),
        keys,
    ))
}

/// Worker-side verification trusting both [`issuer`] and [`other_issuer`].
///
/// A dedicated fixture rather than widening [`inbound_security`] itself:
/// seven other tests in this file build a worker against [`inbound_security`],
/// and growing the trust of that shared fixture would silently change what
/// every one of them exercises.
fn inbound_security_trusting_both_issuers() -> Arc<InboundEnvelopeSecurity> {
    let keys: Arc<dyn VerificationKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_verification_key(
                issuer(),
                key_id(),
                VerificationKey::from(signing_key().verifying_key()),
            )
            .with_verification_key(
                other_issuer(),
                key_id(),
                VerificationKey::from(other_signing_key().verifying_key()),
            )
            .build(),
    );
    let config = EnvelopeSecurityConfig::builder()
        .with_accepted_audience(audience())
        .build()
        .expect("valid configuration");
    Arc::new(InboundEnvelopeSecurity::new(keys, config))
}

/// A [`VerificationKeySource`] that never resolves any key, standing in for a
/// consumer whose local key cache has not yet learned a signer's key. Counts
/// how many times [`VerificationKeySource::refresh`] is actually called, so
/// [`an_unknown_key_triggers_exactly_one_refresh_for_twenty_envelopes`] can
/// observe the rate limit from the transport side rather than re-testing the
/// counter itself, already covered at the unit level in `verifier.rs`.
#[derive(Debug, Default)]
struct AlwaysUnknownKeySource {
    refreshes: Arc<AtomicUsize>,
}

#[async_trait]
impl VerificationKeySource for AlwaysUnknownKeySource {
    async fn verification_key(
        &self,
        _issuer: &Issuer,
        _key_id: &KeyId,
    ) -> Result<VerificationKey, KeySourceError> {
        Err(KeySourceError::UnknownKey)
    }

    async fn refresh(&self) -> Result<(), KeySourceError> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// -------------------------------------------------- forged fixture builders

/// An unsigned envelope carrying [`FIXTURE_PAYLOAD`], ready to be signed by
/// [`sign_as`] or published unsigned as-is.
fn fixture_envelope(
    message_id: Uuid,
    correlation_id: Uuid,
    published_at: SystemTime,
) -> BusEnvelope {
    BusEnvelope::restore_from_transport(
        message_id,
        OrderPlaced::MESSAGE_TYPE.to_owned(),
        FIXTURE_PAYLOAD.to_vec(),
        correlation_id,
        None,
        HashMap::new(),
        HashMap::new(),
        published_at,
    )
}

/// Sign `envelope` as [`issuer`], for `audience`, as if it were about to be
/// published to `destination`.
///
/// `destination` need not be the routing key the envelope is actually
/// published on: keeping the two independent is what lets
/// [`an_envelope_published_on_another_routing_key_is_rejected`] build an
/// envelope signed for one destination and delivered on another.
fn sign_as(envelope: &BusEnvelope, destination: &str, audience: &Audience) -> SecurityHeaders {
    let keys: Arc<dyn SigningKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_signing_key(key_id(), SigningKeyHandle::from(signing_key()))
            .build(),
    );
    EnvelopeSigner::new(issuer(), keys)
        .sign(
            envelope,
            &SigningContext {
                destination,
                audience,
            },
        )
        .expect("signing must succeed")
}

/// AMQP properties for a delivery carrying `security_headers` (or none, for
/// an unsigned fixture), matching exactly what
/// `hexeract_bus_rabbitmq::worker::delivery_to_envelope` reconstructs from:
/// `type`, `message_id`, `correlation_id`, `timestamp` and the header table.
fn properties_for(
    message_id: Uuid,
    correlation_id: Uuid,
    published_at: SystemTime,
    security_headers: Option<&SecurityHeaders>,
) -> BasicProperties {
    let mut fields = FieldTable::default();
    if let Some(headers) = security_headers {
        for (name, value) in headers {
            fields.insert(name.into(), AMQPValue::LongString(value.into()));
        }
    }
    let published_at_secs = published_at
        .duration_since(UNIX_EPOCH)
        .expect("after epoch")
        .as_secs();
    BasicProperties::default()
        .with_type(OrderPlaced::MESSAGE_TYPE.into())
        .with_message_id(message_id.to_string().into())
        .with_correlation_id(correlation_id.to_string().into())
        .with_timestamp(published_at_secs)
        .with_headers(fields)
}

/// Sign a fresh fixture for `signed_destination` and publish it on
/// `routing_key`, carrying [`FIXTURE_PAYLOAD`] untampered.
///
/// The two destinations coincide for a legitimate publish and differ only in
/// [`an_envelope_published_on_another_routing_key_is_rejected`].
async fn publish_signed(
    channel: &Channel,
    signed_destination: &str,
    routing_key: &str,
    audience: &Audience,
) {
    let message_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let published_at = SystemTime::now();
    let envelope = fixture_envelope(message_id, correlation_id, published_at);
    let headers = sign_as(&envelope, signed_destination, audience);
    let properties = properties_for(message_id, correlation_id, published_at, Some(&headers));
    harness::publish_with_properties(channel, routing_key, properties, FIXTURE_PAYLOAD).await;
}

// -------------------------------------------------- broker-side helpers

/// Declare a transient, non-exclusive queue for a worker under test to
/// consume from. `RabbitMqWorker::run` never declares its own consume
/// queue, only the retry and dead-letter queues, so the queue must exist
/// before the worker starts.
async fn declare_temporary_queue(uri: &str, name: &str) {
    let connection = Connection::connect(uri, ConnectionProperties::default())
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

/// Poll `counter` until it reaches `target`, giving up silently after
/// `attempts` polls so the caller's own assertion reports the failure.
async fn wait_until_at_least(counter: &AtomicUsize, target: usize, attempts: usize) {
    for _ in 0..attempts {
        if counter.load(Ordering::SeqCst) >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Assert `queue` holds no message: a security rejection must never leave a
/// delivery sitting for redelivery.
async fn assert_queue_empty(uri: &str, queue: &str) {
    let probe = Connection::connect(uri, ConnectionProperties::default())
        .await
        .expect("probe connection must open");
    let channel = probe
        .create_channel()
        .await
        .expect("probe channel must open");
    let remaining = channel
        .basic_get(queue.into(), BasicGetOptions::default())
        .await
        .expect("basic_get must succeed");
    assert!(
        remaining.is_none(),
        "a security rejection must never leave the delivery queued for redelivery"
    );
}

/// Poll `dead_letter_queue` until one message is parked there.
async fn wait_for_dead_letter(
    uri: &str,
    dead_letter_queue: &str,
) -> Option<lapin::message::BasicGetMessage> {
    let probe = Connection::connect(uri, ConnectionProperties::default())
        .await
        .expect("probe connection must open");
    for _ in 0..80 {
        let channel = probe
            .create_channel()
            .await
            .expect("probe channel must open");
        if let Ok(candidate) = channel
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

/// Drain up to `expected` messages already parked on `dead_letter_queue`,
/// polling until they all arrive or `attempts` polls have passed.
///
/// A fresh channel per attempt, and a failed `basic_get` treated as "not yet"
/// rather than as an error, for one reason: the worker declares its
/// dead-letter queue as it starts, so a probe that reaches the broker first
/// gets `NOT_FOUND`, and AMQP closes the channel that asked. Holding one
/// channel across the whole poll would therefore turn a startup race into a
/// permanent failure, and treating the first miss as fatal would report a
/// timing artefact as a rejection that never happened. Mirrors
/// [`wait_for_dead_letter`], which polls the same queue under the same race.
async fn count_dead_letters(
    uri: &str,
    dead_letter_queue: &str,
    expected: usize,
    attempts: usize,
) -> usize {
    let probe = Connection::connect(uri, ConnectionProperties::default())
        .await
        .expect("probe connection must open");
    let mut drained = 0usize;
    for _ in 0..attempts {
        let channel = probe
            .create_channel()
            .await
            .expect("probe channel must open");
        // Anything but a message ends this attempt: either the queue is empty
        // for now, or it does not exist yet and the refusal closed this
        // channel. Both cases are answered the same way, by opening a fresh
        // channel on the next attempt.
        while drained < expected {
            match channel
                .basic_get(dead_letter_queue.into(), BasicGetOptions::default())
                .await
            {
                Ok(Some(_)) => drained += 1,
                _ => break,
            }
        }
        if drained >= expected {
            return drained;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drained
}

/// The probe helper above must survive a queue that does not exist yet, which
/// is the state every dead-letter queue passes through while its worker is
/// still starting. Held by a test of its own because the failure it prevents
/// is a race: it surfaced once in twenty runs, on a merge commit, after three
/// green runs of the same code, and it turned a timing artefact into a red
/// pipeline on `main`. A helper that only works when it wins a race is a
/// helper that reports other people's tests as broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn the_dead_letter_probe_survives_a_queue_that_does_not_exist_yet() {
    let broker = harness::start_rabbitmq().await;

    let drained = count_dead_letters(broker.uri(), "no-such-queue-anywhere", 1, 2).await;

    assert_eq!(
        drained, 0,
        "probing a queue that has not been declared yet must report nothing parked, \
         never fail the test that is only waiting for a worker to finish starting"
    );
}

// -------------------------------------------------- 1. precision symmetry

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_signed_envelope_survives_a_real_broker_round_trip() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.round-trip";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri())
        .await
        .expect("transport must connect")
        .with_outbound_envelope_security(outbound_security());

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after epoch")
        .as_secs();
    let published_at =
        UNIX_EPOCH + Duration::from_secs(now_secs) + Duration::from_nanos(123_456_789);
    assert_ne!(
        published_at
            .duration_since(UNIX_EPOCH)
            .expect("after epoch")
            .subsec_nanos(),
        0,
        "the fixture must carry sub-second precision, or the round trip below proves nothing"
    );
    let envelope = fixture_envelope(Uuid::now_v7(), Uuid::now_v7(), published_at);

    transport
        .publish_envelope(queue_name, &envelope)
        .await
        .expect("publish must succeed");

    let seen = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    wait_until_at_least(&seen, 1, 60).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "a signature computed over sub-second precision must still verify once the AMQP \
         `timestamp` property has truncated `published_at` to the second on the way back"
    );

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 2. payload integrity

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_tampered_payload_never_reaches_the_handler() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.tampered-payload";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let publisher = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publisher_channel = publisher.create_channel().await.unwrap();

    // Positive control: signed and published untampered, in the exact same
    // conditions as the forged delivery below.
    publish_signed(&publisher_channel, queue_name, queue_name, &audience()).await;

    // Same signature, a different payload on the wire. The signature covers
    // a digest of the payload, so one changed byte after signing must
    // invalidate it.
    let message_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let published_at = SystemTime::now();
    let envelope = fixture_envelope(message_id, correlation_id, published_at);
    let headers = sign_as(&envelope, queue_name, &audience());
    let properties = properties_for(message_id, correlation_id, published_at, Some(&headers));
    let tampered_payload = b"{\"order_id\":\"11111111-1111-1111-1111-111111111111\"}".to_vec();
    harness::publish_with_properties(
        &publisher_channel,
        queue_name,
        properties,
        &tampered_payload,
    )
    .await;

    wait_until_at_least(&seen, 1, 60).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "only the untampered envelope may reach the handler"
    );
    assert_queue_empty(broker.uri(), queue_name).await;

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 3. audience binding

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn an_envelope_signed_for_another_audience_is_rejected() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.wrong-audience";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let publisher = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publisher_channel = publisher.create_channel().await.unwrap();

    publish_signed(&publisher_channel, queue_name, queue_name, &audience()).await;

    let stranger = Audience::new("attacker-service").expect("valid audience");
    publish_signed(&publisher_channel, queue_name, queue_name, &stranger).await;

    wait_until_at_least(&seen, 1, 60).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "an envelope signed for an audience this worker does not accept must never reach the \
         handler, even though its signature is otherwise perfectly valid"
    );
    assert_queue_empty(broker.uri(), queue_name).await;

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 4. destination binding

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn an_envelope_published_on_another_routing_key_is_rejected() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.actual-destination";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let publisher = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publisher_channel = publisher.create_channel().await.unwrap();

    // Signed for, and published on, the queue the worker actually consumes:
    // must be accepted.
    publish_signed(&publisher_channel, queue_name, queue_name, &audience()).await;

    // Signed for an entirely different destination, but published on this
    // worker's own routing key. The canonical representation must bind to
    // the destination observed at delivery, never to the value the
    // `x-hexeract-destination` header announces: reading that header back
    // here instead would make this envelope verify as if it had been
    // legitimately sent to this queue.
    publish_signed(
        &publisher_channel,
        "envelope-security.decoy-destination",
        queue_name,
        &audience(),
    )
    .await;

    wait_until_at_least(&seen, 1, 60).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "an envelope signed for another destination but delivered here must never reach the \
         handler, or a message could be rerouted onto a queue it was never signed for"
    );
    assert_queue_empty(broker.uri(), queue_name).await;

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 5 & 6. unsigned policy

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn an_unsigned_envelope_is_rejected_when_the_policy_requires_one() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.unsigned-required";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let publisher = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publisher_channel = publisher.create_channel().await.unwrap();

    publish_signed(&publisher_channel, queue_name, queue_name, &audience()).await;

    let message_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let published_at = SystemTime::now();
    let properties = properties_for(message_id, correlation_id, published_at, None);
    harness::publish_with_properties(&publisher_channel, queue_name, properties, FIXTURE_PAYLOAD)
        .await;

    wait_until_at_least(&seen, 1, 60).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "an unsigned envelope must never reach the handler under a Required policy"
    );
    assert_queue_empty(broker.uri(), queue_name).await;

    cancel.cancel();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn an_unsigned_envelope_is_accepted_under_the_named_derogation() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.unsigned-allowed";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security_allowing_unsigned())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let publisher = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publisher_channel = publisher.create_channel().await.unwrap();

    // No signature at all: the derogation accepts the absence of one.
    let message_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let published_at = SystemTime::now();
    let properties = properties_for(message_id, correlation_id, published_at, None);
    harness::publish_with_properties(&publisher_channel, queue_name, properties, FIXTURE_PAYLOAD)
        .await;

    wait_until_at_least(&seen, 1, 60).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "an unsigned envelope must be accepted under the named derogation"
    );

    // A signature that IS present but broken. The derogation covers the
    // absence of a signature, never a present-but-invalid one: a broken
    // signature must still be rejected exactly as it would be under the
    // Required policy.
    let tampered_message_id = Uuid::now_v7();
    let tampered_correlation_id = Uuid::now_v7();
    let tampered_published_at = SystemTime::now();
    let tampered_envelope = fixture_envelope(
        tampered_message_id,
        tampered_correlation_id,
        tampered_published_at,
    );
    let tampered_headers = sign_as(&tampered_envelope, queue_name, &audience());
    let tampered_properties = properties_for(
        tampered_message_id,
        tampered_correlation_id,
        tampered_published_at,
        Some(&tampered_headers),
    );
    let tampered_payload = b"{\"order_id\":\"22222222-2222-2222-2222-222222222222\"}".to_vec();
    harness::publish_with_properties(
        &publisher_channel,
        queue_name,
        tampered_properties,
        &tampered_payload,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "a present but broken signature must still be rejected, even under the derogation that \
         allows an absent one"
    );
    assert_queue_empty(broker.uri(), queue_name).await;

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 7. refresh rate limit

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn an_unknown_key_triggers_exactly_one_refresh_for_twenty_envelopes() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.unknown-key";
    let dead_letter_queue = "envelope-security.unknown-key.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri())
        .await
        .expect("transport must connect")
        .with_outbound_envelope_security(outbound_security());
    for _ in 0..20 {
        let envelope = fixture_envelope(Uuid::now_v7(), Uuid::now_v7(), SystemTime::now());
        transport
            .publish_envelope(queue_name, &envelope)
            .await
            .expect("publish must succeed");
    }

    let refreshes = Arc::new(AtomicUsize::new(0));
    let keys: Arc<dyn VerificationKeySource> = Arc::new(AlwaysUnknownKeySource {
        refreshes: Arc::clone(&refreshes),
    });
    let config = EnvelopeSecurityConfig::builder()
        .with_accepted_audience(audience())
        .build()
        .expect("valid configuration");
    let security = Arc::new(InboundEnvelopeSecurity::new(keys, config));

    let attempts = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .dead_letter_routing_key(dead_letter_queue)
            .envelope_security(security)
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&attempts),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let parked = count_dead_letters(broker.uri(), dead_letter_queue, 20, 100).await;
    assert_eq!(
        parked, 20,
        "every envelope whose key never resolves must be dead-lettered, none dropped or stuck"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "a delivery whose key never resolves must never reach the handler"
    );
    assert_eq!(
        refreshes.load(Ordering::SeqCst),
        1,
        "twenty consecutive unresolvable-key envelopes within the refresh interval must trigger \
         exactly one refresh of the key source, not one per envelope"
    );

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 8. quarantine shape

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_rejected_envelope_lands_in_the_dead_letter_queue_without_its_headers() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.quarantine";
    let dead_letter_queue = "envelope-security.quarantine.parked";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .dead_letter_routing_key(dead_letter_queue)
            .envelope_security(inbound_security())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::new(Mutex::new(None)),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let publisher = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publisher_channel = publisher.create_channel().await.unwrap();

    // Positive control: a legitimate envelope on the same worker must still
    // reach the handler, proving the quarantine below is specific to the
    // forged delivery, not some unrelated failure mode.
    publish_signed(&publisher_channel, queue_name, queue_name, &audience()).await;
    wait_until_at_least(&seen, 1, 60).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "the legitimate envelope must reach the handler"
    );

    let forged_message_id = Uuid::now_v7();
    harness::publish_with_properties(
        &publisher_channel,
        queue_name,
        properties_for(
            forged_message_id,
            Uuid::now_v7(),
            SystemTime::now(),
            Some(&sign_as(
                &fixture_envelope(forged_message_id, Uuid::now_v7(), SystemTime::now()),
                "envelope-security.quarantine.decoy",
                &audience(),
            )),
        ),
        FIXTURE_PAYLOAD,
    )
    .await;

    let parked = wait_for_dead_letter(broker.uri(), dead_letter_queue)
        .await
        .expect("a rejected envelope must be routed to the dead-letter queue");

    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "the forged envelope must never reach the handler"
    );

    let quarantined_headers = parked
        .delivery
        .properties
        .headers()
        .as_ref()
        .expect("the quarantine copy must carry an explicit empty table");
    assert!(
        quarantined_headers.inner().is_empty(),
        "neither an application header nor the unauthenticated security headers may be \
         republished"
    );
    assert_eq!(
        parked
            .delivery
            .properties
            .message_id()
            .as_ref()
            .map(ShortString::as_str),
        Some(forged_message_id.to_string()).as_deref(),
        "the quarantine copy must stay diagnosable"
    );

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 9. handler-observed publisher identity

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_handler_learns_which_issuer_signed_the_message_it_received() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.publisher-identity";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri())
        .await
        .expect("transport must connect")
        .with_outbound_envelope_security(outbound_security());

    let envelope = fixture_envelope(Uuid::now_v7(), Uuid::now_v7(), SystemTime::now());
    transport
        .publish_envelope(queue_name, &envelope)
        .await
        .expect("publish must succeed");

    let seen = Arc::new(AtomicUsize::new(0));
    let observed_issuer = Arc::new(Mutex::new(None));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::clone(&observed_issuer),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    wait_until_at_least(&seen, 1, 60).await;
    assert_eq!(
        observed_issuer.lock().expect("not poisoned").as_deref(),
        Some("billing-service"),
        "the handler must learn the issuer the signature was actually checked against, and \
         nothing in this test writes that value on the handler side"
    );

    cancel.cancel();
    let _ = handle.await;
}

/// Symmetric to [`a_handler_learns_which_issuer_signed_the_message_it_received`]:
/// without a second issuer signing with its own key, an implementation that
/// always reported `billing-service` regardless of who actually signed would
/// still pass the test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_handler_learns_the_second_issuer_that_signed_a_message() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.publisher-identity.second-issuer";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let transport = RabbitMqTransport::new(broker.uri())
        .await
        .expect("transport must connect")
        .with_outbound_envelope_security(other_outbound_security());

    let envelope = fixture_envelope(Uuid::now_v7(), Uuid::now_v7(), SystemTime::now());
    transport
        .publish_envelope(queue_name, &envelope)
        .await
        .expect("publish must succeed");

    let seen = Arc::new(AtomicUsize::new(0));
    let observed_issuer = Arc::new(Mutex::new(None));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security_trusting_both_issuers())
            .register_handler::<OrderPlaced, _>(RecordingHandler {
                seen: Arc::clone(&seen),
                observed_issuer: Arc::clone(&observed_issuer),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    wait_until_at_least(&seen, 1, 60).await;
    assert_eq!(
        observed_issuer.lock().expect("not poisoned").as_deref(),
        Some("shipping-service"),
        "the handler must learn the issuer that actually signed this delivery, not the issuer \
         some other test in this file happens to sign with"
    );

    cancel.cancel();
    let _ = handle.await;
}

/// A worker under [`VerificationPolicy::AllowInsecureUnauthenticatedEnvelopes`]
/// receiving an unsigned envelope: the handler still runs, and must observe
/// [`PublisherAuthentication::WaivedUnsigned`], never
/// [`PublisherAuthentication::NotEnforced`]. The two look identical from a
/// handler that only checks "was this signed", yet mean opposite things
/// operationally: one is a deliberate, logged exception, the other is the
/// absence of any security configuration at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_waived_unsigned_envelope_is_reported_as_waived_not_unenforced() {
    let broker = harness::start_rabbitmq().await;
    let queue_name = "envelope-security.publisher-identity.waived";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let seen = Arc::new(AtomicUsize::new(0));
    let observed_authentication = Arc::new(Mutex::new(None));
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security_allowing_unsigned())
            .register_handler::<OrderPlaced, _>(AuthenticationRecordingHandler {
                seen: Arc::clone(&seen),
                observed_authentication: Arc::clone(&observed_authentication),
            })
            .build()
            .unwrap();
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let publisher = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let publisher_channel = publisher.create_channel().await.unwrap();

    // No signature at all: the derogation accepts the absence of one.
    let message_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let published_at = SystemTime::now();
    let properties = properties_for(message_id, correlation_id, published_at, None);
    harness::publish_with_properties(&publisher_channel, queue_name, properties, FIXTURE_PAYLOAD)
        .await;

    wait_until_at_least(&seen, 1, 60).await;
    assert_eq!(
        observed_authentication
            .lock()
            .expect("not poisoned")
            .as_ref(),
        Some(&PublisherAuthentication::WaivedUnsigned),
        "an unsigned envelope accepted under the named derogation must report the waiver, not \
         the absence of any security configuration"
    );

    cancel.cancel();
    let _ = handle.await;
}

// -------------------------------------------------- 10. authenticated RPC

#[derive(Debug, Serialize, Deserialize)]
struct Ping {
    seq: u64,
}

impl Message for Ping {
    const MESSAGE_TYPE: &'static str = "envelope-security.rpc.ping";
}

impl Request for Ping {
    type Reply = Pong;
}

#[derive(Debug, Serialize, Deserialize)]
struct Pong {
    seq: u64,
}

impl Message for Pong {
    const MESSAGE_TYPE: &'static str = "envelope-security.rpc.pong";
}

/// Responder that echoes the request's `seq` back in the reply, and records
/// the issuer of whichever signature the transport actually verified on the
/// inbound request.
struct Echo {
    observed_issuer: Arc<Mutex<Option<String>>>,
}

impl RequestHandler<Ping> for Echo {
    type Error = BusError;

    async fn handle(&self, request: Ping, ctx: &RequestContext<'_>) -> Result<Pong, BusError> {
        if let PublisherAuthentication::Authenticated(identity) = &ctx.handler.authentication {
            *self.observed_issuer.lock().expect("not poisoned") =
                Some(identity.issuer().to_owned());
        }
        Ok(Pong { seq: request.seq })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_signed_reply_survives_the_request_reply_path() {
    let broker = harness::start_rabbitmq().await;
    let cancel = CancellationToken::new();
    let queue_name = "envelope-security.rpc.ping";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let responder_issuer = Issuer::new("envelope-security-responder").expect("valid issuer");
    let caller_audience = Audience::new("envelope-security-caller").expect("valid audience");
    let responder_key_id = KeyId::new("2026-09").expect("valid key id");

    let responder_signing_keys: Arc<dyn SigningKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_signing_key(
                responder_key_id.clone(),
                SigningKeyHandle::from(SigningKey::from_bytes(&[21; 32])),
            )
            .build(),
    );
    let outbound = Arc::new(OutboundEnvelopeSecurity::new(
        responder_issuer.clone(),
        caller_audience.clone(),
        responder_signing_keys,
    ));
    let responder_transport = Arc::new(
        RabbitMqTransport::new(broker.uri())
            .await
            .expect("responder transport must connect")
            .with_outbound_envelope_security(outbound),
    );
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .register_request_handler::<Ping, _>(
                Echo {
                    observed_issuer: Arc::new(Mutex::new(None)),
                },
                Arc::clone(&responder_transport),
            )
            .build()
            .unwrap();
    let worker_cancel = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let caller_keys: Arc<dyn VerificationKeySource> = Arc::new(
        StaticKeySource::builder()
            .with_verification_key(
                responder_issuer,
                responder_key_id,
                VerificationKey::from(SigningKey::from_bytes(&[21; 32]).verifying_key()),
            )
            .build(),
    );
    let caller_config = EnvelopeSecurityConfig::builder()
        .with_accepted_audience(caller_audience)
        .build()
        .expect("valid configuration");
    let caller_security = Arc::new(InboundEnvelopeSecurity::new(caller_keys, caller_config));

    let client_config = RabbitMqRequestClientConfigBuilder::new()
        .envelope_security(caller_security)
        .build();
    let client = connect_request_client_with_config(
        broker.uri(),
        Duration::from_secs(10),
        cancel.clone(),
        client_config,
    )
    .await
    .expect("the request client must connect");

    let pong = client
        .request(Ping { seq: 77 })
        .await
        .expect("a correctly signed reply must verify and resolve the request");
    assert_eq!(pong.seq, 77);

    cancel.cancel();
    let _ = worker_handle.await;
}

/// The request/reply mirror of
/// [`a_handler_learns_which_issuer_signed_the_message_it_received`].
///
/// `RepliedHandler` builds its `RequestContext` by borrowing the very
/// `HandlerContext` the worker already verified the inbound request
/// against, rather than reconstructing one, so this path carries the
/// verified publisher identity without a single line of `hexeract-bus`
/// having changed for it. This test would fail if a future change ever
/// rebuilt that context instead of borrowing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_request_handler_learns_which_issuer_signed_the_request_it_received() {
    let broker = harness::start_rabbitmq().await;
    let cancel = CancellationToken::new();
    let queue_name = "envelope-security.rpc.ping";
    declare_temporary_queue(broker.uri(), queue_name).await;

    let observed_issuer = Arc::new(Mutex::new(None));
    let responder_transport = Arc::new(
        RabbitMqTransport::new(broker.uri())
            .await
            .expect("responder transport must connect"),
    );
    let worker =
        RabbitMqWorkerBuilder::new(RabbitMqConnection::connect(broker.uri()).await.unwrap())
            .queue(queue_name)
            .envelope_security(inbound_security())
            .register_request_handler::<Ping, _>(
                Echo {
                    observed_issuer: Arc::clone(&observed_issuer),
                },
                Arc::clone(&responder_transport),
            )
            .build()
            .unwrap();
    let worker_cancel = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let registry = Arc::new(RequestRegistry::default());
    let inbox_connection = RabbitMqConnection::connect(broker.uri()).await.unwrap();
    let inbox_channel = inbox_connection.create_channel().await.unwrap();
    let inbox = declare_reply_inbox_for_test(&inbox_channel).await.unwrap();
    let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Ready(inbox.clone())));
    let inbox_cancel = cancel.clone();
    let inbox_registry = Arc::clone(&registry);
    let inbox_name = inbox.clone();
    let inbox_handle = tokio::spawn(async move {
        let _ =
            run_reply_inbox_for_test(inbox_channel, inbox_name, inbox_registry, inbox_cancel).await;
    });

    let publisher_transport = Arc::new(
        RabbitMqTransport::new(broker.uri())
            .await
            .expect("publisher transport must connect")
            .with_outbound_envelope_security(outbound_security()),
    );
    let client = RequestClient::new(
        publisher_transport,
        registry,
        reply_inbox,
        Duration::from_secs(10),
        RequestClientSupervisor::detached(cancel.clone()),
    );

    let pong = client
        .request(Ping { seq: 77 })
        .await
        .expect("the signed request must reach the handler and its reply must resolve");
    assert_eq!(pong.seq, 77);

    assert_eq!(
        observed_issuer.lock().expect("not poisoned").as_deref(),
        Some("billing-service"),
        "the responder must learn the issuer whose signature it actually verified on the \
         inbound request, and nothing in this test writes that value on the responder side"
    );

    cancel.cancel();
    let _ = worker_handle.await;
    let _ = inbox_handle.await;
}
