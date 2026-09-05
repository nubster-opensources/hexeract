//! Consumer for the exclusive, auto-delete reply inbox used by the
//! request-reply client path.
//!
//! [`declare_reply_inbox`] declares a server-named exclusive queue that
//! dies with the connection; [`run_reply_inbox`] consumes it with
//! `no_ack` and routes every delivery to a
//! [`hexeract_bus::RequestRegistry`] by request id, so a caller
//! waiting on a [`hexeract_bus::PendingReply`] is woken as soon as its
//! reply arrives.
//!
//! # Design
//!
//! The inbox consumer must run on a connection distinct from the
//! auto-recovering publisher connection: lapin's native auto-recovery
//! would keep the consumer stream alive across a broker drop and mask
//! the outage from the supervisor that owns this consumer's lifecycle.
//! See [`run_reply_inbox`] for the resulting contract on connection loss.

use std::sync::Arc;

use futures_util::StreamExt;
use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::RequestRegistry;
use lapin::BasicProperties;
use lapin::Channel;
use lapin::options::BasicConsumeOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use tokio_util::sync::CancellationToken;

use crate::envelope_security::InboundEnvelopeSecurity;
use crate::metadata::AmqpMetadataLimits;
use crate::transport::to_short_string;
use crate::worker::DEFAULT_MAX_PAYLOAD_BYTES;
use crate::worker::RequiredEnvelopeFields;
use crate::worker::delivery_to_envelope;

/// Declare an exclusive, auto-delete, server-named reply inbox and
/// return its generated name.
///
/// The queue dies with the connection: on reconnect the caller must
/// declare a fresh inbox and mint a new name, since a stale inbox name
/// is never delivered to again once its owning connection is gone.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if the broker rejects the
/// declaration.
pub async fn declare_reply_inbox(channel: &Channel) -> Result<String, BusError> {
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                durable: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|err| BusError::connection(Box::new(err), true))?;
    Ok(queue.name().as_str().to_owned())
}

/// Decode one AMQP delivery from the reply inbox into a [`BusEnvelope`].
///
/// Delegates to [`crate::worker::delivery_to_envelope`] so the reply
/// inbox and the regular consumer worker share exactly the same
/// AMQP-property-to-envelope reconstruction: `message_type` from the
/// `type` property, `correlation_id` and `reply_to` from their
/// respective properties, and free-form headers. `required_fields` is the
/// same policy the worker enforces, so a signed or strictly-required reply
/// is rejected on this path exactly as it would be on the worker's.
fn decode_delivery(
    properties: &BasicProperties,
    payload: &[u8],
    metadata_limits: AmqpMetadataLimits,
    required_fields: RequiredEnvelopeFields,
) -> Result<BusEnvelope, BusError> {
    delivery_to_envelope(
        properties,
        payload,
        DEFAULT_MAX_PAYLOAD_BYTES,
        metadata_limits,
        required_fields,
    )
}

/// Consume the reply inbox with `no_ack` and route each delivery to
/// `registry` by request id.
///
/// Runs until `cancel` fires, returning `Ok(())`. A delivery that fails
/// to decode into a [`BusEnvelope`] is logged and dropped rather than
/// tearing down the consumer, since a foreign or malformed message on the
/// inbox is untrusted input, not a framework bug.
///
/// # Errors
///
/// Returns [`BusError::Connection`] (always `retryable: true`) if the
/// consumer cannot be established or if the delivery stream ends before
/// cancellation, so a supervisor can drain in-flight requests and
/// re-declare a fresh inbox on a new connection.
pub async fn run_reply_inbox(
    channel: Channel,
    inbox: String,
    registry: Arc<RequestRegistry>,
    cancel: CancellationToken,
) -> Result<(), BusError> {
    run_reply_inbox_with_limits(
        channel,
        inbox,
        registry,
        cancel,
        AmqpMetadataLimits::default(),
        RequiredEnvelopeFields::default(),
        None,
    )
    .await
}

/// Consume the reply inbox under caller-selected metadata limits and
/// verification policy.
///
/// The reply path applies exactly the same limits, through exactly the same
/// decoder, as the normal worker: a reply inbox that accepted metadata the
/// worker refuses would be a complete bypass of the worker's bound, and it is
/// the path that feeds an RPC correlation slot. `required_fields` is the same
/// policy the worker enforces on its own deliveries, for the same reason: a
/// reply that is signed, or strictly required, must not decode here under a
/// weaker rule than the worker applies to its own inbound deliveries.
///
/// `envelope_security`, when set, authenticates every decoded reply, on this
/// exclusive inbox's own routing key, before [`RequestRegistry::resolve`]
/// ever sees it: see [`verify_before_resolution`] for why that order is
/// non-negotiable. `None` preserves the historical behaviour: a reply
/// resolves its slot regardless of whether it carries a signature.
///
/// # Errors
///
/// Same contract as [`run_reply_inbox`].
pub(crate) async fn run_reply_inbox_with_limits(
    channel: Channel,
    inbox: String,
    registry: Arc<RequestRegistry>,
    cancel: CancellationToken,
    metadata_limits: AmqpMetadataLimits,
    required_fields: RequiredEnvelopeFields,
    envelope_security: Option<Arc<InboundEnvelopeSecurity>>,
) -> Result<(), BusError> {
    let mut consumer = channel
        .basic_consume(
            to_short_string(inbox.as_str(), "reply inbox queue name")?,
            to_short_string("hexeract-reply-inbox", "consumer tag")?,
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|err| BusError::connection(Box::new(err), true))?;

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            next = consumer.next() => match next {
                Some(Ok(delivery)) => match decode_delivery(
                    &delivery.properties,
                    &delivery.data,
                    metadata_limits,
                    required_fields,
                ) {
                    Ok(envelope) => {
                        verify_before_resolution(
                            envelope_security.as_deref(),
                            envelope,
                            delivery.routing_key.as_str(),
                            |envelope| registry.resolve(envelope),
                        )
                        .await;
                    }
                    // The typed error carries a reason and sizes only, never a
                    // header key or value, and the delivery is dropped under
                    // the existing no_ack contract before it can take a
                    // correlation slot.
                    Err(error) => {
                        tracing::warn!(%error, "undecodable reply delivery, dropping");
                    }
                },
                Some(Err(error)) => {
                    return Err(BusError::connection(Box::new(error), true));
                }
                None => {
                    return Err(BusError::connection(
                        "reply inbox consumer stream ended: connection or channel lost",
                        true,
                    ));
                }
            }
        }
    }
}

/// Verify `envelope`, delivered on `destination`, against `envelope_security`,
/// then, only once verification passes, hand it to `resolve`.
///
/// Generic over the resolution operation, the same pattern
/// [`crate::worker::RabbitMqWorker::verify_before_settlement`] uses for the
/// symmetric ordering problem on the worker's dispatch path, so the ordering
/// is unit-testable without a broker.
///
/// `destination` must be the routing key the delivery actually arrived on,
/// never the `x-hexeract-destination` header read back from the envelope
/// itself: the exclusive reply inbox's own name is the one fact a forged or
/// replayed delivery cannot fake, and it is what lets a signature produced
/// for one caller's inbox be rejected when replayed into another's, since
/// each caller's inbox is a distinct, broker-generated name.
///
/// A verification failure is logged and the delivery is dropped without ever
/// reaching `resolve`: the correlation slot the reply claims stays intact,
/// so the legitimate reply, if one is still coming, can still resolve it.
/// This is what makes the first *valid* reply win rather than the first
/// delivery to arrive, exactly mirroring
/// [`hexeract_bus::RequestRegistry::resolve`]'s own contract for a reply that
/// fails its protocol-shape check.
///
/// No `envelope_security` configured is an unconditional pass: `verify` is
/// never reached, so a verification key source configured elsewhere in the
/// process is never consulted, and an unconfigured client resolves every
/// reply exactly as it did before this security surface existed.
async fn verify_before_resolution<Resolve>(
    envelope_security: Option<&InboundEnvelopeSecurity>,
    envelope: BusEnvelope,
    destination: &str,
    resolve: Resolve,
) where
    Resolve: FnOnce(BusEnvelope),
{
    match envelope_security {
        None => resolve(envelope),
        Some(security) => match security.verify(&envelope, destination).await {
            Ok(_principal) => resolve(envelope),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "forged or misdirected reply rejected before it could resolve a \
                     correlation slot, the slot is left pending"
                );
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use hexeract_bus::REQUEST_ID_HEADER;
    use lapin::types::AMQPValue;

    use super::*;

    /// Build reply properties carrying `headers` and the AMQP `type` a
    /// delivery needs to decode into an envelope.
    fn reply_properties<'a>(
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> BasicProperties {
        let mut table = FieldTable::default();
        for (key, value) in headers {
            table.insert(key.into(), AMQPValue::LongString(value.as_bytes().into()));
        }
        BasicProperties::default()
            .with_type("orders.replied".into())
            .with_headers(table)
    }

    #[test]
    fn a_bounded_reply_decodes_and_keeps_its_protocol_header() {
        let properties = reply_properties([(REQUEST_ID_HEADER, "request-1")]);
        let envelope = decode_delivery(
            &properties,
            b"{}",
            AmqpMetadataLimits::default(),
            RequiredEnvelopeFields::Lenient,
        )
        .expect("a bounded reply must decode");
        assert_eq!(envelope.header(REQUEST_ID_HEADER), Some("request-1"));
    }

    #[test]
    fn oversized_reply_metadata_fails_before_resolution() {
        let properties = reply_properties([(REQUEST_ID_HEADER, "request-1")]);
        let limits = AmqpMetadataLimits {
            max_headers: 0,
            ..AmqpMetadataLimits::default()
        };
        assert!(
            matches!(
                decode_delivery(&properties, b"{}", limits, RequiredEnvelopeFields::Lenient),
                Err(BusError::MetadataLimitExceeded {
                    limit: hexeract_bus::MetadataLimit::HeaderCount,
                    actual: 1,
                    max: 0,
                })
            ),
            "an oversized reply must fail decoding, never reach RequestRegistry::resolve"
        );
    }

    #[test]
    fn the_reply_inbox_inherits_the_configured_mode() {
        let properties = reply_properties([(REQUEST_ID_HEADER, "request-1")]);

        let err = decode_delivery(
            &properties,
            b"{}",
            AmqpMetadataLimits::default(),
            RequiredEnvelopeFields::Strict,
        )
        .expect_err("a strict policy must reach the reply inbox decoder too");
        assert!(
            matches!(
                err,
                BusError::EnvelopeSecurity(
                    hexeract_bus::EnvelopeSecurityError::MissingRequiredField {
                        field: "message_id"
                    }
                )
            ),
            "expected EnvelopeSecurity(MissingRequiredField), got {err:?}"
        );
    }

    mod verify_before_resolution {
        use std::collections::HashMap;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::SystemTime;

        use async_trait::async_trait;
        use ed25519_dalek::SigningKey;
        use hexeract_bus::Audience;
        use hexeract_bus::BusEnvelope;
        use hexeract_bus::EnvelopeSecurityConfig;
        use hexeract_bus::Issuer;
        use hexeract_bus::KeyId;
        use hexeract_bus::KeySourceError;
        use hexeract_bus::PROTOCOL_VERSION;
        use hexeract_bus::PROTOCOL_VERSION_HEADER;
        use hexeract_bus::REPLY_STATUS_HEADER;
        use hexeract_bus::REPLY_STATUS_OK;
        use hexeract_bus::ReplyExpectation;
        use hexeract_bus::RequestRegistry;
        use hexeract_bus::SigningKeyHandle;
        use hexeract_bus::SigningKeySource;
        use hexeract_bus::StaticKeySource;
        use hexeract_bus::VerificationKey;
        use hexeract_bus::VerificationKeySource;
        use hexeract_bus::VerificationPolicy;
        use hexeract_core::RequestId;
        use uuid::Uuid;

        use super::*;
        use crate::envelope_security::OutboundEnvelopeSecurity;

        const CLIENT_INBOX: &str = "amq.gen-client-inbox";
        const OTHER_INBOX: &str = "amq.gen-other-inbox";
        const REPLY_MESSAGE_TYPE: &str = "tests.pong";

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

        fn required_security_with_keys(
            keys: Arc<dyn VerificationKeySource>,
        ) -> InboundEnvelopeSecurity {
            let config = EnvelopeSecurityConfig::builder()
                .with_policy(VerificationPolicy::Required)
                .with_accepted_audience(audience())
                .build()
                .expect("valid configuration");
            InboundEnvelopeSecurity::new(keys, config)
        }

        fn required_security() -> InboundEnvelopeSecurity {
            let keys: Arc<dyn VerificationKeySource> = Arc::new(
                StaticKeySource::builder()
                    .with_verification_key(
                        issuer(),
                        key_id(),
                        VerificationKey::from(signing_key().verifying_key()),
                    )
                    .build(),
            );
            required_security_with_keys(keys)
        }

        fn outbound_security() -> OutboundEnvelopeSecurity {
            let keys: Arc<dyn SigningKeySource> = Arc::new(
                StaticKeySource::builder()
                    .with_signing_key(key_id(), SigningKeyHandle::from(signing_key()))
                    .build(),
            );
            OutboundEnvelopeSecurity::new(issuer(), audience(), keys)
        }

        fn reply_protocol_headers(request_id: RequestId) -> HashMap<String, String> {
            let mut headers = HashMap::new();
            headers.insert(
                PROTOCOL_VERSION_HEADER.to_owned(),
                PROTOCOL_VERSION.to_string(),
            );
            headers.insert(REPLY_STATUS_HEADER.to_owned(), REPLY_STATUS_OK.to_owned());
            headers.insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
            headers
        }

        /// An unsigned reply, otherwise shaped exactly as
        /// [`hexeract_bus::RequestRegistry::resolve`] requires: the protocol
        /// version, the reply status, and the request id of the slot it
        /// targets.
        fn unsigned_reply(request_id: RequestId) -> BusEnvelope {
            BusEnvelope::restore_from_transport(
                Uuid::from_u128(1),
                REPLY_MESSAGE_TYPE.to_owned(),
                b"{}".to_vec(),
                Uuid::from_u128(2),
                None,
                HashMap::new(),
                reply_protocol_headers(request_id),
                SystemTime::now(),
            )
        }

        /// Sign a fresh reply for `destination`, correctly, under `security`.
        ///
        /// Signs the envelope carrying its full set of protocol headers
        /// first, since the canonical representation covers them, then
        /// merges the resulting security headers alongside those protocol
        /// headers rather than replacing them: a reply missing its request
        /// id would never reach [`hexeract_bus::RequestRegistry::resolve`]
        /// in the first place, which would make a test built on it prove
        /// nothing about verification.
        fn signed_reply_with(
            security: &OutboundEnvelopeSecurity,
            destination: &str,
            request_id: RequestId,
        ) -> BusEnvelope {
            let envelope = unsigned_reply(request_id);
            let security_headers = security
                .sign(&envelope, destination)
                .expect("signing must succeed");
            let mut protocol_headers = reply_protocol_headers(request_id);
            for (name, value) in &security_headers {
                protocol_headers.insert(name.to_owned(), value.to_owned());
            }
            BusEnvelope::restore_from_transport(
                envelope.message_id,
                envelope.message_type.clone(),
                envelope.payload.clone(),
                envelope.correlation_id,
                envelope.reply_to.clone(),
                envelope.headers.clone(),
                protocol_headers,
                envelope.published_at,
            )
        }

        fn signed_reply(destination: &str, request_id: RequestId) -> BusEnvelope {
            signed_reply_with(&outbound_security(), destination, request_id)
        }

        fn register(
            registry: &RequestRegistry,
            request_id: RequestId,
        ) -> hexeract_bus::PendingReply<'_> {
            registry
                .register(request_id, ReplyExpectation::new(REPLY_MESSAGE_TYPE))
                .expect("registration must succeed")
        }

        #[tokio::test]
        async fn a_forged_reply_never_resolves_the_correlation_slot() {
            let registry = RequestRegistry::default();
            let request_id = RequestId::new();
            let _pending = register(&registry, request_id);
            let security = required_security();

            // Signed correctly, then tampered: the signature headers are
            // present and well-formed, but no longer match the canonical
            // representation.
            let mut envelope = signed_reply(CLIENT_INBOX, request_id);
            envelope.payload = b"{ \"tampered\": true }".to_vec();

            verify_before_resolution(Some(&security), envelope, CLIENT_INBOX, |envelope| {
                registry.resolve(envelope);
            })
            .await;

            assert!(
                !registry.is_empty(),
                "a forged reply must never resolve the correlation slot"
            );
        }

        #[tokio::test]
        async fn a_correctly_signed_reply_resolves_the_correlation_slot() {
            // Symmetric to the forged-reply test above: an implementation
            // that rejected every reply would also leave the slot pending,
            // so this proves a validly signed reply, delivered to the inbox
            // it was signed for, still resolves the caller waiting on it.
            let registry = RequestRegistry::default();
            let request_id = RequestId::new();
            let mut pending = register(&registry, request_id);
            let security = required_security();

            let envelope = signed_reply(CLIENT_INBOX, request_id);

            verify_before_resolution(Some(&security), envelope, CLIENT_INBOX, |envelope| {
                registry.resolve(envelope);
            })
            .await;

            assert!(
                registry.is_empty(),
                "a correctly signed reply must resolve the correlation slot"
            );
            let resolved = pending
                .wait()
                .await
                .expect("the caller must receive its reply");
            assert_eq!(resolved.message_type, REPLY_MESSAGE_TYPE);
        }

        #[tokio::test]
        async fn a_reply_signed_for_another_inbox_is_rejected() {
            // Signed for OTHER_INBOX, but delivered on CLIENT_INBOX: a valid
            // signature captured off one caller's exclusive inbox and
            // replayed onto another's. Each caller's inbox is a distinct,
            // broker-generated name, so this is the cross-caller replay the
            // destination binding exists to close.
            let registry = RequestRegistry::default();
            let request_id = RequestId::new();
            let _pending = register(&registry, request_id);
            let security = required_security();

            let envelope = signed_reply(OTHER_INBOX, request_id);

            verify_before_resolution(Some(&security), envelope, CLIENT_INBOX, |envelope| {
                registry.resolve(envelope);
            })
            .await;

            assert!(
                !registry.is_empty(),
                "a reply signed for another caller's inbox must never resolve this caller's slot"
            );
        }

        /// Records every key lookup a verification performs, delegating to a
        /// real [`StaticKeySource`] so the signature under test genuinely
        /// verifies. The event log this produces is what lets a test observe
        /// that a key lookup, and therefore the whole verification, happened
        /// strictly before resolution, rather than merely trusting the
        /// control flow to have run in the order the source reads.
        struct SpyKeySource {
            inner: StaticKeySource,
            events: Arc<Mutex<Vec<&'static str>>>,
        }

        #[async_trait]
        impl VerificationKeySource for SpyKeySource {
            async fn verification_key(
                &self,
                issuer: &Issuer,
                key_id: &KeyId,
            ) -> Result<VerificationKey, KeySourceError> {
                self.events.lock().unwrap().push("verify");
                self.inner.verification_key(issuer, key_id).await
            }

            async fn refresh(&self) -> Result<(), KeySourceError> {
                self.inner.refresh().await
            }
        }

        #[tokio::test]
        async fn verification_precedes_the_correlation_resolution() {
            let events: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
            let inner = StaticKeySource::builder()
                .with_verification_key(
                    issuer(),
                    key_id(),
                    VerificationKey::from(signing_key().verifying_key()),
                )
                .build();
            let spy: Arc<dyn VerificationKeySource> = Arc::new(SpyKeySource {
                inner,
                events: Arc::clone(&events),
            });
            let security = required_security_with_keys(spy);

            let request_id = RequestId::new();
            let envelope = signed_reply(CLIENT_INBOX, request_id);

            verify_before_resolution(Some(&security), envelope, CLIENT_INBOX, |_envelope| {
                events.lock().unwrap().push("resolve");
            })
            .await;

            assert_eq!(
                *events.lock().unwrap(),
                vec!["verify", "resolve"],
                "verification, key lookup included, must complete strictly before the \
                 correlation slot is resolved"
            );
        }

        /// Counts calls to [`VerificationKeySource::verification_key`] so a
        /// test can prove a source was never consulted at all, rather than
        /// merely that no error surfaced: a client that silently discarded a
        /// verification error would still leave no visible symptom, but
        /// would have paid for a key lookup an unconfigured client must
        /// never make.
        #[derive(Default)]
        struct CountingKeySource {
            lookups: Arc<AtomicUsize>,
        }

        impl CountingKeySource {
            fn lookups(&self) -> Arc<AtomicUsize> {
                Arc::clone(&self.lookups)
            }
        }

        #[async_trait]
        impl VerificationKeySource for CountingKeySource {
            async fn verification_key(
                &self,
                _issuer: &Issuer,
                _key_id: &KeyId,
            ) -> Result<VerificationKey, KeySourceError> {
                self.lookups.fetch_add(1, Ordering::Relaxed);
                Err(KeySourceError::UnknownKey)
            }

            async fn refresh(&self) -> Result<(), KeySourceError> {
                Ok(())
            }
        }

        #[tokio::test]
        async fn an_unconfigured_client_behaves_exactly_as_before() {
            // No `InboundEnvelopeSecurity` is ever constructed for an
            // unconfigured client, so there is no verification key source it
            // could reach for; this counting source stands in for one that
            // exists elsewhere in the process (shared across several
            // clients) but was never wired into this one, and must stay
            // untouched.
            let counting = CountingKeySource::default();
            let lookups = counting.lookups();
            let registry = RequestRegistry::default();
            let request_id = RequestId::new();
            let mut pending = register(&registry, request_id);

            let envelope = unsigned_reply(request_id);

            verify_before_resolution(None, envelope, CLIENT_INBOX, |envelope| {
                registry.resolve(envelope);
            })
            .await;

            assert!(
                registry.is_empty(),
                "an unconfigured client must resolve the slot exactly as before"
            );
            assert!(pending.wait().await.is_ok());
            assert_eq!(
                lookups.load(Ordering::Relaxed),
                0,
                "an unconfigured client must never consult a verification key source"
            );
        }
    }
}
