//! Verification of an inbound envelope before any typed decoding.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Verifier};
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::canonical::{CanonicalBinding, canonical_representation};
use super::error::EnvelopeSecurityError;
use super::identity::{Audience, Issuer, KeyId, SignatureAlgorithm};
use super::key_source::{KeySourceError, VerificationKey, VerificationKeySource};
use super::policy::{EnvelopeSecurityConfig, VerificationPolicy};
use super::principal::VerifiedPrincipal;
use super::protocol::{
    ALGORITHM_HEADER, AUDIENCE_HEADER, DESTINATION_HEADER, ISSUER_HEADER, KEY_ID_HEADER,
    SIGNATURE_HEADER,
};
use crate::BusEnvelope;

/// Where an inbound envelope was actually delivered.
#[derive(Debug, Clone)]
pub struct VerificationContext<'a> {
    /// Routing key the envelope arrived on.
    pub destination: &'a str,
}

/// Verifies inbound envelopes against a key source and a policy.
#[derive(Debug)]
pub struct EnvelopeVerifier<S> {
    keys: S,
    config: EnvelopeSecurityConfig,
    last_refresh: Mutex<Option<Instant>>,
}

impl<S: VerificationKeySource> EnvelopeVerifier<S> {
    /// Verify against `keys`, under `config`.
    #[must_use]
    pub fn new(keys: S, config: EnvelopeSecurityConfig) -> Self {
        Self {
            keys,
            config,
            last_refresh: Mutex::new(None),
        }
    }

    /// Establish the publisher identity of `envelope`.
    ///
    /// Returns `Ok(None)` only when the configured policy allows
    /// unauthenticated envelopes and the envelope carries no signature.
    ///
    /// # Errors
    ///
    /// Every rejection reason of [`EnvelopeSecurityError`]. A rejection is
    /// final: no envelope becomes valid by being delivered again.
    pub async fn verify(
        &self,
        envelope: &BusEnvelope,
        context: &VerificationContext<'_>,
    ) -> Result<Option<VerifiedPrincipal>, EnvelopeSecurityError> {
        let Some(raw_signature) = envelope.header(SIGNATURE_HEADER) else {
            return match self.config.policy() {
                VerificationPolicy::Required => Err(EnvelopeSecurityError::MissingSignature),
                VerificationPolicy::AllowInsecureUnauthenticatedEnvelopes => Ok(None),
            };
        };

        let announced_destination = required_header(envelope, DESTINATION_HEADER)?;
        if announced_destination != context.destination {
            return Err(EnvelopeSecurityError::DestinationMismatch);
        }

        let issuer = Issuer::new(required_header(envelope, ISSUER_HEADER)?)?;
        let audience = Audience::new(required_header(envelope, AUDIENCE_HEADER)?)?;
        let key_id = KeyId::new(required_header(envelope, KEY_ID_HEADER)?)?;
        let algorithm =
            SignatureAlgorithm::from_wire_str(required_header(envelope, ALGORITHM_HEADER)?)?;

        if !self.config.accepted_audiences().contains(&audience) {
            return Err(EnvelopeSecurityError::AudienceMismatch);
        }

        let signature_bytes = URL_SAFE_NO_PAD.decode(raw_signature).map_err(|_error| {
            EnvelopeSecurityError::MalformedSecurityHeader {
                header: SIGNATURE_HEADER,
            }
        })?;
        let signature = Signature::from_slice(&signature_bytes).map_err(|_error| {
            EnvelopeSecurityError::MalformedSecurityHeader {
                header: SIGNATURE_HEADER,
            }
        })?;

        let key = self.resolve_key(&issuer, &key_id).await?;

        let binding = CanonicalBinding {
            destination: context.destination,
            issuer: &issuer,
            audience: &audience,
            key_id: &key_id,
            algorithm,
        };
        let representation = canonical_representation(envelope, &binding)?;

        key.as_verifying_key()
            .verify(&representation, &signature)
            .map_err(|_error| EnvelopeSecurityError::SignatureMismatch)?;

        Ok(Some(VerifiedPrincipal::new(
            issuer, audience, key_id, algorithm,
        )))
    }

    async fn resolve_key(
        &self,
        issuer: &Issuer,
        key_id: &KeyId,
    ) -> Result<VerificationKey, EnvelopeSecurityError> {
        match self.keys.verification_key(issuer, key_id).await {
            Ok(key) => Ok(key),
            Err(KeySourceError::RevokedKey) => Err(EnvelopeSecurityError::RevokedKey),
            Err(KeySourceError::UnknownKey) => {
                self.refresh_if_due().await;
                Err(EnvelopeSecurityError::UnknownKey)
            }
            Err(error) => Err(EnvelopeSecurityError::KeySource(Box::new(error))),
        }
    }

    async fn refresh_if_due(&self) {
        let mut last_refresh = self.last_refresh.lock().await;
        let now = Instant::now();
        let is_due = last_refresh.is_none_or(|previous| {
            now.duration_since(previous) >= self.config.key_refresh_interval()
        });

        if is_due {
            *last_refresh = Some(now);
            drop(last_refresh);
            if let Err(error) = self.keys.refresh().await {
                tracing::debug!(?error, "key source refresh failed");
            }
        }
    }
}

fn required_header<'a>(
    envelope: &'a BusEnvelope,
    header: &'static str,
) -> Result<&'a str, EnvelopeSecurityError> {
    envelope
        .header(header)
        .ok_or(EnvelopeSecurityError::MalformedSecurityHeader { header })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    use async_trait::async_trait;
    use ed25519_dalek::SigningKey;
    use uuid::Uuid;

    use super::super::key_source::{SigningKeyHandle, StaticKeySource};
    use super::super::signer::{EnvelopeSigner, SecurityHeaders, SigningContext};
    use super::*;

    const DESTINATION: &str = "billing.invoice.issued";

    #[derive(Debug, Default, Clone)]
    struct CountingKeySource {
        refreshes: Arc<AtomicUsize>,
        lookups: Arc<AtomicUsize>,
    }

    impl CountingKeySource {
        fn refreshes(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.refreshes)
        }

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
            self.refreshes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn issuer() -> Issuer {
        Issuer::new("billing-service").expect("valid issuer")
    }

    fn audience() -> Audience {
        Audience::new("ledger-service").expect("valid audience")
    }

    fn key_id() -> KeyId {
        KeyId::new("2026-09").expect("valid key id")
    }

    fn apply_security_headers(envelope: &BusEnvelope, headers: &SecurityHeaders) -> BusEnvelope {
        let parts = envelope.security_parts();
        let message_id = *parts.message_id;
        let message_type = parts.message_type.clone();
        let payload = parts.payload.clone();
        let correlation_id = *parts.correlation_id;
        let reply_to = parts.reply_to.clone();
        let application_headers = parts.headers.clone();
        let mut protocol_headers = parts.protocol_headers.clone();
        let published_at = *parts.published_at;

        for (name, value) in headers {
            protocol_headers.insert(name.to_owned(), value.to_owned());
        }

        BusEnvelope::restore_from_transport(
            message_id,
            message_type,
            payload,
            correlation_id,
            reply_to,
            application_headers,
            protocol_headers,
            published_at,
        )
    }

    fn replace_application_headers(
        envelope: &BusEnvelope,
        headers: HashMap<String, String>,
    ) -> BusEnvelope {
        let parts = envelope.security_parts();
        let message_id = *parts.message_id;
        let message_type = parts.message_type.clone();
        let payload = parts.payload.clone();
        let correlation_id = *parts.correlation_id;
        let reply_to = parts.reply_to.clone();
        let protocol_headers = parts.protocol_headers.clone();
        let published_at = *parts.published_at;

        BusEnvelope::restore_from_transport(
            message_id,
            message_type,
            payload,
            correlation_id,
            reply_to,
            headers,
            protocol_headers,
            published_at,
        )
    }

    fn with_replaced_security_header(
        envelope: &BusEnvelope,
        header: &'static str,
        value: &str,
    ) -> BusEnvelope {
        let parts = envelope.security_parts();
        let message_id = *parts.message_id;
        let message_type = parts.message_type.clone();
        let payload = parts.payload.clone();
        let correlation_id = *parts.correlation_id;
        let reply_to = parts.reply_to.clone();
        let application_headers = parts.headers.clone();
        let mut protocol_headers = parts.protocol_headers.clone();
        let published_at = *parts.published_at;

        protocol_headers.insert(header.to_owned(), value.to_owned());

        BusEnvelope::restore_from_transport(
            message_id,
            message_type,
            payload,
            correlation_id,
            reply_to,
            application_headers,
            protocol_headers,
            published_at,
        )
    }

    fn signed_envelope() -> BusEnvelope {
        let keys = StaticKeySource::builder()
            .with_signing_key(
                key_id(),
                SigningKeyHandle::from(SigningKey::from_bytes(&[1; 32])),
            )
            .build();
        let signer = EnvelopeSigner::new(issuer(), keys);
        let envelope = BusEnvelope::restore_from_transport(
            Uuid::from_u128(1),
            DESTINATION.to_owned(),
            b"{}".to_vec(),
            Uuid::from_u128(2),
            None,
            HashMap::new(),
            HashMap::new(),
            UNIX_EPOCH + Duration::from_secs(1_757_000_000),
        );
        let audience = audience();
        let headers = signer
            .sign(
                &envelope,
                &SigningContext {
                    destination: DESTINATION,
                    audience: &audience,
                },
            )
            .expect("signed");

        apply_security_headers(&envelope, &headers)
    }

    fn verifier() -> EnvelopeVerifier<StaticKeySource> {
        let keys = StaticKeySource::builder()
            .with_verification_key(
                issuer(),
                key_id(),
                VerificationKey::from(SigningKey::from_bytes(&[1; 32]).verifying_key()),
            )
            .build();
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .build()
            .expect("valid configuration");

        EnvelopeVerifier::new(keys, config)
    }

    #[tokio::test]
    async fn a_correctly_signed_envelope_yields_its_principal() {
        let principal = verifier()
            .verify(
                &signed_envelope(),
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect("verified")
            .expect("authenticated");

        assert_eq!(principal.issuer().as_str(), "billing-service");
        assert_eq!(principal.key_id().as_str(), "2026-09");
    }

    #[tokio::test]
    async fn changing_one_payload_byte_is_rejected() {
        let mut envelope = signed_envelope();
        envelope.payload = b"{ }".to_vec();

        let error = verifier()
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("mutated payload");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn changing_the_message_type_is_rejected() {
        let mut envelope = signed_envelope();
        envelope.message_type = "billing.invoice.cancelled".to_owned();

        let error = verifier()
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("mutated type");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn changing_the_message_id_is_rejected() {
        let mut envelope = signed_envelope();
        envelope.message_id = uuid::Uuid::from_u128(999);

        let error = verifier()
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("mutated id");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn changing_the_correlation_id_is_rejected() {
        let mut envelope = signed_envelope();
        envelope.correlation_id = uuid::Uuid::from_u128(998);

        let error = verifier()
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("mutated correlation");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn changing_the_reply_to_is_rejected() {
        let mut envelope = signed_envelope();
        envelope.reply_to = Some("attacker.inbox".to_owned());

        let error = verifier()
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("mutated reply_to");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn changing_an_application_header_is_rejected() {
        let envelope = signed_envelope();
        let mut headers = HashMap::new();
        headers.insert("tenant".to_owned(), "globex".to_owned());
        let forged = replace_application_headers(&envelope, headers);

        let error = verifier()
            .verify(
                &forged,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("mutated tenant header");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn an_envelope_delivered_elsewhere_is_reported_as_a_destination_mismatch() {
        let error = verifier()
            .verify(
                &signed_envelope(),
                &VerificationContext {
                    destination: "audit.siphon",
                },
            )
            .await
            .expect_err("rerouted");

        assert!(matches!(error, EnvelopeSecurityError::DestinationMismatch));
    }

    #[tokio::test]
    async fn a_signature_produced_by_another_key_is_rejected() {
        let impostor_keys = StaticKeySource::builder()
            .with_signing_key(
                key_id(),
                SigningKeyHandle::from(SigningKey::from_bytes(&[9; 32])),
            )
            .build();
        let audience = audience();
        let envelope = BusEnvelope::restore_from_transport(
            Uuid::from_u128(1),
            DESTINATION.to_owned(),
            b"{}".to_vec(),
            Uuid::from_u128(2),
            None,
            HashMap::new(),
            HashMap::new(),
            UNIX_EPOCH + Duration::from_secs(1_757_000_000),
        );
        let headers = EnvelopeSigner::new(issuer(), impostor_keys)
            .sign(
                &envelope,
                &SigningContext {
                    destination: DESTINATION,
                    audience: &audience,
                },
            )
            .expect("signed by the impostor");
        let forged = apply_security_headers(&envelope, &headers);

        let error = verifier()
            .verify(
                &forged,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("signed by a key the consumer does not trust");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn an_unsigned_envelope_is_refused_when_verification_is_required() {
        let envelope = BusEnvelope::restore_from_transport(
            Uuid::from_u128(1),
            DESTINATION.to_owned(),
            b"{}".to_vec(),
            Uuid::from_u128(2),
            None,
            HashMap::new(),
            HashMap::new(),
            UNIX_EPOCH + Duration::from_secs(1_757_000_000),
        );

        let error = verifier()
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("unsigned envelope");

        assert!(matches!(error, EnvelopeSecurityError::MissingSignature));
    }

    #[tokio::test]
    async fn an_unsigned_envelope_is_accepted_without_a_principal_under_the_opt_out() {
        let config = EnvelopeSecurityConfig::builder()
            .with_policy(VerificationPolicy::AllowInsecureUnauthenticatedEnvelopes)
            .build()
            .expect("opted out");
        let verifier = EnvelopeVerifier::new(StaticKeySource::builder().build(), config);
        let envelope = BusEnvelope::restore_from_transport(
            Uuid::from_u128(1),
            DESTINATION.to_owned(),
            b"{}".to_vec(),
            Uuid::from_u128(2),
            None,
            HashMap::new(),
            HashMap::new(),
            UNIX_EPOCH + Duration::from_secs(1_757_000_000),
        );

        let principal = verifier
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect("accepted");

        assert!(principal.is_none());
    }

    #[tokio::test]
    async fn a_broken_signature_is_still_rejected_under_the_opt_out() {
        let keys = StaticKeySource::builder()
            .with_verification_key(
                issuer(),
                key_id(),
                VerificationKey::from(SigningKey::from_bytes(&[1; 32]).verifying_key()),
            )
            .build();
        let config = EnvelopeSecurityConfig::builder()
            .with_policy(VerificationPolicy::AllowInsecureUnauthenticatedEnvelopes)
            .with_accepted_audience(audience())
            .build()
            .expect("opted out");
        let verifier = EnvelopeVerifier::new(keys, config);
        let mut envelope = signed_envelope();
        envelope.payload = b"{ }".to_vec();

        let error = verifier
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("a signed envelope with a broken signature is never silently downgraded");

        assert!(matches!(error, EnvelopeSecurityError::SignatureMismatch));
    }

    #[tokio::test]
    async fn an_envelope_for_another_audience_is_rejected() {
        let keys = StaticKeySource::builder()
            .with_verification_key(
                issuer(),
                key_id(),
                VerificationKey::from(SigningKey::from_bytes(&[1; 32]).verifying_key()),
            )
            .build();
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(Audience::new("audit-service").expect("valid audience"))
            .build()
            .expect("valid configuration");
        let verifier = EnvelopeVerifier::new(keys, config);

        let error = verifier
            .verify(
                &signed_envelope(),
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("wrong audience");

        assert!(matches!(error, EnvelopeSecurityError::AudienceMismatch));
    }

    #[tokio::test]
    async fn a_revoked_key_is_rejected_distinctly_from_an_unknown_one() {
        let keys = StaticKeySource::builder()
            .with_revoked_key(issuer(), key_id())
            .build();
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .build()
            .expect("valid configuration");
        let verifier = EnvelopeVerifier::new(keys, config);

        let error = verifier
            .verify(
                &signed_envelope(),
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("revoked key");

        assert!(matches!(error, EnvelopeSecurityError::RevokedKey));
    }

    #[tokio::test]
    async fn an_unsupported_algorithm_is_rejected_before_any_key_lookup() {
        let counting = CountingKeySource::default();
        let lookups = counting.lookups();
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .build()
            .expect("valid configuration");
        let verifier = EnvelopeVerifier::new(counting, config);
        let envelope =
            with_replaced_security_header(&signed_envelope(), ALGORITHM_HEADER, "rsa-pkcs1");

        let error = verifier
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("unsupported algorithm");

        assert!(matches!(error, EnvelopeSecurityError::UnsupportedAlgorithm));
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "an unsupported algorithm must be refused before the key source is asked anything"
        );
    }

    #[tokio::test]
    async fn a_malformed_signature_is_reported_as_a_malformed_header() {
        let envelope = with_replaced_security_header(&signed_envelope(), SIGNATURE_HEADER, "!!!");

        let error = verifier()
            .verify(
                &envelope,
                &VerificationContext {
                    destination: DESTINATION,
                },
            )
            .await
            .expect_err("malformed signature");

        assert!(matches!(
            error,
            EnvelopeSecurityError::MalformedSecurityHeader {
                header: SIGNATURE_HEADER
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_unknown_keys_trigger_a_single_refresh_within_the_interval() {
        let counting = CountingKeySource::default();
        let refreshes = counting.refreshes();
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .with_key_refresh_interval(Duration::from_secs(60))
            .build()
            .expect("valid configuration");
        let verifier = EnvelopeVerifier::new(counting, config);

        for _ in 0..10 {
            let _ = verifier
                .verify(
                    &signed_envelope(),
                    &VerificationContext {
                        destination: DESTINATION,
                    },
                )
                .await;
        }

        assert_eq!(refreshes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_refresh_happens_again_once_the_interval_has_elapsed() {
        let counting = CountingKeySource::default();
        let refreshes = counting.refreshes();
        let interval = Duration::from_secs(60);
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .with_key_refresh_interval(interval)
            .build()
            .expect("valid configuration");
        let verifier = EnvelopeVerifier::new(counting, config);
        let context = VerificationContext {
            destination: DESTINATION,
        };

        let _ = verifier.verify(&signed_envelope(), &context).await;
        tokio::time::advance(interval + Duration::from_secs(1)).await;
        let _ = verifier.verify(&signed_envelope(), &context).await;

        assert_eq!(refreshes.load(Ordering::Relaxed), 2);
    }
}
