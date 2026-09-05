//! Verification of an inbound envelope before any typed decoding.

use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Verifier};
use tokio::sync::Mutex;

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
    use std::time::{Duration, UNIX_EPOCH};

    use ed25519_dalek::SigningKey;
    use uuid::Uuid;

    use super::super::key_source::{SigningKeyHandle, StaticKeySource};
    use super::super::signer::{EnvelopeSigner, SecurityHeaders, SigningContext};
    use super::*;

    const DESTINATION: &str = "billing.invoice.issued";

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
}
