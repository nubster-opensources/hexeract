//! Production of the security headers carried by a signed envelope.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::Signer;

use super::canonical::{CanonicalBinding, canonical_representation};
use super::error::EnvelopeSecurityError;
use super::identity::{Audience, Issuer, SignatureAlgorithm};
use super::key_source::SigningKeySource;
use super::protocol::{
    ALGORITHM_HEADER, AUDIENCE_HEADER, DESTINATION_HEADER, ISSUER_HEADER, KEY_ID_HEADER,
    SIGNATURE_HEADER,
};
use crate::BusEnvelope;

/// The six security headers a signed envelope carries.
#[derive(Debug, Clone)]
pub struct SecurityHeaders {
    entries: Vec<(&'static str, String)>,
}

impl SecurityHeaders {
    /// Iterate over the header names and values to publish.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.entries
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
    }
}

impl<'a> IntoIterator for &'a SecurityHeaders {
    type Item = (&'static str, &'a str);
    type IntoIter = Box<dyn Iterator<Item = (&'static str, &'a str)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// What an outbound envelope is being signed for.
#[derive(Debug, Clone)]
pub struct SigningContext<'a> {
    /// Destination the envelope is published to.
    pub destination: &'a str,
    /// Audience the envelope is intended for.
    pub audience: &'a Audience,
}

/// Signs outbound envelopes on behalf of one issuer.
#[derive(Debug)]
pub struct EnvelopeSigner<S> {
    issuer: Issuer,
    keys: S,
}

impl<S: SigningKeySource> EnvelopeSigner<S> {
    /// Sign on behalf of `issuer`, using `keys`.
    #[must_use]
    pub fn new(issuer: Issuer, keys: S) -> Self {
        Self { issuer, keys }
    }

    /// Produce the security headers binding `envelope` to `context`.
    ///
    /// # Errors
    ///
    /// [`EnvelopeSecurityError::KeySource`] when no signing key is available,
    /// and any error raised while building the canonical representation.
    pub fn sign(
        &self,
        envelope: &BusEnvelope,
        context: &SigningContext<'_>,
    ) -> Result<SecurityHeaders, EnvelopeSecurityError> {
        let (key_id, handle) = self
            .keys
            .current_signing_key()
            .map_err(|error| EnvelopeSecurityError::KeySource(Box::new(error)))?;

        let algorithm = SignatureAlgorithm::Ed25519;
        let binding = CanonicalBinding {
            destination: context.destination,
            issuer: &self.issuer,
            audience: context.audience,
            key_id: &key_id,
            algorithm,
        };

        let representation = canonical_representation(envelope, &binding)?;
        let signature = handle.as_signing_key().sign(&representation);

        Ok(SecurityHeaders {
            entries: vec![
                (
                    SIGNATURE_HEADER,
                    URL_SAFE_NO_PAD.encode(signature.to_bytes()),
                ),
                (KEY_ID_HEADER, key_id.as_str().to_owned()),
                (ISSUER_HEADER, self.issuer.as_str().to_owned()),
                (AUDIENCE_HEADER, context.audience.as_str().to_owned()),
                (ALGORITHM_HEADER, algorithm.as_wire_str().to_owned()),
                (DESTINATION_HEADER, context.destination.to_owned()),
            ],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    use ed25519_dalek::SigningKey;
    use uuid::Uuid;

    use super::super::identity::KeyId;
    use super::super::key_source::{SigningKeyHandle, StaticKeySource};
    use super::*;

    fn signer() -> EnvelopeSigner<StaticKeySource> {
        let keys = StaticKeySource::builder()
            .with_signing_key(
                KeyId::new("2026-09").expect("valid key id"),
                SigningKeyHandle::from(SigningKey::from_bytes(&[1; 32])),
            )
            .build();
        EnvelopeSigner::new(Issuer::new("billing-service").expect("valid issuer"), keys)
    }

    fn envelope() -> BusEnvelope {
        BusEnvelope::restore_from_transport(
            Uuid::from_u128(1),
            "billing.invoice.issued".to_owned(),
            b"{}".to_vec(),
            Uuid::from_u128(2),
            None,
            HashMap::new(),
            HashMap::new(),
            UNIX_EPOCH + Duration::from_secs(1_757_000_000),
        )
    }

    fn audience() -> Audience {
        Audience::new("ledger-service").expect("valid audience")
    }

    fn context(audience: &Audience) -> SigningContext<'_> {
        SigningContext {
            destination: "billing.invoice.issued",
            audience,
        }
    }

    #[test]
    fn signing_produces_every_security_header() {
        let audience = audience();
        let headers = signer()
            .sign(&envelope(), &context(&audience))
            .expect("signed");

        let names: Vec<&str> = headers.iter().map(|(name, _)| name).collect();

        assert!(names.contains(&SIGNATURE_HEADER));
        assert!(names.contains(&KEY_ID_HEADER));
        assert!(names.contains(&ISSUER_HEADER));
        assert!(names.contains(&AUDIENCE_HEADER));
        assert!(names.contains(&ALGORITHM_HEADER));
        assert!(names.contains(&DESTINATION_HEADER));
        assert_eq!(names.len(), 6);
    }

    #[test]
    fn signing_the_same_envelope_twice_yields_the_same_signature() {
        let audience = audience();
        let envelope = envelope();
        let signer = signer();

        let first = signer.sign(&envelope, &context(&audience)).expect("signed");
        let second = signer.sign(&envelope, &context(&audience)).expect("signed");

        assert_eq!(signature_of(&first), signature_of(&second));
    }

    #[test]
    fn changing_one_payload_byte_changes_the_signature() {
        let audience = audience();
        let signer = signer();
        let envelope = envelope();
        let mut mutated = envelope.clone();
        mutated.payload = b"{ }".to_vec();

        let original = signer.sign(&envelope, &context(&audience)).expect("signed");
        let forged = signer.sign(&mutated, &context(&audience)).expect("signed");

        assert_ne!(signature_of(&original), signature_of(&forged));
    }

    #[test]
    fn signing_without_a_configured_key_fails() {
        let audience = audience();
        let signer = EnvelopeSigner::new(
            Issuer::new("billing-service").expect("valid issuer"),
            StaticKeySource::builder().build(),
        );

        let error = signer
            .sign(&envelope(), &context(&audience))
            .expect_err("no signing key");

        assert!(matches!(error, EnvelopeSecurityError::KeySource(_)));
    }

    fn signature_of(headers: &SecurityHeaders) -> String {
        headers
            .iter()
            .find(|(name, _)| *name == SIGNATURE_HEADER)
            .map(|(_, value)| value.to_owned())
            .expect("signature header")
    }

    fn value_of(headers: &SecurityHeaders, name: &str) -> String {
        headers
            .iter()
            .find(|(header, _)| *header == name)
            .map(|(_, value)| value.to_owned())
            .expect("header is present")
    }

    #[test]
    fn every_security_header_announces_the_value_that_was_signed() {
        let audience = audience();
        let headers = signer()
            .sign(&envelope(), &context(&audience))
            .expect("signed");

        assert_eq!(value_of(&headers, KEY_ID_HEADER), "2026-09");
        assert_eq!(value_of(&headers, ISSUER_HEADER), "billing-service");
        assert_eq!(value_of(&headers, AUDIENCE_HEADER), "ledger-service");
        assert_eq!(value_of(&headers, ALGORITHM_HEADER), "ed25519");
        assert_eq!(
            value_of(&headers, DESTINATION_HEADER),
            "billing.invoice.issued"
        );
    }

    #[test]
    fn the_produced_signature_verifies_against_the_matching_public_key() {
        use base64::Engine;
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use ed25519_dalek::{Signature, Verifier};

        let audience = audience();
        let envelope = envelope();
        let headers = signer()
            .sign(&envelope, &context(&audience))
            .expect("signed");

        let issuer = Issuer::new("billing-service").expect("valid issuer");
        let key_id = KeyId::new("2026-09").expect("valid key id");
        let binding = CanonicalBinding {
            destination: "billing.invoice.issued",
            issuer: &issuer,
            audience: &audience,
            key_id: &key_id,
            algorithm: SignatureAlgorithm::Ed25519,
        };
        let representation =
            canonical_representation(&envelope, &binding).expect("canonical representation");

        let raw = URL_SAFE_NO_PAD
            .decode(signature_of(&headers))
            .expect("base64 signature");
        let signature = Signature::from_slice(&raw).expect("64 byte signature");
        let public_key = SigningKey::from_bytes(&[1; 32]).verifying_key();

        assert!(public_key.verify(&representation, &signature).is_ok());
    }
}
