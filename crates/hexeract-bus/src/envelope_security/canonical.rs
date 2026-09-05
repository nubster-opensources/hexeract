//! Deterministic byte representation an envelope signature is computed over.
//!
//! Two properties matter and neither is optional.
//!
//! **Determinism.** The same envelope must always produce the same bytes, on
//! any machine, in any order of header insertion. Headers therefore travel
//! sorted by the bytes of their key, never by a locale-dependent collation.
//!
//! **Unambiguous framing.** Every element is preceded by its length. Without
//! it, the pairs `("ab", "c")` and `("a", "bc")` would concatenate to the same
//! bytes, and an attacker could move the boundary between two fields while
//! keeping a valid signature.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::error::EnvelopeSecurityError;
use super::identity::{Audience, Issuer, KeyId, SignatureAlgorithm};
use super::protocol::{
    ALGORITHM_HEADER, AUDIENCE_HEADER, CANONICAL_DOMAIN, DESTINATION_HEADER, ISSUER_HEADER,
    KEY_ID_HEADER, SIGNATURE_HEADER,
};
use crate::BusEnvelope;

/// Every field of an envelope, borrowed for canonical serialization.
pub(crate) struct EnvelopeSecurityParts<'a> {
    pub(crate) message_id: &'a Uuid,
    pub(crate) message_type: &'a String,
    pub(crate) payload: &'a Vec<u8>,
    pub(crate) correlation_id: &'a Uuid,
    pub(crate) reply_to: &'a Option<String>,
    pub(crate) headers: &'a HashMap<String, String>,
    pub(crate) protocol_headers: &'a HashMap<String, String>,
    pub(crate) published_at: &'a SystemTime,
}

/// The security facts an envelope is bound to by its signature.
#[derive(Debug, Clone)]
pub struct CanonicalBinding<'a> {
    /// Destination the publisher signed the envelope for.
    pub destination: &'a str,
    /// Authenticated publisher.
    pub issuer: &'a Issuer,
    /// Intended recipient.
    pub audience: &'a Audience,
    /// Key producing or having produced the signature.
    pub key_id: &'a KeyId,
    /// Signature algorithm.
    pub algorithm: SignatureAlgorithm,
}

/// Build the canonical byte representation of `envelope` under `binding`.
///
/// # Errors
///
/// Returns [`EnvelopeSecurityError::MissingRequiredField`] when the
/// publication time predates the Unix epoch, and
/// [`EnvelopeSecurityError::FieldTooLarge`] when a covered field is longer
/// than the framing can encode.
pub fn canonical_representation(
    envelope: &BusEnvelope,
    binding: &CanonicalBinding<'_>,
) -> Result<Vec<u8>, EnvelopeSecurityError> {
    let parts = envelope.security_parts();

    let published_at_seconds = parts
        .published_at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| EnvelopeSecurityError::MissingRequiredField {
            field: "published_at",
        })?
        .as_secs();

    let mut bytes = Vec::new();
    push_field(&mut bytes, CANONICAL_DOMAIN.as_bytes())?;
    push_field(&mut bytes, parts.message_id.as_bytes())?;
    push_field(&mut bytes, parts.message_type.as_bytes())?;
    push_field(&mut bytes, parts.correlation_id.as_bytes())?;
    push_optional_field(&mut bytes, parts.reply_to.as_deref().map(str::as_bytes))?;
    push_field(&mut bytes, &published_at_seconds.to_be_bytes())?;
    push_field(&mut bytes, binding.destination.as_bytes())?;
    push_field(&mut bytes, binding.issuer.as_str().as_bytes())?;
    push_field(&mut bytes, binding.audience.as_str().as_bytes())?;
    push_field(&mut bytes, binding.key_id.as_str().as_bytes())?;
    push_field(&mut bytes, binding.algorithm.as_wire_str().as_bytes())?;
    push_headers(&mut bytes, parts.headers, parts.protocol_headers)?;
    push_field(&mut bytes, Sha256::digest(parts.payload).as_slice())?;

    Ok(bytes)
}

fn push_field(bytes: &mut Vec<u8>, field: &[u8]) -> Result<(), EnvelopeSecurityError> {
    let length = u32::try_from(field.len()).map_err(|_| EnvelopeSecurityError::FieldTooLarge)?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(field);
    Ok(())
}

fn push_optional_field(
    bytes: &mut Vec<u8>,
    field: Option<&[u8]>,
) -> Result<(), EnvelopeSecurityError> {
    if let Some(field) = field {
        bytes.push(1);
        push_field(bytes, field)
    } else {
        bytes.push(0);
        Ok(())
    }
}

fn push_headers(
    bytes: &mut Vec<u8>,
    headers: &HashMap<String, String>,
    protocol_headers: &HashMap<String, String>,
) -> Result<(), EnvelopeSecurityError> {
    let mut pairs: Vec<(&str, &str)> = headers
        .iter()
        .chain(protocol_headers.iter())
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .filter(|(key, _)| !is_security_header(key))
        .collect();
    pairs.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));

    let count = u32::try_from(pairs.len()).map_err(|_| EnvelopeSecurityError::FieldTooLarge)?;
    bytes.extend_from_slice(&count.to_be_bytes());

    for (key, value) in pairs {
        push_field(bytes, key.as_bytes())?;
        push_field(bytes, value.as_bytes())?;
    }

    Ok(())
}

fn is_security_header(key: &str) -> bool {
    [
        SIGNATURE_HEADER,
        KEY_ID_HEADER,
        ISSUER_HEADER,
        AUDIENCE_HEADER,
        ALGORITHM_HEADER,
        DESTINATION_HEADER,
    ]
    .contains(&key)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    use uuid::Uuid;

    use super::*;

    fn identities() -> (Issuer, Audience, KeyId) {
        (
            Issuer::new("billing-service").expect("valid issuer"),
            Audience::new("ledger-service").expect("valid audience"),
            KeyId::new("2026-09").expect("valid key id"),
        )
    }

    fn envelope_with(headers: HashMap<String, String>, reply_to: Option<&str>) -> BusEnvelope {
        BusEnvelope::restore_from_transport(
            Uuid::from_u128(1),
            "billing.invoice.issued".to_owned(),
            b"{}".to_vec(),
            Uuid::from_u128(2),
            reply_to.map(ToOwned::to_owned),
            headers,
            HashMap::new(),
            UNIX_EPOCH + Duration::from_secs(1_757_000_000),
        )
    }

    fn representation(envelope: &BusEnvelope) -> Vec<u8> {
        let (issuer, audience, key_id) = identities();
        let binding = CanonicalBinding {
            destination: "billing.invoice.issued",
            issuer: &issuer,
            audience: &audience,
            key_id: &key_id,
            algorithm: SignatureAlgorithm::Ed25519,
        };
        canonical_representation(envelope, &binding).expect("canonical representation")
    }

    #[test]
    fn the_representation_is_stable_across_calls() {
        let envelope = envelope_with(HashMap::new(), None);

        assert_eq!(representation(&envelope), representation(&envelope));
    }

    #[test]
    fn the_representation_opens_with_the_domain_prefix() {
        let envelope = envelope_with(HashMap::new(), None);

        let bytes = representation(&envelope);
        let domain = CANONICAL_DOMAIN.as_bytes();
        let length = u32::try_from(domain.len()).expect("short domain");

        assert_eq!(&bytes[..4], &length.to_be_bytes());
        assert_eq!(&bytes[4..4 + domain.len()], domain);
    }

    #[test]
    fn header_insertion_order_does_not_change_the_representation() {
        let mut first = HashMap::new();
        first.insert("tenant".to_owned(), "acme".to_owned());
        first.insert("region".to_owned(), "eu-west".to_owned());

        let mut second = HashMap::new();
        second.insert("region".to_owned(), "eu-west".to_owned());
        second.insert("tenant".to_owned(), "acme".to_owned());

        assert_eq!(
            representation(&envelope_with(first, None)),
            representation(&envelope_with(second, None))
        );
    }

    #[test]
    fn a_missing_reply_to_differs_from_an_empty_one() {
        let absent = representation(&envelope_with(HashMap::new(), None));
        let empty = representation(&envelope_with(HashMap::new(), Some("")));

        assert_ne!(absent, empty);
    }

    fn representation_with_identities(
        envelope: &BusEnvelope,
        issuer: &str,
        audience: &str,
    ) -> Vec<u8> {
        let issuer = Issuer::new(issuer).expect("valid issuer");
        let audience = Audience::new(audience).expect("valid audience");
        let key_id = KeyId::new("2026-09").expect("valid key id");
        let binding = CanonicalBinding {
            destination: "billing.invoice.issued",
            issuer: &issuer,
            audience: &audience,
            key_id: &key_id,
            algorithm: SignatureAlgorithm::Ed25519,
        };
        canonical_representation(envelope, &binding).expect("canonical representation")
    }

    #[test]
    fn a_field_boundary_cannot_be_shifted_between_two_adjacent_fields() {
        let envelope = envelope_with(HashMap::new(), None);

        assert_ne!(
            representation_with_identities(&envelope, "ab", "c"),
            representation_with_identities(&envelope, "a", "bc"),
            "issuer and audience sit side by side in the stream, so without a length prefix \
             both pairs would concatenate to the same bytes"
        );
    }

    #[test]
    fn the_representation_covers_published_at_to_the_second() {
        let mut envelope = envelope_with(HashMap::new(), None);
        envelope.published_at = UNIX_EPOCH + Duration::from_secs(1_757_000_000);
        let baseline = representation(&envelope);

        let mut next_second = envelope.clone();
        next_second.published_at = UNIX_EPOCH + Duration::from_secs(1_757_000_001);

        assert_ne!(baseline, representation(&next_second));
    }

    #[test]
    fn the_representation_ignores_sub_second_precision_of_published_at() {
        let mut envelope = envelope_with(HashMap::new(), None);
        envelope.published_at = UNIX_EPOCH + Duration::from_secs(1_757_000_000);
        let baseline = representation(&envelope);

        let mut jittered = envelope.clone();
        jittered.published_at =
            UNIX_EPOCH + Duration::from_secs(1_757_000_000) + Duration::from_millis(999);

        assert_eq!(
            baseline,
            representation(&jittered),
            "the AMQP timestamp property carries whole seconds, so signing anything finer \
             would make every signature fail after a round trip through the broker"
        );
    }

    #[test]
    fn adding_an_application_header_changes_the_representation() {
        let mut headers = HashMap::new();
        headers.insert("tenant".to_owned(), "acme".to_owned());

        assert_ne!(
            representation(&envelope_with(HashMap::new(), None)),
            representation(&envelope_with(headers, None))
        );
    }

    #[test]
    fn a_security_header_written_in_another_case_stays_covered() {
        let envelope = envelope_with(HashMap::new(), None);
        let baseline = representation(&envelope);

        let mut tampered = envelope.clone();
        tampered.insert_protocol_header("X-Hexeract-Issuer", "evil".to_owned());

        assert_ne!(
            baseline,
            representation(&tampered),
            "only the six exact header names are covered at a fixed position; anything else \
             must enter the covered block, or it could be added to a signed envelope for free"
        );
    }

    #[test]
    fn changing_one_payload_byte_changes_the_representation() {
        let envelope = envelope_with(HashMap::new(), None);
        let mut mutated = envelope.clone();
        mutated.payload = b"{ }".to_vec();

        assert_ne!(representation(&envelope), representation(&mutated));
    }

    #[test]
    fn changing_an_application_header_changes_the_representation() {
        let mut headers = HashMap::new();
        headers.insert("tenant".to_owned(), "acme".to_owned());
        let mut other = HashMap::new();
        other.insert("tenant".to_owned(), "globex".to_owned());

        assert_ne!(
            representation(&envelope_with(headers, None)),
            representation(&envelope_with(other, None))
        );
    }

    #[test]
    fn changing_the_destination_changes_the_representation() {
        let envelope = envelope_with(HashMap::new(), None);
        let (issuer, audience, key_id) = identities();
        let signed_for = CanonicalBinding {
            destination: "billing.invoice.issued",
            issuer: &issuer,
            audience: &audience,
            key_id: &key_id,
            algorithm: SignatureAlgorithm::Ed25519,
        };
        let rerouted = CanonicalBinding {
            destination: "audit.siphon",
            ..signed_for.clone()
        };

        assert_ne!(
            canonical_representation(&envelope, &signed_for).expect("signed"),
            canonical_representation(&envelope, &rerouted).expect("rerouted")
        );
    }

    #[test]
    fn the_signature_header_is_excluded_from_the_header_block() {
        let mut envelope = envelope_with(HashMap::new(), None);
        let baseline = representation(&envelope);
        envelope.insert_protocol_header(SIGNATURE_HEADER, "not-a-real-signature".to_owned());

        assert_eq!(baseline, representation(&envelope));
    }
}
