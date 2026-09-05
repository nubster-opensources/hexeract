//! Errors raised while signing or verifying an envelope.

use thiserror::Error;

/// Which identity a rejected value was meant to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentityKind {
    /// The authenticated publisher of an envelope.
    Issuer,
    /// The intended recipient of an envelope.
    Audience,
    /// The identifier of a signing key.
    KeyId,
}

impl std::fmt::Display for IdentityKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Issuer => "issuer",
            Self::Audience => "audience",
            Self::KeyId => "key id",
        };
        formatter.write_str(name)
    }
}

/// Failure raised while signing or verifying an envelope.
///
/// No variant ever carries key material, a signature, a payload or a header
/// value: a rejection is diagnosed from identifiers and reason codes alone.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EnvelopeSecurityError {
    /// The envelope carries no security headers at all.
    #[error("envelope carries no signature")]
    MissingSignature,

    /// A security header is present but cannot be parsed.
    #[error("security header {header} is malformed")]
    MalformedSecurityHeader {
        /// Name of the offending header.
        header: &'static str,
    },

    /// The announced algorithm is not implemented.
    #[error("signature algorithm is not supported")]
    UnsupportedAlgorithm,

    /// No key matches the announced issuer and key identifier.
    #[error("signing key is unknown")]
    UnknownKey,

    /// The announced key is known and explicitly revoked.
    #[error("signing key is revoked")]
    RevokedKey,

    /// The envelope was signed for an audience this consumer does not accept.
    #[error("envelope audience is not accepted here")]
    AudienceMismatch,

    /// The envelope arrived somewhere other than the destination it was signed for.
    #[error("envelope was signed for another destination")]
    DestinationMismatch,

    /// The signature does not match the canonical representation.
    #[error("signature does not match the envelope")]
    SignatureMismatch,

    /// A field covered by the signature is absent and has no default.
    #[error("required field {field} is missing")]
    MissingRequiredField {
        /// Name of the missing field.
        field: &'static str,
    },

    /// A covered field exceeds the length the canonical framing can encode.
    #[error("a covered field exceeds the maximum encodable length")]
    FieldTooLarge,

    /// An identity value failed validation.
    #[error("value is not a valid {kind}")]
    InvalidIdentity {
        /// Which identity was being built.
        kind: IdentityKind,
    },

    /// The key source could not answer.
    #[error("key source failure")]
    KeySource(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_source_error_display_hides_its_source() {
        let inner = std::io::Error::other("SUPER_SECRET_KEY_MATERIAL");
        let error = EnvelopeSecurityError::KeySource(Box::new(inner));

        let rendered = error.to_string();

        assert!(
            !rendered.contains("SUPER_SECRET_KEY_MATERIAL"),
            "rendered as {rendered}"
        );
    }

    #[test]
    fn every_variant_renders_a_non_empty_message() {
        let variants = [
            EnvelopeSecurityError::MissingSignature,
            EnvelopeSecurityError::MalformedSecurityHeader {
                header: "x-hexeract-signature",
            },
            EnvelopeSecurityError::UnsupportedAlgorithm,
            EnvelopeSecurityError::UnknownKey,
            EnvelopeSecurityError::RevokedKey,
            EnvelopeSecurityError::AudienceMismatch,
            EnvelopeSecurityError::DestinationMismatch,
            EnvelopeSecurityError::SignatureMismatch,
            EnvelopeSecurityError::MissingRequiredField {
                field: "published_at",
            },
            EnvelopeSecurityError::FieldTooLarge,
            EnvelopeSecurityError::InvalidIdentity {
                kind: IdentityKind::Issuer,
            },
        ];

        for variant in variants {
            assert!(!variant.to_string().is_empty());
        }
    }
}
