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
/// No variant ever renders key material, a signature, a payload or a header
/// value: a rejection is diagnosed from identifiers and reason codes alone.
///
/// [`Debug`] is written by hand rather than derived, because the derived
/// implementation would unfold the cause boxed inside
/// [`EnvelopeSecurityError::KeySource`], and that cause routinely names a
/// vault endpoint, a key file path or a token. A caller that deliberately
/// wants the cause reaches it through [`std::error::Error::source`], which
/// makes the disclosure a decision rather than a side effect of logging with
/// `?error`.
#[derive(Error)]
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

    /// A configuration value is present but not usable.
    #[error("configuration value {field} is not usable")]
    InvalidConfiguration {
        /// Name of the offending setting.
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

impl std::fmt::Debug for EnvelopeSecurityError {
    /// Renders the variant without ever unfolding a boxed cause.
    ///
    /// The match below is exhaustive on purpose and carries no wildcard arm:
    /// a variant added later stops compiling here until someone decides what
    /// it is allowed to disclose.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSignature => formatter.write_str("MissingSignature"),
            Self::MalformedSecurityHeader { header } => formatter
                .debug_struct("MalformedSecurityHeader")
                .field("header", header)
                .finish(),
            Self::UnsupportedAlgorithm => formatter.write_str("UnsupportedAlgorithm"),
            Self::UnknownKey => formatter.write_str("UnknownKey"),
            Self::RevokedKey => formatter.write_str("RevokedKey"),
            Self::AudienceMismatch => formatter.write_str("AudienceMismatch"),
            Self::DestinationMismatch => formatter.write_str("DestinationMismatch"),
            Self::SignatureMismatch => formatter.write_str("SignatureMismatch"),
            Self::MissingRequiredField { field } => formatter
                .debug_struct("MissingRequiredField")
                .field("field", field)
                .finish(),
            Self::InvalidConfiguration { field } => formatter
                .debug_struct("InvalidConfiguration")
                .field("field", field)
                .finish(),
            Self::FieldTooLarge => formatter.write_str("FieldTooLarge"),
            Self::InvalidIdentity { kind } => formatter
                .debug_struct("InvalidIdentity")
                .field("kind", kind)
                .finish(),
            Self::KeySource(_) => formatter.write_str("KeySource(..)"),
        }
    }
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
    fn the_key_source_error_debug_hides_its_source() {
        let inner = std::io::Error::other("SUPER_SECRET_KEY_MATERIAL");
        let error = EnvelopeSecurityError::KeySource(Box::new(inner));

        let rendered = format!("{error:?}");

        assert!(
            !rendered.contains("SUPER_SECRET_KEY_MATERIAL"),
            "rendered as {rendered}"
        );
    }

    #[test]
    fn the_key_source_error_debug_still_names_the_variant() {
        let inner = std::io::Error::other("unreachable vault");
        let error = EnvelopeSecurityError::KeySource(Box::new(inner));

        let rendered = format!("{error:?}");

        assert!(rendered.contains("KeySource"), "rendered as {rendered}");
    }

    #[test]
    fn the_key_source_cause_stays_reachable_through_source() {
        use std::error::Error;

        let inner = std::io::Error::other("SUPER_SECRET_KEY_MATERIAL");
        let error = EnvelopeSecurityError::KeySource(Box::new(inner));

        let cause = error.source().map(ToString::to_string);

        assert_eq!(cause.as_deref(), Some("SUPER_SECRET_KEY_MATERIAL"));
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
            EnvelopeSecurityError::InvalidConfiguration {
                field: "key_refresh_interval",
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
