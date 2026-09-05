//! Identities carried by the envelope security protocol.
//!
//! All three identity types share one validation rule: a non-empty run of
//! printable ASCII characters, no longer than
//! [`MAX_IDENTITY_BYTES`](crate::envelope_security::identity::MAX_IDENTITY_BYTES).
//! The rule is deliberately narrow. These values travel in AMQP headers,
//! appear in logs and in traces, and are compared byte by byte during
//! verification, so anything that could be rendered ambiguously is refused at
//! construction.

use super::error::{EnvelopeSecurityError, IdentityKind};

/// Maximum byte length accepted for any security identity value.
pub const MAX_IDENTITY_BYTES: usize = 128;

fn validate(value: &str, kind: IdentityKind) -> Result<(), EnvelopeSecurityError> {
    let is_valid = !value.is_empty()
        && value.len() <= MAX_IDENTITY_BYTES
        && value.bytes().all(|byte| byte.is_ascii_graphic());

    if is_valid {
        Ok(())
    } else {
        Err(EnvelopeSecurityError::InvalidIdentity { kind })
    }
}

macro_rules! identity_type {
    ($name:ident, $kind:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            /// Build the identity from `value`.
            ///
            /// # Errors
            ///
            /// Returns [`EnvelopeSecurityError::InvalidIdentity`] when `value`
            /// is empty, exceeds [`MAX_IDENTITY_BYTES`] bytes, or contains a
            /// byte outside the printable ASCII range.
            pub fn new(value: impl Into<String>) -> Result<Self, EnvelopeSecurityError> {
                let value = value.into();
                validate(&value, $kind)?;
                Ok(Self(value))
            }

            /// Borrow the identity as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

identity_type!(
    Issuer,
    IdentityKind::Issuer,
    "Authenticated publisher of an envelope."
);
identity_type!(
    Audience,
    IdentityKind::Audience,
    "Intended recipient of an envelope."
);
identity_type!(
    KeyId,
    IdentityKind::KeyId,
    "Identifier of a signing key within an issuer's key set."
);

/// Signature algorithm carried by an envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SignatureAlgorithm {
    /// Ed25519, as specified by RFC 8032.
    Ed25519,
}

impl SignatureAlgorithm {
    /// Token this algorithm is announced by on the wire.
    #[must_use]
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::Ed25519 => "ed25519",
        }
    }

    /// Parse a wire token into an algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeSecurityError::UnsupportedAlgorithm`] for any token
    /// this build does not implement.
    pub fn from_wire_str(token: &str) -> Result<Self, EnvelopeSecurityError> {
        match token {
            "ed25519" => Ok(Self::Ed25519),
            _ => Err(EnvelopeSecurityError::UnsupportedAlgorithm),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_well_formed_issuer_is_accepted() {
        let issuer = Issuer::new("billing-service").expect("valid issuer");

        assert_eq!(issuer.as_str(), "billing-service");
    }

    #[test]
    fn an_empty_identity_is_rejected() {
        let error = Issuer::new("").expect_err("empty issuer");

        assert!(matches!(
            error,
            EnvelopeSecurityError::InvalidIdentity {
                kind: IdentityKind::Issuer
            }
        ));
    }

    #[test]
    fn an_identity_longer_than_the_maximum_is_rejected() {
        let oversized = "a".repeat(MAX_IDENTITY_BYTES + 1);

        let error = Audience::new(oversized).expect_err("oversized audience");

        assert!(matches!(
            error,
            EnvelopeSecurityError::InvalidIdentity {
                kind: IdentityKind::Audience
            }
        ));
    }

    #[test]
    fn an_identity_carrying_a_control_character_is_rejected() {
        let error = KeyId::new("key\u{7}one").expect_err("control character");

        assert!(matches!(
            error,
            EnvelopeSecurityError::InvalidIdentity {
                kind: IdentityKind::KeyId
            }
        ));
    }

    #[test]
    fn an_identity_carrying_a_space_is_rejected() {
        let error = Issuer::new("billing service").expect_err("space");

        assert!(matches!(
            error,
            EnvelopeSecurityError::InvalidIdentity {
                kind: IdentityKind::Issuer
            }
        ));
    }

    #[test]
    fn the_algorithm_round_trips_through_its_wire_token() {
        let parsed = SignatureAlgorithm::from_wire_str(SignatureAlgorithm::Ed25519.as_wire_str())
            .expect("known algorithm");

        assert_eq!(parsed, SignatureAlgorithm::Ed25519);
    }

    #[test]
    fn an_unknown_algorithm_token_is_rejected() {
        let error = SignatureAlgorithm::from_wire_str("rsa-pkcs1").expect_err("unknown token");

        assert!(matches!(error, EnvelopeSecurityError::UnsupportedAlgorithm));
    }
}
