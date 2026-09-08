//! What a request caller learns about the identity that signed its reply.
//!
//! [`ReplyAuthentication`] is the caller-side mirror of
//! [`hexeract_core::PublisherAuthentication`], the value a handler already
//! reads off its [`hexeract_core::HandlerContext`]. Before this type existed,
//! [`crate::request_registry::RequestRegistry::resolve`] established a
//! [`crate::envelope_security::principal::VerifiedPrincipal`] and threw it
//! away: a caller of [`crate::request_client::RequestClient`] had no way to
//! learn which issuer, if any, actually signed the reply it received.

use hexeract_core::PublisherAuthentication;

use crate::envelope_security::principal::VerifiedPrincipal;

/// What the transport was able to establish about the publisher of a reply.
///
/// Mirrors [`hexeract_core::PublisherAuthentication`] on the request-reply
/// caller side: the three situations are kept apart for the same reason. An
/// absent signature does not mean the same thing when no envelope security is
/// configured at all and when an operator has explicitly waived signatures
/// during a migration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplyAuthentication {
    /// No envelope security is configured on this caller's reply inbox.
    NotEnforced,
    /// Envelope security is configured, and this reply arrived unsigned
    /// under an explicit waiver.
    WaivedUnsigned,
    /// A signature was checked against a known key.
    Authenticated(VerifiedPrincipal),
}

impl ReplyAuthentication {
    /// Build the authentication a verification established.
    ///
    /// `None` can only mean one thing here: envelope security was
    /// configured and the verification it ran, through
    /// [`crate::envelope_security::verifier::EnvelopeVerifier::verify`] (or
    /// a backend's own facade wrapping it, such as
    /// `hexeract-bus-rabbitmq`'s `InboundEnvelopeSecurity::verify`),
    /// completed under an explicit waiver of the signature requirement. It
    /// can never mean that no security was configured at all, because a
    /// caller with none configured never reaches a verification step in the
    /// first place; its transport must report [`Self::NotEnforced`] directly
    /// instead of calling this constructor. Reading `None` as "nothing
    /// enforced" would collapse that distinction and misreport a deliberate
    /// operational exception as the total absence of any check.
    #[must_use]
    pub fn from_verification(principal: Option<VerifiedPrincipal>) -> Self {
        match principal {
            Some(principal) => Self::Authenticated(principal),
            None => Self::WaivedUnsigned,
        }
    }
}

/// Narrow a caller-observed reply authentication down to what
/// `hexeract-core` exposes to application code.
///
/// Only the [`VerifiedPrincipal`] carried by
/// [`ReplyAuthentication::Authenticated`] is translated, and even then only
/// as far as [`hexeract_core::PublisherIdentity`]: the audience is already
/// checked by the transport before this value ever exists, and the key
/// identifier and signature algorithm are details the business layer has no
/// reason to know. Passing the full principal across that boundary would let
/// application code re-derive a verification decision the transport already
/// made correctly, which is exactly the dependency rule this crate's
/// business-facing types exist to prevent: the business layer learns who
/// signed, never how the signature was checked.
impl From<&ReplyAuthentication> for PublisherAuthentication {
    fn from(authentication: &ReplyAuthentication) -> Self {
        match authentication {
            ReplyAuthentication::NotEnforced => Self::NotEnforced,
            ReplyAuthentication::WaivedUnsigned => Self::WaivedUnsigned,
            ReplyAuthentication::Authenticated(principal) => Self::Authenticated(principal.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope_security::identity::{Audience, Issuer, KeyId, SignatureAlgorithm};

    fn principal() -> VerifiedPrincipal {
        VerifiedPrincipal::new(
            Issuer::new("billing-service").expect("valid issuer"),
            Audience::new("ledger-service").expect("valid audience"),
            KeyId::new("2026-09").expect("valid key id"),
            SignatureAlgorithm::Ed25519,
        )
    }

    #[test]
    fn a_verification_that_established_a_principal_is_authenticated() {
        let principal = principal();
        assert_eq!(
            ReplyAuthentication::from_verification(Some(principal.clone())),
            ReplyAuthentication::Authenticated(principal)
        );
    }

    #[test]
    fn a_verification_under_a_waiver_is_waived_unsigned() {
        assert_eq!(
            ReplyAuthentication::from_verification(None),
            ReplyAuthentication::WaivedUnsigned
        );
    }

    #[test]
    fn not_enforced_translates_to_not_enforced() {
        let authentication = PublisherAuthentication::from(&ReplyAuthentication::NotEnforced);
        assert_eq!(authentication, PublisherAuthentication::NotEnforced);
    }

    #[test]
    fn waived_unsigned_translates_to_waived_unsigned() {
        let authentication = PublisherAuthentication::from(&ReplyAuthentication::WaivedUnsigned);
        assert_eq!(authentication, PublisherAuthentication::WaivedUnsigned);
    }

    #[test]
    fn authenticated_translates_to_authenticated_and_preserves_the_issuer() {
        let principal = principal();
        let authentication =
            PublisherAuthentication::from(&ReplyAuthentication::Authenticated(principal));
        match authentication {
            PublisherAuthentication::Authenticated(identity) => {
                assert_eq!(identity.issuer(), "billing-service");
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }
    }
}
