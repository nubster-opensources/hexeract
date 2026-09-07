//! Publisher identity established by a successful verification.

use hexeract_core::PublisherIdentity;

use super::identity::{Audience, Issuer, KeyId, SignatureAlgorithm};

/// Identity of the publisher of a verified envelope.
///
/// This type has no public constructor on purpose. A value can only be
/// produced by a successful verification, so holding one is proof that a
/// signature was checked against a known key. Never derive an identity from
/// raw header values: an unverified header says only what its writer chose to
/// write.
///
/// # Examples
///
/// The identity is readable by any consumer of the crate:
///
/// ```
/// use hexeract_bus::envelope_security::principal::VerifiedPrincipal;
///
/// fn issuer_of(principal: &VerifiedPrincipal) -> &str {
///     principal.issuer().as_str()
/// }
/// ```
///
/// Building one from outside the crate does not compile, which is what makes
/// holding a value proof that a signature was checked:
///
/// ```compile_fail
/// use hexeract_bus::envelope_security::identity::{
///     Audience, Issuer, KeyId, SignatureAlgorithm,
/// };
/// use hexeract_bus::envelope_security::principal::VerifiedPrincipal;
///
/// let principal = VerifiedPrincipal::new(
///     Issuer::new("billing-service").unwrap(),
///     Audience::new("ledger-service").unwrap(),
///     KeyId::new("2026-09").unwrap(),
///     SignatureAlgorithm::Ed25519,
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPrincipal {
    issuer: Issuer,
    audience: Audience,
    key_id: KeyId,
    algorithm: SignatureAlgorithm,
}

impl VerifiedPrincipal {
    /// Build a verified principal from an identity already established by
    /// verification. Only the verifier is allowed to call this: it is the
    /// sole caller in the crate that has checked a signature before reaching
    /// for it.
    pub(crate) fn new(
        issuer: Issuer,
        audience: Audience,
        key_id: KeyId,
        algorithm: SignatureAlgorithm,
    ) -> Self {
        Self {
            issuer,
            audience,
            key_id,
            algorithm,
        }
    }

    /// The authenticated publisher.
    #[must_use]
    pub fn issuer(&self) -> &Issuer {
        &self.issuer
    }

    /// The audience the envelope was signed for.
    #[must_use]
    pub fn audience(&self) -> &Audience {
        &self.audience
    }

    /// The key that signed the envelope.
    #[must_use]
    pub fn key_id(&self) -> &KeyId {
        &self.key_id
    }

    /// The algorithm the signature was produced with.
    #[must_use]
    pub fn algorithm(&self) -> SignatureAlgorithm {
        self.algorithm
    }
}

/// Narrow a verified principal down to what a handler needs to authorize.
///
/// Only the issuer crosses into `hexeract-core`. The audience is already
/// checked by the transport before a handler exists, and the key identifier
/// and algorithm are signature details the business layer has no reason to
/// know; the transport logs them.
impl From<&VerifiedPrincipal> for PublisherIdentity {
    fn from(principal: &VerifiedPrincipal) -> Self {
        Self::from_verified_issuer(principal.issuer().as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal() -> VerifiedPrincipal {
        VerifiedPrincipal::new(
            Issuer::new("billing-service").expect("valid issuer"),
            Audience::new("ledger-service").expect("valid audience"),
            KeyId::new("2026-09").expect("valid key id"),
            SignatureAlgorithm::Ed25519,
        )
    }

    #[test]
    fn the_principal_exposes_the_identity_it_was_built_from() {
        let principal = principal();

        assert_eq!(principal.issuer().as_str(), "billing-service");
        assert_eq!(principal.audience().as_str(), "ledger-service");
        assert_eq!(principal.key_id().as_str(), "2026-09");
        assert_eq!(principal.algorithm(), SignatureAlgorithm::Ed25519);
    }

    #[test]
    fn the_translation_carries_the_issuer() {
        let principal = VerifiedPrincipal::new(
            Issuer::new("billing-service").expect("valid issuer"),
            Audience::new("ledger-service").expect("valid audience"),
            KeyId::new("2026-09").expect("valid key id"),
            SignatureAlgorithm::Ed25519,
        );
        let identity = PublisherIdentity::from(&principal);
        assert_eq!(identity.issuer(), "billing-service");
    }

    #[test]
    fn the_translation_follows_the_principal_it_is_given() {
        let billing = VerifiedPrincipal::new(
            Issuer::new("billing-service").expect("valid issuer"),
            Audience::new("ledger-service").expect("valid audience"),
            KeyId::new("2026-09").expect("valid key id"),
            SignatureAlgorithm::Ed25519,
        );
        let shipping = VerifiedPrincipal::new(
            Issuer::new("shipping-service").expect("valid issuer"),
            Audience::new("ledger-service").expect("valid audience"),
            KeyId::new("2026-09").expect("valid key id"),
            SignatureAlgorithm::Ed25519,
        );
        assert_eq!(
            PublisherIdentity::from(&billing).issuer(),
            "billing-service"
        );
        assert_eq!(
            PublisherIdentity::from(&shipping).issuer(),
            "shipping-service"
        );
    }
}
