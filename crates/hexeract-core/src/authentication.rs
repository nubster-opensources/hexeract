//! Publisher identity as a handler learns it, independent of any transport.

/// Identity of the publisher of a message, as established by the transport.
///
/// Unlike `hexeract_bus::envelope_security::VerifiedPrincipal`, which cannot be
/// built outside the crate that checks signatures, this type has a public
/// constructor: a crate that verifies signatures lives outside this one and
/// must be able to build a value. **Holding one is therefore not proof that a
/// signature was checked.** The guarantee comes from where the value is set,
/// not from the type: on the dispatch path, the only code that sets it is the
/// code that has just verified a signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublisherIdentity {
    issuer: String,
}

impl PublisherIdentity {
    /// Build an identity from an issuer a verification has just established.
    ///
    /// The name states what the caller asserts. Callers that have not verified
    /// anything are lying to their own readers, not defeating a check.
    #[must_use]
    pub fn from_verified_issuer(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
        }
    }

    /// The authenticated publisher.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }
}

/// What the transport was able to establish about the publisher.
///
/// The three situations are kept apart on purpose. An absent identity does not
/// say the same thing when no security is configured at all and when an
/// operator has explicitly waived signatures during a migration: the first
/// means the question is not asked, the second that it is asked and answered
/// by a deliberate exception.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublisherAuthentication {
    /// No envelope security is configured on this consumer.
    NotEnforced,
    /// Envelope security is configured, and this envelope arrived unsigned
    /// under an explicit waiver.
    WaivedUnsigned,
    /// A signature was checked against a known key.
    Authenticated(PublisherIdentity),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_identity_exposes_the_issuer_it_was_built_from() {
        let identity = PublisherIdentity::from_verified_issuer("billing-service");
        assert_eq!(identity.issuer(), "billing-service");
    }

    #[test]
    fn an_identity_follows_the_issuer_it_is_given() {
        let billing = PublisherIdentity::from_verified_issuer("billing-service");
        let ledger = PublisherIdentity::from_verified_issuer("ledger-service");
        assert_eq!(billing.issuer(), "billing-service");
        assert_eq!(ledger.issuer(), "ledger-service");
        assert_ne!(billing, ledger);
    }

    #[test]
    fn the_three_situations_are_distinct_values() {
        let authenticated = PublisherAuthentication::Authenticated(
            PublisherIdentity::from_verified_issuer("billing-service"),
        );
        assert_ne!(
            PublisherAuthentication::NotEnforced,
            PublisherAuthentication::WaivedUnsigned
        );
        assert_ne!(PublisherAuthentication::NotEnforced, authenticated);
        assert_ne!(PublisherAuthentication::WaivedUnsigned, authenticated);
    }
}
