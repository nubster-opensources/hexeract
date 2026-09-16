//! Why a transport refused a delivery before any verdict on its content.

use crate::reply_acceptance::ReplyRejection;

/// Why a transport refused a delivery before any verdict on its content.
///
/// The only rejection a transport may hand to [`crate::RequestRegistry::resolve`].
/// Shape rules are the registry's own business: see [`crate::ReplyRejection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportRefusal {
    /// The signature could not be established against a known key.
    ///
    /// Carries no cause, for the reason documented on
    /// [`crate::ReplyRejection::Unauthenticated`].
    Unauthenticated,
}

impl From<TransportRefusal> for ReplyRejection {
    fn from(refusal: TransportRefusal) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reply_rejection_kind::ReplyRejectionKind;

    #[test]
    fn unauthenticated_refusal_becomes_the_unauthenticated_rejection() {
        let rejection = ReplyRejection::from(TransportRefusal::Unauthenticated);

        assert_eq!(rejection, ReplyRejection::Unauthenticated);
        assert_eq!(rejection.kind(), ReplyRejectionKind::Unauthenticated);
    }
}
