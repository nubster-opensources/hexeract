//! What an operator counts when a reply delivery is refused.

/// What an operator counts. One value per counter, named once so #441 can
/// use these as metric labels without reopening the vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplyRejectionKind {
    /// No envelope could be reconstructed from the delivery.
    Undecodable,
    /// Identity absent, unparsable, or never seen by this process.
    Orphaned,
    /// Signature refused before any resolution could happen.
    Unauthenticated,
    /// Identity known, shape refused. See [`crate::ReplyRejection`] for
    /// which rule.
    Invalid,
    /// Identity already resolved by a valid reply.
    Duplicate,
    /// Identity abandoned (timeout, drain) before this delivery arrived.
    Late,
}
