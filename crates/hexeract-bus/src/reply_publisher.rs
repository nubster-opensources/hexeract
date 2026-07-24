//! Publication of a reply, isolated from application routing.
//!
//! A responder never publishes a reply through the transport it uses for
//! application traffic: a caller-supplied `reply_to` would then be routed by
//! application bindings. This trait is the only way to emit a reply, and its
//! backends target the AMQP default exchange.

use crate::reply_destination::{ReplyDestination, ReplyDestinationError};
use crate::{BoxFuture, BusEnvelope, BusError};

/// Publishes replies to validated destinations.
pub trait ReplyPublisher: Send + Sync + 'static {
    /// Publish `envelope` to `destination`.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if the broker is unreachable, or
    /// [`BusError::Transport`] if the broker rejected the publish.
    fn publish_reply<'a>(
        &'a self,
        destination: &'a ReplyDestination,
        envelope: &'a BusEnvelope,
    ) -> BoxFuture<'a, Result<(), BusError>>;

    /// Validate a caller-supplied `reply_to` against this backend's policy.
    ///
    /// The default implementation applies only the transport-neutral rules.
    /// A backend whose reply inboxes live in a reserved namespace overrides
    /// this to reject anything outside it.
    ///
    /// # Errors
    ///
    /// Returns the [`ReplyDestinationError`] describing the rule violated.
    fn accept_destination(&self, raw: &str) -> Result<ReplyDestination, ReplyDestinationError> {
        ReplyDestination::parse(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NeutralPublisher;

    impl ReplyPublisher for NeutralPublisher {
        fn publish_reply<'a>(
            &'a self,
            _destination: &'a ReplyDestination,
            _envelope: &'a BusEnvelope,
        ) -> BoxFuture<'a, Result<(), BusError>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn the_default_policy_applies_only_the_neutral_rules() {
        // A core publisher with no backend override accepts any well-formed
        // name: the namespace policy belongs to the backend, not the core.
        assert!(NeutralPublisher.accept_destination("orders.inbox").is_ok());
        assert_eq!(
            NeutralPublisher.accept_destination(""),
            Err(ReplyDestinationError::Empty)
        );
    }
}
