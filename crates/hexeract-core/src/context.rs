use tokio_util::sync::CancellationToken;

use crate::authentication::PublisherAuthentication;
use crate::ids::{CorrelationId, MessageId};

/// Contextual information injected into every handler invocation.
///
/// The context carries the identifiers of the in-flight message, a
/// [`CancellationToken`] for cooperative cancellation, and the active
/// [`tracing::Span`] for distributed tracing propagation.
///
/// This structure is marked as `#[non_exhaustive]` to allow adding new
/// fields without breaking existing code. Direct struct construction
/// is not possible from outside this crate; use [`HandlerContext::new`]
/// and builder methods like [`HandlerContext::with_authentication`] instead.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HandlerContext {
    /// Unique identifier of this specific message instance.
    pub message_id: MessageId,
    /// Identifier linking all messages in the same causal chain.
    pub correlation_id: CorrelationId,
    /// Token that is cancelled when the dispatch is aborted or timed out.
    pub cancellation: CancellationToken,
    /// Active tracing span at the time of dispatch.
    pub span: tracing::Span,
    /// What the transport established about the publisher of this message.
    ///
    /// `NotEnforced` on a context the framework did not fill in, which
    /// includes every context an application builds in its own unit tests.
    pub authentication: PublisherAuthentication,
}

impl HandlerContext {
    /// Creates a new context for the given message identifiers.
    ///
    /// The [`CancellationToken`] is fresh (not yet cancelled) and the span is
    /// captured from the current tracing context. The authentication is
    /// initialized to [`PublisherAuthentication::NotEnforced`].
    #[must_use]
    pub fn new(message_id: MessageId, correlation_id: CorrelationId) -> Self {
        Self {
            message_id,
            correlation_id,
            cancellation: CancellationToken::new(),
            span: tracing::Span::current(),
            authentication: PublisherAuthentication::NotEnforced,
        }
    }

    /// Overrides the tracing span. Useful when the caller manages span
    /// lifecycle explicitly.
    #[must_use]
    pub fn with_span(mut self, span: tracing::Span) -> Self {
        self.span = span;
        self
    }

    /// Overrides the cancellation token. Use this to share a parent token
    /// with the dispatch so that cancelling the parent propagates here.
    #[must_use]
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = token;
        self
    }

    /// Attaches what the transport established about the publisher.
    ///
    /// Separate from [`HandlerContext::new`] so that adding it breaks no
    /// existing caller. Only the code that has just verified a signature has
    /// any business calling this on the dispatch path.
    #[must_use]
    pub fn with_authentication(mut self, authentication: PublisherAuthentication) -> Self {
        self.authentication = authentication;
        self
    }

    /// Returns `true` if the cancellation token has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::{PublisherAuthentication, PublisherIdentity};

    #[test]
    fn new_context_is_not_cancelled() {
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        assert!(!ctx.is_cancelled());
    }

    #[test]
    fn cancellation_propagates_from_parent() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let ctx =
            HandlerContext::new(MessageId::new(), CorrelationId::new()).with_cancellation(child);

        assert!(!ctx.is_cancelled());
        parent.cancel();
        assert!(ctx.is_cancelled());
    }

    #[test]
    fn context_is_clone() {
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let cloned = ctx.clone();
        assert_eq!(ctx.message_id, cloned.message_id);
        assert_eq!(ctx.correlation_id, cloned.correlation_id);
    }

    #[test]
    fn a_fresh_context_enforces_nothing() {
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        assert_eq!(ctx.authentication, PublisherAuthentication::NotEnforced);
    }

    #[test]
    fn with_authentication_carries_the_established_identity() {
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new()).with_authentication(
            PublisherAuthentication::Authenticated(PublisherIdentity::from_verified_issuer(
                "billing-service",
            )),
        );
        match &ctx.authentication {
            PublisherAuthentication::Authenticated(identity) => {
                assert_eq!(identity.issuer(), "billing-service");
            }
            other => panic!("expected an authenticated publisher, got {other:?}"),
        }
    }

    #[test]
    fn with_authentication_leaves_the_identifiers_alone() {
        let message_id = MessageId::new();
        let correlation_id = CorrelationId::new();
        let ctx = HandlerContext::new(message_id, correlation_id)
            .with_authentication(PublisherAuthentication::WaivedUnsigned);
        assert_eq!(ctx.message_id, message_id);
        assert_eq!(ctx.correlation_id, correlation_id);
        assert!(!ctx.is_cancelled());
    }
}
