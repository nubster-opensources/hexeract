use tokio_util::sync::CancellationToken;

use crate::ids::{CorrelationId, MessageId};

/// Contextual information injected into every handler invocation.
///
/// The context carries the identifiers of the in-flight message, a
/// [`CancellationToken`] for cooperative cancellation, and the active
/// [`tracing::Span`] for distributed tracing propagation.
#[derive(Debug, Clone)]
pub struct HandlerContext {
    /// Unique identifier of this specific message instance.
    pub message_id: MessageId,
    /// Identifier linking all messages in the same causal chain.
    pub correlation_id: CorrelationId,
    /// Token that is cancelled when the dispatch is aborted or timed out.
    pub cancellation: CancellationToken,
    /// Active tracing span at the time of dispatch.
    pub span: tracing::Span,
}

impl HandlerContext {
    /// Creates a new context for the given message identifiers.
    ///
    /// The [`CancellationToken`] is fresh (not yet cancelled) and the span is
    /// captured from the current tracing context.
    #[must_use]
    pub fn new(message_id: MessageId, correlation_id: CorrelationId) -> Self {
        Self {
            message_id,
            correlation_id,
            cancellation: CancellationToken::new(),
            span: tracing::Span::current(),
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

    /// Returns `true` if the cancellation token has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_context_is_not_cancelled() {
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        assert!(!ctx.is_cancelled());
    }

    #[test]
    fn cancellation_propagates_from_parent() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new())
            .with_cancellation(child);

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
}
