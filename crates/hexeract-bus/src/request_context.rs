use std::time::Duration;

use hexeract_core::{HandlerContext, RequestId};

use crate::deadline::LocalDeadline;

/// What a request handler knows about the call it is serving.
///
/// Built by the framework and handed to [`crate::RequestHandler::handle`];
/// application code never constructs one except in its own unit tests,
/// through [`RequestContext::new`].
///
/// Derives `Debug` because every field does today. The day a field carrying
/// a secret is added (`#444`'s authenticated principal is the expected
/// first one), it must not fall into the derived output unexamined: switch
/// to a manual `impl Debug` that redacts that field instead.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RequestContext<'a> {
    /// Identity of this exact call, distinct from the causal correlation.
    pub request_id: RequestId,
    /// Protocol version carried by the request that reached this handler.
    ///
    /// The responder rejects any request whose version does not match
    /// [`crate::PROTOCOL_VERSION`] before the handler ever runs, so when
    /// built by [`crate::RepliedHandler`] this field always holds that
    /// single value. It exists as the vehicle for a future multi-version
    /// negotiation, not as a value this handler is meant to branch on.
    pub protocol_version: u32,
    /// Absolute deadline the caller attached to this call, anchored on the
    /// local monotonic clock, or `None` when the caller set none.
    ///
    /// Reaching the deadline does not interrupt a running handler. A handler
    /// doing long or segmented work is expected to consult
    /// [`RequestContext::remaining`] itself and stop early when it chooses
    /// to; the framework only refuses work before dispatch and suppresses a
    /// reply nobody awaits.
    pub deadline: Option<LocalDeadline>,
    /// Local dispatch context: correlation, cancellation, span.
    pub handler: &'a HandlerContext,
}

impl<'a> RequestContext<'a> {
    /// Builds a context from its mandatory core: the call identity, the
    /// negotiated protocol version, and the local dispatch context.
    ///
    /// This constructor's parameter list is the obligatory core of
    /// `RequestContext`, not an exhaustive list of its fields: a future
    /// field (a deadline, an authenticated principal) arrives through a
    /// `with_*` method on the built value, never through an added
    /// parameter here. Growing `new` would break every caller that
    /// unit-tests its handler through this constructor, which is exactly
    /// the population it exists to serve; `#[non_exhaustive]` on the
    /// struct is what keeps that door open for `with_*` instead.
    ///
    /// Positional rather than a builder: `RequestId`, `u32` and
    /// `&HandlerContext` are pairwise distinct types, so no argument can be
    /// swapped for another without a compile error.
    #[must_use]
    pub fn new(request_id: RequestId, protocol_version: u32, handler: &'a HandlerContext) -> Self {
        Self {
            request_id,
            protocol_version,
            deadline: None,
            handler,
        }
    }

    /// Attaches the caller's deadline to a context.
    ///
    /// Separate from [`RequestContext::new`] so that adding it breaks no
    /// existing caller, in particular the application unit tests that build
    /// a context to exercise their own handler.
    #[must_use]
    pub fn with_deadline(mut self, deadline: LocalDeadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Time left before the caller's deadline, or `None` when no deadline
    /// was set or it has already passed.
    ///
    /// Recomputed on every call rather than frozen at dispatch, so a handler
    /// consulting it after thirty seconds of work sees thirty seconds less.
    #[must_use]
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline?.remaining()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hexeract_core::{CorrelationId, MessageId};

    use crate::deadline::LocalDeadline;

    use super::*;

    #[test]
    fn new_carries_the_three_arguments_it_was_given() {
        let request_id = RequestId::new();
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        let ctx = RequestContext::new(request_id, 7, &handler_ctx);

        assert_eq!(ctx.request_id, request_id);
        assert_eq!(ctx.protocol_version, 7);
        assert_eq!(ctx.handler.correlation_id, handler_ctx.correlation_id);
    }

    #[test]
    fn a_context_built_without_a_deadline_reports_no_remaining_time() {
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        let ctx = RequestContext::new(RequestId::new(), 1, &handler_ctx);

        assert_eq!(ctx.deadline, None);
        assert_eq!(ctx.remaining(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_time_shrinks_as_the_handler_works() {
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let ctx = RequestContext::new(RequestId::new(), 1, &handler_ctx)
            .with_deadline(LocalDeadline::after(Duration::from_secs(10)));

        tokio::time::advance(Duration::from_secs(7)).await;

        assert_eq!(ctx.remaining(), Some(Duration::from_secs(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_time_is_absent_once_the_deadline_has_passed() {
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let ctx = RequestContext::new(RequestId::new(), 1, &handler_ctx)
            .with_deadline(LocalDeadline::after(Duration::from_secs(10)));

        tokio::time::advance(Duration::from_secs(11)).await;

        assert_eq!(ctx.remaining(), None);
    }
}
