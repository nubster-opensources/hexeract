use hexeract_core::{HandlerContext, RequestId};

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
    /// [`crate::PROTOCOL_VERSION`] before the handler ever runs, so today
    /// this field always holds that single value. It exists as the vehicle
    /// for a future multi-version negotiation, not as a value this handler
    /// is meant to branch on.
    pub protocol_version: u32,
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
            handler,
        }
    }
}

#[cfg(test)]
mod tests {
    use hexeract_core::{CorrelationId, MessageId};

    use super::*;

    #[test]
    fn new_carries_the_three_arguments_it_was_given() {
        let request_id = RequestId::new();
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        let ctx = RequestContext::new(request_id, 1, &handler_ctx);

        assert_eq!(ctx.request_id, request_id);
        assert_eq!(ctx.protocol_version, 1);
        assert_eq!(ctx.handler.correlation_id, handler_ctx.correlation_id);
    }
}
