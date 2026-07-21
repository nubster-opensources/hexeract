use hexeract_core::HandlerContext;

use crate::{BusError, Request};

/// Responder-side handler that produces a typed reply for a [`Request`].
///
/// Symmetric to [`crate::Handler`] but, instead of a side effect, it returns
/// the reply value the framework publishes back to the caller.
#[trait_variant::make(Send)]
pub trait RequestHandler<R: Request>: Send + Sync + 'static {
    /// Handler-defined error type, convertible into [`BusError`] and encoded
    /// into the error reply sent to the caller.
    type Error: Into<BusError> + Send + Sync + 'static;

    /// Handle `request` and produce its reply.
    async fn handle(&self, request: R, ctx: &HandlerContext) -> Result<R::Reply, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::{CorrelationId, MessageId};
    use serde::{Deserialize, Serialize};

    use crate::Message;

    #[derive(Debug, Serialize, Deserialize)]
    struct Ping {
        seq: u64,
    }
    impl Message for Ping {
        const MESSAGE_TYPE: &'static str = "tests.ping";
    }
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Pong {
        seq: u64,
    }
    impl Message for Pong {
        const MESSAGE_TYPE: &'static str = "tests.pong";
    }
    impl Request for Ping {
        type Reply = Pong;
    }

    struct Echo;
    impl RequestHandler<Ping> for Echo {
        type Error = BusError;
        async fn handle(&self, request: Ping, _ctx: &HandlerContext) -> Result<Pong, BusError> {
            Ok(Pong { seq: request.seq })
        }
    }

    #[tokio::test]
    async fn handler_returns_typed_reply() {
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let pong = Echo.handle(Ping { seq: 5 }, &ctx).await.unwrap();
        assert_eq!(pong, Pong { seq: 5 });
    }
}
