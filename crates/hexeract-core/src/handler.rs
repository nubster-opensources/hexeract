use crate::command::Command;
use crate::context::HandlerContext;
use crate::error::HexeractError;
use crate::query::Query;

/// Asynchronous handler for a [`Command`].
///
/// Each [`Command`] type has exactly one registered `CommandHandler`. The
/// handler receives an immutable reference to itself, the command value and
/// a [`HandlerContext`] carrying tracing and cancellation information.
///
/// The handler returns the [`Command::Output`] type, or a custom
/// `Self::Error` that converts into [`HexeractError`].
///
/// The trait is decorated with `#[trait_variant::make(Send)]` so that the
/// returned futures are `Send` and can be moved across threads by the tokio
/// scheduler.
#[trait_variant::make(Send)]
pub trait CommandHandler<C: Command>: Send + Sync + 'static {
    /// The handler-defined error type, convertible into [`HexeractError`].
    type Error: Into<HexeractError> + Send + Sync + 'static;

    /// Handles the command and produces its output.
    async fn handle(
        &self,
        command: C,
        ctx: &HandlerContext,
    ) -> Result<C::Output, Self::Error>;
}

/// Asynchronous handler for a [`Query`].
///
/// See [`CommandHandler`] for the equivalent contract; only the input type
/// changes.
#[trait_variant::make(Send)]
pub trait QueryHandler<Q: Query>: Send + Sync + 'static {
    /// The handler-defined error type, convertible into [`HexeractError`].
    type Error: Into<HexeractError> + Send + Sync + 'static;

    /// Handles the query and produces its output.
    async fn handle(
        &self,
        query: Q,
        ctx: &HandlerContext,
    ) -> Result<Q::Output, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{CorrelationId, MessageId};

    struct Ping;
    impl Command for Ping {
        type Output = &'static str;
    }

    #[derive(Debug, thiserror::Error)]
    enum PingError {
        #[error("boom")]
        Boom,
    }

    impl From<PingError> for HexeractError {
        fn from(value: PingError) -> Self {
            Self::handler_failed(value)
        }
    }

    struct PingHandler;
    impl CommandHandler<Ping> for PingHandler {
        type Error = PingError;
        async fn handle(
            &self,
            _command: Ping,
            _ctx: &HandlerContext,
        ) -> Result<&'static str, Self::Error> {
            Ok("pong")
        }
    }

    fn assert_send<T: Send>(_: &T) {}

    #[tokio::test]
    async fn command_handler_returns_output() {
        let handler = PingHandler;
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let result = handler.handle(Ping, &ctx).await;
        assert_eq!(result.unwrap(), "pong");
    }

    #[tokio::test]
    async fn handler_future_is_send() {
        let handler = PingHandler;
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let future = handler.handle(Ping, &ctx);
        assert_send(&future);
        let _ = future.await;
    }

    #[tokio::test]
    async fn handler_runs_in_spawned_task() {
        let handler = PingHandler;
        let result = tokio::spawn(async move {
            let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
            handler.handle(Ping, &ctx).await
        })
        .await
        .expect("task panicked");
        assert_eq!(result.unwrap(), "pong");
    }

    struct FailingHandler;
    impl CommandHandler<Ping> for FailingHandler {
        type Error = PingError;
        async fn handle(
            &self,
            _command: Ping,
            _ctx: &HandlerContext,
        ) -> Result<&'static str, Self::Error> {
            Err(PingError::Boom)
        }
    }

    #[tokio::test]
    async fn handler_error_converts_into_hexeract_error() {
        let handler = FailingHandler;
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let result = handler.handle(Ping, &ctx).await;
        let err: HexeractError = result.unwrap_err().into();
        assert!(matches!(err, HexeractError::HandlerFailed { .. }));
    }
}
