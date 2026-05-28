//! Tracing middleware: opens a span around every dispatch and emits
//! structured events on entry, completion and failure.

use std::time::Instant;

use hexeract_core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};
use tracing::Level;

/// Middleware that opens a [`tracing::Span`] around every dispatch.
///
/// The span records `type_name`, `message_id` and `correlation_id` from
/// the envelope. A structured event is emitted on entry, on success with
/// the elapsed duration in milliseconds, and on failure with the error
/// converted to a string at [`Level::ERROR`] regardless of the configured
/// level.
///
/// The level defaults to [`Level::INFO`] and can be tuned through
/// [`Self::with_level`].
pub struct TracingMiddleware {
    level: Level,
}

impl TracingMiddleware {
    /// Builds a middleware at [`Level::INFO`].
    #[must_use]
    pub fn new() -> Self {
        Self { level: Level::INFO }
    }

    /// Builds a middleware at the requested level.
    #[must_use]
    pub fn with_level(level: Level) -> Self {
        Self { level }
    }
}

impl Default for TracingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

impl Middleware for TracingMiddleware {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError> {
        // The `tracing` macros require compile-time levels, so we dispatch
        // the runtime-configurable level to one of five monomorphic helpers.
        match self.level {
            Level::TRACE => run_at_trace(envelope, ctx, next).await,
            Level::DEBUG => run_at_debug(envelope, ctx, next).await,
            Level::INFO => run_at_info(envelope, ctx, next).await,
            Level::WARN => run_at_warn(envelope, ctx, next).await,
            Level::ERROR => run_at_error(envelope, ctx, next).await,
        }
    }
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "elapsed_ms above u64::MAX is not a realistic dispatch duration"
)]
fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

fn log_failure(elapsed_ms: u64, err: &HexeractError) {
    tracing::event!(Level::ERROR, elapsed_ms, error = %err, "failed");
}

macro_rules! impl_run_at {
    ($name:ident, $span_macro:ident, $event_macro:ident) => {
        async fn $name(
            envelope: &MessageEnvelope,
            ctx: &HandlerContext,
            next: Next,
        ) -> Result<BoxOutput, HexeractError> {
            let span = tracing::$span_macro!(
                "hexeract.dispatch",
                type_name = envelope.type_name(),
                message_id = %envelope.message_id(),
                correlation_id = %envelope.correlation_id(),
            );
            let _enter = span.enter();
            tracing::$event_macro!("entering");
            let started = Instant::now();
            let result = next.run(envelope, ctx).await;
            let elapsed_ms = elapsed_ms(started);
            match &result {
                Ok(_) => tracing::$event_macro!(elapsed_ms, "completed"),
                Err(err) => log_failure(elapsed_ms, err),
            }
            result
        }
    };
}

impl_run_at!(run_at_trace, trace_span, trace);
impl_run_at!(run_at_debug, debug_span, debug);
impl_run_at!(run_at_info, info_span, info);
impl_run_at!(run_at_warn, warn_span, warn);
impl_run_at!(run_at_error, error_span, error);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hexeract_core::{
        BoxOutput, CorrelationId, HandlerContext, HexeractError, MessageEnvelope, MessageId,
        Middleware, Next, Terminal,
    };
    use tracing_test::traced_test;

    use super::TracingMiddleware;

    struct StaticTerminal;

    impl Terminal for StaticTerminal {
        fn dispatch<'a>(
            &'a self,
            _envelope: &'a MessageEnvelope,
            _ctx: &'a HandlerContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<BoxOutput, HexeractError>> + Send + 'a>,
        > {
            Box::pin(async move { Ok(Box::new(42_i32) as BoxOutput) })
        }
    }

    struct FailingTerminal;

    impl Terminal for FailingTerminal {
        fn dispatch<'a>(
            &'a self,
            _envelope: &'a MessageEnvelope,
            _ctx: &'a HandlerContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<BoxOutput, HexeractError>> + Send + 'a>,
        > {
            Box::pin(async move {
                Err(HexeractError::Dispatch(
                    "handler refused to play".to_string(),
                ))
            })
        }
    }

    struct Probe;
    impl hexeract_core::Command for Probe {
        type Output = i32;
    }

    fn fresh_env() -> MessageEnvelope {
        MessageEnvelope::for_command::<Probe>(MessageId::new(), CorrelationId::new())
    }

    fn fresh_ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
    }

    async fn run_through(
        middleware: TracingMiddleware,
        terminal: Arc<dyn Terminal>,
    ) -> Result<BoxOutput, HexeractError> {
        let env = fresh_env();
        let ctx = fresh_ctx();
        let next = Next::new(Vec::new(), terminal);
        middleware.execute(&env, &ctx, next).await
    }

    #[tokio::test]
    #[traced_test]
    async fn emits_entering_and_completed_events_on_success() {
        let _ = run_through(TracingMiddleware::new(), Arc::new(StaticTerminal))
            .await
            .expect("dispatch must succeed");
        assert!(logs_contain("entering"));
        assert!(logs_contain("completed"));
        assert!(logs_contain("elapsed_ms"));
    }

    #[tokio::test]
    #[traced_test]
    async fn emits_failed_event_on_error() {
        let _ = run_through(TracingMiddleware::new(), Arc::new(FailingTerminal))
            .await
            .expect_err("dispatch must fail");
        assert!(logs_contain("failed"));
        assert!(logs_contain("handler refused to play"));
    }

    #[tokio::test]
    #[traced_test]
    async fn records_envelope_fields_on_span() {
        let _ = run_through(TracingMiddleware::new(), Arc::new(StaticTerminal))
            .await
            .expect("dispatch must succeed");
        assert!(logs_contain("type_name"));
        assert!(logs_contain("Probe"));
        assert!(logs_contain("correlation_id"));
    }

    #[tokio::test]
    async fn propagates_handler_output_unchanged() {
        let output = run_through(TracingMiddleware::new(), Arc::new(StaticTerminal))
            .await
            .expect("dispatch must succeed");
        assert_eq!(*output.downcast::<i32>().unwrap(), 42);
    }

    #[tokio::test]
    #[traced_test]
    async fn with_level_changes_the_emitted_level() {
        let _ = run_through(
            TracingMiddleware::with_level(tracing::Level::DEBUG),
            Arc::new(StaticTerminal),
        )
        .await
        .expect("dispatch must succeed");
        assert!(logs_contain("entering"));
    }
}
