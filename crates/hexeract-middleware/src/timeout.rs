//! Timeout middleware: aborts the dispatch with [`HexeractError::Timeout`]
//! when the inner pipeline takes longer than the configured duration.

use std::time::Duration;

use hexeract_core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};

/// Middleware that aborts a dispatch when the inner pipeline takes longer
/// than the configured duration.
///
/// On expiration, returns [`HexeractError::Timeout`] carrying the envelope
/// type name and the configured duration. Cancellation honors tokio's
/// cooperative semantics: the inner future is dropped at the next await
/// point, which lets `Drop` implementations run their cleanup.
pub struct TimeoutMiddleware {
    duration: Duration,
}

impl TimeoutMiddleware {
    /// Builds a middleware that aborts dispatches taking longer than
    /// `duration`. A [`Duration::ZERO`] polls the inner future once and
    /// then returns [`HexeractError::Timeout`] if it has not completed.
    #[must_use]
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl Middleware for TimeoutMiddleware {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError> {
        match tokio::time::timeout(self.duration, next.run(envelope, ctx)).await {
            Ok(result) => result,
            Err(_elapsed) => {
                ctx.cancellation.cancel();
                Err(HexeractError::timeout(envelope.type_name(), self.duration))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use hexeract_core::{
        BoxOutput, CorrelationId, HandlerContext, HexeractError, MessageEnvelope, MessageId,
        Middleware, Next, Terminal,
    };

    use super::TimeoutMiddleware;

    struct SlowTerminal {
        delay: Duration,
    }

    impl Terminal for SlowTerminal {
        fn dispatch<'a>(
            &'a self,
            _envelope: &'a MessageEnvelope,
            _ctx: &'a HandlerContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<BoxOutput, HexeractError>> + Send + 'a>,
        > {
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(Box::new(7_i32) as BoxOutput)
            })
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
                    "handler bailed out early".to_string(),
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
        middleware: TimeoutMiddleware,
        terminal: Arc<dyn Terminal>,
    ) -> Result<BoxOutput, HexeractError> {
        let env = fresh_env();
        let ctx = fresh_ctx();
        let next = Next::new(Vec::new(), terminal);
        middleware.execute(&env, &ctx, next).await
    }

    #[tokio::test(start_paused = true)]
    async fn returns_timeout_error_when_inner_too_slow() {
        let err = run_through(
            TimeoutMiddleware::new(Duration::from_millis(50)),
            Arc::new(SlowTerminal {
                delay: Duration::from_millis(500),
            }),
        )
        .await
        .expect_err("dispatch must fail with timeout");

        match err {
            HexeractError::Timeout {
                type_name,
                duration,
                ..
            } => {
                assert!(type_name.ends_with("::Probe"));
                assert_eq!(duration, Duration::from_millis(50));
            }
            other => panic!("expected Timeout variant, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn passes_through_when_inner_fast_enough() {
        let output = run_through(
            TimeoutMiddleware::new(Duration::from_secs(10)),
            Arc::new(SlowTerminal {
                delay: Duration::from_millis(10),
            }),
        )
        .await
        .expect("fast inner must succeed");
        assert_eq!(*output.downcast::<i32>().unwrap(), 7);
    }

    #[tokio::test]
    async fn propagates_inner_error_unchanged() {
        let err = run_through(
            TimeoutMiddleware::new(Duration::from_secs(10)),
            Arc::new(FailingTerminal),
        )
        .await
        .expect_err("inner failure must propagate");
        match err {
            HexeractError::Dispatch(msg) => assert_eq!(msg, "handler bailed out early"),
            other => panic!("expected Dispatch variant, got {other:?}"),
        }
    }

    // RED test for #226: on timeout the context cancellation token must be
    // signalled so that handlers following the documented select! pattern can
    // observe the abort and stop escaped work.
    #[tokio::test(start_paused = true)]
    async fn cancels_context_token_on_timeout() {
        let env = fresh_env();
        let ctx = fresh_ctx();
        let next = Next::new(
            Vec::new(),
            Arc::new(SlowTerminal {
                delay: Duration::from_millis(500),
            }),
        );
        let middleware = TimeoutMiddleware::new(Duration::from_millis(50));
        let result = middleware.execute(&env, &ctx, next).await;
        assert!(
            matches!(result, Err(HexeractError::Timeout { .. })),
            "expected Timeout error"
        );
        assert!(
            ctx.is_cancelled(),
            "cancellation token must be cancelled when the dispatch times out"
        );
    }

    // Cancellation must NOT be triggered when the dispatch completes in time.
    #[tokio::test(start_paused = true)]
    async fn does_not_cancel_context_token_on_success() {
        let env = fresh_env();
        let ctx = fresh_ctx();
        let next = Next::new(
            Vec::new(),
            Arc::new(SlowTerminal {
                delay: Duration::from_millis(10),
            }),
        );
        let middleware = TimeoutMiddleware::new(Duration::from_secs(10));
        let _ = middleware
            .execute(&env, &ctx, next)
            .await
            .expect("must succeed");
        assert!(
            !ctx.is_cancelled(),
            "cancellation token must remain uncancelled when dispatch completes in time"
        );
    }
}
