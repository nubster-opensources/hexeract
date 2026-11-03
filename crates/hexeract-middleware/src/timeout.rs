//! Timeout middleware: aborts the dispatch with [`HexeractError::Timeout`]
//! when the inner pipeline takes longer than the configured duration.

use std::time::Duration;

use hexeract_core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};

/// Default grace period a timed-out handler gets to observe its cancellation.
pub const DEFAULT_TIMEOUT_GRACE: Duration = Duration::from_secs(1);

/// Middleware that aborts a dispatch when the inner pipeline takes longer
/// than the configured duration.
///
/// On expiration, returns [`HexeractError::Timeout`] carrying the envelope
/// type name and the configured duration. Cancellation honors tokio's
/// cooperative semantics: the inner future is dropped at the next await
/// point, which lets `Drop` implementations run their cleanup.
pub struct TimeoutMiddleware {
    duration: Duration,
    #[allow(dead_code, reason = "wired in Building")]
    grace: Duration,
}

impl TimeoutMiddleware {
    /// Builds a middleware that aborts dispatches taking longer than
    /// `duration`. A [`Duration::ZERO`] polls the inner future once and
    /// then returns [`HexeractError::Timeout`] if it has not completed.
    #[must_use]
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            grace: DEFAULT_TIMEOUT_GRACE,
        }
    }

    /// Time the handler keeps being polled after `ctx.cancellation` fires, so
    /// it can observe it and clean up. Zero drops it right after cancelling.
    #[must_use]
    pub fn with_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
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
    use std::sync::atomic::{AtomicBool, Ordering};
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

    struct CancellationAwareTerminal {
        observed_cancellation: Arc<AtomicBool>,
    }

    impl Terminal for CancellationAwareTerminal {
        fn dispatch<'a>(
            &'a self,
            _envelope: &'a MessageEnvelope,
            ctx: &'a HandlerContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<BoxOutput, HexeractError>> + Send + 'a>,
        > {
            Box::pin(async move {
                tokio::select! {
                    () = ctx.cancellation.cancelled() => {
                        self.observed_cancellation.store(true, Ordering::SeqCst);
                        Err(HexeractError::Dispatch("cancelled".into()))
                    }
                    () = tokio::time::sleep(Duration::from_secs(3600)) => {
                        Ok(Box::new(7_i32) as BoxOutput)
                    }
                }
            })
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct CancellationDeafTerminal {
        was_dropped: Arc<AtomicBool>,
        polled_after_cancellation: Arc<AtomicBool>,
    }

    impl Terminal for CancellationDeafTerminal {
        fn dispatch<'a>(
            &'a self,
            _envelope: &'a MessageEnvelope,
            ctx: &'a HandlerContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<BoxOutput, HexeractError>> + Send + 'a>,
        > {
            let guard = DropFlag(Arc::clone(&self.was_dropped));
            let polled_after_cancellation = Arc::clone(&self.polled_after_cancellation);
            Box::pin(async move {
                let _guard = guard;
                std::future::poll_fn(|_cx| {
                    if ctx.is_cancelled() {
                        polled_after_cancellation.store(true, Ordering::SeqCst);
                    }
                    std::task::Poll::<()>::Pending
                })
                .await;
                Ok(Box::new(7_i32) as BoxOutput)
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_selecting_on_cancellation_observes_it_during_the_grace_and_the_result_stays_timeout()
     {
        let observed_cancellation = Arc::new(AtomicBool::new(false));
        let result = run_through(
            TimeoutMiddleware::new(Duration::from_millis(50)).with_grace(Duration::from_secs(1)),
            Arc::new(CancellationAwareTerminal {
                observed_cancellation: Arc::clone(&observed_cancellation),
            }),
        )
        .await;

        assert!(
            matches!(result, Err(HexeractError::Timeout { .. })),
            "criterion 6: the result must stay Timeout even when the handler finishes during the grace"
        );
        assert!(
            observed_cancellation.load(Ordering::SeqCst),
            "criterion 6: the handler must still be polled after cancellation so it can observe it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_ignoring_cancellation_is_dropped_when_the_grace_ends() {
        let was_dropped = Arc::new(AtomicBool::new(false));
        let started = tokio::time::Instant::now();
        let result = run_through(
            TimeoutMiddleware::new(Duration::from_millis(50))
                .with_grace(Duration::from_millis(200)),
            Arc::new(CancellationDeafTerminal {
                was_dropped: Arc::clone(&was_dropped),
                polled_after_cancellation: Arc::new(AtomicBool::new(false)),
            }),
        )
        .await;
        let elapsed = started.elapsed();

        assert!(
            matches!(result, Err(HexeractError::Timeout { .. })),
            "criterion 7: an unresponsive handler still yields Timeout"
        );
        assert!(
            was_dropped.load(Ordering::SeqCst),
            "criterion 7: the handler must be dropped once the grace is spent"
        );
        assert_eq!(
            elapsed,
            Duration::from_millis(250),
            "criterion 7: the wait must be exactly duration plus grace, no more"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_grace_cancels_then_drops_without_polling_again() {
        let was_dropped = Arc::new(AtomicBool::new(false));
        let polled_after_cancellation = Arc::new(AtomicBool::new(false));
        let started = tokio::time::Instant::now();
        let result = run_through(
            TimeoutMiddleware::new(Duration::from_millis(50)).with_grace(Duration::ZERO),
            Arc::new(CancellationDeafTerminal {
                was_dropped: Arc::clone(&was_dropped),
                polled_after_cancellation: Arc::clone(&polled_after_cancellation),
            }),
        )
        .await;

        assert!(
            matches!(result, Err(HexeractError::Timeout { .. })),
            "criterion 8: a zero grace still yields Timeout"
        );
        assert!(
            was_dropped.load(Ordering::SeqCst),
            "criterion 8: a zero grace must drop the handler"
        );
        assert!(
            !polled_after_cancellation.load(Ordering::SeqCst),
            "criterion 8: a zero grace must not poll the handler again after cancelling"
        );
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(50),
            "criterion 8: a zero grace adds no waiting beyond the duration"
        );
    }
}
