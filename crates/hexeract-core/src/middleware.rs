use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::context::HandlerContext;
use crate::envelope::MessageEnvelope;
use crate::error::HexeractError;

/// Type-erased handler output, passed through the middleware chain.
///
/// The terminal dispatcher boxes the concrete `C::Output` into this alias.
/// Callers downcast back to the typed output at the dispatch boundary.
pub type BoxOutput = Box<dyn Any + Send + Sync>;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Intercepts a dispatch before reaching its handler.
///
/// Middlewares are stacked onion-style: the first registered middleware
/// wraps all the others, observing both the entry and the exit of every
/// dispatch.
///
/// # Example
///
/// ```
/// use hexeract_core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};
///
/// struct LoggingMiddleware;
///
/// impl Middleware for LoggingMiddleware {
///     async fn execute(
///         &self,
///         envelope: &MessageEnvelope,
///         ctx: &HandlerContext,
///         next: Next,
///     ) -> Result<BoxOutput, HexeractError> {
///         tracing::info!(type_name = envelope.type_name(), "dispatching");
///         let result = next.run(envelope, ctx).await;
///         tracing::info!(type_name = envelope.type_name(), "dispatched");
///         result
///     }
/// }
/// ```
#[trait_variant::make(Send)]
pub trait Middleware: Send + Sync + 'static {
    /// Executes the middleware. The implementation must call `next.run(...)`
    /// to proceed to the next middleware or terminal, unless it intentionally
    /// short-circuits the chain.
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError>;
}

#[doc(hidden)]
pub trait DynMiddleware: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
        next: Next,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>>;
}

impl<M: Middleware> DynMiddleware for M {
    fn execute<'a>(
        &'a self,
        envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
        next: Next,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        Box::pin(<M as Middleware>::execute(self, envelope, ctx, next))
    }
}

/// Terminal of the middleware chain. The mediator (issue #6) supplies a
/// concrete implementation that downcasts the message and invokes the
/// registered handler.
///
/// This trait is public so external dispatchers and test harnesses can
/// build a pipeline without depending on the mediator. The API may evolve
/// before v1.0.
pub trait Terminal: Send + Sync + 'static {
    /// Dispatches the message to its terminal destination.
    fn dispatch<'a>(
        &'a self,
        envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>>;
}

/// Opaque continuation passed to a [`Middleware`]. Calling [`Next::run`]
/// proceeds to the next middleware in the chain or to the [`Terminal`] if
/// the chain is empty.
pub struct Next {
    chain: VecDeque<Arc<dyn DynMiddleware>>,
    terminal: Arc<dyn Terminal>,
}

impl Next {
    /// Builds a new [`Next`] from a chain of middlewares and a terminal.
    ///
    /// Middlewares are executed in the order they appear in the slice: the
    /// first one wraps the second, which wraps the third, and so on.
    #[must_use]
    pub fn new(middlewares: Vec<Arc<dyn DynMiddleware>>, terminal: Arc<dyn Terminal>) -> Self {
        Self {
            chain: middlewares.into(),
            terminal,
        }
    }

    /// Advances the pipeline by one step.
    ///
    /// # Errors
    ///
    /// Returns the [`HexeractError`] produced by the next middleware in the
    /// chain or by the [`Terminal`] when the chain is exhausted.
    pub async fn run(
        mut self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
    ) -> Result<BoxOutput, HexeractError> {
        if let Some(head) = self.chain.pop_front() {
            head.execute(envelope, ctx, self).await
        } else {
            self.terminal.dispatch(envelope, ctx).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{CorrelationId, MessageId};
    use std::sync::Mutex;

    fn dyn_mw<M: Middleware>(m: M) -> Arc<dyn DynMiddleware> {
        Arc::new(m)
    }

    struct DummyCmd;
    impl crate::command::Command for DummyCmd {
        type Output = i32;
    }

    fn fresh_env() -> MessageEnvelope {
        MessageEnvelope::for_command::<DummyCmd>(MessageId::new(), CorrelationId::new())
    }

    fn fresh_ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
    }

    struct StaticTerminal {
        value: i32,
    }

    impl Terminal for StaticTerminal {
        fn dispatch<'a>(
            &'a self,
            _envelope: &'a MessageEnvelope,
            _ctx: &'a HandlerContext,
        ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
            let value = self.value;
            Box::pin(async move { Ok(Box::new(value) as BoxOutput) })
        }
    }

    struct FailingTerminal;
    impl Terminal for FailingTerminal {
        fn dispatch<'a>(
            &'a self,
            _envelope: &'a MessageEnvelope,
            _ctx: &'a HandlerContext,
        ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
            Box::pin(async move { Err(HexeractError::Dispatch("terminal failure".into())) })
        }
    }

    #[derive(Clone)]
    struct Recorder {
        trace: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Recorder {
        fn new() -> Self {
            Self {
                trace: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn snapshot(&self) -> Vec<&'static str> {
            self.trace.lock().expect("poisoned").clone()
        }
    }

    struct TracingMiddleware {
        name: &'static str,
        post_label: &'static str,
        recorder: Recorder,
    }

    impl Middleware for TracingMiddleware {
        async fn execute(
            &self,
            envelope: &MessageEnvelope,
            ctx: &HandlerContext,
            next: Next,
        ) -> Result<BoxOutput, HexeractError> {
            self.recorder
                .trace
                .lock()
                .expect("poisoned")
                .push(self.name);
            let result = next.run(envelope, ctx).await;
            self.recorder
                .trace
                .lock()
                .expect("poisoned")
                .push(self.post_label);
            result
        }
    }

    fn tracing_mw(name: &'static str, post: &'static str, recorder: Recorder) -> TracingMiddleware {
        TracingMiddleware {
            name,
            post_label: post,
            recorder,
        }
    }

    #[tokio::test]
    async fn single_middleware_delegates_to_terminal() {
        let recorder = Recorder::new();
        let next = Next::new(
            vec![dyn_mw(tracing_mw("A", "A_post", recorder.clone()))],
            Arc::new(StaticTerminal { value: 42 }),
        );
        let output = next
            .run(&fresh_env(), &fresh_ctx())
            .await
            .expect("dispatch should succeed");
        let downcast = output.downcast::<i32>().expect("output must be i32");
        assert_eq!(*downcast, 42);
        assert_eq!(recorder.snapshot(), vec!["A", "A_post"]);
    }

    #[tokio::test]
    async fn chain_of_three_executes_in_onion_order() {
        let recorder = Recorder::new();
        let next = Next::new(
            vec![
                dyn_mw(tracing_mw("A", "A_post", recorder.clone())),
                dyn_mw(tracing_mw("B", "B_post", recorder.clone())),
                dyn_mw(tracing_mw("C", "C_post", recorder.clone())),
            ],
            Arc::new(StaticTerminal { value: 7 }),
        );
        let _ = next.run(&fresh_env(), &fresh_ctx()).await.unwrap();
        assert_eq!(
            recorder.snapshot(),
            vec!["A", "B", "C", "C_post", "B_post", "A_post"]
        );
    }

    struct ShortCircuit;
    impl Middleware for ShortCircuit {
        async fn execute(
            &self,
            _envelope: &MessageEnvelope,
            _ctx: &HandlerContext,
            _next: Next,
        ) -> Result<BoxOutput, HexeractError> {
            Ok(Box::new(99_i32) as BoxOutput)
        }
    }

    #[tokio::test]
    async fn short_circuit_middleware_skips_terminal() {
        let next = Next::new(vec![dyn_mw(ShortCircuit)], Arc::new(FailingTerminal));
        let output = next
            .run(&fresh_env(), &fresh_ctx())
            .await
            .expect("short-circuit must succeed");
        assert_eq!(*output.downcast::<i32>().unwrap(), 99);
    }

    #[tokio::test]
    async fn error_from_terminal_propagates_through_chain() {
        let recorder = Recorder::new();
        let next = Next::new(
            vec![dyn_mw(tracing_mw("A", "A_post", recorder.clone()))],
            Arc::new(FailingTerminal),
        );
        let result = next.run(&fresh_env(), &fresh_ctx()).await;
        assert!(matches!(result, Err(HexeractError::Dispatch(_))));
        assert_eq!(recorder.snapshot(), vec!["A", "A_post"]);
    }

    struct ErrorMiddleware;
    impl Middleware for ErrorMiddleware {
        async fn execute(
            &self,
            _envelope: &MessageEnvelope,
            _ctx: &HandlerContext,
            _next: Next,
        ) -> Result<BoxOutput, HexeractError> {
            Err(HexeractError::Dispatch("middleware refusal".into()))
        }
    }

    #[tokio::test]
    async fn error_from_middleware_propagates() {
        let next = Next::new(
            vec![dyn_mw(ErrorMiddleware)],
            Arc::new(StaticTerminal { value: 0 }),
        );
        let err = next
            .run(&fresh_env(), &fresh_ctx())
            .await
            .expect_err("middleware should fail");
        match err {
            HexeractError::Dispatch(ref m) => assert_eq!(m, "middleware refusal"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    fn assert_send<T: Send>(_: &T) {}

    #[tokio::test]
    async fn next_run_future_is_send() {
        let next = Next::new(vec![], Arc::new(StaticTerminal { value: 1 }));
        let env = fresh_env();
        let ctx = fresh_ctx();
        let future = next.run(&env, &ctx);
        assert_send(&future);
        let _ = future.await;
    }

    #[tokio::test]
    async fn empty_chain_invokes_terminal_directly() {
        let next = Next::new(vec![], Arc::new(StaticTerminal { value: 123 }));
        let output = next.run(&fresh_env(), &fresh_ctx()).await.unwrap();
        assert_eq!(*output.downcast::<i32>().unwrap(), 123);
    }

    struct EnvelopeInspector {
        observed: Arc<Mutex<Option<String>>>,
    }

    impl Middleware for EnvelopeInspector {
        async fn execute(
            &self,
            envelope: &MessageEnvelope,
            ctx: &HandlerContext,
            next: Next,
        ) -> Result<BoxOutput, HexeractError> {
            *self.observed.lock().expect("poisoned") = Some(envelope.type_name().to_string());
            next.run(envelope, ctx).await
        }
    }

    #[tokio::test]
    async fn middleware_reads_envelope_type_name() {
        let observed = Arc::new(Mutex::new(None));
        let mw = EnvelopeInspector {
            observed: Arc::clone(&observed),
        };
        let next = Next::new(vec![dyn_mw(mw)], Arc::new(StaticTerminal { value: 0 }));
        let _ = next.run(&fresh_env(), &fresh_ctx()).await;
        let observed = observed.lock().unwrap().clone();
        assert!(observed.unwrap().ends_with("::DummyCmd"));
    }
}
