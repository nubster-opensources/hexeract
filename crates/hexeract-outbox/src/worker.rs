use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use hexeract_core::CorrelationId;
use hexeract_core::HandlerContext;
use hexeract_core::MessageId;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Event;
use crate::Handler;
use crate::OutboxEnvelope;
use crate::OutboxError;

/// Pinned, boxed, send future returned by trait object methods.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Backend-agnostic contract for the outbox storage operations driven by
/// [`OutboxWorker`].
///
/// The split between [`Self::Client`] and [`Self::Tx`] keeps the trait
/// idiomatic: the connection guard is owned by the worker's polling
/// cycle, and the transaction borrows from it for the duration of the
/// cycle. Backends that follow the `Pool` + `Transaction` pattern
/// (deadpool_postgres, sqlx, ...) map onto this trait directly.
///
/// Implemented via `async_trait` (boxed futures) to work around the
/// current Rust limitation around HRTB inference on GATs (see
/// rust-lang/rust#100013). The runtime cost is one heap allocation per
/// trait method call, negligible at the outbox dispatch cadence.
#[async_trait::async_trait]
pub trait OutboxStore: Send + Sync + 'static {
    /// Pooled connection guard owned by the worker for one polling cycle.
    type Client: Send;
    /// Transaction borrowed from a [`Self::Client`].
    type Tx<'tx>: Send
    where
        Self: 'tx;

    /// Acquire a connection from the underlying pool.
    async fn acquire(&self) -> Result<Self::Client, OutboxError>;

    /// Open a transaction borrowing from the given connection.
    async fn begin<'a>(&self, client: &'a mut Self::Client) -> Result<Self::Tx<'a>, OutboxError>;

    /// Poll a batch of pending envelopes, locking them via `FOR UPDATE SKIP LOCKED`.
    ///
    /// Envelopes that have reached or exceeded `max_attempts`, or whose
    /// `next_retry_at` is in the future, are excluded.
    async fn poll<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        batch_size: usize,
        max_attempts: u32,
    ) -> Result<Vec<OutboxEnvelope>, OutboxError>;

    /// Mark an envelope as successfully delivered.
    async fn mark_delivered<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
    ) -> Result<(), OutboxError>;

    /// Mark an envelope as failed, incrementing attempts and setting next retry.
    async fn mark_failed<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
        error: &str,
        next_retry_at: SystemTime,
    ) -> Result<(), OutboxError>;

    /// Commit the transaction.
    async fn commit<'a>(&self, tx: Self::Tx<'a>) -> Result<(), OutboxError>;
}

/// Type-erased handler that the worker dispatches to.
///
/// Most users do not implement this trait directly; they use
/// [`TypedHandler`] to adapt a typed [`Handler<E>`] into an erased one
/// the worker can store in a registry keyed by `event_type`.
pub trait ErasedHandler: Send + Sync + 'static {
    /// Event type this handler reacts to, matching [`Event::EVENT_TYPE`].
    fn event_type(&self) -> &'static str;

    /// Decode the envelope and dispatch to the underlying typed handler.
    fn handle<'a>(
        &'a self,
        envelope: &'a OutboxEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), OutboxError>>;
}

/// Adapter that lifts a typed [`Handler<E>`] into an [`ErasedHandler`].
pub struct TypedHandler<E, H>
where
    E: Event,
    H: Handler<E>,
{
    handler: Arc<H>,
    _phantom: PhantomData<fn() -> E>,
}

impl<E, H> TypedHandler<E, H>
where
    E: Event,
    H: Handler<E>,
{
    /// Wrap a freshly owned handler.
    #[must_use]
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: PhantomData,
        }
    }

    /// Wrap a handler already shared behind an `Arc`.
    #[must_use]
    pub fn shared(handler: Arc<H>) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<E, H> ErasedHandler for TypedHandler<E, H>
where
    E: Event,
    H: Handler<E>,
{
    fn event_type(&self) -> &'static str {
        E::EVENT_TYPE
    }

    fn handle<'a>(
        &'a self,
        envelope: &'a OutboxEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), OutboxError>> {
        Box::pin(async move {
            let event: E = envelope.decode()?;
            self.handler.handle(event, ctx).await.map_err(Into::into)
        })
    }
}

/// Tuning parameters for an [`OutboxWorker`].
#[derive(Debug, Clone)]
pub struct OutboxWorkerConfig {
    /// Sleep duration between empty polls.
    pub poll_interval: Duration,
    /// Maximum number of envelopes returned by a single poll.
    pub batch_size: usize,
    /// Number of attempts allowed before an envelope stops being polled.
    pub max_attempts: u32,
    /// Constant delay added to `next_retry_at` after a failed dispatch.
    pub retry_delay: Duration,
    /// Minimum delay applied between consecutive non-empty poll cycles.
    ///
    /// A full batch otherwise loops with no delay, busy-spinning the store
    /// under a sustained backlog. This floor paces back-to-back non-empty
    /// cycles without affecting the empty-poll path (which still waits for
    /// [`Self::poll_interval`]). Set it to [`Duration::ZERO`] to disable
    /// pacing and restore the previous tight-loop behavior.
    pub min_cycle_delay: Duration,
}

impl Default for OutboxWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            batch_size: 10,
            max_attempts: 5,
            retry_delay: Duration::from_secs(5),
            min_cycle_delay: Duration::from_millis(5),
        }
    }
}

/// Worker that polls the outbox in a loop and dispatches envelopes to
/// their registered handlers.
///
/// Generic over any [`OutboxStore`] backend. The worker takes ownership
/// of the store and a registry mapping `event_type` to its
/// [`ErasedHandler`], then [`Self::start`] spawns the polling task and
/// returns a [`JoinHandle`].
pub struct OutboxWorker<S>
where
    S: OutboxStore,
{
    store: S,
    handlers: Arc<HashMap<&'static str, Arc<dyn ErasedHandler>>>,
    config: OutboxWorkerConfig,
}

impl<S> OutboxWorker<S>
where
    S: OutboxStore,
{
    /// Build a new worker.
    #[must_use]
    pub fn new(
        store: S,
        handlers: HashMap<&'static str, Arc<dyn ErasedHandler>>,
        config: OutboxWorkerConfig,
    ) -> Self {
        Self {
            store,
            handlers: Arc::new(handlers),
            config,
        }
    }

    /// Returns the polling loop as a boxed `Send` future that the caller spawns.
    ///
    /// The future resolves to `Ok(())` once the supplied
    /// [`CancellationToken`] is cancelled. Transient store errors are
    /// logged via `tracing` and the loop continues. Typical usage:
    ///
    /// ```ignore
    /// let cancel = CancellationToken::new();
    /// let join = tokio::spawn(worker.run(cancel.clone()));
    /// // ...
    /// cancel.cancel();
    /// join.await??;
    /// ```
    ///
    /// The return type is boxed to work around a current Rust compiler
    /// limitation around HRTB inference on GATs (see rust-lang/rust#100013).
    pub fn run(
        self,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutboxError>> + Send>>
    where
        for<'a> S::Tx<'a>: Send,
    {
        Box::pin(async move {
            while !cancel.is_cancelled() {
                let sleep_for = match self.poll_cycle(&cancel).await {
                    Ok(0) => Some(self.config.poll_interval),
                    Ok(_) => {
                        if self.config.min_cycle_delay.is_zero() {
                            None
                        } else {
                            Some(self.config.min_cycle_delay)
                        }
                    }
                    Err(err) => {
                        tracing::error!(
                            error = ?err,
                            "outbox worker poll cycle failed, sleeping before retry"
                        );
                        Some(self.config.poll_interval)
                    }
                };
                if let Some(delay) = sleep_for {
                    tokio::time::sleep(delay).await;
                }
            }
            Ok(())
        })
    }

    async fn poll_cycle(&self, cancel: &CancellationToken) -> Result<usize, OutboxError> {
        let mut client = self.store.acquire().await?;
        let mut tx = self.store.begin(&mut client).await?;

        let envelopes = self
            .store
            .poll(&mut tx, self.config.batch_size, self.config.max_attempts)
            .await?;
        let count = envelopes.len();

        for envelope in envelopes {
            let next_retry_at = SystemTime::now() + self.config.retry_delay;
            match self.dispatch(&envelope, cancel).await {
                Ok(()) => {
                    self.store
                        .mark_delivered(&mut tx, envelope.event_id)
                        .await?;
                }
                Err(err) => {
                    let message = err.to_string();
                    tracing::warn!(
                        event_id = %envelope.event_id,
                        event_type = %envelope.event_type,
                        error = %message,
                        "outbox handler dispatch failed"
                    );
                    self.store
                        .mark_failed(&mut tx, envelope.event_id, &message, next_retry_at)
                        .await?;
                }
            }
        }

        self.store.commit(tx).await?;
        Ok(count)
    }

    async fn dispatch(
        &self,
        envelope: &OutboxEnvelope,
        cancel: &CancellationToken,
    ) -> Result<(), OutboxError> {
        let Some(handler) = self.handlers.get(envelope.event_type.as_str()) else {
            return Err(OutboxError::MissingHandler {
                event_type: envelope.event_type.clone(),
            });
        };

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new())
            .with_cancellation(cancel.clone());

        tracing::debug!(
            event_id = %envelope.event_id,
            event_type = %envelope.event_type,
            "dispatching outbox envelope"
        );

        handler.handle(envelope, &ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Mutex;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct UserRegistered {
        user_id: Uuid,
    }

    impl Event for UserRegistered {
        const EVENT_TYPE: &'static str = "users.registered";
    }

    struct RecordingHandler {
        seen: Arc<Mutex<Vec<Uuid>>>,
    }

    impl Handler<UserRegistered> for RecordingHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            event: UserRegistered,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.seen.lock().unwrap().push(event.user_id);
            Ok(())
        }
    }

    struct FailingHandler;
    impl Handler<UserRegistered> for FailingHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Err(OutboxError::Internal("forced".into()))
        }
    }

    fn fresh_envelope(user_id: Uuid) -> OutboxEnvelope {
        let publisher_test_event = UserRegistered { user_id };
        OutboxEnvelope::new(Uuid::new_v4(), &publisher_test_event).unwrap()
    }

    #[tokio::test]
    async fn typed_handler_decodes_envelope_and_calls_inner_handler() {
        let seen = Arc::new(Mutex::new(Vec::<Uuid>::new()));
        let handler = TypedHandler::new(RecordingHandler {
            seen: Arc::clone(&seen),
        });
        let erased: Arc<dyn ErasedHandler> = Arc::new(handler);

        let user_id = Uuid::from_u128(42);
        let envelope = fresh_envelope(user_id);
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        erased
            .handle(&envelope, &ctx)
            .await
            .expect("erased dispatch must succeed");

        assert_eq!(seen.lock().unwrap().as_slice(), &[user_id]);
    }

    #[tokio::test]
    async fn typed_handler_propagates_handler_error_as_outbox_error() {
        let handler = TypedHandler::new(FailingHandler);
        let erased: Arc<dyn ErasedHandler> = Arc::new(handler);

        let envelope = fresh_envelope(Uuid::nil());
        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        let err = erased.handle(&envelope, &ctx).await.expect_err("must fail");
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn typed_handler_reports_event_type_from_const() {
        let handler = TypedHandler::new(RecordingHandler {
            seen: Arc::new(Mutex::new(Vec::new())),
        });
        assert_eq!(handler.event_type(), "users.registered");
    }

    #[test]
    fn default_config_has_expected_values() {
        let cfg = OutboxWorkerConfig::default();
        assert_eq!(cfg.poll_interval, Duration::from_millis(100));
        assert_eq!(cfg.batch_size, 10);
        assert_eq!(cfg.max_attempts, 5);
        assert_eq!(cfg.retry_delay, Duration::from_secs(5));
        assert_eq!(cfg.min_cycle_delay, Duration::from_millis(5));
    }

    /// Store that records the virtual instant of the first empty poll so a
    /// test can assert non-empty cycles were paced.
    #[derive(Clone)]
    struct PacingStore {
        pending: Arc<Mutex<Vec<OutboxEnvelope>>>,
        empty_poll_at: Arc<Mutex<Option<tokio::time::Instant>>>,
    }

    impl PacingStore {
        fn new(initial: Vec<OutboxEnvelope>) -> Self {
            Self {
                pending: Arc::new(Mutex::new(initial)),
                empty_poll_at: Arc::new(Mutex::new(None)),
            }
        }
    }

    #[async_trait::async_trait]
    impl OutboxStore for PacingStore {
        type Client = MockClient;
        type Tx<'tx> = MockTx;

        async fn acquire(&self) -> Result<Self::Client, OutboxError> {
            Ok(MockClient)
        }

        async fn begin<'a>(
            &self,
            _client: &'a mut Self::Client,
        ) -> Result<Self::Tx<'a>, OutboxError> {
            Ok(MockTx)
        }

        async fn poll<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            batch_size: usize,
            _max_attempts: u32,
        ) -> Result<Vec<OutboxEnvelope>, OutboxError> {
            let mut pending = self.pending.lock().unwrap();
            let take = batch_size.min(pending.len());
            let batch: Vec<OutboxEnvelope> = pending.drain(..take).collect();
            if batch.is_empty() {
                let mut slot = self.empty_poll_at.lock().unwrap();
                if slot.is_none() {
                    *slot = Some(tokio::time::Instant::now());
                }
            }
            Ok(batch)
        }

        async fn mark_delivered<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            _event_id: Uuid,
        ) -> Result<(), OutboxError> {
            Ok(())
        }

        async fn mark_failed<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            _event_id: Uuid,
            _error: &str,
            _next_retry_at: SystemTime,
        ) -> Result<(), OutboxError> {
            Ok(())
        }

        async fn commit<'a>(&self, _tx: Self::Tx<'a>) -> Result<(), OutboxError> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn run_paces_consecutive_non_empty_cycles() {
        let non_empty_cycles: u32 = 4;
        let envelopes: Vec<OutboxEnvelope> = (0..non_empty_cycles)
            .map(|i| fresh_envelope(Uuid::from_u128(u128::from(i) + 1)))
            .collect();
        let store = PacingStore::new(envelopes);
        let empty_poll_at = Arc::clone(&store.empty_poll_at);

        let delay = Duration::from_millis(10);
        let config = OutboxWorkerConfig {
            poll_interval: Duration::from_secs(3600),
            batch_size: 1,
            min_cycle_delay: delay,
            ..OutboxWorkerConfig::default()
        };

        let registry = registry_with(vec![Arc::new(TypedHandler::new(RecordingHandler {
            seen: Arc::new(Mutex::new(Vec::new())),
        }))]);
        let worker = OutboxWorker::new(store, registry, config);

        let cancel = CancellationToken::new();
        let start = tokio::time::Instant::now();
        let join = tokio::spawn(worker.run(cancel.clone()));

        tokio::time::sleep(delay * non_empty_cycles + Duration::from_millis(1)).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let empty_at = empty_poll_at
            .lock()
            .unwrap()
            .expect("the loop should have reached the empty poll");
        assert_eq!(
            empty_at.duration_since(start),
            delay * non_empty_cycles,
            "each non-empty cycle must be paced by min_cycle_delay before the empty poll"
        );
    }

    /// `MockStore` lets us drive the worker without a real database in
    /// unit tests. Integration testing of the SQL semantics happens in
    /// `hexeract-outbox-postgres` via testcontainers.
    #[derive(Clone)]
    struct MockStore {
        pending: Arc<Mutex<Vec<OutboxEnvelope>>>,
        delivered: Arc<Mutex<Vec<Uuid>>>,
        failed: Arc<Mutex<Vec<(Uuid, String)>>>,
    }

    impl MockStore {
        fn new(initial: Vec<OutboxEnvelope>) -> Self {
            Self {
                pending: Arc::new(Mutex::new(initial)),
                delivered: Arc::new(Mutex::new(Vec::new())),
                failed: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    struct MockClient;
    struct MockTx;

    #[async_trait::async_trait]
    impl OutboxStore for MockStore {
        type Client = MockClient;
        type Tx<'tx> = MockTx;

        async fn acquire(&self) -> Result<Self::Client, OutboxError> {
            Ok(MockClient)
        }

        async fn begin<'a>(
            &self,
            _client: &'a mut Self::Client,
        ) -> Result<Self::Tx<'a>, OutboxError> {
            Ok(MockTx)
        }

        async fn poll<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            batch_size: usize,
            _max_attempts: u32,
        ) -> Result<Vec<OutboxEnvelope>, OutboxError> {
            let mut pending = self.pending.lock().unwrap();
            let take = batch_size.min(pending.len());
            Ok(pending.drain(..take).collect())
        }

        async fn mark_delivered<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            event_id: Uuid,
        ) -> Result<(), OutboxError> {
            self.delivered.lock().unwrap().push(event_id);
            Ok(())
        }

        async fn mark_failed<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            event_id: Uuid,
            error: &str,
            _next_retry_at: SystemTime,
        ) -> Result<(), OutboxError> {
            self.failed
                .lock()
                .unwrap()
                .push((event_id, error.to_owned()));
            Ok(())
        }

        async fn commit<'a>(&self, _tx: Self::Tx<'a>) -> Result<(), OutboxError> {
            Ok(())
        }
    }

    fn registry_with(
        handlers: Vec<Arc<dyn ErasedHandler>>,
    ) -> HashMap<&'static str, Arc<dyn ErasedHandler>> {
        let mut map = HashMap::new();
        for handler in handlers {
            map.insert(handler.event_type(), handler);
        }
        map
    }

    #[tokio::test]
    async fn worker_dispatches_pending_envelopes_and_marks_delivered() {
        let envelopes = vec![
            fresh_envelope(Uuid::from_u128(1)),
            fresh_envelope(Uuid::from_u128(2)),
        ];
        let event_ids: Vec<Uuid> = envelopes.iter().map(|e| e.event_id).collect();
        let store = MockStore::new(envelopes);

        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingHandler {
            seen: Arc::clone(&seen),
        }));
        let registry = registry_with(vec![handler]);

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(
            store.delivered.lock().unwrap().as_slice(),
            event_ids.as_slice()
        );
        assert!(store.failed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn worker_marks_failed_when_handler_errors() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = MockStore::new(vec![envelope]);

        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(FailingHandler));
        let registry = registry_with(vec![handler]);

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        assert!(store.delivered.lock().unwrap().is_empty());
        let failed = store.failed.lock().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, event_id);
        assert!(failed[0].1.contains("forced"));
    }

    #[tokio::test]
    async fn worker_marks_failed_when_no_handler_registered() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = MockStore::new(vec![envelope]);

        let registry = HashMap::new();

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let failed = store.failed.lock().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, event_id);
        assert!(failed[0].1.contains("no handler"));
    }

    #[tokio::test]
    async fn worker_stops_promptly_on_cancellation() {
        let store = MockStore::new(Vec::new());
        let registry = HashMap::new();
        let worker = OutboxWorker::new(store, registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        cancel.cancel();
        let started = std::time::Instant::now();
        join.await.unwrap().unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "worker took {:?} to stop",
            started.elapsed()
        );
    }
}
