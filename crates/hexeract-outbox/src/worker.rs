use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

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
/// (`deadpool_postgres`, `sqlx`, ...) map onto this trait directly.
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

    /// Mark an envelope as successfully delivered, if the caller still owns the
    /// claim.
    ///
    /// `claimed_attempt` is the attempt number this caller's [`Self::claim`]
    /// wrote, that is `OutboxEnvelope::attempts + 1` as read from the polled
    /// batch. The update applies only while the stored `attempts` still equals it
    /// and the envelope is not already delivered.
    ///
    /// Returns `false` when no row matched: the lease was lost and a newer owner
    /// holds the envelope. This is not an error; the caller leaves the newer
    /// outcome untouched.
    ///
    /// A backend that does not override [`Self::claim`] takes no lease and has no
    /// fence to check: it ignores `claimed_attempt` and returns `true`.
    async fn mark_delivered<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
        claimed_attempt: u32,
    ) -> Result<bool, OutboxError>;

    /// Mark an envelope as failed, recording the error and the backoff delay
    /// before the next retry, if the caller still owns the claim.
    ///
    /// `retry_in` is the delay from *now*; the backend adds it to its own
    /// database clock when persisting `next_retry_at`, so retry scheduling is
    /// not affected by skew between the worker host and the database host.
    ///
    /// The attempt counter is advanced by [`Self::claim`], not here. That is
    /// what lets `claimed_attempt` fence this call: a settlement that moved the
    /// counter would move its own fence.
    ///
    /// Same fencing contract as [`Self::mark_delivered`].
    async fn mark_failed<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
        error: &str,
        retry_in: Duration,
        claimed_attempt: u32,
    ) -> Result<bool, OutboxError>;

    /// Commit the transaction.
    async fn commit<'a>(&self, tx: Self::Tx<'a>) -> Result<(), OutboxError>;

    /// Move an envelope that has exhausted its retry budget to the dead-letter
    /// store, if the caller still owns the claim.
    ///
    /// Called by the worker only after [`Self::mark_failed`] has returned
    /// `true`, within the same transaction as `mark_failed`: both succeed or
    /// both roll back. The default implementation is a no-op returning `true`,
    /// so backends that do not persist dead-lettered envelopes need not
    /// override it.
    async fn mark_dead_lettered<'a>(
        &self,
        _tx: &mut Self::Tx<'a>,
        _event_id: Uuid,
        _error: &str,
        _claimed_attempt: u32,
    ) -> Result<bool, OutboxError> {
        Ok(true)
    }

    /// Claim a batch of envelopes: advance the soft lease and consume one retry
    /// slot, for the envelopes this caller still finds at the attempt count it
    /// polled.
    ///
    /// Called within the same transaction as [`Self::poll`], immediately after
    /// the batch is fetched. The backend sets `next_retry_at` to its own
    /// database clock plus `lease_for` (so the lease window is immune to
    /// app/DB clock skew) and increments `attempts` by one. The worker commits
    /// the transaction right after this call so row-level locks are released
    /// promptly. Competing workers skip envelopes whose `next_retry_at` is in
    /// the future, so the claiming worker can dispatch outside the lock
    /// without risking concurrent re-delivery.
    ///
    /// Incrementing `attempts` here (rather than only on failure) is what makes
    /// a worker crash between claim and acknowledgement safe: the attempt is
    /// already counted, so a poison envelope eventually reaches the dead-letter
    /// threshold instead of being redelivered forever. It is also what makes
    /// the counter usable as a lease token, since no settlement writes it.
    ///
    /// `claims` pairs each candidate's `event_id` with the `attempts` value read
    /// by [`Self::poll`]. The update applies to a row only while its stored
    /// `attempts` still equals that value, so two workers that polled the same
    /// row before either claimed it cannot both believe they hold it. This is
    /// what `FOR UPDATE SKIP LOCKED` already guarantees on the dialects that
    /// support it, enforced on the write instead of on the read so it holds on
    /// every dialect.
    ///
    /// Returns the `event_id`s actually claimed; the caller dispatches only
    /// those. A backend that takes no lease returns every candidate, which the
    /// default implementation does.
    async fn claim<'a>(
        &self,
        _tx: &mut Self::Tx<'a>,
        claims: &[(Uuid, u32)],
        _lease_for: Duration,
    ) -> Result<Vec<Uuid>, OutboxError> {
        Ok(claims.iter().map(|(event_id, _)| *event_id).collect())
    }
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
    /// Base delay for the exponential backoff applied after a failed dispatch.
    ///
    /// The actual delay before attempt `n` is:
    /// `min(retry_max_delay, retry_base_delay × 2^n)`
    ///
    /// When [`Self::jitter`] is `true` (the default), a uniform random value
    /// in `[0, computed_delay]` is drawn instead (full-jitter strategy), which
    /// spreads retries across the window and avoids thundering-herd spikes.
    pub retry_base_delay: Duration,
    /// Upper bound on the computed backoff delay.
    ///
    /// Caps `retry_base_delay × 2^n` so retries never wait longer than this
    /// value regardless of the attempt count.
    pub retry_max_delay: Duration,
    /// Apply full jitter to the backoff delay.
    ///
    /// When `true` (the default), the worker draws a uniform random value in
    /// `[0, capped_delay]` instead of using the deterministic exponential.
    /// Set to `false` to get a predictable delay (useful in tests or
    /// environments where all workers share the same retry schedule).
    pub jitter: bool,
    /// Minimum delay applied between consecutive non-empty poll cycles.
    ///
    /// A full batch otherwise loops with no delay, busy-spinning the store
    /// under a sustained backlog. This floor paces back-to-back non-empty
    /// cycles without affecting the empty-poll path (which still waits for
    /// [`Self::poll_interval`]). Set it to [`Duration::ZERO`] to disable
    /// pacing and restore the previous tight-loop behavior.
    pub min_cycle_delay: Duration,
    /// Per-envelope handler deadline and soft-lease unit.
    ///
    /// Two related roles:
    ///
    /// - **Hard timeout.** Each handler invocation is wrapped in a
    ///   `tokio::time::timeout` of this duration. A handler that exceeds it has
    ///   its cancellation token signalled and the dispatch is recorded as a
    ///   failed attempt ([`OutboxError::DispatchTimeout`]), so a hung handler
    ///   cannot stall the worker.
    /// - **Lease unit.** After polling a batch, the worker leases the whole
    ///   batch by setting `next_retry_at` to the database clock plus
    ///   `batch_size x dispatch_timeout`, then commits immediately. This
    ///   releases the `FOR UPDATE SKIP LOCKED` row-level locks before dispatch
    ///   begins while keeping the lease alive across the worst-case sequential
    ///   dispatch of every envelope in the batch, so competing workers do not
    ///   re-pick a tail envelope mid-batch.
    ///
    /// Set it to the worst-case duration of a *single* handler; the
    /// `batch_size` multiplier for the lease is applied internally, so do not
    /// pre-multiply it yourself.
    ///
    /// [`OutboxError::DispatchTimeout`]: crate::OutboxError::DispatchTimeout
    pub dispatch_timeout: Duration,
}

impl Default for OutboxWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            batch_size: 10,
            max_attempts: 5,
            retry_base_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(300),
            jitter: true,
            min_cycle_delay: Duration::from_millis(5),
            dispatch_timeout: Duration::from_secs(30),
        }
    }
}

impl OutboxWorkerConfig {
    /// Compute the next retry delay for an envelope that has failed `attempts` times.
    ///
    /// Uses bounded exponential backoff: `min(retry_max_delay, retry_base_delay × 2^attempts)`.
    /// When [`Self::jitter`] is `true`, returns a uniform random value in
    /// `[0, computed_delay]` (full-jitter strategy).
    ///
    /// The computation is overflow-safe: `checked_shl` returns `None` when the
    /// shift overflows, in which case the factor falls back to `u32::MAX`, and
    /// `Duration::saturating_mul` clamps the result instead of panicking.
    #[must_use]
    pub fn next_retry_delay(&self, attempts: u32) -> Duration {
        let factor = 1u32.checked_shl(attempts).unwrap_or(u32::MAX);
        let capped = self
            .retry_base_delay
            .saturating_mul(factor)
            .min(self.retry_max_delay);
        if self.jitter {
            let nanos = capped.as_nanos();
            // fastrand::u64 covers the full jitter range; clamp to u64::MAX nanos
            // (~584 years) to handle theoretical caps beyond that bound.
            let nanos_u64 = u64::try_from(nanos).unwrap_or(u64::MAX);
            Duration::from_nanos(fastrand::u64(0..=nanos_u64))
        } else {
            capped
        }
    }
}

/// Worker that polls the outbox in a loop and dispatches envelopes to
/// their registered handlers.
///
/// Generic over any [`OutboxStore`] backend. The worker takes ownership
/// of the store and a registry mapping `event_type` to its
/// [`ErasedHandler`], then [`Self::run`] returns a boxed future that
/// polls until cancelled.
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
                    // Race the sleep against cancellation so a shutdown during
                    // a long poll_interval is observed promptly instead of
                    // after the full delay elapses (#231).
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = cancel.cancelled() => break,
                    }
                }
            }
            Ok(())
        })
    }

    async fn poll_cycle(&self, cancel: &CancellationToken) -> Result<usize, OutboxError> {
        // Phase 1: claim (short transaction: SQL only, no handler I/O)
        // The FOR UPDATE SKIP LOCKED locks are held only for the SELECT +
        // the claim UPDATE, then released at commit. Competing workers can
        // immediately pick up other rows.
        let envelopes = {
            let mut client = self.store.acquire().await?;
            let mut tx = self.store.begin(&mut client).await?;
            let batch = self
                .store
                .poll(&mut tx, self.config.batch_size, self.config.max_attempts)
                .await?;
            let claimed = if batch.is_empty() {
                batch
            } else {
                let claims: Vec<(Uuid, u32)> =
                    batch.iter().map(|e| (e.event_id, e.attempts)).collect();
                let obtained = self
                    .store
                    .claim(&mut tx, &claims, self.lease_for(claims.len()))
                    .await?;
                // An envelope read by the poll but not obtained by the claim
                // belongs to another worker: it is neither dispatched nor
                // reported, because losing that race is a normal outcome of
                // concurrent consumers.
                let obtained: HashSet<Uuid> = obtained.into_iter().collect();
                batch
                    .into_iter()
                    .filter(|envelope| obtained.contains(&envelope.event_id))
                    .collect()
            };
            self.store.commit(tx).await?;
            claimed
        };
        let count = envelopes.len();

        // Phase 2: dispatch + ack (one transaction per envelope, outside the poll lock)
        for envelope in &envelopes {
            // Isolate per-envelope settle failures: a transient DB error while
            // acking one envelope must not abandon the rest of the claimed
            // batch (those rows would otherwise wait out the whole lease before
            // being retried). Log and continue (#231).
            if let Err(err) = self.dispatch_and_settle(envelope, cancel).await {
                tracing::error!(
                    event_id = %envelope.event_id,
                    event_type = %envelope.event_type,
                    error = ?err,
                    "failed to settle outbox envelope; continuing with the rest of the batch"
                );
            }
        }

        Ok(count)
    }

    /// Soft-lease duration covering the worst-case sequential dispatch of a
    /// whole claimed batch.
    ///
    /// A single batch is claimed once, then its envelopes are dispatched
    /// sequentially. The lease must therefore outlast `batch_len` back-to-back
    /// dispatches, each bounded by `dispatch_timeout`; sizing it as merely
    /// `dispatch_timeout` lets the tail envelopes' lease expire mid-batch and a
    /// competing worker double-dispatch them (#215).
    fn lease_for(&self, batch_len: usize) -> Duration {
        let factor = u32::try_from(batch_len.max(1)).unwrap_or(u32::MAX);
        self.config.dispatch_timeout.saturating_mul(factor)
    }

    /// Dispatch a single envelope and settle its outcome in its own
    /// transaction.
    async fn dispatch_and_settle(
        &self,
        envelope: &OutboxEnvelope,
        cancel: &CancellationToken,
    ) -> Result<(), OutboxError> {
        match self.dispatch(envelope, cancel).await {
            Ok(()) => {
                let mut client = self.store.acquire().await?;
                let mut tx = self.store.begin(&mut client).await?;
                let applied = self
                    .store
                    .mark_delivered(&mut tx, envelope.event_id, envelope.attempts + 1)
                    .await?;
                if !applied {
                    warn_lease_lost(envelope, "delivery");
                }
                self.store.commit(tx).await?;
            }
            Err(err) => {
                let message = err.to_string();
                tracing::warn!(
                    event_id = %envelope.event_id,
                    event_type = %envelope.event_type,
                    error = %message,
                    "outbox handler dispatch failed"
                );
                let retry_in = self.config.next_retry_delay(envelope.attempts);
                let mut client = self.store.acquire().await?;
                let mut tx = self.store.begin(&mut client).await?;
                let recorded = self
                    .store
                    .mark_failed(
                        &mut tx,
                        envelope.event_id,
                        &message,
                        retry_in,
                        envelope.attempts + 1,
                    )
                    .await?;
                if !recorded {
                    // Dead-lettering after a refused failure would remove an
                    // envelope that a newer owner holds, and may already have
                    // delivered.
                    warn_lease_lost(envelope, "failure");
                } else if envelope.attempts + 1 >= self.config.max_attempts {
                    tracing::error!(
                        event_id = %envelope.event_id,
                        event_type = %envelope.event_type,
                        attempts = envelope.attempts + 1,
                        "outbox envelope exhausted retry budget, moving to dead letter"
                    );
                    let moved = self
                        .store
                        .mark_dead_lettered(
                            &mut tx,
                            envelope.event_id,
                            &message,
                            envelope.attempts + 1,
                        )
                        .await?;
                    if !moved {
                        warn_lease_lost(envelope, "dead-lettering");
                    }
                }
                self.store.commit(tx).await?;
            }
        }
        Ok(())
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

        // Stable across retries: both IDs are pinned to the envelope's
        // immutable event_id so every dispatch attempt of the same row
        // presents an identical context to the handler.
        //
        // A child token lets a dispatch timeout cancel only this handler
        // without tearing down the worker's shared token; the child is still
        // cancelled if the parent (worker) token is cancelled.
        let ctx = HandlerContext::new(
            MessageId::from(envelope.event_id),
            CorrelationId::from(envelope.event_id),
        )
        .with_cancellation(cancel.child_token());

        tracing::debug!(
            event_id = %envelope.event_id,
            event_type = %envelope.event_type,
            "dispatching outbox envelope"
        );

        // Enforce dispatch_timeout as a hard deadline so a hung handler cannot
        // stall the worker forever (#229). The cancellation token is signalled
        // first so a cooperative handler can unwind before the future is
        // dropped.
        match tokio::time::timeout(self.config.dispatch_timeout, handler.handle(envelope, &ctx))
            .await
        {
            Ok(result) => result,
            Err(_elapsed) => {
                ctx.cancellation.cancel();
                Err(OutboxError::DispatchTimeout {
                    event_id: envelope.event_id,
                    event_type: envelope.event_type.clone(),
                    timeout: self.config.dispatch_timeout,
                })
            }
        }
    }
}

/// Report that a settlement was refused because a newer owner holds the
/// envelope.
///
/// A lost lease is an operational fact, not a worker failure: the newer
/// owner's outcome stands and the cycle carries on.
fn warn_lease_lost(envelope: &OutboxEnvelope, settlement: &'static str) {
    tracing::warn!(
        event_id = %envelope.event_id,
        event_type = %envelope.event_type,
        settlement,
        "outbox settlement not applied: the lease was lost to a newer owner"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tokio::sync::Notify;

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

    struct ContextCapturingHandler {
        captured_ids: Arc<Mutex<Vec<MessageId>>>,
    }

    impl Handler<UserRegistered> for ContextCapturingHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.captured_ids.lock().unwrap().push(ctx.message_id);
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
        assert_eq!(cfg.retry_base_delay, Duration::from_secs(1));
        assert_eq!(cfg.retry_max_delay, Duration::from_secs(300));
        assert!(cfg.jitter);
        assert_eq!(cfg.min_cycle_delay, Duration::from_millis(5));
        assert_eq!(cfg.dispatch_timeout, Duration::from_secs(30));
    }

    fn deterministic_config(base: Duration, max: Duration) -> OutboxWorkerConfig {
        OutboxWorkerConfig {
            retry_base_delay: base,
            retry_max_delay: max,
            jitter: false,
            ..OutboxWorkerConfig::default()
        }
    }

    #[test]
    fn backoff_grows_exponentially_without_jitter() {
        let cfg = deterministic_config(Duration::from_secs(1), Duration::from_secs(300));
        assert_eq!(cfg.next_retry_delay(0), Duration::from_secs(1));
        assert_eq!(cfg.next_retry_delay(1), Duration::from_secs(2));
        assert_eq!(cfg.next_retry_delay(2), Duration::from_secs(4));
        assert_eq!(cfg.next_retry_delay(3), Duration::from_secs(8));
    }

    #[test]
    fn backoff_caps_at_max_delay() {
        let cfg = deterministic_config(Duration::from_secs(1), Duration::from_secs(30));
        assert_eq!(cfg.next_retry_delay(10), Duration::from_secs(30));
        assert_eq!(cfg.next_retry_delay(100), Duration::from_secs(30));
    }

    #[test]
    fn backoff_overflow_safe_for_large_attempts() {
        let cfg = deterministic_config(Duration::from_secs(1), Duration::from_secs(300));
        let delay = cfg.next_retry_delay(64);
        assert_eq!(
            delay,
            Duration::from_secs(300),
            "overflow must saturate at cap"
        );
    }

    #[test]
    fn backoff_jitter_stays_within_bounds() {
        let base = Duration::from_secs(1);
        let max = Duration::from_secs(30);
        let cfg = OutboxWorkerConfig {
            retry_base_delay: base,
            retry_max_delay: max,
            jitter: true,
            ..OutboxWorkerConfig::default()
        };
        for attempts in 0u32..8 {
            let delay = cfg.next_retry_delay(attempts);
            let cap = base
                .saturating_mul(1u32.checked_shl(attempts).unwrap_or(u32::MAX))
                .min(max);
            assert!(
                delay <= cap,
                "attempt {attempts}: jittered delay {delay:?} must be <= cap {cap:?}"
            );
        }
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
            _claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            Ok(true)
        }

        async fn mark_failed<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            _event_id: Uuid,
            _error: &str,
            _retry_in: Duration,
            _claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            Ok(true)
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
    /// `hexeract-outbox-sql` via testcontainers.
    #[derive(Clone)]
    struct MockStore {
        pending: Arc<Mutex<Vec<OutboxEnvelope>>>,
        delivered: Arc<Mutex<Vec<Uuid>>>,
        failed: Arc<Mutex<Vec<(Uuid, String)>>>,
        dead_lettered: Arc<Mutex<Vec<Uuid>>>,
        claimed: Arc<Mutex<Vec<Uuid>>>,
        fail_claim: Arc<AtomicBool>,
        /// When set, `mark_delivered` returns an error for this event id once,
        /// simulating a transient ack failure (used to test batch isolation).
        fail_mark_delivered_for: Arc<Mutex<Option<Uuid>>>,
        /// Completed poll cycles, so a test can wait for the worker to have
        /// actually run instead of sleeping for a duration it guessed.
        polls: Arc<AtomicUsize>,
        polled: Arc<Notify>,
    }

    impl MockStore {
        fn new(initial: Vec<OutboxEnvelope>) -> Self {
            Self {
                pending: Arc::new(Mutex::new(initial)),
                delivered: Arc::new(Mutex::new(Vec::new())),
                failed: Arc::new(Mutex::new(Vec::new())),
                dead_lettered: Arc::new(Mutex::new(Vec::new())),
                claimed: Arc::new(Mutex::new(Vec::new())),
                fail_claim: Arc::new(AtomicBool::new(false)),
                fail_mark_delivered_for: Arc::new(Mutex::new(None)),
                polls: Arc::new(AtomicUsize::new(0)),
                polled: Arc::new(Notify::new()),
            }
        }

        /// Wait until the worker has completed at least `count` poll cycles.
        ///
        /// Two cycles prove the first batch was fully settled: the run loop is
        /// sequential, so reaching poll number two means everything the first
        /// poll returned has been dispatched and acknowledged. Tests that
        /// assert an absence need that proof, because "nothing was claimed"
        /// also holds for a worker that never started.
        ///
        /// # Panics
        ///
        /// Panics when the worker does not reach `count` cycles within
        /// [`POLL_CYCLE_TIMEOUT`].
        async fn await_polls(&self, count: usize) {
            let wait = async {
                loop {
                    // Enqueue the waiter before reading the counter:
                    // `notified()` alone does not register it until first
                    // polled, so a notification fired in between would be lost.
                    let notified = self.polled.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if self.polls.load(Ordering::SeqCst) >= count {
                        return;
                    }
                    notified.await;
                }
            };
            let outcome = tokio::time::timeout(POLL_CYCLE_TIMEOUT, wait).await;
            assert!(
                outcome.is_ok(),
                "worker completed {} poll cycle(s), expected at least {count}",
                self.polls.load(Ordering::SeqCst)
            );
        }
    }

    /// Upper bound on any wait for the worker to complete poll cycles. Under
    /// `start_paused` this is virtual time, so a stalled worker fails the test
    /// at once instead of holding the suite for the whole duration.
    const POLL_CYCLE_TIMEOUT: Duration = Duration::from_secs(5);

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
            let batch = {
                let mut pending = self.pending.lock().unwrap();
                let take = batch_size.min(pending.len());
                pending.drain(..take).collect()
            };
            self.polls.fetch_add(1, Ordering::SeqCst);
            self.polled.notify_waiters();
            Ok(batch)
        }

        async fn mark_delivered<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            event_id: Uuid,
            _claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            {
                let mut slot = self.fail_mark_delivered_for.lock().unwrap();
                if *slot == Some(event_id) {
                    *slot = None;
                    return Err(OutboxError::PoolTimeout);
                }
            }
            self.delivered.lock().unwrap().push(event_id);
            Ok(true)
        }

        async fn mark_failed<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            event_id: Uuid,
            error: &str,
            _retry_in: Duration,
            _claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            self.failed
                .lock()
                .unwrap()
                .push((event_id, error.to_owned()));
            Ok(true)
        }

        async fn mark_dead_lettered<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            event_id: Uuid,
            _error: &str,
            _claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            self.dead_lettered.lock().unwrap().push(event_id);
            Ok(true)
        }

        async fn commit<'a>(&self, _tx: Self::Tx<'a>) -> Result<(), OutboxError> {
            Ok(())
        }

        async fn claim<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            claims: &[(Uuid, u32)],
            _lease_for: Duration,
        ) -> Result<Vec<Uuid>, OutboxError> {
            if self.fail_claim.load(Ordering::Relaxed) {
                return Err(OutboxError::Internal("claim failed".into()));
            }
            let event_ids: Vec<Uuid> = claims.iter().map(|(event_id, _)| *event_id).collect();
            self.claimed.lock().unwrap().extend_from_slice(&event_ids);
            Ok(event_ids)
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

    #[tokio::test(start_paused = true)]
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

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(
            store.delivered.lock().unwrap().as_slice(),
            event_ids.as_slice()
        );
        assert!(store.failed.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn worker_marks_failed_when_handler_errors() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = MockStore::new(vec![envelope]);

        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(FailingHandler));
        let registry = registry_with(vec![handler]);

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        assert!(store.delivered.lock().unwrap().is_empty());
        let failed = store.failed.lock().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, event_id);
        assert!(failed[0].1.contains("forced"));
    }

    #[tokio::test(start_paused = true)]
    async fn worker_marks_failed_when_no_handler_registered() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = MockStore::new(vec![envelope]);

        let registry = HashMap::new();

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let failed = store.failed.lock().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, event_id);
        assert!(failed[0].1.contains("no handler"));
    }

    #[tokio::test(start_paused = true)]
    async fn worker_dead_letters_envelope_when_max_attempts_exhausted() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = MockStore::new(vec![envelope]);

        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(FailingHandler));
        let registry = registry_with(vec![handler]);

        let config = OutboxWorkerConfig {
            max_attempts: 1,
            batch_size: 1,
            ..OutboxWorkerConfig::default()
        };
        let worker = OutboxWorker::new(store.clone(), registry, config);
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let failed = store.failed.lock().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, event_id);
        let dead = store.dead_lettered.lock().unwrap();
        assert_eq!(dead.as_slice(), &[event_id]);
    }

    #[tokio::test(start_paused = true)]
    async fn worker_does_not_dead_letter_before_attempts_exhausted() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = MockStore::new(vec![envelope]);

        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(FailingHandler));
        let registry = registry_with(vec![handler]);

        let config = OutboxWorkerConfig {
            max_attempts: 3,
            batch_size: 1,
            ..OutboxWorkerConfig::default()
        };
        let worker = OutboxWorker::new(store.clone(), registry, config);
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let failed = store.failed.lock().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, event_id);
        assert!(
            store.dead_lettered.lock().unwrap().is_empty(),
            "should not dead-letter when attempts(1) < max_attempts(3)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_claims_envelopes_before_dispatch() {
        let envelopes = vec![
            fresh_envelope(Uuid::from_u128(1)),
            fresh_envelope(Uuid::from_u128(2)),
        ];
        let expected_ids: Vec<Uuid> = envelopes.iter().map(|e| e.event_id).collect();
        let store = MockStore::new(envelopes);

        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingHandler {
            seen: Arc::clone(&seen),
        }));
        let registry = registry_with(vec![handler]);

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let claimed = store.claimed.lock().unwrap();
        assert_eq!(
            claimed.as_slice(),
            expected_ids.as_slice(),
            "claim must be called with all polled ids"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_does_not_claim_when_batch_is_empty() {
        let store = MockStore::new(Vec::new());
        let registry = HashMap::new();

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        assert!(
            store.claimed.lock().unwrap().is_empty(),
            "claim must not be called when no envelopes are polled"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_aborts_poll_cycle_when_claim_fails_without_dispatching() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let store = MockStore::new(vec![envelope]);
        store.fail_claim.store(true, Ordering::Relaxed);

        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingHandler {
            seen: Arc::clone(&seen),
        }));
        let registry = registry_with(vec![handler]);

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        assert!(
            seen.lock().unwrap().is_empty(),
            "handler must not be called when claim fails"
        );
        assert!(
            store.delivered.lock().unwrap().is_empty(),
            "envelope must not be marked delivered when claim fails"
        );
        assert!(
            store.claimed.lock().unwrap().is_empty(),
            "claim ids must not be recorded when claim returns an error"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_derives_context_ids_from_event_id_stable_across_retries() {
        let event_id = Uuid::from_u128(99);
        let e1 = OutboxEnvelope::new(
            event_id,
            &UserRegistered {
                user_id: Uuid::from_u128(1),
            },
        )
        .unwrap();
        let e2 = OutboxEnvelope::new(
            event_id,
            &UserRegistered {
                user_id: Uuid::from_u128(2),
            },
        )
        .unwrap();
        let store = MockStore::new(vec![e1, e2]);

        let captured_ids = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn ErasedHandler> =
            Arc::new(TypedHandler::new(ContextCapturingHandler {
                captured_ids: Arc::clone(&captured_ids),
            }));
        let registry = registry_with(vec![handler]);

        let worker = OutboxWorker::new(store.clone(), registry, OutboxWorkerConfig::default());
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let ids = captured_ids.lock().unwrap();
        assert_eq!(ids.len(), 2, "both envelopes must be dispatched");
        assert_eq!(
            ids[0], ids[1],
            "message_id must be identical across dispatches of the same event_id"
        );
        assert_eq!(
            ids[0],
            MessageId::from(event_id),
            "message_id must equal MessageId::from(event_id)"
        );
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

    /// Handler that never resolves, used to prove `dispatch_timeout` is
    /// enforced as a hard deadline (#229).
    struct HangingHandler;
    impl Handler<UserRegistered> for HangingHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            std::future::pending::<()>().await;
            unreachable!("hanging handler never resolves")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn worker_enforces_dispatch_timeout_on_a_hung_handler() {
        // #229: a handler that never returns must not stall the worker. With
        // dispatch_timeout enforced, the envelope is marked failed instead.
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = MockStore::new(vec![envelope]);

        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(HangingHandler));
        let registry = registry_with(vec![handler]);

        let config = OutboxWorkerConfig {
            dispatch_timeout: Duration::from_millis(50),
            batch_size: 1,
            ..OutboxWorkerConfig::default()
        };
        let worker = OutboxWorker::new(store.clone(), registry, config);
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        // Advance virtual time well past the dispatch timeout.
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        let failed = store.failed.lock().unwrap();
        assert_eq!(failed.len(), 1, "hung handler must be recorded as failed");
        assert_eq!(failed[0].0, event_id);
        assert!(
            failed[0].1.contains("timed out"),
            "failure must be the dispatch timeout, got {:?}",
            failed[0].1
        );
        assert!(
            store.delivered.lock().unwrap().is_empty(),
            "a timed-out envelope must not be marked delivered"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_settles_remaining_batch_when_one_ack_fails() {
        // #231: a transient ack failure on one envelope must not abandon the
        // rest of the claimed batch.
        let e1 = fresh_envelope(Uuid::from_u128(1));
        let e2 = fresh_envelope(Uuid::from_u128(2));
        let id1 = e1.event_id;
        let id2 = e2.event_id;
        let store = MockStore::new(vec![e1, e2]);
        *store.fail_mark_delivered_for.lock().unwrap() = Some(id1);

        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingHandler {
            seen: Arc::clone(&seen),
        }));
        let registry = registry_with(vec![handler]);

        let config = OutboxWorkerConfig {
            batch_size: 2,
            ..OutboxWorkerConfig::default()
        };
        let worker = OutboxWorker::new(store.clone(), registry, config);
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        store.await_polls(2).await;
        cancel.cancel();
        join.await.unwrap().unwrap();

        // The first envelope's ack failed (transient), but the second must
        // still have been dispatched and marked delivered.
        let delivered = store.delivered.lock().unwrap();
        assert!(
            delivered.contains(&id2),
            "second envelope must be delivered despite the first ack failing, got {delivered:?}"
        );
        assert!(
            seen.lock().unwrap().len() >= 2,
            "both envelopes must have been dispatched to the handler"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_observes_cancellation_during_a_long_poll_interval() {
        // #231 defect 1: cancellation arriving during the poll sleep must be
        // observed promptly, not after the full poll_interval elapses.
        let store = MockStore::new(Vec::new());
        let registry = HashMap::new();
        let config = OutboxWorkerConfig {
            poll_interval: Duration::from_secs(3600),
            ..OutboxWorkerConfig::default()
        };
        let worker = OutboxWorker::new(store, registry, config);
        let cancel = CancellationToken::new();
        let join = tokio::spawn(worker.run(cancel.clone()));

        // Let the worker reach the sleep, then cancel mid-sleep.
        tokio::time::sleep(Duration::from_millis(1)).await;
        cancel.cancel();

        // Without racing the sleep against cancellation this would hang for the
        // full hour of virtual time; with the fix it returns immediately.
        tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("worker must stop without waiting out the poll interval")
            .unwrap()
            .unwrap();
    }

    #[test]
    fn lease_for_scales_with_batch_size() {
        // #215: the batch lease must cover the worst-case sequential dispatch
        // of the whole batch, i.e. batch_len * dispatch_timeout.
        let config = OutboxWorkerConfig {
            dispatch_timeout: Duration::from_secs(30),
            ..OutboxWorkerConfig::default()
        };
        let store = MockStore::new(Vec::new());
        let worker = OutboxWorker::new(store, HashMap::new(), config);

        assert_eq!(worker.lease_for(1), Duration::from_secs(30));
        assert_eq!(worker.lease_for(10), Duration::from_secs(300));
        // An empty batch is never claimed, but the helper must stay safe.
        assert_eq!(worker.lease_for(0), Duration::from_secs(30));
    }

    /// Writer handed to a tracing subscriber so a test can read what was logged.
    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl LogBuffer {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }

        /// Lines logged at warning level or above that mention `event_id`.
        fn warning_lines_mentioning(&self, event_id: Uuid) -> usize {
            let needle = event_id.to_string();
            self.text()
                .lines()
                .filter(|line| line.contains(&needle))
                .filter(|line| line.contains("WARN") || line.contains("ERROR"))
                .count()
        }
    }

    /// Capture everything the worker logs at warning level or above, for as
    /// long as the returned guard lives. The subscriber is thread local, so it
    /// only sees a worker driven on the test's own task.
    fn capture_warnings() -> (LogBuffer, tracing::subscriber::DefaultGuard) {
        let buffer = LogBuffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        (buffer, tracing::subscriber::set_default(subscriber))
    }

    /// Store that simulates a lease lost to another worker.
    ///
    /// It can refuse to grant part of a claim, and it can report any
    /// settlement as not applied, which is what a real backend returns when
    /// the stored attempt no longer matches the caller's fence.
    /// One call to `claim`: the candidates paired with the attempts they were polled at.
    type ClaimRequest = Vec<(Uuid, u32)>;

    #[derive(Clone)]
    struct FencedStore {
        pending: Arc<Mutex<Vec<OutboxEnvelope>>>,
        granted_claims: Option<Vec<Uuid>>,
        delivery_applies: bool,
        failure_applies: bool,
        dead_lettering_applies: bool,
        claim_requests: Arc<Mutex<Vec<ClaimRequest>>>,
        delivered_calls: Arc<Mutex<Vec<(Uuid, u32)>>>,
        failed_calls: Arc<Mutex<Vec<(Uuid, u32)>>>,
        dead_letter_calls: Arc<Mutex<Vec<(Uuid, u32)>>>,
    }

    impl FencedStore {
        fn new(initial: Vec<OutboxEnvelope>) -> Self {
            Self {
                pending: Arc::new(Mutex::new(initial)),
                granted_claims: None,
                delivery_applies: true,
                failure_applies: true,
                dead_lettering_applies: true,
                claim_requests: Arc::new(Mutex::new(Vec::new())),
                delivered_calls: Arc::new(Mutex::new(Vec::new())),
                failed_calls: Arc::new(Mutex::new(Vec::new())),
                dead_letter_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Grant the claim only for `event_ids`: the others were won by
        /// another worker between the poll and the claim.
        fn granting_only(mut self, event_ids: Vec<Uuid>) -> Self {
            self.granted_claims = Some(event_ids);
            self
        }

        fn losing_the_delivery_lease(mut self) -> Self {
            self.delivery_applies = false;
            self
        }

        fn losing_the_failure_lease(mut self) -> Self {
            self.failure_applies = false;
            self
        }

        fn losing_the_dead_lettering_lease(mut self) -> Self {
            self.dead_lettering_applies = false;
            self
        }
    }

    #[async_trait::async_trait]
    impl OutboxStore for FencedStore {
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
            claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            self.delivered_calls
                .lock()
                .unwrap()
                .push((event_id, claimed_attempt));
            Ok(self.delivery_applies)
        }

        async fn mark_failed<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            event_id: Uuid,
            _error: &str,
            _retry_in: Duration,
            claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            self.failed_calls
                .lock()
                .unwrap()
                .push((event_id, claimed_attempt));
            Ok(self.failure_applies)
        }

        async fn mark_dead_lettered<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            event_id: Uuid,
            _error: &str,
            claimed_attempt: u32,
        ) -> Result<bool, OutboxError> {
            self.dead_letter_calls
                .lock()
                .unwrap()
                .push((event_id, claimed_attempt));
            Ok(self.dead_lettering_applies)
        }

        async fn commit<'a>(&self, _tx: Self::Tx<'a>) -> Result<(), OutboxError> {
            Ok(())
        }

        async fn claim<'a>(
            &self,
            _tx: &mut Self::Tx<'a>,
            claims: &[(Uuid, u32)],
            _lease_for: Duration,
        ) -> Result<Vec<Uuid>, OutboxError> {
            self.claim_requests.lock().unwrap().push(claims.to_vec());
            Ok(claims
                .iter()
                .map(|(event_id, _)| *event_id)
                .filter(|event_id| {
                    self.granted_claims
                        .as_ref()
                        .is_none_or(|granted| granted.contains(event_id))
                })
                .collect())
        }
    }

    /// Run exactly one poll cycle of a worker over `store`.
    async fn run_one_cycle(
        store: &FencedStore,
        handler: Arc<dyn ErasedHandler>,
        config: OutboxWorkerConfig,
    ) -> Result<usize, OutboxError> {
        let worker = OutboxWorker::new(store.clone(), registry_with(vec![handler]), config);
        worker.poll_cycle(&CancellationToken::new()).await
    }

    fn recording_handler() -> (Arc<dyn ErasedHandler>, Arc<Mutex<Vec<Uuid>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingHandler {
            seen: Arc::clone(&seen),
        }));
        (handler, seen)
    }

    fn failing_handler() -> Arc<dyn ErasedHandler> {
        Arc::new(TypedHandler::new(FailingHandler))
    }

    fn single_attempt_config() -> OutboxWorkerConfig {
        OutboxWorkerConfig {
            max_attempts: 1,
            batch_size: 10,
            ..OutboxWorkerConfig::default()
        }
    }

    /// Guard for the token itself (criteria 1 to 3 rely on it): the fence the
    /// worker presents is the attempt it polled plus the one its claim wrote.
    #[tokio::test]
    async fn worker_fences_with_the_polled_attempts_plus_one() {
        let mut delivered = fresh_envelope(Uuid::from_u128(1));
        delivered.attempts = 3;
        let mut failed = fresh_envelope(Uuid::from_u128(2));
        failed.attempts = 3;
        let delivered_id = delivered.event_id;
        let failed_id = failed.event_id;

        let store = FencedStore::new(vec![delivered]);
        let (handler, _seen) = recording_handler();
        run_one_cycle(&store, handler, OutboxWorkerConfig::default())
            .await
            .unwrap();
        assert_eq!(
            store.claim_requests.lock().unwrap().as_slice(),
            &[vec![(delivered_id, 3)]],
            "claim must present the attempts value read by the poll"
        );
        assert_eq!(
            store.delivered_calls.lock().unwrap().as_slice(),
            &[(delivered_id, 4)]
        );

        let store = FencedStore::new(vec![failed]);
        run_one_cycle(&store, failing_handler(), OutboxWorkerConfig::default())
            .await
            .unwrap();
        assert_eq!(
            store.failed_calls.lock().unwrap().as_slice(),
            &[(failed_id, 4)]
        );
    }

    /// Criterion 4: a `mark_failed` that reports `false` forbids
    /// `mark_dead_lettered`. A worker whose lease expired would otherwise
    /// dead-letter an envelope that another worker holds, and may already
    /// have delivered.
    #[tokio::test]
    async fn lost_failure_settlement_forbids_dead_lettering() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let store = FencedStore::new(vec![envelope]).losing_the_failure_lease();

        let outcome = run_one_cycle(&store, failing_handler(), single_attempt_config()).await;

        assert!(outcome.is_ok());
        assert_eq!(
            store.failed_calls.lock().unwrap().len(),
            1,
            "the failure settlement must have been attempted"
        );
        assert!(
            store.dead_letter_calls.lock().unwrap().is_empty(),
            "mark_dead_lettered must not run after mark_failed returned false"
        );
    }

    /// Control for criterion 4: when the failure does apply, an exhausted
    /// envelope is dead-lettered with the same fence.
    #[tokio::test]
    async fn applied_failure_settlement_still_dead_letters_an_exhausted_envelope() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = FencedStore::new(vec![envelope]);

        run_one_cycle(&store, failing_handler(), single_attempt_config())
            .await
            .unwrap();

        assert_eq!(
            store.dead_letter_calls.lock().unwrap().as_slice(),
            &[(event_id, 1)]
        );
    }

    /// Criterion 6: an envelope read by the poll but not returned by the
    /// claim belongs to another worker. It is not dispatched, not settled,
    /// and not logged as a problem: that is a normal concurrent batch.
    #[tokio::test]
    async fn worker_does_not_dispatch_an_envelope_the_claim_did_not_return() {
        let owned = fresh_envelope(Uuid::from_u128(1));
        let taken = fresh_envelope(Uuid::from_u128(2));
        let owned_id = owned.event_id;
        let taken_id = taken.event_id;
        let store = FencedStore::new(vec![owned, taken]).granting_only(vec![owned_id]);
        let (handler, seen) = recording_handler();
        let (logs, _guard) = capture_warnings();

        run_one_cycle(&store, handler, OutboxWorkerConfig::default())
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[Uuid::from_u128(1)],
            "only the envelope the claim returned may reach the handler"
        );
        assert_eq!(
            store.delivered_calls.lock().unwrap().as_slice(),
            &[(owned_id, 1)],
            "only the claimed envelope may be settled"
        );
        assert_eq!(
            logs.warning_lines_mentioning(taken_id),
            0,
            "losing a claim to another worker is not worth a warning"
        );
    }

    /// Criterion 6, failure side: an unclaimed envelope is not settled as
    /// failed either, whatever its handler would have done.
    #[tokio::test]
    async fn unclaimed_envelope_is_never_settled_as_failed() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = FencedStore::new(vec![envelope]).granting_only(Vec::new());
        let (logs, _guard) = capture_warnings();

        let outcome = run_one_cycle(&store, failing_handler(), single_attempt_config()).await;

        assert!(outcome.is_ok());
        assert!(store.failed_calls.lock().unwrap().is_empty());
        assert!(store.dead_letter_calls.lock().unwrap().is_empty());
        assert_eq!(logs.warning_lines_mentioning(event_id), 0);
    }

    /// Criterion 8: a delivery that did not apply is observable through a
    /// warning naming the envelope, is not an error, and still counts as a
    /// processed envelope.
    #[tokio::test]
    async fn lost_delivery_lease_is_warned_and_counted_as_processed() {
        let first = fresh_envelope(Uuid::from_u128(1));
        let second = fresh_envelope(Uuid::from_u128(2));
        let first_id = first.event_id;
        let second_id = second.event_id;
        let store = FencedStore::new(vec![first, second]).losing_the_delivery_lease();
        let (handler, _seen) = recording_handler();
        let (logs, _guard) = capture_warnings();

        let outcome = run_one_cycle(&store, handler, OutboxWorkerConfig::default()).await;

        assert_eq!(outcome.unwrap(), 2, "both envelopes were dequeued");
        assert_eq!(
            store.delivered_calls.lock().unwrap().len(),
            2,
            "the rest of the batch is still settled after a lost lease"
        );
        assert!(
            logs.warning_lines_mentioning(first_id) >= 1,
            "a lost delivery lease must be logged as a warning naming the envelope"
        );
        assert!(logs.warning_lines_mentioning(second_id) >= 1);
    }

    /// Criterion 8, failure side. The handler failure already logs one
    /// warning for the envelope, so the lost lease must add a second.
    #[tokio::test]
    async fn lost_failure_lease_is_warned_and_counted_as_processed() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = FencedStore::new(vec![envelope]).losing_the_failure_lease();
        let (logs, _guard) = capture_warnings();

        let outcome = run_one_cycle(&store, failing_handler(), OutboxWorkerConfig::default()).await;

        assert_eq!(outcome.unwrap(), 1);
        let warnings = logs
            .text()
            .lines()
            .filter(|line| line.contains(&event_id.to_string()) && line.contains("WARN"))
            .count();
        assert!(
            warnings >= 2,
            "the handler failure and the lost lease must each be warned, got: {}",
            logs.text()
        );
    }

    /// Criterion 8, dead-letter side: the failure applied but the move to the
    /// dead-letter store did not. The warning is distinct from the handler
    /// failure and from the retry-budget error.
    #[tokio::test]
    async fn lost_dead_lettering_lease_is_warned_and_counted_as_processed() {
        let envelope = fresh_envelope(Uuid::from_u128(1));
        let event_id = envelope.event_id;
        let store = FencedStore::new(vec![envelope]).losing_the_dead_lettering_lease();
        let (logs, _guard) = capture_warnings();

        let outcome = run_one_cycle(&store, failing_handler(), single_attempt_config()).await;

        assert_eq!(outcome.unwrap(), 1);
        assert_eq!(store.dead_letter_calls.lock().unwrap().len(), 1);
        let warnings = logs
            .text()
            .lines()
            .filter(|line| line.contains(&event_id.to_string()) && line.contains("WARN"))
            .count();
        assert!(
            warnings >= 2,
            "the handler failure and the lost dead-lettering lease must each be warned, got: {}",
            logs.text()
        );
    }

    /// Criterion 9: a store that takes no lease ignores the fence. The
    /// default `claim` hands back every candidate whatever attempt it carries,
    /// and the default `mark_dead_lettered` applies whatever fence it is given.
    #[tokio::test]
    async fn store_without_a_lease_ignores_the_fence() {
        let store = PacingStore::new(Vec::new());
        let mut tx = MockTx;
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);

        let claimed = store
            .claim(&mut tx, &[(first, 0), (second, 9)], Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claimed, vec![first, second]);

        for fence in [0, 7, u32::MAX] {
            assert!(
                store
                    .mark_dead_lettered(&mut tx, first, "error", fence)
                    .await
                    .unwrap(),
                "the default dead-lettering must ignore claimed_attempt {fence}"
            );
        }
    }
}
