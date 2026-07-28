//! Backend-agnostic scenarios exercised by every dialect integration test.
//!
//! Each scenario drives a freshly set up outbox through the [`hexeract_outbox`]
//! contracts only, so the same behaviour is asserted identically against
//! Postgres, MySQL and SQLite. A dialect test file owns its container or file
//! setup, implements [`Backend`], and instantiates these functions.
//!
//! Scenarios never sleep for a fixed duration. They wait for the worker to
//! report progress, either through [`Recorder`] or by probing the database,
//! and fail with a named timeout when that progress never comes. A scenario
//! that asserts an absence publishes a witness event and waits for it, which
//! proves the worker completed a full poll cycle rather than assuming a delay
//! was long enough.

#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use hexeract_core::HandlerContext;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::IdempotentOutboxEnqueue;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxStore;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Outbox table every dialect test file creates.
pub(crate) const TABLE: &str = "audit_outbox";

/// Upper bound on any wait for the worker to make progress. Generous enough
/// for a loaded continuous integration runner, since a healthy run never
/// spends it: the wait ends as soon as the expected progress is observed.
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(10);

/// Delay between two probes of the database while waiting for a condition.
const PROBE_INTERVAL: Duration = Duration::from_millis(20);

/// Sample event persisted by the scenarios.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct UserRegistered {
    pub(crate) user_id: Uuid,
    pub(crate) email: String,
}

impl Event for UserRegistered {
    const EVENT_TYPE: &'static str = "users.registered";
}

/// What a dialect test file provides so the shared scenarios can run against
/// it. Everything dialect specific lives behind this trait: the pool type,
/// transaction handling, and the verification queries.
pub(crate) trait Backend
where
    for<'tx> <Self::Store as OutboxStore>::Tx<'tx>: Send,
{
    /// Store under test, already bound to [`TABLE`].
    type Store: OutboxStore + Clone;

    /// Publisher under test, already bound to [`TABLE`].
    type Publisher: OutboxPublisher + IdempotentOutboxEnqueue;

    /// A store over the same database as every other handle this backend hands
    /// out. Called more than once by scenarios that run competing workers.
    fn store(&self) -> Self::Store;

    /// A publisher over the same database as [`Backend::store`].
    fn publisher(&self) -> Self::Publisher;

    /// Publish `event` inside a transaction, roll that transaction back, and
    /// return the identifier the publisher assigned.
    async fn publish_then_rollback(&self, event: &UserRegistered) -> Uuid;

    /// Rows carrying `event_id`, delivered or not.
    async fn row_count(&self, event_id: Uuid) -> i64;

    /// Rows carrying `event_id` whose `delivered_at` is set.
    async fn delivered_count(&self, event_id: Uuid) -> i64;

    /// Every delivered row in the table, whatever its identifier.
    async fn total_delivered(&self) -> i64;

    /// Current `attempts` for `event_id`.
    async fn attempts(&self, event_id: Uuid) -> i64;

    /// Whether `next_retry_at` is set for `event_id`.
    async fn has_next_retry_at(&self, event_id: Uuid) -> bool;

    /// Push `next_retry_at` for `event_id` one day into the future, using the
    /// clock of the database itself rather than the clock of the test process.
    async fn defer_next_retry_by_one_day(&self, event_id: Uuid);
}

/// Handler that records the events it saw and lets a scenario await progress
/// instead of sleeping for a fixed duration.
#[derive(Clone, Default)]
pub(crate) struct Recorder {
    seen: Arc<Mutex<Vec<UserRegistered>>>,
    progress: Arc<Notify>,
}

impl Recorder {
    /// A recorder that has seen nothing yet.
    fn new() -> Self {
        Self::default()
    }

    /// Events handled so far.
    fn count(&self) -> usize {
        self.seen.lock().expect("recorder mutex").len()
    }

    /// Events handled so far whose address is `email`.
    fn count_for(&self, email: &str) -> usize {
        self.seen
            .lock()
            .expect("recorder mutex")
            .iter()
            .filter(|event| event.email == email)
            .count()
    }

    /// Wait until at least `expected` events have been handled.
    ///
    /// # Panics
    ///
    /// Panics when the worker fails to reach `expected` within
    /// [`PROGRESS_TIMEOUT`].
    async fn await_count(&self, expected: usize) {
        let wait = async {
            loop {
                // Subscribe before reading the count, so a notification fired
                // between the two is not lost.
                let notified = self.progress.notified();
                if self.count() >= expected {
                    return;
                }
                notified.await;
            }
        };
        let outcome = tokio::time::timeout(PROGRESS_TIMEOUT, wait).await;
        assert!(
            outcome.is_ok(),
            "timed out waiting for {expected} handled event(s), saw {}",
            self.count()
        );
    }
}

impl Handler<UserRegistered> for Recorder {
    type Error = OutboxError;

    async fn handle(
        &self,
        event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.seen.lock().expect("recorder mutex").push(event);
        self.progress.notify_waiters();
        Ok(())
    }
}

/// Handler that always fails, and reports each attempt so a scenario can wait
/// for the worker to have tried at least once.
#[derive(Clone, Default)]
struct FailingHandler {
    attempts: Arc<Mutex<usize>>,
    progress: Arc<Notify>,
}

impl Handler<UserRegistered> for FailingHandler {
    type Error = OutboxError;

    async fn handle(
        &self,
        _event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        *self.attempts.lock().expect("attempts mutex") += 1;
        self.progress.notify_waiters();
        Err(OutboxError::Internal("forced failure".to_owned()))
    }
}

/// A worker running in the background for the duration of a scenario.
struct RunningWorker {
    cancel: CancellationToken,
    join: JoinHandle<Result<(), OutboxError>>,
}

impl RunningWorker {
    /// Spawn a worker dispatching [`UserRegistered`] to `handler`.
    fn spawn<S, H>(store: S, handler: H, config: OutboxWorkerConfig) -> Self
    where
        S: OutboxStore,
        for<'tx> S::Tx<'tx>: Send,
        H: Handler<UserRegistered>,
    {
        let cancel = CancellationToken::new();
        let worker = OutboxWorker::new(store, registry_with(handler), config);
        let join = tokio::spawn(worker.run(cancel.clone()));
        Self { cancel, join }
    }

    /// Stop the worker and propagate whatever it returned.
    ///
    /// # Panics
    ///
    /// Panics when the worker task died or its run returned an error.
    async fn shutdown(self) {
        self.cancel.cancel();
        self.join
            .await
            .expect("worker task must not panic")
            .expect("worker run must succeed");
    }
}

/// A worker configuration that polls often enough for a scenario to observe
/// progress promptly, and never retries within the life of that scenario.
fn eager_polling() -> OutboxWorkerConfig {
    OutboxWorkerConfig {
        poll_interval: Duration::from_millis(20),
        ..OutboxWorkerConfig::default()
    }
}

/// Wait until `probe` reports true.
///
/// # Panics
///
/// Panics when `probe` never reports true within [`PROGRESS_TIMEOUT`], naming
/// `expectation` so the failure reads as a missing behaviour rather than as a
/// bare timeout.
async fn await_condition<P, F>(expectation: &str, mut probe: P)
where
    P: FnMut() -> F,
    F: Future<Output = bool>,
{
    let wait = async {
        loop {
            if probe().await {
                return;
            }
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    };
    let outcome = tokio::time::timeout(PROGRESS_TIMEOUT, wait).await;
    assert!(
        outcome.is_ok(),
        "timed out after {PROGRESS_TIMEOUT:?} waiting for {expectation}"
    );
}

/// A registry routing [`UserRegistered`] to `handler`.
fn registry_with<H>(handler: H) -> HashMap<&'static str, Arc<dyn ErasedHandler>>
where
    H: Handler<UserRegistered>,
{
    let mut map = HashMap::new();
    let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(handler));
    map.insert(erased.event_type(), erased);
    map
}

/// An event addressed to `email`.
fn sample(email: &str) -> UserRegistered {
    UserRegistered {
        user_id: Uuid::now_v7(),
        email: email.to_owned(),
    }
}

/// Publish a witness event and wait for it to be handled.
///
/// A scenario asserting that some event is *not* dispatched cannot wait for
/// something that must never happen. It publishes a witness instead: once the
/// witness has been handled, the worker has completed a full poll cycle, so an
/// unwanted dispatch would already have been recorded.
async fn drive_one_full_cycle<B: Backend>(backend: &B, recorder: &Recorder)
where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    const WITNESS: &str = "witness@example.com";

    let before = recorder.count();
    backend
        .publisher()
        .publish(&sample(WITNESS))
        .await
        .expect("witness must publish");
    recorder.await_count(before + 1).await;
    assert_eq!(
        recorder.count_for(WITNESS),
        1,
        "the witness event must be the one that completed the cycle"
    );
}

/// A transaction rolled back after a publish leaves no row behind.
pub(crate) async fn publish_in_tx_rollback_discards_the_insert<B: Backend>(backend: &B)
where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    let event_id = backend
        .publish_then_rollback(&sample("rollback@example.com"))
        .await;

    assert_eq!(
        backend.row_count(event_id).await,
        0,
        "a rolled back transaction must leave no outbox row"
    );
}

/// A published event reaches its handler and its row is marked delivered.
pub(crate) async fn worker_dispatches_published_event_and_marks_delivered<B: Backend>(backend: &B)
where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    let event_id = backend
        .publisher()
        .publish(&sample("alice@example.com"))
        .await
        .expect("publish must succeed");

    let recorder = Recorder::new();
    let worker = RunningWorker::spawn(backend.store(), recorder.clone(), eager_polling());

    recorder.await_count(1).await;
    await_condition("the delivered row to be marked", || async {
        backend.delivered_count(event_id).await == 1
    })
    .await;

    worker.shutdown().await;
    assert_eq!(
        recorder.count(),
        1,
        "the event must be handled exactly once"
    );
}

/// A failing handler leaves the row undelivered, consumes an attempt, and
/// schedules a retry.
pub(crate) async fn worker_marks_failed_and_increments_attempts_on_handler_error<B: Backend>(
    backend: &B,
) where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    let event_id = backend
        .publisher()
        .publish(&sample("bob@example.com"))
        .await
        .expect("publish must succeed");

    // A retry delay longer than the scenario keeps the failure to a single
    // attempt, so the assertions below observe a settled state.
    let config = OutboxWorkerConfig {
        poll_interval: Duration::from_millis(20),
        retry_base_delay: Duration::from_secs(60),
        retry_max_delay: Duration::from_secs(60),
        jitter: false,
        ..OutboxWorkerConfig::default()
    };
    let worker = RunningWorker::spawn(backend.store(), FailingHandler::default(), config);

    await_condition("the failed attempt to be recorded", || async {
        backend.attempts(event_id).await >= 1
    })
    .await;
    await_condition("the retry to be scheduled", || async {
        backend.has_next_retry_at(event_id).await
    })
    .await;

    worker.shutdown().await;
    assert_eq!(
        backend.delivered_count(event_id).await,
        0,
        "a failed event must not be marked delivered"
    );
}

/// An event whose retry is scheduled in the future stays out of the poll.
pub(crate) async fn future_next_retry_at_excludes_event_from_poll<B: Backend>(backend: &B)
where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    const DEFERRED: &str = "carol@example.com";

    let event_id = backend
        .publisher()
        .publish(&sample(DEFERRED))
        .await
        .expect("publish must succeed");
    backend.defer_next_retry_by_one_day(event_id).await;

    let recorder = Recorder::new();
    let worker = RunningWorker::spawn(backend.store(), recorder.clone(), eager_polling());

    drive_one_full_cycle(backend, &recorder).await;

    worker.shutdown().await;
    assert_eq!(
        recorder.count_for(DEFERRED),
        0,
        "an event scheduled in the future must not be dispatched"
    );
    assert_eq!(
        backend.delivered_count(event_id).await,
        0,
        "an event scheduled in the future must not be marked delivered"
    );
}

/// Enqueuing the same identifier twice inserts a single row.
pub(crate) async fn enqueue_idempotent_twice_inserts_one_row<B: Backend>(backend: &B)
where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    let publisher = backend.publisher();
    let event_id = Uuid::now_v7();

    let inserted = publisher
        .enqueue_idempotent(event_id, "x.due", b"{\"k\":1}")
        .await
        .expect("first enqueue must succeed");
    assert!(inserted, "first enqueue must insert a new row");

    let duplicate = publisher
        .enqueue_idempotent(event_id, "x.due", b"{\"k\":1}")
        .await
        .expect("second enqueue must succeed");
    assert!(
        !duplicate,
        "second enqueue with the same event_id must be a no-op"
    );

    assert_eq!(
        backend.row_count(event_id).await,
        1,
        "exactly one row must exist after two enqueues"
    );
}

/// An event enqueued twice under one identifier is dispatched once.
pub(crate) async fn idempotent_enqueue_delivered_once_by_worker<B: Backend>(backend: &B)
where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    const IDEMPOTENT: &str = "idem@example.com";

    let publisher = backend.publisher();
    let event_id = Uuid::now_v7();
    let payload = format!(
        "{{\"user_id\":\"00000000-0000-0000-0000-000000000001\",\"email\":\"{IDEMPOTENT}\"}}"
    );
    for _ in 0..2 {
        publisher
            .enqueue_idempotent(event_id, UserRegistered::EVENT_TYPE, payload.as_bytes())
            .await
            .expect("enqueue must succeed");
    }

    let recorder = Recorder::new();
    let worker = RunningWorker::spawn(backend.store(), recorder.clone(), eager_polling());

    recorder.await_count(1).await;
    await_condition("the delivered row to be marked", || async {
        backend.delivered_count(event_id).await == 1
    })
    .await;
    // A second dispatch would happen on a later cycle, so let one elapse
    // before concluding that it never comes.
    drive_one_full_cycle(backend, &recorder).await;

    worker.shutdown().await;
    assert_eq!(
        recorder.count_for(IDEMPOTENT),
        1,
        "the event must be dispatched exactly once despite two enqueues"
    );
    assert_eq!(
        backend.row_count(event_id).await,
        1,
        "two enqueues must leave a single row"
    );
}

/// Two workers polling the same table dispatch each event exactly once.
///
/// Relies on `FOR UPDATE SKIP LOCKED`, so only the Postgres and MySQL files
/// instantiate it. SQLite has no such lock and its store stays single-writer
/// by design, as its module documentation states.
pub(crate) async fn multi_worker_skip_locked_prevents_double_dispatch<B: Backend>(backend: &B)
where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    const EVENT_COUNT: usize = 20;

    let publisher = backend.publisher();
    for index in 0..EVENT_COUNT {
        publisher
            .publish(&sample(&format!("user{index}@example.com")))
            .await
            .expect("publish must succeed");
    }

    let competing = || OutboxWorkerConfig {
        poll_interval: Duration::from_millis(20),
        batch_size: 5,
        ..OutboxWorkerConfig::default()
    };
    let recorder_a = Recorder::new();
    let recorder_b = Recorder::new();
    let worker_a = RunningWorker::spawn(backend.store(), recorder_a.clone(), competing());
    let worker_b = RunningWorker::spawn(backend.store(), recorder_b.clone(), competing());

    let expected = i64::try_from(EVENT_COUNT).expect("event count fits in i64");
    await_condition("every event to be delivered", || async {
        backend.total_delivered().await == expected
    })
    .await;

    worker_a.shutdown().await;
    worker_b.shutdown().await;

    assert_eq!(
        recorder_a.count() + recorder_b.count(),
        EVENT_COUNT,
        "each event must be dispatched exactly once across competing workers"
    );
}

/// Claiming a batch consumes a retry slot on its own.
///
/// #213: `attempts` is incremented at claim time rather than only on failure,
/// so a worker that dies between the claim and the acknowledgement has already
/// burnt an attempt. Without it, a poison row would be redelivered forever
/// instead of reaching the dead-letter threshold. [`OutboxStore::claim`] has a
/// no-op default implementation, so a backend that forgets to override it
/// compiles and silently loses this protection.
pub(crate) async fn claim_consumes_a_retry_slot_even_without_a_clean_failure<B: Backend>(
    backend: &B,
) where
    for<'tx> <B::Store as OutboxStore>::Tx<'tx>: Send,
{
    let event_id = backend
        .publisher()
        .publish(&sample("erin@example.com"))
        .await
        .expect("publish must succeed");

    // Claim directly, with no dispatch, to simulate a worker that dies between
    // the claim and the acknowledgement.
    let store = backend.store();
    let mut client = store.acquire().await.expect("acquire must succeed");
    let mut tx = store.begin(&mut client).await.expect("begin must succeed");
    store
        .claim(&mut tx, &[event_id], Duration::from_secs(30))
        .await
        .expect("claim must succeed");
    store.commit(tx).await.expect("commit must succeed");

    assert_eq!(
        backend.attempts(event_id).await,
        1,
        "claiming alone must consume one retry slot (crash safety, #213)"
    );
}
