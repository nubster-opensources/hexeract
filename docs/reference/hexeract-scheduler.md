# `hexeract-scheduler` API reference

Backend-agnostic scheduling primitives: the outbox plus time, persisting a message together with the instant it is due and an optional recurrence rule, then relaying it once that instant is reached.

## Public surface

### Scheduling API

```rust
pub struct ScheduledMessage {
    pub schedule_id: Uuid,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub target: Target,
    pub trigger: Trigger,
    pub scheduled_for: SystemTime,
}

impl ScheduledMessage {
    pub fn delay<E: Event>(
        target: Target,
        at: SystemTime,
        event: &E,
    ) -> Result<Self, SchedulerError>;

    pub fn cron<E: Event>(
        target: Target,
        expression: &str,
        first_occurrence: SystemTime,
        event: &E,
    ) -> Result<Self, SchedulerError>;

    pub fn occurrence_id(&self) -> OccurrenceId;
}
```

`delay` builds a message that fires once at `at`; `cron` builds one that fires repeatedly on `expression`, with its first occurrence at `first_occurrence`. Both mint the schedule identifier as a `UUIDv7` and encode `event` as JSON. `occurrence_id()` is derived from `schedule_id` and `scheduled_for`: it is the stable deduplication key consumers use under the at-least-once delivery contract.

### `Trigger` and `CronExpression`

```rust
pub enum Trigger {
    Delay(SystemTime),
    Cron(CronExpression),
}

impl Trigger {
    pub fn delay(at: SystemTime) -> Self;
    pub fn cron(expression: &str) -> Result<Self, SchedulerError>;
    pub fn is_recurring(&self) -> bool;
    pub fn kind(&self) -> &'static str;
}

pub struct CronExpression(String);

impl CronExpression {
    pub fn parse(expression: &str) -> Result<Self, SchedulerError>;
    pub fn next_occurrence(&self, after: SystemTime) -> Result<Option<SystemTime>, SchedulerError>;
}
```

`Trigger::delay` fires once; `Trigger::cron` fires repeatedly and is rejected with `SchedulerError::InvalidTrigger` if `expression` is not structurally valid. `is_recurring()` distinguishes the two, `kind()` returns a stable lowercase tag (`"delay"` or `"cron"`). `CronExpression::parse` accepts a five field expression, a six field expression with a leading seconds field, or a supported macro such as `@daily`, all validated through the `isochron` cron engine. `next_occurrence` returns the next occurrence strictly after `after`, evaluated in UTC, or `None` when the engine's bounded search horizon finds nothing.

### `Target`

```rust
pub enum Target {
    Mediator,
    Outbox,
    Bus { routing_key: String },
}

impl Target {
    pub fn mediator() -> Self;
    pub fn outbox() -> Self;
    pub fn bus(routing_key: impl Into<String>) -> Self;
}
```

`Target` is the dispatch destination the worker routes a due occurrence to: in-process through the mediator, transactionally into the outbox, or onto the message bus under a routing key.

### Store contract

```rust
#[trait_variant::make(Send)]
pub trait ScheduleStore: Send + Sync + 'static {
    async fn insert(&self, message: &ScheduledMessage, max_attempts: u32) -> Result<(), SchedulerError>;
    async fn claim_due(&self, now: SystemTime, batch_size: usize, lease: Duration) -> Result<Vec<LeasedOccurrence>, SchedulerError>;
    async fn mark_delivered(&self, schedule_id: Uuid) -> Result<(), SchedulerError>;
    async fn reschedule(&self, schedule_id: Uuid, next: SystemTime) -> Result<(), SchedulerError>;
    async fn mark_failed(&self, schedule_id: Uuid, retry_at: SystemTime, error: &str) -> Result<(), SchedulerError>;
    async fn mark_dead_lettered(&self, schedule_id: Uuid, error: &str) -> Result<(), SchedulerError>;
    async fn cancel(&self, schedule_id: Uuid) -> Result<(), SchedulerError>;
    async fn set_paused(&self, schedule_id: Uuid, paused: bool) -> Result<(), SchedulerError>;
    async fn inspect(&self, schedule_id: Uuid) -> Result<Option<ScheduleSnapshot>, SchedulerError>;
    async fn resume(&self, schedule_id: Uuid, next: Option<SystemTime>) -> Result<(), SchedulerError>;
}

#[trait_variant::make(Send)]
pub trait ScheduleAdmin: ScheduleStore {
    async fn list_pending(&self, limit: usize) -> Result<Vec<ScheduleSnapshot>, SchedulerError>;
    async fn list_dead_letter(&self, limit: usize) -> Result<Vec<ScheduleSnapshot>, SchedulerError>;
    async fn replay(&self, schedule_id: Uuid) -> Result<(), SchedulerError>;
}
```

`ScheduleStore` is the backend-agnostic persistence contract: `claim_due` atomically selects due, unleased, eligible occurrences, advancing their attempt counter and stamping a fresh lease, which is what makes at-least-once delivery crash-safe. `ScheduleAdmin` extends it with the operator surface (listing and dead-letter replay), kept separate so the worker hot path never depends on it. Both are backend-facing contracts; application code drives a schedule's lifecycle through `SchedulerControl` instead.

### Worker construction

```rust
impl<S: ScheduleStore, K: ScheduleSink> SchedulerBuilder<S, K> {
    pub fn new(store: S, sink: K) -> Self;
    pub fn poll_interval(mut self, value: Duration) -> Self;
    pub fn batch_size(mut self, value: usize) -> Self;
    pub fn lease(mut self, value: Duration) -> Self;
    pub fn retry_base_delay(mut self, value: Duration) -> Self;
    pub fn retry_max_delay(mut self, value: Duration) -> Self;
    pub fn jitter(mut self, value: bool) -> Self;
    pub fn min_cycle_delay(mut self, value: Duration) -> Self;
    pub fn dispatch_timeout(mut self, value: Duration) -> Self;
    pub fn build(self) -> Result<SchedulerWorker<S, K>, SchedulerError>;
}

impl<S: ScheduleStore, K: ScheduleSink> SchedulerWorker<S, K> {
    pub async fn run(self, cancel: CancellationToken) -> Result<(), SchedulerError>;
}
```

`SchedulerBuilder::new` starts from `SchedulerWorkerConfig::default`; the eight setters override individual tuning fields, and `build()` validates the combination (batch size at least 1, non-zero durations, `retry_max_delay >= retry_base_delay`) before returning the worker, rejecting an incoherent configuration with `SchedulerError::InvalidConfiguration`. `SchedulerWorker::run` drives the polling loop until `cancel` is triggered: each cycle claims and settles one batch, dispatches under `dispatch_timeout`, then retries with bounded exponential backoff or dead-letters once the attempt budget is exhausted.

### `SchedulerControl`

```rust
impl<S: ScheduleStore> SchedulerControl<S> {
    pub fn new(store: Arc<S>) -> Self;
    pub async fn inspect(&self, id: Uuid) -> Result<Option<ScheduleSnapshot>, SchedulerError>;
    pub async fn pause(&self, id: Uuid) -> Result<(), SchedulerError>;
    pub async fn cancel(&self, id: Uuid) -> Result<(), SchedulerError>;
    pub async fn resume(&self, id: Uuid) -> Result<(), SchedulerError>;
}
```

The ergonomic lifecycle facade application code uses to drive a single schedule: `inspect` reads a snapshot, `pause`/`resume` toggle claim eligibility, `cancel` excludes a schedule from future claims. `cancel` is a no-op on an already terminal schedule; `resume` realigns a past-due paused cron schedule to its next strictly future occurrence instead of firing once per missed tick.

### Sinks

```rust
#[trait_variant::make(Send)]
pub trait ScheduleSink: Send + Sync + 'static {
    async fn dispatch(&self, message: &ScheduledMessage) -> Result<(), SchedulerError>;
}

impl<T: RawBusPublish> BusSink<T> {
    pub fn new(transport: T) -> Self;
}

impl<Q: IdempotentOutboxEnqueue> OutboxSink<Q> {
    pub fn new(enqueue: Q) -> Self;
}

impl MediatorSink {
    pub fn builder(mediator: Arc<Mediator>) -> MediatorSinkBuilder;
}

impl MediatorSinkBuilder {
    pub fn register<N>(mut self) -> Self
    where
        N: Notification + Event + DeserializeOwned + 'static;

    pub fn build(self) -> MediatorSink;
}
```

`ScheduleSink` is the contract a due occurrence is dispatched through, with at-least-once delivery semantics: implementations must tolerate redelivery and deduplicate on `occurrence_id()` when an effect must happen only once. `BusSink` publishes on the message bus under the target's routing key, stamping the occurrence id as message id. `OutboxSink` enqueues idempotently into the transactional outbox, keyed on the occurrence id, so a redelivery is a no-op rather than a second row. `MediatorSink` republishes in-process through the mediator; `MediatorSinkBuilder::register` binds a notification type to its `Event::EVENT_TYPE` before `build()` finishes the sink.

### `ScheduleSnapshot` and `ScheduleStatus`

```rust
pub enum ScheduleStatus {
    Pending,
    Paused,
    Delivered,
    Cancelled,
    DeadLettered,
}

pub struct ScheduleSnapshot {
    pub schedule_id: Uuid,
    pub status: ScheduleStatus,
    pub scheduled_for: SystemTime,
    pub attempts: u32,
    pub max_attempts: u32,
    pub trigger: Trigger,
    pub last_error: Option<String>,
}
```

`ScheduleSnapshot` is the read-only view returned by `ScheduleStore::inspect` and `SchedulerControl::inspect`. Both types are `#[non_exhaustive]`: build a snapshot through `ScheduleSnapshot::new` in backend code, and match `ScheduleStatus` with a wildcard arm to stay forward compatible.

### `InMemoryScheduleStore` and `SchedulerError`

```rust
impl InMemoryScheduleStore {
    pub fn new() -> Self;
}

pub enum SchedulerError {
    Serialization(#[from] serde_json::Error),
    InvalidTrigger { reason: String },
    InvalidConfiguration { reason: String },
    Database(#[source] Box<dyn std::error::Error + Send + Sync>),
    ScheduleNotFound { schedule_id: Uuid },
    NotReplayable { schedule_id: Uuid, status: ScheduleStatus },
    Dispatch(#[source] Box<dyn std::error::Error + Send + Sync>),
    Internal(String),
}
```

`InMemoryScheduleStore` is a reference `ScheduleStore` implementation for tests: it implements the full claim and lease contract synchronously under a mutex, with no database, and is what the worker's own test suite is exercised against. `SchedulerError` is the unified, `#[non_exhaustive]` error type; variants carrying data are built through constructors (`SchedulerError::invalid_trigger`, `::dispatch`, and so on) rather than by literal, so their internals can evolve without a breaking change.

## Where to read next

- [Scheduler quick start](../getting-started/scheduler-quick-start.md)
- [Scheduler triggers concept](../concepts/scheduler-triggers.md)
- [Scheduler delivery concept](../concepts/scheduler-delivery.md)
