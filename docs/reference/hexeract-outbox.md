# `hexeract-outbox` API reference

Backend-agnostic core of the outbox feature. Pair with [`hexeract-outbox-sql`](hexeract-outbox-sql.md) for the PostgreSQL, MySQL and SQLite backends. (`hexeract-outbox-postgres` is deprecated since 0.4.0.)

The full rustdoc lives at <https://docs.rs/hexeract-outbox>.

## Public surface

### Marker trait

| Item | Role |
| --- | --- |
| `Event` | Marker for any domain event flowing through the outbox. Implementors define a stable `EVENT_TYPE: &'static str` used for routing. |

### Envelope and error

| Item | Role |
| --- | --- |
| `OutboxEnvelope` | Row representation of a persisted event. Holds `event_id`, `event_type`, JSON `payload`, optional `subject_id`, retry bookkeeping (`attempts`, `last_error`, `next_retry_at`) and `delivered_at`. `Debug` masks the payload. |
| `OutboxEnvelope::new(event_id, &E)` | Builds a fresh envelope without `subject_id`. |
| `OutboxEnvelope::with_subject(event_id, subject_id, &E)` | Builds a fresh envelope tagged with a subject for partial ordering. |
| `OutboxEnvelope::restore(...)` | Backend hook to rebuild an envelope from a database row. |
| `OutboxEnvelope::decode::<E>()` | Deserialises the payload and validates `event_type` matches `E::EVENT_TYPE`. |
| `OutboxError` | Non-exhaustive error enum: `Serialization`, `Database(Box<...>)`, `MissingHandler { event_type }`, `TypeMismatch { expected, actual }`, `PoolTimeout`, `DispatchTimeout { event_id, event_type, timeout }`, `Internal(String)`. (`MaxRetries { event_id, attempts }` is declared but reserved; the current worker never constructs it.) |

### Publisher contract

```rust
#[trait_variant::make(Send)]
pub trait OutboxPublisher: Send + Sync + 'static {
    type Tx<'tx>: Send;

    async fn publish_in_tx<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        event: &E,
    ) -> Result<Uuid, OutboxError>;

    async fn publish_in_tx_with_subject<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        subject_id: Uuid,
        event: &E,
    ) -> Result<Uuid, OutboxError>;

    async fn publish<E: Event>(&self, event: &E) -> Result<Uuid, OutboxError>;
}
```

`Tx<'tx>` is a generic associated type so each backend can expose its own lifetime-bound transaction handle (e.g. `deadpool_postgres::Transaction<'tx>`).

### Store contract

```rust
#[async_trait::async_trait]
pub trait OutboxStore: Send + Sync + 'static {
    type Client: Send;
    type Tx<'tx>: Send where Self: 'tx;

    async fn acquire(&self) -> Result<Self::Client, OutboxError>;
    async fn begin<'a>(&self, client: &'a mut Self::Client) -> Result<Self::Tx<'a>, OutboxError>;
    async fn poll<'a>(&self, tx: &mut Self::Tx<'a>, batch_size: usize, max_attempts: u32) -> Result<Vec<OutboxEnvelope>, OutboxError>;
    async fn mark_delivered<'a>(&self, tx: &mut Self::Tx<'a>, event_id: Uuid) -> Result<(), OutboxError>;
    async fn mark_failed<'a>(&self, tx: &mut Self::Tx<'a>, event_id: Uuid, error: &str, retry_in: Duration) -> Result<(), OutboxError>;
    async fn commit<'a>(&self, tx: Self::Tx<'a>) -> Result<(), OutboxError>;

    // Default no-op implementations; SQL backends override both.
    async fn mark_dead_lettered<'a>(&self, tx: &mut Self::Tx<'a>, event_id: Uuid, error: &str) -> Result<(), OutboxError> { Ok(()) }
    async fn claim<'a>(&self, tx: &mut Self::Tx<'a>, event_ids: &[Uuid], lease_for: Duration) -> Result<(), OutboxError> { Ok(()) }
}
```

`mark_failed` receives `retry_in: Duration`, a relative duration from now that the backend adds to the database clock when scheduling the next attempt. `mark_dead_lettered` and `claim` have default no-op implementations; the SQL backends override them.

Implemented through `async_trait` (boxed futures) to work around `rust-lang/rust#100013` until GAT inference for HRTB lands.

### Handler contract

```rust
#[trait_variant::make(Send)]
pub trait Handler<E: Event>: Send + Sync + 'static {
    type Error: Into<OutboxError> + Send + Sync + 'static;
    async fn handle(&self, event: E, ctx: &HandlerContext) -> Result<(), Self::Error>;
}
```

Symmetric with `hexeract_bus::Handler<M>`.

### Worker

| Item | Role |
| --- | --- |
| `OutboxWorker::new(store, handlers, config)` | Build a generic worker over any `OutboxStore` impl. |
| `OutboxWorker::run(cancel)` | Boxed `Send` future the caller spawns. Honours `CancellationToken`. |
| `OutboxWorkerConfig` | `poll_interval` (100 ms), `batch_size` (10), `max_attempts` (5), `retry_base_delay` (1 s), `retry_max_delay` (300 s / 5 min), `jitter` (true), `dispatch_timeout` (30 s), `min_cycle_delay` (5 ms). Retries use bounded exponential backoff with full jitter: `min(retry_max_delay, retry_base_delay x 2^attempts)`. |
| `ErasedHandler` + `TypedHandler<E, H>` | Adapter pair lifting a typed handler into the dyn-safe form. |
| `BoxFuture<'a, T>` | Pinned, boxed, Send future returned by trait-object methods. |

## Where to read next

- [Outbox quick start](../getting-started/outbox-quick-start.md)
- [Outbox flow architecture](../architecture/outbox-flow.md)
- [Outbox pattern concept](../concepts/outbox-pattern.md)
- [Worker concept](../concepts/worker.md)
