# Migration v0.4.0 to v0.5.0

v0.5.0 is the reliability release. It removes the deprecated `hexeract-outbox-postgres` crate, reshapes the `OutboxStore` trait and `OutboxWorkerConfig` for crash-safe retries, and freezes the remaining public enums and error types before 1.0. Most upgrades are mechanical: bump the pins, run `cargo update`, then apply only the source edits for the APIs you actually use.

If you are still on v0.3.x, apply the [v0.3 to v0.4 guide](migration-v0.3-v0.4.md) first.

## What changed

| Area | Change | Action required |
| --- | --- | --- |
| Outbox backend | `hexeract-outbox-postgres` removed | Switch to `hexeract-outbox-sql` (the table schema is unchanged) |
| `OutboxStore` trait | `mark_failed` takes `retry_in: Duration`; new `claim` and `mark_dead_lettered` methods | Update custom implementations (built-in backends already do this) |
| Outbox config | `retry_delay` removed, replaced by `retry_base_delay` / `retry_max_delay` / `jitter` | Replace the field at construction |
| Outbox errors | New `OutboxError::DispatchTimeout` variant | Add a wildcard arm to exhaustive matches |
| Middleware | `TracingMiddleware::with_level` is now a chainable method, not a constructor | Build with `new()` then chain `with_level` |
| Core / mediator / bus | `HandlerNotFound`, `HandlerFailed`, `MediatorBuildError`, `HandlersVerificationError`, `AckMode`, `Dialect`, `RabbitMqWorkerConfig` are now `#[non_exhaustive]` | Add wildcard arms; construct configs through builders |
| Bus consumer | `RabbitMqWorkerConfig::max_buffered` (opt-in) and `max_payload_bytes` cap | Set `max_buffered` under `AckMode::Unacknowledged` if you rely on it |
| Bus pool | `ChannelPool::idle_len` is now synchronous | Drop the `.await` |
| Outbox events | `event_type` capped at 64 bytes | Shorten any `EVENT_TYPE` over 64 bytes |

## 1. Bump the crates

```toml
hexeract = { version = "0.5", features = ["mediator", "bus-rabbitmq", "outbox-sql-postgres"] }
```

Or, if you depend on the individual crates:

```toml
hexeract-core = "0.5"
hexeract-mediator = "0.5"
hexeract-middleware = "0.5"
hexeract-bus = "0.5"
hexeract-bus-rabbitmq = "0.5"
hexeract-outbox = "0.5"
hexeract-outbox-sql = { version = "0.5", features = ["postgres"] }
```

Run `cargo update` and rebuild.

## 2. Outbox: drop `hexeract-outbox-postgres`

The deprecated `hexeract-outbox-postgres` crate (built on `deadpool_postgres`) is removed in 0.5.0. Move to `hexeract-outbox-sql`, which is built on `sqlx`, so the constructors take a `sqlx::PgPool` rather than a `deadpool_postgres::Pool`. The PostgreSQL table schema is byte-for-byte identical, so existing tables keep working with no data migration.

Before:

```toml
hexeract = { version = "0.4", features = ["outbox-postgres"] }
```

```rust
use hexeract::outbox_postgres::{PgOutboxWorkerBuilder, ensure_schema};
```

After:

```toml
hexeract = { version = "0.5", features = ["outbox-sql-postgres"] }
```

```rust
use hexeract::outbox_sql::PgOutboxWorkerBuilder;
use hexeract::outbox_sql::postgres::ensure_schema;

let pool = sqlx::PgPool::connect(&database_url).await?;
ensure_schema(&pool, "audit_outbox").await?; // POC/tests only; production applies Dialect::schema_ddl via migration tooling
let worker = PgOutboxWorkerBuilder::new(pool)
    .register_handler::<MyEvent, _>(my_handler)
    .build()?;
```

The same crate also covers MySQL (`outbox-sql-mysql`) and SQLite (`outbox-sql-sqlite`) behind their own features. SQLite is single-worker only; for competing-consumers fan-out across many workers, stay on PostgreSQL or MySQL. See [SQLite outbox concurrency](../concepts/sqlite-outbox-concurrency.md).

## 3. Custom `OutboxStore` implementers

If you implement `OutboxStore` yourself (most users do not, they use the built-in PostgreSQL, MySQL or SQLite backends), three signatures changed for crash-safe reliability.

`mark_failed` now receives `retry_in: Duration`, a delay from *now*, instead of an absolute application timestamp. The backend adds it to its own database clock when persisting `next_retry_at`, so retry scheduling is immune to skew between the worker host and the database host.

```rust
// Before: an absolute timestamp computed on the application clock
async fn mark_failed<'a>(&self, tx: &mut Self::Tx<'a>, event_id: Uuid, error: &str, next_retry_at: SystemTime) -> Result<(), OutboxError>;

// After: a delay the backend resolves against its own clock
async fn mark_failed<'a>(&self, tx: &mut Self::Tx<'a>, event_id: Uuid, error: &str, retry_in: Duration) -> Result<(), OutboxError>;
```

Two methods were added, both with a default no-op body, so a backend that neither leases nor dead-letters can ignore them:

- `claim(tx, event_ids, lease_for: Duration)` advances the soft lease (`next_retry_at` set to the database clock plus `lease_for`) and increments the attempt counter at claim time. Incrementing here, rather than only on failure, is what makes a worker crash between claim and acknowledgement safe: the attempt is already counted, so a poison envelope eventually reaches the dead-letter threshold.
- `mark_dead_lettered(tx, event_id, error)` persists an envelope that has exhausted its retry budget, called within the same transaction as `mark_failed`.

## 4. Outbox config: `retry_delay` replaced by bounded backoff

`OutboxWorkerConfig::retry_delay` is removed. Retries now use bounded exponential backoff: the next retry waits `min(retry_max_delay, retry_base_delay × 2^attempts)`, with optional jitter.

```rust
// Before
let config = OutboxWorkerConfig {
    retry_delay: Duration::from_secs(5),
    ..Default::default()
};

// After
let config = OutboxWorkerConfig {
    retry_base_delay: Duration::from_secs(1), // default 1 s
    retry_max_delay: Duration::from_secs(300), // default 300 s
    jitter: true, // default true
    ..Default::default()
};
```

A fixed delay maps to setting `retry_base_delay` and a `retry_max_delay` equal to it with `jitter: false`.

## 5. Outbox: new `DispatchTimeout` error variant

`dispatch_timeout` (default 30 s) is now enforced as a hard per-handler deadline: a hung handler is cancelled and the envelope retried instead of stalling the worker. It surfaces through the new `OutboxError::DispatchTimeout` variant. If you exhaustively match `OutboxError`, add a wildcard arm:

```rust
match err {
    OutboxError::MaxRetries => { /* ... */ }
    // ...
    _ => { /* covers DispatchTimeout and future variants */ }
}
```

## 6. Middleware: `TracingMiddleware::with_level` is chainable

`with_level` is no longer a constructor; it is a chainable consuming method on a built middleware.

```rust
// Before
let mw = TracingMiddleware::with_level(Level::DEBUG);

// After
let mw = TracingMiddleware::new().with_level(Level::DEBUG);
```

## 7. Frozen enums and error types (`#[non_exhaustive]`)

These types are now `#[non_exhaustive]` so future variants and fields are not breaking. Exhaustive `match` arms must add a wildcard `_` (or `..` for struct variants), and configs must be built through their builders rather than struct literals:

- `HexeractError::HandlerNotFound` and `HexeractError::HandlerFailed`: construct `HandlerNotFound` through `HexeractError::handler_not_found()`; add `..` to pattern matches.
- `MediatorBuildError` and `HandlersVerificationError`: add a wildcard arm.
- `AckMode` and `Dialect`: add a wildcard `_` arm.
- `RabbitMqWorkerConfig`: construct through `RabbitMqWorkerBuilder`, not a struct literal. `AckMode::default()` is still `Manual`.

The new `HexeractError::InputDowncastFailed { expected }` variant (with the `input_downcast_failed` constructor) replaces an opaque failure when a dispatched input does not downcast to the handler's message type; a wildcard arm already covers it.

## 8. Bus consumer: bounded buffer and payload cap

Two opt-in safety bounds were added on the RabbitMQ worker:

- `RabbitMqWorkerConfig::max_buffered` (builder method `max_buffered`) bounds the number of in-flight deliveries buffered under `AckMode::Unacknowledged`. It defaults to unbounded for backwards compatibility, but a `no_ack` consumer with no bound logs a warning; set it to cap memory.
- `RabbitMqWorkerConfig::max_payload_bytes` (default `DEFAULT_MAX_PAYLOAD_BYTES`, 1 MiB) rejects oversized deliveries with `BusError::PayloadTooLarge`. Raise it through the `max_payload_bytes` builder method if your messages are legitimately larger.

```rust
let worker = RabbitMqWorkerBuilder::new(conn)
    .ack_mode(AckMode::Unacknowledged)
    .max_buffered(1024)        // bound the no_ack buffer
    .max_payload_bytes(4 * 1024 * 1024) // raise the cap to 4 MiB
    .register_handler::<OrderPlaced, _>(Projector)
    .build()?;
```

## 9. `ChannelPool::idle_len` is synchronous

Reporting the idle channel count no longer awaits.

```rust
// Before
let n = pool.idle_len().await;

// After
let n = pool.idle_len();
```

## 10. `event_type` capped at 64 bytes

`event_type` is validated to be at most 64 bytes at the envelope boundary, matching the schema-bound limit. An `Event::EVENT_TYPE` longer than 64 bytes is now rejected rather than silently truncated downstream. Shorten any over-long identifier.

## Verification checklist

After the bump:

- [ ] `cargo build --workspace` succeeds.
- [ ] `cargo test --workspace --all-features` succeeds.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` succeeds.
- [ ] No remaining reference to `hexeract-outbox-postgres` or the `outbox-postgres` feature in `Cargo.toml`.
- [ ] Custom `OutboxStore::mark_failed` takes `retry_in: Duration`; `claim` and `mark_dead_lettered` are implemented or left as the default no-op.
- [ ] `OutboxWorkerConfig` uses `retry_base_delay` / `retry_max_delay` / `jitter` instead of `retry_delay`.
- [ ] `TracingMiddleware` is built with `new().with_level(...)`.
- [ ] Exhaustive matches on the frozen enums and errors carry a wildcard arm.
- [ ] No `EVENT_TYPE` exceeds 64 bytes.
