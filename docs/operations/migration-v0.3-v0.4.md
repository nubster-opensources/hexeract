# Migration v0.3.0 to v0.4.0

v0.4.0 ships the multi-database outbox (`hexeract-outbox-sql`) and three small breaking changes on the bus and core crates. Most upgrades are mechanical: bump the pins, run `cargo update`, then apply the source edits below only for the APIs you actually use.

## What changed

| Area | Change | Action required |
| --- | --- | --- |
| Outbox backend | `hexeract-outbox-postgres` deprecated in favour of `hexeract-outbox-sql` | Switch the dependency and the constructors (the table schema is unchanged) |
| Bus ack modes | `AckMode::Auto` removed, replaced by `AckOnReceive` and `Unacknowledged` | Map `Auto` to one of the two explicit modes |
| Core errors | `HexeractError::HandlerNotFound` field `command_type` renamed `message_type` | Rename the field at construction and match sites |
| Notifications | `Notification` no longer requires `Clone`; handlers receive `Arc<N>` | Update notification handler signatures |

## 1. Bump the crates

```toml
hexeract = { version = "0.4", features = ["mediator", "bus-rabbitmq", "outbox-sql-postgres"] }
```

Or, if you depend on the individual crates:

```toml
hexeract-core = "0.4"
hexeract-mediator = "0.4"
hexeract-bus = "0.4"
hexeract-bus-rabbitmq = "0.4"
hexeract-outbox = "0.4"
hexeract-outbox-sql = { version = "0.4", features = ["postgres"] }
```

Run `cargo update` and rebuild.

## 2. Outbox: switch to `hexeract-outbox-sql`

The new backend is built on `sqlx` instead of `deadpool_postgres`, so the constructors take a `sqlx::PgPool` rather than a `deadpool_postgres::Pool`. The PostgreSQL table schema is byte-for-byte identical, so existing tables keep working with no data migration.

Before:

```toml
hexeract = { version = "0.3", features = ["outbox-postgres"] }
```

```rust
use hexeract::outbox_postgres::{PgOutboxWorkerBuilder, ensure_schema};
```

After:

```toml
hexeract = { version = "0.4", features = ["outbox-sql-postgres"] }
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

The same crate now also covers MySQL (`outbox-sql-mysql`) and SQLite (`outbox-sql-sqlite`) behind their own features. SQLite is single-worker only; for competing-consumers fan-out across many workers, stay on PostgreSQL or MySQL. See [SQLite outbox concurrency](../concepts/sqlite-outbox-concurrency.md).

`hexeract-outbox-postgres` keeps its `deadpool_postgres` implementation for the 0.4.x cycle, but it is deprecated in 0.4.x and has since been removed in 0.5.0. Migrate before upgrading to 0.5.

## 3. Bus ack modes: `AckMode::Auto` removed

`AckMode::Auto` silently lost messages on handler failure or crash, so it is replaced by two explicit modes:

- `AckMode::Unacknowledged` keeps the previous `no_ack = true` fire-and-forget behaviour under an honest name. Use it for identical semantics.
- `AckMode::AckOnReceive` acknowledges each delivery on receive, before the handler runs (`no_ack = false`), giving explicit at-most-once with prefetch back-pressure.

```rust
// Before
let worker = RabbitMqWorkerBuilder::new(conn)
    .ack_mode(AckMode::Auto)
    .build()?;

// After: identical fire-and-forget
.ack_mode(AckMode::Unacknowledged)

// After: explicit at-most-once with prefetch back-pressure
.ack_mode(AckMode::AckOnReceive)
```

See [ack modes](../concepts/ack-modes.md) for the full comparison.

## 4. Core: `HandlerNotFound` field renamed

`HexeractError::HandlerNotFound` carries the type name of commands, queries and notifications alike, so its field `command_type` is renamed `message_type`.

```rust
// Before
HexeractError::HandlerNotFound { command_type: name }

// After
HexeractError::HandlerNotFound { message_type: name }
```

Update any pattern match on the variant accordingly.

## 5. Notifications: `Arc<N>` instead of `Clone`

The `Notification` trait no longer requires `Clone`. The mediator shares a single `Arc<N>` across the fan-out instead of deep-cloning the payload per handler, and `NotificationHandler::handle` now receives `Arc<N>`.

```rust
// Before
impl NotificationHandler<UserRegistered> for Welcomer {
    async fn handle(&self, event: UserRegistered, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        // ...
    }
}

// After
impl NotificationHandler<UserRegistered> for Welcomer {
    async fn handle(&self, event: Arc<UserRegistered>, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        // ...
    }
}
```

Handlers written through `#[handler(notification)]` take `Arc<N>` as their message argument:

```rust
#[handler(notification)]
async fn on_user_registered(event: Arc<UserRegistered>, ctx: &HandlerContext) -> Result<(), HexeractError> {
    // ...
}
```

The `Clone` bound can be dropped from your notification types if it was only there for the mediator.

## Verification checklist

After the bump:

- [ ] `cargo build --workspace` succeeds.
- [ ] `cargo test --workspace --all-features` succeeds.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` succeeds.
- [ ] Outbox constructors take a `sqlx::PgPool`; the deprecated `hexeract-outbox-postgres` is gone from `Cargo.toml`.
- [ ] No remaining reference to `AckMode::Auto`.
- [ ] Notification handlers accept `Arc<N>` and their types no longer carry a `Clone` bound added only for the fan-out.
