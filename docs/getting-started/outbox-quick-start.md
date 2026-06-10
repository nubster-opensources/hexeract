# Getting started with Hexeract

This guide walks you through wiring the Hexeract Outbox into an existing Rust service backed by PostgreSQL. By the end you will have a publisher that writes outbox rows inside your business transactions and a worker that polls those rows and dispatches them to typed handlers.

Estimated time: **5 minutes** (assuming you already have a PostgreSQL instance reachable).

## 1. Add the dependencies

The outbox runs on PostgreSQL, MySQL or SQLite through the `sqlx`-backed `hexeract-outbox-sql` crate. This guide uses the `postgres` feature; the `mysql` and `sqlite` features expose the same surface (`MySqlOutboxPublisher` / `SqliteOutboxPublisher` and their builders).

```toml
[dependencies]
hexeract-outbox = "0.4"
hexeract-outbox-sql = { version = "0.4", features = ["postgres"] }

# Already in most async Rust services:
sqlx = { version = "0.8", features = ["runtime-tokio", "tls-rustls-ring", "postgres", "uuid"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-util = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v7"] }
```

> The legacy `hexeract-outbox-postgres` crate (built on `deadpool_postgres`) is deprecated since 0.4.0 and will be removed in 0.5.0. New projects should start on `hexeract-outbox-sql`.

## 2. Apply the canonical schema

The outbox needs a single table. Generate the canonical SQL with the `hexeract` CLI and pipe it into your migration tool:

```sh
cargo install hexeract-cli
hexeract outbox patch --table audit_outbox > migrations/0042_outbox.sql
```

For local development you can apply the schema directly against a running database (requires `--yes-i-know` to avoid surprises in production):

```sh
hexeract outbox apply --conn "postgres://user:pass@localhost/db" --table audit_outbox --yes-i-know
```

For POCs and integration tests you can apply the schema from Rust with `hexeract_outbox_sql::postgres::ensure_schema(&pool, "audit_outbox").await?`. Production deployments should run their own migration tooling rather than apply DDL from the running service.

The schema is documented in [outbox-postgres-schema.md](../reference/outbox-postgres-schema.md).

## 3. Declare a domain event

```rust
use hexeract_outbox::Event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct UserRegistered {
    pub user_id: Uuid,
    pub email: String,
}

impl Event for UserRegistered {
    const EVENT_TYPE: &'static str = "users.registered";
}
```

Pick a stable identifier for `EVENT_TYPE`. The convention is `"<bounded-context>.<verb>"`.

## 4. Publish inside your business transaction

```rust
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox_sql::PgOutboxPublisher;

async fn register_user(
    pool: &sqlx::PgPool,
    publisher: &PgOutboxPublisher,
    email: String,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;

    let user_id = Uuid::now_v7();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(user_id)
        .bind(&email)
        .execute(&mut *tx)
        .await?;

    let event_id = publisher
        .publish_in_tx(&mut tx, &UserRegistered { user_id, email })
        .await?;

    tx.commit().await?;
    tracing::info!(?event_id, "registered user");
    Ok(user_id)
}
```

The outbox row is committed atomically with the `INSERT INTO users`. If the business transaction rolls back, the event is never published.

## 5. Declare a handler

```rust
use hexeract_core::HandlerContext;
use hexeract_outbox::{Event, Handler, OutboxError};

pub struct AuditWriter {
    pub audit_pool: sqlx::PgPool,
}

impl Handler<UserRegistered> for AuditWriter {
    type Error = OutboxError;

    async fn handle(
        &self,
        event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        let payload =
            serde_json::to_string(&event).map_err(|e| OutboxError::Internal(e.to_string()))?;
        sqlx::query("INSERT INTO audit_log (event_type, payload) VALUES ($1, $2)")
            .bind(UserRegistered::EVENT_TYPE)
            .bind(payload)
            .execute(&self.audit_pool)
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        Ok(())
    }
}
```

Handlers must be **idempotent**: the outbox guarantees at-least-once delivery, so the same event can be dispatched more than once if a previous attempt crashed between the side effect and the database commit that marks the row as delivered.

## 6. Spawn the worker

```rust
use std::time::Duration;
use hexeract_outbox_sql::PgOutboxWorkerBuilder;
use tokio_util::sync::CancellationToken;

async fn run_service(
    pool: sqlx::PgPool,
    audit_pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let worker = PgOutboxWorkerBuilder::new(pool.clone())
        .table_name("audit_outbox")
        .register_handler::<UserRegistered, _>(AuditWriter { audit_pool })
        .poll_interval(Duration::from_millis(50))
        .build()?;

    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    // ... serve requests ...

    cancel.cancel();
    join.await??;
    Ok(())
}
```

## 7. Verify

`hexeract outbox check --conn "postgres://..." --table audit_outbox` validates that the table contains every required column. Exit code 0 means you are ready to go.

## What next

- [Outbox flow](../architecture/outbox-flow.md): how the publisher, worker and store cooperate, and what guarantees they provide.
- [Outbox PostgreSQL schema](../reference/outbox-postgres-schema.md): canonical schema and migration tooling guidance.
- [Outbox pattern concept](../concepts/outbox-pattern.md): the why, the guarantees, the trade-offs.
- [Outbox MVP requirements (v0.1.0)](../design/outbox-mvp-requirements.md): the public requirements that drove v0.1.0.
- The runnable [`examples/02_outbox_transactional.rs`](../../crates/hexeract-examples/examples/02_outbox_transactional.rs) demonstrates the full publish-to-dispatch flow against a real PostgreSQL container: `cargo run --example 02_outbox_transactional -p hexeract-examples` (Docker required).
