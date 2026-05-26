# Getting started with Hexeract

This guide walks you through wiring the Hexeract Outbox into an existing Rust service backed by PostgreSQL. By the end you will have a publisher that writes outbox rows inside your business transactions and a worker that polls those rows and dispatches them to typed handlers.

Estimated time: **5 minutes** (assuming you already have a PostgreSQL instance reachable).

## 1. Add the dependencies

```toml
[dependencies]
hexeract-outbox = "0.1"
hexeract-outbox-postgres = "0.1"

# Already in most async Rust services:
deadpool-postgres = "0.14"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-postgres = { version = "0.7", features = ["with-uuid-1"] }
tokio-util = "0.7"
serde = { version = "1", features = ["derive"] }
uuid = { version = "1", features = ["v7"] }
```

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

The schema is documented in [docs/outbox-postgres-schema.md](../outbox-postgres-schema.md).

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
use hexeract_outbox_postgres::PgOutboxPublisher;

async fn register_user(
    pool: &deadpool_postgres::Pool,
    publisher: &PgOutboxPublisher,
    email: String,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let mut client = pool.get().await?;
    let mut tx = client.transaction().await?;

    let user_id = Uuid::new_v4();
    tx.execute(
        "INSERT INTO users (id, email) VALUES ($1, $2)",
        &[&user_id, &email],
    )
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
use hexeract_outbox::{Handler, OutboxError};

pub struct AuditWriter {
    pub audit_pool: deadpool_postgres::Pool,
}

impl Handler<UserRegistered> for AuditWriter {
    type Error = OutboxError;

    async fn handle(
        &self,
        event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        let client = self
            .audit_pool
            .get()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        client
            .execute(
                "INSERT INTO audit_log (event_type, payload) VALUES ($1, $2)",
                &[&UserRegistered::EVENT_TYPE, &serde_json::to_string(&event)?],
            )
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
use hexeract_outbox_postgres::PgOutboxWorkerBuilder;
use tokio_util::sync::CancellationToken;

async fn run_service(
    pool: deadpool_postgres::Pool,
    audit_pool: deadpool_postgres::Pool,
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

- [outbox-architecture.md](../outbox-architecture.md): how the publisher, worker and store cooperate, and what guarantees they provide.
- [outbox-postgres-schema.md](../outbox-postgres-schema.md): canonical schema and migration tooling guidance.
- [design/outbox-mvp-requirements.md](../design/outbox-mvp-requirements.md): the public requirements that drove v0.1.0.
- The runnable [`examples/02_outbox_two_databases.rs`](../../crates/hexeract-outbox-postgres/examples/02_outbox_two_databases.rs) demonstrates the full flow against two isolated PostgreSQL containers.
