# Telescope the outbox inside a mediator command handler

You want a single command (say, `RegisterUser`) to do two things atomically:

1. Persist the new user row.
2. Enqueue an outgoing event so downstream services learn about the registration.

The outbox pattern (write the event row in the same transaction as the business state) plus the in-process mediator are designed to compose. Here is the standard wiring.

## Recipe

```rust
use deadpool_postgres::Pool;
use hexeract::core::{Command, CommandHandler, HandlerContext, HexeractError};
use hexeract::outbox::{Event, OutboxPublisher};
use hexeract::outbox_postgres::PgOutboxPublisher;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct UserRegistered {
    pub id: Uuid,
}

impl Event for UserRegistered {
    const EVENT_TYPE: &'static str = "users.registered";
}

pub struct RegisterUser {
    pub email: String,
}

impl Command for RegisterUser {
    type Output = Uuid;
}

pub struct RegisterUserHandler {
    pool: Pool,
    publisher: PgOutboxPublisher,
}

impl CommandHandler<RegisterUser> for RegisterUserHandler {
    type Error = HexeractError;

    async fn handle(
        &self,
        cmd: RegisterUser,
        _ctx: &HandlerContext,
    ) -> Result<Uuid, HexeractError> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(HexeractError::handler_failed)?;
        let mut tx = client
            .transaction()
            .await
            .map_err(HexeractError::handler_failed)?;

        let id = Uuid::new_v4();
        tx.execute(
            "INSERT INTO users (id, email) VALUES ($1, $2)",
            &[&id, &cmd.email],
        )
        .await
        .map_err(HexeractError::handler_failed)?;

        self.publisher
            .publish_in_tx(&mut tx, &UserRegistered { id })
            .await
            .map_err(HexeractError::handler_failed)?;

        tx.commit().await.map_err(HexeractError::handler_failed)?;
        Ok(id)
    }
}
```

The single transaction wraps both writes. If the commit fails, neither the user row nor the outbox row exists. If the commit succeeds, the outbox worker (running in a separate task) picks up the row in its next poll cycle and publishes it to the bus.

## Why telescope rather than compose at the call site

The naive alternative is:

```rust,ignore
// In the application's main / HTTP handler
let id = mediator.send(RegisterUser { ... }).await?;
publisher.publish(&UserRegistered { id }).await?;   // wrong
```

This loses atomicity. If the process crashes between `send` and `publish`, the user exists but no event is enqueued, leaving downstream consumers permanently inconsistent. Putting `publish_in_tx` *inside* the handler's transaction is the only way to make the two writes succeed or fail together.

## Wiring at startup

```rust,ignore
use std::time::Duration;
use hexeract::mediator::MediatorBuilder;
use hexeract::outbox_postgres::{PgOutboxPublisher, PgOutboxWorkerBuilder};
use tokio_util::sync::CancellationToken;

# struct AuditWriter;
# impl hexeract::outbox::Handler<UserRegistered> for AuditWriter {
#     type Error = hexeract::outbox::OutboxError;
#     async fn handle(&self, _event: UserRegistered, _ctx: &hexeract::core::HandlerContext) -> Result<(), Self::Error> { Ok(()) }
# }
# async fn run(pool: deadpool_postgres::Pool) -> Result<(), Box<dyn std::error::Error>> {
let publisher = PgOutboxPublisher::new(pool.clone(), "user_outbox")?;

let mediator = MediatorBuilder::new()
    .register_command_handler::<RegisterUser, _>(RegisterUserHandler {
        pool: pool.clone(),
        publisher: publisher.clone(),
    })
    .build()?;

let worker = PgOutboxWorkerBuilder::new(pool.clone())
    .table_name("user_outbox")
    .register_handler::<UserRegistered, _>(AuditWriter)
    .poll_interval(Duration::from_millis(100))
    .build()?;

let cancel = CancellationToken::new();
let worker_join = tokio::spawn(worker.run(cancel.clone()));

// ... application runs, mediator.send(RegisterUser { ... }) is called ...

cancel.cancel();
worker_join.await??;
# Ok(()) }
```

Two long-running concerns coexist:

- **The mediator** dispatches synchronous commands and queries on demand from your HTTP / gRPC handler.
- **The outbox worker** drains the table on its own schedule, decoupled from request lifecycle, publishing rows to the bus.

The transaction in the command handler is the only synchronization point. Everything downstream is at-least-once.

## Variants

**Multiple outboxes per service.** If you have multiple aggregates with different consistency requirements, run multiple worker instances with different `table_name`s. Each handler picks its outbox by injecting the corresponding `PgOutboxPublisher`.

**Idempotent downstream consumers.** Outbox is at-least-once: workers retry on transient failures. Downstream consumers must dedupe on `event_id` (carried in the envelope). See [Outbox pattern](../concepts/outbox-pattern.md) for the full guarantees.

**Notifications inside the same transaction.** If you also want to dispatch an in-process notification (say, to refresh a local cache), call `mediator.publish(...)` **after** the commit, not before; the notification fan-out is not transactional. If your local cache must absolutely see the new row, write it from inside the transaction directly.
