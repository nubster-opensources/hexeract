# Mediator quick start

The Hexeract mediator dispatches **commands**, **queries** and **notifications** to in-process handlers. Dispatch is type-safe and reflection-free: every call resolves to its handler at compile time through a generic, while the internal registry erases handler types behind a `TypeId` table.

This guide wires a mediator with one handler per channel and dispatches each one in five minutes.

## Add the dependency

```toml
[dependencies]
hexeract = { version = "0.3", features = ["mediator"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The `mediator` feature pulls in `hexeract-core` and `hexeract-mediator`. You can also depend on `hexeract-mediator` directly if you prefer per-crate SemVer.

## Declare the messages

A `Command` mutates state and produces an output. A `Query` reads state. A `Notification` is a broadcast event fanned out to every subscriber.

```rust
use hexeract::core::{Command, Notification, Query};

pub struct RegisterUser {
    pub email: String,
}

impl Command for RegisterUser {
    type Output = u64;
}

pub struct CountUsers;

impl Query for CountUsers {
    type Output = u64;
}

pub struct UserRegistered {
    pub id: u64,
}

impl Notification for UserRegistered {}
```

Notifications are shared across every handler as `Arc<N>`, so `Notification` does not require `Clone`.

## Implement the handlers

Each channel has its own trait. Errors flow through `HexeractError` or any type that implements `Into<HexeractError>`.

```rust
use std::sync::Arc;

use hexeract::core::{CommandHandler, HandlerContext, HexeractError, NotificationHandler, QueryHandler};

pub struct UserRepository;

impl CommandHandler<RegisterUser> for UserRepository {
    type Error = HexeractError;
    async fn handle(&self, cmd: RegisterUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        // ... persist `cmd.email` and return the new identifier ...
        Ok(42)
    }
}

pub struct UserCounter;

impl QueryHandler<CountUsers> for UserCounter {
    type Error = HexeractError;
    async fn handle(&self, _q: CountUsers, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        Ok(1)
    }
}

pub struct AuditWriter;

impl NotificationHandler<UserRegistered> for AuditWriter {
    type Error = HexeractError;
    async fn handle(&self, n: Arc<UserRegistered>, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        // ... append to audit storage ...
        let _ = n.id;
        Ok(())
    }
}
```

## Build the mediator

The `MediatorBuilder` is a fluent immutable builder. Each `register_*` call returns the builder by value.

```rust
use hexeract::mediator::MediatorBuilder;

let mediator = MediatorBuilder::new()
    .register_command_handler::<RegisterUser, _>(UserRepository)
    .register_query_handler::<CountUsers, _>(UserCounter)
    .register_notification_handler::<UserRegistered, _>(AuditWriter)
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Registering a second handler for the same `Command` or `Query` is a build-time error (`MediatorBuildError::DuplicateHandler`). Notifications accept multiple handlers and fan out in registration order.

## Dispatch

```rust
# async fn run(mediator: hexeract::mediator::Mediator) -> Result<(), Box<dyn std::error::Error>> {
let id = mediator.send(RegisterUser { email: "alice@example.com".into() }).await?;
assert_eq!(id, 42);

let total = mediator.query(CountUsers).await?;
assert_eq!(total, 1);

mediator.publish(UserRegistered { id }).await?;
# Ok(()) }
```

`Mediator` is cheap to `Clone` (shared `Arc<MediatorInner>`); pass clones to spawned tasks freely.

## Where to go next

- [Mediator CQRS semantics](../concepts/mediator-cqrs.md): exact contract of each channel.
- [Middleware pipeline](../concepts/middleware-pipeline.md): wire tracing, timeouts, your own cross-cutting concerns.
- [`#[handler]` macro](../concepts/handler-macro.md): generate trait implementations and a sanity check that catches missing registrations.
- [Mediator architecture](../architecture/mediator-flow.md): registry layout, dispatch sequence, fan-out fail-safe semantics.
