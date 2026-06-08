# Mediator CQRS semantics

Hexeract's in-process mediator splits dispatch across three channels: **Command**, **Query** and **Notification**. Each channel has a distinct contract enforced at the trait level. This page details those contracts.

## Command

A command expresses the intent to mutate state. Exactly one handler is allowed per command type; registering a second is a build error.

```rust
use hexeract::core::{Command, CommandHandler, HandlerContext};

pub struct ChangeUserEmail {
    pub id: u64,
    pub new_email: String,
}

impl Command for ChangeUserEmail {
    type Output = ();
}
```

The associated `Output` type names what `Mediator::send` returns. Use `()` when the handler returns nothing meaningful (typical for write-only commands).

**Single handler invariant.** Commands have a unique owner in the codebase: the write-side service that knows how to mutate the corresponding aggregate. Hexeract enforces this at build time so that a refactor that accidentally registers two services for the same command fails fast.

## Query

A query expresses the intent to read state without mutation. Like commands, exactly one handler is allowed per query type.

```rust
use hexeract::core::{Query, QueryHandler, HandlerContext};

pub struct GetUserByEmail {
    pub email: String,
}

#[derive(Debug)]
pub struct UserView {
    pub id: u64,
    pub email: String,
}

impl Query for GetUserByEmail {
    type Output = Option<UserView>;
}
```

The trait `Query` is a marker. Hexeract does not enforce read-only semantics at the type level: a `QueryHandler` *could* mutate state. Convention asks that you do not.

## Notification

A notification is a broadcast event. Zero, one, or many handlers may subscribe to the same notification type. `Mediator::publish` fans out to every registered handler concurrently.

```rust
use hexeract::core::{Notification, NotificationHandler, HandlerContext};

#[derive(Clone)]
pub struct UserEmailChanged {
    pub id: u64,
    pub previous: String,
    pub current: String,
}

impl Notification for UserEmailChanged {}
```

`Notification` does not require `Clone`: the mediator shares one `Arc<N>` across every registered handler, so the payload is never deep-cloned per handler. A handler that needs an owned value clones out of the `Arc`.

**Fan-out fail-safe.** Handlers run concurrently and every handler runs regardless of its siblings. If one or more return an error, the mediator aggregates the typed errors into a single `HexeractError::PublishFailed`, exposing each failure as a `NotificationFailure { handler, error }` in registration order so the caller can recover an individual handler's error and its `source` chain. Its `Display` keeps the `"publish: N of M handlers failed: ..."` summary. Sibling handlers never silently lose their turn.

**Zero handlers is a no-op.** Publishing a notification with no registered subscriber returns `Ok(())`. This is intentional: an audit hook removed in development should not break the publishing code path.

## Identifiers

Every dispatch carries two identifiers in its `HandlerContext`:

- **`MessageId`** uniquely identifies one dispatch invocation. Each handler in a notification fan-out gets its own fresh `MessageId`.
- **`CorrelationId`** links a dispatch to its causal chain. All handlers in a single `publish` call share the same `CorrelationId`, so traces can correlate the fan-out back to the publish.

See [Correlation ID propagation](correlation-id.md) for the broader pattern.

## Error model

All three handler traits expose an associated `Error: Into<HexeractError>` type. Use `HexeractError` directly for fast prototyping, or your own typed error for production:

```rust
use hexeract::core::{CommandHandler, HandlerContext, HexeractError};

#[derive(thiserror::Error, Debug)]
pub enum UserError {
    #[error("email already in use")]
    EmailTaken,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

impl From<UserError> for HexeractError {
    fn from(err: UserError) -> Self {
        HexeractError::handler_failed(err)
    }
}

# struct ChangeUserEmail;
# impl hexeract::core::Command for ChangeUserEmail { type Output = (); }
struct UserService;
impl CommandHandler<ChangeUserEmail> for UserService {
    type Error = UserError;
    async fn handle(&self, _cmd: ChangeUserEmail, _ctx: &HandlerContext) -> Result<(), UserError> {
        Err(UserError::EmailTaken)
    }
}
```

The mediator wraps `UserError` via `Into<HexeractError>` at the dispatch boundary. The original error stays available through `HexeractError::HandlerFailed { source }`.

## Missing handler

Dispatching a command or a query with no registered handler returns `HexeractError::HandlerNotFound { message_type }`, where `message_type` is the fully-qualified type name of the unregistered command, query or notification.

## When to use which channel

| If your call... | Pick |
| --- | --- |
| Mutates state and may fail | Command |
| Returns data without mutating | Query |
| Announces a fact that other parts of the system may react to | Notification |

This is the standard CQRS triad. Hexeract does not enforce read-write separation at the type level beyond the single-handler invariant; the discipline is on you, and that is the point.
