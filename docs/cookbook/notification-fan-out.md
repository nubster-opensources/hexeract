# Fan out a domain event to multiple subscribers

You want a single domain event (say, `UserRegistered`) to trigger several independent reactions: write an audit row, send a welcome email, refresh a search index. Each reaction lives in its own handler; failures in one must not block the others.

This is exactly what `mediator.publish::<N>` does.

## Recipe

```rust
use std::sync::Arc;
use std::sync::Mutex;

use hexeract::core::{HandlerContext, HexeractError, Notification, NotificationHandler};
use hexeract::mediator::MediatorBuilder;

#[derive(Clone)]
pub struct UserRegistered {
    pub id: u64,
    pub email: String,
}

impl Notification for UserRegistered {}

pub struct AuditHandler {
    audit_log: Arc<Mutex<Vec<u64>>>,
}

impl NotificationHandler<UserRegistered> for AuditHandler {
    type Error = HexeractError;
    async fn handle(&self, n: UserRegistered, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        self.audit_log
            .lock()
            .expect("audit log poisoned")
            .push(n.id);
        Ok(())
    }
}

pub struct EmailHandler;

impl NotificationHandler<UserRegistered> for EmailHandler {
    type Error = HexeractError;
    async fn handle(&self, n: UserRegistered, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        // ... send welcome email to `n.email` ...
        let _ = n.email;
        Ok(())
    }
}

pub struct SearchIndexHandler;

impl NotificationHandler<UserRegistered> for SearchIndexHandler {
    type Error = HexeractError;
    async fn handle(&self, n: UserRegistered, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        // ... insert into search index ...
        let _ = n.id;
        Ok(())
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let audit_log = Arc::new(Mutex::new(Vec::new()));

let mediator = MediatorBuilder::new()
    .register_notification_handler::<UserRegistered, _>(AuditHandler {
        audit_log: Arc::clone(&audit_log),
    })
    .register_notification_handler::<UserRegistered, _>(EmailHandler)
    .register_notification_handler::<UserRegistered, _>(SearchIndexHandler)
    .build()?;

mediator
    .publish(UserRegistered {
        id: 1,
        email: "alice@example.com".into(),
    })
    .await?;

assert_eq!(*audit_log.lock().unwrap(), vec![1]);
# Ok(()) }
```

The three handlers run sequentially in registration order. Each one receives its own clone of the notification (`Notification: Clone` is enforced by the trait). All three share the same `CorrelationId` so observability tools can stitch the fan-out back to its publish site.

## Fail-safe semantics

If one of the three handlers returns an error, the mediator still calls the remaining two. The final `Err` returned by `publish` aggregates all failures into a `HexeractError::Dispatch` with the format:

```text
publish: 1 of 3 handlers failed: dispatch error: smtp connection refused
```

Caller code can match on the variant:

```rust,ignore
match mediator.publish(UserRegistered { /* ... */ }).await {
    Ok(()) => { /* all good */ }
    Err(HexeractError::Dispatch(msg)) if msg.starts_with("publish:") => {
        // at least one handler failed; sibling handlers still ran
        tracing::error!(error = %msg, "partial notification fan-out failure");
    }
    Err(err) => return Err(err),
}
```

This is the right semantic for audit + email + search-index: the audit must always be written even if the email service is down, and the search index must catch up even if both audit and email failed.

## Zero handlers is a no-op

`publish` returns `Ok(())` if no handler is registered for the notification type. This is intentional: removing the only audit handler during development should not break the publishing code path.

## Large payloads

Each handler receives a clone of the notification. If your payload is large (`Vec<u8>`, deep struct), the clone overhead adds up. Wrap shared data behind `Arc<T>`:

```rust,ignore
#[derive(Clone)]
pub struct UserRegistered {
    pub id: u64,
    pub user_record: Arc<UserRecord>,   // shared, no deep clone per handler
}
```

`Arc::clone` is `O(1)` (one atomic increment).

## Async pre-event side effects

A common temptation: a notification handler that reads from a database to fetch context. If the handler can fail because the database is down, you have to decide whether to retry or skip. Hexeract does not retry handlers automatically; a custom middleware can wrap a handler in retry-on-error logic, but be careful: the middleware runs once per dispatch, not per handler, so it cannot retry a single handler in isolation. The clean approach is to make the handler itself retryable (with its own backoff and a circuit breaker) and accept that the aggregate error in `publish` is the upper-bound failure signal.

## Combining with the outbox

If your notification really represents a fact crossing service boundaries (downstream services must see it), do **not** publish it via the in-process mediator. Use the [outbox pattern](outbox-plus-mediator.md) instead: the outbox guarantees at-least-once delivery, the in-process mediator does not.

The in-process `publish` is the right tool for **local reactions** (audit, cache invalidation, in-memory index updates). Use the outbox for **cross-service events**.
