# Outbox MVP: requirements that drove v0.1.0

This document captures the public requirements that shaped the Outbox MVP shipped in v0.1.0. The Outbox is the first feature of the Hexeract framework to reach a usable release because a real-world dogfooding service needed it before the rest of the framework could grow around it.

The audience for this document is anyone considering Hexeract for an audit-grade event log or for atomic publication of domain events alongside business state mutations.

## Context

The dogfooding service required atomic publication of two effects per business operation:

1. Persistence of the operational state (user registrations, password resets, login events, etc.) into an operational PostgreSQL database.
2. Emission of an immutable audit event into a separate audit log stored on a different PostgreSQL database, isolated for defense-in-depth and compliance reasons (SOC 2 / ISO 27001 controls around audit storage).

Cross-database transactions are not an option for this client. Two-phase commit was rejected as too operationally expensive, and an "audit best-effort" mode that could lose audit entries was rejected as non-compliant. The Outbox pattern was the only acceptable design.

## Use cases

The dogfooding service publishes a handful of event families through the outbox:

| Domain operation | Event |
|---|---|
| Account creation | `UserRegistered` |
| Email verification | `EmailVerified` |
| Login success | `LoginSucceeded` |
| Login failure | `LoginFailed` |
| Password reset requested | `PasswordResetRequested` |
| Password changed | `PasswordChanged` |
| Logout | `LogoutSucceeded` |

Each event is dispatched to a single handler that writes a derived audit record into the audit database. The audit record carries a tamper-resistance chain (HMAC-based), but that detail is a concern of the handler implementation, not of Hexeract.

## API requirements

### Publication

The use case publishes an event from inside its own business transaction:

```rust
async fn register_user(
    &self,
    cmd: RegisterUserCommand,
) -> Result<RegisterUserOutput, ApplicationError> {
    let mut client = self.pool.get().await?;
    let mut tx = client.transaction().await?;

    self.user_repo.insert(&mut tx, &user).await?;

    let event_id = self
        .outbox
        .publish_in_tx(
            &mut tx,
            &UserRegistered { user_id, email, occurred_at },
        )
        .await?;

    tx.commit().await?;
    Ok(RegisterUserOutput { user_id, event_id })
}
```

Guarantee: if `tx.commit()` succeeds, the event IS persisted in the outbox. If `tx.commit()` fails, neither the state mutation nor the event exist.

### Worker

The dogfooding service starts the worker at process boot:

```rust
let worker = PgOutboxWorkerBuilder::new(operational_pool.clone())
    .table_name("audit_outbox")
    .register_handler::<UserRegistered, _>(audit_writer.clone())
    .register_handler::<EmailVerified, _>(audit_writer.clone())
    // ... one handler registration per event type ...
    .poll_interval(Duration::from_millis(100))
    .batch_size(10)
    .build()?;

let cancel = CancellationToken::new();
let join = tokio::spawn(worker.run(cancel.clone()));
```

The worker is generic so the same code path works for any PostgreSQL-backed outbox.

## Guarantees demanded

| Guarantee | Required level |
|---|---|
| Publication atomicity | Strict (same transaction as business writes). |
| Dispatch | At-least-once. Handlers are idempotent through a separate uniqueness constraint on the audit log. |
| Partial ordering | Events sharing the same aggregate (a user, an account, ...) must arrive in insertion order. |
| Multi-worker safety | `SELECT ... FOR UPDATE SKIP LOCKED` for safe horizontal scaling. |
| Retry | Implicit (failure leaves `delivered_at` NULL). Exponential backoff was explicitly out of scope for the MVP. |

## Performance targets

| Metric | Target | Rationale |
|---|---|---|
| `publish_in_tx` p99 latency | < 5 ms | Single insert inside an existing transaction; no extra connection acquisition. |
| Dispatch latency p99 | < 200 ms | Acceptable for an audit log feeding compliance reports; not real-time. |
| Sustained throughput | 100 events/s with default tuning | Peak operational volume is around 10 events/s; the 10x margin handles spikes comfortably. |
| Workers concurrent | 1..N | The service runs N replicas behind a load balancer; the outbox must support multi-worker dispatch without coordination. |

## Security requirements

- **No secrets in payload**. The caller is responsible for not putting passwords, tokens or hashes into events. Hexeract logs `event_type` and `event_id` only at the INFO level and never logs payload bytes (the `Debug` impl of `OutboxEnvelope` masks the payload).
- **Multi-database isolation**. The worker reads the outbox from one pool and dispatches handlers that own their own pool. Hexeract does not pilot the handler's database connection.
- **Schema injection prevention**. The table name is validated strictly (`^[a-zA-Z_][a-zA-Z0-9_]*$`) before being concatenated into prepared SQL statements.

## Out of scope (explicitly deferred)

The following capabilities were deliberately excluded from v0.1.0 to keep the MVP focused:

- **Bus**: external message broker integration (RabbitMQ, NATS, Kafka, SQS). Deferred to v0.2 / v0.9.
- **Sagas**: long-running workflows with persisted state. Deferred to v0.8.
- **Scheduler**: delayed and cron-scheduled messages. Deferred to v0.6.
- **Request and Reply**: RPC-style synchronous calls on top of an asynchronous bus. Deferred to v0.7.
- **Mediator in-process**: command and query dispatch through a typed registry. Partially landed in v0.0.1 placeholders, full release deferred to v0.3.
- **Exponential backoff**: failed rows are retried with a constant `retry_delay`. Backoff lands in v0.5.
- **Dead-letter queue**: rows past `max_attempts` stop being polled; observability is via SQL.

## Design decisions worth flagging

- **UUIDv7 minted by the publisher**: the publisher returns the freshly minted `event_id` so callers can attach it to traces and downstream calls without generating it themselves. UUIDv7 carries an embedded millisecond timestamp + monotonic counter so ordering by `event_id` matches insertion order.
- **JSON payload over BYTEA**: JSONB supports indexing and ad-hoc querying. The handler decodes the payload through `OutboxEnvelope::decode<E>()` which validates the `event_type` before deserialisation.
- **Boxed `Database` error**: backend implementations box their native error (e.g. `tokio_postgres::Error`) so the `hexeract-outbox` crate stays free of backend dependencies. Callers can downcast to the concrete error type if needed.
- **Generic worker, concrete builder**: the `OutboxWorker` is generic over `OutboxStore` so future backends (SQLite, MySQL) reuse the same worker code. The `PgOutboxWorkerBuilder` is concrete to give the common PostgreSQL case a single-call fluent API.

## Acknowledgements

The dogfooding service team contributed the use cases, guarantees and performance targets that shaped this MVP. The pattern (`publish_in_tx` returning the event_id, builder-driven worker, multi-database handler ownership) maps directly to their integration sequence. Subsequent versions of Hexeract will harvest more feedback as the framework grows beyond a single client.
