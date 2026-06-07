# Production checklist

Run through this list before letting a Hexeract-powered service answer a real workload. Each item is a one-line check followed by where to read more.

## Outbox

- [ ] **Schema applied.** Either `POSTGRES_SCHEMA_SQL` rendered with your table name or `ensure_schema(pool, "<table>")` ran at startup. See [outbox PostgreSQL schema](../reference/outbox-postgres-schema.md).
- [ ] **Pool sized for both writers and the worker.** Each `OutboxWorker` instance holds one connection per poll cycle; size your `deadpool_postgres::Pool` at least `business_writers + workers + headroom`.
- [ ] **Idempotency wired on the handler side.** Handlers can be redelivered. Store a `processed_event_id` table or short-circuit on a deduplication key.
- [ ] **Tuning matches your latency target.** Default `poll_interval = 100 ms` gives a publish-to-dispatch p99 around 200 ms. Drop to `20-50 ms` for tighter SLOs, scale workers horizontally before lowering further.
- [ ] **`max_attempts` not silently absorbing bugs.** A row past `max_attempts` stops being polled. Audit pending failures with `SELECT event_id, last_error FROM audit_outbox WHERE delivered_at IS NULL AND attempts >= 5`.
- [ ] **Backup includes the outbox table.** It carries side-effect commitments that have not yet been dispatched.

## Bus

- [ ] **Topology declared outside the hot path.** Run `hexeract bus declare --topology FILE` during deployment, or call `ensure_topology` once at service startup. Do not call `declare_*` helpers on every publish.
- [ ] **Durable queues for at-least-once semantics.** Set `durable = true` on every queue that must survive a broker restart, plus `auto_delete = false`.
- [ ] **Prefetch matched to handler throughput.** Default `prefetch = 16` is appropriate for most cases; raise for fast, CPU-bound handlers, lower for handlers that block on slow downstream calls.
- [ ] **AckMode chosen consciously.** Manual (at-least-once) is the default; only choose a lossy [`AckMode`](../concepts/ack-modes.md) (`AckOnReceive` for at-most-once, `Unacknowledged` for fire-and-forget) when delivery loss is acceptable.
- [ ] **Publish mode chosen consciously.** The transport awaits a publisher confirm by default, so `Ok` proves the broker stored the message and an unroutable routing key raises `BusError::Unroutable`. Only switch a transport to `fire_and_forget()` when loss is acceptable on the publish side, mirroring the consume-side trade-off above.
- [ ] **Dead-letter routing key configured** when at-least-once must not drop on exhaustion. See [retry policy](../concepts/retry-policy.md).
- [ ] **Broker reconnect tested.** `RabbitMqConnection::connect_with_retry` retries on startup, but the running connection does not auto-reconnect mid-session. Wrap your worker spawn in a supervisor that restarts on terminal broker errors.

## Service runtime

- [ ] **Graceful shutdown propagates the `CancellationToken`.** SIGTERM, SIGINT and admin-triggered drains all call `cancel.cancel()` before awaiting the worker join handle.
- [ ] **Worker `JoinHandle` awaited and inspected.** A panic inside a handler bubbles to the join handle; surface it through structured logging.
- [ ] **Tracing subscriber installed early.** `hexeract-bus-rabbitmq` and `hexeract-outbox` emit `tracing::warn` and `tracing::error` events on retries, decode failures and DLR routing. A missing subscriber discards those signals.
- [ ] **No `RUSTFLAGS=-D warnings` removed in production builds.** Warnings flag unused futures, unhandled results and lint regressions that often turn into runtime bugs.

## Observability

- [ ] **Structured logs.** `tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env())` is the minimum; pair with a JSON layer when shipping to a log aggregator.
- [ ] **Per-publish `message_id` propagated.** Log it on the producer side, log it on the consumer side, correlate across services. Both `OutboxEnvelope` and `BusEnvelope` carry UUIDv7 identifiers ready to be stitched together.
- [ ] **Correlation chain preserved.** Use `publish_with_correlation_id` from inside handlers to forward the inbound `ctx.correlation_id`. See [correlation ID](../concepts/correlation-id.md).
- [ ] **Metrics exported.** Hexeract does not (yet) expose Prometheus metrics natively; instrument the handler call site and the publish call site with your existing instrumentation crate.

## Security

- [ ] **Connection string out of source control.** Use environment variables (`DATABASE_URL`, `HEXERACT_BUS_URL`) or a secret manager.
- [ ] **TLS enabled on broker connections.** `amqps://` instead of `amqp://`; the lapin connection picks the right scheme automatically.
- [ ] **Credentials scoped per service.** A consumer service does not need publish permissions on every exchange; tighten the broker authorisation rules.
- [ ] **Database role least-privileged.** The outbox publisher needs `INSERT` on the outbox table; the worker needs `SELECT FOR UPDATE` and `UPDATE`. No `DROP`, no `TRUNCATE`.

## CI gates

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] `cargo deny check` (supply-chain audit; see the project `deny.toml`)
- [ ] Integration tests with `--ignored` against real PostgreSQL and RabbitMQ containers on the merge queue.

## Capacity planning

| Workload shape | Recommendation |
| --- | --- |
| Bursts up to 100 events/s | Default `OutboxWorker` config, single worker |
| Sustained 100-500 events/s | Two `OutboxWorker` instances sharing the table; `SELECT ... FOR UPDATE SKIP LOCKED` handles the contention |
| > 500 events/s | Horizontal worker pool, per-service outbox table, partition by `subject_id` if hot rows appear |
| Bursty bus consumer with slow downstream calls | Raise `prefetch` cautiously, prefer scaling worker instances over inflating prefetch |
