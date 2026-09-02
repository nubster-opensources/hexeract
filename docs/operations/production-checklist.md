# Production checklist

Run through this list before letting a Hexeract-powered service answer a real workload. Each item is a one-line check followed by where to read more.

## Outbox

- [ ] **Schema applied via migration tooling.** Generate the canonical DDL with `hexeract outbox patch --table <name>` (or programmatically via `Dialect::Postgres.schema_ddl("<name>")?`) and apply it through your versioned migration tool before deployment. `hexeract_outbox_sql::postgres::ensure_schema(&pool, "<name>")` is an idempotent helper reserved for POC and integration tests; do not call it at production startup (the runtime role should not hold DDL privileges). See [outbox PostgreSQL schema](../reference/outbox-postgres-schema.md).
- [ ] **Pool sized for both writers and the worker.** Each `OutboxWorker` instance holds one connection per poll cycle; size your `sqlx::PgPool` at least `business_writers + workers + headroom` (configure via `sqlx::postgres::PgPoolOptions::new().max_connections(n)`).
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
- [ ] **Metadata limits reviewed against your real headers.** [`AmqpMetadataLimits`](../reference/hexeract-bus-rabbitmq.md#metadata-limits) defaults to 64 headers, 128 key bytes, 8 KiB per value and 32 KiB in total, applied to application and framework `x-hexeract-*` headers together. Count what your deployment actually sends (trace context, tenancy, RPC wire fields, and RabbitMQ's own `x-death` history on a retrying queue) and raise or lower the bound deliberately. A publish above the bound fails, and an inbound delivery above it is refused before any handler runs, so a limit set too low is an outage, not a silent truncation.
- [ ] **Metadata limits set identically on every path.** Set the same value on the worker (`.metadata_limits(..)`), the publisher (`RabbitMqTransport::with_metadata_limits`) and the request client (`RabbitMqRequestClientConfigBuilder::metadata_limits`). A single path left on the defaults is the bound that actually applies to an attacker.
- [ ] **Broker-side `max_message_size` set to the deployment's real ceiling.** This is the ingress defense that acts *before* the client: Hexeract's limits only bound work after `lapin` has already decoded a delivery, so they complement it rather than replace it. Set it to the largest message the application legitimately sends, not to a value chosen to mirror the client limits.
- [ ] **`frame_max` left at the negotiated default.** RabbitMQ recommends retaining the broker/client negotiated value. It is a transport framing parameter, not an application metadata policy; bound metadata with `AmqpMetadataLimits` and messages with `max_message_size` instead.
- [ ] **Dead-letter consumers tolerate an empty header table.** A delivery quarantined for invalid metadata is republished with its field table rebuilt empty, keeping only bounded core properties (`message_id`, `correlation_id`, `type`, `reply_to`, `timestamp`, delivery mode). Anything downstream that routes on a header must handle its absence.

## Scheduler

- [ ] **Schema applied via the CLI, never hand-edited.** Generate the DDL with `hexeract scheduler schema --dialect <postgres|my-sql|sqlite>` (note: the MySQL dialect token is `my-sql`, kebab-cased) and apply it through your versioned migration tool. The CLI is the source of truth for the table shape.
- [ ] **Worker sized for your throughput.** `build()` enforces `lease >= batch_size x dispatch_timeout` (rejecting the configuration otherwise, including on overflow), because settling a claimed batch is sequential and a shorter lease could expire before the last occurrence in the batch is even dispatched. Defaults: `lease` 300s, `batch_size` 10, `dispatch_timeout` 30s, `poll_interval` 100ms. If you raise `batch_size` or `dispatch_timeout`, raise `lease` to match or `build()` will return `SchedulerError::InvalidConfiguration`.
- [ ] **Dispatch lag monitored.** Track the gap between an occurrence's due time and its actual dispatch time; a growing gap signals an under-provisioned worker pool or a stuck sink.
- [ ] **Dead-letter alerted and operated.** Alert on dead-letter growth, inspect entries with `hexeract scheduler dead-letter list`, and replay a schedule with `hexeract scheduler dead-letter replay <schedule-id>` once the underlying cause is fixed.

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
- [ ] **TLS enabled on broker connections.** Use `amqps://` instead of `amqp://`; the default configuration validates against the platform trust store. For an internal CA or mutual TLS, pass a `RabbitMqConnectionConfig` containing `OwnedTLSConfig` to every connection constructor, or through `RabbitMqRequestClientConfigBuilder::connection_config`; load the CA, client certificate, and its password from the service's secret manager. An internal CA is added to the platform trust store, not substituted for it, so it does not pin trust to your own authority: a certificate issued by any publicly trusted CA for the broker hostname remains acceptable. Rely on mutual TLS and per-service credentials for authentication, not on the private CA alone.
- [ ] **Plaintext restricted to local development.** `amqp://` is accepted by default only for `localhost`, `127.0.0.0/8`, and `::1`; remote plaintext is rejected before connecting. Use `amqps://` in production, never `allow_insecure_plaintext_transport`. The same rule governs `hexeract bus declare`, `peek` and `purge`: no runbook targeting a production broker should carry their `--insecure-plaintext` flag.
- [ ] **Remote brokers addressed by hostname, not by IPv6 literal.** `lapin` discards a bracketed IPv6 literal and dials `localhost` instead, so `amqps://[2001:db8::1]:5671` silently targets the local machine. Use a hostname, which is also what certificate validation needs.
- [ ] **TLS material matched to the URI scheme.** Configuring a CA or a client identity alongside a plaintext `amqp://` URI is refused rather than ignored, because lapin would discard it and connect in cleartext. If a deployment fails to start with that error, fix the scheme rather than reaching for `allow_insecure_plaintext_transport`, which re-enables the silent downgrade.
- [ ] **`outbox apply` and `outbox check` use TLS by default; scheduler admin commands do not.** `outbox apply`/`outbox check` upgrade any `sslmode` other than an explicit `disable` to `require` and connect via `rustls` against the operating-system trust store; only `sslmode=disable` in the connection string opts into plaintext, and a warning is logged when it does. `scheduler list`/`inspect`/`dead-letter` open their PostgreSQL pool through `sqlx` directly, which defaults to `sslmode=prefer` and silently falls back to cleartext if the server declines TLS. For those commands, set `sslmode=require` explicitly in `DATABASE_URL`.
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
