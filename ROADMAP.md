# Roadmap

Hexeract is pre-stable. This document captures the intended trajectory of the project up to v1.0, ordered by release. **No dates are committed.** The project is sponsored on a best-effort basis by Encelade Technologies, and releases ship when they are ready, not when a calendar says so.

The roadmap mirrors the [GitHub milestones](https://github.com/nubster-opensources/hexeract/milestones) one-for-one. Each section here is the public, prose form of a milestone; each milestone groups the issues that must close before the release ships. The full design notes for any given release live under `docs/design/`.

## Out of scope

Hexeract is a Wolverine-style messaging framework for Rust, combining in-process dispatch, transactional outbox, broker transports, schedulers, request-reply and sagas. The following will never be in scope, regardless of demand:

- **Service mesh.** No sidecar proxies, no traffic shaping, no L7 routing.
- **Brokered queue replacement.** Hexeract integrates with brokers; it does not aim to replace Kafka, NATS, RabbitMQ or their managed equivalents.
- **Saga choreography engine without an outbox.** Distributed coordination in Hexeract is grounded in the transactional outbox pattern. Best-effort choreography without a durable boundary will not be added.
- **General-purpose application framework.** Hexeract focuses on messaging, handlers and transactional dispatch. It is not an HTTP framework, an ORM or a dependency-injection container.

These boundaries are deliberate and non-negotiable. If a feature request crosses one of them, it belongs in another project.

## v0.1.0: Outbox MVP — **DONE**

**Goal.** A Rust service writes a domain event inside the same database transaction as its business state, and a background worker delivers that event reliably with retry and back-off.

**Shipped:**

- `hexeract-outbox` backend-agnostic core: `Event`, `OutboxEnvelope`, `OutboxPublisher`, `OutboxStore`, `Handler`, `OutboxWorker`, `OutboxError`, `ErasedHandler`, `TypedHandler`.
- `hexeract-outbox-postgres`: canonical schema, `PgOutboxPublisher`, `PgOutboxStore`, `PgOutboxWorkerBuilder`, BYO-schema strategy with `POSTGRES_SCHEMA_SQL` + `ensure_schema` helper.
- `hexeract-cli` with the `outbox` namespace: `patch`, `apply`, `check` subcommands.
- Worker poll loop using `SELECT ... FOR UPDATE SKIP LOCKED` for safe multi-worker concurrency, basic retry with `attempts`, `last_error`, `next_retry_at` and a fixed `retry_delay`.
- Publishing via `publish_in_tx(&mut tx, &event) -> Result<Uuid, OutboxError>` mints a UUIDv7 internally and returns it for traceability.
- Tracing instrumentation that never logs payload bytes.
- End-to-end runnable example against two PostgreSQL containers (`02_outbox_two_databases`).
- Criterion benchmark of `publish_in_tx` against a real PostgreSQL container.
- Full release infrastructure: CHANGELOG, SEMVER and MSRV policies, SECURITY policy, Code of Conduct, GitHub templates, dependabot, release and docs workflows, integration tests workflow.

Released as v0.1.0 on crates.io. The seven shipped crates are `hexeract-core`, `hexeract-outbox`, `hexeract-outbox-postgres`, `hexeract-macros`, `hexeract-mediator`, `hexeract-cli` and the `hexeract` facade.

## v0.2.0: Bus RabbitMQ

**Goal.** A unified `Transport` trait with a first RabbitMQ implementation via `lapin`. Publish, subscribe and ack semantics, JSON serialization, type-based routing, message envelopes carrying `MessageId`, `CorrelationId`, optional `reply_to` and free-form headers. Distant messaging is functional without persistence.

**Crates:**

- `hexeract-bus`: backend-agnostic core (`Message`, `BusEnvelope`, `BusError`, `Transport`, `Handler<M>`, `ErasedHandler`, `TypedHandler`, topology types).
- `hexeract-bus-rabbitmq`: lapin-backed `Transport`, topology declaration helpers, consumer worker with auto-ack and retry policy.

**CLI:**

- `hexeract bus declare`, `peek`, `purge` subcommands.

## v0.3.0: Mediator Core

**Goal.** Dispatch a typed `Command` to its `Handler` in-process, type-safe and reflection-free. The pattern Wolverine and MediatR popularised in .NET, but with compile-time guarantees instead of runtime registries.

**Scope:**

- `MediatorBuilder` with `register_command_handler::<C, H>(handler)` API.
- Built-in middlewares: `TracingMiddleware`, `LoggingMiddleware`, `TimeoutMiddleware`.
- `#[handler]` procedural macro that wires a struct into the compile-time registry without boilerplate.
- `hexeract` facade crate re-exports the curated surface.

## v0.4.0: Outbox Multi-Database

**Goal.** Portable outbox schema across SQLite and MySQL in addition to Postgres. The same `OutboxStore` trait, same `OutboxWorker` code, three backend crates.

**Scope:**

- `hexeract-outbox-sqlite` and `hexeract-outbox-mysql` crates mirroring the PostgreSQL backend.
- Portable SQL schema strategy with embedded migrations available as constants.
- Integration tests via `testcontainers` covering each engine.

## v0.5.0: Reliability

**Goal.** Configurable resilience for handlers and workers. Failures become predictable, not catastrophic.

**Scope:**

- Per-handler retry policies with exponential backoff and jitter.
- Dead-letter queue for poison messages, observable via SQL or CLI.
- Deadline propagation on message envelopes.
- Cancellation-safe handler execution and graceful shutdown.

## v0.6.0: Scheduler

**Goal.** Send a message in the future. Same primitives as the Outbox plus a time dimension.

**Scope:**

- Scheduled messages with `delay` and `cron` triggers.
- Persistent retry storage.
- Automatic promotion to the dead-letter queue after exhausted retries.
- Native integration with the Bus (publish later via a broker) and the Outbox (commit later in a business transaction).

## v0.7.0: Request and Reply

**Goal.** Synchronous RPC pattern over the asynchronous bus, via correlation IDs.

**Scope:**

- `tokio::sync::oneshot` correlation map keyed by `CorrelationId`.
- Per-call timeouts and context propagation.
- Cancellation-safe: a dropped request stops the wait without leaking the reply slot.
- Reply queue management via the existing `reply_to` field on `BusEnvelope`.

## v0.8.0: Sagas

**Goal.** Long-running stateful workflows with explicit compensation.

**Scope:**

- State machine persisted in the same database as the Outbox (atomic transition + outbox dispatch).
- Explicit compensation steps invoked on terminal failure.
- Saga-level retries, timeouts and correlation strategies.
- Test harness for fast-forwarding saga state in unit tests.

## v0.9.0: Polyglot Transports

**Goal.** Cover the rest of the brokered transport landscape so non-Rust services can join the conversation.

**Scope:**

- `hexeract-bus-nats` via `async-nats`.
- `hexeract-bus-kafka` via `rdkafka`.
- `hexeract-bus-sqs` for AWS SQS.
- Per-message-route transport selection (a single application can publish to RabbitMQ for some events and Kafka for others).
- Integration tests via `testcontainers` for each broker.

## v0.10.0: Polish and Stability

**Goal.** Hexeract is usable by external early adopters without hand holding, the public API is frozen, and the documentation lives somewhere permanent.

**Scope:**

- Dedicated documentation site, either standalone or as a section on the Encelade Technologies portal.
- Onboarding tutorials per primary use case (web service, worker, CLI integration).
- Throughput and latency benchmarks covering Outbox, Bus and Scheduler.
- Full OpenTelemetry span coverage across the framework.
- Prometheus-compatible metrics endpoint exposed by the worker runtime.
- Dependency audit and minimal version selection review.
- Public Rust API surface frozen, with the migration plan towards v1.0 documented.

## Post-1.0 backlog

The items below have been discussed during the design phase but are not committed to any release. They will only ship if the project gains enough traction to justify the maintenance cost, and each will require its own design pass before any code lands.

- **gRPC transport.** A `hexeract-bus-grpc` backend for service-to-service messaging where stream semantics matter.
- **Inbox pattern.** Symmetric to the Outbox on the consumer side, deduplicating `message_id` at the database boundary.
- **WASM hosts.** Run handlers compiled to WebAssembly in a sandbox for untrusted plugin scenarios.
- **Visual saga inspector.** Web UI to observe saga state transitions in real time.
- **Sustainability.** Open Core or hosted premium tier, GitHub Sponsors.

## How this roadmap is maintained

Changes to this document are made by pull request, with a `docs(roadmap):` Conventional Commit. The scope of any released version is locked once its tag is pushed; the scope of later releases stays adjustable until the previous release ships.

If you spot something missing, redundant or out of scope, open an issue against the relevant milestone and tag it `discussion`.
