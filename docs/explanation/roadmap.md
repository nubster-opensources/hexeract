# Roadmap

| Version | Theme | Status |
| --- | --- | --- |
| v0.1.0 | Outbox MVP | Shipped |
| v0.2.0 | Bus RabbitMQ | Shipped |
| v0.3.0 | Mediator Core | Shipped |
| v0.4.0 | Outbox Multi-Database | Shipped |
| v0.5.0 | Reliability | Shipped |
| v0.6.0 | Scheduler | Shipped |
| v0.7.0 | Secure Request and Reply | In progress |
| v0.8.0 | Inbox and Consumer Reliability | Planned |
| v0.9.0 | Durable Sagas | Planned |
| v0.10.0 | External Adoption and v1.0 Readiness | Planned |
| v1.0.0 | Stable Contract | Planned |

Hexeract is pre-stable. This document captures the intended trajectory of the project up to v1.0, ordered by release. **No dates are committed.** The project is sponsored on a best-effort basis by Nubster, and releases ship when they are ready, not when a calendar says so.

The roadmap mirrors the [repository milestones](https://github.com/nubster-opensources/hexeract/milestones) one-for-one. Each section here is the public, prose form of a milestone; each milestone groups the issues that must close before the release ships. The full design notes for any given release live under `docs/design/`.

From v0.7 onward, each release has an explicit exit gate:

- Security and reliability work required by the release contract is not deferred as "polish".
- A milestone closes only when its documented guarantees are backed by tests.
- A later milestone may depend on an earlier one, but it must not silently absorb unfinished work from it.
- Speculative framework conveniences and additional transports wait until after v1.0 and external adopter feedback.

## Out of scope

Hexeract is a messaging framework for Rust, combining in-process dispatch, transactional outbox, broker transports, schedulers, request-reply and sagas. The following will never be in scope, regardless of demand:

- **Service mesh.** No sidecar proxies, no traffic shaping, no L7 routing.
- **Brokered queue replacement.** Hexeract integrates with brokers; it does not aim to replace Kafka, NATS, RabbitMQ or their managed equivalents.
- **Saga choreography engine without an outbox.** Distributed coordination in Hexeract is grounded in the transactional outbox pattern. Best-effort choreography without a durable boundary will not be added.
- **General-purpose application framework.** Hexeract focuses on messaging, handlers and transactional dispatch. It is not an HTTP framework, an ORM or a dependency-injection container.

These boundaries are deliberate and non-negotiable. If a feature request crosses one of them, it belongs in another project.

## v0.1.0: Outbox MVP (DONE)

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
- Full release infrastructure: CHANGELOG, SEMVER and MSRV policies, SECURITY policy, Code of Conduct, repository templates, dependabot, release and docs workflows, integration tests workflow.

Released as v0.1.0 on crates.io. The seven shipped crates are `hexeract-core`, `hexeract-outbox`, `hexeract-outbox-postgres`, `hexeract-macros`, `hexeract-mediator`, `hexeract-cli` and the `hexeract` facade.

## v0.2.0: Bus RabbitMQ (DONE)

**Goal.** A unified `Transport` trait with a first RabbitMQ implementation via `lapin`. Publish and ack semantics, JSON serialization, type-based routing, message envelopes carrying `message_id`, `correlation_id`, optional `reply_to` and free-form headers. Distant messaging is functional without persistence.

**Crates shipped:**

- `hexeract-bus`: backend-agnostic core (`Message`, `BusEnvelope`, `BusError`, `Transport`, `Handler<M>`, `ErasedHandler`, `TypedHandler`, topology types `Exchange`, `Queue`, `Binding`, `RoutingKey`).
- `hexeract-bus-rabbitmq`: lapin-backed `Transport`, `ChannelPool`, bounded reconnect loop, topology declaration helpers, consumer worker with `AckMode::Auto` / `AckMode::Manual` and a `max_attempts` + dead-letter routing-key retry policy.

**CLI shipped:**

- `hexeract bus declare --conn URL --topology FILE` applies a TOML topology.
- `hexeract bus peek --conn URL --queue NAME [--count N]` dumps the first messages of a queue non-destructively.
- `hexeract bus purge --conn URL --queue NAME --yes-i-know` drops every message from a queue (gated by the explicit safety flag).

**Deliverables:**

- End-to-end pub/sub example `crates/hexeract-bus-rabbitmq/examples/03_bus_pubsub.rs` spinning up a real RabbitMQ container via `testcontainers`.
- Sample topology file at `crates/hexeract-cli/examples/topology.toml`.
- Integration tests `#[ignore]`-gated and triggered by the existing `integration.yml` workflow.

## v0.3.0: Mediator Core (DONE)

**Goal.** Dispatch a typed `Command` to its `Handler` in-process, type-safe and reflection-free. A pattern popularised in the .NET ecosystem, with compile-time guarantees instead of runtime registries.

**Scope:**

- `MediatorBuilder` with `register_command_handler::<C, H>(handler)` API.
- Built-in middlewares: `TracingMiddleware` and `TimeoutMiddleware` (in `hexeract-middleware`).
- `#[handler]` procedural macro that wires a struct into the compile-time registry without boilerplate.
- `hexeract` facade crate re-exports the curated surface.

## v0.4.0: Outbox Multi-Database (DONE)

**Goal.** Portable outbox schema across SQLite and MySQL in addition to Postgres. The same `OutboxStore` trait, same `OutboxWorker` code, one `sqlx`-backed crate with a backend per feature.

**Scope:**

- A single `hexeract-outbox-sql` crate with one compile-time backend per Cargo feature (`postgres`, `mysql`, `sqlite`), replacing the originally planned separate `hexeract-outbox-sqlite` and `hexeract-outbox-mysql` crates.
- A shared `Dialect` centralizing statement templating, row locking and the per-engine schema DDL. The PostgreSQL schema stays byte-for-byte identical to `hexeract-outbox-postgres`, so no data migration is required.
- Integration tests via `testcontainers` covering each engine.

## v0.5.0: Reliability (DONE)

**Goal.** Configurable resilience for handlers and workers. Failures become predictable, not catastrophic.

**Scope:**

- Bounded exponential backoff with jitter for outbox retries (`retry_base_delay`, `retry_max_delay`, `jitter`).
- Opt-in durable dead-letter handling for poison messages, observable via SQL or CLI.
- Crash-safe claim (attempt counted at claim time) and dispatch outside the database transaction.
- `dispatch_timeout` enforced as a hard per-handler deadline; deadline and cancellation-safe graceful shutdown.
- Bus hardening: publisher confirms, bounded consumer buffer (`max_buffered`) and payload cap, plus a security sweep on the AMQP surface and the release CI.

## v0.6.0: Scheduler (DONE)

**Goal.** Send a message in the future. Same primitives as the Outbox plus a time dimension.

**Scope:**

- Scheduled messages with `delay` and `cron` triggers.
- Persistent retry storage.
- Automatic promotion to the dead-letter queue after exhausted retries.
- Native integration with the Bus (publish later via a broker) and the Outbox (commit later in a business transaction).

## v0.7.0: Secure Request and Reply (IN PROGRESS)

**Tracking:** [Request/Reply epic #442](https://github.com/nubster-opensources/hexeract/issues/442)

**Goal.** Deliver typed Request/Reply over the asynchronous bus without treating
broker metadata as an application trust boundary.

The versioned RPC v1 wire protocol is implemented on `main`: `RequestId` is
distinct from causal `correlation_id`, destinations are explicit, response
status/version/type are validated and internal error strings do not cross the
wire. Crate versions remain at 0.6.0 until every v0.7 release gate closes.

**Scope:**

- `tokio::sync::oneshot` registry keyed by `RequestId`, bounded by configurable
  backpressure.
- Per-call timeout, distributed deadline and causal context propagation.
- Cancellation-safe lifecycle: drop, timeout, reconnect and shutdown release
  every slot and permit exactly once.
- Exclusive RabbitMQ reply inbox with readiness gating across reconnects.
- First **valid** authenticated reply wins; malformed, late and duplicate
  replies cannot consume another call's slot.
- Dedicated, confirmed reply publication with a validated and authenticated
  `reply_to`.
- End-to-end envelope integrity, publisher identity and intended audience,
  complemented by least-privilege RabbitMQ ACLs and mTLS.
- Bounded payload and AMQP metadata, with a protected `x-hexeract-*` protocol
  namespace.
- RPC lifecycle spans and metrics that never expose payloads, secrets or
  internal error messages.

**Non-guarantee.** v0.7 does not claim global exactly-once processing. A valid
unchanged message can still be redelivered. Persistent cross-process duplicate
and replay suppression belongs to the v0.8 Inbox transaction boundary.

## v0.8.0: Inbox and Consumer Reliability

**Tracking:** [Inbox and consumer reliability epic #447](https://github.com/nubster-opensources/hexeract/issues/447)

**Goal.** Make consumer-side reliability as explicit as the transactional
Outbox before building long-running workflow orchestration on top.

**Scope:**

- Backend-agnostic persistent Inbox keyed by authenticated issuer, audience and
  message/request identity.
- Transactional idempotency middleware for PostgreSQL, MySQL and SQLite.
- Duplicate and replay suppression within a precisely documented database
  transaction boundary; no global exactly-once claim.
- Fenced claims and settlements so stale workers cannot overwrite newer work.
- Crash-safe Outbox and Scheduler failure handling, backoff, retention and
  terminal-row cleanup.
- Bounded batch concurrency and cancellation-aware worker drain.
- Public in-memory transport and conformance harnesses for deterministic tests.
- Priority and time-to-live semantics propagated consistently through Bus,
  Outbox and Scheduler.

This milestone is the durability foundation required by v0.9 Sagas.

## v0.9.0: Durable Sagas

**Tracking:** [Durable Sagas epic #455](https://github.com/nubster-opensources/hexeract/issues/455)

**Goal.** Deliver long-running, versioned stateful workflows with atomic Outbox
transitions and explicit compensation.

**Scope:**

- Typed, backend-independent saga definition and transition contract.
- Versioned persisted state with an explicit migration/upcasting policy.
- SQL saga store for PostgreSQL, MySQL and SQLite.
- Atomic saga-state transition plus Outbox append.
- Fenced multi-worker claims, correlation strategies and per-instance
  concurrency control.
- Durable timeouts through the Scheduler, bounded retries and terminal failure
  handling.
- Explicit, idempotent compensation whose progress survives restart.
- Deterministic virtual-time and crash-interleaving test harness.
- Operational metrics, CLI inspection, recovery guidance and an end-to-end
  runnable example.

## v0.10.0: External Adoption and v1.0 Readiness

**Tracking:** [External adoption and v1 readiness epic #467](https://github.com/nubster-opensources/hexeract/issues/467)

**Goal.** Make Hexeract usable by external early adopters without maintainer
hand-holding and turn the pre-stable surface into an intentional v1 contract.

**Scope:**

- Versioned documentation site and CI-verified production onboarding paths.
- Host ergonomics, configuration presets, feature bundles and actionable CLI
  diagnostics that stay inside the messaging-framework boundary.
- Full tracing and metrics coverage with versioned dashboards.
- Reproducible throughput, latency, allocation and resource baselines with
  documented regression budgets.
- Public API, feature-graph, MSRV, minimal-version, dependency, license and
  security audits.
- Supported database, broker, Rust and platform matrix.
- Wire/schema rolling-upgrade policy and final 0.x to v1 migration guide.
- Public Rust API freeze, semver/deprecation checklist and release packaging
  dry-runs.

Large convention-based features such as automatic handler construction,
cascading messages and DI-like resource resolution are not v1 blockers and
remain in the post-1.0 expansion backlog.

## v1.0.0: Stable Contract

**Tracking:** [v1.0 release gate #466](https://github.com/nubster-opensources/hexeract/issues/466)

**Goal.** Publish the already-qualified v0.10 surface as a stable contract.

No feature is added in this milestone. The release is cut only after the
documentation, performance, compatibility, migration, security and packaging
gates are complete, all earlier milestones are closed and a clean external
project has consumed the crates from crates.io.

## Post-1.0 backlog

The items below have been discussed during the design phase but are not
committed to any pre-1.0 release. They will only ship if external feedback
justifies the maintenance cost, and each requires its own design pass.

- **Additional broker transports.** NATS/JetStream, Kafka, SQS, Azure Service
  Bus and gRPC are evaluated one at a time. Hexeract will prefer capability
  traits (`Publish`, `Consume`, `RequestReply`, `TopologyAdmin`,
  `OrderedDelivery`, `ScheduledDelivery`) over forcing every broker into one
  universal topology abstraction.
- **Pluggable wire codecs.** Protobuf, Avro and schema-registry integration,
  designed alongside the first transport that requires them.
- **Per-route multi-broker selection.** Deferred until at least two production
  transport backends prove the routing contract.
- **Framework conveniences.** Automatic handler construction, cascading
  messages, convention-based routing and DI-like resources require adopter
  evidence and must remain inside the messaging boundary.
- **WASM hosts.** Run handlers compiled to WebAssembly in a sandbox for untrusted plugin scenarios.
- **Visual saga inspector.** Web UI to observe saga state transitions in real time.
- **Sustainability.** Open Core or hosted premium tier, the sponsoring page of the repository.

## How this roadmap is maintained

Changes to this document are made by pull request, with a `docs(roadmap):` Conventional Commit. The scope of any released version is locked once its tag is pushed; the scope of later releases stays adjustable until the previous release ships.

If you spot something missing, redundant or out of scope, open an issue against the relevant milestone and tag it `discussion`.
