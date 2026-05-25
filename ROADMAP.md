# Roadmap

Hexeract is pre-stable. This document captures the intended trajectory of
the project up to v1.0, ordered by release. **No dates are committed.** The
project is sponsored on a best-effort basis by Encelade Technologies, and
releases ship when they are ready, not when a calendar says so.

The roadmap mirrors the GitHub milestones one-for-one. Each section here is
the public, prose form of a milestone; each milestone groups the issues that
must close before the release ships. The full design notes for any given
release live under `docs/design/`.

## Out of scope

Hexeract is a Wolverine-style in-process messaging framework for Rust. The
following will never be in scope, regardless of demand:

- **Service mesh.** No sidecar proxies, no traffic shaping, no L7 routing.
- **Brokered queue replacement.** Hexeract integrates with brokers; it does
  not aim to replace Kafka, NATS, RabbitMQ or their managed equivalents.
- **Saga choreography engine without an outbox.** Distributed coordination
  in Hexeract is grounded in the transactional outbox pattern. Best-effort
  choreography without a durable boundary will not be added.
- **General-purpose application framework.** Hexeract focuses on messaging,
  handlers and transactional dispatch. It is not an HTTP framework, an ORM
  or a dependency-injection container.

These boundaries are deliberate and non-negotiable. If a feature request
crosses one of them, it belongs in another project.

## v0.1.0: Transactional outbox foundation

**Goal.** A Rust service writes a domain event inside the same database
transaction as its business state, and a background worker delivers that
event reliably with retry and back-off.

**Crates.**
- `hexeract-outbox`: backend-agnostic core (`Event`, `OutboxEnvelope`,
  `OutboxPublisher`, `OutboxStore`, `Handler`, `OutboxWorker`,
  `OutboxError`).
- `hexeract-outbox-postgres`: PostgreSQL backend over `deadpool_postgres`,
  with `SELECT ... FOR UPDATE SKIP LOCKED` for safe multi-worker polling.
- `hexeract-cli`: `hexeract outbox patch | apply | check` subcommands to
  emit, install and verify the canonical schema.

**Documentation.**
- README, CONTRIBUTING, SECURITY, Code of Conduct.
- MSRV and semver policies.
- `docs/getting-started`, `docs/outbox-architecture`,
  `docs/outbox-postgres-schema`.

## v0.2.0: Handler ergonomics and observability

**Goal.** Make the day-to-day handler experience more pleasant and make the
outbox observable end to end.

**Scope.**
- Structured tracing spans on publish, poll, dispatch and ack paths
  (correlation id, event type, attempt counter).
- Metrics surface (counts, lag, retries) exposed via a feature-gated
  adapter.
- Handler middleware pipeline for cross-cutting concerns (retries policy,
  idempotency keys).

Details: **TBD** until v0.1.0 feedback lands.

## v0.3.0: Additional backends

**Goal.** Broaden backend coverage beyond PostgreSQL.

**Scope.**
- Additional outbox backend, candidate: **TBD** (SQLite for embedded,
  MySQL or SQL Server for enterprise compatibility — to be decided based
  on demand).
- Backend compatibility test suite shared across implementations.

Details: **TBD**.

## v0.4.0: Messaging beyond the outbox

**Goal.** Expand from the transactional outbox into the broader Wolverine
feature set, while preserving the in-process focus.

**Scope.**
- In-memory mediator for command and query dispatch within a process.
- Local scheduling primitives (delayed and recurring messages) grounded in
  the outbox for durability.

Details: **TBD**.

## v0.5.0: Polish and stability

**Goal.** Hexeract is usable by external early adopters without hand
holding, the public API is frozen, and the documentation lives somewhere
permanent.

**Documentation.**
- Dedicated documentation site or a section on the Encelade docs portal.
- Onboarding tutorials per primary use case (web service, worker, CLI).

**Testing.**
- Integration tests built on `testcontainers` for every supported backend.
- Cross-OS smoke tests in CI (Linux first, macOS next, Windows last).

**API.**
- Public Rust API surface frozen.
- Migration plan towards v1.0 documented.

## Post-1.0 backlog

The following items are explicitly post-1.0 and will not be picked up
before the public API freezes:

- Broker-backed transports (Kafka, NATS, RabbitMQ) as optional adapters
  that respect the in-process focus.
- Distributed sagas with explicit compensation steps, layered on top of
  the outbox.
- Multi-tenant routing and per-tenant retry policies.
- Schema registry integration for forward and backward compatibility of
  event payloads.
- Web UI for inspecting outbox lag and replaying failed events.

## How this roadmap is maintained

Changes to this document are made by pull request, with a
`docs(roadmap):` Conventional Commit. The scope of v0.1.0 is locked; the
scope of later releases stays adjustable until the previous release ships.

If you spot something missing, redundant or out of scope, open an issue
against the relevant milestone and tag it `discussion`.
