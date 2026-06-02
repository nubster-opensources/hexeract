# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to the [versioning policy](docs/SEMVER_POLICY.md) and [MSRV policy](docs/MSRV_POLICY.md).

## [Unreleased]

### Added

- `hexeract-outbox-sql`: new outbox backend crate built on `sqlx`, with one compile-time backend per Cargo feature, `postgres` (default), `mysql` and `sqlite`. A shared `Dialect` centralizes statement templating, row locking and the per-engine schema DDL. The PostgreSQL schema is byte-for-byte identical to `hexeract-outbox-postgres`, so no data migration is required, and the payload stays native `JSONB`. (#110, #111)
- `hexeract` facade: `outbox-sql-postgres`, `outbox-sql-mysql` and `outbox-sql-sqlite` features re-export the new crate as `hexeract::outbox_sql`. (#113)
- Integration tests for the three backends, covering publish, dispatch, retry accounting, competing-consumers `FOR UPDATE SKIP LOCKED` on PostgreSQL and MySQL, and `next_retry_at` scheduling on SQLite, run by the integration workflow. (#112)
- `hexeract-core`: `HexeractError::Cancelled { type_name }` variant and the `HexeractError::cancelled` constructor for structured cancellation reporting. (#126)

### Changed

- `hexeract-bus-rabbitmq`: **breaking**. `AckMode::Auto` is replaced by two explicit modes. `AckMode::AckOnReceive` acknowledges each delivery on receive, before the handler runs (`no_ack = false`, at-most-once with prefetch back-pressure), and `AckMode::Unacknowledged` keeps the previous `no_ack = true` fire-and-forget behavior under an honest name. The old `Auto` mode silently lost messages on handler failure or crash. Migrate `AckMode::Auto` to `AckMode::Unacknowledged` for identical semantics, or to `AckMode::AckOnReceive` for explicit at-most-once. (#120)
- `hexeract-core`: **breaking**. The `HexeractError::HandlerNotFound` field `command_type` is renamed `message_type`, since it carries the type name of commands, queries and notifications alike. `HexeractError::Dispatch(String)` is now documented as a last-resort variant; prefer the structured variants. (#126)

### Deprecated

- `hexeract-outbox-postgres` and the `hexeract` `outbox-postgres` feature are deprecated since 0.4.0 and will be removed in 0.5.0. Use `hexeract-outbox-sql` with the `postgres` feature (the `outbox-sql-postgres` facade feature) instead. The deprecated crate keeps its `deadpool_postgres` implementation for this release cycle.

### Migration

The new backend is built on `sqlx` instead of `deadpool_postgres`, so the constructors take a `sqlx::PgPool` rather than a `deadpool_postgres::Pool`. The table schema is unchanged, so existing tables keep working.

Before:

```toml
hexeract = { version = "0.3", features = ["outbox-postgres"] }
```

```rust
use hexeract::outbox_postgres::{PgOutboxWorkerBuilder, ensure_schema};
```

After:

```toml
hexeract = { version = "0.4", features = ["outbox-sql-postgres"] }
```

```rust
use hexeract::outbox_sql::PgOutboxWorkerBuilder;
use hexeract::outbox_sql::postgres::ensure_schema;

let pool = sqlx::PgPool::connect(&database_url).await?;
ensure_schema(&pool, "audit_outbox").await?;
let worker = PgOutboxWorkerBuilder::new(pool)
    .register_handler::<MyEvent, _>(my_handler)
    .build()?;
```

SQLite (`outbox-sql-sqlite`) is single-worker only; for competing-consumers fan-out across many workers use PostgreSQL or MySQL. See `docs/concepts/sqlite-outbox-concurrency.md`.

## [0.3.1] - 2026-05-31

Patch release. Hardening and diagnostics across the mediator, outbox, bus and macro crates, plus a critical facade fix for the `#[handler]` macro. No breaking changes.

### Fixed

- `hexeract-macros`: the `#[handler]` macro resolves the `hexeract-core` path through `proc-macro-crate`, so handlers compile both when depending on `hexeract-core` directly and through the `hexeract` facade. The handler output type is derived from `<M as Command>::Output` / `<M as Query>::Output` instead of the type written in the `handle` signature. (#114, #115)
- `hexeract-macros`: clearer compile-time diagnostics for handler misuse. Generic handlers, an invalid `ctx` argument, a message passed by reference, a non-path message type such as a tuple, array or slice, a `&mut HandlerContext`, and lifetime-only handlers are each rejected with a spanned, actionable error instead of an obscure failure on generated code. (#123, #124)
- `hexeract-core`: dispatch returns a structured `HexeractError::DowncastFailed { expected }`, with a `downcast_failed` constructor, when a short-circuiting middleware boxes a value whose type is not the message output, instead of an opaque failure. (#125)
- `hexeract-outbox`: non-empty poll cycles are paced by a configurable `min_cycle_delay` (default 5 ms) to avoid busy-spinning when the outbox is under sustained load. (#132)
- `hexeract-bus-rabbitmq`: the consumer keeps running when an ack or nack call fails, classifying the delivery disposition instead of letting a transient broker error terminate the worker loop. (#122)

### Performance

- `hexeract-outbox-postgres`: the JSONB payload is bound directly to the query, avoiding an intermediate conversion through a string. (#134)

### Documentation

- `hexeract-outbox`: dropped the per-subject ordering guarantee that the worker could not actually enforce. (#131)
- `hexeract-bus-rabbitmq`: documented `amqps` TLS connections and credential handling. (#139)

### Internal

- The release workflow publishes `hexeract-middleware` and orders publication to respect dev-dependencies, since `hexeract-macros` dev-depends on `hexeract-mediator`.
- New `hexeract-umbrella-tests` crate (`publish = false`) exercises the `#[handler]` macro through the `hexeract` facade; its path dependency is pinned to satisfy `cargo-deny`.

## [0.3.0] - 2026-05-28

Third public release. Ships the in-process Mediator, two built-in middlewares (`TracingMiddleware`, `TimeoutMiddleware`), and the `#[handler]` attribute proc-macro with inventory-based discovery. Completes the v0.3.0 milestone (issues #6, #7, #8, #9).

### Added

- `hexeract-mediator` (new crate, first release):
  - `MediatorBuilder` fluent builder with `register_command_handler<C, H>`, `register_query_handler<Q, H>`, `register_notification_handler<N, H>`, `with_middleware<M>`, `verify_handlers()` and `build()`.
  - `Mediator` clone-cheap dispatcher (`Arc<MediatorInner>` internally) exposing `send::<C>`, `query::<Q>` and `publish::<N>`.
  - Type-erased registry: `HashMap<TypeId, Arc<dyn Erased*Handler>>` per channel, with `Typed*Handler<M, H>` adapters that downcast the payload, await the typed handler, map the handler error through `Into<HexeractError>`, and re-box the output.
  - Per-dispatch terminals (`CommandTerminal`, `QueryTerminal`, `NotificationTerminal`) with `Mutex<Option<BoxAny>>` for the move-from-`&self` problem and re-entry detection.
  - Notification fan-out semantics: shared `CorrelationId` across handlers, per-handler `MessageId`, sequential dispatch, fail-safe (every handler runs even if a previous one failed), aggregated `HexeractError::Dispatch("publish: N of M handlers failed: ...")`.
  - `MediatorBuildError::DuplicateHandler` reports the first accumulated registration conflict.
  - `MediatorBuilder::verify_handlers()` cross-checks the `inventory`-collected `HandlerRegistration` entries against the registered handlers; returns `HandlersVerificationError::Missing { missing: Vec<MissingHandler> }` listing declared-but-unregistered handlers.
- `hexeract-middleware` (new crate, first release):
  - `TracingMiddleware` opens a `tracing::Span` per dispatch (`type_name`, `message_id`, `correlation_id` recorded), emits `entering` on entry, `completed` with `elapsed_ms` on success, `failed` with `error = %err` at `Level::ERROR` on failure. Configurable level via `with_level`; defaults to `INFO`. Hierarchical span inheritance from the calling task.
  - `TimeoutMiddleware` wraps the inner pipeline in `tokio::time::timeout`, returning `HexeractError::Timeout { type_name, duration, .. }` on expiration.
- `hexeract-macros`:
  - `#[handler(command|query|notification)]` attribute macro:
    - On an inherent `impl` block: infers the message type from the `async fn handle(&self, msg: M, ctx: &HandlerContext) -> Result<T, E>` signature and generates the matching trait impl.
    - On a free `async fn`: generates a unit struct `<PascalCaseFnName>Handler` and the trait impl forwarding to the function.
    - Submits a `HandlerRegistration` entry to `inventory` for `verify_handlers()` cross-checking.
  - Comprehensive compile-fail diagnostics (8 trybuild ui snapshots): missing kind, unknown kind, trait impl, non-async, wrong arity, no Result return, notification with non-unit output.
- `hexeract-core`:
  - `HandlerKind { Command, Query, Notification }` enum.
  - `HandlerRegistration { kind, message_type_name: fn() -> &'static str, handler_type_name: fn() -> &'static str }` (fn-pointer fields so `inventory::submit!` can stay const).
  - `inventory::collect!(HandlerRegistration)` declaration.
  - `HandlerRegistration::__private` module re-exports `inventory` for macro expansion.
  - `HexeractError::Timeout` variant extended with `type_name: &'static str` and `duration: Duration` fields; marked `#[non_exhaustive]` at the variant level.
  - `HexeractError::timeout(type_name, duration)` public constructor (required to build the `Timeout` variant from outside the crate).
- `hexeract` umbrella crate:
  - New feature flag `middleware = ["core", "dep:hexeract-middleware"]` re-exporting `hexeract_middleware` as `hexeract::middleware`.
  - `mediator` and `macros` features now expose the full crates instead of placeholders.

### Changed

- `HexeractError::Timeout`: was `{ elapsed: Duration }`, now `{ type_name, duration, .. }`. Breaking for callers that pattern-matched the variant without `..`. Pre-1.0 minor-version bump.
- `MediatorBuilder` now maintains three parallel `HashSet<&'static str>` of registered message type names (one per channel) to power `verify_handlers()` against `inventory` string-keyed metadata.

### Documentation

- 16 new documentation pages under `docs/`: a Mediator getting-started, three concepts pages (CQRS semantics, middleware pipeline, `#[handler]`), one architecture flow, three crate references (`hexeract-mediator`, `hexeract-middleware`, `hexeract-macros`), two migration guides (from MediatR, from Wolverine), five cookbook recipes (wire tracing+timeout, outbox+mediator, handler with state, notification fan-out, sanity-check handlers), plus an updated `docs/index.md`.
- README extended with a `Mediator (in-process)` quick start; status badge raised from `pre-alpha` to `alpha`.
- Crate-level rustdocs cleaned of obsolete `placeholder` mentions inherited from earlier v0.x phases.
- `docs/architecture/overview.md` mermaid graph and crate roles table now reflect the v0.3.0 lineup (`hexeract-mediator`, `hexeract-middleware`, `hexeract-macros` all `Stable`).

### Internal

- New workspace dependencies: `inventory = "0.3"`, `tracing-test = "0.2"`, `trybuild = "1"`.
- Workspace package version bumped from `0.2.0` to `0.3.0`; the `hexeract-middleware` and `hexeract` crates that previously held explicit overrides now inherit from `workspace.package.version`.
- All inter-crate dependency requirements normalized to `version = "0.3"` (was a mix of `"0.2"` and `"0.2.0"`).

## [0.2.0] - 2026-05-26

Second public release. Ships the Bus feature end to end against RabbitMQ via `lapin`, alongside topology types, a typed consumer worker with ack modes and retry policy, an end-to-end pub/sub example and a `hexeract bus` CLI namespace.

### Added

- `hexeract-bus`:
  - `Message` marker trait with an associated `MESSAGE_TYPE: &'static str` for stable routing.
  - `BusEnvelope` wire representation with a custom `Debug` impl that masks the payload bytes to avoid leaking message content into traces. Includes a `restore` constructor for backend implementations.
  - `BusError` enum with variants `Serialization`, `Transport`, `Connection`, `MissingHandler`, `TypeMismatch`, `InvalidTopology` and `Internal`.
  - `Transport` async trait (via `async_trait`) with `publish` and `publish_with_headers` methods generic over `M: Message`, returning the freshly minted `message_id`.
  - `Handler<M: Message>` async trait decorated with `#[trait_variant::make(Send)]` and a handler-defined `Error: Into<BusError>`, symmetric with `hexeract_outbox::Handler<E>`.
  - `ErasedHandler` trait and `TypedHandler<M, H>` adapter for runtime dispatch by `MESSAGE_TYPE`.
  - Topology types `Exchange`, `ExchangeKind` (`Direct`, `Topic`, `Fanout`, `Headers`), `Queue`, `RoutingKey` newtype, `Binding`. Each is validated on construction (`<= 127 byte names`, `<= 255 byte routing keys`, no ASCII control characters) and `Serialize` + `Deserialize` round-trips re-run the validation through `try_from`.
- `hexeract-bus-rabbitmq`:
  - `RabbitMqConnection` wrapper over `lapin::Connection` with single-shot `connect` and bounded exponential-backoff `connect_with_retry`.
  - `ChannelPool` per-publisher pool of `lapin::Channel` handles with `PooledChannel` RAII guard that returns the channel to the pool on drop, on a best-effort basis.
  - `RabbitMqTransport` implementing `Transport`. `new(uri)` targets the AMQP default exchange; `with_exchange(uri, exchange)` declares and uses a typed `Exchange`. Publishes carry the `BusEnvelope` as JSON with AMQP properties (`message_id`, `correlation_id`, `content_type`, `type`, optional `reply_to`, free-form headers).
  - Topology declaration helpers (`declare_exchange`, `declare_queue`, `bind_queue`, `ensure_topology`) for development convenience; long-running services should declare topology at startup, not on the publish hot path.
  - `RabbitMqWorker` consumer worker with `RabbitMqWorkerBuilder` fluent API mirroring `PgOutboxWorkerBuilder`. Supports `AckMode::Auto` and `AckMode::Manual`; manual mode applies `basic_nack(requeue=true)` up to `max_attempts` (counter keyed on `message_id` to survive redeliveries) before publishing to the configured dead-letter routing key or dropping. Graceful shutdown via `CancellationToken`.
  - End-to-end runnable example `examples/03_bus_pubsub.rs` spinning up a RabbitMQ container via `testcontainers`, declaring topology, spawning the worker, publishing five messages and asserting consumption under one second.
- `hexeract-cli`:
  - `bus` subcommand namespace: `hexeract bus declare --conn URL --topology FILE` applies a TOML topology (validated through the bus constructors); `hexeract bus peek --conn URL --queue NAME [--count N]` dumps the first `N` messages of a queue non-destructively (each delivery is `basic_nack(requeue=true)`-ed after print); `hexeract bus purge --conn URL --queue NAME --yes-i-know` drops every message from a queue, gated by the same safety flag as `outbox apply`.
  - Sample topology file at `crates/hexeract-cli/examples/topology.toml`.

### Documentation

- README extended with a `Bus (RabbitMQ)` quick start covering the SDK snippet, the three CLI subcommands and a production note about declaring topology at startup.
- ROADMAP marks v0.2.0 as delivered.

### Notes for upgraders

This is a non-breaking addition for projects already on v0.1.0: `hexeract-outbox`, `hexeract-outbox-postgres` and the existing `hexeract outbox` CLI keep their v0.1.0 surface. Adding the bus is a matter of pulling the two new crates and the new CLI namespace.

## [0.1.0] - 2026-05-24

First public release. Ships the transactional outbox feature end to end against PostgreSQL via `deadpool_postgres`.

### Added

- `hexeract-outbox`:
  - `Event` marker trait with an associated `EVENT_TYPE: &'static str` for stable routing.
  - `OutboxEnvelope` row representation with a custom `Debug` impl that masks the payload bytes to avoid leaking event content into traces. Includes a `restore` constructor for backend implementations.
  - `OutboxError` enum with variants `Serialization`, `Database` (boxed for backend-agnostic source), `MissingHandler`, `MaxRetries`, `TypeMismatch` and `Internal`.
  - `Handler<E: Event>` async trait dispatched by the worker, decorated with `#[trait_variant::make(Send)]` and a handler-defined `Error: Into<OutboxError>`.
  - `OutboxPublisher` async trait with a generic associated transaction handle (`type Tx<'tx>: Send`) and three methods: `publish_in_tx`, `publish_in_tx_with_subject`, `publish`.
  - `OutboxStore` async trait (via `async_trait` to work around `rust-lang/rust#100013`) with `Client` and `Tx<'tx>` associated types and methods `acquire`, `begin`, `poll`, `mark_delivered`, `mark_failed`, `commit`.
  - `ErasedHandler` trait and `TypedHandler<E, H>` adapter for runtime dispatch by `EVENT_TYPE`.
  - `OutboxWorkerConfig` with defaults `poll_interval = 100 ms`, `batch_size = 10`, `max_attempts = 5`, `retry_delay = 5 s`.
  - `OutboxWorker<S>` with `run(cancel)` returning a boxed `Send` future the caller spawns.
- `hexeract-outbox-postgres`:
  - `POSTGRES_SCHEMA_SQL` constant with templated `{{table}}` placeholder.
  - `render_schema(table_name)` helper and `ensure_schema(pool, table_name)` for POC and integration tests.
  - Strict `validate_table_name` rejecting SQL injection attempts.
  - `PgOutboxPublisher` implementing `OutboxPublisher` with `Tx<'tx> = deadpool_postgres::Transaction<'tx>`.
  - `PgOutboxStore` implementing `OutboxStore` with poll-and-update SQL using `SELECT ... FOR UPDATE SKIP LOCKED` for safe multi-worker concurrency.
  - `PgOutboxWorkerBuilder` fluent API: `new(pool).table_name(...).register_handler::<E, _>(...).poll_interval(...).build()?`.
  - `DEFAULT_TABLE_NAME = "audit_outbox"` constant.
  - End-to-end example `examples/02_outbox_two_databases.rs` demonstrating the full flow against two PostgreSQL containers.
- `hexeract-cli`:
  - Binary `hexeract` with the `outbox` subcommand namespace.
  - `hexeract outbox patch --table NAME`: prints the canonical schema SQL templated with the given table name to stdout.
  - `hexeract outbox apply --conn URL --table NAME`: applies the schema to a target PostgreSQL database (with production warning and `--yes-i-know` flag).
  - `hexeract outbox check --conn URL --table NAME`: validates that the target table exists with the expected columns and indexes.

### Documentation

- README with vision, six features and anti-scope.
- CONTRIBUTING with trunk-based development conventions.
- SECURITY policy and Code of Conduct (Contributor Covenant 2.1).
- MSRV and semver policies.
- docs/tutorial/getting-started, docs/outbox-architecture and docs/outbox-postgres-schema.

### Notes for upgraders

This is the first published version, so no upgrade path applies.

[Unreleased]: https://github.com/nubster-opensources/hexeract/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/nubster-opensources/hexeract/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/nubster-opensources/hexeract/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nubster-opensources/hexeract/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nubster-opensources/hexeract/releases/tag/v0.1.0
