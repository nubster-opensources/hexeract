# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to the [versioning policy](docs/SEMVER_POLICY.md) and [MSRV policy](docs/MSRV_POLICY.md).

## [Unreleased]

### Added
- _Items in flight will be listed here until the next release._

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

[Unreleased]: https://github.com/nubster-opensources/hexeract/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/nubster-opensources/hexeract/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nubster-opensources/hexeract/releases/tag/v0.1.0
