# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to the [versioning policy](docs/SEMVER_POLICY.md) and [MSRV policy](docs/MSRV_POLICY.md).

## [Unreleased]

### Added
- _Items in flight will be listed here until the next release._

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
- docs/getting-started, docs/outbox-architecture and docs/outbox-postgres-schema.

### Notes for upgraders

This is the first published version, so no upgrade path applies.

[Unreleased]: https://github.com/nubster-opensources/hexeract/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nubster-opensources/hexeract/releases/tag/v0.1.0
