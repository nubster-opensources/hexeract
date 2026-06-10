# `hexeract-outbox-sql` API reference

Multi-database outbox backend built on `sqlx`. Implements the backend-agnostic [`OutboxPublisher`](hexeract-outbox.md) and [`OutboxStore`](hexeract-outbox.md) contracts, with one compile-time backend per Cargo feature. This is the recommended outbox backend; it supersedes the deprecated [`hexeract-outbox-postgres`](hexeract-outbox-postgres.md).

The full rustdoc lives at <https://docs.rs/hexeract-outbox-sql>.

## Backends and features

At least one backend feature must be enabled (a compile error fires otherwise).

| Feature | Pool | Types |
| --- | --- | --- |
| `postgres` (default) | `sqlx::PgPool` | `PgOutboxPublisher`, `PgOutboxStore`, `PgOutboxWorkerBuilder` |
| `mysql` | `sqlx::MySqlPool` | `MySqlOutboxPublisher`, `MySqlOutboxStore`, `MySqlOutboxWorkerBuilder` |
| `sqlite` | `sqlx::SqlitePool` | `SqliteOutboxPublisher`, `SqliteOutboxStore`, `SqliteOutboxWorkerBuilder` |

Through the `hexeract` umbrella, these map to the `outbox-sql-postgres`, `outbox-sql-mysql` and `outbox-sql-sqlite` features, re-exported as `hexeract::outbox_sql`.

The PostgreSQL schema is byte-for-byte identical to `hexeract-outbox-postgres`, so moving from the deprecated crate requires no data migration. MySQL requires **8.0.13 or later** (the schema defaults `created_at` to the `(UTC_TIMESTAMP(6))` expression). SQLite is single-writer: run exactly one worker per database. See [SQLite outbox concurrency](../concepts/sqlite-outbox-concurrency.md).

## Public surface

The three backends expose the same surface; the items below use the PostgreSQL names.

### Dialect

| Item | Role |
| --- | --- |
| `Dialect::{Postgres, MySql, Sqlite}` | Marker for the target engine. `#[non_exhaustive]`, so external `match` arms need a wildcard `_`. |
| `Dialect::schema_ddl(table)` | Render the canonical outbox schema (table + indexes) for this engine. Validates the table name. |
| `Dialect::dead_letter_schema_ddl(table)` | Render the `{table}_dead_letter` companion schema. |
| `Dialect::supports_skip_locked()` | `true` for PostgreSQL and MySQL, `false` for SQLite. |

### Schema helpers

| Item | Role |
| --- | --- |
| `ensure_schema(pool, table_name)` | Apply the rendered schema to the target database. Lives in each backend module (`postgres::ensure_schema`, etc.). POC / dev only; strict table-name validation. |
| `DEFAULT_TABLE_NAME = "audit_outbox"` | Default table name when a builder does not override it. |

Production deployments should run their own migration tooling against `Dialect::schema_ddl` rather than applying DDL from the running service.

### Publisher

| Item | Role |
| --- | --- |
| `PgOutboxPublisher::new(pool, table_name)` | Construct a publisher. Validates the table name. |
| `publish_in_tx(&mut tx, &event)` | Enrol the outbox row in the caller's `sqlx` transaction. Mints and returns a UUIDv7. |
| `publish_in_tx_with_subject(&mut tx, subject_id, &event)` | Same, recording an aggregate `subject_id` for partial ordering. |
| `publish(&event)` | Convenience: open a transaction, publish and commit in one call. |
| `pool()` / `table_name()` | Accessors. |

### Store

| Item | Role |
| --- | --- |
| `PgOutboxStore::new(pool, table_name)` | Construct a store. Caches the templated SQL. Validates the table name. |
| `with_dead_letter(dlq_table)` | Enable dead-letter persistence: exhausted envelopes are moved to `dlq_table` (INSERT + DELETE in one transaction). |
| Implements `OutboxStore` | `poll` runs `SELECT ... [FOR UPDATE SKIP LOCKED]` per dialect; `mark_delivered`, `mark_failed`, `claim` and `mark_dead_lettered` settle envelopes. |

### Builder

| Item | Role |
| --- | --- |
| `PgOutboxWorkerBuilder::new(pool)` | Fluent entry point. |
| `.table_name(name)` | Override the default table. |
| `.dead_letter_table(name)` | Move poison envelopes to this table once they exhaust `max_attempts`. |
| `.register_handler::<E, _>(handler)` | Register a typed handler per `EVENT_TYPE`. Repeated registration replaces silently. |
| `.shared_handler::<E, _>(Arc<H>)` | Register a handler already shared behind an `Arc`. |
| `.poll_interval(d)` | Sleep between empty polls (default 100 ms). |
| `.batch_size(n)` | Rows per poll (default 10). |
| `.max_attempts(n)` | Excludes a row from polling once it reaches this value (default 5). |
| `.retry_base_delay(d)` | Base delay for exponential backoff (default 1 s). |
| `.retry_max_delay(d)` | Cap on the backoff delay (default 5 min). |
| `.jitter(enabled)` | Full jitter on the backoff delay (default `true`). |
| `.dispatch_timeout(d)` | Soft-lease duration for claimed envelopes (default 30 s). |
| `.build()?` | Returns `OutboxWorker<PgOutboxStore>`. Validates the table name. |

## Where to read next

- [Outbox quick start](../getting-started/outbox-quick-start.md)
- [Outbox flow architecture](../architecture/outbox-flow.md)
- [Outbox PostgreSQL schema](outbox-postgres-schema.md)
- [SQLite outbox concurrency](../concepts/sqlite-outbox-concurrency.md)
