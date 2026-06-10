# `hexeract-outbox-postgres` API reference

> **Deprecated since 0.4.0, scheduled for removal in 0.5.0.** Use [`hexeract-outbox-sql`](hexeract-outbox-sql.md) with the `postgres` feature instead (the `outbox-sql-postgres` facade feature). The PostgreSQL schema is byte-for-byte identical, so no data migration is required; the constructors take a `sqlx::PgPool` rather than a `deadpool_postgres::Pool`. See the migration steps in the [CHANGELOG](../../CHANGELOG.md) under `[0.4.0]`.

PostgreSQL backend for the outbox, powered by `deadpool_postgres`. Implements [`OutboxPublisher`](hexeract-outbox.md) and [`OutboxStore`](hexeract-outbox.md) plus a fluent worker builder.

The full rustdoc lives at <https://docs.rs/hexeract-outbox-postgres>.

## Public surface

### Schema strategy

| Item | Role |
| --- | --- |
| `POSTGRES_SCHEMA_SQL` | Canonical schema with templated `{{table}}` placeholder. See [outbox PostgreSQL schema](outbox-postgres-schema.md) for the full SQL. |
| `render_schema(table_name)` | Substitute the placeholder. Returns a `String` ready to feed to `tokio_postgres::Client::batch_execute`. |
| `ensure_schema(pool, table_name)` | Apply the schema to the target database. POC / dev only. Strict validation rejects SQL injection attempts in `table_name`. |
| `validate_table_name(name)` | Public helper rejecting anything not matching `^[a-zA-Z_][a-zA-Z0-9_]*$`. |
| `DEFAULT_TABLE_NAME = "audit_outbox"` | Default table name picked by builders that do not override it. |

### Publisher

| Item | Role |
| --- | --- |
| `PgOutboxPublisher::new(pool, table_name)` | Construct a publisher. Validates the table name. |
| Implements `OutboxPublisher` with `Tx<'tx> = deadpool_postgres::Transaction<'tx>`. | The caller's business transaction is reused, so the outbox row enrols in the same unit of work. |

### Store

| Item | Role |
| --- | --- |
| `PgOutboxStore::new(pool, table_name)` | Construct a store. Validates the table name. |
| Implements `OutboxStore` | `acquire` returns a `deadpool_postgres::Object`, `begin` opens a transaction, `poll` runs `SELECT ... FOR UPDATE SKIP LOCKED` for safe multi-worker concurrency. |

### Builder

| Item | Role |
| --- | --- |
| `PgOutboxWorkerBuilder::new(pool)` | Fluent entry point. |
| `.table_name(name)` | Override the default table. |
| `.register_handler::<E, _>(handler)` | Register a typed handler per `EVENT_TYPE`. Repeated registration replaces silently. |
| `.shared_handler::<E, _>(Arc<H>)` | Register a handler that is already shared (`Arc<H>`). |
| `.poll_interval(d)` | Sleep between empty polls (default 100 ms). |
| `.batch_size(n)` | Rows per poll (default 10). |
| `.max_attempts(n)` | Excludes a row from polling once it reaches this value (default 5). |
| `.retry_delay(d)` | Constant cooldown between failed attempts (default 5 s). |
| `.build()?` | Returns `OutboxWorker<PgOutboxStore>`. Validates the table name. |

## Where to read next

- [Outbox quick start](../getting-started/outbox-quick-start.md)
- [Outbox flow architecture](../architecture/outbox-flow.md)
- [Outbox PostgreSQL schema](outbox-postgres-schema.md)
