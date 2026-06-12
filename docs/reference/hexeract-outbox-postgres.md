# `hexeract-outbox-postgres` API reference

> **Removed in 0.5.0.** This crate was deprecated in 0.4.0 and is no longer published. Use [`hexeract-outbox-sql`](hexeract-outbox-sql.md) with the `postgres` feature flag instead.
>
> **Migration summary:**
> - Replace `deadpool_postgres::Pool` with `sqlx::PgPool` (built via `sqlx::postgres::PgPoolOptions` or `PgPool::connect`).
> - Replace `hexeract-outbox-postgres` (or the `outbox-postgres` facade feature) with `hexeract-outbox-sql` and the feature `postgres` (or the `outbox-sql-postgres` facade feature).
> - The PostgreSQL schema is byte-for-byte identical; no data migration is required on existing tables.
> - `POSTGRES_SCHEMA_SQL` and `render_schema` are replaced by `Dialect::Postgres.schema_ddl(table)?`, which returns the same DDL as a `String`. Feed it into your migration tooling.
> - `ensure_schema(&pool, table)` still exists on `hexeract_outbox_sql::postgres` as a dev/test helper; do not call it at production startup.
> - See the full step-by-step instructions in the [migration guide v0.3 to v0.4](../operations/migration-v0.3-v0.4.md).

For the current API reference, see [`hexeract-outbox-sql`](hexeract-outbox-sql.md).
