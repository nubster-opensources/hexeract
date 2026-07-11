# `hexeract-scheduler-sql` API reference

Multi-database schedule store built on `sqlx`. Implements the backend-agnostic [`ScheduleStore`](hexeract-scheduler.md) and [`ScheduleAdmin`](hexeract-scheduler.md) contracts, with one compile-time backend per Cargo feature. It reuses the [`Dialect`](hexeract-outbox-sql.md) of `hexeract-outbox-sql` for injection-safe quoting and database-clock lease anchoring, and adds the scheduler's own `scheduled_messages` schema (`scheduled_for`, an optional cron expression, a dispatch target, lease columns and a paused flag), which differs from the outbox table.

The full rustdoc lives at <https://docs.rs/hexeract-scheduler-sql>.

## Backends and features

At least one backend feature must be enabled (a compile error fires otherwise).

| Feature | Pool | Type |
| --- | --- | --- |
| `postgres` (default) | `sqlx::PgPool` | `PgScheduleStore` |
| `mysql` | `sqlx::MySqlPool` | `MySqlScheduleStore` |
| `sqlite` | `sqlx::SqlitePool` | `SqliteScheduleStore` |

Through the `hexeract` umbrella, these map to the `scheduler-sql-postgres`, `scheduler-sql-mysql` and `scheduler-sql-sqlite` features, re-exported as `hexeract::scheduler_sql`.

PostgreSQL and SQLite index the pending set with a partial index that excludes delivered, dead-lettered, cancelled and paused rows; MySQL uses a plain index since it has no partial indexes. MySQL requires **8.0.13 or later** (the schema defaults `created_at` to the `(UTC_TIMESTAMP(6))` expression). SQLite is single-writer: run exactly one worker per database. PostgreSQL and MySQL claim due occurrences with `FOR UPDATE SKIP LOCKED`, so they support competing consumers; SQLite does not.

## Public surface

The three backends expose the same surface; the items below use the PostgreSQL names.

### Dialect

| Item | Role |
| --- | --- |
| `Dialect` | Re-exported from [`hexeract_outbox_sql::Dialect`](hexeract-outbox-sql.md). Marker for the target engine, `#[non_exhaustive]`, so external `match` arms need a wildcard `_`. |

### Schema

```rust
pub fn schema_ddl(dialect: Dialect, table: &str) -> Result<String, SchedulerError>;
```

`schema_ddl` renders the canonical `scheduled_messages` table and its indexes for the given dialect, substituting `table` and validating it as an identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`. It lives in the `schema` module. There is no separate schema reference page by design: the canonical DDL is obtained from `hexeract scheduler schema --dialect <postgres|my-sql|sqlite>` (see [`cli.md`](cli.md)), which calls this same function, so the CLI output is always the single source of truth for the table shape.

| Item | Role |
| --- | --- |
| `DEFAULT_TABLE_NAME = "scheduled_messages"` | Default table name when a store is not given an explicit one. |

Production deployments should run their own migration tooling against `schema_ddl` rather than applying DDL from the running service; the stores themselves do not require an `ensure_schema` step at runtime.

### Store

```rust
impl PgScheduleStore {
    pub fn new(pool: PgPool, table_name: impl Into<String>) -> Result<Self, SchedulerError>;
    pub fn pool(&self) -> &PgPool;
    pub fn table_name(&self) -> &str;
}
```

`new` templates and caches every SQL statement at construction so each poll cycle reuses the same strings, and validates `table_name` against the same identifier pattern as `schema_ddl`, returning `SchedulerError::Internal` on a mismatch. `MySqlScheduleStore::new(pool: MySqlPool, table_name: impl Into<String>) -> Result<Self, SchedulerError>` and `SqliteScheduleStore::new(pool: SqlitePool, table_name: impl Into<String>) -> Result<Self, SchedulerError>` share this shape, with `pool()` and `table_name()` accessors of the matching type. Every store is `Clone`: the pool and the cached SQL strings are reference-counted.

| Item | Role |
| --- | --- |
| Implements `ScheduleStore` | `claim_due` atomically selects due, unleased, eligible occurrences, advances the attempt counter and stamps a fresh lease. On PostgreSQL and SQLite it is a single `UPDATE ... RETURNING` driven by a `FOR UPDATE SKIP LOCKED` CTE (SQLite renders the lease through a `strftime` modifier instead of `SKIP LOCKED`, since it has none). MySQL supports neither `UPDATE ... RETURNING` nor a `FOR UPDATE SKIP LOCKED` that also returns the updated rows, so its claim runs as a short internal transaction: select and lock the due rows, lease them and consume one attempt, then reselect the leased rows. `insert`, `mark_delivered`, `reschedule`, `mark_failed`, `mark_dead_lettered`, `cancel`, `set_paused` and `resume` settle a schedule by id. |
| Implements `ScheduleAdmin` | `list_pending` and `list_dead_letter` page the non-terminal and dead-lettered sets; `replay` resets attempts and reschedules now, returning `SchedulerError::NotReplayable` when the schedule is not eligible. |

## Where to read next

- [Scheduler quick start](../getting-started/scheduler-quick-start.md)
- [`hexeract` CLI reference](cli.md)
