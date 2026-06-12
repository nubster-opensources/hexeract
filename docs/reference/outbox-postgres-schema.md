# PostgreSQL outbox schema

The Hexeract PostgreSQL backend operates on a single table whose schema is fully owned by the application (Bring-Your-Own-Schema). This document describes the canonical schema, how to derive it for your migration tooling, and the assumptions the worker relies on.

## Canonical SQL

Generate the canonical SQL with the `hexeract` CLI:

```sh
hexeract outbox patch --table audit_outbox
```

You can also generate it programmatically via `Dialect::schema_ddl`:

```rust
use hexeract_outbox_sql::Dialect;

let sql = Dialect::Postgres.schema_ddl("audit_outbox")?;
```

The rendered SQL is:

```sql
CREATE TABLE IF NOT EXISTS audit_outbox (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      UUID         NOT NULL UNIQUE,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    subject_id    UUID         NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempts      INTEGER      NOT NULL DEFAULT 0,
    last_error    TEXT         NULL,
    next_retry_at TIMESTAMPTZ  NULL,
    delivered_at  TIMESTAMPTZ  NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_outbox_pending
    ON audit_outbox (created_at)
    WHERE delivered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_audit_outbox_subject
    ON audit_outbox (subject_id, id)
    WHERE subject_id IS NOT NULL;
```

The table name is templated. Replace `audit_outbox` with whatever name you pass via `--table`. Hexeract enforces `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent SQL injection.

## Column reference

| Column | Type | Nullable | Purpose |
|---|---|---|---|
| `id` | `BIGSERIAL` | NO | Monotonic insertion order. The worker polls `ORDER BY id` to guarantee insertion-order delivery. |
| `event_id` | `UUID` | NO (UNIQUE) | Stable event identifier minted by the publisher (UUIDv7). The UNIQUE constraint makes accidental duplicate inserts fail loudly. |
| `event_type` | `VARCHAR(64)` | NO | Routing key matching `Event::EVENT_TYPE`. |
| `payload` | `JSONB` | NO | JSON-serialised event body. JSONB supports indexing and ad-hoc querying for ops. |
| `subject_id` | `UUID` | YES | Optional aggregate identifier used for partial ordering. |
| `created_at` | `TIMESTAMPTZ` | NO | Insertion timestamp. Set by the database default. |
| `attempts` | `INTEGER` | NO | Number of dispatch attempts already consumed. Default `0`. |
| `last_error` | `TEXT` | YES | Error message from the last failed dispatch. Cleared on success implicitly because the row leaves the pending set. |
| `next_retry_at` | `TIMESTAMPTZ` | YES | Earliest instant at which the worker will retry. Set on failure to `NOW() + retry_delay`. |
| `delivered_at` | `TIMESTAMPTZ` | YES | Marker of successful dispatch. The worker filters on `IS NULL` to find pending rows. |

## Indexes

Both indexes are **partial** so they only cover the working set:

- `idx_<table>_pending` accelerates the worker's poll (`WHERE delivered_at IS NULL`) by scanning insertion order without touching delivered rows.
- `idx_<table>_subject` supports partial ordering lookups (`WHERE subject_id IS NOT NULL`).

Once a row's `delivered_at` is set, it leaves both partial indexes and stops contributing to scan cost.

## Migration tooling

Hexeract does **not** ship a migration runner. Pipe the canonical SQL into the tooling you already use:

### sqlx-cli

```sh
hexeract outbox patch --table audit_outbox > migrations/0042_outbox.sql
sqlx migrate run
```

### refinery

```sh
hexeract outbox patch --table audit_outbox > migrations/V0042__outbox.sql
refinery migrate -e DATABASE_URL -p migrations
```

### dbmate

```sh
dbmate new outbox
# paste the canonical SQL into db/migrations/<timestamp>_outbox.sql
dbmate up
```

### Flyway

```sh
hexeract outbox patch --table audit_outbox > migrations/V20260601__outbox.sql
flyway migrate
```

### POC / development helper

For local POCs and integration tests, `hexeract outbox apply` runs the DDL directly. It requires `--yes-i-know` to prevent accidental production runs:

```sh
hexeract outbox apply --conn "$DATABASE_URL" --table audit_outbox --yes-i-know
```

Production deployments should never use `apply`; the runtime database role typically should not own the privileges required to run DDL.

## Verifying the schema is correct

`hexeract outbox check --conn "$DATABASE_URL" --table audit_outbox` queries `information_schema.columns` and reports any missing column. Exit code 0 means the table is valid; 1 means at least one expected column is missing and the message lists which.

## Operational queries

### Pending rows

```sql
SELECT id, event_type, attempts, last_error
FROM audit_outbox
WHERE delivered_at IS NULL
ORDER BY id;
```

### Stuck rows (past `max_attempts`)

```sql
SELECT id, event_type, attempts, last_error, created_at
FROM audit_outbox
WHERE delivered_at IS NULL
  AND attempts >= 5  -- match your OutboxWorkerConfig::max_attempts
ORDER BY id;
```

### Throughput last hour

```sql
SELECT
    date_trunc('minute', delivered_at) AS minute,
    COUNT(*) AS delivered
FROM audit_outbox
WHERE delivered_at >= NOW() - INTERVAL '1 hour'
GROUP BY 1
ORDER BY 1;
```

### Replay a stuck row

If you have fixed the root cause and want to retry a row that exhausted its budget, reset `attempts`:

```sql
UPDATE audit_outbox
SET attempts = 0, last_error = NULL, next_retry_at = NULL
WHERE event_id = '<uuid>';
```

The next poll will pick it up.

## Schema evolution

- **Multi-database (shipped in v0.4)**: the `hexeract-outbox-sql` crate renders this same schema for PostgreSQL plus equivalent canonical schemas for MySQL and SQLite, through `Dialect::schema_ddl`. The PostgreSQL schema is byte-for-byte identical to the one above, so no data migration is required when moving from `hexeract-outbox-postgres` to `hexeract-outbox-sql`.
- **Dead-letter table (shipped in v0.4)**: each backend also renders a `{table}_dead_letter` companion schema via `Dialect::dead_letter_schema_ddl`. Exhausted envelopes are moved there instead of being left in place.
- **Partitioning**: for high-volume deployments, the table can be partitioned by `created_at` or by `event_type` ranges without changes to the publisher or worker.
