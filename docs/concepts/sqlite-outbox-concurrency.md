# SQLite outbox concurrency

The `hexeract-outbox-sql` crate ships three backends behind Cargo features:
`postgres`, `mysql` and `sqlite`. They share the same `OutboxStore` and
`OutboxWorker` contracts, but SQLite has a different concurrency model that
shapes how you deploy its worker.

## Competing consumers on PostgreSQL and MySQL

The outbox worker relies on `SELECT ... FOR UPDATE SKIP LOCKED` to let several
workers poll the same table at once. Each worker locks the rows it claims and
skips rows already locked by another worker, so a backlog is drained in parallel
without any envelope being dispatched twice. PostgreSQL and MySQL 8.0+ both
support this clause, so they support competing consumers out of the box: run as
many workers as you need.

## SQLite is single-writer

SQLite has no row-level `SKIP LOCKED`. It serializes writes through a single
writer and exposes no way for one poller to claim a subset of pending rows while
another poller claims a different subset. If two workers poll the same SQLite
database, both can read the same pending rows before either marks them
delivered, and the same envelope is dispatched more than once.

Because of this, the SQLite backend assumes **one `OutboxWorker` per database**.
This matches how SQLite is normally used: a single process, embedded or in
tests, rather than a shared server fronting many workers.

- Run exactly one `SqliteOutboxWorkerBuilder::build()` worker against a given
  SQLite database.
- Configure `busy_timeout` on the pool so concurrent writes (for example a
  publisher inserting while the worker marks rows delivered) wait for the lock
  instead of failing with `SQLITE_BUSY`.
- When you need competing-consumers fan-out across many workers, use the
  PostgreSQL or MySQL backend.

The poll statement for SQLite therefore omits the `FOR UPDATE SKIP LOCKED`
clause that the other two dialects emit.

## Timestamps

PostgreSQL stores timestamps as `TIMESTAMPTZ`, MySQL as UTC `DATETIME(6)`, and
SQLite as `TEXT`. The SQLite backend writes and reads timestamps as UTC RFC 3339
strings with millisecond precision (`YYYY-MM-DDTHH:MM:SS.mmmZ`). This layout is
identical to the `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')` expression the dialect
uses for the current instant, so the `next_retry_at <= now` comparison that
drives retries stays correct under plain lexicographic ordering.

## Choosing a backend

| Need | Backend |
| --- | --- |
| Many workers, competing consumers | `postgres` or `mysql` |
| Embedded, tests, single process | `sqlite` |
| Native `JSONB` indexing on the payload | `postgres` |
