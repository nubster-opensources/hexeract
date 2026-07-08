# `hexeract` CLI reference

The `hexeract` binary ships in the `hexeract-cli` crate. Install with `cargo install hexeract-cli` (workspace path during development).

```text
hexeract <SUBCOMMAND>
```

Top-level subcommands:

- `outbox`: operate on the outbox storage
- `bus`: operate on the bus broker (RabbitMQ)
- `scheduler`: operate on the scheduler storage

Each top-level subcommand has its own set of actions documented below.

## `hexeract outbox`

### `outbox patch`

Print the canonical outbox schema SQL templated with the given table name. No network access.

```bash
hexeract outbox patch --table audit_outbox
```

### `outbox apply`

Apply the schema to a target PostgreSQL database. Requires the `--yes-i-know` safety flag because the operation creates a table.

```bash
hexeract outbox apply \
  --conn "$DATABASE_URL" \
  --table audit_outbox \
  --yes-i-know
```

### `outbox check`

Validate that the target table exists with the expected columns and indexes.

```bash
hexeract outbox check --conn "$DATABASE_URL" --table audit_outbox
```

## `hexeract bus`

The bus subcommands accept `--conn AMQP_URL` or the `HEXERACT_BUS_URL` environment variable.

### `bus declare`

Apply a topology described in TOML.

```bash
export HEXERACT_BUS_URL=amqp://guest:guest@localhost:5672
hexeract bus declare --topology crates/hexeract-cli/examples/topology.toml
```

The TOML schema:

```toml
[[exchanges]]
name = "orders.exchange"
kind = "topic"          # direct | topic | fanout | headers
durable = true          # default true
auto_delete = false     # default false

[[queues]]
name = "orders.received"
durable = true          # default true
exclusive = false       # default false
auto_delete = false     # default false

[[bindings]]
queue = "orders.received"
exchange = "orders.exchange"
routing_key = "orders.*"
```

Each entry is re-validated through the typed constructors (`Exchange::new`, `Queue::new`, `RoutingKey::new`, `Binding::new`). A malformed value fails with `BusError::InvalidTopology` before the broker is contacted.

### `bus peek`

Dump the first `N` messages of a queue **without consuming them**. Each delivery is `basic_nack(requeue=true)`-ed after print, so the queue is left intact.

```bash
hexeract bus peek --queue orders.received --count 5
```

Output (per message):

```text
#1 type=orders.placed message_id=<uuid> correlation_id=<uuid>
    payload: {"order_id":"..."}
```

If the queue is empty, prints `(queue `<name>` is empty)`.

### `bus purge`

Drop every message from a queue. Gated by the `--yes-i-know` safety flag, mirroring `outbox apply`.

```bash
hexeract bus purge --queue orders.received --yes-i-know
```

Output: `purged <N> message(s) from <name>`.

Without `--yes-i-know`, the command exits with a non-zero code and prints `refusing to purge without the explicit '--yes-i-know' safety flag` before opening any connection.

## `hexeract scheduler`

The `list`, `inspect`, `dead-letter list` and `dead-letter replay` subcommands share connection flags: `--conn` (env `DATABASE_URL`; the URL scheme selects the backend, `postgres://`/`postgresql://`, `mysql://` or `sqlite://`) and `--table` (env `HEXERACT_SCHEDULER_TABLE`, default `scheduled_messages`). Of these, only `list`, `inspect` and `dead-letter list` also accept `--format text|json` (default `text`): `dead-letter replay` has no `--format` flag. `scheduler schema` is offline DDL generation: it only accepts `--dialect` and `--table`, no `--conn` or `--format`.

### `scheduler schema`

Print the scheduler schema DDL for the selected dialect. No network access.

```bash
hexeract scheduler schema --dialect postgres --table scheduled_messages
```

`--dialect` accepts `postgres` (default), `my-sql` or `sqlite`. This is the CLI dialect token, not a connection URL scheme: `--conn` on the admin subcommands below uses `mysql://`, not `my-sql://`.

### `scheduler list`

List non-terminal (pending and paused) schedules. Accepts `--limit` (default 50).

```bash
hexeract scheduler list --conn "$DATABASE_URL" --format text
```

### `scheduler inspect`

Show the full state of one schedule by id.

```bash
hexeract scheduler inspect <SCHEDULE_ID> --conn "$DATABASE_URL"
```

### `scheduler dead-letter list`

List dead-lettered schedules. Accepts `--limit` (default 50).

```bash
hexeract scheduler dead-letter list --conn "$DATABASE_URL" --limit 50
```

### `scheduler dead-letter replay`

Replay a dead-lettered schedule: reset attempts and reschedule now.

```bash
hexeract scheduler dead-letter replay <SCHEDULE_ID> --conn "$DATABASE_URL"
```

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Generic runtime error (broker unreachable, validation failure, etc.) |
| `2` | clap parse error or safety-flag refusal |

## Environment variables

| Variable | Purpose |
| --- | --- |
| `HEXERACT_BUS_URL` | Default value for `--conn` on every `bus` subcommand |
| `DATABASE_URL` | Default value for `--conn` on every `scheduler` admin subcommand (`list`, `inspect`, `dead-letter`) |
| `HEXERACT_SCHEDULER_TABLE` | Default value for `--table` on every `scheduler` subcommand |
| `RUST_LOG` | Standard `tracing_subscriber` filter; default is `info` |

## Integration tests

The `hexeract-cli` crate ships two `#[ignore]` integration tests against a RabbitMQ container spun up via `testcontainers`. Run them locally with:

```bash
cargo test -p hexeract-cli -- --ignored
```

Docker is required.
