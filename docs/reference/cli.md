# `hexeract` CLI reference

The `hexeract` binary ships in the `hexeract-cli` crate. Install with `cargo install hexeract-cli` (workspace path during development).

```text
hexeract <SUBCOMMAND>
```

Top-level subcommands:

- `outbox`: operate on the outbox storage
- `bus`: operate on the bus broker (RabbitMQ)

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
| `RUST_LOG` | Standard `tracing_subscriber` filter; default is `info` |

## Integration tests

The `hexeract-cli` crate ships two `#[ignore]` integration tests against a RabbitMQ container spun up via `testcontainers`. Run them locally with:

```bash
cargo test -p hexeract-cli -- --ignored
```

Docker is required.
