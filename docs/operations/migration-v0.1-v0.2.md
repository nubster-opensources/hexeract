# Migration v0.1.0 to v0.2.0

v0.2.0 is a **strictly additive** release. Projects that only used the outbox in v0.1.0 keep their code untouched: every `hexeract-outbox`, `hexeract-outbox-postgres` and `hexeract outbox` CLI surface stays bit-for-bit compatible.

## What's new

| Crate | Status | Notes |
| --- | --- | --- |
| `hexeract-bus` | New in v0.2.0 | Backend-agnostic bus core |
| `hexeract-bus-rabbitmq` | New in v0.2.0 | RabbitMQ backend via lapin |
| `hexeract-outbox` | Unchanged from v0.1.0 | Same trait surface |
| `hexeract-outbox-postgres` | Unchanged from v0.1.0 | Same builder, same schema |
| `hexeract-cli` | Extended | New `hexeract bus declare / peek / purge` subcommands. `hexeract outbox patch / apply / check` unchanged. |
| `hexeract-core` | Unchanged | `HandlerContext`, `MessageId`, `CorrelationId` available to bus handlers |

## Upgrade in three steps

1. Bump every Hexeract crate in your `Cargo.toml` to `0.2`:

   ```toml
   hexeract-outbox = "0.2"
   hexeract-outbox-postgres = "0.2"
   hexeract-cli = "0.2"
   ```

2. Run `cargo update` and rebuild. No code change should be required.

3. (Optional) Adopt the bus where it makes sense. Start with the [bus quick start](../getting-started/bus-quick-start.md).

## API changes

None on the outbox side. The bus side is entirely new.

## What did NOT change

- The PostgreSQL schema served by `POSTGRES_SCHEMA_SQL` is identical to v0.1.0.
- `PgOutboxWorkerBuilder` defaults are identical (`poll_interval = 100 ms`, `batch_size = 10`, `max_attempts = 5`, `retry_delay = 5 s`).
- `Event`, `Handler<E>`, `OutboxPublisher`, `OutboxStore`, `OutboxWorker` keep their v0.1.0 signatures.

## What is NOT yet covered by the bus

- Native NATS, Kafka and SQS backends land in v0.9.0 (Polyglot Transports). Use the polyglot bus pattern when you need broker portability today: stay on RabbitMQ, switch when the v0.9.0 backends ship.
- Saga orchestration (v0.8.0), Scheduler (v0.6.0), Request/Reply (v0.7.0), Mediator (v0.3.0) and Reliability extensions (v0.5.0) are roadmap items. See [ROADMAP.md](../../ROADMAP.md).

## CLI workflow that did not exist in v0.1.0

After upgrading, expose the new bus operator surface in your runbook:

```bash
export HEXERACT_BUS_URL=amqp://guest:guest@localhost:5672

hexeract bus declare --topology infra/topology.toml
hexeract bus peek    --queue orders.received --count 5
hexeract bus purge   --queue orders.received --yes-i-know
```

The legacy outbox surface stays:

```bash
hexeract outbox patch --table audit_outbox
hexeract outbox apply --conn "$DATABASE_URL" --table audit_outbox --yes-i-know
hexeract outbox check --conn "$DATABASE_URL" --table audit_outbox
```

## Verification checklist

After the bump:

- [ ] `cargo build --workspace` succeeds.
- [ ] `cargo test --workspace` succeeds.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` succeeds.
- [ ] Outbox runtime behaviour observed on staging is unchanged (poll rate, throughput, retry semantics).
- [ ] New bus crates pulled in only by services that publish or consume on the bus (no unnecessary `lapin` in projects that stay on the outbox alone).
