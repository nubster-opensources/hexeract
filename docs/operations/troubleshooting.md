# Troubleshooting

Symptoms first, hypotheses second, fixes last. Open an issue if your case is not covered.

## Outbox

### Events never reach the handler

| Likely cause | How to confirm | Fix |
| --- | --- | --- |
| Worker not spawned | No log line from the worker | Wire `tokio::spawn(worker.run(cancel))` and inspect the `JoinHandle` for early termination |
| Wrong table name | `SELECT count(*) FROM <your_table>` shows rows but the worker polls a different name | Pass the same `table_name` to `PgOutboxPublisher::new` and `PgOutboxWorkerBuilder::new` |
| Handler not registered | Worker logs `MissingHandler { event_type: ... }` | Call `register_handler::<E, _>(handler)` on the builder for that event type |
| `max_attempts` reached | `SELECT attempts, last_error FROM <table> WHERE delivered_at IS NULL AND attempts >= 5` shows rows | Reset the row (`UPDATE ... SET attempts = 0`) after fixing the handler, or raise `max_attempts` |

### Handler runs but downstream system never sees the side effect

The handler returned `Ok(())` but the side effect did not commit. Wrap the handler body in `Result::Err` mapping and inspect the actual return path. The outbox marks the row delivered the moment the handler returns `Ok`; a silent swallow inside the handler is invisible from the outbox.

### Duplicate side effects

Expected behaviour: at-least-once delivery permits duplicates on crash or `max_attempts` retry. Make the handler idempotent (deduplication key, conditional INSERT, `ON CONFLICT DO NOTHING`).

### Worker idle even with pending events

| Likely cause | How to confirm | Fix |
| --- | --- | --- |
| `next_retry_at` in the future | `SELECT event_id, next_retry_at FROM <table> WHERE delivered_at IS NULL` shows future timestamps | Wait `retry_base_delay` (default 1 s; bounded exponential backoff up to `retry_max_delay` = 300 s); reset the timestamp manually only for emergency dispatch |
| Pool exhausted | Worker logs `acquire` errors or hangs | Raise `max_connections` on `sqlx::postgres::PgPoolOptions` or check for connection leaks in the caller code |
| Cancellation token already cancelled | Worker exited on startup with `Ok(())` | Re-issue a fresh `CancellationToken` |

## Bus (RabbitMQ)

### Producer cannot connect

| Likely cause | Fix |
| --- | --- |
| `plaintext amqp is restricted to loopback` | Since v0.7.0 a plain `amqp://` URI is refused for any host other than loopback, before the socket opens, because the AMQP handshake sends the broker password in cleartext. Point the URI at `amqps://` (port 5671 by default). Only if the broker has no TLS listener yet, opt out explicitly with `RabbitMqConnectionConfig::allow_insecure_plaintext_transport`, or `--insecure-plaintext` on the `hexeract bus` commands. See [Migration v0.6 to v0.7](migration-v0.6-v0.7.md). |
| Wrong URI scheme | Use `amqps://` for a remote broker, `amqp://` for loopback; not `amqp+ssl://` |
| `invalid amqp uri` | The URI did not parse. Percent-encode the vhost (`/` becomes `%2f`) and any reserved character in the password, and drop stray spaces. |
| Broker not reachable | `nc -zv host 5671` to confirm; check firewall, NAT, TLS cert chain |
| Default retry exhausted | Raise `connect_with_retry` attempts or `base_delay` when wrapping `RabbitMqConnection::connect_with_retry` directly |

A cleartext refusal is permanent: it is classified non-retryable so a supervisor stops instead of reconnecting forever against a configuration no retry can fix.

### Consumer registered but handler never invoked

| Likely cause | Fix |
| --- | --- |
| Queue name mismatch | `hexeract bus peek --queue <queue> --count 1` shows messages there; verify worker builder uses the same name |
| `MESSAGE_TYPE` mismatch | The `Message::MESSAGE_TYPE` constant must match the AMQP `type` property the producer sets. If a foreign producer uses a different routing convention, register a handler for that exact type. |
| Messages lost under `AckMode::AckOnReceive` or `AckMode::Unacknowledged` | Switch to `AckMode::Manual` if you need at-least-once and to retry handler failures |

### Same message dispatched again and again

Expected when the handler returns `Err` under `AckMode::Manual` and `max_attempts` is high. Confirm the retry counter visible in your logs (`attempt = N`); raise the budget only if the handler can genuinely succeed after more tries, otherwise drop the message via the [dead-letter routing key](../concepts/retry-policy.md).

### Worker stops without panic on broker restart

Lapin does not auto-reconnect mid-session. The `run` future returns once the consumer stream errors out. Wrap the spawn in a supervisor:

```rust
loop {
    let cancel_for_run = cancel.clone();
    let result = worker.clone().run(cancel_for_run).await;
    if cancel.is_cancelled() {
        return result;
    }
    tracing::warn!(?result, "rabbitmq worker exited, reconnecting");
    tokio::time::sleep(Duration::from_secs(1)).await;
    // rebuild worker through the builder using a fresh connection
}
```

A first-class supervisor is planned for a future release.

## Scheduler

### Occurrence dead-lettered

An occurrence exhausted its `max_attempts` budget, or was swept by `dead_letter_exhausted` after a worker crashed mid-attempt (logged as `swept schedules whose attempt budget was exhausted by a crashed worker` at `error` level, with a `count` field). Inspect the entry with `hexeract scheduler dead-letter list --conn "$DATABASE_URL"`, fix the underlying cause (a broken handler, an unreachable sink), then replay it with `hexeract scheduler dead-letter replay <SCHEDULE_ID> --conn "$DATABASE_URL"`.

### Growing dispatch lag

Track `lag_ms` on the `scheduled occurrence dispatched` event or the `scheduler.dispatch` span (see [observability](observability.md)). Sustained growth signals the worker pool is under-provisioned for the occurrence volume, or the sink (bus broker, outbox table, mediator handler) is slow or blocked. Scale workers horizontally, or investigate the sink's own latency first.

### Repeated `lease lost; occurrence settled by another worker` warnings

This `warn` fires when a worker tries to acknowledge an occurrence whose lease already moved to another worker. A few during normal operation are harmless (the fencing worked as intended). Repeated warnings point to either a zombie worker (paused on I/O or descheduled long enough that its lease expired before it could acknowledge) or a `lease` configured too close to `dispatch_timeout x batch_size`. Raise `lease` with more headroom, or investigate why a worker is stalling past its lease window.

## CLI

### `hexeract bus declare` fails with `InvalidTopology`

Inspect the error message: it surfaces the validation rule that fired (empty name, > 127 bytes, control characters, > 255 bytes routing key, ...). See [topology validation rules](../concepts/topology.md).

### `hexeract bus purge` reports `refusing to purge without ...`

Add `--yes-i-know` to the command line. The flag is mandatory by design for destructive operations, mirroring `hexeract outbox apply`.

### `hexeract bus peek` prints `(queue ... is empty)` despite known traffic

`peek` fetches through `basic_get`, accumulates the delivery tags as it prints each message, then releases the whole batch at once with a single `basic_nack(multiple=true, requeue=true)`. If the queue has just been drained by a live consumer, `peek` will see it empty. Run `peek` before starting consumers, or against a paused consumer, when you want to inspect in-flight traffic.

## Build, test and CI

### `cargo deny check` fails on license

A new transitive dependency uses a license not in `deny.toml`. Either add the license to the allow list (if compatible with MIT/Apache-2.0) or vendor a fork that swaps the offending crate.

### Integration tests skipped silently

Integration tests are `#[ignore]` by default to keep `cargo test` Docker-free. Pass `-- --ignored` to run them: `cargo test --workspace -- --ignored`. The CI workflow `integration.yml` runs them on Linux with Docker installed.
