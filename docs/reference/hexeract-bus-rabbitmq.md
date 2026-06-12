# `hexeract-bus-rabbitmq` API reference

RabbitMQ backend for the bus, powered by `lapin`. Implements the [`Transport`](hexeract-bus.md) trait and ships a consumer worker, a channel pool, a typed connection wrapper and topology helpers.

The full rustdoc lives at <https://docs.rs/hexeract-bus-rabbitmq>.

## Public surface

### Connection

| Item | Role |
| --- | --- |
| `RabbitMqConnection::connect(uri)` | Single-shot connect. Returns `BusError::Connection` on failure. |
| `RabbitMqConnection::connect_with_retry(uri, attempts, base_delay)` | Bounded exponential-backoff retry loop. Logs each failure at `warn`. |
| `RabbitMqConnection::create_channel()` | Open a fresh AMQP channel. |
| `RabbitMqConnection::with_channel(|ch| async { ... })` | Open a short-lived channel, hand it to the closure, drop on return. Used by every topology helper. |
| `DEFAULT_RETRY_ATTEMPTS = 5`, `DEFAULT_RETRY_BASE_DELAY = 250 ms` | Defaults used by `RabbitMqTransport::new`. |

### Channel pool

| Item | Role |
| --- | --- |
| `ChannelPool::new(connection, max_size)` | Build a per-publisher bounded cache. Channels are opened with publisher confirms enabled. |
| `ChannelPool::without_confirms()` | Opt out of `confirm_select` on freshly opened channels. Call before the first `acquire()`: confirm mode is sticky per channel. |
| `ChannelPool::acquire()` | Return a `PooledChannel<'_>` RAII guard that releases the channel on drop. |
| `DEFAULT_POOL_MAX_SIZE = 8` | Default capacity. |

### Transport

| Item | Role |
| --- | --- |
| `RabbitMqTransport::new(uri)` | Connect with retry and target the AMQP default exchange. |
| `RabbitMqTransport::with_exchange(uri, exchange)` | Connect, declare a typed `Exchange`, target it. |
| `RabbitMqTransport::from_connection(connection, pool_size)` | Reuse an existing connection (useful when several transports share a broker session). |
| `RabbitMqTransport::fire_and_forget()` | Switch to fire-and-forget publishing: no publisher confirm, no `mandatory` flag, `Ok` no longer proves delivery. Messages stay persistent. |
| Implements `Transport` from `hexeract-bus` (three publish methods). | Mints `BusEnvelope`, encodes JSON, sends through `lapin::Channel::basic_publish` with `mandatory` set, awaits the publisher confirm. An unroutable routing key surfaces as `BusError::Unroutable` instead of silently dropping the message. |

AMQP `BasicProperties` set on every publish: `message_id`, `correlation_id`, `content_type = "application/json"`, `type = MESSAGE_TYPE`, `delivery_mode = 2` (persistent), `timestamp` (the envelope's `published_at` in epoch seconds), optional `reply_to`, free-form `headers` (each as `LongString`).

### Worker

| Item | Role |
| --- | --- |
| `RabbitMqWorkerBuilder::new(connection)` | Fluent entry point. Symmetric with `PgOutboxWorkerBuilder`. |
| `.queue(name)` | Mandatory. The queue to consume from. |
| `.register_handler::<M, _>(handler)` | Register a typed handler per `MESSAGE_TYPE`. Repeated registration replaces silently. |
| `.ack_mode(AckMode)` | `Manual` (default), `AckOnReceive`, or `Unacknowledged`. |
| `.max_attempts(n)` | Default 5. |
| `.prefetch(n)` | Default 16. |
| `.dead_letter_routing_key(rk)` | Routes exhausted deliveries to that routing key on the default exchange. |
| `.build()?` | Returns `RabbitMqWorker`. Errors if `.queue(...)` was never set. |
| `RabbitMqWorker::run(cancel)` | Drives the consume loop until the `CancellationToken` fires. |

| Item | Role |
| --- | --- |
| `AckMode::Manual` | Default. At-least-once. Retries per `message_id` up to `max_attempts`, then DLR or drop. |
| `AckMode::AckOnReceive` | At-most-once. Explicit `basic_ack` on receive before the handler runs (`no_ack = false`). |
| `AckMode::Unacknowledged` | Fire-and-forget. Consumer-side `no_ack = true`, lossy on handler failure or crash. |
| `RabbitMqWorkerConfig` | Tunable knobs: `ack_mode`, `max_attempts`, `prefetch`, `dead_letter_routing_key`, `max_buffered: Option<usize>`. |
| `.max_buffered(n)` | Bounds the in-memory delivery buffer under `AckMode::Unacknowledged` (`None` = unbounded, not recommended). Has no effect under `AckMode::Manual` or `AckMode::AckOnReceive`, which are already bounded by `basic.qos` prefetch. |

See the [worker concept](../concepts/worker.md), the [ack modes](../concepts/ack-modes.md) and the [retry policy](../concepts/retry-policy.md).

### Topology helpers

| Item | Role |
| --- | --- |
| `declare_exchange(connection, &Exchange)` | Short-lived channel, `exchange.declare`. |
| `declare_queue(connection, &Queue)` | `queue.declare`. |
| `bind_queue(connection, &Binding)` | `queue.bind`. |
| `ensure_topology(connection, &[Exchange], &[Queue], &[Binding])` | Apply the three phases on a single channel, in dependency order. |

Documented as POC / dev-convenience: declare your topology once at startup, not on the publish hot path.

## Where to read next

- [Bus quick start](../getting-started/bus-quick-start.md)
- [Bus flow architecture](../architecture/bus-flow.md)
- [Worker concept](../concepts/worker.md)
- Runnable example: `cargo run --example 03_bus_pubsub -p hexeract-examples`
