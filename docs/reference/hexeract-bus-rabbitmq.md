# `hexeract-bus-rabbitmq` API reference

RabbitMQ backend for the bus, powered by `lapin`. Implements the [`Transport`](hexeract-bus.md) trait and ships a consumer worker, a channel pool, a typed connection wrapper and topology helpers.

The full rustdoc lives at <https://docs.rs/hexeract-bus-rabbitmq>.

## Public surface

### Connection

| Item | Role |
| --- | --- |
| `RabbitMqConnection::connect(uri)` | Single-shot connect. Returns `BusError::Connection` on failure. |
| `RabbitMqConnection::connect_with_config(uri, config)` | Single-shot connect with a private-CA or client-certificate TLS configuration. |
| `RabbitMqConnection::connect_with_retry(uri, attempts, base_delay)` | Bounded exponential-backoff retry loop. Logs each failure at `warn`. |
| `RabbitMqConnection::connect_with_retry_with_config(uri, attempts, base_delay, config)` | Same retry loop while retaining the TLS settings for every attempt. |
| `RabbitMqConnection::create_channel()` | Open a fresh AMQP channel. |
| `RabbitMqConnection::with_channel(|ch| async { ... })` | Open a short-lived channel, hand it to the closure, drop on return. Used by every topology helper. |
| `DEFAULT_RETRY_ATTEMPTS = 5`, `DEFAULT_RETRY_BASE_DELAY = 250 ms` | Defaults used by `RabbitMqTransport::new`. |

### TLS with a private CA or mTLS

Use an `amqps://` URI to select TLS. The default connection configuration uses
the platform trust store. For a broker using an internal CA, build a
`RabbitMqConnectionConfig` with the re-exported TLS types; adding a client
identity enables mutual TLS when the broker requires it.

```rust,no_run
use hexeract_bus_rabbitmq::{
    OwnedIdentity, OwnedTLSConfig, RabbitMqConnectionConfig, RabbitMqTransport,
};

# async fn connect() -> Result<(), Box<dyn std::error::Error>> {
let tls_config = OwnedTLSConfig {
    cert_chain: Some(std::fs::read_to_string("/run/secrets/rabbitmq-ca.pem")?),
    identity: Some(OwnedIdentity::PKCS12 {
        der: std::fs::read("/run/secrets/rabbitmq-client.p12")?,
        password: std::env::var("RABBITMQ_CLIENT_CERT_PASSWORD")?,
    }),
};
let connection_config = RabbitMqConnectionConfig::default().with_tls_config(tls_config);

let transport = RabbitMqTransport::new_with_config(
    "amqps://rabbitmq.internal:5671/%2f",
    &connection_config,
)
.await?;
# drop(transport);
# Ok(())
# }
```

Load certificate material through the application's secret-management system;
do not place private keys or PKCS#12 passwords in source control. The same
`connection_config` is accepted by `RabbitMqTransport::with_exchange_with_config`
and `RabbitMqRequestClientConfigBuilder::connection_config`, the latter applying
it to both the publisher and reply-inbox connections.

#### Prerequisite: one rustls crypto provider

TLS goes through `rustls`, which refuses to choose a cryptographic provider on
its own when its crate features name more than one. It does not return an
error in that case: it **panics inside lapin's io loop at the first
handshake**, and the failure reaches the caller as an ordinary retryable
connection error, so a supervisor will retry it forever.

The dependency graph of this crate alone resolves `aws-lc-rs` and nothing
else, so `amqps://` works with no action required. The ambiguity appears when
the final binary pulls a second provider, which is common: an HTTP client on
`ring`, a database driver on a different provider, or `testcontainers` in a
test binary. Whenever that happens, select one explicitly, once, before the
first connection:

```rust,ignore
rustls::crypto::aws_lc_rs::default_provider()
    .install_default()
    .expect("the crypto provider must be selected once, before any TLS use");
```

The choice belongs to the binary, never to a library, which is why this crate
installs nothing on your behalf. `cargo tree -i ring` and
`cargo tree -i aws-lc-rs` tell you whether your build is ambiguous.

A private CA is an **additional** trust anchor, not a replacement: it is
appended to the platform trust store, so a certificate issued by any publicly
trusted authority for the broker hostname stays acceptable. Supplying an
internal CA therefore widens what is accepted rather than pinning trust to your
own authority, and lapin exposes no way to restrict verification further.
Treat broker authentication as resting on mutual TLS and on the credentials in
the URI, not on the private CA alone.

TLS material only takes effect on an `amqps://` URI. Pairing it with a
plaintext `amqp://` URI is refused with a permanent connection error rather
than silently ignored, so a mis-templated scheme cannot downgrade a session to
cleartext while the deployment believes it runs mutual TLS. A test harness that
deliberately reuses one configuration across both transports opts out with
`RabbitMqConnectionConfig::allow_plaintext_transport`.

A rejected certificate (unknown authority, hostname outside the SAN, expired
certificate, refused client certificate) is classified as a permanent
connection failure, so a supervisor stops instead of rebuilding the connection
against a trust chain that can never succeed. The failure is named in the
error's source and in the `failure` field of the `warn` log line, which
distinguishes a certificate fault from an unreachable broker without ever
rendering the credential-bearing URI.

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
| `RabbitMqTransport::new_with_config(uri, config)` | Connect with retry and caller-selected TLS settings. |
| `RabbitMqTransport::with_exchange(uri, exchange)` | Connect, declare a typed `Exchange`, target it. |
| `RabbitMqTransport::with_exchange_with_config(uri, exchange, config)` | Declare and target an exchange with caller-selected TLS settings. |
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
