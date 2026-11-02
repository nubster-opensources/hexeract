# Migration v0.6 to v0.7

## RabbitMQ plaintext transport

RabbitMQ connections now require `amqps://` outside local development. Plain
`amqp://` remains available without extra configuration only for `localhost`,
`127.0.0.0/8`, and `::1`; the decision does not resolve DNS names.

The host is read with `lapin`'s own URI parser, the one that opens the
connection, so the check always covers the host actually dialled rather than
the raw URI text. Two consequences are worth knowing:

- An empty authority means loopback. `amqp://` and `amqp:///%2f` are valid
  AMQP URIs for `localhost:5672` and stay accepted.
- `lapin` discards a bracketed IPv6 literal and dials `localhost` instead, so
  `amqp://[::1]:5672` works and `amqps://[2001:db8::1]:5671` does **not**
  reach that address. Use a hostname for a remote IPv6 broker.

Change production broker URLs to `amqps://` and use
`RabbitMqConnectionConfig::with_tls_config` when the broker uses a private CA
or mutual TLS.

For a deliberately remote development broker that cannot offer TLS, opt in at
the construction site:

```rust,no_run
use hexeract_bus_rabbitmq::{
    RabbitMqConnectionConfig, RabbitMqTransport, RabbitMqTransportConfig,
};

# async fn connect() -> Result<(), hexeract_bus::BusError> {
let config = RabbitMqTransportConfig::default().with_connection_config(
    RabbitMqConnectionConfig::default().allow_insecure_plaintext_transport(),
);
let transport = RabbitMqTransport::new_with_config("amqp://dev-broker:5672", &config).await?;
# drop(transport);
# Ok(())
# }
```

This opt-in sends credentials and messages in cleartext. Do not use it in
production.

## `hexeract bus` commands

`hexeract bus declare`, `peek` and `purge` follow the same rule: a plain
`amqp://` connection string is refused for any host outside loopback. Each
command takes `--insecure-plaintext` to override the refusal for one
invocation, and prints a warning on stderr when it does.

```shell
hexeract bus peek --conn amqps://broker.internal:5671 --queue orders
hexeract bus peek --conn amqp://broker.internal:5672 --queue orders --insecure-plaintext
```

The flag is deliberately per-invocation rather than an environment variable:
it stays visible in the shell history and in the runbook that carries it.

## Renamed opt-out

`RabbitMqConnectionConfig::allow_plaintext_transport` is now
`allow_insecure_plaintext_transport`. The old name never shipped in a tagged
release, so only code tracking the development branch is affected; the
behaviour is unchanged apart from also covering the loopback restriction.

## Reserved `x-hexeract-*` headers

The whole `x-hexeract-*` prefix is now reserved for framework protocol
metadata, in every ASCII case variant. An application header using it is
refused with `BusError::ReservedHeaderNamespace` rather than published:

```text
before: headers.insert("x-hexeract-tenant".into(), tenant);  // published
after:  headers.insert("x-hexeract-tenant".into(), tenant);  // ReservedHeaderNamespace
```

**What to do:** rename any application header carrying that prefix, for example
to `x-acme-tenant` or a plain `tenant`. Grep your services for `x-hexeract`
before upgrading; the failure is loud at publish time, not silent, but it is a
publish failure.

On the consume side, a reserved key is accepted from the wire only in its
canonical lowercase spelling. A producer sending `X-Hexeract-Request-Id`
now has its delivery refused as invalid metadata instead of having it treated
as a protocol field.

## Bounded AMQP metadata

Metadata is now bounded in both directions, defaulting to 64 headers, 128 key
bytes, 8 KiB per value and 32 KiB in total across application *and* framework
headers. A payload under `max_payload_bytes` could previously carry an
unbounded field table; that gap is closed.

**What to do:** count the headers your deployment actually sends before
upgrading. The defaults leave ample room for trace context, tenancy metadata,
the RPC wire fields and a normal `x-death` history, but a service that
propagates a large baggage header may sit above them. Raise the bound
explicitly where needed:

```rust,ignore
let worker = RabbitMqWorkerBuilder::new(connection)
    .queue("orders.work")
    .metadata_limits(AmqpMetadataLimits {
        max_total_bytes: 64 * 1024,
        ..AmqpMetadataLimits::default()
    })
    .build()?;
```

Set the same value on the publisher and the request client, otherwise the
lowest-configured path is not the one that applies. A delivery above the bound
never reaches a handler: the worker routes it through its existing poison path,
and its dead-letter copy is republished with an empty field table, so any
downstream consumer that routes on a header must tolerate its absence.

## Envelope authenticity on the RabbitMQ transport

Signing and verification are now wired into the transport. Nothing changes for
a deployment that configures neither: publishing stays unsigned, delivery stays
unverified, and the worker keeps minting a fresh `message_id`, `correlation_id`
or `published_at` when a producer omits one.

Enabling it changes three things you must plan for.

**Header budget.** The six signature headers travel on the same AMQP field
table as your own and count against the same bounds, so an application's
header budget drops from 64 to 58 while a signature is attached. Count what
your services actually send before turning signing on, or raise
`AmqpMetadataLimits::max_headers` accordingly.

**Required wire fields.** A signed envelope must carry `message_id`,
`correlation_id` and `timestamp`. The signature covers them, so the worker can
no longer mint a replacement for an absent one: doing so would invalidate every
signature it touched. Under a verifying worker, an absent field is a rejection.
A producer that publishes through this crate always sets all three; a foreign
producer may not.

**Order of rollout.** Verification is what breaks a deployment, not signing.
Roll out in two steps, never one:

```rust,ignore
// Step 1: every publisher signs. Consumers still accept everything.
let transport = RabbitMqTransport::new_with_config(
    "amqps://broker.internal",
    &RabbitMqTransportConfig::default().with_outbound_envelope_security(Arc::clone(&outbound)),
)
.await?;

// Step 2, once every producer is signing: consumers start verifying.
let worker = RabbitMqWorkerBuilder::new(connection)
    .queue("orders.work")
    .envelope_security(Arc::clone(&inbound))
    .build()?;
```

Between the two steps, a worker configured with
`VerificationPolicy::AllowInsecureUnauthenticatedEnvelopes` accepts an envelope
carrying **no** signature while still rejecting one whose signature is present
and wrong. That is the migration setting, and it is deliberately not a way to
ignore a bad signature.

**What a rejection does.** A rejected delivery is never requeued. Its
dead-letter copy is republished with an empty field table, so a downstream
consumer that routes on a header must tolerate its absence, and without the
AMQP `user-id` property, which the broker validates against the republishing
connection rather than treating as descriptive metadata.

**What this does not give you.** The payload is not encrypted: a hostile broker
still reads every message. Replaying an authentic, unmodified message is not
prevented. And a handler still cannot learn who signed the message it received,
because the verified principal is not yet exposed to `HandlerContext`. Envelope
security decides what reaches a handler, not what the handler knows.

## Scheduler admin commands now enforce TLS

`hexeract scheduler list`, `inspect`, `dead-letter list` and `dead-letter
replay` used to open their PostgreSQL and MySQL pools through `sqlx` with no
transport policy of their own: an omitted `sslmode`/`ssl-mode`, or an explicit
`prefer`/`require` (`preferred`/`required` for MySQL), let the driver try TLS
and silently fall back to a cleartext session if the server declined it.

Every scheduler admin connection now raises an unspecified, `prefer` or
`require` mode to the strongest verification level the driver offers:
`sslmode=verify-full` on PostgreSQL, `ssl-mode=verify_identity` on MySQL.
Both verify the server
certificate against a trusted authority and check that the certificate
matches the host being dialled; the connection attempt fails outright rather
than downgrading if the server cannot present one.

**The only opt-out is an explicit `disable`.** Set `sslmode=disable`
(PostgreSQL) or `ssl-mode=disabled` (MySQL) in the connection string, or
`PGSSLMODE=disable` in the environment for PostgreSQL, to keep connecting in
cleartext. The CLI logs a warning naming the redacted target whenever this
opt-out is taken. There is no other way to disable the policy: unlike the
previous behaviour, a value merely containing the text `disable` inside a
password or an unrelated parameter is never mistaken for the setting.

**`verify-ca` is respected, not raised.** An explicit `sslmode=verify-ca` or
`ssl-mode=verify_ca` is kept exactly as configured. It already verifies the
certificate chain against a trusted authority; it just does not also check
that the certificate's name matches the host, which is a deliberate choice
for a deployment that reaches its database by IP address or through a tunnel.
Raising it to the `verify-full`/`verify-identity` level would break that
setup for no additional guarantee it asked for.

**The trust store is now the operating system's.** The workspace's `sqlx`
feature moved from `tls-rustls-ring` (a Mozilla certificate bundle baked into
the binary) to `tls-rustls-ring-native-roots`, so the scheduler commands
trust the same certificate authorities as `outbox apply`/`outbox check`
instead of a second, disjoint set. A database certificate issued by an
internal or enterprise CA that is installed in the operating system's trust
store is now accepted, where it previously was not.

The consequence runs the other way too: on a machine whose system trust
store is empty (a minimal container image without a `ca-certificates`
package is the common case), a connection to a database with a public,
well-known certificate now fails where it previously succeeded against the
baked-in bundle. Install `ca-certificates` (or your platform's equivalent) in
any image that runs `hexeract scheduler` commands against a TLS-protected
database.

**What to do if a scheduler command that used to work now fails to
connect.** Either add the database's certificate authority to the machine's
trust store, or, if the connection is genuinely local or already tunnelled,
opt out in full knowledge of the trade-off with `sslmode=disable` /
`ssl-mode=disabled`.

## RabbitMQ public surface settled on one configuration style

Every setting of the `hexeract-bus-rabbitmq` constructors is now a `with_*`
method on a config value, and each type has exactly two constructors: `x(uri)`
with the defaults, and `x_with_config(uri, config)`. The stacked constructors
and the separate `RabbitMqRequestClientConfigBuilder` are gone. Nothing of this
shipped in a tagged release, so only code tracking the development branch is
affected.

| Before | After |
| --- | --- |
| `RabbitMqConnection::connect_with_retry(uri, n, delay)` | `connect_with_config(uri, &RabbitMqConnectionConfig::default().with_retry_attempts(n).with_retry_base_delay(delay))` |
| `RabbitMqConnection::connect_with_retry_with_config(uri, n, delay, &config)` | `connect_with_config(uri, &config.with_retry_attempts(n).with_retry_base_delay(delay))` |
| `connect(uri)` (one attempt) | `connect(uri)` now retries; `with_retry_attempts(1)` keeps a single attempt |
| `RabbitMqTransport::with_exchange(uri, x)` | `new_with_config(uri, &RabbitMqTransportConfig::default().with_exchange(x))` |
| `RabbitMqTransport::with_exchange_with_config(uri, x, &conn)` | `new_with_config(uri, &RabbitMqTransportConfig::default().with_connection_config(conn).with_exchange(x))` |
| `RabbitMqTransport::new_with_config(uri, &conn)` | `new_with_config(uri, &RabbitMqTransportConfig::default().with_connection_config(conn))` |
| `RabbitMqTransport::new_with_transport_config(uri, &config)` | `new_with_config(uri, &config)` |
| `transport.fire_and_forget()` | `RabbitMqTransportConfig::default().fire_and_forget()` |
| `transport.with_metadata_limits(limits)` | `RabbitMqTransportConfig::default().with_metadata_limits(limits)` |
| `transport.with_outbound_envelope_security(security)` | `RabbitMqTransportConfig::default().with_outbound_envelope_security(security)` |
| `RabbitMqTransport::from_connection(connection, size)` | Removed from the public surface; open the transport with `new_with_config` |
| `RabbitMqRequestClientConfigBuilder::new().max_in_flight(n).build()` | `RabbitMqRequestClientConfig::default().with_max_in_flight(n)` |
| `RabbitMqRequestClientConfigBuilder` `.connection_config(..)`, `.metadata_limits(..)`, `.envelope_security(..)`, `.outbound_envelope_security(..)` | The same names prefixed with `with_` on `RabbitMqRequestClientConfig` |
| `RabbitMqWorkerConfig` | No longer public; keep setting the same values through `RabbitMqWorkerBuilder` |

**Retry is on by default.** `RabbitMqConnection::connect` used to make one
attempt and now makes up to five, waiting 250 ms, 500 ms, 1 s and 2 s between
them, as `RabbitMqTransport::new` always did. A permanent failure (refused
credentials, a rejected certificate, a cleartext transport that is refused)
still stops at the first attempt. A caller that wants the old behaviour writes
`RabbitMqConnectionConfig::default().with_retry_attempts(1)`.

**Worker queue declaration is opt-in.** `RabbitMqWorkerBuilder::declare_queue()`
makes the worker declare the queue it consumes (durable, not exclusive, not
auto-delete) when it starts. Without that call nothing changes, and the queue
stays the operator's. Against an existing queue declared with other arguments,
`run` fails with a `BusError` carrying the broker's `PRECONDITION_FAILED`.
