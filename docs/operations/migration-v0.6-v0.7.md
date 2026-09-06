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
use hexeract_bus_rabbitmq::{RabbitMqConnectionConfig, RabbitMqTransport};

# async fn connect() -> Result<(), hexeract_bus::BusError> {
let config = RabbitMqConnectionConfig::default().allow_insecure_plaintext_transport();
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
let transport = RabbitMqTransport::new("amqps://broker.internal")
    .await?
    .with_outbound_envelope_security(Arc::clone(&outbound));

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
