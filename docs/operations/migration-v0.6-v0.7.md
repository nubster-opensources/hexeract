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
