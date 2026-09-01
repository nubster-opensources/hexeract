# Migration v0.6 to v0.7

## RabbitMQ plaintext transport

RabbitMQ connections now require `amqps://` outside local development. Plain
`amqp://` remains available without extra configuration only for `localhost`,
`127.0.0.0/8`, and `::1`; the decision does not resolve DNS names.

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
