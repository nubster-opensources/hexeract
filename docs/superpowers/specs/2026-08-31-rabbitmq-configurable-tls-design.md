# RabbitMQ configurable TLS design

## Goal

Allow a RabbitMQ deployment using a private certificate authority or mutual TLS
to connect through every Hexeract RabbitMQ path.  The default remains the
platform trust store used by lapin; callers opt into a custom TLS configuration
only when their broker requires it.

This implements issue #350.  It deliberately does not implement issue #449:
the URI scheme remains the policy boundary in this change.  `amqps://` selects
TLS and `amqp://` remains supported for existing local-development and test
setups until the secure-default policy is introduced separately.

## Public API

`hexeract-bus-rabbitmq` will expose a new `RabbitMqConnectionConfig` with a
`Default` implementation.  It owns a `lapin::tcp::OwnedTLSConfig`, initially
set to lapin's default configuration (the platform trust store).  Its builder
method accepts a replacement `OwnedTLSConfig` for a private CA and, where the
broker requires it, a client identity.

The crate will re-export lapin's `OwnedTLSConfig` and `OwnedIdentity` at the
RabbitMQ boundary.  Applications can configure TLS without adding a direct
lapin dependency, while Hexeract does not duplicate certificate parsing,
identity formats, or trust-store handling.

Existing URI-only constructors retain their behaviour and delegate to a
default `RabbitMqConnectionConfig`.  New configuration-aware entry points are:

- `RabbitMqConnection::connect_with_config`;
- `RabbitMqConnection::connect_with_retry_with_config`;
- `RabbitMqTransport::new_with_config`;
- `RabbitMqTransport::with_exchange_with_config`.

The existing `RabbitMqRequestClientConfig` gains a connection-configuration
field and matching builder method.  Thus `connect_request_client_with_config`
uses one configuration for both of its independent connections: the
auto-recovering publisher and the supervised reply inbox.  The simpler
`connect_request_client` retains the default configuration.

## Connection flow

The connection module gains one internal connect primitive which receives a
`ConnectionProperties` value and a borrowed `RabbitMqConnectionConfig`.  Each
attempt clones the owned TLS configuration and calls lapin's
`Connection::connect_with_config` using the default runtime.  This preserves
the existing error redaction and transient/permanent failure classification.

The retry loop, the publisher probe, and the auto-recovering publisher session
all call that primitive.  A custom CA or client identity therefore cannot be
lost between retries or between the probe and the long-lived publisher
connection.  A configuration is never logged, formatted in an error, or stored
in tracing fields.

`RabbitMqConnectionConfig` does not change URI policy: a custom TLS
configuration accompanies the connection, while `amqps://` still selects TLS.
No insecure fallback is introduced.  The later secure-default work can add a
scheme policy to this same configuration type without changing the transport
or request-client APIs again.

## Error handling and compatibility

TLS handshake and certificate failures remain `BusError::Connection`, using
the existing credential-redacted URI.  Retriability continues to be determined
from the lapin error kind.  No certificate bytes, passwords, private keys, or
raw URIs appear in errors or logs.

All current constructors and their defaults remain source compatible.  The new
configuration is additive.  The crate keeps `lapin` as the implementation
authority for trust-store, PEM-chain, and client-identity validation rather
than providing a competing format or file-loading API.

## Tests and documentation

Unit tests will cover configuration propagation through the single-shot,
retry, recovering-publisher, transport, and request-client paths using a
connection seam that records the supplied configuration without rendering its
secrets.  Existing connection-failure tests continue to prove safe redaction.

An ignored Docker integration test will run RabbitMQ with a test private CA
and require a client certificate.  It will exercise a publish/consume round
trip and a request/reply round trip through the new configuration.  CI's
Docker integration job executes it.

The RabbitMQ reference and production checklist will show a minimal private-CA
and mTLS configuration, state that the URI must remain `amqps://`, and explain
that the default uses the platform trust store.

## Out of scope

- Requiring TLS by default or deciding which loopback endpoints may use
  plaintext (#449).
- Envelope-level publisher authentication and authorization (#444).
- Loading certificate files from environment variables or configuration files.
  Applications load their secrets and construct `OwnedTLSConfig` themselves.
