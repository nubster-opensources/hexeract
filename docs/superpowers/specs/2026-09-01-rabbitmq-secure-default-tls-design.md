# RabbitMQ secure-default TLS design

## Goal

Prevent a production RabbitMQ client from silently connecting over plaintext
AMQP when its URI is misconfigured. TLS remains selected by `amqps://`; this
change makes plaintext `amqp://` an explicit local-development exception.

## Policy

Every RabbitMQ connection entry point applies one shared URI policy before it
opens a network connection:

- `amqps://` is allowed and continues to use the configured trust and client
  identity material from `RabbitMqConnectionConfig`.
- `amqp://` is allowed by default only for a syntactically loopback endpoint:
  the hostname `localhost` (case-insensitive), an IPv4 address in
  `127.0.0.0/8`, or the IPv6 address `::1`.
- Plaintext to every other hostname or address is rejected as a permanent,
  credential-redacted `BusError::Connection` before any connection attempt.
- An unrecognised or malformed URI is rejected through the existing
  credential-redacted connection-error path.

The loopback decision is syntactic. The client never resolves a hostname to
decide whether it is local, avoiding a DNS lookup, a time-of-check/time-of-use
gap, and a hostname that changes meaning after validation. In particular,
`dev-broker` is not accepted merely because a local DNS configuration currently
points it to `127.0.0.1`.

## Explicit plaintext override

`RabbitMqConnectionConfig` gains
`allow_insecure_plaintext_transport()`. It is the only public opt-in that
allows a non-loopback `amqp://` URI. Its rustdoc states that it exposes broker
credentials and message content in transit and is suitable only for an
intentional development environment.

The existing `allow_plaintext_transport()` method, introduced with #350 only
to permit a test harness to reuse TLS material on a plaintext connection, is
retired in favour of this consistently named policy. The configuration keeps
the TLS-material guard: without the explicit override, a configuration holding
a CA or client identity cannot be paired with `amqp://`, including on
loopback. Thus a deployment that believes it configured mTLS never connects in
cleartext by accident.

## Propagation

The policy is enforced in the common connection primitive used by:

- `RabbitMqConnection::{connect, connect_with_config, connect_with_retry,
  connect_with_retry_with_config}`;
- recovering publisher sessions used by `RabbitMqTransport::{new,
  with_exchange}` and their configuration-aware variants;
- the initial and reconnecting reply-inbox sessions owned by
  `connect_request_client_with_config`.

Workers are constructed from `RabbitMqConnection`, so they inherit the policy
at the point their connection is created without gaining a competing builder
option.

## Diagnostics and compatibility

Rejections use a fixed reason and the existing URI redaction helper. No
diagnostic renders credentials or TLS material. Existing local examples and
testcontainers helpers continue to use loopback plaintext without an opt-in.
Remote plaintext users must add the explicit insecure policy or move to
`amqps://`; this intentional behaviour change is documented in the production
checklist and v0.7 migration notes.

## Verification

Unit tests cover the permitted loopback forms, non-loopback rejection, the
explicit override, redaction, and the interaction with configured TLS
material. A Docker integration test confirms that the pre-existing plaintext
test harness remains usable on a loopback URI. Existing private-CA/mTLS tests
remain the compatibility proof for `amqps://`.

## Out of scope

This issue does not require TLS for loopback development, resolve arbitrary
hostnames, alter TLS trust verification, or add a system-wide network policy.
Those are separate deployment concerns.
