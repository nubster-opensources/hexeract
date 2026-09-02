# Changelog

All notable changes to this crate are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Exclusive, auto-delete, server-named reply inbox (`declare_reply_inbox` / `run_reply_inbox`) consumed on a connection distinct from the auto-recovering publisher, routing every delivery to a `hexeract_bus::RequestRegistry` by request id, read from the `x-hexeract-request-id` header. (#400)
- `connect_request_client`, assembling a ready-to-use `RequestClient<RabbitMqTransport>`: an auto-recovering publisher transport plus a supervised reply inbox whose background task drains in-flight requests, reconnects and re-declares a fresh inbox on broker loss. (#400)
- `RabbitMqWorkerBuilder::register_request_handler`, registering a `RequestHandler` on the worker so an incoming request is dispatched and its reply, or an encoded error, published back automatically via `RepliedHandler`. (#401)
- `RabbitMqWorkerBuilder::register_request_handler_with_counters`, registering the same responder while sharing a `ResponderCounters` handle that reports invalid reply destinations, request identities and protocol versions rejected before handler dispatch. (#491)
- `AmqpMetadataLimits`, bounding AMQP metadata in both directions: at most
  `DEFAULT_MAX_HEADERS` (64) top-level headers, `DEFAULT_MAX_HEADER_KEY_BYTES`
  (128) bytes per key, `DEFAULT_MAX_HEADER_VALUE_BYTES` (8 KiB) per top-level
  value and `DEFAULT_MAX_METADATA_BYTES` (32 KiB) across all keys and values.
  Every dimension is a byte length, input sitting exactly on a limit is
  accepted, a one-byte overflow is rejected, and zero is a valid deny-all
  value. Configure it with `RabbitMqTransport::with_metadata_limits`,
  `RabbitMqWorkerBuilder::metadata_limits` or
  `RabbitMqRequestClientConfigBuilder::metadata_limits`; the request-client
  setting reaches the publisher and every reply inbox the supervisor rebuilds
  after a reconnect. (#448)

### Changed

- Breaking: `BusError::Connection` is now a struct variant `{ source, retryable }`.
  Build it with `BusError::connection(source, retryable)` and read the transience
  classification with `BusError::is_retryable_connection()`.
- Publishing an envelope whose application headers use any ASCII case variant
  of the reserved `x-hexeract-*` namespace now fails with
  `BusError::ReservedHeaderNamespace` instead of putting the header on the
  wire. Reserved keys are accepted from the wire in canonical lowercase only:
  `X-Hexeract-Request-Id` is invalid metadata, never an alias. (#448)

### Fixed

- A small payload can no longer smuggle unbounded metadata past the worker's
  resource bounds: `max_payload_bytes` never covered the AMQP field table, so
  a tiny message could still carry a large one, which the worker cloned into a
  `BusEnvelope` and handed to a typed handler. Inbound tables are now measured
  before anything is copied, counting nested arrays and tables iteratively so
  attacker-controlled nesting cannot consume the call stack, and only valid
  UTF-8 long strings reach the envelope. A delivery refused this way follows
  the existing poison path without reaching a handler, and its dead-letter copy
  rebuilds its AMQP properties with an empty field table rather than
  republishing the metadata just refused. The reply inbox applies the same
  limits through the same decoder and drops a violation before it can take a
  correlation slot. Errors and logs report a reason and sizes only, never a
  header key or value. These limits run after `lapin` has decoded a delivery,
  so they bound the client's work and complement broker ingress limits rather
  than replacing them; authenticating metadata remains out of scope (#444).
  (#448)
- The publisher self-heals after a broker outage: the connection enables lapin
  auto-recovery, so a long-lived publisher transparently reconnects and replays
  its topology instead of failing forever (#334).
- A publish issued during the reconnect window is retried once across the
  recovered channel, so a transient blip does not surface as a publish failure
  (#334).
- Connect fails fast on a permanent authentication failure (`ACCESS_REFUSED`)
  instead of hammering the broker through the whole retry budget. The
  publisher's recovery backoff is bounded so a refused or unreachable connect
  returns promptly rather than looping for minutes (#340).
- The consumer worker deliberately does not enable lapin auto-recovery: a dead
  broker ends its consumer stream so `RabbitMqWorker::run` returns a connection
  error for its supervisor to rebuild and restart, instead of blocking forever
  while lapin keeps the subscription in recovery. Auto-recovery is reserved for
  the long-lived publisher, which has no supervisor to rebuild it (#334).
- Transient requeue nacks are paced, ending the redelivery hot loop that spun
  the CPU when a retry or dead-letter publish kept failing (#336).
- In-flight `no_ack` handlers are drained at shutdown, so a cancelled
  unacknowledged consumer no longer silently loses messages that the broker had
  already removed from the queue (#338).
