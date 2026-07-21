# Changelog

All notable changes to this crate are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Exclusive, auto-delete, server-named reply inbox (`declare_reply_inbox` / `run_reply_inbox`) consumed on a connection distinct from the auto-recovering publisher, routing every delivery to a `hexeract_bus::CorrelationRegistry` by correlation id. (#400)
- `connect_request_client`, assembling a ready-to-use `RequestClient<RabbitMqTransport>`: an auto-recovering publisher transport plus a supervised reply inbox whose background task drains in-flight requests, reconnects and re-declares a fresh inbox on broker loss. (#400)
- `RabbitMqWorkerBuilder::register_request_handler`, registering a `RequestHandler` on the worker so an incoming request is dispatched and its reply, or an encoded error, published back automatically via `RepliedHandler`. (#401)

### Changed

- Breaking: `BusError::Connection` is now a struct variant `{ source, retryable }`.
  Build it with `BusError::connection(source, retryable)` and read the transience
  classification with `BusError::is_retryable_connection()`.

### Fixed

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
