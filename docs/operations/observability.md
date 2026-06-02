# Observability

Hexeract emits `tracing` events on every poll cycle, dispatch, retry and ack. This page documents the events you can hook into and the metrics you should layer on top.

## Tracing events

| Event | Crate | Level | Fields | Triggered when |
| --- | --- | --- | --- | --- |
| `outbox handler dispatch failed` | `hexeract-outbox` | `warn` | `event_id`, `event_type`, `error` | Handler returned `Err` in a poll cycle |
| `outbox poll cycle error` | `hexeract-outbox` | `error` | `error` | The store returned an error during the poll cycle |
| `dispatching outbox envelope` | `hexeract-outbox` | `debug` | `event_id`, `event_type` | About to invoke the handler |
| `rabbitmq connect failed` | `hexeract-bus-rabbitmq` | `warn` | `attempt`, `error` | A `connect_with_retry` attempt failed |
| `rabbitmq consumer stream error` | `hexeract-bus-rabbitmq` | `warn` | `error` | The lapin consumer stream surfaced an error |
| `rabbitmq delivery decode failed` | `hexeract-bus-rabbitmq` | `warn` | `error` | `delivery_to_envelope` returned `Err`, delivery `basic_nack`-ed without requeue |
| `handler failed under AckMode::AckOnReceive, delivery already acked` | `hexeract-bus-rabbitmq` | `warn` | `message_type`, `error` | Handler returned `Err` under `AckMode::AckOnReceive` (delivery acked before the handler ran) |
| `handler failed under AckMode::Unacknowledged (no_ack), message already gone` | `hexeract-bus-rabbitmq` | `warn` | `message_type`, `error` | Handler returned `Err` under `AckMode::Unacknowledged` (broker removed the message on delivery) |
| `handler failed` | `hexeract-bus-rabbitmq` | `warn` | `message_type`, `attempt`, `max_attempts`, `error` | Handler returned `Err` in `AckMode::Manual`, before the nack/DLR decision |
| `delivery dropped after exhausting retry budget` | `hexeract-bus-rabbitmq` | `warn` | `message_type`, `attempts` | `max_attempts` reached with no DLR configured |
| `rabbitmq worker cancelled` | `hexeract-bus-rabbitmq` | `info` | `queue` | The `CancellationToken` fired and the consume loop is exiting |

## Recommended subscriber

```rust
use tracing_subscriber::EnvFilter;

tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
    .json()
    .init();
```

Override at runtime with `RUST_LOG=hexeract_bus_rabbitmq=debug,hexeract_outbox=debug,info` to inspect a specific feature without flooding the rest of the application.

## Metrics to derive

Hexeract does not export Prometheus metrics natively in v0.2.0 (planned for v0.10.0). Until then, instrument the call sites where Hexeract hands control back to your code.

| Metric | Where to measure | Useful labels |
| --- | --- | --- |
| `outbox.publish.duration_seconds` | Around `OutboxPublisher::publish_in_tx` | `event_type` |
| `outbox.dispatch.duration_seconds` | Inside the handler `handle` body | `event_type`, `outcome` (`ok` / `err`) |
| `outbox.pending.gauge` | Periodic `SELECT count(*) FROM <table> WHERE delivered_at IS NULL` | `table` |
| `bus.publish.duration_seconds` | Around `Transport::publish_*` | `routing_key` |
| `bus.dispatch.duration_seconds` | Inside the handler `handle` body | `message_type`, `outcome` |
| `bus.retry.counter` | Increment on `tracing::warn` parsing or via a custom field visitor | `message_type` |
| `bus.dlr.counter` | Increment when the worker publishes to the dead-letter routing key | `message_type` |

## Correlation across services

`OutboxEnvelope.event_id`, `BusEnvelope.message_id` and `BusEnvelope.correlation_id` are UUIDv7 by construction (lexically sortable by mint timestamp). Log every one of them at every hop:

```rust
tracing::info!(
    %message_id,
    correlation_id = %ctx.correlation_id,
    "consuming"
);
```

A single grep across log streams reconstructs the chain. For automated propagation, see [correlation ID](../concepts/correlation-id.md).

## OpenTelemetry

OpenTelemetry span coverage is a v0.10.0 milestone item. Today the recommended setup is:

1. Add `tracing-opentelemetry` to your application crate.
2. Wrap your handler bodies in a span: `let _span = tracing::info_span!("handle", message_type, ...).entered();`.
3. Propagate the W3C `traceparent` through the bus headers (set on `publish_with_headers`, read on the consumer side before entering the span).

When v0.10.0 ships, Hexeract will instrument its own internal spans (publish, consume, dispatch) so this manual layer is no longer needed.
