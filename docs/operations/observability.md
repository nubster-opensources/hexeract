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
| `swept schedules whose attempt budget was exhausted by a crashed worker` | `hexeract-scheduler` | `error` | `count` | `dead_letter_exhausted` found at least one row left `Pending` with its attempt budget exhausted and no active lease, at the start of a poll cycle |
| `scheduler claimed due occurrences` | `hexeract-scheduler` | `debug` | `claimed` | A poll cycle claimed at least one due occurrence |
| `lease already expired before dispatch, skipping` | `hexeract-scheduler` | `warn` | `schedule_id` | The claimed occurrence's lease had already elapsed by the time the worker was about to dispatch it; the worker skips it and lets it expire back to claimable |
| `scheduled occurrence dispatched` | `hexeract-scheduler` | `debug` | `schedule_id`, `trigger`, `lag_ms` | The sink returned `Ok` for an occurrence |
| `scheduled occurrence rescheduled` | `hexeract-scheduler` | `debug` | `schedule_id`, `trigger` | A cron schedule was advanced to its next occurrence after a successful dispatch |
| `scheduled occurrence retried` | `hexeract-scheduler` | `warn` | `schedule_id`, `attempts`, `error` | A dispatch failed and the attempt budget is not yet exhausted |
| `scheduled occurrence dead-lettered` | `hexeract-scheduler` | `error` | `schedule_id`, `attempts`, `error` | A dispatch failed and the attempt budget is exhausted |
| `lease lost; occurrence settled by another worker` | `hexeract-scheduler` | `warn` | `schedule_id` | An acknowledgement (`mark_delivered`, `reschedule`, `mark_failed` or `mark_dead_lettered`) found the occurrence's lease no longer matched, meaning another worker already reclaimed and settled it |

The scheduler also emits two spans: `scheduler.tick` (one per poll cycle, carries the `claimed` field) and `scheduler.dispatch` (one per settled occurrence, carries `schedule_id`, `trigger`, `attempt` and `lag_ms`). The `lag_ms` field measures the duration between the occurrence's `scheduled_for` time and the moment the worker picks it up. Sustained growth of `lag_ms` is the primary signal that the worker is falling behind the dispatch rate.

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

Hexeract does not export Prometheus metrics natively (planned for a future release). Until then, instrument the call sites where Hexeract hands control back to your code.

| Metric | Where to measure | Useful labels |
| --- | --- | --- |
| `outbox.publish.duration_seconds` | Around `OutboxPublisher::publish_in_tx` | `event_type` |
| `outbox.dispatch.duration_seconds` | Inside the handler `handle` body | `event_type`, `outcome` (`ok` / `err`) |
| `outbox.pending.gauge` | Periodic `SELECT count(*) FROM <table> WHERE delivered_at IS NULL` | `table` |
| `bus.publish.duration_seconds` | Around `Transport::publish_*` | `routing_key` |
| `bus.dispatch.duration_seconds` | Inside the handler `handle` body | `message_type`, `outcome` |
| `bus.retry.counter` | Increment on `tracing::warn` parsing or via a custom field visitor | `message_type` |
| `bus.dlr.counter` | Increment when the worker publishes to the dead-letter routing key | `message_type` |

### Counters Hexeract exposes directly

Request-reply is the exception to the section above: both ends of a call keep
in-process counters, so the failures each end cannot see from the other need no
instrumentation of your own, only a periodic read.

| Snapshot field | Read from | Counts |
| --- | --- | --- |
| `ReplyCountersSnapshot.invalid` | `RequestRegistry::counters()` | Replies whose request identity was known but which failed validation before the registry consumed the pending slot |
| `ReplyCountersSnapshot.orphaned` | `RequestRegistry::counters()` | Replies with an absent, unparsable or unknown identity, a second reply to an already resolved call included |
| `ResponderCountersSnapshot.invalid_reply_to` | a retained `ResponderCounters` handle | Requests refused for an absent or policy-rejected reply destination |
| `ResponderCountersSnapshot.invalid_request_id` | idem | Requests with an absent or unparsable `x-hexeract-request-id` |
| `ResponderCountersSnapshot.unsupported_protocol_version` | idem | Requests announcing a missing or unsupported protocol version |

The caller-side snapshot comes from the registry the client already owns. The
responder-side handle is supplied at registration instead, because the worker
erases the handler it wraps, so keep a clone of it:

```rust
let counters = ResponderCounters::default();
let worker = RabbitMqWorkerBuilder::new(connection)
    .queue("orders.requests")
    .register_request_handler_with_counters::<PlaceOrder, _>(
        handler,
        Arc::clone(&transport),
        counters.clone(),
    )
    .build()?;

// from your metrics loop, on the clone you kept
let snapshot = counters.snapshot();
report_gauge("rpc.responder.invalid_reply_to", snapshot.invalid_reply_to);
```

Read them as rates rather than as totals. They are monotonic for the life of
the process, and they count rejections rather than distinct requests: a
delivery the transport redelivers is counted again. They also merge
sub-reasons that share one remedy, so when you need to tell a missing header
from an unreadable one, the `warn` event on the same path names the sub-reason
and the message type.

These are the drops nothing else reports. A request refused on its envelope
never reaches a handler and never produces a reply, so the caller observes
nothing but a timeout, and a reply refused on arrival resolves no call at all.

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

OpenTelemetry span coverage is planned for a future release. Today the recommended setup is:

1. Add `tracing-opentelemetry` to your application crate.
2. Wrap your handler bodies in a span: `let _span = tracing::info_span!("handle", message_type, ...).entered();`.
3. Propagate the W3C `traceparent` through the bus headers (set on `publish_with_headers`, read on the consumer side before entering the span).

When that future release ships, Hexeract will instrument its own internal spans (publish, consume, dispatch) so this manual layer is no longer needed.
