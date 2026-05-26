# Worker lifecycle

Hexeract ships two workers in v0.2.0: [`OutboxWorker`](../reference/hexeract-outbox.md) for the database-backed outbox and [`RabbitMqWorker`](../reference/hexeract-bus-rabbitmq.md) for the AMQP bus. They are intentionally symmetric: same fluent builder shape, same `run(cancel)` entry point, same handler dispatch via `ErasedHandler`.

## Generic shape

```mermaid
flowchart LR
    builder["WorkerBuilder<br/>register_handler::<M, _>(handler)<br/>tuning knobs<br/>build()"]
    worker["Worker"]
    run["run(cancel: CancellationToken)"]
    handlers["HashMap<&'static str, Arc<dyn ErasedHandler>>"]
    loop["Polling / consume loop"]

    builder --> worker
    worker --> run
    worker --> handlers
    run --> loop
    loop --"per envelope"--> dispatch["ErasedHandler::handle"]
    dispatch --> typed["TypedHandler<M, H>"]
    typed --"decode payload"--> handler["Handler<M>::handle"]
```

## Spawn pattern

```rust
let cancel = CancellationToken::new();
let join = tokio::spawn(worker.run(cancel.clone()));

// ... business code emits events / messages ...

cancel.cancel();
join.await??;
```

Both workers honour the `CancellationToken`: `OutboxWorker` checks between poll cycles, `RabbitMqWorker` selects on `consumer.next()` and the cancel signal. A cancelled worker drains the in-flight envelope (if any) and returns `Ok(())`.

## Outbox-specific timing

`OutboxWorker` is a poll loop. Two knobs drive its rhythm:

- `poll_interval` (default `100 ms`): sleep duration when a poll returned no rows.
- `batch_size` (default `10`): maximum rows fetched per poll.

A non-empty poll runs back-to-back without sleeping, so a backlog drains as fast as the handler can process. An empty poll sleeps `poll_interval` and tries again.

## Bus-specific timing

`RabbitMqWorker` is push-based: it calls `basic_consume` and reacts to deliveries the broker pushes. Two knobs:

- `prefetch` (default `16`): how many unacknowledged deliveries the broker may have in flight at once.
- `max_attempts` (default `5`): retry budget per `message_id` before the delivery is parked or dropped (see [retry policy](retry-policy.md)).

## ErasedHandler and TypedHandler

The worker keeps handlers in a `HashMap<&'static str, Arc<dyn ErasedHandler>>` keyed by `MESSAGE_TYPE` / `EVENT_TYPE`. The user-facing trait is the typed `Handler<M>`; `TypedHandler<M, H>` is the adapter that translates from the dyn-safe `ErasedHandler::handle(&envelope, &ctx) -> BoxFuture<Result<(), BusError>>` to the typed `H::handle(message, &ctx) -> Result<(), H::Error>`.

The decoding step (`envelope.decode::<M>()`) lives in `TypedHandler`, so the worker's dispatch loop never needs to know the concrete message type. If the inbound envelope carries a `message_type` no handler registered for, the dispatch returns `BusError::MissingHandler { message_type }`, which the worker logs and (in `AckMode::Manual`) treats as a handler failure subject to the retry policy.

## Idempotency expectations

| Worker | Delivery semantics | Idempotency requirement |
| --- | --- | --- |
| `OutboxWorker` | At-least-once; a crashed worker releases its `SELECT ... FOR UPDATE` lock and another worker picks the envelope up. | Required. Same `event_id` may invoke the handler more than once. |
| `RabbitMqWorker` | At-least-once; redeliveries on failure, and broker reconnects can replay messages. | Required. Same `message_id` may invoke the handler more than once. |

Idempotency is not optional. The recommended pattern is to write the side effect plus a `processed_message_id` row in the same database transaction, then short-circuit on the second delivery when the `processed_message_id` is already present.

## Graceful shutdown

Both workers honour cooperative cancellation:

1. Caller flips the `CancellationToken`.
2. `OutboxWorker` finishes the current poll cycle (commits the transaction or rolls back) and exits the loop.
3. `RabbitMqWorker` lets the in-flight delivery resolve through the handler, sends the ack or nack, then exits the consume loop.
4. `run` returns `Ok(())` and the `JoinHandle` resolves.

A worker that crashes (panic or unwrap) bubbles the panic to the `JoinHandle`. Wrap your handler logic in `Result::Err` mapping rather than panicking.
