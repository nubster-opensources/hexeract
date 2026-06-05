# Retry policy and dead-letter routing

In `AckMode::Manual`, the `RabbitMqWorker` retries each delivery up to `max_attempts` times before routing it to a dead-letter destination or dropping it. Retries are delayed and counted by the broker: failed deliveries sit in a durable wait queue (`<queue>.retry`) whose TTL enforces `retry_delay`, and the attempt count travels in the broker-maintained `x-death` header. This page explains the state machine, the durable accounting, and the operational caveats.

## State machine per delivery

```mermaid
stateDiagram-v2
    [*] --> Received: Delivery in<br/>(message_id, delivery_tag)
    Received --> Decoded: delivery_to_envelope OK
    Received --> Discarded: Decode failed<br/>basic_nack(requeue=false)
    Decoded --> Dispatched
    Dispatched --> Delivered: Handler Ok
    Dispatched --> Failed: Handler Err
    Failed --> Waiting: attempts < max_attempts<br/>basic_publish to wait queue<br/>basic_ack original
    Waiting --> Received: TTL expires, broker dead-letters<br/>back to the queue<br/>(x-death count += 1)
    Failed --> Parked: attempts == max_attempts<br/>+ DLR configured<br/>basic_publish to DLR<br/>basic_ack
    Failed --> Dropped: attempts == max_attempts<br/>+ no DLR<br/>basic_ack
    Delivered --> [*]
    Parked --> [*]
    Dropped --> [*]
    Discarded --> [*]
```

## Durable accounting via `x-death`

Every time the broker dead-letters a message it appends or updates an entry in the `x-death` header. The wait queue is a dead-letter hop by construction (the message expires there and is routed back to the consumed queue), so the entry whose `queue` is the wait queue and whose `reason` is `expired` counts the completed retry cycles. The worker reads that count and adds one to obtain the current attempt number; nothing is stored in process memory.

```mermaid
graph LR
    subgraph "First attempt"
        d1[x-death absent<br/>attempt = 1] -- handler Err --> w1[publish to wait queue + ack]
    end
    subgraph "Second attempt"
        d2[x-death count = 1<br/>attempt = 2] -- handler Err --> w2[publish to wait queue + ack]
    end
    subgraph "Third attempt"
        d3[x-death count = 2<br/>attempt = 3] -- handler Err --> dlr[publish to DLR + ack]
    end
    w1 -. TTL expiry .-> d2
    w2 -. TTL expiry .-> d3
```

Because the count travels with the message, it survives worker restarts and is shared by every consumer of the queue: a poison message restarted mid-retry resumes at its real attempt number instead of getting a fresh budget.

## Operational caveats

1. **Wait queue arguments are frozen at declare time.** The TTL is baked into the `<queue>.retry` arguments, so changing `retry_delay` for an existing wait queue makes the declaration fail with a broker precondition error. Delete the wait queue first (or keep the delay stable per queue).
2. **Duplicates are possible.** If the ack of the original fails after the retry copy reached the wait queue, the broker redelivers the original and both copies eventually run. This is inherent to at-least-once delivery; handlers must be idempotent.
3. **The delay is uniform per queue.** Every retry of every message on a given queue waits the same `retry_delay`; per-message TTLs are deliberately avoided because RabbitMQ only expires the head of a queue (head-of-line blocking).

## Dead-letter routing key

Set `dead_letter_routing_key` on the builder to route exhausted deliveries to the default exchange under a known routing key. The default exchange routes by routing key directly to the queue of the same name, so declaring a `dead_letter_routing_key = "orders.parked"` plus a queue named `orders.parked` is all you need.

```rust
let worker = RabbitMqWorkerBuilder::new(connection)
    .queue("orders.received")
    .max_attempts(5)
    .dead_letter_routing_key("orders.parked")
    .register_handler::<OrderPlaced, _>(MyHandler)
    .build()?;
```

When the routing key is not configured, exhausted deliveries are silently dropped (logged via `tracing::warn`).

## Roadmap notes

- **Delayed retries and persistent counters** shipped with the wait-queue mechanism described above (`Reliability` milestone).
- **Exponential backoff tiers** (multiple wait queues with increasing TTLs) are a possible extension; today the delay is a single fixed `retry_delay` per queue.
- **Per-handler retry policies** (different `max_attempts` per message type) are tracked in the `Reliability` milestone.
