# Retry policy and dead-letter routing

In `AckMode::Manual`, the `RabbitMqWorker` retries each delivery up to `max_attempts` times before routing it to a dead-letter destination or dropping it. Retries are delayed by the broker and counted by the worker: failed deliveries sit in a durable wait queue (`<queue>.retry`) whose TTL enforces `retry_delay`, and the attempt count travels in the `x-hexeract-retry-attempt` header the worker stamps on every retry copy, with the broker `x-death` header as a fallback. This page explains the state machine, the durable accounting, and the operational caveats.

## State machine per delivery

```mermaid
stateDiagram-v2
    [*] --> Received: Delivery in<br/>(message_id, delivery_tag)
    Received --> Decoded: delivery_to_envelope OK
    Received --> Parked: Decode failed (oversize payload,<br/>missing AMQP type) + DLR configured<br/>confirmed basic_publish + basic_ack
    Received --> Discarded: Decode failed<br/>+ no DLR<br/>basic_nack(requeue=false)
    Decoded --> Dispatched
    Dispatched --> Delivered: Handler Ok
    Dispatched --> Failed: Handler Err
    Failed --> Waiting: attempts < max_attempts<br/>confirmed mandatory basic_publish to wait queue<br/>basic_ack original
    Waiting --> Received: TTL expires, broker dead-letters<br/>back to the queue<br/>(retry header kept)
    Failed --> Parked: attempts == max_attempts<br/>+ DLR configured<br/>basic_publish to DLR<br/>basic_ack
    Failed --> Dropped: attempts == max_attempts<br/>+ no DLR<br/>basic_ack
    Delivered --> [*]
    Parked --> [*]
    Dropped --> [*]
    Discarded --> [*]
```

## Durable accounting via the retry header

Every time the worker republishes a failed delivery to the wait queue it writes the number of attempts that have failed so far into the `x-hexeract-retry-attempt` header (`worker::RETRY_ATTEMPT_HEADER`), as an AMQP `long-long-int`. The count is read back as the larger of that header and the `x-death` count: every time the broker dead-letters a message it appends or updates an entry in `x-death`, and the entry whose `queue` is the wait queue and whose `reason` is `expired` counts the completed retry cycles. The worker adds one to the larger of the two to obtain the current attempt number; nothing is stored in process memory.

The worker owns its own counter because RabbitMQ 4.x, and 3.13.0 to 3.13.2, no longer interpret the `x-death` header a client republishes: a republished message starts from a fresh history, so `x-death` alone never passes 1 and `max_attempts` would never run out. Taking the larger of the two values also keeps a message that was waiting in the wait queue during an upgrade at its real attempt number. Both values are clamped, so a forged count can shorten the budget of its own message but never lengthen it.

```mermaid
graph LR
    subgraph "First attempt"
        d1[no retry header<br/>attempt = 1] -- handler Err --> w1[publish to wait queue + ack]
    end
    subgraph "Second attempt"
        d2[retry header = 1<br/>attempt = 2] -- handler Err --> w2[publish to wait queue + ack]
    end
    subgraph "Third attempt"
        d3[retry header = 2<br/>attempt = 3] -- handler Err --> dlr[publish to DLR + ack]
    end
    w1 -. TTL expiry .-> d2
    w2 -. TTL expiry .-> d3
```

Because the count travels with the message, it survives worker restarts and is shared by every consumer of the queue: a poison message restarted mid-retry resumes at its real attempt number instead of getting a fresh budget.

## Operational caveats

1. **Wait queue arguments are frozen at declare time.** The TTL is baked into the `<queue>.retry` arguments, so changing `retry_delay` for an existing wait queue makes the declaration fail with a broker precondition error. Delete the wait queue first (or keep the delay stable per queue).
2. **Duplicates are possible.** If the ack of the original fails after the retry copy reached the wait queue, the broker redelivers the original and both copies eventually run. This is inherent to at-least-once delivery; handlers must be idempotent.
3. **The delay is uniform per queue.** Every retry of every message on a given queue waits the same `retry_delay`; per-message TTLs are deliberately avoided because RabbitMQ only expires the head of a queue (head-of-line blocking).
4. **A replayed dead-letter keeps its header.** The copy parked in the dead-letter queue still carries `x-hexeract-retry-attempt`, so a message replayed from it gets a single attempt: its first failure parks it again. Remove the header (`worker::RETRY_ATTEMPT_HEADER`) before replaying to give the message a fresh budget.

## Dead-letter queue

Set `dead_letter_routing_key` on the builder to route exhausted deliveries to a durable dead-letter queue of that name. The worker declares the queue at startup, so the routing key always has a bound queue on the default exchange and an exhausted delivery can never be silently unroutable.

```rust
let worker = RabbitMqWorkerBuilder::new(connection)
    .queue("orders.received")
    .max_attempts(5)
    .dead_letter_routing_key("orders.parked")
    .register_handler::<OrderPlaced, _>(MyHandler)
    .build()?;
```

The dead-letter publish is hardened end to end: the worker channel runs with publisher confirms (enabled whenever a dead-letter routing key is set, regardless of ack mode, and always under `Manual`), the publish is `mandatory`, and the copy is forced to persistent delivery (`delivery_mode` 2) so it survives a broker restart. Success requires a broker ack without a returned message. Any other outcome (a nack, an unroutable return, a missing confirm) means the copy did not land, so the worker nacks the original to free its prefetch slot rather than leaving it unsettled: a transient transport failure requeues the delivery for another attempt, while an unroutable dead-letter queue drops it. Leaving the delivery unacked on a live channel would not trigger redelivery (brokers only redeliver after the channel or connection closes) and would silently consume a prefetch slot until the consumer stalled.

The same hardened publish parks poison deliveries: a payload larger than `max_payload_bytes` or a delivery missing the AMQP `type` property never reaches a handler, skips the retry budget entirely, and is routed straight to the dead-letter queue when one is configured. Without a dead-letter queue, a `Manual` consume nacks the poison delivery without requeue, so a broker-level dead-letter exchange configured on the queue still receives it.

Two operational notes:

1. **Queue arguments must match.** The worker declares the dead-letter queue as a plain durable queue. If a queue of the same name already exists with different arguments, the declaration fails with a broker precondition error at startup; align or delete the pre-existing queue.
2. **Duplicates are possible.** As with retries, if the final ack fails after the dead-letter copy was confirmed, the broker redelivers the original and a second copy can reach the dead-letter queue. Consumers of the dead-letter queue must tolerate duplicates.

When the routing key is not configured, exhausted deliveries are dropped with a `tracing::warn` log.

## Roadmap notes

- **Delayed retries and persistent counters** shipped with the wait-queue mechanism described above (`Reliability` milestone).
- **Exponential backoff tiers** (multiple wait queues with increasing TTLs) are a possible extension; today the delay is a single fixed `retry_delay` per queue.
- **Per-handler retry policies** (different `max_attempts` per message type) are tracked in the `Reliability` milestone.
