# Bus flow

The bus carries messages between processes through a broker (RabbitMQ in v0.2.0). Producers serialize a typed [`Message`](../concepts/message-envelope.md) into a [`BusEnvelope`](../concepts/message-envelope.md), publish through a [`Transport`](../reference/hexeract-bus.md), and consumers receive deliveries through a [`Worker`](../concepts/worker.md) that dispatches to a typed [`Handler`](../concepts/worker.md).

## Publish then consume

```mermaid
sequenceDiagram
    autonumber
    participant App as Producer service
    participant Tx as RabbitMqTransport
    participant Pool as ChannelPool
    participant Broker as RabbitMQ broker
    participant Worker as RabbitMqWorker
    participant Handler as Handler<M>

    App->>Tx: publish_with_correlation_id(rk, cid, &msg)
    Tx->>Tx: BusEnvelope::new(cid, &msg)
    Tx->>Pool: acquire()
    Pool-->>Tx: PooledChannel
    Tx->>Broker: basic_publish(exchange, rk, props, payload)
    Broker-->>Tx: publisher confirm
    Tx-->>App: Ok(message_id)

    Note over Broker,Worker: Broker routes per binding<br/>and prefetch
    Broker->>Worker: Delivery (props + payload)
    Worker->>Worker: delivery_to_envelope(props, data)
    Worker->>Worker: build_handler_context(props)
    Worker->>Handler: ErasedHandler::handle(envelope, ctx)
    Handler-->>Worker: Result<(), HandlerError>

    alt Handler Ok
        Worker->>Broker: basic_ack(delivery_tag)
    else Handler Err & attempts < max
        Worker->>Broker: basic_publish(wait queue, payload, props)
        Worker->>Broker: basic_ack(delivery_tag)
        Note over Broker: Wait queue TTL expires, the broker<br/>dead-letters the message back to the queue<br/>and increments x-death
    else Handler Err & attempts == max
        Worker->>Broker: basic_publish(dead-letter queue,<br/>mandatory, persistent, payload)
        Broker-->>Worker: publisher confirm
        Worker->>Broker: basic_ack(delivery_tag)
    end
```

## AckMode decision

A [`RabbitMqWorker`](../concepts/worker.md) reacts to handler failures differently depending on its [`AckMode`](../concepts/ack-modes.md).

```mermaid
flowchart TD
    delivery([Delivery received])
    decode{Decode<br/>envelope?}
    ack_mode{AckMode?}
    dispatch[/Dispatch to handler/]
    handler_ok{Handler<br/>Ok?}
    attempts{x-death + 1<br/>< max?}
    dlr{DLR<br/>configured?}
    ack[basic_ack]
    publish_wait[basic_publish<br/>to wait queue + ack]
    publish_dlr[confirmed basic_publish<br/>to DLQ + ack]
    drop[ack & drop]

    delivery --> decode
    decode -- No --> nack_drop[basic_nack<br/>requeue=false]
    decode -- Yes --> ack_mode
    ack_mode -- AckOnReceive/Unacknowledged --> dispatch_auto[/Dispatch to handler/]
    ack_mode -- Manual --> dispatch
    dispatch_auto -. already settled .-> ignore_outcome([Log on error])

    dispatch --> handler_ok
    handler_ok -- Yes --> ack
    handler_ok -- No --> attempts
    attempts -- Yes --> publish_wait
    publish_wait -. TTL expiry, broker<br/>dead-letters back .-> delivery
    attempts -- No --> dlr
    dlr -- Yes --> publish_dlr
    dlr -- No --> drop
```

## Where each step lives

| Step | Code |
| --- | --- |
| Envelope construction | `BusEnvelope::new` / `with_headers` |
| Channel acquisition | `ChannelPool::acquire` |
| Publish + confirm | `RabbitMqTransport::publish_envelope` |
| Delivery decode | `worker::delivery_to_envelope` |
| Handler context build | `worker::build_handler_context` |
| Dispatch | `ErasedHandler::handle` (via `TypedHandler<M, H>`) |
| Retry accounting | `x-death` header read by `worker::death_count` |
| Retry scheduling | `RabbitMqWorker::schedule_retry` (wait queue declared at startup) |
| Dead-letter routing | `RabbitMqWorker::handle_exhausted` (queue declared at startup, confirmed publish) |

For the full retry state machine and the durable accounting via `x-death`, see the [retry policy](../concepts/retry-policy.md).
