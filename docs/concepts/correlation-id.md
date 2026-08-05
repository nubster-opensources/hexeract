# Correlation ID propagation

Hexeract distinguishes the `message_id` (unique per outgoing message) from the `correlation_id` (shared by every message belonging to the same causal chain). The bus carries the `correlation_id` as a first-class AMQP property and exposes it on both sides of the wire.

## End-to-end propagation

```mermaid
sequenceDiagram
    autonumber
    participant Caller as Inbound handler
    participant Tx as Transport
    participant Broker as RabbitMQ
    participant Worker as RabbitMqWorker
    participant Handler as Downstream handler

    Note over Caller: Inbound message arrives<br/>HandlerContext carries cid = X
    Caller->>Tx: publish_with_correlation_id(rk, X, &message)
    Tx->>Tx: BusEnvelope::new(cid = X, &message)
    Tx->>Broker: basic_publish(props.correlation_id = X)
    Broker->>Worker: Delivery (props.correlation_id = X)
    Worker->>Worker: build_handler_context(props)
    Worker->>Handler: handle(message, ctx { correlation_id = X })
    Handler-->>Worker: Ok(())
    Note over Caller,Handler: Same correlation_id flows<br/>across processes
```

## API surface

The `Transport` trait exposes three publish methods. Each tells the broker something different about how the `correlation_id` is sourced:

| Method | `correlation_id` source |
| --- | --- |
| `publish(routing_key, &M)` | Minted by the transport (`Uuid::now_v7()`). Use for the root message of a new chain. |
| `publish_with_headers(routing_key, headers, &M)` | Minted by the transport. Use when the caller needs to attach W3C trace headers or tenancy alongside. |
| `publish_with_correlation_id(routing_key, correlation_id, &M)` | Supplied by the caller. Use to continue an existing causal chain. |

On the consumer side, every `Handler<M>::handle` receives a `HandlerContext` whose `correlation_id` field reflects the `BasicProperties.correlation_id` of the inbound AMQP delivery (or a fresh UUIDv7 if the property is absent).

```rust
impl Handler<OrderPlaced> for Projector {
    type Error = BusError;

    async fn handle(&self, msg: OrderPlaced, ctx: &HandlerContext) -> Result<(), Self::Error> {
        // Forward to a downstream service while keeping the same chain.
        self.downstream
            .publish_with_correlation_id("audit.events", ctx.correlation_id.as_uuid(), &msg)
            .await?;
        Ok(())
    }
}
```

## Distinction between `message_id` and `correlation_id`

| Field | Lifetime | Cardinality | Mint policy |
| --- | --- | --- | --- |
| `message_id` | One per outgoing message | Strict 1-to-1 with a publish call | Always minted server-side (UUIDv7) by the transport |
| `correlation_id` | One per causal chain | 1-to-many: the same `correlation_id` spans every publish in the chain | Caller-supplied when continuing a chain, else minted by the transport |

A `correlation_id` is a means to ask "which inbound request triggered this work?" across an arbitrary number of hops. The `message_id` answers "which specific publish are we talking about?".

## Where the value travels

The `correlation_id` rides through the AMQP property of the same name. The bus serialises it as a UUID string (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) and consumers parse it back through `Uuid::parse_str`. If the parse fails (a non-UUID `correlation_id` produced by another framework), the worker falls back to a fresh UUIDv7 so the chain is broken but the handler still runs.

## Correlation identity versus request identity

Request-reply layers a second identifier on top of `correlation_id`: the reserved header `x-hexeract-request-id`, minted fresh by `RequestClient` on every call. The two answer different questions and must never be confused:

- `correlation_id` labels a causal chain and is shared by every message that chain contains, request-reply calls included. `RequestClient::request` mints a fresh `correlation_id` for its request, the same way `publish` does for a plain message; `RequestClient::request_with` continues an existing chain instead when passed `RequestOptions::new().with_correlation_id(ctx.correlation_id)`, exactly like `publish_with_correlation_id`.
- `request_id` identifies exactly one request-reply call and is never shared, not even by two concurrent calls on the same `correlation_id`. `RequestRegistry` keys its in-flight slots on `request_id` for precisely this reason: keying on `correlation_id` would let two concurrent replies on the same chain cross into the wrong caller.

Two concurrent calls sharing a `correlation_id` therefore still mint distinct `request_id`s. Each is routed to its own waiting slot regardless of which reply arrives first, and two calls issued with no `correlation_id` override never share one.

See [RPC protocol](../architecture/rpc-protocol.md) for the full wire contract, and [Request-reply](request-reply.md) for the pattern this identifier pair supports.

## Tracing integration (v0.10.0)

Full OpenTelemetry span coverage lands in v0.10.0. Until then, the recommended setup is:

1. Carry your W3C `traceparent` in the `headers` map (alongside the `correlation_id` AMQP property).
2. On the consumer side, read the header in your handler and attach it to the local span context.
3. When re-emitting downstream, use `publish_with_headers` to forward the trace header verbatim, and `publish_with_correlation_id` to forward the causal chain identifier.
