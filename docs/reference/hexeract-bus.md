# `hexeract-bus` API reference

Backend-agnostic core of the bus feature. Use it directly to write a custom transport or worker on top of a broker we do not yet ship. For RabbitMQ, pair it with [`hexeract-bus-rabbitmq`](hexeract-bus-rabbitmq.md).

The full rustdoc lives at <https://docs.rs/hexeract-bus>.

## Public surface

### Marker traits

| Item | Role |
| --- | --- |
| `Message` | Marker for any domain message flowing through the bus. Implementors define a stable `MESSAGE_TYPE: &'static str` used for routing. |

### Envelope and error

| Item | Role |
| --- | --- |
| `BusEnvelope` | In-flight representation of a message. Holds `message_id` (UUIDv7), `message_type`, JSON payload, `correlation_id`, optional `reply_to`, free-form `headers`, `published_at`. `Debug` masks the payload. |
| `BusEnvelope::new(correlation_id, &M)` | Builds a fresh envelope; mints `message_id`. |
| `BusEnvelope::with_headers(correlation_id, headers, &M)` | Builds a fresh envelope with custom headers. |
| `BusEnvelope::with_reply_to(correlation_id, reply_to, &M)` | Builds a fresh envelope with a reply queue. |
| `BusEnvelope::restore(...)` | Backend hook to rebuild an envelope from broker properties. |
| `BusEnvelope::decode::<M>()` | Deserialises the payload and validates the `message_type` matches `M::MESSAGE_TYPE`. |
| `BusError` | Non-exhaustive error enum: `Serialization`, `Transport(Box<...>)`, `Connection(Box<...>)`, `MissingHandler { message_type }`, `TypeMismatch { expected, actual }`, `InvalidTopology { reason }`, `Internal(String)`. |

### Topology

| Item | Role |
| --- | --- |
| `Exchange { name, kind, durable, auto_delete }` | Typed exchange declaration. Constructed via `Exchange::new(name, ExchangeKind)`. |
| `ExchangeKind` | `Direct`, `Topic`, `Fanout`, `Headers` (`#[non_exhaustive]`). |
| `Queue { name, durable, exclusive, auto_delete }` | Typed queue declaration. Constructed via `Queue::new(name)`. |
| `Binding { queue, exchange, routing_key }` | Typed binding. Constructed via `Binding::new(queue, exchange, routing_key)`. |
| `RoutingKey` | Newtype around `String`. Validated on construction and on `serde` deserialization via `try_from = "String"`. |
| `MAX_NAME_LEN` | `127` bytes, matching AMQP 0.9.1. |
| `MAX_ROUTING_KEY_LEN` | `255` bytes. |

### Transport contract

```rust
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn publish<M: Message>(
        &self,
        routing_key: &str,
        message: &M,
    ) -> Result<Uuid, BusError>;

    async fn publish_with_headers<M: Message>(
        &self,
        routing_key: &str,
        headers: HashMap<String, String>,
        message: &M,
    ) -> Result<Uuid, BusError>;

    async fn publish_with_correlation_id<M: Message>(
        &self,
        routing_key: &str,
        correlation_id: Uuid,
        message: &M,
    ) -> Result<Uuid, BusError>;
}
```

Return value is the freshly minted `message_id` (UUIDv7) of the outgoing envelope. See [correlation ID](../concepts/correlation-id.md) for picking between the three methods.

### Handler contract

```rust
#[trait_variant::make(Send)]
pub trait Handler<M: Message>: Send + Sync + 'static {
    type Error: Into<BusError> + Send + Sync + 'static;

    async fn handle(&self, message: M, ctx: &HandlerContext) -> Result<(), Self::Error>;
}
```

Symmetric with `hexeract_outbox::Handler<E>`. Pairs with `TypedHandler<M, H>` (the adapter) and `ErasedHandler` (the dyn-safe form the worker dispatches against).

### Re-exports

`hexeract_bus` re-exports `BusEnvelope`, `BusError`, `Message`, `Transport`, `Handler`, `ErasedHandler`, `TypedHandler`, `BoxFuture`, plus the topology types `Exchange`, `ExchangeKind`, `Queue`, `Binding`, `RoutingKey` at the crate root.

## Where to read next

- [Bus quick start](../getting-started/bus-quick-start.md)
- [Bus flow architecture](../architecture/bus-flow.md)
- [Topology concept](../concepts/topology.md)
- [Correlation ID concept](../concepts/correlation-id.md)
