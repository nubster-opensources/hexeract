# Message and envelope

Hexeract carries two parallel payload types: an [`Event`](../reference/hexeract-outbox.md) for the outbox and a [`Message`](../reference/hexeract-bus.md) for the bus. They look almost identical and share the same intent: tie a Rust struct to a stable string identifier that survives across services and language boundaries.

## Marker traits

```rust
pub trait Event: Send + Sync + 'static + Serialize + DeserializeOwned {
    const EVENT_TYPE: &'static str;
}

pub trait Message: Send + Sync + 'static + Serialize + DeserializeOwned {
    const MESSAGE_TYPE: &'static str;
}
```

Pick a stable, kebab-cased identifier scoped by bounded context: `"orders.placed"`, `"users.registered"`. Changing the value after consumers have shipped breaks dispatch on the consumer side.

## Envelopes carry the metadata

Each side wraps the user payload in an envelope. The envelope is what crosses the wire (`BusEnvelope`) or what sits on disk (`OutboxEnvelope`).

| Field | `BusEnvelope` | `OutboxEnvelope` | Role |
| --- | --- | --- | --- |
| `message_id` / `event_id` | UUIDv7, minted by the publisher | UUIDv7, minted by the publisher | Stable identifier for the unit of work |
| `message_type` / `event_type` | `M::MESSAGE_TYPE` | `E::EVENT_TYPE` | Dispatch key on the consumer side |
| `payload` | JSON bytes | JSON bytes | Serialised body of the user struct |
| `correlation_id` | UUIDv7 (required) | not applicable in v0.1.0 | Causal chain identifier |
| `reply_to` | `Option<String>` (AMQP property) | not applicable | Request-reply hint |
| `headers` | `HashMap<String, String>` | not applicable in v0.1.0 | Trace context, tenancy, custom metadata |
| `subject_id` | not applicable | `Option<Uuid>` | Aggregate identifier for partial ordering |
| `attempts` | not exposed (in-memory) | `i32` (persisted) | Retry counter |
| `next_retry_at` | not applicable | `Option<SystemTime>` | Cooldown for the next dispatch |
| `delivered_at` | not applicable | `Option<SystemTime>` | Marks successful dispatch |
| `published_at` / `created_at` | `SystemTime` | `SystemTime` | Producer-side timestamp; the bus propagates it to the AMQP `timestamp` property (epoch seconds) |

## Debug masks the payload

Both envelopes implement `Debug` by hand so the payload bytes never leak into traces or logs:

```text
BusEnvelope { message_id: ..., message_type: "orders.placed", payload: <42 bytes>, ... }
```

If you `tracing::debug!(?envelope)` an envelope, the payload appears as `<N bytes>`. To recover the typed body, call `envelope.decode::<M>()` on the bus side or `envelope.decode::<E>()` on the outbox side. The decoder validates the `message_type` / `event_type` matches the requested generic and returns `TypeMismatch` otherwise.

## Constructors

| API | Constructor | Notes |
| --- | --- | --- |
| Outbox publish | `OutboxPublisher::publish_in_tx` | Mints `event_id`, serialises, INSERTs in the caller's transaction |
| Outbox restore (backends) | `OutboxEnvelope::restore(...)` | Used by store implementations rebuilding a row read from the database |
| Bus publish | `Transport::publish` / `publish_with_headers` / `publish_with_correlation_id` | Mints `message_id`, builds the envelope, dispatches to the broker driver |
| Bus restore (backends) | `BusEnvelope::restore(...)` | Used by consumer code that materialises a broker delivery |

`restore` is the seam any backend uses to ressurect an envelope from broker properties or a database row. It bypasses payload validation; the resulting envelope is expected to come from a trusted source.
