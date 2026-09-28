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
| `correlation_id` | UUIDv7 (required) | not applicable | Causal chain identifier |
| `reply_to` | `Option<String>` (AMQP property) | not applicable | Request-reply hint |
| `headers` | `HashMap<String, String>` | not applicable | Trace context, tenancy, custom metadata; never printed by `Debug`, neither names nor values |
| `subject_id` | not applicable | `Option<Uuid>` | Aggregate identifier for partial ordering |
| `attempts` | not exposed (in-memory) | `i32` (persisted) | Retry counter |
| `next_retry_at` | not applicable | `Option<SystemTime>` | Cooldown for the next dispatch |
| `delivered_at` | not applicable | `Option<SystemTime>` | Marks successful dispatch |
| `published_at` / `created_at` | `SystemTime` | `SystemTime` | Producer-side timestamp; the bus propagates it to the AMQP `timestamp` property (epoch seconds) |

## Debug redacts the payload and every header value

Both envelopes implement `Debug` by hand so the payload bytes never leak into traces or logs. On `BusEnvelope`, no header value is printed either, application or protocol:

```text
BusEnvelope { message_id: 018f..., message_type: "orders.placed", payload: <42 bytes>,
  correlation_id: 018f..., reply_to: Some("q.replies"), headers: <2 redacted>,
  protocol_headers: [x-hexeract-request-id, x-hexeract-signature, <1 redacted>],
  published_at: SystemTime { .. } }
```

Read that output as follows:

- `payload: <42 bytes>` is the serialised body, masked.
- `headers: <2 redacted>` is the application header map: its size, and nothing else. The names are withheld too, because a name built from a tenant or a subject carries the datum itself and the framework cannot tell such a name apart from a schema name.
- `protocol_headers: [...]` names the framework headers the envelope carries, so you can see that it is signed, that it announces a request identity or that it carries a deadline, without any of their values. Each name is printed from the framework constant, never from the spelling read off the wire, so a hostile peer cannot push an arbitrary string into your log line. A reserved name this version does not know is counted as `<N redacted>` at the end of the list.
- `reply_to` stays readable: it is a routing key the broker already records in its own logs.

`tracing::debug!(?envelope)` is therefore safe with respect to the envelope's own metadata. To read a value deliberately, ask for it by name with `envelope.header("tenant")`, which makes the disclosure a decision at the call site rather than a side effect of formatting. To recover the typed body, call `envelope.decode::<M>()` on the bus side or `envelope.decode::<E>()` on the outbox side. The decoder validates the `message_type` / `event_type` matches the requested generic and returns `TypeMismatch` otherwise.

Application code should still avoid putting credentials in headers. Redaction here protects the log; it does not protect the broker, which sees every header value in the clear unless the envelope is encrypted end to end.

## Constructors

| API | Constructor | Notes |
| --- | --- | --- |
| Outbox publish | `OutboxPublisher::publish_in_tx` | Mints `event_id`, serialises, INSERTs in the caller's transaction |
| Outbox restore (backends) | `OutboxEnvelope::restore(...)` | Used by store implementations rebuilding a row read from the database |
| Bus publish | `Transport::publish` / `publish_with_headers` / `publish_with_correlation_id` | Mints `message_id`, builds the envelope, dispatches to the broker driver |
| Bus restore (backends) | `BusEnvelope::restore(...)` | Used by consumer code that materialises a broker delivery |

`restore` is the seam any backend uses to ressurect an envelope from broker properties or a database row. It bypasses payload validation; the resulting envelope is expected to come from a trusted source.
