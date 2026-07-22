# RPC protocol (request-reply, wire version 1)

Two independent processes, a caller and a responder, agree on the request-reply contract purely through envelope conventions: a reserved header namespace, a versioned wire shape, and a closed set of failure categories. Neither side holds shared connection state; the contract is carried entirely in the envelope. This page is the public reference for that contract. For the surrounding pattern (the request registry, the drop-guard, the reply inbox lifecycle), see [Request-reply](../concepts/request-reply.md).

## Addressing a request

A request is published to `Request::DESTINATION`, the routing key that `RabbitMqTransport::publish_envelope` targets. `DESTINATION` defaults to `Message::MESSAGE_TYPE`, so for the common case of one responder per request type, the responder queue is simply named after the type. Override `DESTINATION` when a request must reach a queue distinct from the type's usual destination, for example when several request types share one responder queue. Whichever value it resolves to, the responder side is responsible for declaring and binding, out-of-band, a queue named exactly `DESTINATION`: see [Request-reply: declaring the responder queue](../concepts/request-reply.md#declaring-the-responder-queue).

## Reserved namespace

Every header prefixed `x-hexeract-` is reserved by the framework: an application must not write one of its own. The framework does not enforce this today: nothing filters or rejects an application-supplied `x-hexeract-*` header, so a domain header sharing the prefix silently collides with the framework's own value rather than being ignored or rejected.

## Headers

| Header | Status | Carries |
| --- | --- | --- |
| `x-hexeract-protocol-version` | Implemented | The wire version (`u32`) of the request or the reply. Currently `1`. |
| `x-hexeract-request-id` | Implemented | The identity of one request-reply call (a UUID), minted by the caller and used to route the reply back to its waiting slot. |
| `x-hexeract-reply-status` | Implemented | `ok` or `error`, stamped on the reply only. |
| `x-hexeract-deadline` | Reserved | An RFC 3339 UTC timestamp naming the request's absolute deadline. No code reads or enforces this header today: it is reserved for a later version. |

## Three identifiers, three purposes

A request-reply round trip carries three distinct identifiers, each answering a different question:

| Identifier | Question it answers | Cardinality | Where it travels |
| --- | --- | --- | --- |
| `message_id` | Which specific envelope is this? | One per publish, always minted fresh (`UUIDv7`) | The envelope's own AMQP property, unrelated to the protocol |
| `correlation_id` | Which causal chain triggered this work? | One per chain, shared by every message in it | The AMQP `correlation_id` property, inherited or minted by the transport (see [Correlation ID propagation](../concepts/correlation-id.md)) |
| `x-hexeract-request-id` | Which call is this a reply to? | One per request-reply call, never shared even by two concurrent calls on the same chain | The reserved header, minted by `RequestClient` on every call. The responder parses it as a `Uuid` and re-stamps that parsed value on the reply, so a non-canonical form is normalized, not echoed verbatim; when the inbound header is absent or fails to parse, the responder omits it from the reply and the error payload's `request_id` falls back to `Uuid::nil()`. |

`RequestClient::request` starts a fresh causal chain; `RequestClient::request_in` continues the causal chain carried by a `HandlerContext`. Either way, every call mints its own `request_id`: two requests issued from the same handler, on the same `correlation_id`, are routed and resolved independently by `RequestRegistry`, keyed on `request_id` and never on `correlation_id`.

## Error payload and categories

A failed reply's `message_type` is stamped with the sentinel `hexeract.rpc.error`, and its payload decodes as `RemoteErrorPayload`, a protocol type deliberately not a `Message`: a remote fault is not a domain message.

```rust
pub struct RemoteErrorPayload {
    pub error_type: RemoteErrorType,
    pub request_id: Uuid,
}
```

`request_id` is the identity of the call, correlating with the full failure trace recorded on the responder side; it is `Uuid::nil()` when the inbound request carried no usable request id. The payload never carries free-form text: an internal detail (a connection string, a host, a serialization error) must never cross the wire to a remote caller. `error_type` is a closed set:

| Category | Meaning |
| --- | --- |
| `Internal` | The responder failed while handling the request. |
| `Malformed` | The request could not be decoded, or its payload was rejected. |
| `Unavailable` | A connection or transport failure occurred, or the request could not be routed to any queue. |
| `Unsupported` | The announced protocol version is not supported by the responder. |
| `Expired` | Reserved by this version. The request deadline had already passed. No code produces this category today: it exists so a future version that honors `x-hexeract-deadline` does not need a new one. |

The responder collapses an internal failure into a category rather than forwarding it verbatim: distinct internal causes deliberately land under the same public label, so no internal detail crosses the boundary.

## Version rules and coexistence

The protocol version travels in the message, in the `x-hexeract-protocol-version` header, rather than in the channel or the queue topology. That is a deliberate choice: two versions can coexist on the same topology during a progressive rollout, because compatibility is decided per message, not per connection or per queue.

A request that carries a `reply_to` but announces a version other than the one the responder's crate implements is rejected before decoding: the responder replies with `RemoteErrorType::Unsupported` and never runs the handler. A request that carries no `x-hexeract-protocol-version` header at all is rejected the same way: the responder cannot distinguish silence about the version from an explicit wrong one, so a missing header is treated identically to an unsupported one. A request that announces a supported version but fails to decode is rejected the same way, with `RemoteErrorType::Malformed`. Either way the caller gets a fast, categorized answer instead of a request silently dropped or misinterpreted. A request with no `reply_to` and an unsupported or missing version is dropped without running the handler too, just without a reply: there is no address to tell.

The two sides do not treat a missing version identically, though. On the responder side, above, a missing header collapses into `Unsupported` alongside an explicit wrong version. On the caller side, the two stay distinct: a reply with no version header at all fails as `RequestError::Protocol(ProtocolViolation::MissingHeader)`, while a reply announcing a version this crate does not implement fails as `RequestError::Protocol(ProtocolViolation::UnsupportedVersion)`.

## Validating the reply type

Before decoding, the caller checks the reply's `message_type` against what it expects: `Request::Reply::MESSAGE_TYPE` for an `ok` reply, the sentinel `hexeract.rpc.error` for an `error` reply. A reply that carries the right status but the wrong `message_type` is rejected as a protocol violation rather than decoded speculatively: a reply meant for a different request type must never be silently accepted just because the status header happened to parse.

## Guarantees

- **First reply wins.** `RequestRegistry` keys each in-flight call by its `request_id` and removes the slot as soon as the first reply for it arrives. A later reply for the same, already-resolved `request_id` (a duplicate under at-least-once delivery, or a stray one) is dropped, logged, never treated as an error.
- **The reply is acked only after its publisher confirm, under the default settings.** Under `AckMode::Manual` (the default) with a reply transport that has not called `RabbitMqTransport::fire_and_forget()`, the responder publishes the reply through a transport that awaits the broker's publisher confirm before returning; only once that publish is confirmed does the responder's handler dispatch complete, and only then does the worker ack the original request delivery. A crash between producing a reply and acking the request causes the request to be redelivered, never causes a reply to be silently lost without a trace. This guarantee does not hold under `AckMode::AckOnReceive` (see [Ack modes](../concepts/ack-modes.md)), which acks the request delivery on receipt, before the handler even runs, nor when the reply is published through a `fire_and_forget()` transport, which disables the publisher confirm the guarantee depends on.
- **Handlers must be idempotent.** Request-reply delivery is at-least-once, the same as any other handler dispatch (see [Ack modes](../concepts/ack-modes.md)): a `RequestHandler` can be invoked more than once for the same logical call, for example after a crash that redelivers an unacked request. The framework does not deduplicate; a handler with side effects is responsible for tolerating a repeat invocation.
- **There is no remote cancellation.** A caller giving up, whether by timing out or by dropping its future, only cleans up its own local registry slot. It does not, and cannot, notify the responder: the responder's handler keeps running to completion, and its eventual reply is simply an orphaned reply the caller is no longer waiting for. Nothing in this version of the protocol interrupts a handler already dispatched.
