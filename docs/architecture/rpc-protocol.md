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
| `x-hexeract-request-id` | Implemented | The identity of one request-reply call (a UUID), minted by the caller and used to route the reply back to its waiting slot. Mandatory on a request: an absent or unparsable value drops the request before the handler ever runs. |
| `x-hexeract-reply-status` | Implemented | `ok` or `error`, stamped on the reply only. |
| `x-hexeract-deadline` | Reserved | An RFC 3339 UTC timestamp naming the request's absolute deadline. No code reads or enforces this header today: it is reserved for a later version. |

## Three identifiers, three purposes

A request-reply round trip carries three distinct identifiers, each answering a different question:

| Identifier | Question it answers | Cardinality | Where it travels |
| --- | --- | --- | --- |
| `message_id` | Which specific envelope is this? | One per publish, always minted fresh (`UUIDv7`) | The envelope's own AMQP property, unrelated to the protocol |
| `correlation_id` | Which causal chain triggered this work? | One per chain, shared by every message in it | The AMQP `correlation_id` property, inherited or minted by the transport (see [Correlation ID propagation](../concepts/correlation-id.md)) |
| `x-hexeract-request-id` | Which call is this a reply to? | One per request-reply call, never shared even by two concurrent calls on the same chain | The reserved header, minted by `RequestClient` on every call. The responder parses it as a `Uuid` and re-stamps that parsed value on the reply, so a non-canonical form is normalized, not echoed verbatim; when the inbound header is absent or fails to parse, the responder omits it from the reply and the error payload's `request_id` falls back to `Uuid::nil()`. |

`RequestClient::request` always starts a fresh causal chain; `RequestClient::request_with` does too unless its `RequestOptions` carries a `correlation_id`, in which case the call joins that chain instead. Either way, every call mints its own `request_id`: two requests sharing a `correlation_id` are still routed and resolved independently by `RequestRegistry`, keyed on `request_id` and never on `correlation_id`.

## Error payload and categories

A failed reply's `message_type` is stamped with the sentinel `hexeract.rpc.error`, and its payload decodes as `RemoteErrorPayload`, a protocol type deliberately not a `Message`: a remote fault is not a domain message.

```rust
pub struct RemoteErrorPayload {
    pub error_type: RemoteErrorType,
    pub request_id: Uuid,
}
```

`request_id` is the identity of the call, correlating with the full failure trace recorded on the responder side, exactly as the caller sent it: a responder never publishes an error reply for a request with no readable identity. The payload never carries free-form text: an internal detail (a connection string, a host, a serialization error) must never cross the wire to a remote caller. `error_type` is a closed set:

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

Before any version check runs, the responder validates `reply_to`, then the request identity. A request with no `reply_to` is dropped without running the handler, whatever the version it announces: a `Request` has a reply by definition, so there is no legitimate case for running side-effecting work with no way to report on it. A request whose `reply_to` is present but is not a server-named reply inbox (the RabbitMQ backend requires the `amq.gen-` prefix) is likewise dropped without running the handler and without publishing anything: an unrecognized destination is refused rather than trusted. Only once `reply_to` passes this check does the responder look at the request identity: a request whose `x-hexeract-request-id` header is absent or does not parse as a UUID carries no readable identity and is dropped the same way, without running the handler and without publishing anything, since a reply built without one could never be matched to any in-flight call by the caller's registry. Only once both `reply_to` and the request identity pass these checks does the responder look at the version. A request that carries a valid, server-named `reply_to` and a readable identity, but announces a version other than the one the responder's crate implements, is rejected: the responder replies with `RemoteErrorType::Unsupported` and never runs the handler. A request that carries no `x-hexeract-protocol-version` header at all is rejected the same way: the responder cannot distinguish silence about the version from an explicit wrong one, so a missing header is treated identically to an unsupported one. A request that announces a supported version but fails to decode is rejected the same way, with `RemoteErrorType::Malformed`. Either way the caller gets a fast, categorized answer instead of a request silently dropped or misinterpreted. Validating `reply_to` and the request identity before the version check means the version-mismatch reply is only ever published to an already-validated destination and a definite identity, closing the path where a forged `reply_to` could relay that reply anywhere else.

The two sides do not treat a missing version identically, though. On the responder side, above, a missing header collapses into `Unsupported` alongside an explicit wrong version. On the caller side, the two stay distinct: a reply with no version header at all fails as `RequestError::Protocol(ProtocolViolation::MissingHeader)`, while a reply announcing a version this crate does not implement fails as `RequestError::Protocol(ProtocolViolation::UnsupportedVersion)`.

## Validating the reply type

Before decoding, the caller checks the reply's `message_type` against what it expects: `Request::Reply::MESSAGE_TYPE` for an `ok` reply, the sentinel `hexeract.rpc.error` for an `error` reply. This check is applied twice, at two different layers, guarding two different things.

The first layer is `RequestRegistry`, before a delivery is even routed to its waiting caller. A delivery whose `message_type` does not match what the pending slot expects, or that fails any other protocol check (see [Guarantees](#guarantees) below), is refused there: it is logged, counted as an invalid delivery, and never wakes the caller. The slot is left intact, so a well-formed reply that arrives afterwards can still complete the call. The caller does not observe the specific violation in this case: if no matching reply ever arrives, the call simply ends in `RequestError::Timeout`.

The second layer is `decode_reply`, run by `RequestClient` once a delivery has already been routed to its waiting caller by the registry. It re-applies the same rule and rejects a reply carrying the right status but the wrong `message_type` as `RequestError::Protocol`. Under a well-behaved registry this path is not reachable for a delivery that should have been filtered upstream; it is kept anyway as defense in depth, in case the two validators ever drift apart.

## Guarantees

- **First *valid* reply wins.** `RequestRegistry` keys each in-flight call by its `request_id`, but a slot is removed only once a delivery for it passes validation, never before: the registry checks the protocol version, the reply status and the `message_type` against the pending expectation before consuming the slot. A delivery that fails that check is refused, logged, and counted as an invalid delivery; the slot stays open, so the legitimate reply, if it has not arrived yet, can still win. A later reply for the same, already-resolved `request_id` (a duplicate under at-least-once delivery, or a stray one) is dropped, logged, and counted as orphaned, never treated as an error.
- **A request with no readable identity gets no answer.** `x-hexeract-request-id` is mandatory on a request, not merely conventional: a request whose header is absent or does not parse as a UUID is dropped before the handler ever runs and before anything is published, since a reply built without a caller-supplied identity could never be routed back to any in-flight call. This is a hardening of the v1 wire contract: an earlier revision ran the handler in this case regardless, and let the resulting reply be silently discarded as orphaned by the caller's registry instead.
- **The request identity is not an authorization boundary.** `x-hexeract-request-id` is minted by the caller and revealed to the responder on every call, so anyone able to observe or guess it can forge a delivery carrying it. Validating a delivery bounds what a forged one can accomplish, namely that it can no longer end a legitimate call simply by arriving first; it does not authenticate where the delivery actually came from.
- **The reply is acked only after its publisher confirm, under the default settings.** Under `AckMode::Manual` (the default), the responder publishes the reply through the dedicated `RabbitMqReplyPublisher`, which always targets the AMQP default exchange and is built internally with publisher confirms enabled: a caller cannot switch it to fire-and-forget. The publisher always awaits the broker's confirm before returning; only once that publish is confirmed does the responder's handler dispatch complete, and only then does the worker ack the original request delivery. A crash between producing a reply and acking the request causes the request to be redelivered, never causes a reply to be silently lost without a trace. This guarantee does not hold under `AckMode::AckOnReceive` (see [Ack modes](../concepts/ack-modes.md)), which acks the request delivery on receipt, before the handler even runs. The `fire_and_forget()` caveat no longer applies to the reply; it can still apply to the caller's own request publish, which goes through the application transport and is unaffected by this change.
- **Handlers must be idempotent.** Request-reply delivery is at-least-once, the same as any other handler dispatch (see [Ack modes](../concepts/ack-modes.md)): a `RequestHandler` can be invoked more than once for the same logical call, for example after a crash that redelivers an unacked request. The framework does not deduplicate; a handler with side effects is responsible for tolerating a repeat invocation.
- **There is no remote cancellation.** A caller giving up, whether by timing out or by dropping its future, only cleans up its own local registry slot. It does not, and cannot, notify the responder: the responder's handler keeps running to completion, and its eventual reply is simply an orphaned reply the caller is no longer waiting for. Nothing in this version of the protocol interrupts a handler already dispatched.

## Least-privilege topology

The guarantees above describe what the protocol and the framework enforce on their own. In a multi-tenant broker, where mutually untrusting requesters and responders share the same RabbitMQ instance, the operator has an additional lever the protocol cannot provide by itself: the broker's own access control. This section describes the minimum permissions each RPC role needs and, just as importantly, states plainly what those permissions do not achieve.

### The two roles

| Role | Needs to | Does not need to |
| --- | --- | --- |
| Requester (RPC client) | Declare its own server-named, exclusive reply inbox (the broker mints the name under `amq.gen-`); consume from that inbox; publish the request to the request destination. | Publish to any other party's inbox, or declare a non-generated (explicitly named) queue. |
| Responder (RPC server) | Consume from its request queue; publish replies to the AMQP default exchange, addressed to whichever `amq.gen-*` inbox the request's `reply_to` named. | Publish to any application exchange to reply. Post-#446, the framework does not construct a reply publish that targets one, so this restriction is enforced twice: once by the code, once by the broker if the operator configures it below. |

### A starting `rabbitmqctl` template

RabbitMQ's access control distinguishes three permissions per resource, verified against the official access control documentation (rabbitmq.com/docs/access-control, "Authorisation: How Permissions Work"): `configure` creates, destroys, or alters a resource's own definition; `write` injects a message into it; `read` retrieves a message from it. Which resource each AMQP operation checks, and under which permission, is fixed by the broker, not configurable: `queue.declare` and `queue.delete` check `configure` on the queue; `basic.consume`, `basic.get`, and `queue.purge` check `read` on the queue; `basic.publish` checks `write` on the exchange being published to; `queue.bind` checks `write` on the queue and `read` on the exchange.

That last point matters for the default exchange specifically. The default exchange is itself a permission-checked resource, named by the empty string in AMQP 0-9-1: `basic.publish` through it is authorized by `write` on that empty-string exchange name, not by any permission on the destination queue. Delivery to the queue named by the routing key happens through an implicit, pre-existing binding that AMQP 0-9-1 does not allow a client to create, alter, or remove; there is no `queue.bind` step to gate. Consequently, whatever regex a role's `write` permission uses must match the empty string for that role to publish through the default exchange at all, and the tightest way to grant exactly that and nothing else is `^$`, a pattern that matches only the empty string.

The requester's `^amq\.gen-.*$` grant for `configure`/`read` necessarily matches every other requester's server-named inbox too, not just its own; that breadth is safe because a server-generated `amq.gen-*` queue is declared exclusive to the connection that created it, so no other connection, whatever its permissions, can consume from it, and the pattern therefore cannot be used to reach another party's inbox.

The following is a starting template, not a drop-in configuration: adapt the queue and request-destination names to the real topology before using it.

```
# Requester, default transport (RabbitMqTransport::new(), the default
# exchange -- this is the doc-canonical topology): declare and consume
# only its own broker-generated reply inbox; publish only through the
# default exchange, exactly as the responder does below.
# configure: only auto-generated (amq.gen-*) queues, never an explicitly
#            named one -- this is what a reply inbox always is.
# write:     "^$" matches the empty string only, i.e. the default
#            exchange. `Request::DESTINATION` is the ROUTING KEY on that
#            publish, not the exchange (see "A starting rabbitmqctl
#            template" above): it is therefore an ungated routing key
#            under this grant, the same exchange-vs-routing-key split
#            already stated for the responder's reply publish below.
# read:      only its own reply inbox, for the same reason as configure.
rabbitmqctl set_permissions -p "tenant-a" "requester-svc" \
  "^amq\.gen-.*$" \
  "^$" \
  "^amq\.gen-.*$"

# Requester, named-exchange transport (RabbitMqTransport::with_exchange()):
# here the request is published to a named application exchange, so
# `write` must instead name that exchange (or those exchanges).
# configure/read: unchanged from above.
# write:     only the request exchange(s) this role calls; replace the
#            alternation with the real exchange name(s) -- this is NOT
#            the destination queue name, it is the exchange the
#            transport was constructed with.
rabbitmqctl set_permissions -p "tenant-a" "requester-svc" \
  "^amq\.gen-.*$" \
  "^(orders\.create|orders\.cancel)$" \
  "^amq\.gen-.*$"

# Responder: declare and consume only its own request queue; publish
# only through the default exchange, never through a named one.
# configure: only its own request queue (see "declaring the responder
#            queue" above); replace with the real destination name.
# write:     "^$" matches the empty string only, i.e. the default
#            exchange and no application exchange -- this is what
#            authorizes the reply publish, and nothing more.
# read:      only its own request queue, to consume incoming requests.
rabbitmqctl set_permissions -p "tenant-a" "responder-svc" \
  "^orders\.create$" \
  "^$" \
  "^orders\.create$"
```

One consequence of `write` being checked against the exchange rather than the destination queue is worth stating precisely, since it is easy to over-read what the `^$` grant above buys: once a principal holds `write` on the default exchange, the standard permission model does not re-check the routing key against a per-queue pattern for that publish (topic authorisation does check the routing key, but only for topic exchanges, and the default exchange is not one). The broker ACL therefore narrows a responder to *whether* it may use the default exchange at all; it is the framework's own `reply_to` validation, described above under [Version rules and coexistence](#version-rules-and-coexistence), that narrows *which* `amq.gen-*` inbox a given publish can actually reach. The two controls are complementary, not redundant: removing either one leaves the other still holding that line.

### The limit this does not close

These permissions bound who, within a vhost, may reach the default exchange or a given queue. They do not authenticate the origin of a reply. As already stated under [Guarantees](#guarantees), **the request identity is not an authorization boundary**: `x-hexeract-request-id` is revealed to the responder on every call, and any principal already authorized to publish through the default exchange, precisely the permission granted above, remains free to send a reply carrying any request id it can observe or guess, whether or not it is the responder that legitimately received the matching request. The broker has no notion of "this reply must correspond to a request this same connection actually consumed." Closing that residue, authenticating the responder's identity itself rather than merely its right to publish, is tracked by #444 first, then by #350: #444 (authenticate producers and verify end-to-end envelope integrity) binds an envelope to the party that sent it, the mechanism that actually closes this residue; #350 (mutual TLS) authenticates the connection to the broker and complements that without being sufficient on its own, since a legitimate but compromised responder would remain free to forge a reply. Both issues are open: this residue is tracked, not closed.

### Recommendation: one vhost per trust boundary

Vhost membership is the first access check RabbitMQ performs, before any resource-level permission is even consulted: a connection is rejected at that point if the authenticated user has no permissions at all on the target vhost. Resource ACLs inside a single shared vhost remain necessary, but they are not the strongest isolation available, and a single overly broad regex left over from a wider grant (a `write` of `.*` matches the empty string too) quietly erodes them. When mutually suspicious tenants coexist, isolating each trust boundary in its own vhost is the stronger control and the recommended posture: a principal confined to one tenant's vhost has no path, ACL or otherwise, to a queue or exchange that lives in another tenant's vhost.
