# Request-reply

Hexeract's request-reply pattern layers a synchronous-over-async RPC call on top of the fire-and-forget bus: a caller publishes a typed request and awaits a typed reply, correlated back to it by a freshly minted identifier. This page covers the request registry and its drop-guard, the wire contract two independent processes agree on for a reply envelope, the ways a request can fail, and the lifecycle of the exclusive reply inbox.

## The pattern

A `Request` is a `Message` that names its own reply type via the associated `Reply: Message`. `RequestClient::request` publishes the request envelope with a fresh correlation id and a `reply_to` inbox name, then awaits the matching reply; `RequestClient::request_with` accepts a `RequestOptions` to override the timeout, the destination, or to join an existing causal chain instead of opening a fresh one for that one call (see [Correlation ID propagation](correlation-id.md#correlation-identity-versus-request-identity)). On the responder side, `RequestHandler<R>` is the typed counterpart to `Handler<M>` (see [Message and envelope](message-envelope.md)): instead of producing a side effect, it returns `R::Reply`. `RepliedHandler` adapts a `RequestHandler<R>` into an `ErasedHandler` a worker can dispatch: it decodes the request, runs the handler, and publishes the typed reply (or an encoded error) back to the caller's `reply_to`, preserving the inbound correlation id. `RabbitMqWorkerBuilder::register_request_handler` wires a `RequestHandler<R>` into a worker exactly the way `register_handler` wires a plain `Handler<M>`.

```mermaid
sequenceDiagram
    autonumber
    participant Client as RequestClient
    participant Reg as RequestRegistry
    participant Broker as RabbitMQ
    participant Worker as RabbitMqWorker
    participant Handler as RequestHandler<R>

    Client->>Reg: register() -> PendingReply(rid)
    Client->>Broker: publish(reply_to = inbox, x-hexeract-request-id = rid)
    Broker->>Worker: Delivery
    Worker->>Handler: handle(request, ctx)
    Handler-->>Worker: Ok(reply) or Err(error)
    Worker->>Broker: publish(reply_to, status header, x-hexeract-request-id = rid)
    Broker->>Reg: Delivery on inbox
    Reg->>Client: resolve(rid) wakes PendingReply
```

## Request registry and the drop-guard

`RequestRegistry` is the rendezvous point between a waiting caller and an inbound reply. `register()` mints a fresh request id, opens a `tokio::sync::oneshot` channel, and returns a `PendingReply`: an RAII guard over that channel. The inbox consumer calls `resolve(envelope)` to route a delivered reply to its waiting slot by request identity, read from the reserved `x-hexeract-request-id` header; an unknown or already-resolved id is dropped with a warning, never an error, and the first reply for a slot wins.

The drop-guard is what makes the registry leak-free. `PendingReply`'s `Drop` implementation removes its slot from the registry on every exit path, whatever gets the caller out: a successful `wait()`, a timeout racing the `wait()` future, a cancellation, or a panic unwinding through the caller. A slot is never left behind for a reply that never arrives, and `RequestRegistry::drain()` (used on connection loss) closes every outstanding channel at once so every waiting caller observes a closed channel immediately instead of waiting out its timeout.

## Wire contract

Requester and responder agree on the reply shape purely through envelope conventions, with no shared connection state:

- The request envelope carries `reply_to` (the inbox queue name), the header `x-hexeract-request-id` (the fresh request identity the caller registered, used to route the reply; mandatory, see [RPC protocol: version rules and coexistence](../architecture/rpc-protocol.md#version-rules-and-coexistence) for what happens when it is absent or unreadable) and `correlation_id` (the causal-chain identifier, unrelated to routing).
- The reply envelope stamps the header `x-hexeract-reply-status` to either `ok` or `error`, and carries the same `correlation_id` as the request.
- On success, the reply payload decodes as `R::Reply` like any other message.
- On failure, the reply's `message_type` is stamped with the sentinel `hexeract.rpc.error` and the payload decodes as `RemoteErrorPayload`, a protocol type deliberately not a `Message`: a remote fault is not a domain message. See [RPC protocol: error payload and categories](../architecture/rpc-protocol.md#error-payload-and-categories) for the payload shape and the closed set of categories.

Both the request and the reply carry the header `x-hexeract-protocol-version`. An unsupported or undecodable version is rejected before decoding, with a categorized error rather than a silent drop. See [RPC protocol: version rules and coexistence](../architecture/rpc-protocol.md#version-rules-and-coexistence) for the exact rules.

A request that reaches `RepliedHandler` without a usable `reply_to` (bypassing `RequestClient`, for example a hand-crafted envelope) is dropped before the handler runs. That covers both an absent `reply_to` and one the reply publisher refuses, the RabbitMQ backend accepting only a server-named `amq.gen-` inbox. Since the handler never runs, there is no handler `Result` to drive the delivery, and this guard publishes nothing at all: `RepliedHandler::handle` logs a warning carrying the request's `correlation_id`, the only trace of the incident anywhere in the system, and returns `Ok(())`. Under the default `AckMode::Manual` the delivery is therefore acked exactly as a successfully answered request would be, and the worker's nack, retry and dead-letter policy is never reached (see [Retry policy](retry-policy.md) and [Ack modes](ack-modes.md)). A real `RequestClient` always stamps `reply_to`, so this path is only reachable from a non-conforming producer. It is not a way to use a `Request` fire-and-forget: a handler reached that way never runs at all. For work that produces a side effect and returns nothing, use a plain `Handler<M>` (see [Message and envelope](message-envelope.md)).

## Business rejection versus protocol failure

An expected domain outcome, an unknown account, a frozen one, an empty search result, is not a failure of the request-reply protocol: it is a value the responder always knows how to produce and the caller always knows how to handle. It belongs in `Request::Reply`, encoded as an ordinary successful reply, never on the protocol's error channel.

```rust
#[derive(Debug, Serialize, Deserialize)]
enum GetBalanceReply {
    Found { cents: u64 },
    UnknownAccount { account_id: Uuid },
    Frozen { since: String },
}

impl Request for GetBalance {
    type Reply = GetBalanceReply;
}
```

The rule: a business rejection is a return value, carried like any other reply. The protocol's error channel is reserved for a fault the caller cannot resolve by matching on a value: the handler failed, the request could not be decoded, or the announced protocol version is unsupported. See [RPC protocol](../architecture/rpc-protocol.md) for the full contract, including the error payload shape and the version rules.

## Declaring the responder queue

`RabbitMqTransport::publish_envelope` publishes every request `mandatory` and awaits the publisher confirm, so a request published to a routing key with no bound queue is returned by the broker as NO_ROUTE and surfaces immediately as `RequestError::Transport(BusError::Unroutable)`, never as a hang: a misconfigured responder fails the caller's very first request outright instead of leaving it to wait out its timeout.

For that fast failure to be useful, the responder side must declare and bind, out-of-band, a queue named exactly the request's `Request::DESTINATION` (which defaults to `MESSAGE_TYPE`): `RequestClient` publishes to that routing key, and the default exchange routes a message to the queue of the same name. See [RPC protocol: addressing a request](../architecture/rpc-protocol.md#addressing-a-request) for when to override `DESTINATION`. `RabbitMqWorkerBuilder` does not auto-declare the queue it consumes from, no more than it does for a plain `Handler<M>`: at startup the worker declares only its own retry (`<queue>.retry`) and dead-letter queues, never the queue passed to `.queue(...)`. The request queue's topology, like any other queue the worker consumes from, is declared out-of-band before the worker starts (see [Worker](worker.md)).

This is a deliberate asymmetry with the reply inbox described below: the inbox is exclusive to one connection, so only the client that owns that connection can declare it, and it does so on every reconnect. The request queue, by contrast, is shared: any number of client processes publish to it and any number of worker instances may consume from it, so it is owned by whoever operates the responder, not minted by the framework.

## Failure modes

`RequestError` has seven variants, all reachable from `RequestClient::request` and `RequestClient::request_with`. It is `#[non_exhaustive]`, so a `match` written outside this crate needs a catch-all arm and the compiler will not point out a variant added later: the first three below are local to the caller's own client and are the ones most easily mistaken for one another.

- **`Timeout(Duration)`**: no reply arrived within the deadline. The `PendingReply` is dropped along with the `tokio::time::timeout` future, so the slot is cleaned up immediately rather than lingering until a reply eventually shows up.
- **`AtCapacity`**: the client already holds `max_in_flight` calls, and this one was refused outright rather than queued behind a free slot. Back-pressure is reported as an immediate failure precisely so it stays distinguishable from a slow responder: a saturated client and a slow remote look identical under a timeout, yet they are relieved at opposite ends. Retrying at once cannot succeed, since nothing has been released in the meantime.
- **`Closed`**: `close()` was called on the client, either while this call was pending or before it started. Unlike the two above, this one is not a symptom of load: it says the client will never serve another call, so the right response is to stop issuing them, not to retry.
- **`Remote { error_type, request_id }`**: the responder's `RequestHandler` returned an error, decoded from a `RemoteErrorPayload` reply. The category is deliberately coarse and carries no detail; the full trace lives on the responder side, indexed by `request_id`.
- **`Protocol(ProtocolViolation)`**: the reply does not honor the protocol, either because a required header is missing or unparsable (`MissingHeader`), the announced protocol version is not implemented by this crate (`UnsupportedVersion`), or the reply's `message_type` does not match what was expected for this call (`UnexpectedReplyType`).
- **`Transport(BusError)`**: the request could not be published, or the reply channel was lost, most notably when the reply inbox's connection drops and the supervisor drains the registry so every in-flight `PendingReply` fails fast instead of waiting out its timeout.
- **`Decode(BusError)`**: the request could not be serialized, or the reply's payload could not be decoded into `R::Reply` (or `RemoteErrorPayload` for an error reply).

## The reply inbox lifecycle

`connect_request_client` assembles a `RequestClient` backed by two independent RabbitMQ connections: an auto-recovering publisher connection for outgoing requests, and a separate, supervised connection dedicated to the reply inbox. The two must stay separate: lapin's native auto-recovery on the publisher side would keep a stale consumer stream alive across a broker drop and mask the outage from the supervisor that owns the inbox's lifecycle.

The inbox itself, declared by `declare_reply_inbox`, is exclusive, auto-delete and server-named: it dies with the connection that declared it. On reconnect there is no way to resume consuming the old inbox, so the supervisor mints a fresh name over the new connection and publishes it into the shared, mutex-guarded name `RequestClient` reads on every request. Concretely, on a broker drop:

1. `run_reply_inbox` returns `Err` (a `BusError::Connection`, always retryable).
2. The supervisor calls `RequestRegistry::drain()`, so every request in flight against the dead inbox observes `RequestError::Transport` immediately instead of waiting out its timeout.
3. The supervisor reconnects, declares a fresh exclusive inbox (a new name), and republishes it for future requests.
4. `run_reply_inbox` resumes on the new inbox.

## Out of scope

Request-reply is strictly one request, one reply: there is no streaming or multi-reply variant. A responder that needs to send more than one message back to a caller is a different pattern, one `RequestHandler` and `RepliedHandler` do not support.

## Where to read next

- [Correlation ID propagation](correlation-id.md)
- [Message and envelope](message-envelope.md)
- [`hexeract-bus` API reference](../reference/hexeract-bus.md)
- [`hexeract-bus-rabbitmq` API reference](../reference/hexeract-bus-rabbitmq.md)

Neither reference page covers the request-reply surface yet. Until they do, the rustdoc of `hexeract-bus` is the per-item reference for `RequestClient`, `RequestHandler`, `RequestContext` and `RequestError`.
