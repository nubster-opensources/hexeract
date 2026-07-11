# `hexeract-core` API reference

Cross-cutting foundation every other Hexeract crate depends on: the marker traits for messages, their matching handler traits, the type-erased envelope and context passed alongside every dispatch, the middleware pipeline primitives, and the unified error type.

The full rustdoc lives at <https://docs.rs/hexeract-core>.

## Public surface

### Message markers

| Item | Role |
| --- | --- |
| `Command` | Marker for a message expressing the intent to mutate state. Has an associated `Output` type and exactly one registered `CommandHandler`. |
| `Query` | Marker for a read-only message asking for information. Same machinery as `Command`, kept as a distinct trait for clarity. |
| `Notification` | Marker for a broadcast message with fan-out semantics: zero or more handlers may react. No output. The mediator shares a single `Arc<N>` across every handler, so `Notification` does not require `Clone`. |

### Handler traits

| Item | Role |
| --- | --- |
| `CommandHandler<C>` | `async fn handle(&self, cmd: C, ctx: &HandlerContext) -> Result<C::Output, Self::Error>`. Exactly one per `Command` type. |
| `QueryHandler<Q>` | Same shape as `CommandHandler`, for `Query`. |
| `NotificationHandler<N>` | `async fn handle(&self, n: Arc<N>, ctx: &HandlerContext) -> Result<(), Self::Error>`. Zero or more per `Notification` type. |

### Envelope, context and identifiers

| Item | Role |
| --- | --- |
| `MessageEnvelope` | Type-erased metadata carried alongside a dispatch: the message's fully-qualified type name, its `MessageId` and its `CorrelationId`. Built through `for_command`, `for_query` or `for_notification`. |
| `HandlerContext` | Carries `message_id`, `correlation_id`, a `CancellationToken` for cooperative cancellation and the active `tracing::Span`. |
| `MessageId` | Unique identifier for one message instance, backed by a `Uuid`. |
| `CorrelationId` | Identifier linking a chain of causally related messages. |

### Middleware pipeline

| Item | Role |
| --- | --- |
| `Middleware` | Intercepts a dispatch before it reaches its handler. Middlewares stack onion-style: the first registered wraps all the others. |
| `Next` | Handle to invoke the rest of the pipeline from inside a `Middleware::execute`. |
| `Terminal` | The innermost stage of the pipeline: the actual handler invocation. |
| `BoxOutput` | `Box<dyn Any + Send + Sync>`, the type-erased handler output passed through the chain and downcast back to the typed output at the dispatch boundary. |
| `DynMiddleware` | Object-safe form used to store a heterogeneous list of middlewares. |

### Error type

| Item | Role |
| --- | --- |
| `HexeractError` | Unified, `#[non_exhaustive]` framework error: `HandlerNotFound`, `HandlerFailed`, `Timeout`, `DowncastFailed`, `InputDowncastFailed`, `Cancelled`, `PublishFailed { failures: Vec<NotificationFailure>, .. }`, `Dispatch(String)`. Variants carrying data are built through constructors (`handler_not_found`, `handler_failed`, `timeout`, `cancelled`, `publish_failed`, ...) rather than by literal. |
| `NotificationFailure` | One handler's typed error from a `PublishFailed` fan-out, pairing `handler: &'static str` with the original `HexeractError` (source chain intact). |

### Handler registration metadata

| Item | Role |
| --- | --- |
| `HandlerRegistration` | One handler discovered at link time by the `#[handler]` macro from `hexeract-macros`, collected via `inventory::submit!` and iterated by `MediatorBuilder::verify_handlers` to catch a handler declared with the macro but never wired into the registry. |
| `HandlerKind` | `Command`, `Query` or `Notification`, tagging a `HandlerRegistration`. |

## Features

| Feature | Default | Effect |
| --- | --- | --- |
| `serde` | off | Derives `Serialize`/`Deserialize` on `MessageId` and `CorrelationId` (enables `uuid/serde`). |

## Where to read next

- [Handler macro concept](../concepts/handler-macro.md)
- [Message envelope concept](../concepts/message-envelope.md)
- [Middleware pipeline concept](../concepts/middleware-pipeline.md)
- [Mediator CQRS concept](../concepts/mediator-cqrs.md)
