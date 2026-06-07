# Middleware pipeline

Every Hexeract mediator dispatch flows through a chain of middlewares before reaching the terminal handler. Middlewares wrap the dispatch onion-style: the first one registered sees the entry and the exit of every following one. Use them to add tracing, timeouts, retries, validation or any other cross-cutting concern that should run around every command, query and notification.

## The trait

```rust
use hexeract::core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};

#[trait_variant::make(Send)]
pub trait Middleware: Send + Sync + 'static {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError>;
}
```

Three observations on this signature.

- **`envelope`** carries the message type name, the `MessageId` and the `CorrelationId`. It is what observability middlewares record.
- **`next`** is the continuation. Calling `next.run(envelope, ctx).await` advances the pipeline to the next middleware or, if the chain is exhausted, to the handler. The middleware can short-circuit by *not* calling `next.run` and returning a `BoxOutput` of its own.
- **`BoxOutput`** is a type-erased `Box<dyn Any>` produced by the handler. The mediator downcasts it back to the typed `Output` at the dispatch boundary; middleware code passes it through unchanged.

## The onion order

`MediatorBuilder::with_middleware` registers middlewares in order. The first one registered is the **outermost**: it observes the entry and the exit of every subsequent middleware and of the handler itself. The last one registered is the **innermost**, sitting directly above the handler.

```rust
# use std::time::Duration;
# use hexeract::mediator::MediatorBuilder;
# use hexeract::middleware::{TimeoutMiddleware, TracingMiddleware};
let mediator = MediatorBuilder::new()
    .with_middleware(TracingMiddleware::new())          // outermost
    .with_middleware(TimeoutMiddleware::new(Duration::from_secs(5)))
    // .register_*_handler(...)
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

For each dispatch, the timeline is:

```
TracingMiddleware: entering
  TimeoutMiddleware: starting timer
    Handler::handle
  TimeoutMiddleware: timer fired or completed
TracingMiddleware: completed (with elapsed_ms) or failed (with error)
```

The recommended order is `Tracing` first, then `Timeout`. With this order, the tracing span observes the entry, the timeout, and the resulting failure. With the inverse, the span never opens when the timeout fires, which makes the failure harder to debug.

## Building your own

Two patterns cover most cases.

**Pass-through** measures or annotates without changing the outcome:

```rust
use std::time::Instant;
use hexeract::core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};

pub struct LatencyMiddleware;

impl Middleware for LatencyMiddleware {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError> {
        let started = Instant::now();
        let result = next.run(envelope, ctx).await;
        // record `started.elapsed()` into a metric somewhere.
        let _ = (envelope, started);
        result
    }
}
```

**Short-circuit** answers the call without going further. This is how cache, authorization or feature flag middlewares work:

```rust
use hexeract::core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};

pub struct FeatureFlagMiddleware {
    enabled: bool,
}

impl Middleware for FeatureFlagMiddleware {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError> {
        if !self.enabled {
            return Err(HexeractError::Dispatch(format!(
                "feature flag disabled for {}", envelope.type_name()
            )));
        }
        next.run(envelope, ctx).await
    }
}
```

## Cooperative cancellation

`HandlerContext` carries a `tokio_util::sync::CancellationToken` in its `cancellation` field. The pipeline observes it before each step: when the token fires, the next `Next::run` call returns `HexeractError::Cancelled { type_name }` instead of advancing, and the handler never runs. A step that is already executing is not interrupted; cancellation takes effect at the next pipeline boundary.

A middleware can use this to shed load or enforce a deadline without crafting the error itself:

```rust
use hexeract::core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};

pub struct LoadSheddingMiddleware {
    overloaded: bool,
}

impl Middleware for LoadSheddingMiddleware {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError> {
        if self.overloaded {
            ctx.cancellation.cancel();
        }
        next.run(envelope, ctx).await
    }
}
```

Handlers with long cooperative sections can also poll `ctx.is_cancelled()` themselves and bail out early; `HexeractError::cancelled(type_name)` builds the matching error.

## What middlewares are not

- **Not interceptors per type.** Every middleware runs on every dispatch. If you need per-message-type behavior, branch inside `execute` on `envelope.type_name()` or build the registry conditionally.
- **Not retry loops in disguise.** Calling `next.run` twice from a middleware will surface as `HexeractError::Dispatch("...terminal called twice")` because the underlying terminal is consumed on first invocation. Retries that need to re-dispatch should call back on the mediator itself (cloning it cheaply via `Arc`).
- **Not a replacement for handler logic.** A middleware that ends up encoding business rules is a smell; move the rule into the handler and keep middlewares to truly cross-cutting concerns.

## Built-ins

Hexeract ships two built-ins in `hexeract-middleware` (feature `middleware` on the umbrella crate):

- **`TracingMiddleware`** opens a `tracing::Span` per dispatch and emits structured events (`entering`, `completed`, `failed`). Level is configurable through `with_level`; defaults to `INFO`. Failures always log at `ERROR` regardless of configured level.
- **`TimeoutMiddleware`** wraps the inner pipeline in `tokio::time::timeout`. On expiration returns `HexeractError::Timeout { type_name, duration, .. }`.

See [`hexeract-middleware` reference](../reference/hexeract-middleware.md) for the exact API surface.

## Internals

Under the hood the pipeline is built lazily for each dispatch:

1. `Mediator::send` (or `query` / `publish`) resolves the handler from the registry and wraps it in a `Terminal`.
2. It clones the middleware `Vec<Arc<dyn DynMiddleware>>` (cheap: clones `Arc`s, not the middlewares themselves) and pairs it with the terminal in a `Next`.
3. `Next::run(envelope, ctx)` pops the first middleware and calls its `execute`, handing the rest of the chain as a new `Next`.
4. When the chain is empty, `Next::run` calls the terminal directly.

The dispatch flow is detailed in [Mediator architecture](../architecture/mediator-flow.md).
