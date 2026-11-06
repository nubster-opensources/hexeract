# `hexeract-middleware` API reference

Stable surface of the `hexeract-middleware` crate. Re-exported through `hexeract::middleware` when the `middleware` feature is enabled on the umbrella crate.

The crate ships two built-in middlewares. Both implement the `Middleware` trait from `hexeract-core`; bring your own implementation if neither fits.

## `TracingMiddleware`

```rust
pub struct TracingMiddleware { /* ... */ }

impl TracingMiddleware {
    pub fn new() -> Self;                                            // Level::INFO
    pub fn with_level(self, level: tracing::Level) -> Self;         // consuming, chainable
}

impl Default for TracingMiddleware { /* INFO */ }
impl Middleware for TracingMiddleware { /* ... */ }
```

Usage:

```rust
use tracing::Level;
use hexeract_middleware::TracingMiddleware;

let middleware = TracingMiddleware::new().with_level(Level::DEBUG);
```

Opens a `tracing::Span` around every dispatch and emits a structured event on entry and on completion or failure.

**Span fields.** `type_name`, `message_id`, `correlation_id`, all from the `MessageEnvelope`.

**Events.**

- On entry: `"entering"` at the configured level.
- On success: `"completed"` at the configured level, with `elapsed_ms` (`u64`).
- On failure: `"failed"` at `Level::ERROR` regardless of configured level, with `elapsed_ms` and `error = %err`.

**Target.** Events use the default `tracing` target (the module path), which means a subscriber filter like `hexeract_middleware=trace` captures every emitted event.

**Span parent.** The span inherits the current `tracing::Span` of the calling task. If your application opens a request-level span before calling `mediator.send`, the dispatch span will be a child of yours, preserving the end-to-end trace.

## `TimeoutMiddleware`

```rust
pub struct TimeoutMiddleware { /* ... */ }

impl TimeoutMiddleware {
    pub fn new(duration: std::time::Duration) -> Self;
    pub fn with_grace(self, grace: std::time::Duration) -> Self;
}

pub const DEFAULT_TIMEOUT_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

impl Middleware for TimeoutMiddleware { /* ... */ }
```

Races the inner pipeline against a timer. On expiration it fires `ctx.cancellation`, keeps polling the handler for the grace period (`DEFAULT_TIMEOUT_GRACE` unless changed with `with_grace`) so it can observe the cancellation and clean up, then drops it and returns `HexeractError::Timeout { type_name, duration, .. }`. The result is a `Timeout` even when the handler finishes during the grace period: the deadline was missed, and the grace is for cleanup. `with_grace(Duration::ZERO)` cancels and drops straight away. A handler that blocks its thread never yields to the runtime, so move blocking work to `tokio::task::spawn_blocking`.

**Cancellation semantics.** When the timeout fires, the inner future is dropped at its next await point. `Drop` implementations in the inner future run normally; partial state should be guarded by RAII if your handler mutates external resources.

**`Duration::ZERO`** polls the inner future once and returns `Timeout` if it has not completed. Useful for tight no-op handlers; rarely useful in production.

## Recommended order

Wire `TracingMiddleware` first (outermost) so that the span observes the full lifetime of the dispatch, including the timeout:

```rust
use std::time::Duration;
use hexeract::mediator::MediatorBuilder;
use hexeract::middleware::{TimeoutMiddleware, TracingMiddleware};

let mediator = MediatorBuilder::new()
    .with_middleware(TracingMiddleware::new())
    .with_middleware(TimeoutMiddleware::new(Duration::from_secs(5)))
    // .register_*_handler(...)
    .build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

With the inverse order, `TracingMiddleware` sits inside `TimeoutMiddleware`: the span still opens and the "entering" event is emitted, but when the timeout fires the inner future is dropped, so the "completed" or "failed" exit events from `TracingMiddleware` are never emitted. The recommended order keeps the full entry-to-exit record in the span.

## Error variant

The `Timeout` variant on `HexeractError` is `#[non_exhaustive]` to allow future additions. Construct it through the public constructor:

```rust
use std::time::Duration;
use hexeract::core::HexeractError;

let err = HexeractError::timeout("MyCmd", Duration::from_secs(5));
```

Pattern-match it with `..`:

```rust
# use hexeract::core::HexeractError;
# let err = HexeractError::timeout("MyCmd", std::time::Duration::from_secs(5));
match err {
    HexeractError::Timeout { type_name, duration, .. } => {
        eprintln!("{type_name} timed out after {duration:?}");
    }
    _ => {}
}
```

## Building your own

`Middleware` is a public trait; any `Send + Sync + 'static` type implementing it can be wired through `MediatorBuilder::with_middleware`. See the [middleware pipeline concept page](../concepts/middleware-pipeline.md) for the full contract and worked examples (pass-through, short-circuit, envelope inspection).
