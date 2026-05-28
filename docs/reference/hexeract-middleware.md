# `hexeract-middleware` API reference

Stable surface of the `hexeract-middleware` crate. Re-exported through `hexeract::middleware` when the `middleware` feature is enabled on the umbrella crate.

The crate ships two built-in middlewares. Both implement the `Middleware` trait from `hexeract-core`; bring your own implementation if neither fits.

## `TracingMiddleware`

```rust
pub struct TracingMiddleware { /* ... */ }

impl TracingMiddleware {
    pub fn new() -> Self;                                 // Level::INFO
    pub fn with_level(level: tracing::Level) -> Self;
}

impl Default for TracingMiddleware { /* INFO */ }
impl Middleware for TracingMiddleware { /* ... */ }
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
}

impl Middleware for TimeoutMiddleware { /* ... */ }
```

Wraps the inner pipeline in `tokio::time::timeout`. On expiration returns `HexeractError::Timeout { type_name, duration, .. }`.

**Cancellation semantics.** When the timeout fires, the inner future is dropped at its next await point. `Drop` implementations in the inner future run normally; partial state should be guarded by RAII if your handler mutates external resources.

**`Duration::ZERO`** polls the inner future once and returns `Timeout` if it has not completed. Useful for tight no-op handlers; rarely useful in production.

## Recommended order

Wire `TracingMiddleware` first so that the span observes the entry, the timeout, and the resulting failure with the typed error in the exit event:

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

With the inverse order, the span never opens when the timeout fires, which makes the failure harder to debug.

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
