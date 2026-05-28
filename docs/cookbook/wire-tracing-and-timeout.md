# Wire tracing and timeout around every dispatch

You want every command, query and notification to be observable in your tracing pipeline, and you want a hard ceiling on per-dispatch latency. The two built-in middlewares cover both in three lines of wiring.

## Recipe

```rust
use std::time::Duration;
use hexeract::mediator::MediatorBuilder;
use hexeract::middleware::{TimeoutMiddleware, TracingMiddleware};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mediator = MediatorBuilder::new()
    .with_middleware(TracingMiddleware::new())
    .with_middleware(TimeoutMiddleware::new(Duration::from_secs(5)))
    // .register_command_handler::<_, _>(...)
    // .register_query_handler::<_, _>(...)
    .build()?;
# Ok(()) }
```

The order matters: register `TracingMiddleware` first so the span observes the timeout failure when it fires. The span emits an `entering` event on entry, a `completed` event with `elapsed_ms` on success, and a `failed` event at `ERROR` level with the typed error message on failure.

## What the trace looks like

With a `tracing_subscriber::fmt` set up at INFO level, dispatching `CreateUser` produces (annotated):

```text
INFO hexeract_middleware::tracing: entering
INFO hexeract_middleware::tracing: completed elapsed_ms=12
```

On a timeout:

```text
INFO  hexeract_middleware::tracing: entering
ERROR hexeract_middleware::tracing: failed elapsed_ms=5001 error=dispatch of `my_app::CreateUser` timed out after 5s
```

The `elapsed_ms` measures the wall-clock time spent inside the middleware (which itself wraps everything below it, including the timeout middleware and the handler).

## Variants

**Lower verbosity.** Replace `TracingMiddleware::new()` with `TracingMiddleware::with_level(tracing::Level::DEBUG)` to drop the `entering` / `completed` events out of INFO-level logs while keeping `failed` at ERROR.

**Per-channel filtering.** The events use the default target (module path `hexeract_middleware::tracing`). Filter with `RUST_LOG=hexeract_middleware=warn` in production to see only failures, `=info` in staging for full visibility.

**Hierarchical spans.** The dispatch span inherits the current `tracing::Span` of the calling task. If your HTTP handler opens `info_span!("http_request", path=...)` before calling `mediator.send`, the dispatch span will be a child of yours; structured collectors (Tempo, Jaeger via OpenTelemetry) stitch the full trace.

## Pitfalls

**Timeout before tracing.** Register `TimeoutMiddleware` first means the span never opens when the timeout fires. Your trace shows a void at the dispatch site instead of a `failed` event. Always register `TracingMiddleware` first.

**Short timeouts and slow startup.** `Duration::from_millis(50)` is enough to hit the timeout during a slow first dispatch (cold caches, JIT-warmed monomorphization). If you see flaky timeouts in test, raise the default and rely on the typed error to bubble up actual production timeouts.

**Capturing dispatch time in metrics.** The `elapsed_ms` field is emitted as a `u64` field on the `completed` event. A `tracing` -> Prometheus bridge such as `tracing_opentelemetry` + `opentelemetry-prometheus` exports it automatically as a metric.
