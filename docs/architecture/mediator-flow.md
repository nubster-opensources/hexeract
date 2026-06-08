# Mediator architecture

This page describes how the in-process mediator routes a dispatch from the call site to the handler. It covers the registry layout, the per-dispatch pipeline assembly, and the fan-out fail-safe semantics for notifications.

## Registry layout

`MediatorBuilder` accumulates handlers into four maps:

```text
command_handlers       : HashMap<TypeId, Arc<dyn ErasedCommandHandler>>
query_handlers         : HashMap<TypeId, Arc<dyn ErasedQueryHandler>>
notification_handlers  : HashMap<TypeId, Vec<Arc<dyn ErasedNotificationHandler>>>
middlewares            : Vec<Arc<dyn DynMiddleware>>
```

Plus three `HashSet<&'static str>` mirrors of the keys, used exclusively by `verify_handlers`.

The `Typed*Handler<M, H>` adapters wrap the user's typed handler `H: CommandHandler<M>` and erase the message type. Each adapter:

1. downcasts the boxed payload back to `M`,
2. awaits `H::handle`,
3. maps `H::Error` via `Into<HexeractError>`,
4. re-boxes the output into a type-erased `BoxOutput`.

`build()` moves the four maps into an `Arc<MediatorInner>` so cloning the `Mediator` is `O(1)`.

## Dispatch sequence (command and query)

```text
Mediator::send::<C>(command)
  └─> lookup TypeId::of::<C>() in command_handlers
      ├─ miss → Err(HexeractError::HandlerNotFound { message_type })
      └─ hit  → continue
  ├─> mint MessageId + CorrelationId
  ├─> build MessageEnvelope::for_command::<C>(...)
  ├─> build HandlerContext::new(...)
  ├─> wrap handler in CommandTerminal { handler, payload: Mutex<Option<BoxAny>> }
  ├─> Next::new(middlewares.clone(), terminal)
  ├─> next.run(&envelope, &ctx).await
  │     └─> drains middleware chain onion-style
  │           └─> terminal.dispatch(&envelope, &ctx)
  │                 └─> downcast payload → call H::handle → re-box output
  └─> downcast BoxOutput → C::Output
```

`query` follows the same shape against `query_handlers`.

### The `Mutex<Option<BoxAny>>` trick

`Terminal::dispatch(&self, envelope, ctx)` takes `&self` to be object-safe across the entire middleware chain. But the handler needs to *own* the input to consume it. The terminal parks the boxed payload in a `Mutex<Option<BoxAny>>` and `take()`s it on first call. A second call (a buggy middleware that invokes `next.run` twice) returns `HexeractError::Dispatch("...terminal called twice")` instead of silently re-dispatching.

## Fan-out (notification)

`publish::<N>` builds one future per handler and drives them concurrently:

```text
Mediator::publish::<N>(notification)
  └─> lookup TypeId::of::<N>() in notification_handlers
      ├─ miss or empty Vec → Ok(())
      └─ hit → continue
  ├─> mint a shared CorrelationId once for the entire fan-out
  ├─> wrap the payload in a single Arc<N> shared across handlers
  ├─> build one future per handler, in registration order:
  │     ├─> capture the handler's type name (for failure attribution)
  │     ├─> mint a fresh MessageId
  │     ├─> build MessageEnvelope::for_notification::<N>(message_id, correlation_id)
  │     ├─> build HandlerContext with the shared correlation_id
  │     ├─> wrap handler in NotificationTerminal with payload = Arc::clone(&shared)
  │     ├─> Next::new(middlewares.clone(), terminal)
  │     └─> async move { next.run(&envelope, &ctx).await
  │             .map_err(|error| NotificationFailure { handler, error }) }
  ├─> join_all(futures).await        // cooperative, single task, no spawn
  ├─> collect the Err arms into failures: Vec<NotificationFailure>
  └─> if failures.is_empty() { Ok(()) }
      else { Err(HexeractError::publish_failed(type_name::<N>(), total, failures)) }
```

Four properties fall out of this design:

1. **Concurrent.** Handler futures are built up front and driven together with `join_all`, so a slow handler no longer blocks the handlers registered after it. The fan-out is cooperative and runtime-agnostic: it runs on the caller's task without spawning, so a CPU-bound handler that never `.await`s still blocks its siblings until it yields.
2. **Fail-safe.** Sibling handlers always run, even if a predecessor returns an error. This matches the "audit + email + projection" pattern where you do not want a failing audit to prevent the email from going out.
3. **Structured diagnostics.** Failures are aggregated into `HexeractError::PublishFailed`, which keeps every `NotificationFailure { handler, error }` in registration order. Each entry retains the failing handler's typed error and its `source` chain, so the caller can recover an individual failure instead of parsing a flattened string. Execution is interleaved, so only this *collection* order is deterministic, not the order in which handlers observe the notification.
4. **Causal correlation.** The shared `CorrelationId` lets traces stitch the entire fan-out back to its publish point, even though each handler also has its own `MessageId` for per-handler observability.

## Why type erasure

A naive implementation would put `HashMap<TypeId, Box<dyn CommandHandler<???>>>`, but `CommandHandler<C>` is generic over `C`, and trait objects cannot have generic methods like that. The erased adapters break the generic by downcasting at the boundary, and the public API stays generic on `Mediator::send::<C>` thanks to `TypeId::of::<C>()` retrieving the correct adapter at runtime.

The cost is one downcast per dispatch on the input and one downcast on the output. Both are `Box<dyn Any>::downcast::<T>` and resolve in a few nanoseconds.

## Why the per-dispatch pipeline assembly

Building a `Next` per dispatch (rather than once at `build` time) lets each dispatch carry its own `Terminal` capturing the typed payload. The middleware chain itself (a `Vec<Arc<dyn DynMiddleware>>`) is shared by `Arc` cloning, so the cost of assembling a `Next` is one `Vec::clone` of `Arc`s plus one `Arc::new` for the terminal. Both are constant-time and allocate roughly one cache line per dispatch.

The trade-off is: less state across dispatches (easier reasoning, no cross-dispatch interference), at the cost of a small per-dispatch allocation overhead. Hexeract picks reasoning clarity; if you ever profile this hot path as a bottleneck, the answer is probably to push more work into a middleware that batches at the call site rather than to change this design.
