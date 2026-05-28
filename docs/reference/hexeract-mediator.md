# `hexeract-mediator` API reference

Stable surface of the `hexeract-mediator` crate. Re-exported through `hexeract::mediator` when the `mediator` feature is enabled on the umbrella crate.

## `MediatorBuilder`

```rust
pub struct MediatorBuilder { /* ... */ }

impl MediatorBuilder {
    pub fn new() -> Self;
    pub fn register_command_handler<C, H>(self, handler: H) -> Self
    where C: Command, H: CommandHandler<C>;
    pub fn register_query_handler<Q, H>(self, handler: H) -> Self
    where Q: Query, H: QueryHandler<Q>;
    pub fn register_notification_handler<N, H>(self, handler: H) -> Self
    where N: Notification, H: NotificationHandler<N>;
    pub fn with_middleware<M: Middleware>(self, middleware: M) -> Self;
    pub fn verify_handlers(&self) -> Result<(), HandlersVerificationError>;
    pub fn build(self) -> Result<Mediator, MediatorBuildError>;
}
```

Fluent builder; every `register_*` and `with_middleware` returns `Self` by value. Calls accumulate; nothing is consumed until `build`.

**Duplicate detection.** Calling `register_command_handler` or `register_query_handler` twice for the same message type accumulates a `MediatorBuildError::DuplicateHandler`. The first error is surfaced by `build`; fix it and call `build` again to see the next.

**Notification multiplicity.** `register_notification_handler` can be called any number of times for the same `N`; every call appends to the fan-out list.

**Middleware order.** `with_middleware` calls register the middleware at the **outermost** position of the onion. The first one added wraps every subsequent one and the handler itself.

## `Mediator`

```rust
#[derive(Clone)]
pub struct Mediator { /* ... */ }

impl Mediator {
    pub async fn send<C: Command>(&self, command: C) -> Result<C::Output, HexeractError>;
    pub async fn query<Q: Query>(&self, query: Q) -> Result<Q::Output, HexeractError>;
    pub async fn publish<N: Notification>(&self, notification: N) -> Result<(), HexeractError>;
}
```

`Clone` is `O(1)` (shared `Arc<MediatorInner>`); pass clones to spawned tasks freely.

**`send` errors.** Returns `HexeractError::HandlerNotFound { command_type }` if no handler is registered for `C`. Returns the handler's own error wrapped through `Into<HexeractError>` if the handler fails.

**`query` errors.** Same shape as `send` against the query registry.

**`publish` errors.** Returns `Ok(())` if no handler is registered or the handler list is empty. Otherwise, every handler is invoked even if previous handlers failed; failures are aggregated into `HexeractError::Dispatch(format!("publish: N of M handlers failed: ..."))`.

## `MediatorBuildError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum MediatorBuildError {
    #[error("duplicate handler registered for {type_name}")]
    DuplicateHandler { type_name: &'static str },
}
```

Only one variant ships in v0.3.0. The enum is not currently marked `#[non_exhaustive]`; that attribute is planned for v1.0 alongside the broader API freeze, so that adding future variants will remain a non-breaking change.

## `HandlersVerificationError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum HandlersVerificationError {
    #[error("{} handler(s) declared via #[handler] are missing from the registry", missing.len())]
    Missing { missing: Vec<MissingHandler> },
}

#[derive(Debug, Clone)]
pub struct MissingHandler {
    pub kind: HandlerKind,
    pub message_type_name: &'static str,
    pub handler_type_name: &'static str,
}
```

Returned by `MediatorBuilder::verify_handlers`. The `missing` list iterates in `inventory` collection order (effectively link order, which is stable per platform but not portable).

## Type relationships

```text
MediatorBuilder
    ── register_*       → command_handlers / query_handlers / notification_handlers
    ── with_middleware  → middlewares
    ── build            → Mediator { inner: Arc<MediatorInner> }
                              └─ Mediator::send / query / publish

#[handler] from hexeract-macros
    ── inventory::submit!(HandlerRegistration)
                              └─ MediatorBuilder::verify_handlers
```

## Stability

The public types and methods listed above are part of the v0.3.x stable surface and follow the [SemVer policy](../SEMVER_POLICY.md). The crate currently exposes one known dette to be addressed at v1.0:

- **`HexeractError::HandlerNotFound { command_type }`**. The field name `command_type` is populated for queries and notifications too. It will be renamed `message_type` at v1.0.
