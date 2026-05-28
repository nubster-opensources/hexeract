# A handler that holds state (DB pool, configuration, secret store)

Hexeract handlers are regular Rust structs. They can hold any field you can put in a struct: a database pool, a tracing subscriber handle, a feature flag store, a `Arc<Config>` shared with the rest of the application. The mediator does **not** ship a DI container; you construct the handler yourself and pass it to `register_*_handler` at startup.

This page is the recipe for that wiring.

## Recipe

```rust
use std::sync::Arc;

use deadpool_postgres::Pool;
use hexeract::core::{Command, CommandHandler, HandlerContext, HexeractError};

pub struct AppConfig {
    pub feature_send_welcome_email: bool,
    pub default_avatar_url: String,
}

pub struct CreateUser {
    pub email: String,
}

impl Command for CreateUser {
    type Output = u64;
}

pub struct CreateUserHandler {
    pool: Pool,
    config: Arc<AppConfig>,
}

impl CreateUserHandler {
    pub fn new(pool: Pool, config: Arc<AppConfig>) -> Self {
        Self { pool, config }
    }
}

impl CommandHandler<CreateUser> for CreateUserHandler {
    type Error = HexeractError;
    async fn handle(&self, cmd: CreateUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        let _ = &self.config.default_avatar_url;
        let _ = &self.pool;
        // ... persistence logic ...
        Ok(42)
    }
}
```

At startup, build the dependencies, then the handler, then the mediator:

```rust,ignore
let pool = build_pool().await?;
let config = Arc::new(AppConfig {
    feature_send_welcome_email: true,
    default_avatar_url: "https://example.com/avatar.png".to_string(),
});

let handler = CreateUserHandler::new(pool.clone(), Arc::clone(&config));

let mediator = MediatorBuilder::new()
    .register_command_handler::<CreateUser, _>(handler)
    .build()?;
```

The mediator wraps your handler in an internal adapter and stores it behind an `Arc<dyn ErasedCommandHandler>`. **The handler is owned by the mediator from that point on.** You cannot retrieve it; you can only dispatch through it. If you need shared mutable state across dispatches, use an `Arc<Mutex<T>>` or an `Arc<tokio::sync::RwLock<T>>` field on the handler.

## Sharing state across multiple handlers

Two patterns work cleanly.

**Shared `Arc<T>` injection.** Each handler that needs the dependency holds its own `Arc<T>` field. The application constructs the `T` once and clones the `Arc` into each handler. This is what the recipe above does for `config`.

```rust,ignore
let config = Arc::new(AppConfig { /* ... */ });

let mediator = MediatorBuilder::new()
    .register_command_handler::<CreateUser, _>(CreateUserHandler::new(pool.clone(), Arc::clone(&config)))
    .register_command_handler::<DeleteUser, _>(DeleteUserHandler::new(pool.clone(), Arc::clone(&config)))
    .register_query_handler::<GetUser, _>(GetUserHandler::new(pool.clone()))
    .build()?;
```

**Wrapper builder.** If three handlers all need `(pool, config, audit_log)`, write a small helper:

```rust,ignore
struct AppState {
    pool: Pool,
    config: Arc<AppConfig>,
    audit: Arc<AuditLog>,
}

impl AppState {
    fn install(self, builder: MediatorBuilder) -> MediatorBuilder {
        builder
            .register_command_handler::<CreateUser, _>(
                CreateUserHandler::new(self.pool.clone(), Arc::clone(&self.config)))
            .register_command_handler::<DeleteUser, _>(
                DeleteUserHandler::new(self.pool.clone(), Arc::clone(&self.config), Arc::clone(&self.audit)))
            .register_query_handler::<GetUser, _>(
                GetUserHandler::new(self.pool.clone()))
    }
}
```

This concentrates the boilerplate at startup and keeps `main` readable.

## Hot-reloadable configuration

If your configuration is reloadable (config files watched at runtime), store it behind `Arc<arc_swap::ArcSwap<AppConfig>>` instead of `Arc<AppConfig>`. The handler reads the current snapshot on every dispatch:

```rust,ignore
use arc_swap::ArcSwap;

pub struct CreateUserHandler {
    pool: Pool,
    config: Arc<ArcSwap<AppConfig>>,
}

impl CommandHandler<CreateUser> for CreateUserHandler {
    type Error = HexeractError;
    async fn handle(&self, _cmd: CreateUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        let cfg = self.config.load();      // O(1) clone of the Arc
        let _ = &cfg.default_avatar_url;
        Ok(0)
    }
}
```

The reload mechanism (file watcher, signal handler, admin endpoint) calls `self.config.store(Arc::new(new_cfg))` and every subsequent dispatch sees the new snapshot.

## Connection pool sizing

Hexeract is `Send + Sync` and clonable. Once the mediator is built, you can clone it freely and dispatch concurrently. Connection pool sizing decisions live in the pool itself (`deadpool_postgres`, `sqlx::PgPool`, etc.), not in the mediator. The standard `5 * num_cpus` heuristic for `deadpool_postgres` is a reasonable starting point.

## What about per-request state?

If you need per-dispatch state (a request id from an HTTP middleware, a user identity, etc.), do not put it on the handler. Two options:

1. **Carry it in the message payload.** Add a `RequestContext` field to `CreateUser`. Honest and explicit.
2. **Carry it on `HandlerContext`.** Hexeract plans to extend `HandlerContext` with a typed extension map (`tracing`-style) for cross-cutting per-dispatch values. Until then, option 1 is the recommended path.

## Pitfalls

**Holding a `Pool` directly rather than an `Arc<Pool>` is fine.** `deadpool_postgres::Pool` already clones cheaply through an internal `Arc`. Same for `sqlx::PgPool`. Wrapping it in another `Arc` is harmless but redundant.

**`tokio::sync::Mutex` in a handler.** Acceptable but be aware: an `async fn handle` that locks a `tokio::sync::Mutex` will serialize all dispatches for that handler. For commands with single-handler semantics this might be exactly what you want; for queries it serializes reads, which is rarely what you want. Prefer interior mutability through `arc_swap` or `dashmap` for read-heavy paths.
