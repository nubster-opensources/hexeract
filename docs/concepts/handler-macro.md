# The `#[handler]` macro

`#[handler]` is the attribute proc-macro from `hexeract-macros` that generates the trait implementation of `CommandHandler`, `QueryHandler` or `NotificationHandler` from a more compact signature, and submits a metadata entry to the [`inventory`](https://docs.rs/inventory) collector so that `MediatorBuilder::verify_handlers()` can detect handlers declared via the macro but never registered.

## Two forms

**On an inherent `impl` block**, the macro inspects the `async fn handle` method and generates the matching trait impl alongside the original block:

```rust
use hexeract::core::{Command, HandlerContext, HexeractError};
use hexeract::macros::handler;

struct CreateUser { email: String }
impl Command for CreateUser {
    type Output = u64;
}

pub struct UserService;

#[handler(command)]
impl UserService {
    async fn handle(&self, cmd: CreateUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        // ... real persistence here ...
        Ok(u64::try_from(cmd.email.len()).unwrap_or(0))
    }
}
```

The macro generates `impl CommandHandler<CreateUser> for UserService { ... }` that forwards to your `handle` method. You still register the handler explicitly via the fluent builder; the macro never auto-instantiates.

**On a free `async fn`**, the macro generates a unit struct wrapper named `<PascalCaseFnName>Handler`:

```rust
use hexeract::core::{HandlerContext, HexeractError, Query};
use hexeract::macros::handler;

struct ListUsers;
impl Query for ListUsers {
    type Output = u32;
}

#[handler(query)]
async fn list_users(_q: ListUsers, _ctx: &HandlerContext) -> Result<u32, HexeractError> {
    Ok(42)
}

// Generated: pub struct ListUsersHandler;
//            impl QueryHandler<ListUsers> for ListUsersHandler { ... }
```

The wrapper struct is `pub` if the original function is `pub`, otherwise it inherits the function's visibility. Register it as `ListUsersHandler`.

## Why the kind is mandatory

Command and query handlers have **identical signatures**: `handle(&self, msg: M, ctx: &HandlerContext) -> Result<T, E>`. There is no way to distinguish them by parsing alone, so `#[handler]` requires an explicit kind argument: `command`, `query` or `notification`. The macro fails at compile time with a clear diagnostic if the argument is missing, unknown, or inconsistent with the signature (a notification handler must return `Result<(), E>`).

## Why you still register explicitly

Hexeract deliberately does not auto-instantiate handlers from inventory metadata. Real-world handlers carry state: a database pool, a configuration struct, a feature flag store. None of that is reconstructible from a `&'static str` collected at link time.

The macro emits a `HandlerRegistration` entry alongside the trait impl, and `MediatorBuilder::verify_handlers()` cross-checks the entries against the registered handlers, returning `HandlersVerificationError::Missing` with the list of declared-but-not-registered handlers. The check is a sanity guard for typos and forgotten wirings, not a runtime auto-discovery mechanism.

```rust
use hexeract::mediator::MediatorBuilder;

# struct UserService;
# struct CreateUser;
# impl hexeract::core::Command for CreateUser { type Output = (); }
# impl hexeract::core::CommandHandler<CreateUser> for UserService {
#     type Error = hexeract::core::HexeractError;
#     async fn handle(&self, _: CreateUser, _: &hexeract::core::HandlerContext) -> Result<(), hexeract::core::HexeractError> { Ok(()) }
# }
let builder = MediatorBuilder::new()
    .register_command_handler::<CreateUser, _>(UserService);

builder.verify_handlers().expect("every #[handler] is wired");

let mediator = builder.build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Call `verify_handlers` **before** `build` (which consumes the builder). It is safe to call multiple times: it takes `&self` and never mutates state.

## How link-time discovery works

The macro expansion contains:

```rust
::hexeract_core::registration::__private::inventory::submit!(
    ::hexeract_core::HandlerRegistration {
        kind: ::hexeract_core::HandlerKind::Command,
        message_type_name: ::core::any::type_name::<CreateUser>,
        handler_type_name: ::core::any::type_name::<UserService>,
    }
);
```

`inventory` registers the entry at link time through platform-specific init sections (`.init_array` on Linux, `__DATA,__mod_init_func` on macOS, the CRT init table on Windows). `MediatorBuilder::verify_handlers` iterates the collected entries via `inventory::iter::<HandlerRegistration>` and compares the message type names against the registry.

`message_type_name` and `handler_type_name` are stored as `fn() -> &'static str` rather than `&'static str` because `std::any::type_name::<T>()` is not yet a `const fn` on stable. `inventory::submit!` requires a const-initialized static; storing the function pointer (which **is** const-compatible) defers the name resolution to call time.

## Caveats

- **Generic handlers** are not supported in the MVP. A `#[handler] impl<T> Service<T> { async fn handle ... }` will fail at expansion because `inventory::submit!` cannot capture the generic context. Concrete `impl Service { ... }` blocks are the supported shape.
- **`wasm32-unknown-unknown`** is not supported by `inventory` itself: no static init mechanism in pure wasm. The macro still expands, but `verify_handlers` returns an empty inventory.
- **Naming collisions**: if your free function is `create_user` and you already have a `CreateUserHandler` struct in scope, the macro's generated struct will collide. Pick a different function name or use the impl form.

## Compile-fail diagnostics

The macro emits typed errors for every malformed input. Selected examples:

```text
error: #[handler] requires a kind argument: #[handler(command)], #[handler(query)] or #[handler(notification)]
error: unknown handler kind `event`; expected `command`, `query` or `notification`
error: #[handler] must annotate a bare inherent impl, not a trait implementation
error: `handle` must be `async`
error: `handle` must take exactly 3 arguments
error: a notification handler must return `Result<(), Error>`
```

The full UI snapshot suite is in `crates/hexeract-macros/tests/ui/`.
