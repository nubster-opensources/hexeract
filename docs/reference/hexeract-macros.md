# `hexeract-macros` API reference

Stable surface of the `hexeract-macros` crate. Re-exported through `hexeract::macros` when the `macros` feature is enabled on the umbrella crate.

The crate exposes a single procedural macro: `#[handler]`. Its companion runtime types live in `hexeract-core::registration` (`HandlerKind`, `HandlerRegistration`) and `hexeract-mediator` (`MissingHandler`, `HandlersVerificationError`).

## `#[handler]`

```rust
#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream;
```

### Syntax

```rust
#[handler(command|query|notification)]
```

The kind argument is mandatory: `command`, `query` or `notification`. Omitting it or passing any other identifier is a compile error.

### Supported items

**Inherent `impl` block.** The macro reads the `handle` method's signature and generates the matching trait impl.

```rust,ignore
#[handler(command)]
impl MyService {
    async fn handle(&self, cmd: MyCommand, ctx: &HandlerContext) -> Result<MyOutput, MyError> {
        // ...
    }
}
```

The original block is preserved verbatim. The generated trait impl is appended alongside, plus an `inventory::submit!` call.

**Free `async fn`.** The macro generates a `pub struct <PascalCaseFnName>Handler;` and the trait impl that forwards to the function.

```rust,ignore
#[handler(query)]
async fn list_users(q: ListUsers, ctx: &HandlerContext) -> Result<Vec<User>, MyError> {
    // ...
}
// Generated: pub struct ListUsersHandler;
//            impl QueryHandler<ListUsers> for ListUsersHandler { ... }
```

The generated struct inherits the function's visibility (`pub` if the fn is `pub`, otherwise crate-private).

### Required signatures

| Form | Signature |
| --- | --- |
| Impl block, command | `async fn handle(&self, msg: C, ctx: &HandlerContext) -> Result<C::Output, E>` |
| Impl block, query | `async fn handle(&self, msg: Q, ctx: &HandlerContext) -> Result<Q::Output, E>` |
| Impl block, notification | `async fn handle(&self, msg: N, ctx: &HandlerContext) -> Result<(), E>` |
| Free fn, command | `async fn name(msg: C, ctx: &HandlerContext) -> Result<C::Output, E>` |
| Free fn, query | `async fn name(msg: Q, ctx: &HandlerContext) -> Result<Q::Output, E>` |
| Free fn, notification | `async fn name(msg: N, ctx: &HandlerContext) -> Result<(), E>` |

`E` must implement `Into<HexeractError>`. `&HandlerContext` is the literal expected type; the second argument name is free.

### Diagnostics

The macro fails at compile time on every malformed input with a typed `compile_error!`. Selected messages:

```text
error: #[handler] requires a kind argument: #[handler(command)], #[handler(query)] or #[handler(notification)]
error: unknown handler kind `event`; expected `command`, `query` or `notification`
error: #[handler] must annotate a bare inherent impl, not a trait implementation
error: `handle` must be `async`
error: `handle` must take `&self` as first argument
error: `handle` must take exactly 3 arguments
error: function must take exactly 2 arguments
error: handler must return `Result<Output, Error>`
error: a notification handler must return `Result<(), Error>`
```

The full UI snapshot suite is checked in at `crates/hexeract-macros/tests/ui/`.

## Companion runtime types

These types live in `hexeract-core::registration` and are re-exported at the crate root:

```rust
pub enum HandlerKind {
    Command,
    Query,
    Notification,
}

pub struct HandlerRegistration {
    pub kind: HandlerKind,
    pub message_type_name: fn() -> &'static str,
    pub handler_type_name: fn() -> &'static str,
}
```

**`fn() -> &'static str` rather than `&'static str`** because `std::any::type_name::<T>()` is not yet `const fn` on stable, while `inventory::submit!` requires a const-initialized value. Storing the function pointer (which **is** const) defers name resolution to call time.

Consumers usually do not touch `HandlerRegistration` directly. The standard interaction is `MediatorBuilder::verify_handlers`, documented in [`hexeract-mediator` reference](hexeract-mediator.md).

## Platform support

`#[handler]` itself works everywhere `syn` / `quote` / `proc-macro2` work, which is every standard Rust target.

`inventory::submit!` requires platform-specific static init mechanism (`.init_array` on Linux, `__DATA,__mod_init_func` on macOS, the CRT init table on Windows). Targets supported by `inventory` 0.3:

- All `x86_64-*` and `aarch64-*` mainstream tier-1 and tier-2 targets.
- All major UNIX-likes (Linux, macOS, FreeBSD, NetBSD).
- Windows MSVC and GNU.
- iOS, Android.

**Not supported**: `wasm32-unknown-unknown`. The macro still expands but `inventory::iter` returns an empty iterator. `verify_handlers` will report every annotated handler as "missing" on wasm32-unknown-unknown.
