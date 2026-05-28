# Catch missing handler wirings in CI

You annotate handlers with `#[handler]` for the trait impl boilerplate, then register them on `MediatorBuilder` at startup. The risk: someone adds a new `#[handler]` annotated function but forgets to register it. The mediator builds successfully, the service starts, and dispatch fails at runtime with `HexeractError::HandlerNotFound`.

`MediatorBuilder::verify_handlers()` catches this class of bug at startup or, better, in a unit test.

## Recipe: assertion in a unit test

The pattern that puts the check before the bug reaches production:

```rust
use hexeract::core::{Command, HandlerContext, HexeractError};
use hexeract::macros::handler;
use hexeract::mediator::MediatorBuilder;

pub struct CreateUser { pub email: String }
impl Command for CreateUser {
    type Output = u64;
}

pub struct UserService;

#[handler(command)]
impl UserService {
    async fn handle(&self, _cmd: CreateUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        Ok(0)
    }
}

fn build_app_mediator() -> MediatorBuilder {
    MediatorBuilder::new()
        .register_command_handler::<CreateUser, _>(UserService)
        // ... every other handler the application uses ...
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_handler_macro_is_wired() {
        build_app_mediator()
            .verify_handlers()
            .expect("a #[handler] annotated handler is missing from the registry");
    }
}
```

The check runs in CI alongside the rest of your test suite. A new `#[handler]` without a matching `register_*` call breaks the test, and the failure message lists exactly which handlers are missing.

## Reading the error

`HandlersVerificationError::Missing { missing }` carries a `Vec<MissingHandler>` with one entry per declared-but-unregistered handler:

```rust,ignore
match build_app_mediator().verify_handlers() {
    Ok(()) => {}
    Err(hexeract::mediator::HandlersVerificationError::Missing { missing }) => {
        for m in missing {
            eprintln!("{:?}: {} (handler {})", m.kind, m.message_type_name, m.handler_type_name);
        }
        std::process::exit(1);
    }
}
```

Each `MissingHandler` exposes the fully-qualified type names captured by `std::any::type_name` at expansion time:

- `kind`: `HandlerKind::Command`, `Query` or `Notification`.
- `message_type_name`: fully-qualified message type name (`my_app::user::CreateUser`).
- `handler_type_name`: fully-qualified handler type name (`my_app::user::UserService` or, for free fns, the generated `<PascalCaseFnName>Handler` struct).

## When to call `verify_handlers`

| Location | Trade-off |
| --- | --- |
| Unit test (recommended) | Fails before merge; no runtime cost in production |
| Service startup, after `build` | Fails fast at deploy time; takes a few ms |
| Anywhere later in the program | Acceptable but pointless; `inventory` is link-time |

The recommended pattern is the unit test plus an optional startup check for belt-and-suspenders.

## Order of calls

`verify_handlers()` takes `&self`, so it must be called **before** `build()` (which consumes the builder by value). It is safe to call any number of times.

```rust,ignore
let builder = MediatorBuilder::new()
    .register_command_handler::<CreateUser, _>(UserService);

builder.verify_handlers()?;       // first sanity check
let mediator = builder.build()?;  // builder is consumed here
```

If your test wants both the check and a built mediator, the standard idiom is:

```rust,ignore
let builder = build_app_mediator();
builder.verify_handlers().expect("wirings are complete");
let mediator = build_app_mediator().build().expect("build succeeds");
```

Two calls to your `build_app_mediator` helper avoid the move-after-borrow problem.

## What it does *not* catch

`verify_handlers` reports handlers visible in `inventory` but absent from the registry. It does **not** report the inverse: handlers registered through the builder that have no `#[handler]` annotation. That is by design: hand-written handlers without the macro are perfectly valid, and many handlers (especially generic or conditionally registered) cannot use the macro.

It also does not catch:

- **Type mismatches** between `#[handler]` and `register_*_handler`. If you annotate `#[handler(command)]` but then call `register_query_handler`, the compiler accepts both (different registries, different keys); `verify_handlers` will report the command as missing. The mismatch usually manifests at the next dispatch.
- **Multiple handlers for the same command** (the macro emits one registration per `#[handler]` invocation; the registry rejects duplicates separately through `MediatorBuildError::DuplicateHandler`).

## Pitfall: wasm32-unknown-unknown

`inventory` requires platform-specific static init mechanism not available on `wasm32-unknown-unknown`. On that target, `inventory::iter` returns an empty iterator and `verify_handlers` will always return `Ok(())` (vacuously true). Run the check on a host build (`x86_64-unknown-linux-gnu`, `aarch64-apple-darwin`) as part of CI.
