//! Integration test for `#[handler]` expansion inside `hexeract-core` itself.
//!
//! `proc_macro_crate` returns [`proc_macro_crate::FoundCrate::Itself`] when
//! the macro is expanded in a compilation unit that belongs to `hexeract-core`
//! (integration tests, examples, doctests). Before the fix, the `Itself` arm
//! emitted the bare `crate` keyword as the root path, which refers to the
//! integration-test binary rather than the library — causing every generated
//! path (`crate::CommandHandler`, `crate::HandlerContext`, etc.) to fail to
//! resolve. After the fix the absolute path `::hexeract_core` is emitted,
//! which resolves correctly in all compilation contexts.

use hexeract_core::{Command, HandlerContext, HexeractError};
use hexeract_macros::handler;

struct Greet {
    name: String,
}

impl Command for Greet {
    type Output = String;
}

/// A free-fn command handler registered via `#[handler]` inside `hexeract-core`
/// integration tests. This would fail to compile before the `Itself` path fix.
#[handler(command)]
async fn greet(cmd: Greet, _ctx: &HandlerContext) -> Result<String, HexeractError> {
    Ok(format!("hello {}", cmd.name))
}

#[test]
fn handler_macro_compiles_and_handler_struct_exists() {
    // If the macro generated `crate::CommandHandler` instead of
    // `::hexeract_core::CommandHandler`, the file would not compile at all.
    // Reaching this point proves the generated paths resolve correctly.
    let _h = GreetHandler;
}
