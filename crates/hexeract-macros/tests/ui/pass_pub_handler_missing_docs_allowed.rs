//! A `pub` free-fn handler in a crate that denies `missing_docs` must compile
//! cleanly. The `#[handler]` macro generates a public unit struct for the
//! handler; without a doc comment or `#[allow(missing_docs)]` on that generated
//! struct the lint fires and the user cannot annotate the generated item away
//! at the call site.
#![deny(missing_docs)]

use hexeract_core::{Command, HandlerContext, HexeractError};
use hexeract_macros::handler;

/// A minimal command for this test.
pub struct Ping;

impl Command for Ping {
    type Output = ();
}

/// Handles a [`Ping`] command.
#[handler(command)]
pub async fn handle_ping(
    _cmd: Ping,
    _ctx: &HandlerContext,
) -> Result<(), HexeractError> {
    Ok(())
}

fn main() {}
