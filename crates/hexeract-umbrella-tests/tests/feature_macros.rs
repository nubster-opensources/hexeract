//! The `macros` facade feature must re-export the `#[handler]` attribute through
//! the `hexeract` umbrella crate, and the code it generates must resolve its
//! paths through the facade alone.

#![cfg(feature = "macros")]
#![allow(
    clippy::unused_async,
    reason = "the handler stays async to match the trait the macro expands to"
)]

use hexeract::core::{Command, HandlerContext, HexeractError};
use hexeract::macros::handler;

struct Ping {
    value: u32,
}

impl Command for Ping {
    type Output = u32;
}

struct PingService;

#[handler(command)]
impl PingService {
    async fn handle(&self, cmd: Ping, _ctx: &HandlerContext) -> Result<u32, HexeractError> {
        Ok(cmd.value + 1)
    }
}

#[test]
fn macros_feature_reexports_the_handler_attribute() {
    assert_eq!(std::mem::size_of::<PingService>(), 0);
}
