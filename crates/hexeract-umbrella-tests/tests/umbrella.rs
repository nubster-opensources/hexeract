//! Compiles and dispatches a handler depending only on the `hexeract`
//! umbrella crate, exercising `#[handler]` path resolution through the facade.

#![allow(
    clippy::unused_async,
    reason = "the handler stays async to match the trait the macro expands to"
)]

use hexeract::core::{Command, HandlerContext, HexeractError};
use hexeract::macros::handler;
use hexeract::mediator::MediatorBuilder;

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

#[tokio::test]
async fn handler_macro_dispatches_through_umbrella_crate() {
    let mediator = MediatorBuilder::new()
        .register_command_handler::<Ping, _>(PingService)
        .build()
        .expect("build must succeed");

    let output = mediator
        .send(Ping { value: 41 })
        .await
        .expect("dispatch must succeed");

    assert_eq!(output, 42);
}
