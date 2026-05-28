use hexeract_core::{Command, HandlerContext, HexeractError};
use hexeract_macros::handler;

struct Cmd;
impl Command for Cmd {
    type Output = ();
}

struct H;

#[handler(event)]
impl H {
    async fn handle(&self, _c: Cmd, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        Ok(())
    }
}

fn main() {}
