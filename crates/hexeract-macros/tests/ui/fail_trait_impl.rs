use hexeract_core::{Command, CommandHandler, HandlerContext, HexeractError};
use hexeract_macros::handler;

struct Cmd;
impl Command for Cmd {
    type Output = ();
}

struct H;

#[handler(command)]
impl CommandHandler<Cmd> for H {
    type Error = HexeractError;
    async fn handle(&self, _c: Cmd, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        Ok(())
    }
}

fn main() {}
