use hexeract_core::{Command, HexeractError};
use hexeract_macros::handler;

struct Cmd;
impl Command for Cmd {
    type Output = ();
}

struct H;

#[handler(command)]
impl H {
    async fn handle(&self, _c: Cmd) -> Result<(), HexeractError> {
        Ok(())
    }
}

fn main() {}
