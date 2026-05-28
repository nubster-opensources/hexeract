use hexeract_core::{Command, HandlerContext};
use hexeract_macros::handler;

struct Cmd;
impl Command for Cmd {
    type Output = ();
}

struct H;

#[handler(command)]
impl H {
    async fn handle(&self, _c: Cmd, _ctx: &HandlerContext) -> Vec<u8> {
        Vec::new()
    }
}

fn main() {}
