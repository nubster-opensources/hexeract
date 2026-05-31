use hexeract_macros::handler;

struct Cmd;

struct CmdHandler;

#[handler(command)]
impl CmdHandler {
    async fn handle(&self, cmd: Cmd, ctx: u32) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

fn main() {}
