use hexeract_macros::handler;

struct SomeCmd;

struct SomeCmdHandler;

#[handler(command)]
impl SomeCmdHandler {
    async fn handle(
        &self,
        cmd: &SomeCmd,
        ctx: &HandlerContext,
    ) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

fn main() {}
