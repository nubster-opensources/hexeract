use hexeract_macros::handler;

struct TupleHandler;

#[handler(command)]
impl TupleHandler {
    async fn handle(
        &self,
        msg: (u8, u8),
        ctx: &HandlerContext,
    ) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

fn main() {}
