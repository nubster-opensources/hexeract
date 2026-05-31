use hexeract_macros::handler;

struct Cmd;

struct LifetimeHandler<'a> {
    _marker: core::marker::PhantomData<&'a ()>,
}

#[handler(command)]
impl<'a> LifetimeHandler<'a> {
    async fn handle(&self, cmd: Cmd, ctx: &HandlerContext) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

fn main() {}
