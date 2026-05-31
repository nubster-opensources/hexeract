use hexeract_macros::handler;

struct Cmd;

struct GenericHandler<T> {
    _marker: core::marker::PhantomData<T>,
}

#[handler(command)]
impl<T> GenericHandler<T> {
    async fn handle(&self, cmd: Cmd, ctx: &HandlerContext) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

fn main() {}
