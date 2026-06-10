use hexeract_core::{HandlerContext, HexeractError, Notification};
use hexeract_macros::handler;

mod foo {
    pub use std::sync::Arc;
}

struct N;
impl Notification for N {}

struct H;

#[handler(notification)]
impl H {
    async fn handle(&self, _n: foo::Arc<N>, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        Ok(())
    }
}

fn main() {}
