use hexeract_core::{HandlerContext, HexeractError, Notification};
use hexeract_macros::handler;

struct N;
impl Notification for N {}

struct H;

#[handler(notification)]
impl H {
    async fn handle(&self, _n: N, _ctx: &HandlerContext) -> Result<(), HexeractError> {
        Ok(())
    }
}

fn main() {}
