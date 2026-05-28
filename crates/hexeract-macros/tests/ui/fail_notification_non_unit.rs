use hexeract_core::{HandlerContext, HexeractError, Notification};
use hexeract_macros::handler;

#[derive(Clone)]
struct N;
impl Notification for N {}

struct H;

#[handler(notification)]
impl H {
    async fn handle(&self, _n: N, _ctx: &HandlerContext) -> Result<i32, HexeractError> {
        Ok(0)
    }
}

fn main() {}
