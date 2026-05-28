use hexeract_core::{Query, HandlerContext, HexeractError};
use hexeract_macros::handler;

struct Q;
impl Query for Q {
    type Output = u32;
}

#[handler(query)]
fn list_things(_q: Q, _ctx: &HandlerContext) -> Result<u32, HexeractError> {
    Ok(0)
}

fn main() {}
