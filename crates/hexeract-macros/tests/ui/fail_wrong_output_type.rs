use hexeract_core::{Command, HandlerContext, HexeractError};
use hexeract_macros::handler;

struct Ping;

impl Command for Ping {
    type Output = u32;
}

struct PingService;

// The inherent `handle` returns `String`, but `<Ping as Command>::Output` is
// `u32`. Since the generated trait signature is derived from the associated
// `Output` type, the body coercion must fail to compile.
#[handler(command)]
impl PingService {
    async fn handle(&self, _cmd: Ping, _ctx: &HandlerContext) -> Result<String, HexeractError> {
        Ok(String::new())
    }
}

fn main() {}
