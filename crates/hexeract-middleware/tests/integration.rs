//! End-to-end integration tests wiring [`TracingMiddleware`] and
//! [`TimeoutMiddleware`] into a real [`hexeract_mediator::Mediator`].

use std::time::Duration;

use hexeract_core::{Command, CommandHandler, HandlerContext, HexeractError};
use hexeract_mediator::MediatorBuilder;
use hexeract_middleware::{TimeoutMiddleware, TracingMiddleware};

struct Echo {
    value: i32,
    delay: Duration,
}

impl Command for Echo {
    type Output = i32;
}

struct EchoHandler;

impl CommandHandler<Echo> for EchoHandler {
    type Error = HexeractError;

    async fn handle(&self, cmd: Echo, _ctx: &HandlerContext) -> Result<i32, Self::Error> {
        tokio::time::sleep(cmd.delay).await;
        Ok(cmd.value)
    }
}

#[tokio::test]
async fn tracing_then_timeout_succeeds_when_handler_is_fast_enough() {
    let mediator = MediatorBuilder::new()
        .with_middleware(TracingMiddleware::new())
        .with_middleware(TimeoutMiddleware::new(Duration::from_secs(5)))
        .register_command_handler::<Echo, _>(EchoHandler)
        .build()
        .expect("build must succeed");

    let out = mediator
        .send(Echo {
            value: 21,
            delay: Duration::from_millis(5),
        })
        .await
        .expect("dispatch must succeed");
    assert_eq!(out, 21);
}

#[tokio::test(start_paused = true)]
async fn tracing_then_timeout_surfaces_typed_timeout_error_when_handler_is_too_slow() {
    let mediator = MediatorBuilder::new()
        .with_middleware(TracingMiddleware::new())
        .with_middleware(TimeoutMiddleware::new(Duration::from_millis(50)))
        .register_command_handler::<Echo, _>(EchoHandler)
        .build()
        .expect("build must succeed");

    let err = mediator
        .send(Echo {
            value: 0,
            delay: Duration::from_secs(10),
        })
        .await
        .expect_err("dispatch must time out");

    match err {
        HexeractError::Timeout {
            type_name,
            duration,
            ..
        } => {
            assert!(type_name.ends_with("::Echo"));
            assert_eq!(duration, Duration::from_millis(50));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}
