//! Per-dispatch terminals plugged into the middleware pipeline.
//!
//! `Next::run` calls `Terminal::dispatch(&self, envelope, ctx)` once the
//! middleware chain is exhausted. Because `dispatch` takes `&self`, the
//! command or query value cannot be moved out of the terminal directly;
//! it is parked in a `Mutex<Option<_>>` and taken on the first call.
//! Re-entry (a middleware that calls `next.run` twice) is detected and
//! surfaced as `HexeractError::Dispatch`.
//!
//! The lock is only ever held across the `.take()` call, so poisoning it
//! is practically unreachable today. Still, the payload lock is recovered
//! with `unwrap_or_else(PoisonError::into_inner)` rather than unwrapped: a
//! future panic while the guard is held must not turn every later dispatch
//! through this terminal into a caller panic.

use std::sync::{Arc, Mutex, PoisonError};

use hexeract_core::{HandlerContext, HexeractError, MessageEnvelope, Terminal};

use crate::erased::{
    BoxAny, BoxFuture, BoxOutput, ErasedCommandHandler, ErasedNotificationHandler,
    ErasedQueryHandler,
};

pub(crate) struct CommandTerminal {
    pub(crate) handler: Arc<dyn ErasedCommandHandler>,
    pub(crate) payload: Mutex<Option<BoxAny>>,
}

impl Terminal for CommandTerminal {
    fn dispatch<'a>(
        &'a self,
        _envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        let payload = self
            .payload
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Box::pin(async move {
            let Some(payload) = payload else {
                return Err(HexeractError::Dispatch(
                    "command terminal called twice".into(),
                ));
            };
            self.handler.handle(payload, ctx).await
        })
    }
}

pub(crate) struct QueryTerminal {
    pub(crate) handler: Arc<dyn ErasedQueryHandler>,
    pub(crate) payload: Mutex<Option<BoxAny>>,
}

impl Terminal for QueryTerminal {
    fn dispatch<'a>(
        &'a self,
        _envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        let payload = self
            .payload
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Box::pin(async move {
            let Some(payload) = payload else {
                return Err(HexeractError::Dispatch(
                    "query terminal called twice".into(),
                ));
            };
            self.handler.handle(payload, ctx).await
        })
    }
}

pub(crate) struct NotificationTerminal {
    pub(crate) handler: Arc<dyn ErasedNotificationHandler>,
    pub(crate) payload: Mutex<Option<BoxAny>>,
}

impl Terminal for NotificationTerminal {
    fn dispatch<'a>(
        &'a self,
        _envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        let payload = self
            .payload
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Box::pin(async move {
            let Some(payload) = payload else {
                return Err(HexeractError::Dispatch(
                    "notification terminal called twice".into(),
                ));
            };
            self.handler.handle(payload, ctx).await?;
            Ok(Box::new(()) as BoxOutput)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::Arc as StdArc;

    use hexeract_core::{
        Command, CommandHandler, CorrelationId, MessageId, Notification, NotificationHandler,
        Query, QueryHandler,
    };

    use super::*;
    use crate::erased::{TypedCommandHandler, TypedNotificationHandler, TypedQueryHandler};

    fn fresh_ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
    }

    /// Locks `lock`, panics while the guard is alive, and asserts that the
    /// panic poisoned it. Mirrors the only realistic way a handler panic
    /// could poison the payload lock: holding the guard across a panicking
    /// call.
    fn poison<T>(lock: &Mutex<T>) {
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = lock
                .lock()
                .expect("lock must be acquirable before the simulated poisoning");
            panic!("simulated handler panic while holding the payload lock");
        }));
        assert!(
            outcome.is_err(),
            "the closure must panic to poison the lock"
        );
        assert!(
            lock.is_poisoned(),
            "lock must be poisoned by the panic above"
        );
    }

    struct Ping {
        value: u32,
    }

    impl Command for Ping {
        type Output = u32;
    }

    struct PingHandler;

    impl CommandHandler<Ping> for PingHandler {
        type Error = HexeractError;

        async fn handle(&self, cmd: Ping, _ctx: &HandlerContext) -> Result<u32, Self::Error> {
            Ok(cmd.value * 2)
        }
    }

    #[tokio::test]
    async fn command_terminal_dispatch_recovers_a_poisoned_payload_lock() {
        let handler: StdArc<dyn ErasedCommandHandler> =
            StdArc::new(TypedCommandHandler::<Ping, _>::new(PingHandler));
        let terminal = CommandTerminal {
            handler,
            payload: Mutex::new(Some(Box::new(Ping { value: 21 }) as BoxAny)),
        };
        poison(&terminal.payload);

        let envelope = MessageEnvelope::for_command::<Ping>(MessageId::new(), CorrelationId::new());
        let ctx = fresh_ctx();
        let output = terminal
            .dispatch(&envelope, &ctx)
            .await
            .expect("dispatch must recover from a poisoned lock instead of panicking");
        let value = *output.downcast::<u32>().expect("output must be u32");
        assert_eq!(value, 42);
    }

    struct GetCount;

    impl Query for GetCount {
        type Output = i64;
    }

    struct CountHandler;

    impl QueryHandler<GetCount> for CountHandler {
        type Error = HexeractError;

        async fn handle(&self, _q: GetCount, _ctx: &HandlerContext) -> Result<i64, Self::Error> {
            Ok(7)
        }
    }

    #[tokio::test]
    async fn query_terminal_dispatch_recovers_a_poisoned_payload_lock() {
        let handler: StdArc<dyn ErasedQueryHandler> =
            StdArc::new(TypedQueryHandler::<GetCount, _>::new(CountHandler));
        let terminal = QueryTerminal {
            handler,
            payload: Mutex::new(Some(Box::new(GetCount) as BoxAny)),
        };
        poison(&terminal.payload);

        let envelope =
            MessageEnvelope::for_query::<GetCount>(MessageId::new(), CorrelationId::new());
        let ctx = fresh_ctx();
        let output = terminal
            .dispatch(&envelope, &ctx)
            .await
            .expect("dispatch must recover from a poisoned lock instead of panicking");
        let value = *output.downcast::<i64>().expect("output must be i64");
        assert_eq!(value, 7);
    }

    #[derive(Clone)]
    struct UserSignedUp {
        id: u64,
    }

    impl Notification for UserSignedUp {}

    struct NoopNotifHandler {
        seen: StdArc<Mutex<Vec<u64>>>,
    }

    impl NotificationHandler<UserSignedUp> for NoopNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            notif: StdArc<UserSignedUp>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(notif.id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn notification_terminal_dispatch_recovers_a_poisoned_payload_lock() {
        let seen = StdArc::new(Mutex::new(Vec::new()));
        let handler: StdArc<dyn ErasedNotificationHandler> = StdArc::new(
            TypedNotificationHandler::<UserSignedUp, _>::new(NoopNotifHandler {
                seen: StdArc::clone(&seen),
            }),
        );
        let payload = Box::new(StdArc::new(UserSignedUp { id: 99 })) as BoxAny;
        let terminal = NotificationTerminal {
            handler,
            payload: Mutex::new(Some(payload)),
        };
        poison(&terminal.payload);

        let envelope = MessageEnvelope::for_notification::<UserSignedUp>(
            MessageId::new(),
            CorrelationId::new(),
        );
        let ctx = fresh_ctx();
        terminal
            .dispatch(&envelope, &ctx)
            .await
            .expect("dispatch must recover from a poisoned lock instead of panicking");
        assert_eq!(
            seen.lock().unwrap_or_else(PoisonError::into_inner).clone(),
            vec![99]
        );
    }

    #[tokio::test]
    async fn command_terminal_still_rejects_a_second_dispatch_after_recovery() {
        let handler: StdArc<dyn ErasedCommandHandler> =
            StdArc::new(TypedCommandHandler::<Ping, _>::new(PingHandler));
        let terminal = CommandTerminal {
            handler,
            payload: Mutex::new(Some(Box::new(Ping { value: 1 }) as BoxAny)),
        };
        poison(&terminal.payload);

        let envelope = MessageEnvelope::for_command::<Ping>(MessageId::new(), CorrelationId::new());
        let ctx = fresh_ctx();
        terminal
            .dispatch(&envelope, &ctx)
            .await
            .expect("first dispatch after recovery must still succeed");

        let err = terminal
            .dispatch(&envelope, &ctx)
            .await
            .expect_err("re-entrant dispatch must still be rejected");
        assert!(matches!(err, HexeractError::Dispatch(_)));
    }
}
