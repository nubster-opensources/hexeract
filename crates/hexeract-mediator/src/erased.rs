//! Internal type-erased adapters used by the mediator registry.
//!
//! Each public handler trait (`CommandHandler<C>`, `QueryHandler<Q>`,
//! `NotificationHandler<N>`) is wrapped in a `Typed...Handler<_, H>` adapter
//! that implements the matching `Erased...Handler` trait, so the mediator
//! can store every handler behind `Arc<dyn Erased...Handler>` while keeping
//! the public dispatch API strongly typed.

use std::any::{Any, type_name};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use hexeract_core::{
    Command, CommandHandler, HandlerContext, HexeractError, Notification, NotificationHandler,
    Query, QueryHandler,
};

pub(crate) type BoxAny = Box<dyn Any + Send + Sync>;
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub(crate) type BoxOutput = Box<dyn Any + Send + Sync>;

pub(crate) trait ErasedCommandHandler: Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        input: BoxAny,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>>;
}

pub(crate) struct TypedCommandHandler<C, H> {
    handler: H,
    _phantom: PhantomData<fn() -> C>,
}

impl<C, H> TypedCommandHandler<C, H>
where
    C: Command,
    H: CommandHandler<C>,
{
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<C, H> ErasedCommandHandler for TypedCommandHandler<C, H>
where
    C: Command,
    H: CommandHandler<C>,
{
    fn handle<'a>(
        &'a self,
        input: BoxAny,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        Box::pin(async move {
            let command = *input.downcast::<C>().map_err(|_| {
                HexeractError::Dispatch(format!(
                    "command input downcast failed for {}",
                    type_name::<C>()
                ))
            })?;
            let output = self
                .handler
                .handle(command, ctx)
                .await
                .map_err(Into::into)?;
            Ok(Box::new(output) as BoxOutput)
        })
    }
}

pub(crate) trait ErasedQueryHandler: Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        input: BoxAny,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>>;
}

pub(crate) struct TypedQueryHandler<Q, H> {
    handler: H,
    _phantom: PhantomData<fn() -> Q>,
}

impl<Q, H> TypedQueryHandler<Q, H>
where
    Q: Query,
    H: QueryHandler<Q>,
{
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<Q, H> ErasedQueryHandler for TypedQueryHandler<Q, H>
where
    Q: Query,
    H: QueryHandler<Q>,
{
    fn handle<'a>(
        &'a self,
        input: BoxAny,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        Box::pin(async move {
            let query = *input.downcast::<Q>().map_err(|_| {
                HexeractError::Dispatch(format!(
                    "query input downcast failed for {}",
                    type_name::<Q>()
                ))
            })?;
            let output = self.handler.handle(query, ctx).await.map_err(Into::into)?;
            Ok(Box::new(output) as BoxOutput)
        })
    }
}

pub(crate) trait ErasedNotificationHandler: Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        input: BoxAny,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), HexeractError>>;
}

pub(crate) struct TypedNotificationHandler<N, H> {
    handler: H,
    _phantom: PhantomData<fn() -> N>,
}

impl<N, H> TypedNotificationHandler<N, H>
where
    N: Notification,
    H: NotificationHandler<N>,
{
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<N, H> ErasedNotificationHandler for TypedNotificationHandler<N, H>
where
    N: Notification,
    H: NotificationHandler<N>,
{
    fn handle<'a>(
        &'a self,
        input: BoxAny,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), HexeractError>> {
        Box::pin(async move {
            let notification = *input.downcast::<N>().map_err(|_| {
                HexeractError::Dispatch(format!(
                    "notification input downcast failed for {}",
                    type_name::<N>()
                ))
            })?;
            self.handler
                .handle(notification, ctx)
                .await
                .map_err(Into::into)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::{CorrelationId, MessageId};

    fn fresh_ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
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

    #[derive(thiserror::Error, Debug)]
    enum CustomError {
        #[error("boom")]
        Boom,
    }

    impl From<CustomError> for HexeractError {
        fn from(err: CustomError) -> Self {
            HexeractError::HandlerFailed {
                source: Box::new(err),
            }
        }
    }

    struct FailingPingHandler;

    impl CommandHandler<Ping> for FailingPingHandler {
        type Error = CustomError;

        async fn handle(&self, _cmd: Ping, _ctx: &HandlerContext) -> Result<u32, Self::Error> {
            Err(CustomError::Boom)
        }
    }

    #[tokio::test]
    async fn typed_command_handler_routes_through_erased_trait() {
        let typed = TypedCommandHandler::<Ping, _>::new(PingHandler);
        let ctx = fresh_ctx();
        let output = typed
            .handle(Box::new(Ping { value: 21 }), &ctx)
            .await
            .expect("dispatch must succeed");
        let value = *output.downcast::<u32>().expect("output must be u32");
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn typed_command_handler_maps_handler_error_into_hexeract_error() {
        let typed = TypedCommandHandler::<Ping, _>::new(FailingPingHandler);
        let ctx = fresh_ctx();
        let err = typed
            .handle(Box::new(Ping { value: 1 }), &ctx)
            .await
            .expect_err("handler should propagate error");
        assert!(matches!(err, HexeractError::HandlerFailed { .. }));
    }

    #[tokio::test]
    async fn typed_command_handler_reports_downcast_failure() {
        let typed = TypedCommandHandler::<Ping, _>::new(PingHandler);
        let ctx = fresh_ctx();
        let err = typed
            .handle(Box::new(42_u64) as BoxAny, &ctx)
            .await
            .expect_err("wrong input type should fail");
        assert!(matches!(err, HexeractError::Dispatch(_)));
    }

    struct GetCount;

    impl Query for GetCount {
        type Output = i64;
    }

    struct CountHandler {
        value: i64,
    }

    impl QueryHandler<GetCount> for CountHandler {
        type Error = HexeractError;

        async fn handle(&self, _q: GetCount, _ctx: &HandlerContext) -> Result<i64, Self::Error> {
            Ok(self.value)
        }
    }

    #[tokio::test]
    async fn typed_query_handler_routes_through_erased_trait() {
        let typed = TypedQueryHandler::<GetCount, _>::new(CountHandler { value: 7 });
        let ctx = fresh_ctx();
        let output = typed
            .handle(Box::new(GetCount), &ctx)
            .await
            .expect("query dispatch must succeed");
        assert_eq!(*output.downcast::<i64>().unwrap(), 7);
    }

    #[derive(Clone)]
    struct UserSignedUp {
        id: u64,
    }

    impl Notification for UserSignedUp {}

    struct RecordingNotifHandler {
        seen: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
    }

    impl NotificationHandler<UserSignedUp> for RecordingNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            notif: UserSignedUp,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.seen.lock().expect("poisoned").push(notif.id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn typed_notification_handler_routes_through_erased_trait() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let typed = TypedNotificationHandler::<UserSignedUp, _>::new(RecordingNotifHandler {
            seen: std::sync::Arc::clone(&seen),
        });
        let ctx = fresh_ctx();
        typed
            .handle(Box::new(UserSignedUp { id: 99 }), &ctx)
            .await
            .expect("notification dispatch must succeed");
        assert_eq!(seen.lock().unwrap().clone(), vec![99]);
    }
}
