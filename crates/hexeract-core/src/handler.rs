use crate::command::Command;
use crate::context::HandlerContext;
use crate::error::HexeractError;
use crate::query::Query;

/// Asynchronous handler for a [`Command`].
///
/// Each [`Command`] type has exactly one registered `CommandHandler`. The
/// handler receives an immutable reference to itself, the command value and
/// a [`HandlerContext`] carrying tracing and cancellation information.
///
/// # Example
///
/// ```
/// use hexeract_core::{Command, CommandHandler, HandlerContext, HexeractError};
/// use uuid::Uuid;
///
/// struct CreateUser {
///     pub email: String,
/// }
///
/// impl Command for CreateUser {
///     type Output = Uuid;
/// }
///
/// struct UserRepository;
///
/// impl CommandHandler<CreateUser> for UserRepository {
///     type Error = HexeractError;
///
///     async fn handle(
///         &self,
///         cmd: CreateUser,
///         _ctx: &HandlerContext,
///     ) -> Result<Uuid, Self::Error> {
///         let _ = cmd.email;
///         Ok(Uuid::new_v4())
///     }
/// }
/// ```
#[trait_variant::make(Send)]
pub trait CommandHandler<C: Command>: Send + Sync + 'static {
    /// The handler-defined error type, convertible into [`HexeractError`].
    type Error: Into<HexeractError> + Send + Sync + 'static;

    /// Handles the command and produces its output.
    async fn handle(&self, command: C, ctx: &HandlerContext) -> Result<C::Output, Self::Error>;
}

/// Asynchronous handler for a [`Query`].
#[trait_variant::make(Send)]
pub trait QueryHandler<Q: Query>: Send + Sync + 'static {
    /// The handler-defined error type, convertible into [`HexeractError`].
    type Error: Into<HexeractError> + Send + Sync + 'static;

    /// Handles the query and produces its output.
    async fn handle(&self, query: Q, ctx: &HandlerContext) -> Result<Q::Output, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{CorrelationId, MessageId};
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn fresh_ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
    }

    fn assert_send<T: Send>(_: &T) {}

    #[derive(Debug, PartialEq, Eq, Clone)]
    struct UserCreated {
        id: Uuid,
        email: String,
    }

    struct CreateUser {
        email: String,
    }

    impl Command for CreateUser {
        type Output = UserCreated;
    }

    #[derive(Debug, thiserror::Error)]
    enum UserError {
        #[error("invalid email")]
        InvalidEmail,
    }

    impl From<UserError> for HexeractError {
        fn from(value: UserError) -> Self {
            Self::handler_failed(value)
        }
    }

    struct UserRepo {
        prefix: String,
    }

    impl CommandHandler<CreateUser> for UserRepo {
        type Error = UserError;
        async fn handle(
            &self,
            cmd: CreateUser,
            _ctx: &HandlerContext,
        ) -> Result<UserCreated, Self::Error> {
            if cmd.email.is_empty() {
                return Err(UserError::InvalidEmail);
            }
            Ok(UserCreated {
                id: Uuid::new_v4(),
                email: format!("{}-{}", self.prefix, cmd.email),
            })
        }
    }

    #[tokio::test]
    async fn command_handler_returns_complex_output() {
        let repo = UserRepo {
            prefix: "test".into(),
        };
        let ctx = fresh_ctx();
        let result = repo
            .handle(
                CreateUser {
                    email: "alice@example.com".into(),
                },
                &ctx,
            )
            .await
            .expect("handler should succeed");
        assert_eq!(result.email, "test-alice@example.com");
    }

    #[tokio::test]
    async fn command_handler_returns_typed_error_for_invalid_input() {
        let repo = UserRepo {
            prefix: "test".into(),
        };
        let ctx = fresh_ctx();
        let err = repo
            .handle(
                CreateUser {
                    email: String::new(),
                },
                &ctx,
            )
            .await
            .expect_err("empty email must fail");
        assert!(matches!(err, UserError::InvalidEmail));
        let framework_err: HexeractError = err.into();
        assert!(matches!(framework_err, HexeractError::HandlerFailed { .. }));
    }

    #[tokio::test]
    async fn handler_future_is_send() {
        let repo = UserRepo {
            prefix: "send".into(),
        };
        let ctx = fresh_ctx();
        let future = repo.handle(
            CreateUser {
                email: "send@test".into(),
            },
            &ctx,
        );
        assert_send(&future);
        let _ = future.await;
    }

    #[tokio::test]
    async fn handler_runs_in_spawned_task() {
        let repo = Arc::new(UserRepo {
            prefix: "spawn".into(),
        });
        let cloned = Arc::clone(&repo);
        let result = tokio::spawn(async move {
            let ctx = fresh_ctx();
            cloned.handle(CreateUser { email: "ok".into() }, &ctx).await
        })
        .await
        .expect("task panicked");
        assert!(result.is_ok());
    }

    struct DirectErrorHandler;
    impl CommandHandler<CreateUser> for DirectErrorHandler {
        type Error = HexeractError;
        async fn handle(
            &self,
            _cmd: CreateUser,
            _ctx: &HandlerContext,
        ) -> Result<UserCreated, Self::Error> {
            Err(HexeractError::Dispatch("forced".into()))
        }
    }

    #[tokio::test]
    async fn handler_can_use_hexeract_error_directly_as_error_type() {
        let handler = DirectErrorHandler;
        let ctx = fresh_ctx();
        let err = handler
            .handle(
                CreateUser {
                    email: "any".into(),
                },
                &ctx,
            )
            .await
            .expect_err("must fail");
        assert!(matches!(err, HexeractError::Dispatch(_)));
    }

    struct EchoIdsHandler;
    struct EchoIds;
    impl Command for EchoIds {
        type Output = (MessageId, CorrelationId);
    }

    impl CommandHandler<EchoIds> for EchoIdsHandler {
        type Error = HexeractError;
        async fn handle(
            &self,
            _cmd: EchoIds,
            ctx: &HandlerContext,
        ) -> Result<(MessageId, CorrelationId), Self::Error> {
            Ok((ctx.message_id, ctx.correlation_id))
        }
    }

    #[tokio::test]
    async fn handler_reads_message_and_correlation_ids_from_context() {
        let message_id = MessageId::new();
        let correlation_id = CorrelationId::new();
        let ctx = HandlerContext::new(message_id, correlation_id);

        let handler = EchoIdsHandler;
        let (got_msg, got_corr) = handler
            .handle(EchoIds, &ctx)
            .await
            .expect("handler should succeed");
        assert_eq!(got_msg, message_id);
        assert_eq!(got_corr, correlation_id);
    }

    struct SleepHandler;
    struct SleepFor(u64);
    impl Command for SleepFor {
        type Output = &'static str;
    }

    impl CommandHandler<SleepFor> for SleepHandler {
        type Error = HexeractError;
        async fn handle(
            &self,
            cmd: SleepFor,
            ctx: &HandlerContext,
        ) -> Result<&'static str, Self::Error> {
            tokio::select! {
                () = ctx.cancellation.cancelled() => Err(HexeractError::Dispatch("cancelled".into())),
                () = tokio::time::sleep(Duration::from_millis(cmd.0)) => Ok("completed"),
            }
        }
    }

    #[tokio::test]
    async fn handler_observes_external_cancellation() {
        let ctx = fresh_ctx();
        let token = ctx.cancellation.clone();

        let handle = tokio::spawn(async move {
            let handler = SleepHandler;
            handler.handle(SleepFor(5_000), &ctx).await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();

        let result = handle.await.expect("task panicked");
        assert!(matches!(result, Err(HexeractError::Dispatch(ref m)) if m == "cancelled"));
    }

    #[tokio::test]
    async fn handler_is_shareable_via_arc() {
        let handler: Arc<UserRepo> = Arc::new(UserRepo {
            prefix: "arc".into(),
        });
        let h1 = Arc::clone(&handler);
        let h2 = Arc::clone(&handler);

        let t1 = tokio::spawn(async move {
            let ctx = fresh_ctx();
            h1.handle(CreateUser { email: "u1".into() }, &ctx).await
        });
        let t2 = tokio::spawn(async move {
            let ctx = fresh_ctx();
            h2.handle(CreateUser { email: "u2".into() }, &ctx).await
        });

        let (r1, r2) = tokio::join!(t1, t2);
        assert!(r1.unwrap().is_ok());
        assert!(r2.unwrap().is_ok());
    }

    #[derive(Debug)]
    struct UserSummary {
        id: Uuid,
    }

    struct FindUser {
        id: Uuid,
    }

    impl Query for FindUser {
        type Output = Option<UserSummary>;
    }

    struct UserFinder;

    impl QueryHandler<FindUser> for UserFinder {
        type Error = HexeractError;
        async fn handle(
            &self,
            query: FindUser,
            _ctx: &HandlerContext,
        ) -> Result<Option<UserSummary>, Self::Error> {
            Ok(Some(UserSummary { id: query.id }))
        }
    }

    #[tokio::test]
    async fn query_handler_returns_output() {
        let id = Uuid::new_v4();
        let handler = UserFinder;
        let ctx = fresh_ctx();
        let result = handler
            .handle(FindUser { id }, &ctx)
            .await
            .expect("query should succeed");
        assert_eq!(result.unwrap().id, id);
    }

    #[tokio::test]
    async fn query_handler_future_is_send() {
        let handler = UserFinder;
        let ctx = fresh_ctx();
        let future = handler.handle(FindUser { id: Uuid::new_v4() }, &ctx);
        assert_send(&future);
        let _ = future.await;
    }

    #[tokio::test]
    async fn query_handler_runs_in_spawned_task() {
        let handler = Arc::new(UserFinder);
        let cloned = Arc::clone(&handler);
        let result = tokio::spawn(async move {
            let ctx = fresh_ctx();
            cloned.handle(FindUser { id: Uuid::new_v4() }, &ctx).await
        })
        .await
        .expect("task panicked");
        assert!(result.is_ok());
    }

    struct FailingQuery;
    impl QueryHandler<FindUser> for FailingQuery {
        type Error = UserError;
        async fn handle(
            &self,
            _query: FindUser,
            _ctx: &HandlerContext,
        ) -> Result<Option<UserSummary>, Self::Error> {
            Err(UserError::InvalidEmail)
        }
    }

    #[tokio::test]
    async fn query_handler_error_converts_into_hexeract_error() {
        let handler = FailingQuery;
        let ctx = fresh_ctx();
        let err = handler
            .handle(FindUser { id: Uuid::new_v4() }, &ctx)
            .await
            .expect_err("must fail");
        let framework_err: HexeractError = err.into();
        assert!(matches!(framework_err, HexeractError::HandlerFailed { .. }));
    }
}
