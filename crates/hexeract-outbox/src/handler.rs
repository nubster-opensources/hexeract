use hexeract_core::HandlerContext;

use crate::Event;
use crate::OutboxError;

/// Asynchronous handler dispatched by the outbox worker for each event of type `E`.
///
/// Implementors describe the side effect to perform when an event lands:
/// write to an audit log, publish to a broker, send a notification, etc.
/// The handler does not return a business value: success is the side
/// effect itself.
///
/// # Idempotency
///
/// The outbox guarantees at-least-once delivery. Handlers MUST therefore
/// be idempotent: the same event can be delivered more than once if a
/// previous attempt crashed between the side effect and the database
/// commit that marks the row as delivered.
///
/// # Example
///
/// ```
/// use hexeract_core::HandlerContext;
/// use hexeract_outbox::{Event, Handler, OutboxError};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct UserRegistered {
///     user_id: uuid::Uuid,
/// }
///
/// impl Event for UserRegistered {
///     const EVENT_TYPE: &'static str = "users.registered";
/// }
///
/// struct AuditWriter;
///
/// impl Handler<UserRegistered> for AuditWriter {
///     type Error = OutboxError;
///
///     async fn handle(
///         &self,
///         event: UserRegistered,
///         _ctx: &HandlerContext,
///     ) -> Result<(), Self::Error> {
///         let _ = event.user_id;
///         Ok(())
///     }
/// }
/// ```
#[trait_variant::make(Send)]
pub trait Handler<E: Event>: Send + Sync + 'static {
    /// Handler-defined error type, convertible into [`OutboxError`].
    type Error: Into<OutboxError> + Send + Sync + 'static;

    /// Process the event and produce its side effect.
    async fn handle(&self, event: E, ctx: &HandlerContext) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::CorrelationId;
    use hexeract_core::MessageId;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn fresh_ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
    }

    fn assert_send<T: Send>(_: &T) {}

    #[derive(Debug, Serialize, Deserialize)]
    struct UserRegistered {
        user_id: Uuid,
    }

    impl Event for UserRegistered {
        const EVENT_TYPE: &'static str = "users.registered";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: Uuid,
    }

    impl Event for OrderPlaced {
        const EVENT_TYPE: &'static str = "orders.placed";
    }

    #[derive(Debug, thiserror::Error)]
    enum AuditError {
        #[error("audit store unavailable")]
        Unavailable,
    }

    impl From<AuditError> for OutboxError {
        fn from(value: AuditError) -> Self {
            Self::Internal(value.to_string())
        }
    }

    struct AuditWriter {
        accept: bool,
    }

    impl Handler<UserRegistered> for AuditWriter {
        type Error = AuditError;
        async fn handle(
            &self,
            _event: UserRegistered,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            if self.accept {
                Ok(())
            } else {
                Err(AuditError::Unavailable)
            }
        }
    }

    #[tokio::test]
    async fn handler_returns_unit_on_success() {
        let writer = AuditWriter { accept: true };
        let ctx = fresh_ctx();
        writer
            .handle(
                UserRegistered {
                    user_id: Uuid::nil(),
                },
                &ctx,
            )
            .await
            .expect("handler should succeed");
    }

    #[tokio::test]
    async fn handler_typed_error_converts_into_outbox_error() {
        let writer = AuditWriter { accept: false };
        let ctx = fresh_ctx();
        let err = writer
            .handle(
                UserRegistered {
                    user_id: Uuid::nil(),
                },
                &ctx,
            )
            .await
            .expect_err("handler should fail");
        assert!(matches!(err, AuditError::Unavailable));
        let outbox_err: OutboxError = err.into();
        assert!(matches!(outbox_err, OutboxError::Internal(_)));
    }

    struct DirectHandler;
    impl Handler<UserRegistered> for DirectHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Err(OutboxError::Internal("forced".into()))
        }
    }

    #[tokio::test]
    async fn handler_can_use_outbox_error_directly() {
        let handler = DirectHandler;
        let ctx = fresh_ctx();
        let err = handler
            .handle(
                UserRegistered {
                    user_id: Uuid::nil(),
                },
                &ctx,
            )
            .await
            .expect_err("must fail");
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[tokio::test]
    async fn handler_future_is_send() {
        let writer = AuditWriter { accept: true };
        let ctx = fresh_ctx();
        let future = writer.handle(
            UserRegistered {
                user_id: Uuid::nil(),
            },
            &ctx,
        );
        assert_send(&future);
        let _ = future.await;
    }

    #[tokio::test]
    async fn handler_runs_in_spawned_task() {
        let writer = Arc::new(AuditWriter { accept: true });
        let cloned = Arc::clone(&writer);
        let result = tokio::spawn(async move {
            let ctx = fresh_ctx();
            cloned
                .handle(
                    UserRegistered {
                        user_id: Uuid::nil(),
                    },
                    &ctx,
                )
                .await
        })
        .await
        .expect("task panicked");
        assert!(result.is_ok());
    }

    struct EchoCtxHandler;
    impl Handler<UserRegistered> for EchoCtxHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            let _ = ctx.message_id;
            let _ = ctx.correlation_id;
            Ok(())
        }
    }

    #[tokio::test]
    async fn handler_reads_ids_from_context() {
        let message_id = MessageId::new();
        let correlation_id = CorrelationId::new();
        let ctx = HandlerContext::new(message_id, correlation_id);

        let handler = EchoCtxHandler;
        handler
            .handle(
                UserRegistered {
                    user_id: Uuid::nil(),
                },
                &ctx,
            )
            .await
            .expect("handler should succeed");

        assert_eq!(ctx.message_id, message_id);
        assert_eq!(ctx.correlation_id, correlation_id);
    }

    struct SleepHandler;
    impl Handler<UserRegistered> for SleepHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            tokio::select! {
                () = ctx.cancellation.cancelled() => Err(OutboxError::Internal("cancelled".into())),
                () = tokio::time::sleep(Duration::from_millis(5_000)) => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn handler_observes_external_cancellation() {
        let ctx = fresh_ctx();
        let token = ctx.cancellation.clone();

        let handle = tokio::spawn(async move {
            let handler = SleepHandler;
            handler
                .handle(
                    UserRegistered {
                        user_id: Uuid::nil(),
                    },
                    &ctx,
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();

        let result = handle.await.expect("task panicked");
        assert!(matches!(result, Err(OutboxError::Internal(ref m)) if m == "cancelled"));
    }

    #[tokio::test]
    async fn handler_is_shareable_via_arc() {
        let handler: Arc<AuditWriter> = Arc::new(AuditWriter { accept: true });
        let h1 = Arc::clone(&handler);
        let h2 = Arc::clone(&handler);

        let t1 = tokio::spawn(async move {
            let ctx = fresh_ctx();
            h1.handle(
                UserRegistered {
                    user_id: Uuid::nil(),
                },
                &ctx,
            )
            .await
        });
        let t2 = tokio::spawn(async move {
            let ctx = fresh_ctx();
            h2.handle(
                UserRegistered {
                    user_id: Uuid::nil(),
                },
                &ctx,
            )
            .await
        });

        let (r1, r2) = tokio::join!(t1, t2);
        assert!(r1.unwrap().is_ok());
        assert!(r2.unwrap().is_ok());
    }

    struct MultiHandler;
    impl Handler<UserRegistered> for MultiHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }
    impl Handler<OrderPlaced> for MultiHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: OrderPlaced,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn one_struct_can_handle_multiple_event_types() {
        let handler = MultiHandler;
        let ctx = fresh_ctx();
        Handler::<UserRegistered>::handle(
            &handler,
            UserRegistered {
                user_id: Uuid::nil(),
            },
            &ctx,
        )
        .await
        .expect("user handler must succeed");
        let ctx = fresh_ctx();
        Handler::<OrderPlaced>::handle(
            &handler,
            OrderPlaced {
                order_id: Uuid::nil(),
            },
            &ctx,
        )
        .await
        .expect("order handler must succeed");
    }
}
