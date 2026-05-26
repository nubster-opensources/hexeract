use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use hexeract_core::HandlerContext;

use crate::BusEnvelope;
use crate::BusError;
use crate::Message;

/// Pinned, boxed, send future returned by trait object methods.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Asynchronous handler dispatched by the bus consumer for each message of type `M`.
///
/// Implementors describe the side effect to run when a message lands:
/// project a read model, forward to a downstream system, emit a
/// notification, etc. The handler does not return a business value:
/// success is the side effect itself.
///
/// `Self::Error` lets handlers report failures through a domain-specific
/// error type that the consumer worker converts back into a
/// [`BusError`] when it records the failure.
///
/// # Idempotency
///
/// The bus offers at-least-once delivery semantics. Handlers MUST
/// therefore be idempotent: the same message can be redelivered if a
/// previous attempt crashed between the side effect and the consumer's
/// ack.
///
/// # Example
///
/// ```
/// use hexeract_bus::{BusError, Handler, Message};
/// use hexeract_core::HandlerContext;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct OrderPlaced {
///     order_id: uuid::Uuid,
/// }
///
/// impl Message for OrderPlaced {
///     const MESSAGE_TYPE: &'static str = "orders.placed";
/// }
///
/// struct Projector;
///
/// impl Handler<OrderPlaced> for Projector {
///     type Error = BusError;
///
///     async fn handle(
///         &self,
///         message: OrderPlaced,
///         _ctx: &HandlerContext,
///     ) -> Result<(), Self::Error> {
///         let _ = message.order_id;
///         Ok(())
///     }
/// }
/// ```
#[trait_variant::make(Send)]
pub trait Handler<M: Message>: Send + Sync + 'static {
    /// Handler-defined error type, convertible into [`BusError`].
    type Error: Into<BusError> + Send + Sync + 'static;

    /// Process the message and produce its side effect.
    async fn handle(&self, message: M, ctx: &HandlerContext) -> Result<(), Self::Error>;
}

/// Type-erased handler dispatched by the consumer worker.
///
/// Most users do not implement this trait directly; they use
/// [`TypedHandler`] to adapt a typed [`Handler<M>`] into an erased one
/// the worker can store in a registry keyed by `message_type`.
pub trait ErasedHandler: Send + Sync + 'static {
    /// Message type this handler reacts to, matching [`Message::MESSAGE_TYPE`].
    fn message_type(&self) -> &'static str;

    /// Decode the envelope and dispatch to the underlying typed handler.
    fn handle<'a>(
        &'a self,
        envelope: &'a BusEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), BusError>>;
}

/// Adapter that lifts a typed [`Handler<M>`] into an [`ErasedHandler`].
///
/// The adapter holds the typed handler behind an [`Arc`] so it can be
/// cloned cheaply into the worker's dispatch registry.
pub struct TypedHandler<M, H>
where
    M: Message,
    H: Handler<M>,
{
    handler: Arc<H>,
    _phantom: PhantomData<fn() -> M>,
}

impl<M, H> TypedHandler<M, H>
where
    M: Message,
    H: Handler<M>,
{
    /// Wrap a freshly owned handler.
    #[must_use]
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: PhantomData,
        }
    }
}

impl<M, H> ErasedHandler for TypedHandler<M, H>
where
    M: Message,
    H: Handler<M>,
{
    fn message_type(&self) -> &'static str {
        M::MESSAGE_TYPE
    }

    fn handle<'a>(
        &'a self,
        envelope: &'a BusEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            let message: M = envelope.decode()?;
            self.handler.handle(message, ctx).await.map_err(Into::into)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use hexeract_core::CorrelationId;
    use hexeract_core::MessageId;
    use serde::Deserialize;
    use serde::Serialize;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct OrderPlaced {
        order_id: Uuid,
    }

    impl Message for OrderPlaced {
        const MESSAGE_TYPE: &'static str = "orders.placed";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct UserRegistered {
        user_id: Uuid,
    }

    impl Message for UserRegistered {
        const MESSAGE_TYPE: &'static str = "users.registered";
    }

    #[derive(Debug, thiserror::Error)]
    enum ProjectorError {
        #[error("projection store unavailable")]
        Unavailable,
    }

    impl From<ProjectorError> for BusError {
        fn from(value: ProjectorError) -> Self {
            Self::Internal(value.to_string())
        }
    }

    struct RecordingProjector {
        seen: Arc<Mutex<Vec<Uuid>>>,
    }

    impl Handler<OrderPlaced> for RecordingProjector {
        type Error = BusError;

        async fn handle(
            &self,
            message: OrderPlaced,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.seen.lock().unwrap().push(message.order_id);
            Ok(())
        }
    }

    struct FailingProjector;

    impl Handler<OrderPlaced> for FailingProjector {
        type Error = ProjectorError;

        async fn handle(
            &self,
            _message: OrderPlaced,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Err(ProjectorError::Unavailable)
        }
    }

    fn fresh_ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
    }

    fn fresh_envelope(order_id: Uuid) -> BusEnvelope {
        BusEnvelope::new(Uuid::new_v4(), &OrderPlaced { order_id }).unwrap()
    }

    fn assert_send<T: Send>(_: &T) {}

    #[tokio::test]
    async fn handler_returns_unit_on_success() {
        let projector = RecordingProjector {
            seen: Arc::new(Mutex::new(Vec::new())),
        };
        projector
            .handle(
                OrderPlaced {
                    order_id: Uuid::from_u128(1),
                },
                &fresh_ctx(),
            )
            .await
            .expect("handler must succeed");
    }

    #[tokio::test]
    async fn handler_typed_error_converts_into_bus_error() {
        let projector = FailingProjector;
        let err = projector
            .handle(
                OrderPlaced {
                    order_id: Uuid::nil(),
                },
                &fresh_ctx(),
            )
            .await
            .expect_err("handler must fail");
        let bus_error: BusError = err.into();
        assert!(matches!(bus_error, BusError::Internal(_)));
    }

    #[tokio::test]
    async fn handler_future_is_send() {
        let projector = RecordingProjector {
            seen: Arc::new(Mutex::new(Vec::new())),
        };
        let ctx = fresh_ctx();
        let future = projector.handle(
            OrderPlaced {
                order_id: Uuid::nil(),
            },
            &ctx,
        );
        assert_send(&future);
        let _ = future.await;
    }

    #[test]
    fn typed_handler_reports_message_type_from_const() {
        let typed = TypedHandler::new(RecordingProjector {
            seen: Arc::new(Mutex::new(Vec::new())),
        });
        assert_eq!(typed.message_type(), "orders.placed");
    }

    #[tokio::test]
    async fn typed_handler_decodes_envelope_and_invokes_inner_handler() {
        let seen = Arc::new(Mutex::new(Vec::<Uuid>::new()));
        let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingProjector {
            seen: Arc::clone(&seen),
        }));

        let order_id = Uuid::from_u128(42);
        let envelope = fresh_envelope(order_id);
        erased
            .handle(&envelope, &fresh_ctx())
            .await
            .expect("dispatch must succeed");

        let recorded = seen.lock().unwrap();
        assert_eq!(recorded.as_slice(), &[order_id]);
    }

    #[tokio::test]
    async fn typed_handler_returns_type_mismatch_when_envelope_carries_other_type() {
        let envelope = BusEnvelope::new(
            Uuid::new_v4(),
            &UserRegistered {
                user_id: Uuid::nil(),
            },
        )
        .unwrap();
        let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingProjector {
            seen: Arc::new(Mutex::new(Vec::new())),
        }));

        let error = erased
            .handle(&envelope, &fresh_ctx())
            .await
            .expect_err("dispatch must fail");
        match error {
            BusError::TypeMismatch { expected, actual } => {
                assert_eq!(expected, "orders.placed");
                assert_eq!(actual, "users.registered");
            }
            other => panic!("expected BusError::TypeMismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn typed_handler_propagates_inner_handler_error_as_bus_error() {
        let envelope = fresh_envelope(Uuid::nil());
        let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(FailingProjector));

        let error = erased
            .handle(&envelope, &fresh_ctx())
            .await
            .expect_err("dispatch must fail");
        assert!(matches!(error, BusError::Internal(_)));
    }

    #[tokio::test]
    async fn erased_handler_is_shareable_via_arc() {
        let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(RecordingProjector {
            seen: Arc::new(Mutex::new(Vec::new())),
        }));
        let e1 = Arc::clone(&erased);
        let e2 = Arc::clone(&erased);
        let envelope = fresh_envelope(Uuid::from_u128(7));

        let h1 = tokio::spawn({
            let envelope = envelope.clone();
            async move { e1.handle(&envelope, &fresh_ctx()).await }
        });
        let h2 = tokio::spawn({
            let envelope = envelope.clone();
            async move { e2.handle(&envelope, &fresh_ctx()).await }
        });

        let (r1, r2) = tokio::join!(h1, h2);
        r1.unwrap().unwrap();
        r2.unwrap().unwrap();
    }
}
