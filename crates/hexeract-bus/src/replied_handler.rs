use std::marker::PhantomData;
use std::sync::Arc;

use hexeract_core::HandlerContext;

use crate::reply_status::{
    REPLY_ERROR_MESSAGE_TYPE, REPLY_STATUS_ERROR, REPLY_STATUS_HEADER, REPLY_STATUS_OK,
    RemoteErrorPayload,
};
use crate::rpc_protocol::{PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, REQUEST_ID_HEADER};
use crate::{BoxFuture, BusEnvelope, BusError, ErasedHandler, Request, RequestHandler, Transport};

/// Adapts a [`RequestHandler<R>`] into an [`ErasedHandler`] that decodes the
/// request, runs the handler, and publishes the reply (or an encoded error)
/// to the request `reply_to`, preserving the inbound correlation id.
pub struct RepliedHandler<R, H, T> {
    handler: Arc<H>,
    transport: Arc<T>,
    _phantom: PhantomData<fn() -> R>,
}

impl<R, H, T> RepliedHandler<R, H, T>
where
    R: Request,
    H: RequestHandler<R>,
    T: Transport,
{
    /// Wrap a handler and the transport used to publish replies.
    pub fn new(handler: H, transport: Arc<T>) -> Self {
        Self {
            handler: Arc::new(handler),
            transport,
            _phantom: PhantomData,
        }
    }
}

impl<R, H, T> ErasedHandler for RepliedHandler<R, H, T>
where
    R: Request,
    H: RequestHandler<R>,
    T: Transport,
{
    fn message_type(&self) -> &'static str {
        R::MESSAGE_TYPE
    }

    fn handle<'a>(
        &'a self,
        envelope: &'a BusEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            let request: R = envelope.decode()?;
            let Some(reply_to) = envelope.reply_to.clone() else {
                tracing::warn!(
                    message_type = R::MESSAGE_TYPE,
                    "request without reply_to, handled fire-and-forget without a reply"
                );
                // still run the handler for its side effect, ignore the reply value
                let _ = self
                    .handler
                    .handle(request, ctx)
                    .await
                    .map_err(Into::into)?;
                return Ok(());
            };
            let correlation_id = envelope.correlation_id;
            let request_id = envelope.headers.get(REQUEST_ID_HEADER).cloned();
            let reply_envelope = match self.handler.handle(request, ctx).await {
                Ok(reply) => {
                    let mut env = BusEnvelope::new(correlation_id, &reply)?;
                    env.headers
                        .insert(REPLY_STATUS_HEADER.to_owned(), REPLY_STATUS_OK.to_owned());
                    if let Some(request_id) = request_id.as_ref() {
                        env.headers
                            .insert(REQUEST_ID_HEADER.to_owned(), request_id.clone());
                    }
                    env.headers.insert(
                        PROTOCOL_VERSION_HEADER.to_owned(),
                        PROTOCOL_VERSION.to_string(),
                    );
                    env
                }
                Err(error) => {
                    let error: BusError = error.into();
                    let payload = RemoteErrorPayload {
                        error_type: error_variant_name(&error).to_owned(),
                        message: error.to_string(),
                    };
                    let mut headers = std::collections::HashMap::from([
                        (
                            REPLY_STATUS_HEADER.to_owned(),
                            REPLY_STATUS_ERROR.to_owned(),
                        ),
                        (
                            PROTOCOL_VERSION_HEADER.to_owned(),
                            PROTOCOL_VERSION.to_string(),
                        ),
                    ]);
                    if let Some(request_id) = request_id.as_ref() {
                        headers.insert(REQUEST_ID_HEADER.to_owned(), request_id.clone());
                    }
                    BusEnvelope::restore(
                        uuid::Uuid::now_v7(),
                        REPLY_ERROR_MESSAGE_TYPE.to_owned(),
                        serde_json::to_vec(&payload)?,
                        correlation_id,
                        None,
                        headers,
                        std::time::SystemTime::now(),
                    )
                }
            };
            self.transport
                .publish_envelope(&reply_to, &reply_envelope)
                .await?;
            Ok(())
        })
    }
}

/// Short, stable-ish category for a [`BusError`], used as `error_type`.
fn error_variant_name(error: &BusError) -> &'static str {
    match error {
        BusError::Serialization(_) => "Serialization",
        BusError::Transport(_) => "Transport",
        BusError::Connection { .. } => "Connection",
        BusError::Unroutable { .. } => "Unroutable",
        BusError::MissingHandler { .. } => "MissingHandler",
        BusError::TypeMismatch { .. } => "TypeMismatch",
        BusError::PayloadTooLarge { .. } => "PayloadTooLarge",
        BusError::InvalidTopology { .. } => "InvalidTopology",
        BusError::Internal(_) => "Internal",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use hexeract_core::{CorrelationId, MessageId};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::*;
    use crate::Message;

    #[derive(Debug, Serialize, Deserialize)]
    struct Ping {
        seq: u64,
    }
    impl Message for Ping {
        const MESSAGE_TYPE: &'static str = "tests.ping";
    }
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Pong {
        seq: u64,
    }
    impl Message for Pong {
        const MESSAGE_TYPE: &'static str = "tests.pong";
    }
    impl Request for Ping {
        type Reply = Pong;
    }

    struct Echo;
    impl RequestHandler<Ping> for Echo {
        type Error = BusError;
        async fn handle(&self, request: Ping, _ctx: &HandlerContext) -> Result<Pong, BusError> {
            Ok(Pong { seq: request.seq })
        }
    }
    struct Boom;
    impl RequestHandler<Ping> for Boom {
        type Error = BusError;
        async fn handle(&self, _request: Ping, _ctx: &HandlerContext) -> Result<Pong, BusError> {
            Err(BusError::Internal("kaboom".to_owned()))
        }
    }

    struct CapturingTransport {
        published: StdMutex<Vec<(String, BusEnvelope)>>,
    }
    #[async_trait]
    impl Transport for CapturingTransport {
        async fn publish_envelope(
            &self,
            routing_key: &str,
            envelope: &BusEnvelope,
        ) -> Result<Uuid, BusError> {
            self.published
                .lock()
                .unwrap()
                .push((routing_key.to_owned(), envelope.clone()));
            Ok(envelope.message_id)
        }
    }

    fn request_envelope(reply_to: Option<&str>) -> BusEnvelope {
        let mut env = BusEnvelope::new(Uuid::now_v7(), &Ping { seq: 8 }).unwrap();
        env.reply_to = reply_to.map(str::to_owned);
        env
    }
    fn ctx() -> HandlerContext {
        HandlerContext::new(MessageId::new(), CorrelationId::new())
    }

    #[tokio::test]
    async fn ok_reply_is_published_with_status_ok() {
        let transport = Arc::new(CapturingTransport {
            published: StdMutex::new(Vec::new()),
        });
        let erased = RepliedHandler::new(Echo, Arc::clone(&transport));
        let request = request_envelope(Some("reply.inbox"));
        erased.handle(&request, &ctx()).await.unwrap();

        let published = transport.published.lock().unwrap();
        assert_eq!(published.len(), 1);
        let (rk, env) = &published[0];
        assert_eq!(rk, "reply.inbox");
        assert_eq!(env.correlation_id, request.correlation_id);
        assert_eq!(
            env.headers
                .get("x-hexeract-reply-status")
                .map(String::as_str),
            Some("ok")
        );
        let pong: Pong = env.decode().unwrap();
        assert_eq!(pong, Pong { seq: 8 });
    }

    #[tokio::test]
    async fn handler_error_is_published_with_status_error() {
        let transport = Arc::new(CapturingTransport {
            published: StdMutex::new(Vec::new()),
        });
        let erased = RepliedHandler::new(Boom, Arc::clone(&transport));
        erased
            .handle(&request_envelope(Some("reply.inbox")), &ctx())
            .await
            .unwrap();

        let published = transport.published.lock().unwrap();
        let (_, env) = &published[0];
        assert_eq!(
            env.headers
                .get("x-hexeract-reply-status")
                .map(String::as_str),
            Some("error")
        );
        assert_eq!(env.message_type, "hexeract.reply.error");
        let payload: RemoteErrorPayload = serde_json::from_slice(&env.payload).unwrap();
        assert_eq!(payload.error_type, "Internal");
        assert!(payload.message.contains("kaboom"));
    }

    #[tokio::test]
    async fn request_without_reply_to_publishes_nothing() {
        let transport = Arc::new(CapturingTransport {
            published: StdMutex::new(Vec::new()),
        });
        let erased = RepliedHandler::new(Echo, Arc::clone(&transport));
        erased
            .handle(&request_envelope(None), &ctx())
            .await
            .unwrap();
        assert!(transport.published.lock().unwrap().is_empty());
    }
}
