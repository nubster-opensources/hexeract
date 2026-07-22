use std::marker::PhantomData;
use std::sync::Arc;

use hexeract_core::HandlerContext;

use crate::remote_error::{RemoteErrorPayload, RemoteErrorType};
use crate::rpc_protocol::{
    PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, REPLY_ERROR_MESSAGE_TYPE, REPLY_STATUS_ERROR,
    REPLY_STATUS_HEADER, REPLY_STATUS_OK, REQUEST_ID_HEADER, read_protocol_version,
};
use crate::{BoxFuture, BusEnvelope, BusError, ErasedHandler, Request, RequestHandler, Transport};

/// Adapts a [`RequestHandler<R>`] into an [`ErasedHandler`] that decodes the
/// request, runs the handler, and publishes the reply (or an encoded error)
/// to the request `reply_to`, preserving the inbound correlation id and the
/// inbound request identity (the `x-hexeract-request-id` header the caller's
/// [`RequestRegistry`](crate::RequestRegistry) routes the reply on).
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
            let request_id = envelope.headers.get(REQUEST_ID_HEADER).cloned();
            let correlation_id = envelope.correlation_id;
            let Some(reply_to) = envelope.reply_to.clone() else {
                tracing::warn!(
                    message_type = R::MESSAGE_TYPE,
                    "request without reply_to, handled fire-and-forget without a reply"
                );
                let request: R = envelope.decode()?;
                let _ = self
                    .handler
                    .handle(request, ctx)
                    .await
                    .map_err(Into::into)?;
                return Ok(());
            };

            if read_protocol_version(&envelope.headers) != Some(PROTOCOL_VERSION) {
                tracing::warn!(
                    message_type = R::MESSAGE_TYPE,
                    "request announces an unsupported protocol version, rejecting"
                );
                let reply = error_reply(
                    RemoteErrorType::Unsupported,
                    correlation_id,
                    request_id.as_deref(),
                )?;
                self.transport.publish_envelope(&reply_to, &reply).await?;
                return Ok(());
            }

            let request: R = match envelope.decode() {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!(
                        message_type = R::MESSAGE_TYPE,
                        %error,
                        "undecodable request, replying with an opaque category"
                    );
                    let reply = error_reply(
                        RemoteErrorType::Malformed,
                        correlation_id,
                        request_id.as_deref(),
                    )?;
                    self.transport.publish_envelope(&reply_to, &reply).await?;
                    return Ok(());
                }
            };

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
                    tracing::error!(
                        request_id = %request_id.clone().unwrap_or_default(),
                        message_type = R::MESSAGE_TYPE,
                        %error,
                        "request handler failed, replying with an opaque category"
                    );
                    error_reply(
                        RemoteErrorType::from_bus_error(&error),
                        correlation_id,
                        request_id.as_deref(),
                    )?
                }
            };
            self.transport
                .publish_envelope(&reply_to, &reply_envelope)
                .await?;
            Ok(())
        })
    }
}

/// Build the sanitized error reply for `category`.
///
/// The failure detail is deliberately absent from the wire: it has already
/// been recorded on the responder side, indexed by the request identity.
fn error_reply(
    category: RemoteErrorType,
    correlation_id: uuid::Uuid,
    request_id: Option<&str>,
) -> Result<BusEnvelope, BusError> {
    let payload = RemoteErrorPayload {
        error_type: category,
        request_id: request_id
            .and_then(|raw| raw.parse().ok())
            .unwrap_or_else(uuid::Uuid::nil),
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
    if let Some(request_id) = request_id {
        headers.insert(REQUEST_ID_HEADER.to_owned(), request_id.to_owned());
    }
    Ok(BusEnvelope::restore(
        uuid::Uuid::now_v7(),
        REPLY_ERROR_MESSAGE_TYPE.to_owned(),
        serde_json::to_vec(&payload)?,
        correlation_id,
        None,
        headers,
        std::time::SystemTime::now(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use hexeract_core::{CorrelationId, MessageId, RequestId};
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

    #[derive(Default)]
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
    impl CapturingTransport {
        fn last_published(&self) -> Option<BusEnvelope> {
            self.published
                .lock()
                .unwrap()
                .last()
                .map(|(_, envelope)| envelope.clone())
        }
    }

    fn request_envelope(reply_to: Option<&str>) -> BusEnvelope {
        let mut env = BusEnvelope::new(Uuid::now_v7(), &Ping { seq: 8 }).unwrap();
        env.reply_to = reply_to.map(str::to_owned);
        env.headers
            .insert(REQUEST_ID_HEADER.to_owned(), "request-42".to_owned());
        env.headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );
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
        assert_eq!(
            env.headers.get(REQUEST_ID_HEADER).map(String::as_str),
            request.headers.get(REQUEST_ID_HEADER).map(String::as_str),
            "reply must carry the exact inbound request id"
        );
        assert_eq!(
            env.headers.get(PROTOCOL_VERSION_HEADER).map(String::as_str),
            Some(PROTOCOL_VERSION.to_string()).as_deref()
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
        let request = request_envelope(Some("reply.inbox"));
        erased.handle(&request, &ctx()).await.unwrap();

        let published = transport.published.lock().unwrap();
        let (_, env) = &published[0];
        assert_eq!(
            env.headers
                .get("x-hexeract-reply-status")
                .map(String::as_str),
            Some("error")
        );
        assert_eq!(env.message_type, REPLY_ERROR_MESSAGE_TYPE);
        assert_eq!(
            env.headers.get(REQUEST_ID_HEADER).map(String::as_str),
            request.headers.get(REQUEST_ID_HEADER).map(String::as_str),
            "an error reply must still carry the exact inbound request id"
        );
        assert_eq!(
            env.headers.get(PROTOCOL_VERSION_HEADER).map(String::as_str),
            Some(PROTOCOL_VERSION.to_string()).as_deref()
        );
        let payload: RemoteErrorPayload = serde_json::from_slice(&env.payload).unwrap();
        assert_eq!(payload.error_type, RemoteErrorType::Internal);
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

    /// Security assertion, not a serialization test: no fragment of an
    /// internal failure message may cross the boundary. This test must fail
    /// loudly if a `to_string()` is ever reintroduced on the error path.
    #[tokio::test]
    async fn an_internal_failure_message_never_reaches_the_wire() {
        const SECRET: &str = "connection to 10.0.0.7:5432 refused for user vault_admin";

        struct FailingHandler;
        impl RequestHandler<Ping> for FailingHandler {
            type Error = BusError;
            async fn handle(
                &self,
                _request: Ping,
                _ctx: &HandlerContext,
            ) -> Result<Pong, BusError> {
                Err(BusError::Internal(SECRET.to_owned()))
            }
        }

        let transport = Arc::new(CapturingTransport::default());
        let handler = RepliedHandler::new(FailingHandler, Arc::clone(&transport));
        let mut request =
            BusEnvelope::with_reply_to(Uuid::now_v7(), "caller.inbox".to_owned(), &Ping { seq: 1 })
                .expect("ping must serialize");
        request
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), RequestId::new().to_string());
        request.headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        handler
            .handle(&request, &ctx)
            .await
            .expect("reply must publish");

        let published = transport.last_published().expect("a reply was published");
        let wire = String::from_utf8_lossy(&published.payload).to_string();
        for fragment in ["10.0.0.7", "5432", "vault_admin", "refused"] {
            assert!(
                !wire.contains(fragment),
                "internal detail {fragment} leaked on the wire: {wire}"
            );
        }
        let payload: RemoteErrorPayload =
            serde_json::from_slice(&published.payload).expect("payload must decode");
        assert_eq!(payload.error_type, RemoteErrorType::Internal);
    }

    struct RecordingHandler {
        ran: Arc<std::sync::atomic::AtomicBool>,
    }

    impl RequestHandler<Ping> for RecordingHandler {
        type Error = BusError;
        async fn handle(&self, _request: Ping, _ctx: &HandlerContext) -> Result<Pong, BusError> {
            self.ran.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(Pong { seq: 1 })
        }
    }

    #[tokio::test]
    async fn an_unsupported_version_is_rejected_without_running_the_handler() {
        let transport = Arc::new(CapturingTransport::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&transport),
        );
        let mut request =
            BusEnvelope::with_reply_to(Uuid::now_v7(), "caller.inbox".to_owned(), &Ping { seq: 1 })
                .expect("ping must serialize");
        request
            .headers
            .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        handler
            .handle(&request, &ctx)
            .await
            .expect("reply must publish");

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "handler ran"
        );
        let published = transport.last_published().expect("a reply was published");
        let payload: RemoteErrorPayload =
            serde_json::from_slice(&published.payload).expect("payload must decode");
        assert_eq!(payload.error_type, RemoteErrorType::Unsupported);
    }

    #[tokio::test]
    async fn an_undecodable_request_replies_malformed_without_running_the_handler() {
        let transport = Arc::new(CapturingTransport::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&transport),
        );
        let mut request =
            BusEnvelope::with_reply_to(Uuid::now_v7(), "caller.inbox".to_owned(), &Ping { seq: 1 })
                .expect("ping must serialize");
        request.payload = b"{ not json".to_vec();
        request.headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        handler
            .handle(&request, &ctx)
            .await
            .expect("reply must publish");

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "handler ran"
        );
        let published = transport.last_published().expect("a reply was published");
        let payload: RemoteErrorPayload =
            serde_json::from_slice(&published.payload).expect("payload must decode");
        assert_eq!(payload.error_type, RemoteErrorType::Malformed);
    }
}
