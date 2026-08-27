use std::marker::PhantomData;
use std::sync::Arc;

use hexeract_core::{HandlerContext, RequestId};

use crate::remote_error::{RemoteErrorPayload, RemoteErrorType};
use crate::request_context::RequestContext;
use crate::responder_counters::{ResponderCounters, ResponderCountersSnapshot};
use crate::rpc_protocol::{
    PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, REPLY_ERROR_MESSAGE_TYPE, REPLY_STATUS_ERROR,
    REPLY_STATUS_HEADER, REPLY_STATUS_OK, REQUEST_ID_HEADER, read_protocol_version,
};
use crate::{
    BoxFuture, BusEnvelope, BusError, ErasedHandler, ReplyDestination, ReplyPublisher, Request,
    RequestHandler,
};

/// Adapts a [`RequestHandler<R>`] into an [`ErasedHandler`] that decodes the
/// request, runs the handler, and publishes the reply (or an encoded error)
/// to the request `reply_to`, preserving the inbound correlation id and the
/// inbound request identity (the `x-hexeract-request-id` header the caller's
/// [`RequestRegistry`](crate::RequestRegistry) routes the reply on).
///
/// Replies are published through the injected [`ReplyPublisher`], which
/// targets the AMQP default exchange, never through an application
/// transport: a caller-supplied `reply_to` must never be routed by the
/// responder's own application bindings.
pub struct RepliedHandler<R, H, P> {
    handler: Arc<H>,
    replies: Arc<P>,
    counters: ResponderCounters,
    _phantom: PhantomData<fn() -> R>,
}

impl<R, H, P> RepliedHandler<R, H, P>
where
    R: Request,
    H: RequestHandler<R>,
    P: ReplyPublisher,
{
    /// Wrap a handler and the reply publisher used to publish replies.
    pub fn new(handler: H, replies: Arc<P>) -> Self {
        Self::with_counters(handler, replies, ResponderCounters::default())
    }

    /// Wrap a handler with a shared responder rejection counter handle.
    ///
    /// Retain a clone of `counters` to inspect requests rejected before the
    /// domain handler runs.
    pub fn with_counters(handler: H, replies: Arc<P>, counters: ResponderCounters) -> Self {
        Self {
            handler: Arc::new(handler),
            replies,
            counters,
            _phantom: PhantomData,
        }
    }

    /// Return a point-in-time snapshot of responder-side rejection totals.
    #[must_use]
    pub fn counters(&self) -> ResponderCountersSnapshot {
        self.counters.snapshot()
    }

    /// Parse and validate `envelope.reply_to` against
    /// [`ReplyPublisher::accept_destination`], logging and returning `None`
    /// for either an absent or a rejected destination. Called first, before
    /// any other guard: see [`RepliedHandler::handle`] for why.
    fn validated_reply_to(&self, envelope: &BusEnvelope) -> Option<ReplyDestination> {
        let Some(raw_reply_to) = envelope.reply_to.as_deref() else {
            self.counters.count_invalid_reply_to();
            tracing::warn!(
                message_type = R::MESSAGE_TYPE,
                correlation_id = %envelope.correlation_id,
                "request without reply_to, dropping without running the handler"
            );
            return None;
        };
        match self.replies.accept_destination(raw_reply_to) {
            Ok(reply_to) => Some(reply_to),
            Err(rejection) => {
                self.counters.count_invalid_reply_to();
                tracing::warn!(
                    message_type = R::MESSAGE_TYPE,
                    correlation_id = %envelope.correlation_id,
                    ?rejection,
                    "request carries an unusable reply_to, dropping without running the handler"
                );
                None
            }
        }
    }
}

/// Outcome of reading the inbound `x-hexeract-request-id` header.
///
/// Kept as two distinct rejection cases, rather than collapsed into a
/// single `None`, because they call for different diagnoses: a missing
/// header points at a non-conforming client library or version, an
/// unreadable one points at an encoding bug in a peer that otherwise
/// believes it speaks the protocol.
enum RequestIdHeader {
    /// The header parsed into a request identity.
    Present(RequestId),
    /// The envelope carries no `x-hexeract-request-id` header at all.
    Missing,
    /// The header is present but does not parse as a UUID.
    Unreadable,
}

/// Parse the inbound `x-hexeract-request-id` header into a [`RequestIdHeader`].
fn parse_request_id(envelope: &BusEnvelope) -> RequestIdHeader {
    match envelope.headers.get(REQUEST_ID_HEADER) {
        None => RequestIdHeader::Missing,
        Some(raw) => raw
            .parse::<uuid::Uuid>()
            .map_or(RequestIdHeader::Unreadable, |uuid| {
                RequestIdHeader::Present(RequestId::from(uuid))
            }),
    }
}

impl<R, H, P> ErasedHandler for RepliedHandler<R, H, P>
where
    R: Request,
    H: RequestHandler<R>,
    P: ReplyPublisher,
{
    fn message_type(&self) -> &'static str {
        R::MESSAGE_TYPE
    }

    /// Decode the inbound request, run the handler, and publish the reply.
    ///
    /// Four guards run in a fixed order before the handler is ever invoked,
    /// each one stopping the request before the handler runs, two of them
    /// silently and two of them with a categorized error reply, rather than
    /// running the handler on incomplete input:
    ///
    /// 1. `reply_to` is parsed and validated against
    ///    [`ReplyPublisher::accept_destination`] FIRST, before any other
    ///    guard. This order was inverted from an earlier revision that ran
    ///    the protocol-version check before the `reply_to` guard: a guard
    ///    placed after an early return can never protect that path, so once
    ///    a later branch publishes an error reply, that publish must not be
    ///    reachable with an unvalidated destination. Otherwise the version
    ///    check is a publication relay: a third party could have a trusted
    ///    responder emit a message to an arbitrary destination without a
    ///    decodable payload and without ever reaching the handler.
    /// 2. The `x-hexeract-request-id` header is parsed into a [`RequestId`]
    ///    second, right after `reply_to`. A request carrying no readable
    ///    identity is dropped here rather than answered: the caller matches
    ///    its replies by this same identifier
    ///    (`RequestClient`'s pending-reply registry), so a reply built
    ///    without one could never be matched to any in-flight call and
    ///    would only be counted orphaned. Placing this guard here, ahead of
    ///    the version and decode checks, also means every later branch that
    ///    builds a reply can carry `request_id` as a plain [`RequestId`]
    ///    rather than an `Option<RequestId>`: there is no "unknown
    ///    identity" case left for it to represent past this point.
    /// 3. The protocol version is checked third, once both `reply_to` and
    ///    `request_id` are known good: its own rejection branch is the
    ///    first one in this method that publishes, and it now has both a
    ///    validated destination and a definite identity to publish with.
    /// 4. The payload is decoded fourth, and its own rejection reuses the
    ///    same guarantees.
    ///
    /// A nominal reply the framework fails to serialize is treated the same
    /// as an undecodable request: an opaque internal error is published
    /// instead of leaving the caller to exhaust its timeout in silence.
    fn handle<'a>(
        &'a self,
        envelope: &'a BusEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            let correlation_id = envelope.correlation_id;

            let Some(reply_to) = self.validated_reply_to(envelope) else {
                return Ok(());
            };

            let request_id = match parse_request_id(envelope) {
                RequestIdHeader::Present(request_id) => request_id,
                RequestIdHeader::Missing => {
                    self.counters.count_invalid_request_id();
                    tracing::warn!(
                        message_type = R::MESSAGE_TYPE,
                        %correlation_id,
                        "request without a request id header, dropping without running the handler"
                    );
                    return Ok(());
                }
                RequestIdHeader::Unreadable => {
                    self.counters.count_invalid_request_id();
                    tracing::warn!(
                        message_type = R::MESSAGE_TYPE,
                        %correlation_id,
                        "request with an unparsable request id header, dropping without running the handler"
                    );
                    return Ok(());
                }
            };

            let protocol_version = match read_protocol_version(&envelope.headers) {
                Some(version) if version == PROTOCOL_VERSION => version,
                _ => {
                    self.counters.count_unsupported_protocol_version();
                    tracing::warn!(
                        message_type = R::MESSAGE_TYPE,
                        "request announces an unsupported protocol version, rejecting"
                    );
                    let reply =
                        error_reply(RemoteErrorType::Unsupported, correlation_id, request_id)?;
                    self.replies.publish_reply(&reply_to, &reply).await?;
                    return Ok(());
                }
            };

            let request: R = match envelope.decode() {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!(
                        message_type = R::MESSAGE_TYPE,
                        %error,
                        "undecodable request, replying with an opaque category"
                    );
                    let reply =
                        error_reply(RemoteErrorType::Malformed, correlation_id, request_id)?;
                    self.replies.publish_reply(&reply_to, &reply).await?;
                    return Ok(());
                }
            };

            let request_context = RequestContext::new(request_id, protocol_version, ctx);
            let reply_envelope = match self.handler.handle(request, &request_context).await {
                Ok(reply) => match BusEnvelope::new(correlation_id, &reply) {
                    Ok(mut env) => {
                        env.headers
                            .insert(REPLY_STATUS_HEADER.to_owned(), REPLY_STATUS_OK.to_owned());
                        env.headers
                            .insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
                        env.headers.insert(
                            PROTOCOL_VERSION_HEADER.to_owned(),
                            PROTOCOL_VERSION.to_string(),
                        );
                        env
                    }
                    Err(error) => {
                        tracing::error!(
                            %request_id,
                            message_type = R::MESSAGE_TYPE,
                            %error,
                            "reply serialization failed, replying with an opaque category"
                        );
                        error_reply(RemoteErrorType::Internal, correlation_id, request_id)?
                    }
                },
                Err(error) => {
                    let error: BusError = error.into();
                    tracing::error!(
                        %request_id,
                        message_type = R::MESSAGE_TYPE,
                        %error,
                        "request handler failed, replying with an opaque category"
                    );
                    error_reply(
                        RemoteErrorType::from_bus_error(&error),
                        correlation_id,
                        request_id,
                    )?
                }
            };
            self.replies
                .publish_reply(&reply_to, &reply_envelope)
                .await?;
            Ok(())
        })
    }
}

/// Build the sanitized error reply for `category`.
///
/// The failure detail is deliberately absent from the wire: it has already
/// been recorded on the responder side, indexed by the request identity.
///
/// `request_id` is guaranteed present by the caller: [`RepliedHandler::handle`]
/// rejects a request with no readable identity before any code path that
/// could reach this function runs, so there is no "no known identity" case
/// left to represent here. That same value feeds both the header and the
/// payload below, so the two can never disagree about which request an
/// error reply is for.
fn error_reply(
    category: RemoteErrorType,
    correlation_id: uuid::Uuid,
    request_id: RequestId,
) -> Result<BusEnvelope, BusError> {
    let payload = RemoteErrorPayload {
        error_type: category,
        request_id: *request_id.as_uuid(),
    };
    let headers = std::collections::HashMap::from([
        (
            REPLY_STATUS_HEADER.to_owned(),
            REPLY_STATUS_ERROR.to_owned(),
        ),
        (
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        ),
        (REQUEST_ID_HEADER.to_owned(), request_id.to_string()),
    ]);
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

    use hexeract_core::{CorrelationId, MessageId, RequestId};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::*;
    use crate::Message;
    use crate::RequestContext;
    use crate::{ReplyDestination, ReplyDestinationError, ReplyPublisher};

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
        async fn handle(&self, request: Ping, _ctx: &RequestContext<'_>) -> Result<Pong, BusError> {
            Ok(Pong { seq: request.seq })
        }
    }
    struct Boom;
    impl RequestHandler<Ping> for Boom {
        type Error = BusError;
        async fn handle(
            &self,
            _request: Ping,
            _ctx: &RequestContext<'_>,
        ) -> Result<Pong, BusError> {
            Err(BusError::Internal("kaboom".to_owned()))
        }
    }

    #[derive(Default)]
    struct RecordingReplyPublisher {
        published: StdMutex<Vec<(String, BusEnvelope)>>,
    }

    impl ReplyPublisher for RecordingReplyPublisher {
        fn publish_reply<'a>(
            &'a self,
            destination: &'a ReplyDestination,
            envelope: &'a BusEnvelope,
        ) -> BoxFuture<'a, Result<(), BusError>> {
            self.published
                .lock()
                .unwrap()
                .push((destination.as_str().to_owned(), envelope.clone()));
            Box::pin(async { Ok(()) })
        }

        fn accept_destination(&self, raw: &str) -> Result<ReplyDestination, ReplyDestinationError> {
            let destination = ReplyDestination::parse(raw)?;
            if destination.as_str().starts_with("amq.gen-") {
                Ok(destination)
            } else {
                Err(ReplyDestinationError::OutsideReplyNamespace)
            }
        }
    }

    impl RecordingReplyPublisher {
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
            .insert(REQUEST_ID_HEADER.to_owned(), RequestId::new().to_string());
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
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let erased = RepliedHandler::new(Echo, Arc::clone(&publisher));
        let request = request_envelope(Some("amq.gen-inbox"));
        erased.handle(&request, &ctx()).await.unwrap();

        let recorded = publisher.published.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        let (rk, env) = &recorded[0];
        assert_eq!(rk, "amq.gen-inbox");
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
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let erased = RepliedHandler::new(Boom, Arc::clone(&publisher));
        let request = request_envelope(Some("amq.gen-inbox"));
        erased.handle(&request, &ctx()).await.unwrap();

        let recorded = publisher.published.lock().unwrap();
        let (_, env) = &recorded[0];
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
        assert!(
            !String::from_utf8_lossy(&env.payload).contains("kaboom"),
            "the handler's failure message must never reach the wire"
        );
    }

    /// Security assertion, not a serialization test: no fragment of an
    /// internal failure message may cross the boundary, on any channel of
    /// the envelope, headers and `message_type` included, not only the
    /// payload. This test must fail loudly if a `to_string()` is ever
    /// reintroduced on the error path, wherever it lands on the wire.
    #[tokio::test]
    async fn an_internal_failure_message_never_reaches_the_wire() {
        const SECRET: &str = "connection to 10.0.0.7:5432 refused for user vault_admin";

        struct FailingHandler;
        impl RequestHandler<Ping> for FailingHandler {
            type Error = BusError;
            async fn handle(
                &self,
                _request: Ping,
                _ctx: &RequestContext<'_>,
            ) -> Result<Pong, BusError> {
                Err(BusError::Internal(SECRET.to_owned()))
            }
        }

        let publisher = Arc::new(RecordingReplyPublisher::default());
        let handler = RepliedHandler::new(FailingHandler, Arc::clone(&publisher));
        let mut request = BusEnvelope::with_reply_to(
            Uuid::now_v7(),
            "amq.gen-inbox".to_owned(),
            &Ping { seq: 1 },
        )
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

        let recorded = publisher.last_published().expect("a reply was published");
        // Scan every channel the envelope exposes on the wire, not only the
        // payload: a header or a dynamically built message_type travel the
        // same fabric and could leak just as easily.
        let mut wire = String::new();
        wire.push_str(&recorded.message_type);
        for (key, value) in &recorded.headers {
            wire.push_str(key);
            wire.push_str(value);
        }
        wire.push_str(&String::from_utf8_lossy(&recorded.payload));
        for fragment in ["10.0.0.7", "5432", "vault_admin", "refused"] {
            assert!(
                !wire.contains(fragment),
                "internal detail {fragment} leaked on the wire: {wire}"
            );
        }
        let payload: RemoteErrorPayload =
            serde_json::from_slice(&recorded.payload).expect("payload must decode");
        assert_eq!(payload.error_type, RemoteErrorType::Internal);
    }

    struct RecordingHandler {
        ran: Arc<std::sync::atomic::AtomicBool>,
    }

    impl RequestHandler<Ping> for RecordingHandler {
        type Error = BusError;
        async fn handle(
            &self,
            _request: Ping,
            _ctx: &RequestContext<'_>,
        ) -> Result<Pong, BusError> {
            self.ran.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(Pong { seq: 1 })
        }
    }

    #[tokio::test]
    async fn an_unsupported_version_is_rejected_without_running_the_handler() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        let mut request = BusEnvelope::with_reply_to(
            Uuid::now_v7(),
            "amq.gen-inbox".to_owned(),
            &Ping { seq: 1 },
        )
        .expect("ping must serialize");
        request
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), RequestId::new().to_string());
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
        let recorded = publisher.last_published().expect("a reply was published");
        let payload: RemoteErrorPayload =
            serde_json::from_slice(&recorded.payload).expect("payload must decode");
        assert_eq!(payload.error_type, RemoteErrorType::Unsupported);
    }

    /// A request with no `reply_to` is dropped at the very first guard,
    /// before the version check ever runs: there is no validated
    /// destination to report the rejection to, and D5 already forbids
    /// running the handler without one.
    #[tokio::test]
    async fn an_unsupported_version_without_reply_to_is_dropped_without_running_the_handler() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        let mut request =
            BusEnvelope::new(Uuid::now_v7(), &Ping { seq: 1 }).expect("ping must serialize");
        request
            .headers
            .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
        // Deliberately no reply_to.

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        handler
            .handle(&request, &ctx)
            .await
            .expect("dropping must not surface as a framework error");

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "handler ran despite the missing reply_to"
        );
        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "nothing can be published without a reply_to"
        );
    }

    #[tokio::test]
    async fn an_undecodable_request_replies_malformed_without_running_the_handler() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        let mut request = BusEnvelope::with_reply_to(
            Uuid::now_v7(),
            "amq.gen-inbox".to_owned(),
            &Ping { seq: 1 },
        )
        .expect("ping must serialize");
        request.payload = b"{ not json".to_vec();
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

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "handler ran"
        );
        let recorded = publisher.last_published().expect("a reply was published");
        let payload: RemoteErrorPayload =
            serde_json::from_slice(&recorded.payload).expect("payload must decode");
        assert_eq!(payload.error_type, RemoteErrorType::Malformed);
    }

    /// A reply type whose `Serialize` implementation always fails, so tests
    /// can force `BusEnvelope::new` to reject a nominal reply.
    struct UnserializableReply;
    impl Message for UnserializableReply {
        const MESSAGE_TYPE: &'static str = "tests.unserializable_reply";
    }
    impl Serialize for UnserializableReply {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom(
                "reply intentionally refuses to serialize",
            ))
        }
    }
    impl<'de> Deserialize<'de> for UnserializableReply {
        fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Ok(UnserializableReply)
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct WeirdPing {
        seq: u64,
    }
    impl Message for WeirdPing {
        const MESSAGE_TYPE: &'static str = "tests.weird_ping";
    }
    impl Request for WeirdPing {
        type Reply = UnserializableReply;
    }

    struct BrokenReplyHandler;
    impl RequestHandler<WeirdPing> for BrokenReplyHandler {
        type Error = BusError;
        async fn handle(
            &self,
            _request: WeirdPing,
            _ctx: &RequestContext<'_>,
        ) -> Result<UnserializableReply, BusError> {
            Ok(UnserializableReply)
        }
    }

    /// The same asymmetry already fixed for an undecodable request must not
    /// reappear on the reply side: a nominal reply the framework fails to
    /// serialize must still publish an opaque error rather than leave the
    /// caller to exhaust its timeout in silence.
    #[tokio::test]
    async fn an_unserializable_nominal_reply_publishes_an_internal_error_instead_of_silence() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let handler = RepliedHandler::new(BrokenReplyHandler, Arc::clone(&publisher));
        let mut request = BusEnvelope::with_reply_to(
            Uuid::now_v7(),
            "amq.gen-inbox".to_owned(),
            &WeirdPing { seq: 1 },
        )
        .expect("weird ping must serialize");
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
            .expect("the serialization failure must not surface as a framework error");

        let recorded = publisher
            .last_published()
            .expect("a reply must still be published despite the serialization failure");
        assert_eq!(
            recorded
                .headers
                .get("x-hexeract-reply-status")
                .map(String::as_str),
            Some("error")
        );
        assert_eq!(recorded.message_type, REPLY_ERROR_MESSAGE_TYPE);
        let payload: RemoteErrorPayload =
            serde_json::from_slice(&recorded.payload).expect("payload must decode");
        assert_eq!(payload.error_type, RemoteErrorType::Internal);
    }

    /// Resolution 3: `request_id` is obligatory. An inbound
    /// `x-hexeract-request-id` header that does not parse as a UUID carries
    /// no readable identity, so the request is dropped before the handler
    /// runs, exactly like the neighboring `reply_to` guards (resolution 4).
    /// This reuses the `"not-a-uuid"` value the previous revision of this
    /// test already documented, updated from its old outcome (a published
    /// reply with a nil payload id) to the new one (a silent drop): a
    /// response keyed on an identity the client never sent could not be
    /// matched to any pending call and would only be counted orphaned.
    #[tokio::test]
    async fn an_unreadable_request_id_header_is_dropped_without_running_the_handler() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        let mut request = BusEnvelope::with_reply_to(
            Uuid::now_v7(),
            "amq.gen-inbox".to_owned(),
            &Ping { seq: 1 },
        )
        .expect("ping must serialize");
        request
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), "not-a-uuid".to_owned());
        request.headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        handler
            .handle(&request, &ctx)
            .await
            .expect("dropping must not surface as a framework error");

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run without a readable request id"
        );
        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "nothing may be published for a request with no readable identity"
        );
    }

    /// Symmetric to the unreadable-header case above: an absent
    /// `x-hexeract-request-id` header carries no identity at all, and is
    /// dropped the same way, before the handler ever runs.
    #[tokio::test]
    async fn a_request_without_a_request_id_header_is_dropped_without_running_the_handler() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        let mut request = BusEnvelope::with_reply_to(
            Uuid::now_v7(),
            "amq.gen-inbox".to_owned(),
            &Ping { seq: 1 },
        )
        .expect("ping must serialize");
        // Deliberately no REQUEST_ID_HEADER.
        request.headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        handler
            .handle(&request, &ctx)
            .await
            .expect("dropping must not surface as a framework error");

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run without a request id header"
        );
        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "nothing may be published for a request with no request id header"
        );
    }

    /// Combination that the identity guard's placement before the version
    /// check is meant to close off: a request with neither a readable
    /// identity nor a supported version must still be dropped silently by
    /// the identity guard, never answered by the version guard's
    /// `RemoteErrorType::Unsupported` reply. Pins the guard order so that a
    /// future refactor making `error_reply` tolerant of a missing identity
    /// would surface here rather than only in production.
    #[tokio::test]
    async fn a_request_with_no_identity_and_an_unsupported_version_is_dropped_by_identity_first() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        let mut request = BusEnvelope::with_reply_to(
            Uuid::now_v7(),
            "amq.gen-inbox".to_owned(),
            &Ping { seq: 1 },
        )
        .expect("ping must serialize");
        request
            .headers
            .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
        // Deliberately no REQUEST_ID_HEADER, combined with an unsupported version.

        let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        handler
            .handle(&request, &ctx)
            .await
            .expect("dropping must not surface as a framework error");

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run when neither identity nor version is usable"
        );
        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "the identity guard must drop the request before the version guard could reply"
        );
    }

    /// Handler that records the [`RequestContext`] it was invoked with, so
    /// tests can inspect exactly what the framework threads through to a
    /// responder.
    struct CapturingHandler {
        captured: Arc<StdMutex<Option<(RequestId, u32, CorrelationId)>>>,
    }
    impl RequestHandler<Ping> for CapturingHandler {
        type Error = BusError;
        async fn handle(&self, request: Ping, ctx: &RequestContext<'_>) -> Result<Pong, BusError> {
            *self.captured.lock().unwrap() = Some((
                ctx.request_id,
                ctx.protocol_version,
                ctx.handler.correlation_id,
            ));
            Ok(Pong { seq: request.seq })
        }
    }

    /// Build a request envelope carrying exactly `request_id` as its
    /// `x-hexeract-request-id` header, overriding the one [`request_envelope`]
    /// mints on its own, so a test can assert on the precise value a handler
    /// observes.
    fn request_envelope_with_id(reply_to: Option<&str>, request_id: RequestId) -> BusEnvelope {
        let mut env = request_envelope(reply_to);
        env.headers
            .insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
        env
    }

    #[tokio::test]
    async fn the_handler_receives_the_exact_inbound_request_id() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let captured = Arc::new(StdMutex::new(None));
        let handler = RepliedHandler::new(
            CapturingHandler {
                captured: Arc::clone(&captured),
            },
            Arc::clone(&publisher),
        );
        let request_id = RequestId::new();
        let request = request_envelope_with_id(Some("amq.gen-inbox"), request_id);

        handler
            .handle(&request, &ctx())
            .await
            .expect("reply must publish");

        let (seen_request_id, _, _) = captured.lock().unwrap().expect("handler must have run");
        assert_eq!(seen_request_id, request_id);
    }

    #[tokio::test]
    async fn the_handler_receives_the_negotiated_protocol_version() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let captured = Arc::new(StdMutex::new(None));
        let handler = RepliedHandler::new(
            CapturingHandler {
                captured: Arc::clone(&captured),
            },
            Arc::clone(&publisher),
        );
        let request = request_envelope_with_id(Some("amq.gen-inbox"), RequestId::new());

        handler
            .handle(&request, &ctx())
            .await
            .expect("reply must publish");

        let (_, seen_version, _) = captured.lock().unwrap().expect("handler must have run");
        assert_eq!(seen_version, PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn the_correlation_id_stays_reachable_through_ctx_handler_and_matches_the_causal_chain() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let captured = Arc::new(StdMutex::new(None));
        let handler = RepliedHandler::new(
            CapturingHandler {
                captured: Arc::clone(&captured),
            },
            Arc::clone(&publisher),
        );
        let request = request_envelope_with_id(Some("amq.gen-inbox"), RequestId::new());
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        handler
            .handle(&request, &handler_ctx)
            .await
            .expect("reply must publish");

        let (_, _, seen_correlation_id) = captured.lock().unwrap().expect("handler must have run");
        assert_eq!(seen_correlation_id, handler_ctx.correlation_id);
    }

    /// The most important of the four positive tests: it stops a future
    /// refactor from collapsing the per-call request identity into the
    /// causal correlation identity, which would break every caller relying
    /// on `RequestId` to key exactly one in-flight call.
    #[tokio::test]
    async fn request_id_and_correlation_id_are_distinct_values_for_the_same_call() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let captured = Arc::new(StdMutex::new(None));
        let handler = RepliedHandler::new(
            CapturingHandler {
                captured: Arc::clone(&captured),
            },
            Arc::clone(&publisher),
        );
        let request_id = RequestId::new();
        let request = request_envelope_with_id(Some("amq.gen-inbox"), request_id);
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        handler
            .handle(&request, &handler_ctx)
            .await
            .expect("reply must publish");

        let (seen_request_id, _, seen_correlation_id) =
            captured.lock().unwrap().expect("handler must have run");
        assert_eq!(seen_request_id, request_id);
        assert_eq!(seen_correlation_id, handler_ctx.correlation_id);
        assert_ne!(
            seen_request_id.as_uuid(),
            seen_correlation_id.as_uuid(),
            "request_id and correlation_id must never collapse into the same identity"
        );
    }

    #[tokio::test]
    async fn a_request_without_reply_to_does_not_run_the_handler() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        handler
            .handle(&request_envelope(None), &ctx())
            .await
            .expect("dropping must not surface as a framework error");
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run"
        );
        assert!(publisher.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_request_with_an_application_queue_as_reply_to_is_refused() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        // An application queue name is not a server-named reply inbox.
        handler
            .handle(&request_envelope(Some("orders.inbox")), &ctx())
            .await
            .expect("refusing must not surface as a framework error");
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run"
        );
        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "nothing may be published towards an unvalidated destination"
        );
    }

    #[tokio::test]
    async fn an_unsupported_version_never_publishes_to_an_unvalidated_destination() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler = RepliedHandler::new(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
        );
        let mut request = request_envelope(Some("orders.inbox"));
        request
            .headers
            .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
        handler
            .handle(&request, &ctx())
            .await
            .expect("must not surface as a framework error");
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the handler must not run"
        );
        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "the version path must not be usable as a publication relay: reply_to was invalid"
        );
    }

    #[tokio::test]
    async fn responder_counters_categorize_every_pre_dispatch_rejection() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let counters = ResponderCounters::default();
        let handler = RepliedHandler::with_counters(Echo, Arc::clone(&publisher), counters.clone());

        let missing_reply_to = request_envelope(None);
        handler.handle(&missing_reply_to, &ctx()).await.unwrap();

        let rejected_reply_to = request_envelope(Some("orders.inbox"));
        handler.handle(&rejected_reply_to, &ctx()).await.unwrap();

        let mut missing_request_id = request_envelope(Some("amq.gen-inbox"));
        missing_request_id.headers.remove(REQUEST_ID_HEADER);
        handler.handle(&missing_request_id, &ctx()).await.unwrap();

        let mut unreadable_request_id = request_envelope(Some("amq.gen-inbox"));
        unreadable_request_id
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), "not-a-uuid".to_owned());
        handler
            .handle(&unreadable_request_id, &ctx())
            .await
            .unwrap();

        let mut unsupported_version = request_envelope(Some("amq.gen-inbox"));
        unsupported_version
            .headers
            .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
        handler.handle(&unsupported_version, &ctx()).await.unwrap();

        let expected = ResponderCountersSnapshot {
            invalid_reply_to: 2,
            invalid_request_id: 2,
            unsupported_protocol_version: 1,
        };
        assert_eq!(handler.counters(), expected);
        assert_eq!(counters.snapshot(), expected);
    }
}
