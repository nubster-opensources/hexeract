use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hexeract_core::{CorrelationId, RequestId};

use crate::remote_error::RemoteErrorPayload;
use crate::reply_acceptance::ReplyExpectation;
use crate::request_error::ProtocolViolation;
use crate::request_options::RequestOptions;
use crate::request_registry::RequestRegistry;
use crate::rpc_protocol::{
    PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, REPLY_ERROR_MESSAGE_TYPE, REPLY_STATUS_ERROR,
    REPLY_STATUS_HEADER, REPLY_STATUS_OK, REQUEST_ID_HEADER, read_protocol_version,
};
use crate::{BusEnvelope, Message, Request, RequestError, Transport};

/// Synchronous-over-async RPC client: send a [`Request`], await its reply.
pub struct RequestClient<T: Transport> {
    transport: Arc<T>,
    registry: Arc<RequestRegistry>,
    reply_inbox: Arc<Mutex<String>>,
    default_timeout: Duration,
}

impl<T: Transport> RequestClient<T> {
    /// Assemble a client from its collaborators. The `reply_inbox` is shared
    /// so a transport supervisor can update it across reconnects.
    #[must_use]
    pub fn new(
        transport: Arc<T>,
        registry: Arc<RequestRegistry>,
        reply_inbox: Arc<Mutex<String>>,
        default_timeout: Duration,
    ) -> Self {
        Self {
            transport,
            registry,
            reply_inbox,
            default_timeout,
        }
    }

    /// Send `request` on a fresh causal chain, using this client's default
    /// timeout and `R::DESTINATION`.
    ///
    /// Equivalent to `self.request_with(request, RequestOptions::default())`.
    /// Use [`Self::request_with`] to override the timeout or the
    /// destination for a single call.
    ///
    /// # Errors
    ///
    /// See [`Self::request_with`].
    pub async fn request<R: Request>(&self, request: R) -> Result<R::Reply, RequestError> {
        self.request_with(request, RequestOptions::default()).await
    }

    /// Send `request` on a fresh causal chain, applying `options` on top of
    /// this client's own defaults.
    ///
    /// Resolution order:
    /// - timeout: `options.timeout` if set, otherwise this client's default
    ///   timeout;
    /// - destination: `options.destination` if set, otherwise
    ///   `R::DESTINATION`.
    ///
    /// # Errors
    ///
    /// - [`RequestError::Transport`] if publishing fails or the reply channel
    ///   is lost (connection dropped).
    /// - [`RequestError::Timeout`] if no reply arrives within the resolved
    ///   timeout. This is also what a legitimate call observes when every
    ///   delivery bearing its request identity violates the request-reply
    ///   protocol: an unsupported or missing protocol version, a missing or
    ///   unrecognized reply status, or a reply message type other than the
    ///   one expected. The registry ignores such deliveries without waking
    ///   the caller, so the slot stays open for the real reply; if none
    ///   arrives before the timeout, the call times out rather than
    ///   surfacing the violation that caused the delivery to be ignored.
    /// - [`RequestError::Protocol`] if a delivery still reaches this decoding
    ///   step while failing one of those same checks: an unsupported or
    ///   missing protocol version, a missing or unrecognized reply status, or
    ///   a reply message type other than the one expected. This remains a
    ///   reachable defense-in-depth path, not one exercised by a well-behaved
    ///   registry today.
    /// - [`RequestError::Remote`] if the responder reported a failure.
    /// - [`RequestError::Decode`] if the request cannot be serialized, or if
    ///   a reply that already passed protocol and status validation cannot be
    ///   decoded: either an ok reply whose payload does not decode into the
    ///   expected reply type, or an error reply whose `message_type` matches
    ///   but whose payload does not decode into a [`RemoteErrorPayload`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// use hexeract_bus::{Message, Request, RequestClient, RequestOptions, Transport};
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Serialize, Deserialize)]
    /// struct GetBalance {
    ///     account_id: uuid::Uuid,
    /// }
    /// impl Message for GetBalance {
    ///     const MESSAGE_TYPE: &'static str = "accounts.get_balance";
    /// }
    ///
    /// #[derive(Debug, Serialize, Deserialize)]
    /// struct Balance {
    ///     cents: u64,
    /// }
    /// impl Message for Balance {
    ///     const MESSAGE_TYPE: &'static str = "accounts.balance";
    /// }
    ///
    /// impl Request for GetBalance {
    ///     type Reply = Balance;
    /// }
    ///
    /// async fn priority_lookup<T: Transport>(
    ///     client: &RequestClient<T>,
    ///     account_id: uuid::Uuid,
    /// ) -> Balance {
    ///     let options = RequestOptions::new()
    ///         .with_timeout(Duration::from_millis(200))
    ///         .with_destination("accounts.priority");
    ///     client
    ///         .request_with(GetBalance { account_id }, options)
    ///         .await
    ///         .unwrap()
    /// }
    /// ```
    pub async fn request_with<R: Request>(
        &self,
        request: R,
        options: RequestOptions,
    ) -> Result<R::Reply, RequestError> {
        let timeout = options.timeout.unwrap_or(self.default_timeout);
        let destination = options.destination.as_deref().unwrap_or(R::DESTINATION);
        self.request_inner(&request, destination, timeout).await
    }

    async fn request_inner<R: Request>(
        &self,
        request: &R,
        destination: &str,
        timeout: Duration,
    ) -> Result<R::Reply, RequestError> {
        let mut pending = self
            .registry
            .register(ReplyExpectation::new(R::Reply::MESSAGE_TYPE));
        let request_id = pending.request_id();
        let correlation_id = *CorrelationId::new().as_uuid();
        let inbox = self
            .reply_inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut envelope = BusEnvelope::with_reply_to(correlation_id, inbox, request)
            .map_err(RequestError::Decode)?;
        envelope
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
        envelope.headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );
        self.transport
            .publish_envelope(destination, &envelope)
            .await
            .map_err(RequestError::Transport)?;

        let reply = match tokio::time::timeout(timeout, pending.wait()).await {
            Err(_elapsed) => return Err(RequestError::Timeout(timeout)),
            Ok(Err(_closed)) => {
                return Err(RequestError::Transport(reply_channel_lost()));
            }
            Ok(Ok(envelope)) => envelope,
        };

        decode_reply::<R>(reply)
    }
}

fn reply_channel_lost() -> crate::BusError {
    crate::BusError::connection("reply inbox channel closed before a reply arrived", true)
}

/// Validate a reply against the protocol, then decode it.
///
/// Checks are ordered from the most structural to the most specific: an
/// unsupported version makes every later check meaningless, so it comes
/// first.
fn decode_reply<R: Request>(reply: BusEnvelope) -> Result<R::Reply, RequestError> {
    match read_protocol_version(&reply.headers) {
        Some(PROTOCOL_VERSION) => {}
        Some(version) => {
            return Err(RequestError::Protocol(
                ProtocolViolation::UnsupportedVersion { version },
            ));
        }
        None => {
            return Err(RequestError::Protocol(ProtocolViolation::MissingHeader {
                header: PROTOCOL_VERSION_HEADER,
            }));
        }
    }

    match reply.headers.get(REPLY_STATUS_HEADER).map(String::as_str) {
        Some(REPLY_STATUS_OK) => {
            if reply.message_type != R::Reply::MESSAGE_TYPE {
                return Err(RequestError::Protocol(
                    ProtocolViolation::UnexpectedReplyType {
                        expected: R::Reply::MESSAGE_TYPE,
                        actual: reply.message_type,
                    },
                ));
            }
            reply.decode::<R::Reply>().map_err(RequestError::Decode)
        }
        Some(REPLY_STATUS_ERROR) => {
            if reply.message_type != REPLY_ERROR_MESSAGE_TYPE {
                return Err(RequestError::Protocol(
                    ProtocolViolation::UnexpectedReplyType {
                        expected: REPLY_ERROR_MESSAGE_TYPE,
                        actual: reply.message_type,
                    },
                ));
            }
            let payload: RemoteErrorPayload = serde_json::from_slice(&reply.payload)
                .map_err(|error| RequestError::Decode(error.into()))?;
            Err(RequestError::Remote {
                error_type: payload.error_type,
                request_id: RequestId::from(payload.request_id),
            })
        }
        _ => Err(RequestError::Protocol(ProtocolViolation::MissingHeader {
            header: REPLY_STATUS_HEADER,
        })),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use uuid::Uuid;

    use super::*;
    use crate::BusError;
    use crate::remote_error::RemoteErrorType;
    use crate::request_options::RequestOptions;
    use crate::request_registry::ReplyCountersSnapshot;

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

    /// A request whose destination is a dedicated queue, distinct from its
    /// message type, so tests can tell the two apart on the wire.
    #[derive(Debug, Serialize, Deserialize)]
    struct PingToDedicatedQueue {
        seq: u64,
    }
    impl Message for PingToDedicatedQueue {
        const MESSAGE_TYPE: &'static str = "tests.ping.dedicated";
    }
    impl Request for PingToDedicatedQueue {
        type Reply = Pong;
        const DESTINATION: &'static str = "tests.dedicated.queue";
    }

    /// Records every published (routing key, envelope) pair so tests can
    /// craft a reply and assert on the routing decision.
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

        fn last_routing_key(&self) -> Option<String> {
            self.published
                .lock()
                .unwrap()
                .last()
                .map(|(routing_key, _)| routing_key.clone())
        }
    }

    /// Read the request identity the client stamped on its published envelope.
    fn published_request_id(published: &BusEnvelope) -> RequestId {
        let raw = published
            .headers
            .get(REQUEST_ID_HEADER)
            .expect("client stamps a request id header on every request");
        RequestId::from(
            raw.parse::<Uuid>()
                .expect("request id header must be a valid uuid"),
        )
    }

    fn ok_reply(request_id: RequestId, seq: u64) -> BusEnvelope {
        let mut env = BusEnvelope::new(Uuid::now_v7(), &Pong { seq }).unwrap();
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

    fn client(
        transport: Arc<CapturingTransport>,
        registry: Arc<RequestRegistry>,
    ) -> RequestClient<CapturingTransport> {
        RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new("reply.inbox".to_owned())),
            Duration::from_millis(200),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn nominal_round_trip_returns_typed_reply() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let request_fut = client.request(Ping { seq: 3 });
        tokio::pin!(request_fut);
        // drive the request until it has published and registered the slot
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let published = transport.last_published().expect("a request was published");
        assert_eq!(published.reply_to.as_deref(), Some("reply.inbox"));
        registry.resolve(ok_reply(published_request_id(&published), 3));
        let pong = request_fut.await.expect("reply");
        assert_eq!(pong, Pong { seq: 3 });
    }

    #[tokio::test]
    async fn silent_responder_times_out() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            transport,
            Arc::clone(&registry),
            Arc::new(Mutex::new("reply.inbox".to_owned())),
            Duration::from_millis(30),
        );
        let err = client.request(Ping { seq: 1 }).await.expect_err("no reply");
        assert!(matches!(err, RequestError::Timeout(_)));
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn remote_error_reply_maps_to_remote() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let request_fut = client.request(Ping { seq: 9 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let published = transport.last_published().expect("a request was published");
        let request_id = published_request_id(&published);
        let payload = RemoteErrorPayload {
            error_type: RemoteErrorType::Internal,
            request_id: *request_id.as_uuid(),
        };
        let err_env = BusEnvelope::restore(
            Uuid::now_v7(),
            REPLY_ERROR_MESSAGE_TYPE.to_owned(),
            serde_json::to_vec(&payload).unwrap(),
            published.correlation_id,
            None,
            HashMap::from([
                (
                    REPLY_STATUS_HEADER.to_owned(),
                    REPLY_STATUS_ERROR.to_owned(),
                ),
                (REQUEST_ID_HEADER.to_owned(), request_id.to_string()),
                (
                    PROTOCOL_VERSION_HEADER.to_owned(),
                    PROTOCOL_VERSION.to_string(),
                ),
            ]),
            std::time::SystemTime::UNIX_EPOCH,
        );
        registry.resolve(err_env);
        let err = request_fut.await.expect_err("remote error");
        assert!(matches!(
            err,
            RequestError::Remote {
                error_type: RemoteErrorType::Internal,
                request_id: resolved_id,
            } if resolved_id == request_id
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_without_a_status_header_never_reaches_the_caller() {
        let (error, counters) = client_error_for_reply(|_request_id, reply| {
            reply.headers.remove(REPLY_STATUS_HEADER);
        })
        .await;
        assert!(matches!(error, RequestError::Timeout(_)));
        assert_eq!(counters.invalid, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_announcing_an_unknown_version_never_reaches_the_caller() {
        let (error, counters) = client_error_for_reply(|_request_id, reply| {
            reply
                .headers
                .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
        })
        .await;
        assert!(matches!(error, RequestError::Timeout(_)));
        assert_eq!(counters.invalid, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_of_an_unexpected_type_never_reaches_the_caller() {
        let (error, counters) = client_error_for_reply(|_request_id, reply| {
            reply.message_type = "accounts.something_else".to_owned();
        })
        .await;
        assert!(matches!(error, RequestError::Timeout(_)));
        assert_eq!(counters.invalid, 1);
    }

    /// `decode_reply` is the client's defense-in-depth check: the registry is
    /// expected to filter out a protocol-violating delivery upstream (see
    /// `a_reply_without_a_status_header_never_reaches_the_caller` and its
    /// neighbors above), but `decode_reply` itself must still reject one if a
    /// violation ever reaches this step, whatever the reason. These tests
    /// call it directly, bypassing transport, registry and timeout, so the
    /// nominal path above stays free to observe `RequestError::Timeout`
    /// while this defense stays exercised on its own terms.
    #[test]
    fn decode_reply_defense_in_depth_rejects_a_missing_or_unsupported_protocol_version() {
        let mut missing_version = ok_reply(RequestId::new(), 1);
        missing_version.headers.remove(PROTOCOL_VERSION_HEADER);
        let error =
            decode_reply::<Ping>(missing_version).expect_err("missing protocol version header");
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::MissingHeader {
                header: PROTOCOL_VERSION_HEADER
            })
        ));

        let mut unsupported_version = ok_reply(RequestId::new(), 1);
        unsupported_version
            .headers
            .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
        let error =
            decode_reply::<Ping>(unsupported_version).expect_err("unsupported protocol version");
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::UnsupportedVersion { version: 99 })
        ));
    }

    #[test]
    fn decode_reply_defense_in_depth_rejects_a_missing_or_unrecognized_reply_status() {
        let mut missing_status = ok_reply(RequestId::new(), 1);
        missing_status.headers.remove(REPLY_STATUS_HEADER);
        let error = decode_reply::<Ping>(missing_status).expect_err("missing reply status header");
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::MissingHeader {
                header: REPLY_STATUS_HEADER
            })
        ));

        let mut unrecognized_status = ok_reply(RequestId::new(), 1);
        unrecognized_status
            .headers
            .insert(REPLY_STATUS_HEADER.to_owned(), "pending".to_owned());
        let error =
            decode_reply::<Ping>(unrecognized_status).expect_err("unrecognized reply status");
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::MissingHeader {
                header: REPLY_STATUS_HEADER
            })
        ));
    }

    #[test]
    fn decode_reply_defense_in_depth_rejects_an_unexpected_reply_message_type() {
        let mut reply = ok_reply(RequestId::new(), 1);
        reply.message_type = "accounts.something_else".to_owned();
        let error = decode_reply::<Ping>(reply).expect_err("unexpected reply message type");
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::UnexpectedReplyType {
                expected: Pong::MESSAGE_TYPE,
                actual,
            }) if actual == "accounts.something_else"
        ));
    }

    /// An error reply for `request_id`, well-formed enough to decode: valid
    /// protocol version, error status, the error sentinel message type and a
    /// serialized [`RemoteErrorPayload`].
    fn error_reply(request_id: RequestId) -> BusEnvelope {
        let payload = RemoteErrorPayload {
            error_type: RemoteErrorType::Internal,
            request_id: *request_id.as_uuid(),
        };
        BusEnvelope::restore(
            Uuid::now_v7(),
            REPLY_ERROR_MESSAGE_TYPE.to_owned(),
            serde_json::to_vec(&payload).expect("payload must serialize"),
            Uuid::now_v7(),
            None,
            HashMap::from([
                (
                    REPLY_STATUS_HEADER.to_owned(),
                    REPLY_STATUS_ERROR.to_owned(),
                ),
                (REQUEST_ID_HEADER.to_owned(), request_id.to_string()),
                (
                    PROTOCOL_VERSION_HEADER.to_owned(),
                    PROTOCOL_VERSION.to_string(),
                ),
            ]),
            std::time::SystemTime::UNIX_EPOCH,
        )
    }

    /// `accepts` (the registry's gate) and `decode_reply` (the client's
    /// defense-in-depth gate) implement the same protocol rules twice, each
    /// with its own test suite, but nothing else asserts the two agree on
    /// which deliveries are acceptable. If `accepts` ever relaxed a rule, a
    /// delivery would slip past the registry and only then be rejected here,
    /// surfacing as `RequestError::Protocol` to the caller instead of
    /// leaving the slot open for the real reply.
    ///
    /// `decode_reply`'s `Result` also carries legitimate, non-protocol
    /// outcomes as `Err`: a well-formed error reply decodes successfully but
    /// still surfaces as `Err(RequestError::Remote { .. })`, since that is
    /// how the caller learns the responder failed. So the boundary this
    /// test compares against `accepts` is specifically whether `decode_reply`
    /// flags a delivery as `RequestError::Protocol`, not its raw `is_err()`:
    /// the two must agree on which deliveries are protocol violations,
    /// without needing to agree on the specific variant reported (see the
    /// "unknown reply status" case below, where `accepts` reports
    /// `ReplyRejection::UnknownStatus` and `decode_reply` reports
    /// `ProtocolViolation::MissingHeader`).
    #[test]
    fn accepts_and_decode_reply_agree_on_whether_a_delivery_is_a_protocol_violation() {
        let expectation = ReplyExpectation::new(Pong::MESSAGE_TYPE);
        let request_id = RequestId::new();

        let cases: Vec<(&str, BusEnvelope)> = vec![
            ("missing protocol version", {
                let mut envelope = ok_reply(request_id, 1);
                envelope.headers.remove(PROTOCOL_VERSION_HEADER);
                envelope
            }),
            ("unsupported protocol version", {
                let mut envelope = ok_reply(request_id, 1);
                envelope
                    .headers
                    .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
                envelope
            }),
            ("missing reply status", {
                let mut envelope = ok_reply(request_id, 1);
                envelope.headers.remove(REPLY_STATUS_HEADER);
                envelope
            }),
            ("unknown reply status", {
                let mut envelope = ok_reply(request_id, 1);
                envelope
                    .headers
                    .insert(REPLY_STATUS_HEADER.to_owned(), "pending".to_owned());
                envelope
            }),
            ("unexpected message type on an ok status", {
                let mut envelope = ok_reply(request_id, 1);
                envelope.message_type = "accounts.something_else".to_owned();
                envelope
            }),
            ("non sentinel message type on an error status", {
                let mut envelope = error_reply(request_id);
                envelope.message_type = "accounts.something_else".to_owned();
                envelope
            }),
            ("a nominal ok reply", ok_reply(request_id, 1)),
            ("a nominal error reply", error_reply(request_id)),
        ];

        for (label, envelope) in cases {
            let accepts_is_err = crate::reply_acceptance::accepts(&expectation, &envelope).is_err();
            let decode_is_protocol_violation = matches!(
                decode_reply::<Ping>(envelope),
                Err(RequestError::Protocol(_))
            );
            assert_eq!(
                accepts_is_err, decode_is_protocol_violation,
                "accepts and decode_reply disagree on whether this delivery is a protocol violation: {label}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_remote_failure_surfaces_its_category_and_request_id() {
        let request_id = std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured = std::sync::Arc::clone(&request_id);
        let (error, _counters) = client_error_for_reply(move |id, reply| {
            *captured.lock().expect("lock") = Some(id);
            reply.message_type = REPLY_ERROR_MESSAGE_TYPE.to_owned();
            reply.payload = serde_json::to_vec(&RemoteErrorPayload {
                error_type: RemoteErrorType::Unavailable,
                request_id: *id.as_uuid(),
            })
            .expect("payload must serialize");
            reply.headers.insert(
                REPLY_STATUS_HEADER.to_owned(),
                REPLY_STATUS_ERROR.to_owned(),
            );
        })
        .await;

        let expected = request_id.lock().expect("lock").expect("captured");
        assert!(matches!(
            error,
            RequestError::Remote { error_type: RemoteErrorType::Unavailable, request_id }
                if request_id == expected
        ));
    }

    /// Every call mints its own fresh causal chain: `RequestClient` no longer
    /// carries a `HandlerContext` to inherit a `correlation_id` from (see
    /// `request_with`'s doc comment); two calls in a row must therefore
    /// never share one.
    #[tokio::test(start_paused = true)]
    async fn request_starts_a_fresh_chain() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new("caller.inbox".to_owned())),
            Duration::from_secs(5),
        );

        let first_fut = client.request(Ping { seq: 1 });
        tokio::pin!(first_fut);
        tokio::select! {
            _ = &mut first_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let first = transport.last_published().expect("first request");

        let second_fut = client.request(Ping { seq: 2 });
        tokio::pin!(second_fut);
        tokio::select! {
            _ = &mut second_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let second = transport.last_published().expect("second request");

        assert_ne!(first.correlation_id, second.correlation_id);
    }

    #[tokio::test(start_paused = true)]
    async fn request_publishes_to_the_declared_destination_not_the_message_type() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new("caller.inbox".to_owned())),
            Duration::from_secs(5),
        );

        let request_fut = client.request(PingToDedicatedQueue { seq: 1 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let routing_key = transport
            .last_routing_key()
            .expect("a request was published");
        assert_eq!(routing_key, PingToDedicatedQueue::DESTINATION);
        assert_ne!(routing_key, PingToDedicatedQueue::MESSAGE_TYPE);
    }

    /// Without any [`RequestOptions`], `request` must resolve both the
    /// destination and the timeout from the client's own defaults: the
    /// request's declared [`Request::DESTINATION`], never overridden here,
    /// and the client's `default_timeout`, distinguishable from any other
    /// duration because nothing ever replies.
    #[tokio::test]
    async fn request_without_options_uses_request_destination_and_client_default_timeout() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new("caller.inbox".to_owned())),
            Duration::from_millis(30),
        );

        let error = client
            .request(PingToDedicatedQueue { seq: 1 })
            .await
            .expect_err("no responder ever answers");

        match error {
            RequestError::Timeout(elapsed) => assert_eq!(elapsed, Duration::from_millis(30)),
            other => panic!("expected RequestError::Timeout, got {other:?}"),
        }
        let routing_key = transport
            .last_routing_key()
            .expect("a request was published");
        assert_eq!(routing_key, PingToDedicatedQueue::DESTINATION);
    }

    /// `options.destination` takes precedence over `R::DESTINATION`.
    #[tokio::test]
    async fn options_destination_overrides_the_request_declared_destination() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new("caller.inbox".to_owned())),
            Duration::from_millis(30),
        );

        let options = RequestOptions::new().with_destination("tests.overridden.queue");
        let _ = client
            .request_with(PingToDedicatedQueue { seq: 1 }, options)
            .await;

        let routing_key = transport
            .last_routing_key()
            .expect("a request was published");
        assert_eq!(routing_key, "tests.overridden.queue");
        assert_ne!(routing_key, PingToDedicatedQueue::DESTINATION);
    }

    /// `options.timeout` takes precedence over the client's default timeout.
    #[tokio::test]
    async fn options_timeout_overrides_the_client_default_timeout() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new("caller.inbox".to_owned())),
            Duration::from_secs(30),
        );

        let options = RequestOptions::new().with_timeout(Duration::from_millis(30));
        let error = client
            .request_with(Ping { seq: 1 }, options)
            .await
            .expect_err("no responder ever answers");

        match error {
            RequestError::Timeout(elapsed) => assert_eq!(elapsed, Duration::from_millis(30)),
            other => panic!("expected RequestError::Timeout, got {other:?}"),
        }
    }

    /// The request id of the single call currently in flight on `registry`.
    ///
    /// Panics if zero or more than one slot is registered: this helper is
    /// for tests that drive exactly one call at a time.
    fn registry_single_request_id(registry: &Arc<RequestRegistry>) -> RequestId {
        let ids = registry.in_flight_ids();
        assert_eq!(ids.len(), 1, "exactly one call must be in flight");
        ids[0]
    }

    /// A well-formed but unexpected reply, tagged with `request_id`: valid
    /// protocol version and status, but a message type the caller never
    /// asked for.
    fn forged_reply(message_type: &str, request_id: RequestId) -> BusEnvelope {
        let mut headers = HashMap::new();
        headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );
        headers.insert(REPLY_STATUS_HEADER.to_owned(), REPLY_STATUS_OK.to_owned());
        headers.insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
        BusEnvelope::restore(
            Uuid::now_v7(),
            message_type.to_owned(),
            Vec::new(),
            Uuid::now_v7(),
            None,
            headers,
            std::time::SystemTime::now(),
        )
    }

    /// The legitimate reply to a `Ping`, tagged with `request_id`.
    fn pong_reply(request_id: RequestId, seq: u64) -> BusEnvelope {
        let mut envelope = forged_reply(<Pong as Message>::MESSAGE_TYPE, request_id);
        envelope.payload = serde_json::to_vec(&Pong { seq }).expect("Pong serializes");
        envelope
    }

    /// A forged reply that arrives before the legitimate one must not end
    /// the call: the registry leaves the slot intact for a delivery it
    /// refuses, so the real reply that follows still reaches the caller.
    ///
    /// This is the end-to-end counterpart of
    /// `an_invalid_reply_arriving_first_does_not_consume_the_slot` in
    /// `request_registry`: that test proves the property at the registry
    /// alone, this one proves it survives the full `RequestClient::request`
    /// path. If the registry ever regressed to consuming a slot before
    /// validating the delivery, the forged reply would complete this call
    /// first, `decode_reply` would reject its unexpected message type, and
    /// the assertions below on a successful `Pong` would fail.
    #[tokio::test]
    async fn a_forged_reply_of_the_wrong_type_does_not_end_the_call() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let call = tokio::spawn(async move { client.request(Ping { seq: 7 }).await });
        tokio::task::yield_now().await;

        let request_id = registry_single_request_id(&registry);
        registry.resolve(forged_reply("attacker.reply", request_id));
        registry.resolve(pong_reply(request_id, 7));

        let reply = call
            .await
            .expect("task panicked")
            .expect("call must succeed");
        assert_eq!(reply, Pong { seq: 7 });
        assert_eq!(registry.counters().invalid, 1);
    }

    /// Drive one round trip against a capturing transport, letting `mutate`
    /// tamper with the reply before it is resolved, and return the resulting
    /// client error together with the registry's refused-delivery counters.
    ///
    /// A reply `mutate` makes structurally invalid never reaches the caller
    /// at all: the registry refuses it before the slot is consumed, so the
    /// call observes a plain timeout rather than a decoded protocol error.
    /// The timeout is kept short so this stays a fast test rather than a
    /// slow one.
    ///
    /// Uses the same deterministic idiom as `nominal_round_trip_returns_typed_reply`
    /// and `remote_error_reply_maps_to_remote`: the request future is
    /// pinned and driven with `select!` until it has published and
    /// registered its slot, in line rather than through a detached task or
    /// a polling loop.
    async fn client_error_for_reply(
        mutate: impl FnOnce(RequestId, &mut BusEnvelope),
    ) -> (RequestError, ReplyCountersSnapshot) {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Arc::new(Mutex::new("caller.inbox".to_owned())),
            Duration::from_millis(100),
        );

        let request_fut = client.request(Ping { seq: 1 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let published = transport
            .last_published()
            .expect("request must have published by now");
        let request_id = RequestId::from(
            published
                .headers
                .get(REQUEST_ID_HEADER)
                .expect("request id header")
                .parse::<Uuid>()
                .expect("request id must parse"),
        );
        let mut reply = BusEnvelope::new(published.correlation_id, &Pong { seq: 1 })
            .expect("pong must serialize");
        reply
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
        reply.headers.insert(
            PROTOCOL_VERSION_HEADER.to_owned(),
            PROTOCOL_VERSION.to_string(),
        );
        reply
            .headers
            .insert(REPLY_STATUS_HEADER.to_owned(), REPLY_STATUS_OK.to_owned());
        mutate(request_id, &mut reply);
        registry.resolve(reply);

        let error = request_fut
            .await
            .expect_err("the tampered reply must be rejected");
        (error, registry.counters())
    }
}
