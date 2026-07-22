use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hexeract_core::RequestId;
use uuid::Uuid;

use crate::remote_error::RemoteErrorPayload;
use crate::request_error::ProtocolViolation;
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

    /// Send `request` and await its reply within the default timeout.
    ///
    /// # Errors
    ///
    /// See [`RequestClient::request_with_timeout`].
    pub async fn request<R: Request>(&self, request: &R) -> Result<R::Reply, RequestError> {
        self.request_with_timeout(request, self.default_timeout)
            .await
    }

    /// Send `request` and await its reply within `timeout`.
    ///
    /// # Errors
    ///
    /// - [`RequestError::Transport`] if publishing fails or the reply channel
    ///   is lost (connection dropped).
    /// - [`RequestError::Timeout`] if no reply arrives within `timeout`.
    /// - [`RequestError::Protocol`] if the reply violates the request-reply
    ///   protocol: an unsupported or missing protocol version, a missing or
    ///   unrecognized reply status, or a reply message type other than the
    ///   one expected.
    /// - [`RequestError::Remote`] if the responder reported a failure.
    /// - [`RequestError::Decode`] if the request cannot be serialized, or if
    ///   a reply that already passed protocol and status validation cannot be
    ///   decoded: either an ok reply whose payload does not decode into the
    ///   expected reply type, or an error reply whose `message_type` matches
    ///   but whose payload does not decode into a [`RemoteErrorPayload`].
    pub async fn request_with_timeout<R: Request>(
        &self,
        request: &R,
        timeout: Duration,
    ) -> Result<R::Reply, RequestError> {
        let mut pending = self.registry.register();
        let request_id = pending.request_id();
        let correlation_id = Uuid::now_v7();
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
            .publish_envelope(R::DESTINATION, &envelope)
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

    use hexeract_core::RequestId;

    use super::*;
    use crate::BusError;
    use crate::remote_error::RemoteErrorType;

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

    /// Records the last published envelope so tests can craft a reply.
    #[derive(Default)]
    struct CapturingTransport {
        last: StdMutex<Option<BusEnvelope>>,
    }
    #[async_trait]
    impl Transport for CapturingTransport {
        async fn publish_envelope(
            &self,
            _routing_key: &str,
            envelope: &BusEnvelope,
        ) -> Result<Uuid, BusError> {
            *self.last.lock().unwrap() = Some(envelope.clone());
            Ok(envelope.message_id)
        }
    }
    impl CapturingTransport {
        fn last_published(&self) -> Option<BusEnvelope> {
            self.last.lock().unwrap().clone()
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

    #[tokio::test]
    async fn nominal_round_trip_returns_typed_reply() {
        let transport = Arc::new(CapturingTransport {
            last: StdMutex::new(None),
        });
        let registry = Arc::new(RequestRegistry::new());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let request_fut = client.request(&Ping { seq: 3 });
        tokio::pin!(request_fut);
        // drive the request until it has published and registered the slot
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let published = transport.last.lock().unwrap().clone().unwrap();
        assert_eq!(published.reply_to.as_deref(), Some("reply.inbox"));
        registry.resolve(ok_reply(published_request_id(&published), 3));
        let pong = request_fut.await.expect("reply");
        assert_eq!(pong, Pong { seq: 3 });
    }

    #[tokio::test]
    async fn silent_responder_times_out() {
        let transport = Arc::new(CapturingTransport {
            last: StdMutex::new(None),
        });
        let registry = Arc::new(RequestRegistry::new());
        let client = client(transport, Arc::clone(&registry));
        let err = client
            .request_with_timeout(&Ping { seq: 1 }, Duration::from_millis(30))
            .await
            .expect_err("no reply");
        assert!(matches!(err, RequestError::Timeout(_)));
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn remote_error_reply_maps_to_remote() {
        let transport = Arc::new(CapturingTransport {
            last: StdMutex::new(None),
        });
        let registry = Arc::new(RequestRegistry::new());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let request_fut = client.request(&Ping { seq: 9 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let published = transport.last.lock().unwrap().clone().unwrap();
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

    #[tokio::test]
    async fn a_reply_without_a_status_header_is_a_protocol_violation() {
        let error = client_error_for_reply(|_request_id, reply| {
            reply.headers.remove(REPLY_STATUS_HEADER);
        })
        .await;
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::MissingHeader { header })
                if header == REPLY_STATUS_HEADER
        ));
    }

    #[tokio::test]
    async fn a_reply_announcing_an_unknown_version_is_a_protocol_violation() {
        let error = client_error_for_reply(|_request_id, reply| {
            reply
                .headers
                .insert(PROTOCOL_VERSION_HEADER.to_owned(), "99".to_owned());
        })
        .await;
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::UnsupportedVersion { version: 99 })
        ));
    }

    #[tokio::test]
    async fn a_reply_of_an_unexpected_type_is_a_protocol_violation() {
        let error = client_error_for_reply(|_request_id, reply| {
            reply.message_type = "accounts.something_else".to_owned();
        })
        .await;
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::UnexpectedReplyType { .. })
        ));
    }

    #[tokio::test]
    async fn a_remote_failure_surfaces_its_category_and_request_id() {
        let request_id = std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured = std::sync::Arc::clone(&request_id);
        let error = client_error_for_reply(move |id, reply| {
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

    /// Drive one round trip against a capturing transport, letting `mutate`
    /// tamper with the reply before it is resolved, and return the resulting
    /// client error.
    ///
    /// Uses the same deterministic idiom as `nominal_round_trip_returns_typed_reply`
    /// and `remote_error_reply_maps_to_remote`: the request future is
    /// pinned and driven with `select!` until it has published and
    /// registered its slot, in line rather than through a detached task or
    /// a polling loop.
    async fn client_error_for_reply(
        mutate: impl FnOnce(RequestId, &mut BusEnvelope),
    ) -> RequestError {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new());
        let client = RequestClient::new(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Arc::new(Mutex::new("caller.inbox".to_owned())),
            Duration::from_secs(5),
        );

        let request_fut = client.request(&Ping { seq: 1 });
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

        request_fut
            .await
            .expect_err("the tampered reply must be rejected")
    }
}
