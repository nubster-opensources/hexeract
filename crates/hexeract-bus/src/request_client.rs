use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use uuid::Uuid;

use crate::correlation::CorrelationRegistry;
use crate::reply_status::{
    REPLY_STATUS_ERROR, REPLY_STATUS_HEADER, REPLY_STATUS_OK, RemoteErrorPayload,
};
use crate::{BusEnvelope, Request, RequestError, Transport};

/// Synchronous-over-async RPC client: send a [`Request`], await its reply.
pub struct RequestClient<T: Transport> {
    transport: Arc<T>,
    registry: Arc<CorrelationRegistry>,
    reply_inbox: Arc<Mutex<String>>,
    default_timeout: Duration,
}

impl<T: Transport> RequestClient<T> {
    /// Assemble a client from its collaborators. The `reply_inbox` is shared
    /// so a transport supervisor can update it across reconnects.
    #[must_use]
    pub fn new(
        transport: Arc<T>,
        registry: Arc<CorrelationRegistry>,
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
    /// - [`RequestError::Remote`] if the responder returned an error.
    /// - [`RequestError::Decode`] if the request cannot be serialized or the
    ///   reply cannot be decoded (including a malformed or missing status).
    pub async fn request_with_timeout<R: Request>(
        &self,
        request: &R,
        timeout: Duration,
    ) -> Result<R::Reply, RequestError> {
        let mut pending = self.registry.register();
        let correlation_id: Uuid = *pending.correlation_id().as_uuid();
        let inbox = self
            .reply_inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let envelope = BusEnvelope::with_reply_to(correlation_id, inbox, request)
            .map_err(RequestError::Decode)?;
        self.transport
            .publish_envelope(R::MESSAGE_TYPE, &envelope)
            .await
            .map_err(RequestError::Transport)?;

        let reply = match tokio::time::timeout(timeout, pending.wait()).await {
            Err(_elapsed) => return Err(RequestError::Timeout(timeout)),
            Ok(Err(_closed)) => {
                return Err(RequestError::Transport(reply_channel_lost()));
            }
            Ok(Ok(envelope)) => envelope,
        };

        match reply.headers.get(REPLY_STATUS_HEADER).map(String::as_str) {
            Some(REPLY_STATUS_OK) => reply.decode::<R::Reply>().map_err(RequestError::Decode),
            Some(REPLY_STATUS_ERROR) => {
                let payload: RemoteErrorPayload = serde_json::from_slice(&reply.payload)
                    .map_err(|e| RequestError::Decode(e.into()))?;
                Err(RequestError::Remote {
                    error_type: payload.error_type,
                    message: payload.message,
                })
            }
            _ => Err(RequestError::Decode(crate::BusError::Internal(
                "reply is missing a valid x-hexeract-reply-status header".to_owned(),
            ))),
        }
    }
}

fn reply_channel_lost() -> crate::BusError {
    crate::BusError::connection("reply inbox channel closed before a reply arrived", true)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::reply_status::REPLY_ERROR_MESSAGE_TYPE;
    use crate::{BusError, Message};

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

    fn ok_reply(correlation_id: Uuid, seq: u64) -> BusEnvelope {
        let mut env = BusEnvelope::new(correlation_id, &Pong { seq }).unwrap();
        env.headers
            .insert(REPLY_STATUS_HEADER.to_owned(), REPLY_STATUS_OK.to_owned());
        env
    }

    fn client(
        transport: Arc<CapturingTransport>,
        registry: Arc<CorrelationRegistry>,
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
        let registry = Arc::new(CorrelationRegistry::new());
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
        registry.resolve(ok_reply(published.correlation_id, 3));
        let pong = request_fut.await.expect("reply");
        assert_eq!(pong, Pong { seq: 3 });
    }

    #[tokio::test]
    async fn silent_responder_times_out() {
        let transport = Arc::new(CapturingTransport {
            last: StdMutex::new(None),
        });
        let registry = Arc::new(CorrelationRegistry::new());
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
        let registry = Arc::new(CorrelationRegistry::new());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let request_fut = client.request(&Ping { seq: 9 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let published = transport.last.lock().unwrap().clone().unwrap();
        let payload = RemoteErrorPayload {
            error_type: "Internal".to_owned(),
            message: "downstream down".to_owned(),
        };
        let err_env = BusEnvelope::restore(
            Uuid::now_v7(),
            REPLY_ERROR_MESSAGE_TYPE.to_owned(),
            serde_json::to_vec(&payload).unwrap(),
            published.correlation_id,
            None,
            HashMap::from([(
                REPLY_STATUS_HEADER.to_owned(),
                REPLY_STATUS_ERROR.to_owned(),
            )]),
            std::time::SystemTime::UNIX_EPOCH,
        );
        registry.resolve(err_env);
        let err = request_fut.await.expect_err("remote error");
        match err {
            RequestError::Remote {
                error_type,
                message,
            } => {
                assert_eq!(error_type, "Internal");
                assert_eq!(message, "downstream down");
            }
            other => panic!("expected Remote, got {other:?}"),
        }
    }
}
