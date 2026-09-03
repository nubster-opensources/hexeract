use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hexeract_core::{CorrelationId, RequestId};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::deadline::Deadline;
use crate::remote_error::RemoteErrorPayload;
use crate::reply_acceptance::ReplyExpectation;
use crate::reply_inbox_state::ReplyInboxState;
use crate::request_client_supervisor::RequestClientSupervisor;
use crate::request_error::ProtocolViolation;
use crate::request_options::RequestOptions;
use crate::request_registry::RequestRegistry;
use crate::rpc_protocol::{
    DEADLINE_HEADER, PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, REPLY_ERROR_MESSAGE_TYPE,
    REPLY_STATUS_ERROR, REPLY_STATUS_HEADER, REPLY_STATUS_OK, REQUEST_ID_HEADER,
    read_protocol_version,
};
use crate::{BusEnvelope, Message, Request, RequestError, Transport};

/// Synchronous-over-async RPC client: send a [`Request`], await its reply.
///
/// Cheap to clone: every clone shares the same `RequestClientInner`
/// through an [`Arc`], so cloning is how a caller hands out further handles
/// to the same registry, reply consumer and lifecycle. The last handle to
/// drop signals shutdown; see [`Self::close`] for the stronger, awaited
/// alternative.
pub struct RequestClient<T: Transport> {
    inner: Arc<RequestClientInner<T>>,
}

impl<T: Transport> Clone for RequestClient<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Shared state behind every [`RequestClient`] handle.
///
/// Never constructed or named directly by a caller: it exists so the
/// client's lifecycle (the shutdown [`CancellationToken`] and the reply
/// consumer's [`JoinHandle`]) is tied to the last handle disappearing,
/// through its [`Drop`] implementation, rather than to any single clone.
/// `pub(crate)` rather than `pub`: nothing outside this crate can name it,
/// since it has no public constructor or method of its own.
pub(crate) struct RequestClientInner<T: Transport> {
    transport: Arc<T>,
    registry: Arc<RequestRegistry>,
    reply_inbox: Arc<Mutex<ReplyInboxState>>,
    default_timeout: Duration,
    cancel: CancellationToken,
    publication_lifecycle: PublicationLifecycle,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    supervisor_task_id: Option<tokio::task::Id>,
    /// Cancelled by the reply consumer task itself, right before it
    /// returns, on every exit path. This is what lets a concurrent
    /// [`RequestClient::close`] caller, one that finds the supervisor
    /// handle already taken, observe genuine termination instead of
    /// racing ahead of it: see [`RequestClient::close`] for why a `take`
    /// alone is not enough once the client is cloned.
    ///
    /// `None` exactly when this client was built with no supervisor at
    /// all (`supervisor: None` at construction): there is then nothing
    /// for [`RequestClient::close`] to wait for, on any branch, and no
    /// token anyone would ever cancel. `Some` and the `supervisor` field
    /// above are set together, from the same constructor parameter (see
    /// [`RequestClient::new`]), so the two can never disagree about
    /// whether a real consumer exists.
    finished: Option<CancellationToken>,
}

/// Coordinates the boundary between admitting a request publication and
/// starting shutdown. Once shutdown begins, no new publication can be
/// admitted; shutdown then waits for the already-admitted publications to
/// return from their transport call.
struct PublicationLifecycle {
    state: Mutex<PublicationState>,
    drained: watch::Sender<bool>,
}

struct PublicationState {
    closing: bool,
    in_flight: usize,
}

impl Default for PublicationLifecycle {
    fn default() -> Self {
        let (drained, _receiver) = watch::channel(true);
        Self {
            state: Mutex::new(PublicationState {
                closing: false,
                in_flight: 0,
            }),
            drained,
        }
    }
}

impl PublicationLifecycle {
    /// Admit one publication, unless shutdown has already started.
    fn admit(&self) -> Result<PublicationPermit<'_>, RequestError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closing {
            return Err(RequestError::Closed);
        }
        if state.in_flight == 0 {
            self.drained.send_replace(false);
        }
        state.in_flight += 1;
        Ok(PublicationPermit { lifecycle: self })
    }

    /// Mark shutdown as started and return a receiver when publication work
    /// remains. The caller must create this receiver before closing the
    /// registry, because closing the registry wakes the corresponding calls.
    fn begin_close(&self) -> Option<watch::Receiver<bool>> {
        let receiver = self.drained.subscribe();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.closing = true;
        (state.in_flight != 0).then_some(receiver)
    }

    fn is_closing(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closing
    }

    async fn wait_for_drain(mut receiver: watch::Receiver<bool>) {
        while !*receiver.borrow() {
            // `RequestClientInner` owns `drained` for the lifetime of every
            // close call, so this branch is unreachable in normal operation.
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

struct PublicationPermit<'a> {
    lifecycle: &'a PublicationLifecycle,
}

impl Drop for PublicationPermit<'_> {
    fn drop(&mut self) {
        let mut state = self
            .lifecycle
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        debug_assert!(
            state.in_flight > 0,
            "every publication permit is dropped exactly once"
        );
        state.in_flight = state.in_flight.saturating_sub(1);
        if state.in_flight == 0 {
            self.lifecycle.drained.send_replace(true);
        }
    }
}

impl<T: Transport> Drop for RequestClientInner<T> {
    /// Signal shutdown when the last handle disappears: close the registry
    /// and cancel the token.
    ///
    /// This is the weak half of the client's shutdown contract: it can only
    /// signal, never wait, since a `drop` that blocks a tokio worker is a
    /// deadlock under load. It does not join the reply consumer task, which
    /// may still be mid-teardown when this returns. A caller that needs the
    /// stronger guarantee, that the consumer has actually stopped, must call
    /// [`RequestClient::close`] explicitly before dropping its last handle.
    fn drop(&mut self) {
        self.registry.close();
        self.cancel.cancel();
    }
}

impl<T: Transport> RequestClient<T> {
    /// Assemble a client from its collaborators. The `reply_inbox` is shared
    /// so a transport supervisor can update it across reconnects.
    ///
    /// `default_timeout`, like a per-call override through
    /// [`RequestOptions::with_timeout`](crate::RequestOptions::with_timeout),
    /// has no ceiling enforced here: a value beyond the one hour horizon a
    /// responder applies is honored locally, in full, but the published
    /// request carries no `x-hexeract-deadline` header, since a responder
    /// would only refuse a deadline built past that horizon. A responder
    /// therefore cannot refuse such a call early.
    ///
    /// [`Self::request_with`] reads `reply_inbox` only after it has
    /// registered with `registry`, never before: that order is what lets
    /// [`ReplyInboxState`] close the reconnect race instead of leaving a
    /// window inside it. See [`ReplyInboxState`] for the exact guarantee
    /// this gives a caller.
    ///
    /// `supervisor` is an opaque handle built by
    /// [`RequestClientSupervisor::spawn`] or
    /// [`RequestClientSupervisor::detached`], and is the sole source of the
    /// shutdown token this client's reply consumer, if any, observes to
    /// stop: [`RequestClientSupervisor::spawn`] hands the consumer task the
    /// very token the supervisor holds, rather than this constructor taking
    /// an independent `cancel` argument the way earlier revisions of this
    /// crate did, so a caller can no longer assemble a client whose task
    /// watches a token unrelated to the one [`Self::close`] cancels.
    ///
    /// What this still cannot verify is whether the task passed to
    /// [`RequestClientSupervisor::spawn`] actually reacts to the token it is
    /// handed: nothing forces that task to return once the token is
    /// cancelled. A task that ignores its argument keeps running, and
    /// [`Self::close`] then waits for its genuine termination for as long as
    /// that task keeps running, unless the caller aborts it explicitly
    /// through [`RequestClientSupervisor::abort_handle`].
    /// [`RequestClientSupervisor::detached`] is the escape hatch for a
    /// client with no real consumer, such as one built for a unit test:
    /// [`Self::close`] then has nothing to wait for and returns as soon as
    /// it has closed the registry and cancelled the token.
    #[must_use]
    pub fn new(
        transport: Arc<T>,
        registry: Arc<RequestRegistry>,
        reply_inbox: Arc<Mutex<ReplyInboxState>>,
        default_timeout: Duration,
        supervisor: RequestClientSupervisor,
    ) -> Self {
        let (cancel, supervisor, supervisor_task_id, finished) = supervisor.into_parts();
        Self {
            inner: Arc::new(RequestClientInner {
                transport,
                registry,
                reply_inbox,
                default_timeout,
                cancel,
                publication_lifecycle: PublicationLifecycle::default(),
                supervisor: Mutex::new(supervisor),
                supervisor_task_id,
                finished,
            }),
        }
    }

    /// Reject new calls, wait for already-admitted publications to return from
    /// the transport, then wait for the reply consumer to actually stop.
    ///
    /// This is the strong half of the client's shutdown contract: when this
    /// method returns, the registry is closed and empty, and the reply
    /// consumer task, if this client was built with one, has genuinely
    /// finished. That guarantee holds for every caller and every clone,
    /// including one that calls `close` concurrently with another: the
    /// caller that finds the supervisor handle already taken does not
    /// return early, it waits for the same termination signal the
    /// consumer itself raises right before it exits. Compare with the
    /// [`Drop`] impl on `RequestClientInner`, which fires when the last
    /// handle disappears without an explicit `close`: it can only signal
    /// (close the registry, cancel the token) and never await the
    /// consumer.
    ///
    /// This also cancels the [`CancellationToken`] given to the
    /// [`RequestClientSupervisor`] this client was built with, through
    /// [`RequestClientSupervisor::spawn`] or
    /// [`RequestClientSupervisor::detached`]. That token is not private to
    /// this client: a caller that kept its own clone before handing one to
    /// the supervisor may share it with other tasks, such as responder
    /// workers it wants to wind down together with the client; see the
    /// crate's request-reply example for that pattern.
    ///
    /// This waits for the consumer task's actual termination, not merely
    /// for the token to be cancelled: a normal return, a panic, or an
    /// abort through [`RequestClientSupervisor::abort_handle`] all count
    /// and wake this call. The one case this cannot bound is a task that
    /// neither observes its token nor is ever aborted: `close` then waits
    /// for exactly as long as that task keeps running, which may be
    /// forever. [`RequestClientSupervisor::spawn`]'s documentation covers
    /// what it cannot guarantee about the task it spawns.
    ///
    /// Idempotent: the first call takes the supervisor handle and awaits it
    /// directly; every later call, sequential or concurrent, finds `None`
    /// and awaits the same termination signal instead, without changing
    /// registry or token state again. If this client was built with no
    /// supervisor at all, there is no termination signal to wait for on
    /// either branch. Every call, first or later, still waits for any
    /// publication admitted before shutdown, then returns as soon as it has
    /// closed the registry and cancelled `cancel`.
    ///
    /// A call made by the supervisor task itself performs the shutdown signal
    /// but does not await its own join handle. Awaiting itself would deadlock;
    /// another caller still observes actual completion through the shared
    /// termination signal.
    ///
    /// If publication is already admitted when `close` begins, this method
    /// waits until that transport call returns before cancelling the shared
    /// token. Each admitted call is bounded by its own resolved request
    /// timeout, so shutdown can wait up to the longest such remaining
    /// deadline. This preserves the responder while a publication is still
    /// travelling to the broker; a call whose reply slot is then drained
    /// reports [`RequestError::PublicationUnknown`] rather than the safe
    /// pre-publication [`RequestError::Closed`] outcome.
    pub async fn close(&self) {
        let publication_drain = self.inner.publication_lifecycle.begin_close();
        self.inner.registry.close();
        if let Some(receiver) = publication_drain {
            PublicationLifecycle::wait_for_drain(receiver).await;
        }
        self.inner.cancel.cancel();
        // tokio may reuse a task `Id` once it has been retired, but only
        // after the task has both exited and its `JoinHandle` has been
        // dropped. Neither has happened yet at this point: the comparison
        // below still holds this client's own `JoinHandle` in `supervisor`
        // (or has already taken and awaited it on an earlier call), so the
        // `Id` being compared against cannot have been recycled to some
        // unrelated task. Reuse would only become a concern after this
        // `close` call itself has returned, by which point `finished` is
        // already cancelled and every closer, on any branch, returns
        // immediately regardless of what `try_id()` reports.
        //
        // `try_id` rather than `id`: `id` panics outside a task context, and
        // `close` may legitimately be called from a plain `block_on`, which
        // tokio does not assign any task id to at all.
        if self
            .inner
            .supervisor_task_id
            .is_some_and(|id| tokio::task::try_id() == Some(id))
        {
            return;
        }
        let handle = self
            .inner
            .supervisor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match handle {
            Some(handle) => {
                let _ = handle.await;
            }
            None => {
                if let Some(finished) = &self.inner.finished {
                    finished.cancelled().await;
                }
            }
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

    /// Send `request`, applying `options` on top of this client's own
    /// defaults.
    ///
    /// Resolution order:
    /// - timeout: `options.timeout` if set, otherwise this client's default
    ///   timeout;
    /// - destination: `options.destination` if set, otherwise
    ///   `R::DESTINATION`;
    /// - causal chain: `options.correlation_id` if set, joining the chain it
    ///   identifies, otherwise a fresh one minted for this call.
    ///
    /// # Errors
    ///
    /// The first three are refused by this client before anything is
    /// published: the responder never saw the request, and the call left no
    /// trace anywhere. They are grouped because that shared property, not
    /// their cause, is what decides whether retrying is safe.
    ///
    /// - [`RequestError::AtCapacity`] if the client already holds
    ///   `max_in_flight` calls. Back-pressure is reported as an immediate
    ///   failure rather than queued behind a free slot, so a saturated
    ///   client stays distinguishable from a slow responder. Retrying at
    ///   once cannot succeed, since nothing has been released in the
    ///   meantime.
    /// - [`RequestError::Closed`] if [`Self::close`] started before this call
    ///   was admitted for publication. The responder never saw the request,
    ///   so this error is safe to treat as pre-publication.
    /// - [`RequestError::Protocol`] carrying
    ///   [`ProtocolViolation::IdentityCollision`] if the request identity
    ///   minted for this call is already registered. Retrying is safe, and
    ///   is the intended response.
    ///
    /// - [`RequestError::Encode`] if the outbound request cannot be serialized.
    ///   This is guaranteed to happen before the transport is called, so
    ///   nothing was published and the responder cannot have acted on it.
    ///
    /// The rest are reported while publishing the request, or after it has
    /// been published, so the responder may already have acted on it.
    ///
    /// - [`RequestError::Transport`] if publishing fails or the reply channel
    ///   is lost (connection dropped).
    /// - [`RequestError::PublicationUnknown`] if shutdown began after the
    ///   transport accepted the request but before a reply established its
    ///   outcome. The responder may have acted, so retrying can duplicate
    ///   side effects.
    /// - [`RequestError::Timeout`] if publishing and waiting for a reply do
    ///   not both complete within the single resolved timeout. A timeout
    ///   racing publication is ambiguous: the transport future is cancelled,
    ///   but the broker may already have accepted the request and the
    ///   responder may act on it. This is also what a legitimate call observes
    ///   when every delivery bearing its request identity violates the
    ///   request-reply protocol: an unsupported or missing protocol version,
    ///   a missing or unrecognized reply status, or a reply message type other
    ///   than the one expected. The registry ignores such deliveries without
    ///   waking the caller, so the slot stays open for the real reply; if none
    ///   arrives before the timeout, the call times out rather than surfacing
    ///   the violation that caused the delivery to be ignored.
    /// - [`RequestError::Protocol`] again, this time carrying one of the
    ///   other [`ProtocolViolation`] variants, if a delivery reaches this
    ///   decoding step while failing one of those same checks. Unlike the
    ///   collision above, this one arrives after publication, so retrying
    ///   may reach a responder that already served the call. It is a
    ///   reachable defense-in-depth path, not one a well-behaved registry
    ///   exercises today.
    /// - [`RequestError::Remote`] if the responder reported a failure.
    /// - [`RequestError::Decode`] if a reply that already passed protocol and
    ///   status validation cannot be decoded: either an ok reply whose payload
    ///   does not decode into the expected reply type, or an error reply whose
    ///   `message_type` matches but whose payload does not decode into a
    ///   [`RemoteErrorPayload`]. Since a reply exists, the request was already
    ///   published and the responder may have acted on it.
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
    ///
    /// // A handler forwards `GetBalance` on the caller's own causal chain
    /// // instead of opening a new one, by passing the `correlation_id` its
    /// // `HandlerContext` carried.
    /// async fn forward_on_the_same_chain<T: Transport>(
    ///     client: &RequestClient<T>,
    ///     ctx: &hexeract_core::HandlerContext,
    ///     account_id: uuid::Uuid,
    /// ) -> Balance {
    ///     let options = RequestOptions::new().with_correlation_id(ctx.correlation_id);
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
        let timeout = options.timeout.unwrap_or(self.inner.default_timeout);
        let destination = options.destination.as_deref().unwrap_or(R::DESTINATION);
        let correlation_id = options.correlation_id.unwrap_or_default();
        self.request_inner(&request, destination, timeout, correlation_id)
            .await
    }

    async fn request_inner<R: Request>(
        &self,
        request: &R,
        destination: &str,
        timeout: Duration,
        correlation_id: CorrelationId,
    ) -> Result<R::Reply, RequestError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let request_id = RequestId::new();
        // Registering first, and only then reading the inbox state, is
        // what closes the reconnect race: see the `reply_inbox` doc on
        // `Self::new` and `ReplyInboxState` for why this order, not the
        // reverse, is load-bearing rather than incidental.
        let mut pending = self
            .inner
            .registry
            .register(request_id, ReplyExpectation::new(R::Reply::MESSAGE_TYPE))?;
        let correlation_id = *correlation_id.as_uuid();
        let inbox = match &*self
            .inner
            .reply_inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            ReplyInboxState::Ready(inbox) => inbox.clone(),
            ReplyInboxState::Reconnecting => {
                return Err(RequestError::Transport(reply_inbox_reconnecting()));
            }
        };
        let mut envelope = BusEnvelope::with_reply_to(correlation_id, inbox, request)
            .map_err(RequestError::Encode)?;
        envelope.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        envelope.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
        // A timeout beyond the horizon a responder enforces would only be
        // refused as `BeyondHorizon`; publishing no header at all restores
        // the pre-deadline behaviour for that call instead of clamping it,
        // which would make a responder refuse work its caller is still
        // genuinely waiting for.
        if let Some(deadline) = Deadline::within_horizon(timeout) {
            envelope.insert_protocol_header(DEADLINE_HEADER, deadline.to_string());
        }
        let publication = self.inner.publication_lifecycle.admit()?;
        match tokio::time::timeout_at(
            deadline,
            self.inner
                .transport
                .publish_envelope(destination, &envelope),
        )
        .await
        {
            Err(_elapsed) => return Err(RequestError::Timeout(timeout)),
            Ok(Err(error)) => return Err(RequestError::Transport(error)),
            Ok(Ok(_message_id)) => {}
        }
        drop(publication);

        let reply = match tokio::time::timeout_at(deadline, pending.wait()).await {
            Err(_elapsed) => return Err(RequestError::Timeout(timeout)),
            Ok(Err(_closed)) => {
                // The registry drops every sender on both `close` (permanent)
                // and `drain` (transient, on broker loss). A close, including
                // a manual registry close supplied through the public
                // constructor, cannot establish that a successfully published
                // request had no side effects, so it stays conservative.
                return Err(
                    if self.inner.publication_lifecycle.is_closing()
                        || self.inner.registry.is_closed()
                    {
                        RequestError::PublicationUnknown
                    } else {
                        RequestError::Transport(reply_channel_lost())
                    },
                );
            }
            Ok(Ok(envelope)) => envelope,
        };

        decode_reply::<R>(reply)
    }
}

fn reply_channel_lost() -> crate::BusError {
    crate::BusError::connection("reply inbox channel closed before a reply arrived", true)
}

/// Built when [`ReplyInboxState::Reconnecting`] is observed, refusing to
/// publish toward an inbox that no longer exists.
fn reply_inbox_reconnecting() -> crate::BusError {
    crate::BusError::connection(
        "reply inbox is reconnecting: no fresh inbox exists yet after a broker drop",
        true,
    )
}

/// Validate a reply against the protocol, then decode it.
///
/// Checks are ordered from the most structural to the most specific: an
/// unsupported version makes every later check meaningless, so it comes
/// first.
fn decode_reply<R: Request>(reply: BusEnvelope) -> Result<R::Reply, RequestError> {
    match read_protocol_version(&reply) {
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

    match reply.header(REPLY_STATUS_HEADER) {
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::SystemTime;

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    use uuid::Uuid;

    use super::*;
    use crate::BusError;
    use crate::deadline::{Deadline, DeadlineReading};
    use crate::remote_error::RemoteErrorType;
    use crate::request_options::RequestOptions;
    use crate::request_registry::ReplyCountersSnapshot;
    use crate::rpc_protocol::DEADLINE_HEADER;

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

    /// A request fixture that always fails JSON serialization, used to prove
    /// that request encoding is still on the pre-publication side of the RPC
    /// boundary.
    #[derive(Debug, Deserialize)]
    struct UnserializableRequest;

    impl Serialize for UnserializableRequest {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom(
                "deliberate request encoding failure",
            ))
        }
    }

    impl Message for UnserializableRequest {
        const MESSAGE_TYPE: &'static str = "tests.unserializable_request";
    }

    impl Request for UnserializableRequest {
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

    /// Holds publication at an explicit gate so timeout tests can place the
    /// client on either side of the broker-acceptance boundary without relying
    /// on scheduler timing.
    #[derive(Default)]
    struct GatedTransport {
        publish_started: AtomicBool,
        started: Notify,
        release: Notify,
        published: StdMutex<Vec<(String, BusEnvelope)>>,
    }

    #[async_trait]
    impl Transport for GatedTransport {
        async fn publish_envelope(
            &self,
            routing_key: &str,
            envelope: &BusEnvelope,
        ) -> Result<Uuid, BusError> {
            self.publish_started.store(true, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            self.published
                .lock()
                .unwrap()
                .push((routing_key.to_owned(), envelope.clone()));
            Ok(envelope.message_id)
        }
    }

    impl GatedTransport {
        async fn wait_until_publish_started(&self) {
            if !self.publish_started.load(Ordering::SeqCst) {
                self.started.notified().await;
            }
        }

        fn release_publish(&self) {
            self.release.notify_one();
        }

        fn last_published(&self) -> Option<BusEnvelope> {
            self.published
                .lock()
                .unwrap()
                .last()
                .map(|(_, envelope)| envelope.clone())
        }
    }

    fn gated_client(
        transport: Arc<GatedTransport>,
        registry: Arc<RequestRegistry>,
        default_timeout: Duration,
    ) -> RequestClient<GatedTransport> {
        RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            default_timeout,
            RequestClientSupervisor::detached(CancellationToken::new()),
        )
    }

    /// Read the request identity the client stamped on its published envelope.
    fn published_request_id(published: &BusEnvelope) -> RequestId {
        let raw = published
            .header(REQUEST_ID_HEADER)
            .expect("client stamps a request id header on every request");
        RequestId::from(
            raw.parse::<Uuid>()
                .expect("request id header must be a valid uuid"),
        )
    }

    fn ok_reply(request_id: RequestId, seq: u64) -> BusEnvelope {
        let mut env = BusEnvelope::new(Uuid::now_v7(), &Pong { seq }).unwrap();
        env.insert_protocol_header(REPLY_STATUS_HEADER, REPLY_STATUS_OK.to_owned());
        env.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        env.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
        env
    }

    /// A client with no real reply consumer. None of the tests using this
    /// helper call `close`, but a client built this way must still behave
    /// sanely if one did: with `supervisor: None`, `close` has nothing to
    /// wait for on either branch and returns as soon as it has closed the
    /// registry and cancelled `cancel`.
    fn client(
        transport: Arc<CapturingTransport>,
        registry: Arc<RequestRegistry>,
    ) -> RequestClient<CapturingTransport> {
        RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_millis(200),
            RequestClientSupervisor::detached(CancellationToken::new()),
        )
    }

    /// Build a client whose opaque supervisor, and the token it owns, are
    /// both under the caller's control, for the lifecycle tests below.
    fn client_with_lifecycle(
        transport: Arc<CapturingTransport>,
        registry: Arc<RequestRegistry>,
        supervisor: RequestClientSupervisor,
    ) -> RequestClient<CapturingTransport> {
        RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_millis(200),
            supervisor,
        )
    }

    /// A call that reads `Reconnecting` must fail before it ever reaches
    /// the transport, and before any timeout elapses: it is refused at
    /// the door, not by the timer.
    ///
    /// "Sans publier" is proven by the transport itself, via
    /// `CapturingTransport::last_published`, not deduced from the error
    /// variant alone. The paused clock proves "sans attendre le timeout":
    /// under `start_paused = true`, any real wait on the 30 second
    /// timeout would need tokio's virtual clock to advance, which only
    /// happens if every task is parked on a timer; a call that returns
    /// without ever registering a timer leaves the clock exactly where it
    /// started.
    #[tokio::test(start_paused = true)]
    async fn a_call_during_reconnecting_fails_fast_without_publishing() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Reconnecting));
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            reply_inbox,
            Duration::from_secs(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        let started = tokio::time::Instant::now();
        let error = client
            .request(Ping { seq: 1 })
            .await
            .expect_err("a reconnecting inbox must be refused");

        assert!(matches!(error, RequestError::Transport(_)));
        assert!(
            tokio::time::Instant::now() - started < Duration::from_millis(1),
            "a reconnecting inbox must fail before any timeout elapses"
        );
        assert!(
            transport.last_published().is_none(),
            "a reconnecting inbox must never be published to"
        );
    }

    /// The narrow half of resolution 4: a closed client must be rejected
    /// as `Closed`, not `Transport`, even while the inbox is
    /// `Reconnecting`. `register()` runs before the inbox state is ever
    /// read, so a closed registry short-circuits the call before it gets
    /// the chance to observe `Reconnecting` at all; this test checks that
    /// property directly rather than assuming it holds because the code
    /// happens to be written in that order today.
    #[tokio::test]
    async fn a_closed_client_is_rejected_as_closed_even_when_the_inbox_is_reconnecting() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        registry.close();
        let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Reconnecting));
        let client = RequestClient::new(
            transport,
            registry,
            reply_inbox,
            Duration::from_millis(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        let error = client
            .request(Ping { seq: 1 })
            .await
            .expect_err("a closed client refuses new calls");
        assert!(matches!(error, RequestError::Closed));
    }

    /// After the shared state is updated with a fresh inbox name, exactly
    /// what the supervisor does on reconnect, a subsequent call must
    /// publish to the new address, never the one it replaces.
    #[tokio::test(start_paused = true)]
    async fn a_call_uses_the_new_inbox_never_the_old_once_the_supervisor_marks_it_ready() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Ready("old.inbox".to_owned())));
        let client = RequestClient::new(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Arc::clone(&reply_inbox),
            Duration::from_secs(5),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        let first_fut = client.request(Ping { seq: 1 });
        tokio::pin!(first_fut);
        tokio::select! {
            _ = &mut first_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let first = transport.last_published().expect("first request published");
        assert_eq!(first.reply_to.as_deref(), Some("old.inbox"));

        *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner) =
            ReplyInboxState::Ready("new.inbox".to_owned());

        let second_fut = client.request(Ping { seq: 2 });
        tokio::pin!(second_fut);
        tokio::select! {
            _ = &mut second_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let second = transport
            .last_published()
            .expect("second request published");
        assert_eq!(second.reply_to.as_deref(), Some("new.inbox"));
        assert_ne!(second.reply_to, first.reply_to);
    }

    /// Non-regression: a call already in flight when the connection drops
    /// must still fail fast, exactly as it did before this state existed.
    /// Mirrors what the supervisor does, in the mandated order: mark
    /// `Reconnecting`, then drain.
    #[tokio::test(start_paused = true)]
    async fn an_in_flight_call_fails_fast_when_the_supervisor_marks_and_drains() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned())));
        let client = RequestClient::new(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Arc::clone(&reply_inbox),
            Duration::from_secs(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        let request_fut = client.request(Ping { seq: 1 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        assert_eq!(
            registry.len(),
            1,
            "the call must be registered before the drop"
        );

        *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner) = ReplyInboxState::Reconnecting;
        registry.drain();

        let error = tokio::time::timeout(Duration::from_millis(1), request_fut)
            .await
            .expect("a drained slot must resolve well before the 30s timeout")
            .expect_err("connection loss must surface as an error");
        assert!(matches!(error, RequestError::Transport(_)));
    }

    #[tokio::test]
    async fn an_unencodable_request_fails_before_the_transport_is_called() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let error = client
            .request(UnserializableRequest)
            .await
            .expect_err("the request cannot be encoded");

        assert!(matches!(
            error,
            RequestError::Encode(BusError::Serialization(_))
        ));
        assert!(
            transport.last_published().is_none(),
            "encoding must fail before the transport is called"
        );
        assert!(
            registry.is_empty(),
            "the pre-publication failure must release its pending slot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_bounds_a_publication_that_never_completes() {
        let timeout = Duration::from_millis(30);
        let transport = Arc::new(GatedTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = gated_client(Arc::clone(&transport), Arc::clone(&registry), timeout);
        let started_at = tokio::time::Instant::now();

        let request = client.request(Ping { seq: 1 });
        tokio::pin!(request);
        tokio::select! {
            () = transport.wait_until_publish_started() => {}
            result = &mut request => panic!("publication unexpectedly completed: {result:?}"),
        }
        tokio::time::advance(timeout).await;

        let error = request
            .await
            .expect_err("the publication must share the request timeout");

        assert!(matches!(error, RequestError::Timeout(elapsed) if elapsed == timeout));
        assert_eq!(tokio::time::Instant::now() - started_at, timeout);
        assert!(
            transport.last_published().is_none(),
            "the gated transport never crossed its acceptance boundary"
        );
        assert!(
            registry.is_empty(),
            "timing out publication must release the pending slot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn publication_latency_consumes_the_reply_wait_budget_and_late_replies_are_orphaned() {
        let timeout = Duration::from_millis(30);
        let publish_latency = Duration::from_millis(20);
        let transport = Arc::new(GatedTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = gated_client(Arc::clone(&transport), Arc::clone(&registry), timeout);
        let started_at = tokio::time::Instant::now();

        let request = client.request(Ping { seq: 1 });
        tokio::pin!(request);
        tokio::select! {
            () = transport.wait_until_publish_started() => {}
            result = &mut request => panic!("publication unexpectedly completed: {result:?}"),
        }
        tokio::time::advance(publish_latency).await;
        transport.release_publish();
        tokio::select! {
            biased;
            result = &mut request => panic!("request unexpectedly completed: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        let published = transport
            .last_published()
            .expect("the transport crossed its acceptance boundary");

        tokio::time::advance(timeout - publish_latency).await;
        let error = request
            .await
            .expect_err("the reply wait receives only the remaining budget");

        assert!(matches!(error, RequestError::Timeout(elapsed) if elapsed == timeout));
        assert_eq!(tokio::time::Instant::now() - started_at, timeout);
        assert!(registry.is_empty(), "the timed-out slot must be released");

        registry.resolve(ok_reply(published_request_id(&published), 1));
        assert_eq!(
            registry.counters().orphaned,
            1,
            "a reply after the absolute deadline must remain orphaned"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_inside_the_single_absolute_deadline_still_succeeds() {
        let timeout = Duration::from_millis(30);
        let publish_latency = Duration::from_millis(20);
        let transport = Arc::new(GatedTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = gated_client(Arc::clone(&transport), Arc::clone(&registry), timeout);
        let started_at = tokio::time::Instant::now();

        let request = client.request(Ping { seq: 7 });
        tokio::pin!(request);
        tokio::select! {
            () = transport.wait_until_publish_started() => {}
            result = &mut request => panic!("publication unexpectedly completed: {result:?}"),
        }
        tokio::time::advance(publish_latency).await;
        transport.release_publish();
        tokio::select! {
            biased;
            result = &mut request => panic!("request unexpectedly completed: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        let published = transport
            .last_published()
            .expect("the transport crossed its acceptance boundary");

        tokio::time::advance(Duration::from_millis(9)).await;
        registry.resolve(ok_reply(published_request_id(&published), 7));

        assert_eq!(
            request.await.expect("reply is inside the deadline"),
            Pong { seq: 7 }
        );
        assert_eq!(
            tokio::time::Instant::now() - started_at,
            Duration::from_millis(29)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_during_publication_releases_the_pending_slot() {
        let transport = Arc::new(GatedTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = gated_client(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Duration::from_secs(30),
        );

        let mut request = Box::pin(client.request(Ping { seq: 1 }));
        tokio::select! {
            () = transport.wait_until_publish_started() => {}
            result = &mut request => panic!("publication unexpectedly completed: {result:?}"),
        }
        assert_eq!(registry.len(), 1, "the request must hold one pending slot");

        drop(request);

        assert!(
            registry.is_empty(),
            "cancelling the request future must release its pending slot"
        );
        assert!(
            transport.last_published().is_none(),
            "the cancelled publication never crossed the test transport gate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nominal_round_trip_returns_typed_reply() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
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
        assert!(
            published
                .headers
                .keys()
                .all(|key| !crate::rpc_protocol::is_reserved_header(key)),
            "request protocol fields must stay out of application headers"
        );
        assert!(published.header(REQUEST_ID_HEADER).is_some());
        assert_eq!(published.header(PROTOCOL_VERSION_HEADER), Some("1"));
        registry.resolve(ok_reply(published_request_id(&published), 3));
        let pong = request_fut.await.expect("reply");
        assert_eq!(pong, Pong { seq: 3 });
    }

    #[tokio::test(start_paused = true)]
    async fn a_published_request_carries_a_deadline_derived_from_the_default_timeout() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));
        let timeout = Duration::from_millis(200);

        let before = SystemTime::now();
        let request_fut = client.request(Ping { seq: 1 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let after = SystemTime::now();

        let published = transport.last_published().expect("a request was published");
        let deadline: Deadline = published
            .header(DEADLINE_HEADER)
            .expect("a request carries a deadline")
            .parse()
            .expect("a decimal millisecond count");
        assert!(matches!(
            deadline.anchor(SystemTime::now()),
            DeadlineReading::Live(_)
        ));
        let earliest_allowed =
            Deadline::from_wall_clock(before, timeout - Duration::from_millis(1));
        let latest_allowed = Deadline::from_wall_clock(after, timeout);
        assert!(
            deadline >= earliest_allowed,
            "the published deadline is earlier than the effective timeout allows"
        );
        assert!(
            deadline <= latest_allowed,
            "the published deadline is later than the effective timeout allows"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_explicit_per_call_timeout_replaces_the_client_default() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));
        let timeout = Duration::from_secs(600);

        let before = SystemTime::now();
        let request_fut =
            client.request_with(Ping { seq: 1 }, RequestOptions::new().with_timeout(timeout));
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let after = SystemTime::now();

        let published = transport.last_published().expect("a request was published");
        let deadline: Deadline = published
            .header(DEADLINE_HEADER)
            .expect("a request carries a deadline")
            .parse()
            .expect("a decimal millisecond count");
        assert!(
            deadline > Deadline::from_wall_clock(SystemTime::now(), Duration::from_millis(200)),
            "a ten minute call must publish a deadline far beyond the client default"
        );
        let earliest_allowed =
            Deadline::from_wall_clock(before, timeout - Duration::from_millis(1));
        let latest_allowed = Deadline::from_wall_clock(after, timeout);
        assert!(
            deadline >= earliest_allowed,
            "the published deadline is earlier than the effective timeout allows"
        );
        assert!(
            deadline <= latest_allowed,
            "the published deadline is later than the effective timeout allows"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_beyond_the_horizon_publishes_no_deadline_but_still_enforces_it_locally() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));
        // One second beyond `deadline::MAX_DEADLINE_HORIZON`, which is
        // private to that module; the value is mirrored here.
        let timeout = Duration::from_secs(3_601);

        let request_fut =
            client.request_with(Ping { seq: 1 }, RequestOptions::new().with_timeout(timeout));
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let published = transport.last_published().expect("a request was published");
        assert!(
            published.header(DEADLINE_HEADER).is_none(),
            "a timeout beyond the horizon must publish no deadline header at all"
        );
        assert!(published.header(REQUEST_ID_HEADER).is_some());
        assert_eq!(published.header(PROTOCOL_VERSION_HEADER), Some("1"));

        tokio::time::advance(timeout).await;
        let error = request_fut
            .await
            .expect_err("no reply arrived before the caller's own local timeout");
        assert!(matches!(error, RequestError::Timeout(elapsed) if elapsed == timeout));
    }

    #[tokio::test]
    async fn silent_responder_times_out() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            transport,
            Arc::clone(&registry),
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_millis(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );
        let err = client.request(Ping { seq: 1 }).await.expect_err("no reply");
        assert!(matches!(err, RequestError::Timeout(_)));
        assert_eq!(registry.len(), 0);
    }

    /// The `RegisterRejection::AtCapacity -> RequestError::AtCapacity`
    /// mapping is tested directly in `request_error`, but that alone does
    /// not prove a caller of `RequestClient::request` ever observes it: the
    /// `?` in `request_inner` is the only path by which a full registry
    /// becomes visible to a user of this crate. This test drives a first
    /// call far enough to occupy the registry's only slot, then leaves it
    /// pending (never resolved) so a second call finds no room.
    #[tokio::test(start_paused = true)]
    async fn a_request_when_the_registry_is_at_capacity_surfaces_at_capacity() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::new(1));
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let first_fut = client.request(Ping { seq: 1 });
        tokio::pin!(first_fut);
        tokio::select! {
            _ = &mut first_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        assert_eq!(
            registry.len(),
            1,
            "the first call must occupy the registry's only slot"
        );

        let error = client
            .request(Ping { seq: 2 })
            .await
            .expect_err("the registry has no free slot for a second call");
        assert!(matches!(error, RequestError::AtCapacity));
    }

    #[tokio::test(start_paused = true)]
    async fn remote_error_reply_maps_to_remote() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
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
        let mut err_env = BusEnvelope::restore(
            Uuid::now_v7(),
            REPLY_ERROR_MESSAGE_TYPE.to_owned(),
            serde_json::to_vec(&payload).unwrap(),
            published.correlation_id,
            None,
            HashMap::default(),
            std::time::SystemTime::UNIX_EPOCH,
        );
        err_env.insert_protocol_header(REPLY_STATUS_HEADER, REPLY_STATUS_ERROR.to_owned());
        err_env.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        err_env.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
        assert!(
            err_env
                .headers
                .keys()
                .all(|key| !crate::rpc_protocol::is_reserved_header(key)),
            "error reply protocol fields must stay out of application headers"
        );
        assert_eq!(
            err_env.header(REPLY_STATUS_HEADER),
            Some(REPLY_STATUS_ERROR)
        );
        assert_eq!(
            err_env.header(REQUEST_ID_HEADER),
            Some(request_id.to_string()).as_deref()
        );
        assert_eq!(err_env.header(PROTOCOL_VERSION_HEADER), Some("1"));
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
            reply.remove_protocol_header(REPLY_STATUS_HEADER);
        })
        .await;
        assert!(matches!(error, RequestError::Timeout(_)));
        assert_eq!(counters.invalid, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_announcing_an_unknown_version_never_reaches_the_caller() {
        let (error, counters) = client_error_for_reply(|_request_id, reply| {
            reply.insert_protocol_header(PROTOCOL_VERSION_HEADER, "99".to_owned());
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
        missing_version.remove_protocol_header(PROTOCOL_VERSION_HEADER);
        let error =
            decode_reply::<Ping>(missing_version).expect_err("missing protocol version header");
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::MissingHeader {
                header: PROTOCOL_VERSION_HEADER
            })
        ));

        let mut unsupported_version = ok_reply(RequestId::new(), 1);
        unsupported_version.insert_protocol_header(PROTOCOL_VERSION_HEADER, "99".to_owned());
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
        missing_status.remove_protocol_header(REPLY_STATUS_HEADER);
        let error = decode_reply::<Ping>(missing_status).expect_err("missing reply status header");
        assert!(matches!(
            error,
            RequestError::Protocol(ProtocolViolation::MissingHeader {
                header: REPLY_STATUS_HEADER
            })
        ));

        let mut unrecognized_status = ok_reply(RequestId::new(), 1);
        unrecognized_status.insert_protocol_header(REPLY_STATUS_HEADER, "pending".to_owned());
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
        let mut envelope = BusEnvelope::restore(
            Uuid::now_v7(),
            REPLY_ERROR_MESSAGE_TYPE.to_owned(),
            serde_json::to_vec(&payload).expect("payload must serialize"),
            Uuid::now_v7(),
            None,
            HashMap::default(),
            std::time::SystemTime::UNIX_EPOCH,
        );
        envelope.insert_protocol_header(REPLY_STATUS_HEADER, REPLY_STATUS_ERROR.to_owned());
        envelope.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        envelope.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
        envelope
    }

    #[test]
    fn a_malformed_nominal_reply_payload_surfaces_as_decode() {
        let mut reply = ok_reply(RequestId::new(), 1);
        reply.payload = b"not json".to_vec();

        let error = decode_reply::<Ping>(reply).expect_err("reply payload is malformed");

        assert!(matches!(
            error,
            RequestError::Decode(BusError::Serialization(_))
        ));
    }

    #[test]
    fn a_malformed_remote_error_payload_surfaces_as_decode() {
        let mut reply = error_reply(RequestId::new());
        reply.payload = b"not json".to_vec();

        let error = decode_reply::<Ping>(reply).expect_err("remote error payload is malformed");

        assert!(matches!(
            error,
            RequestError::Decode(BusError::Serialization(_))
        ));
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
                envelope.remove_protocol_header(PROTOCOL_VERSION_HEADER);
                envelope
            }),
            ("unsupported protocol version", {
                let mut envelope = ok_reply(request_id, 1);
                envelope.insert_protocol_header(PROTOCOL_VERSION_HEADER, "99".to_owned());
                envelope
            }),
            ("missing reply status", {
                let mut envelope = ok_reply(request_id, 1);
                envelope.remove_protocol_header(REPLY_STATUS_HEADER);
                envelope
            }),
            ("unknown reply status", {
                let mut envelope = ok_reply(request_id, 1);
                envelope.insert_protocol_header(REPLY_STATUS_HEADER, "pending".to_owned());
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
            reply.insert_protocol_header(REPLY_STATUS_HEADER, REPLY_STATUS_ERROR.to_owned());
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
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_secs(5),
            RequestClientSupervisor::detached(CancellationToken::new()),
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
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_secs(5),
            RequestClientSupervisor::detached(CancellationToken::new()),
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
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_millis(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
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
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_millis(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
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
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_secs(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
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

    /// `options.correlation_id` joins an existing causal chain: the supplied
    /// identifier, not a freshly minted one, must travel on the wire.
    #[tokio::test]
    async fn options_correlation_id_is_carried_on_the_wire() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_millis(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        let correlation_id = CorrelationId::new();
        let options = RequestOptions::new().with_correlation_id(correlation_id);
        let _ = client.request_with(Ping { seq: 1 }, options).await;

        let published = transport.last_published().expect("a request was published");
        assert_eq!(published.correlation_id, *correlation_id.as_uuid());
    }

    /// Without a `correlation_id` override, `request_with` opens a fresh
    /// causal chain, exactly like `request` (see `request_starts_a_fresh_chain`):
    /// two calls under `RequestOptions::default()` never share theirs.
    #[tokio::test]
    async fn request_with_default_options_opens_a_fresh_correlation_each_call() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_millis(30),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        let _ = client
            .request_with(Ping { seq: 1 }, RequestOptions::default())
            .await;
        let first = transport
            .last_published()
            .expect("first request")
            .correlation_id;

        let _ = client
            .request_with(Ping { seq: 2 }, RequestOptions::default())
            .await;
        let second = transport
            .last_published()
            .expect("second request")
            .correlation_id;

        assert_ne!(first, second);
    }

    /// Two calls sharing a `correlation_id` (the causal chain) still mint
    /// distinct `RequestId`s (the per-call identity) and each resolves to
    /// its own reply rather than crossing over: the shared correlation must
    /// never be usable to key a pending slot.
    #[tokio::test]
    async fn two_calls_sharing_a_correlation_mint_distinct_request_ids_and_do_not_cross_replies() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_secs(5),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );
        let correlation_id = CorrelationId::new();

        let first_fut = client.request_with(
            Ping { seq: 1 },
            RequestOptions::new().with_correlation_id(correlation_id),
        );
        tokio::pin!(first_fut);
        tokio::select! {
            _ = &mut first_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let first = transport.last_published().expect("first request");

        let second_fut = client.request_with(
            Ping { seq: 2 },
            RequestOptions::new().with_correlation_id(correlation_id),
        );
        tokio::pin!(second_fut);
        tokio::select! {
            _ = &mut second_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let second = transport.last_published().expect("second request");

        assert_eq!(first.correlation_id, *correlation_id.as_uuid());
        assert_eq!(second.correlation_id, *correlation_id.as_uuid());
        let first_request_id = published_request_id(&first);
        let second_request_id = published_request_id(&second);
        assert_ne!(first_request_id, second_request_id);

        // Resolve the second call's reply before the first call's, so a slot
        // keyed on the shared correlation_id (rather than on request_id)
        // would deliver the wrong Pong to the wrong caller.
        registry.resolve(ok_reply(second_request_id, 2));
        registry.resolve(ok_reply(first_request_id, 1));

        let first_reply = first_fut.await.expect("first reply");
        let second_reply = second_fut.await.expect("second reply");
        assert_eq!(first_reply, Pong { seq: 1 });
        assert_eq!(second_reply, Pong { seq: 2 });
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
        let mut envelope = BusEnvelope::restore(
            Uuid::now_v7(),
            message_type.to_owned(),
            Vec::new(),
            Uuid::now_v7(),
            None,
            HashMap::default(),
            std::time::SystemTime::now(),
        );
        envelope.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
        envelope.insert_protocol_header(REPLY_STATUS_HEADER, REPLY_STATUS_OK.to_owned());
        envelope.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        envelope
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
        let registry = Arc::new(RequestRegistry::default());
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
        let registry = Arc::new(RequestRegistry::default());
        let client = RequestClient::new(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Arc::new(Mutex::new(ReplyInboxState::Ready(
                "caller.inbox".to_owned(),
            ))),
            Duration::from_millis(100),
            RequestClientSupervisor::detached(CancellationToken::new()),
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
                .header(REQUEST_ID_HEADER)
                .expect("request id header")
                .parse::<Uuid>()
                .expect("request id must parse"),
        );
        let mut reply = BusEnvelope::new(published.correlation_id, &Pong { seq: 1 })
            .expect("pong must serialize");
        reply.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        reply.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
        reply.insert_protocol_header(REPLY_STATUS_HEADER, REPLY_STATUS_OK.to_owned());
        mutate(request_id, &mut reply);
        registry.resolve(reply);

        let error = request_fut
            .await
            .expect_err("the tampered reply must be rejected");
        (error, registry.counters())
    }

    #[tokio::test]
    async fn close_marks_an_already_published_pending_call_unknown() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client_with_lifecycle(
            Arc::clone(&transport),
            Arc::clone(&registry),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        let call = {
            let client = client.clone();
            tokio::spawn(async move { client.request(Ping { seq: 1 }).await })
        };
        tokio::task::yield_now().await;

        // This assertion distinguishes a call that already published from one
        // rejected before it registered, which still returns `Closed` below.
        assert_eq!(registry.len(), 1, "the call must be in flight before close");

        client.close().await;

        let result = call.await.expect("the spawned call must not panic");
        assert!(
            matches!(result, Err(RequestError::PublicationUnknown)),
            "expected RequestError::PublicationUnknown, got {result:?}"
        );
    }

    #[tokio::test]
    async fn close_waits_for_an_admitted_publication_and_marks_its_result_unknown() {
        let transport = Arc::new(GatedTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = gated_client(
            Arc::clone(&transport),
            Arc::clone(&registry),
            Duration::from_secs(30),
        );

        let request = {
            let client = client.clone();
            tokio::spawn(async move { client.request(Ping { seq: 1 }).await })
        };
        transport.wait_until_publish_started().await;

        let close = {
            let client = client.clone();
            tokio::spawn(async move { client.close().await })
        };
        tokio::task::yield_now().await;
        assert!(
            !close.is_finished(),
            "close must wait for a publication admitted before shutdown"
        );

        transport.release_publish();

        let result = request.await.expect("request task must not panic");
        assert!(
            matches!(result, Err(RequestError::PublicationUnknown)),
            "a request published during shutdown must not report the safe Closed error: {result:?}"
        );
        assert!(
            transport.last_published().is_some(),
            "the transport accepted the request before shutdown completed"
        );
        close.await.expect("close task must not panic");
    }

    #[tokio::test]
    async fn close_delays_shared_cancellation_until_admitted_publication_drains() {
        let transport = Arc::new(GatedTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let cancel = CancellationToken::new();
        let client = RequestClient::new(
            Arc::clone(&transport),
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_secs(30),
            RequestClientSupervisor::detached(cancel.clone()),
        );

        let request = {
            let client = client.clone();
            tokio::spawn(async move { client.request(Ping { seq: 1 }).await })
        };
        transport.wait_until_publish_started().await;

        let close = {
            let client = client.clone();
            tokio::spawn(async move { client.close().await })
        };
        tokio::task::yield_now().await;
        assert!(
            !cancel.is_cancelled(),
            "shared cancellation must not stop responders while publication is in flight"
        );

        transport.release_publish();
        let _ = request.await.expect("request task must not panic");
        close.await.expect("close task must not panic");
        assert!(
            cancel.is_cancelled(),
            "close must eventually cancel the caller-owned token"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_between_registration_and_publication_refuses_the_publication() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));
        let reply_inbox = Arc::clone(&client.inner.reply_inbox);
        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let inbox_locker = std::thread::spawn(move || {
            let inbox_lock = reply_inbox.lock().unwrap_or_else(PoisonError::into_inner);
            locked_tx
                .send(())
                .expect("test must observe the inbox lock before starting the request");
            release_rx
                .recv()
                .expect("test must release the inbox lock after shutdown begins");
            drop(inbox_lock);
        });
        locked_rx
            .recv()
            .expect("inbox-locking thread must report that it holds the lock");

        let request = {
            let client = client.clone();
            tokio::spawn(async move { client.request(Ping { seq: 1 }).await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while registry.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request must register before attempting to read the inbox");

        let close = {
            let client = client.clone();
            tokio::spawn(async move { client.close().await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !registry.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("close must begin before the request can reach publication");
        release_tx
            .send(())
            .expect("inbox-locking thread must still await release");
        inbox_locker
            .join()
            .expect("inbox-locking thread must not panic");

        let error = request
            .await
            .expect("request task must not panic")
            .expect_err("shutdown must refuse a publication not yet admitted");
        assert!(matches!(error, RequestError::Closed));
        assert!(
            transport.last_published().is_none(),
            "the request must not reach the transport after shutdown begins"
        );
        close.await.expect("close task must not panic");
    }

    #[tokio::test]
    async fn request_after_close_is_rejected_with_closed() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client_with_lifecycle(
            Arc::clone(&transport),
            Arc::clone(&registry),
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        client.close().await;

        let error = client
            .request(Ping { seq: 1 })
            .await
            .expect_err("a closed client refuses new calls");
        assert!(matches!(error, RequestError::Closed));
    }

    #[tokio::test(start_paused = true)]
    async fn close_called_twice_does_not_panic_and_the_second_call_returns_promptly() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let cancel = CancellationToken::new();
        let finished_signal = CancellationToken::new();
        let stopped = Arc::new(AtomicBool::new(false));
        let supervisor = tokio::spawn({
            let cancel = cancel.clone();
            let finished_signal = finished_signal.clone();
            let stopped = Arc::clone(&stopped);
            async move {
                cancel.cancelled().await;
                stopped.store(true, Ordering::SeqCst);
                finished_signal.cancel();
            }
        });
        let client = client_with_lifecycle(
            transport,
            Arc::clone(&registry),
            RequestClientSupervisor::from_task_for_test(cancel, supervisor, finished_signal),
        );

        client.close().await;
        assert!(stopped.load(Ordering::SeqCst));
        assert!(registry.is_closed());

        // Second call: no supervisor handle is left to await, so it falls
        // through to waiting on the already-cancelled `finished` token
        // instead of the join handle. It must not panic, must not change
        // any observable state again, and above all must not block: under
        // `start_paused = true`, the timeout below is a deterministic
        // two-valued discriminator rather than a wall-clock bound. A
        // `close` that regressed to waiting on the wrong token, one
        // nothing will ever cancel again, would leave every task parked,
        // which is exactly the condition that makes tokio's virtual clock
        // advance on its own, so `Elapsed` would still fire without a
        // single real millisecond passing; this test does not depend on
        // `SystemTime`, so pausing tokio's clock cannot mislead it.
        tokio::time::timeout(Duration::from_secs(2), client.close())
            .await
            .expect("a second close must not block");
        assert!(registry.is_closed(), "a second close must change nothing");
    }

    /// Proves `close` actually awaits the supervisor task rather than only
    /// cancelling its token and returning. The supervisor sets `finished`
    /// only right before it returns, after observing cancellation and
    /// yielding once more: if `close` did not await the handle, this
    /// assertion, which runs immediately after `close` returns, would
    /// observe `finished` still `false`.
    ///
    /// This proof is exact under the default `current_thread` flavor
    /// `#[tokio::test]` uses here: with a single scheduler thread, the
    /// extra `yield_now` below forces a second scheduler turn the
    /// supervisor cannot skip, so a `close` that only yielded once instead
    /// of genuinely awaiting the handle could not observe `finished` as
    /// `true` yet. Under `flavor = "multi_thread"` the same ordering is
    /// only overwhelmingly likely, not guaranteed: a second OS thread could
    /// in principle race the supervisor to completion ahead of the
    /// assertion regardless of what `close` awaited.
    #[tokio::test]
    async fn close_only_returns_after_the_supervisor_task_has_actually_finished() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let cancel = CancellationToken::new();
        let finished_signal = CancellationToken::new();
        let finished = Arc::new(AtomicBool::new(false));
        let supervisor = tokio::spawn({
            let cancel = cancel.clone();
            let finished_signal = finished_signal.clone();
            let finished = Arc::clone(&finished);
            async move {
                cancel.cancelled().await;
                tokio::task::yield_now().await;
                finished.store(true, Ordering::SeqCst);
                finished_signal.cancel();
            }
        });
        let client = client_with_lifecycle(
            transport,
            registry,
            RequestClientSupervisor::from_task_for_test(cancel, supervisor, finished_signal),
        );

        client.close().await;

        assert!(
            finished.load(Ordering::SeqCst),
            "close returned before the supervisor task actually finished"
        );
    }

    /// A `close()` call from a second clone, made while a first clone's
    /// `close()` already holds the supervisor's `JoinHandle`, must wait for
    /// the supervisor to actually finish rather than returning as soon as
    /// it finds the handle already taken. Before the `finished` token
    /// existed, the `None` branch of `close` had nothing to wait on and
    /// returned immediately, so this second, concurrent caller could
    /// observe `close` returning before the reply consumer had genuinely
    /// stopped: exactly the defect this test pins down. Compare with
    /// `close_only_returns_after_the_supervisor_task_has_actually_finished`
    /// above, which proves the equivalent property for the first,
    /// handle-holding caller.
    ///
    /// This proof is exact under the default `current_thread` flavor
    /// `#[tokio::test]` uses here, for the same reason given on the sibling
    /// test above: with a single scheduler thread, the `yield_now` below
    /// forces a scheduler turn the spawned `first_close` cannot skip before
    /// it reaches its own `handle.await`, so by the time `second.close()`
    /// starts, the supervisor handle is deterministically already taken.
    /// The `assert!` right before `second.close()` checks that directly,
    /// rather than assuming it: without it, a flavor change to
    /// `multi_thread` could let `second.close()` win the race and take the
    /// handle itself, and the assertion below would still pass, since
    /// `finished` becomes `true` on either branch, so this test would
    /// silently stop covering the `None` branch it exists to pin down.
    /// Under `flavor = "multi_thread"` the ordering this test relies on is
    /// only overwhelmingly likely, not guaranteed.
    #[tokio::test]
    async fn concurrent_close_from_a_second_clone_also_waits_for_the_supervisor_task() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let cancel = CancellationToken::new();
        let finished_signal = CancellationToken::new();
        let finished = Arc::new(AtomicBool::new(false));
        let supervisor = tokio::spawn({
            let cancel = cancel.clone();
            let finished_signal = finished_signal.clone();
            let finished = Arc::clone(&finished);
            async move {
                cancel.cancelled().await;
                tokio::task::yield_now().await;
                finished.store(true, Ordering::SeqCst);
                finished_signal.cancel();
            }
        });
        let client = client_with_lifecycle(
            transport,
            registry,
            RequestClientSupervisor::from_task_for_test(cancel, supervisor, finished_signal),
        );
        let second = client.clone();

        // Drive the first close() far enough to take the supervisor's
        // `JoinHandle`, a synchronous step performed before any await
        // inside `close`, and park it on `handle.await`, before the second
        // clone starts its own close(). One `yield_now` is the same idiom
        // already used elsewhere in this module, for example in
        // `close_fails_every_pending_call_with_closed`, to let a spawned
        // task run up to its first await point.
        let first_close = tokio::spawn(async move { client.close().await });
        tokio::task::yield_now().await;

        // Pin the branch: the second, concurrent close() below must find
        // the supervisor handle already taken by the first close(), so it
        // is genuinely exercising the `None` branch and not, say, racing
        // ahead of it under a scheduler that behaves differently.
        assert!(
            second
                .inner
                .supervisor
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none(),
            "the first close() must have already taken the supervisor handle before the \
             second starts, or this test would silently exercise the Some branch instead \
             of the None branch it exists to pin down"
        );

        second.close().await;

        assert!(
            finished.load(Ordering::SeqCst),
            "the second, concurrent close() must not return before the supervisor task actually finished"
        );

        first_close.await.expect("first close task must not panic");
    }

    /// The limit case of the `None` branch: a `close()` on a client built
    /// with no supervisor at all has nothing to take from the supervisor
    /// mutex and no `finished` token to wait on, and must still return
    /// promptly rather than block forever.
    ///
    /// `start_paused = true` turns the timeout below into a deterministic
    /// two-valued discriminator rather than a wall-clock bound, the same
    /// reasoning as on `close_called_twice_does_not_panic_and_the_second_call_returns_promptly`
    /// above: this test reads no `SystemTime`, so tokio's virtual clock
    /// cannot mislead it.
    #[tokio::test(start_paused = true)]
    async fn close_on_a_client_built_without_a_supervisor_does_not_block() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client_with_lifecycle(
            transport,
            registry,
            RequestClientSupervisor::detached(CancellationToken::new()),
        );

        tokio::time::timeout(Duration::from_secs(2), client.close())
            .await
            .expect("close on a client built without a supervisor must not block");
    }

    /// The public supervisor constructor owns both the task and its
    /// completion signal, so concurrent closers cannot accidentally wait on
    /// an unrelated token supplied by the caller.
    #[tokio::test]
    async fn opaque_supervisor_wakes_every_concurrent_closer_on_task_completion() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let stopped = Arc::new(AtomicBool::new(false));
        let supervisor = RequestClientSupervisor::spawn(CancellationToken::new(), {
            let stopped = Arc::clone(&stopped);
            move |cancel| async move {
                cancel.cancelled().await;
                stopped.store(true, Ordering::SeqCst);
            }
        });
        let client = RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_millis(200),
            supervisor,
        );

        let other = client.clone();
        let first = tokio::spawn(async move { client.close().await });
        other.close().await;

        assert!(
            stopped.load(Ordering::SeqCst),
            "each closer must observe the supervisor task's real termination"
        );
        first.await.expect("first close task must not panic");
    }

    /// Issue #500 requires that a supervisor panic still wakes every
    /// closer. The completion guard lives on the supervisor task's own
    /// stack, so unwinding from a panic drops it exactly as a normal
    /// return would, cancelling `finished` and letting `close` return
    /// rather than hang on the now-defunct `JoinHandle`.
    ///
    /// Wrapped in a bounded timeout so a regression that reintroduces the
    /// deadlock fails the test outright instead of hanging the suite.
    #[tokio::test]
    async fn supervisor_task_panic_still_wakes_every_closer() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let supervisor =
            RequestClientSupervisor::spawn(CancellationToken::new(), |cancel| async move {
                cancel.cancelled().await;
                panic!("supervisor task panics on purpose for this test");
            });
        let client = RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_millis(200),
            supervisor,
        );

        tokio::time::timeout(Duration::from_secs(2), client.close())
            .await
            .expect("close must return even though the supervisor task panicked");
    }

    /// Issue #500 also requires that an abandoned (aborted) supervisor
    /// task wakes every closer, the same as a normal return, an error or a
    /// panic would: [`RequestClientSupervisor::abort_handle`] exists
    /// precisely so a caller can treat abandonment as a valid termination
    /// path rather than one `close` would hang on.
    ///
    /// Wrapped in a bounded timeout so a regression that reintroduces the
    /// deadlock fails the test outright instead of hanging the suite.
    #[tokio::test]
    async fn supervisor_task_abort_still_wakes_every_closer() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let supervisor = RequestClientSupervisor::spawn(CancellationToken::new(), |_cancel| {
            std::future::pending::<()>()
        });
        let abort_handle = supervisor
            .abort_handle()
            .expect("a spawned supervisor always has an abort handle");
        let client = RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_millis(200),
            supervisor,
        );

        abort_handle.abort();

        tokio::time::timeout(Duration::from_secs(2), client.close())
            .await
            .expect("close must return even though the supervisor task was aborted");
    }

    #[tokio::test]
    async fn close_called_from_the_supervisor_task_does_not_deadlock() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let returned = Arc::new(AtomicBool::new(false));
        let (client_tx, client_rx) =
            tokio::sync::oneshot::channel::<RequestClient<CapturingTransport>>();
        let supervisor = RequestClientSupervisor::spawn(CancellationToken::new(), {
            let returned = Arc::clone(&returned);
            move |_cancel| async move {
                let client = client_rx
                    .await
                    .expect("client must be handed to the supervisor task");
                client.close().await;
                returned.store(true, Ordering::SeqCst);
            }
        });
        let client = RequestClient::new(
            transport,
            registry,
            Arc::new(Mutex::new(ReplyInboxState::Ready("reply.inbox".to_owned()))),
            Duration::from_millis(200),
            supervisor,
        );

        client_tx
            .send(client.clone())
            .unwrap_or_else(|_| panic!("supervisor must still wait for its client"));
        tokio::time::timeout(Duration::from_secs(2), async {
            while !returned.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("self-close must return instead of deadlocking");

        client.close().await;
    }

    #[tokio::test]
    async fn dropping_the_last_handle_closes_the_registry_and_signals_the_consumer() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let cancel = CancellationToken::new();
        let external_cancel = cancel.clone();
        let finished_signal = CancellationToken::new();
        let stopped = Arc::new(AtomicBool::new(false));
        let supervisor = tokio::spawn({
            let cancel = cancel.clone();
            let finished_signal = finished_signal.clone();
            let stopped = Arc::clone(&stopped);
            async move {
                cancel.cancelled().await;
                stopped.store(true, Ordering::SeqCst);
                finished_signal.cancel();
            }
        });

        let client = client_with_lifecycle(
            transport,
            Arc::clone(&registry),
            RequestClientSupervisor::from_task_for_test(cancel, supervisor, finished_signal),
        );
        let _pending = registry
            .register(RequestId::new(), ReplyExpectation::new(Pong::MESSAGE_TYPE))
            .expect("registration succeeds");

        drop(client);

        assert!(
            registry.is_closed(),
            "dropping the last handle must close the registry"
        );
        assert!(
            registry.is_empty(),
            "dropping the last handle must empty the registry"
        );
        assert!(
            external_cancel.is_cancelled(),
            "dropping the last handle must cancel the token"
        );

        // Drop can only signal, never await: give the already-cancelled
        // supervisor task a chance to actually run to completion.
        tokio::task::yield_now().await;
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dropping_a_clone_while_another_handle_lives_closes_nothing() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let cancel = CancellationToken::new();
        let external_cancel = cancel.clone();
        let client = client_with_lifecycle(
            transport,
            Arc::clone(&registry),
            RequestClientSupervisor::detached(cancel),
        );
        let clone = client.clone();

        drop(clone);

        assert!(
            !registry.is_closed(),
            "a still-live handle must keep the registry open"
        );
        assert!(
            !external_cancel.is_cancelled(),
            "a still-live handle must keep the token uncancelled"
        );

        drop(client);
        assert!(
            registry.is_closed(),
            "dropping the last remaining handle must close the registry"
        );
    }
}
