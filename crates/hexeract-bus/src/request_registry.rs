//! Rendezvous point between request callers and reply deliveries.
//!
//! A caller registers a slot keyed by a freshly minted [`hexeract_core::RequestId`],
//! declaring what it accepts as a reply, and awaits its [`PendingReply`]. The
//! transport-side inbox consumer calls [`RequestRegistry::resolve`], which
//! routes a delivery to the waiting caller only if it satisfies that
//! expectation: the first **valid** reply wins, not the first delivery.
//!
//! The key is the request identity and never the correlation id: two calls
//! issued from the same causal chain share their correlation, so keying on it
//! would let concurrent replies cross.
//!
//! The identity is not an authorization boundary: it is revealed to the
//! responder on every call. Validation bounds what a forged delivery can do,
//! it does not authenticate its origin.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use hexeract_core::RequestId;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::BusEnvelope;
use crate::reply_acceptance::{ReplyExpectation, accepts};
use crate::rpc_protocol::REQUEST_ID_HEADER;

#[derive(Debug)]
struct Slot {
    expectation: ReplyExpectation,
    sender: oneshot::Sender<BusEnvelope>,
}

type Slots = HashMap<RequestId, Slot>;

/// Counts of deliveries the registry refused to route.
///
/// `duplicate` is deliberately absent: once a slot is consumed by a valid
/// reply it is removed, so a second valid reply is indistinguishable from an
/// orphan at this level and is counted as `orphaned`. Telling the two apart
/// requires retaining resolved identities for a short window, which belongs
/// to the observability work (#441).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplyCountersSnapshot {
    /// Deliveries whose identity was known but which `accepts` refused.
    pub invalid: u64,
    /// Deliveries with an absent, unparsable or unknown identity.
    pub orphaned: u64,
}

#[derive(Debug, Default)]
struct ReplyCounters {
    invalid: AtomicU64,
    orphaned: AtomicU64,
}

/// Registry of in-flight request slots.
#[derive(Debug, Default)]
pub struct RequestRegistry {
    slots: Mutex<Slots>,
    counters: ReplyCounters,
}

impl RequestRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            counters: ReplyCounters::default(),
        }
    }

    fn slots(&self) -> MutexGuard<'_, Slots> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Register a fresh slot accepting `expectation`, and return its
    /// RAII-guarded pending reply.
    pub fn register(self: &Arc<Self>, expectation: ReplyExpectation) -> PendingReply {
        let request_id = RequestId::new();
        let (sender, receiver) = oneshot::channel();
        self.slots().insert(
            request_id,
            Slot {
                expectation,
                sender,
            },
        );
        PendingReply {
            request_id,
            receiver,
            registry: Arc::clone(self),
        }
    }

    /// Route `envelope` to its waiting caller, by request identity, if and
    /// only if it is an acceptable reply for that caller.
    ///
    /// A delivery that fails validation leaves the slot **intact**: the
    /// legitimate reply can still arrive and win. This is what makes the
    /// first valid reply win rather than the first delivery.
    pub fn resolve(&self, envelope: BusEnvelope) {
        let Some(raw) = envelope.headers.get(REQUEST_ID_HEADER) else {
            self.counters.orphaned.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("reply without a request id header, dropping");
            return;
        };
        let Ok(uuid) = raw.parse::<Uuid>() else {
            self.counters.orphaned.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("reply with an unparsable request id header, dropping");
            return;
        };
        let request_id = RequestId::from(uuid);

        let mut slots = self.slots();
        let Some(slot) = slots.get(&request_id) else {
            drop(slots);
            self.counters.orphaned.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(%request_id, "reply for an unknown or already-resolved request");
            return;
        };

        if let Err(rejection) = accepts(&slot.expectation, &envelope) {
            drop(slots);
            self.counters.invalid.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(%request_id, ?rejection, "invalid reply, slot left pending");
            return;
        }

        let slot = slots.remove(&request_id);
        drop(slots);
        if let Some(slot) = slot {
            let _ = slot.sender.send(envelope);
        }
    }

    /// Snapshot of the refused-delivery counters.
    #[must_use]
    pub fn counters(&self) -> ReplyCountersSnapshot {
        ReplyCountersSnapshot {
            invalid: self.counters.invalid.load(Ordering::Relaxed),
            orphaned: self.counters.orphaned.load(Ordering::Relaxed),
        }
    }

    /// Drop every in-flight slot: each waiting caller observes a closed
    /// channel. Used on connection loss to fail in-flight requests fast.
    pub fn drain(&self) {
        self.slots().clear();
    }

    /// Number of in-flight slots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots().len()
    }

    /// Whether no slot is in flight.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn remove(&self, request_id: RequestId) {
        self.slots().remove(&request_id);
    }
}

/// RAII guard for one in-flight request slot.
///
/// Dropping a `PendingReply` removes its slot from the registry, whatever the
/// exit path (success, timeout, cancellation, panic): no slot ever leaks.
#[derive(Debug)]
pub struct PendingReply {
    request_id: RequestId,
    receiver: oneshot::Receiver<BusEnvelope>,
    registry: Arc<RequestRegistry>,
}

impl PendingReply {
    /// The request identity this slot waits on.
    #[must_use]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Await the reply envelope. Borrows `self` so the RAII guard stays live
    /// and cleans the slot when the caller drops the [`PendingReply`].
    ///
    /// Call at most once to completion: the underlying channel is consumed
    /// on the first resolved call (`Ok` or `Err`), and awaiting it again
    /// panics.
    ///
    /// # Errors
    ///
    /// Returns [`tokio::sync::oneshot::error::RecvError`] if the reply channel
    /// was closed before a reply arrived, for example after `drain()` on
    /// connection loss.
    pub async fn wait(&mut self) -> Result<BusEnvelope, oneshot::error::RecvError> {
        (&mut self.receiver).await
    }
}

impl Drop for PendingReply {
    fn drop(&mut self) {
        self.registry.remove(self.request_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::*;
    use crate::rpc_protocol::REQUEST_ID_HEADER;
    use crate::{BusEnvelope, Message};

    #[derive(Debug, Serialize, Deserialize)]
    struct Pong {
        seq: u64,
    }
    impl Message for Pong {
        const MESSAGE_TYPE: &'static str = "test.pong";
    }

    fn reply_for(request_id: RequestId, correlation_id: Uuid, seq: u64) -> BusEnvelope {
        let mut envelope =
            BusEnvelope::new(correlation_id, &Pong { seq }).expect("pong must serialize");
        envelope
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
        envelope.headers.insert(
            crate::rpc_protocol::PROTOCOL_VERSION_HEADER.to_owned(),
            crate::rpc_protocol::PROTOCOL_VERSION.to_string(),
        );
        envelope.headers.insert(
            crate::rpc_protocol::REPLY_STATUS_HEADER.to_owned(),
            crate::rpc_protocol::REPLY_STATUS_OK.to_owned(),
        );
        envelope
    }

    fn pong_expectation() -> ReplyExpectation {
        ReplyExpectation {
            reply_message_type: Pong::MESSAGE_TYPE,
        }
    }

    fn ok_reply(message_type: &str) -> BusEnvelope {
        let mut headers = std::collections::HashMap::new();
        headers.insert(
            crate::rpc_protocol::PROTOCOL_VERSION_HEADER.to_owned(),
            crate::rpc_protocol::PROTOCOL_VERSION.to_string(),
        );
        headers.insert(
            crate::rpc_protocol::REPLY_STATUS_HEADER.to_owned(),
            crate::rpc_protocol::REPLY_STATUS_OK.to_owned(),
        );
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

    fn tagged(mut envelope: BusEnvelope, request_id: RequestId) -> BusEnvelope {
        envelope
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), request_id.to_string());
        envelope
    }

    const EXPECTED_REPLY: &str = "test.reply";

    fn expectation() -> ReplyExpectation {
        ReplyExpectation {
            reply_message_type: EXPECTED_REPLY,
        }
    }

    #[tokio::test]
    async fn concurrent_calls_in_one_causal_chain_do_not_cross_replies() {
        let registry = Arc::new(RequestRegistry::new());
        let shared_chain = Uuid::now_v7();

        let mut first = registry.register(pong_expectation());
        let mut second = registry.register(pong_expectation());
        assert_ne!(first.request_id(), second.request_id());

        registry.resolve(reply_for(second.request_id(), shared_chain, 2));
        registry.resolve(reply_for(first.request_id(), shared_chain, 1));

        let first_reply: Pong = first
            .wait()
            .await
            .expect("first reply")
            .decode()
            .expect("decode");
        let second_reply: Pong = second
            .wait()
            .await
            .expect("second reply")
            .decode()
            .expect("decode");
        assert_eq!(first_reply.seq, 1);
        assert_eq!(second_reply.seq, 2);
    }

    #[test]
    fn dropping_a_pending_reply_frees_its_slot() {
        let registry = Arc::new(RequestRegistry::new());
        let pending = registry.register(expectation());
        assert_eq!(registry.len(), 1);
        drop(pending);
        assert!(registry.is_empty());
    }

    #[test]
    fn a_reply_without_a_request_id_header_is_dropped() {
        let registry = Arc::new(RequestRegistry::new());
        let _pending = registry.register(expectation());
        let envelope =
            BusEnvelope::new(Uuid::now_v7(), &Pong { seq: 7 }).expect("pong must serialize");
        registry.resolve(envelope);
        assert_eq!(registry.len(), 1, "the slot must stay in flight");
    }

    #[test]
    fn an_unparsable_request_id_is_dropped() {
        let registry = Arc::new(RequestRegistry::new());
        let _pending = registry.register(expectation());
        let mut envelope =
            BusEnvelope::new(Uuid::now_v7(), &Pong { seq: 7 }).expect("pong must serialize");
        envelope
            .headers
            .insert(REQUEST_ID_HEADER.to_owned(), "not-a-uuid".to_owned());
        registry.resolve(envelope);
        assert_eq!(registry.len(), 1, "the slot must stay in flight");
    }

    #[tokio::test]
    async fn the_first_valid_reply_wins_and_duplicates_are_dropped() {
        let registry = Arc::new(RequestRegistry::new());
        let mut pending = registry.register(pong_expectation());
        let request_id = pending.request_id();
        let chain = Uuid::now_v7();

        registry.resolve(reply_for(request_id, chain, 1));
        registry.resolve(reply_for(request_id, chain, 2));

        let reply: Pong = pending
            .wait()
            .await
            .expect("reply")
            .decode()
            .expect("decode");
        assert_eq!(reply.seq, 1);
    }

    #[tokio::test]
    async fn drain_closes_every_in_flight_slot() {
        let registry = Arc::new(RequestRegistry::new());
        let mut pending = registry.register(expectation());
        registry.drain();
        assert!(pending.wait().await.is_err());
    }

    /// Resolves replies in an order decorrelated from slot-creation order: the
    /// resolver visits index `(i * 7) % SLOT_COUNT` on its `i`-th step. Since 7
    /// and `SLOT_COUNT` (64) are coprime, this sequence still visits every one
    /// of the 64 slots exactly once, but never in creation order. Waiting
    /// happens in plain creation order, so this decorrelation is what makes
    /// the test probant: a routing bug that delivers the `n`-th resolved reply
    /// to the `n`-th waiting caller (FIFO), or the last resolved reply to the
    /// first waiting caller (LIFO), would both fail here, whereas a test that
    /// merely reversed the resolution order would only catch the LIFO case.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_concurrent_calls_each_receive_their_own_reply() {
        const SLOT_COUNT: usize = 64;

        let registry = Arc::new(RequestRegistry::new());
        let chain = Uuid::now_v7();
        let mut pendings: Vec<_> = (0..SLOT_COUNT)
            .map(|_| registry.register(pong_expectation()))
            .collect();
        let ids: Vec<RequestId> = pendings.iter().map(PendingReply::request_id).collect();

        let resolver = Arc::clone(&registry);
        let resolver_ids = ids.clone();
        let task = tokio::spawn(async move {
            for i in 0..SLOT_COUNT {
                let resolve_index = (i * 7) % SLOT_COUNT;
                let seq = u64::try_from(resolve_index).expect("seq fits in u64");
                resolver.resolve(reply_for(resolver_ids[resolve_index], chain, seq));
            }
        });

        for (index, pending) in pendings.iter_mut().enumerate() {
            let expected_seq = u64::try_from(index).expect("seq fits in u64");
            let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), pending.wait())
                .await
                .unwrap_or_else(|_| panic!("caller at index {index} never received a reply"))
                .expect("each caller gets a reply");
            let reply: Pong = envelope.decode().expect("decode");
            assert_eq!(reply.seq, expected_seq, "reply routed to the wrong caller");
        }
        task.await.expect("resolver task must finish");
    }

    #[tokio::test]
    async fn an_invalid_reply_arriving_first_does_not_consume_the_slot() {
        let registry = Arc::new(RequestRegistry::new());
        let mut pending = registry.register(expectation());
        let request_id = pending.request_id();

        registry.resolve(tagged(ok_reply("attacker.reply"), request_id));

        assert_eq!(registry.len(), 1, "the slot must survive an invalid reply");
        assert_eq!(registry.counters().invalid, 1);

        registry.resolve(tagged(ok_reply(EXPECTED_REPLY), request_id));

        let reply = pending.wait().await.expect("the legitimate reply must win");
        assert_eq!(reply.message_type, EXPECTED_REPLY);
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn a_reply_with_an_unknown_identity_is_counted_orphaned() {
        let registry = Arc::new(RequestRegistry::new());
        let _pending = registry.register(expectation());

        registry.resolve(tagged(ok_reply(EXPECTED_REPLY), RequestId::new()));

        assert_eq!(registry.len(), 1);
        assert_eq!(registry.counters().orphaned, 1);
        assert_eq!(registry.counters().invalid, 0);
    }

    #[tokio::test]
    async fn a_reply_without_an_identity_header_is_counted_orphaned() {
        let registry = Arc::new(RequestRegistry::new());
        let _pending = registry.register(expectation());

        registry.resolve(ok_reply(EXPECTED_REPLY));

        assert_eq!(registry.counters().orphaned, 1);
    }
}
