use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use hexeract_core::CorrelationId;
use tokio::sync::oneshot;

use crate::BusEnvelope;

type Slots = HashMap<CorrelationId, oneshot::Sender<BusEnvelope>>;

/// Rendezvous point between request callers and reply deliveries.
///
/// A caller registers a slot keyed by a freshly minted [`CorrelationId`] and
/// awaits its [`PendingReply`]. The transport-side inbox consumer calls
/// [`CorrelationRegistry::resolve`] to route an incoming reply envelope to the
/// waiting caller. Unknown or already-resolved correlation ids are dropped
/// with a warning, never an error; the first reply for a slot wins.
#[derive(Debug, Default)]
pub struct CorrelationRegistry {
    slots: Mutex<Slots>,
}

impl CorrelationRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }

    fn slots(&self) -> MutexGuard<'_, Slots> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Register a fresh slot and return its RAII-guarded pending reply.
    pub fn register(self: &Arc<Self>) -> PendingReply {
        let correlation_id = CorrelationId::new();
        let (sender, receiver) = oneshot::channel();
        self.slots().insert(correlation_id, sender);
        PendingReply {
            correlation_id,
            receiver,
            registry: Arc::clone(self),
        }
    }

    /// Route `envelope` to its waiting caller by `correlation_id`.
    pub fn resolve(&self, envelope: BusEnvelope) {
        let correlation_id = CorrelationId::from(envelope.correlation_id);
        let sender = self.slots().remove(&correlation_id);
        if let Some(sender) = sender {
            let _ = sender.send(envelope);
        } else {
            tracing::warn!(
                %correlation_id,
                "reply for unknown or already-resolved correlation id, dropping"
            );
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

    fn remove(&self, correlation_id: CorrelationId) {
        self.slots().remove(&correlation_id);
    }
}

/// RAII guard for one in-flight request slot.
///
/// Dropping a `PendingReply` removes its slot from the registry, whatever the
/// exit path (success, timeout, cancellation, panic): no slot ever leaks.
#[derive(Debug)]
pub struct PendingReply {
    correlation_id: CorrelationId,
    receiver: oneshot::Receiver<BusEnvelope>,
    registry: Arc<CorrelationRegistry>,
}

impl PendingReply {
    /// The correlation id this slot waits on.
    #[must_use]
    pub fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }

    /// Await the reply envelope. Borrows `self` so the RAII guard stays live
    /// and cleans the slot when the caller drops the [`PendingReply`].
    ///
    /// # Errors
    ///
    /// Returns [`tokio::sync::oneshot::error::RecvError`] if the reply channel
    /// was closed before a reply arrived (for example after `drain()` on
    /// connection loss).
    pub async fn wait(&mut self) -> Result<BusEnvelope, oneshot::error::RecvError> {
        (&mut self.receiver).await
    }
}

impl Drop for PendingReply {
    fn drop(&mut self) {
        self.registry.remove(self.correlation_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    use crate::Message;

    #[derive(Debug, Serialize, Deserialize)]
    struct Pong {
        seq: u64,
    }
    impl Message for Pong {
        const MESSAGE_TYPE: &'static str = "tests.pong";
    }

    fn reply_for(correlation_id: CorrelationId, seq: u64) -> BusEnvelope {
        BusEnvelope::new(*correlation_id.as_uuid(), &Pong { seq }).unwrap()
    }

    #[tokio::test]
    async fn register_then_resolve_delivers_envelope() {
        let registry = Arc::new(CorrelationRegistry::new());
        let mut pending = registry.register();
        let cid = pending.correlation_id();
        assert_eq!(registry.len(), 1);
        registry.resolve(reply_for(cid, 7));
        let env = pending.wait().await.expect("must receive");
        let pong: Pong = env.decode().unwrap();
        assert_eq!(pong.seq, 7);
    }

    #[tokio::test]
    async fn resolve_unknown_correlation_is_dropped() {
        let registry = Arc::new(CorrelationRegistry::new());
        registry.resolve(reply_for(CorrelationId::new(), 1));
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn second_reply_for_same_slot_is_dropped() {
        let registry = Arc::new(CorrelationRegistry::new());
        let mut pending = registry.register();
        let cid = pending.correlation_id();
        registry.resolve(reply_for(cid, 1));
        registry.resolve(reply_for(cid, 2));
        let env = pending.wait().await.unwrap();
        let pong: Pong = env.decode().unwrap();
        assert_eq!(pong.seq, 1);
    }

    #[tokio::test]
    async fn dropping_pending_removes_slot() {
        let registry = Arc::new(CorrelationRegistry::new());
        {
            let _pending = registry.register();
            assert_eq!(registry.len(), 1);
        }
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn timeout_path_leaves_no_slot() {
        let registry = Arc::new(CorrelationRegistry::new());
        {
            let mut pending = registry.register();
            let timed_out =
                tokio::time::timeout(std::time::Duration::from_millis(10), pending.wait()).await;
            assert!(timed_out.is_err());
        }
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn drain_closes_all_in_flight_channels() {
        let registry = Arc::new(CorrelationRegistry::new());
        let mut a = registry.register();
        let mut b = registry.register();
        assert_eq!(registry.len(), 2);
        registry.drain();
        assert_eq!(registry.len(), 0);
        assert!(a.wait().await.is_err());
        assert!(b.wait().await.is_err());
    }

    #[tokio::test]
    async fn thousand_concurrent_slots_never_cross() {
        let registry = Arc::new(CorrelationRegistry::new());
        let mut pendings = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..1000 {
            let pending = registry.register();
            ids.push(pending.correlation_id());
            pendings.push(pending);
        }
        // resolve each with its own sequence number
        for (seq, cid) in ids.iter().enumerate() {
            registry.resolve(reply_for(*cid, seq as u64));
        }
        for (seq, mut pending) in pendings.into_iter().enumerate() {
            let env = pending.wait().await.expect("each slot resolves");
            let pong: Pong = env.decode().unwrap();
            assert_eq!(pong.seq, seq as u64);
        }
        assert_eq!(registry.len(), 0);
    }
}
