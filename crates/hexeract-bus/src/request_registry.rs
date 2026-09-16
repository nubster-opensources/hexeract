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
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use hexeract_core::RequestId;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::BusEnvelope;
use crate::reply_acceptance::{self, ReplyExpectation, ReplyRejection};
use crate::reply_authentication::ReplyAuthentication;
use crate::reply_rejection_kind::ReplyRejectionKind;
use crate::rpc_protocol::REQUEST_ID_HEADER;
use crate::slot_retirement::{RetiredSlots, SlotRetirement};

#[derive(Debug)]
struct Slot {
    expectation: ReplyExpectation,
    sender: oneshot::Sender<(BusEnvelope, ReplyAuthentication)>,
    last_rejection: Option<ReplyRejection>,
    rejected_deliveries: u32,
}

type Slots = HashMap<RequestId, Slot>;

/// Parse the request identity header, if present and well-formed.
///
/// Returns `None` uniformly for an absent header and a header that fails to
/// parse as a UUID: both leave a delivery impossible to attribute to any
/// slot, so every caller of this function treats them identically.
fn readable_request_id(envelope: &BusEnvelope) -> Option<RequestId> {
    let raw = envelope.header(REQUEST_ID_HEADER)?;
    raw.parse::<Uuid>().ok().map(RequestId::from)
}

/// Slot map, retired-identity memory and closed flag, guarded by the same
/// lock.
///
/// The three live under one [`Mutex`] on purpose: `close` must flip
/// `closed` and clear `slots` atomically with respect to `register`, or a
/// `register` racing a `close` could read `closed` as still `false`,
/// insert its slot, and never be undone by `close`'s own clear. Keeping
/// them apart as an atomic bool checked before a separately locked map
/// would reopen exactly that window. `retired` shares the lock for the same
/// reason: recording a retirement must be atomic with the slot removal that
/// caused it.
#[derive(Debug)]
struct RegistryState {
    slots: Slots,
    retired: RetiredSlots,
    closed: bool,
}

impl RegistryState {
    fn new(retired_capacity: usize) -> Self {
        Self {
            slots: Slots::default(),
            retired: RetiredSlots::new(retired_capacity),
            closed: false,
        }
    }
}

/// Counts of deliveries the registry refused to route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplyCountersSnapshot {
    /// No envelope could be reconstructed from the delivery.
    pub undecodable: u64,
    /// Deliveries with an absent, unparsable or never-seen identity.
    pub orphaned: u64,
    /// Deliveries the transport refused before a verdict on their content.
    pub unauthenticated: u64,
    /// Deliveries whose identity was known but which `accepts` refused.
    pub invalid: u64,
    /// Deliveries bearing an identity already resolved by a valid reply.
    pub duplicate: u64,
    /// Deliveries bearing an identity abandoned before they arrived.
    pub late: u64,
}

#[derive(Debug, Default)]
struct ReplyCounters {
    undecodable: AtomicU64,
    orphaned: AtomicU64,
    unauthenticated: AtomicU64,
    invalid: AtomicU64,
    duplicate: AtomicU64,
    late: AtomicU64,
}

/// Why a call could not be registered.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterRejection {
    /// The registry reached its `max_in_flight` bound.
    AtCapacity,
    /// The registry is closed and accepts no further calls.
    Closed,
    /// The slot for this identity is already occupied.
    SlotOccupied,
}

/// Default bound on in-flight requests when a caller does not choose one.
///
/// One thousand and twenty-four concurrent calls comfortably absorbs a
/// realistic burst from a single client without ever touching the bound,
/// while staying small enough that actually reaching it is a signal worth
/// surfacing: a leaked [`PendingReply`], a stuck responder, or a client
/// issuing far more concurrent calls than intended. A caller with a
/// genuinely different workload can choose its own bound through
/// [`RequestRegistry::new`]; this constant only names the default.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 1024;

/// Registry of in-flight request slots.
#[derive(Debug)]
pub struct RequestRegistry {
    state: Mutex<RegistryState>,
    counters: ReplyCounters,
    max_in_flight: usize,
}

impl Default for RequestRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_IN_FLIGHT)
    }
}

impl RequestRegistry {
    /// Create an empty registry that admits at most `max_in_flight`
    /// concurrent slots.
    ///
    /// A `max_in_flight` of zero admits none, ever: every registration is
    /// refused as [`RegisterRejection::AtCapacity`], which makes it a way
    /// to shut request-reply off without stopping the process. It is
    /// accepted rather than asserted against, so a caller probing that
    /// bound observes the documented refusal in every build profile
    /// instead of a panic in one and a refusal in the other.
    #[must_use]
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            state: Mutex::new(RegistryState::new(max_in_flight)),
            counters: ReplyCounters::default(),
            max_in_flight,
        }
    }

    fn state(&self) -> MutexGuard<'_, RegistryState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Register a fresh slot for `request_id`, accepting `expectation`, and
    /// return its RAII-guarded pending reply.
    ///
    /// The caller mints `request_id` itself rather than the registry
    /// generating it: a caller needs the identity in hand before it stamps
    /// and publishes the envelope, and must never receive it back only
    /// after the fact. As a side benefit, this is also what lets a test
    /// force two registrations to collide on the same identity and observe
    /// [`RegisterRejection::SlotOccupied`] deterministically, something an
    /// internally generated identity could never be made to do on purpose.
    ///
    /// Insertion always goes through a vacant [`Entry`], never a raw
    /// `insert`: an already-occupied identity is refused rather than
    /// silently overwriting the first caller's slot.
    ///
    /// # Errors
    ///
    /// - [`RegisterRejection::AtCapacity`] if the registry already holds
    ///   `max_in_flight` slots. The registry never waits for a slot to free
    ///   up: back-pressure must be visible to the caller immediately, not
    ///   disguised as extra latency.
    /// - [`RegisterRejection::SlotOccupied`] if `request_id` already names
    ///   an in-flight slot. The existing slot is left untouched: its caller
    ///   still receives its reply.
    /// - [`RegisterRejection::Closed`] if the registry has been closed.
    pub fn register(
        &self,
        request_id: RequestId,
        expectation: ReplyExpectation,
    ) -> Result<PendingReply<'_>, RegisterRejection> {
        let mut state = self.state();
        if state.closed {
            return Err(RegisterRejection::Closed);
        }
        let in_flight = state.slots.len();
        match state.slots.entry(request_id) {
            Entry::Occupied(_) => Err(RegisterRejection::SlotOccupied),
            Entry::Vacant(vacant) => {
                if in_flight >= self.max_in_flight {
                    return Err(RegisterRejection::AtCapacity);
                }
                let (sender, receiver) = oneshot::channel();
                vacant.insert(Slot {
                    expectation,
                    sender,
                    last_rejection: None,
                    rejected_deliveries: 0,
                });
                Ok(PendingReply {
                    request_id,
                    receiver,
                    registry: self,
                })
            }
        }
    }

    /// Route `envelope` to its waiting caller, by request identity, if and
    /// only if it is an acceptable reply for that caller, alongside
    /// `authentication`, whatever this transport was able to establish about
    /// the identity that signed it.
    ///
    /// A delivery that fails validation leaves the slot **intact**: the
    /// legitimate reply can still arrive and win. This is what makes the
    /// first valid reply win rather than the first delivery. `authentication`
    /// is abandoned along with a rejected delivery: only the reply that
    /// eventually wins hands its own authentication to the waiting caller.
    ///
    /// This registry never verifies anything itself; it only carries what its
    /// caller declares. The caller states what it established, and the
    /// registry never second-guesses it: a transport that ran no verification
    /// at all must write [`ReplyAuthentication::NotEnforced`] itself, since
    /// writing that variant is itself a claim, and a transport that never
    /// asked the question is the one making it.
    ///
    /// A transport that refused the delivery before reaching a verdict on its
    /// content passes `Err`. The transport's refusal always wins over any
    /// other categorization: the retired-identity memory is never consulted
    /// on this branch, and the slot, if it can be found, is always left
    /// pending. This is what keeps a flood of forged signatures against
    /// random identities counted as `unauthenticated`, never diluted across
    /// `orphaned`, `duplicate` and `late`.
    pub fn resolve(
        &self,
        envelope: BusEnvelope,
        authentication: Result<ReplyAuthentication, ReplyRejection>,
    ) {
        let authentication = match authentication {
            Ok(authentication) => authentication,
            Err(rejection) => {
                self.record_rejection_kind(rejection.kind());
                if let Some(request_id) = readable_request_id(&envelope) {
                    let mut state = self.state();
                    if let Some(slot) = state.slots.get_mut(&request_id) {
                        slot.last_rejection = Some(rejection);
                        slot.rejected_deliveries = slot.rejected_deliveries.saturating_add(1);
                    }
                    drop(state);
                    tracing::debug!(
                        %request_id,
                        ?rejection,
                        "delivery refused by the transport, slot left pending"
                    );
                } else {
                    tracing::debug!(
                        ?rejection,
                        "delivery refused by the transport, identity unreadable"
                    );
                }
                return;
            }
        };

        let Some(request_id) = readable_request_id(&envelope) else {
            self.counters.orphaned.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("reply with no usable request id header, dropping");
            return;
        };

        let mut state = self.state();
        let Some(slot) = state.slots.get_mut(&request_id) else {
            let retirement = state.retired.lookup(request_id);
            drop(state);
            match retirement {
                Some(SlotRetirement::Resolved) => {
                    self.counters.duplicate.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(%request_id, "reply for an already-resolved request");
                }
                Some(SlotRetirement::Abandoned) => {
                    self.counters.late.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(%request_id, "reply for an abandoned request, arrived late");
                }
                None => {
                    self.counters.orphaned.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(%request_id, "reply for an unknown request");
                }
            }
            return;
        };

        if let Err(rejection) = reply_acceptance::accepts(&slot.expectation, &envelope) {
            slot.last_rejection = Some(rejection);
            slot.rejected_deliveries = slot.rejected_deliveries.saturating_add(1);
            drop(state);
            self.record_rejection_kind(rejection.kind());
            tracing::debug!(%request_id, ?rejection, "invalid reply, slot left pending");
            return;
        }

        let slot = state.slots.remove(&request_id);
        if slot.is_some() {
            state.retired.record(request_id, SlotRetirement::Resolved);
        }
        drop(state);
        if let Some(slot) = slot {
            let _ = slot.sender.send((envelope, authentication));
        }
    }

    /// Count a delivery that produced no envelope at all.
    ///
    /// The only bare counter mutator on this surface, and the only case
    /// where there is no data to carry: without an envelope there is no
    /// identity, so nothing can be attributed to a slot.
    pub fn record_undecodable(&self) {
        self.counters.undecodable.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the counter named by `kind`.
    fn record_rejection_kind(&self, kind: ReplyRejectionKind) {
        let counter = match kind {
            ReplyRejectionKind::Undecodable => &self.counters.undecodable,
            ReplyRejectionKind::Orphaned => &self.counters.orphaned,
            ReplyRejectionKind::Unauthenticated => &self.counters.unauthenticated,
            ReplyRejectionKind::Invalid => &self.counters.invalid,
            ReplyRejectionKind::Duplicate => &self.counters.duplicate,
            ReplyRejectionKind::Late => &self.counters.late,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot of the refused-delivery counters.
    #[must_use]
    pub fn counters(&self) -> ReplyCountersSnapshot {
        ReplyCountersSnapshot {
            undecodable: self.counters.undecodable.load(Ordering::Relaxed),
            orphaned: self.counters.orphaned.load(Ordering::Relaxed),
            unauthenticated: self.counters.unauthenticated.load(Ordering::Relaxed),
            invalid: self.counters.invalid.load(Ordering::Relaxed),
            duplicate: self.counters.duplicate.load(Ordering::Relaxed),
            late: self.counters.late.load(Ordering::Relaxed),
        }
    }

    /// Drop every in-flight slot: each waiting caller observes a closed
    /// channel. Used on connection loss to fail in-flight requests fast.
    ///
    /// Unlike [`Self::close`], this leaves the registry open: a caller may
    /// still register after a `drain`, which is exactly what a reconnected
    /// transport needs.
    ///
    /// Every slot cleared here is recorded `SlotRetirement::Abandoned`:
    /// a reply bearing one of these identities that arrives after the drain
    /// is late, not orphaned.
    ///
    /// A transport supervisor that owns a [`crate::ReplyInboxState`] must
    /// mark it [`crate::ReplyInboxState::Reconnecting`] before calling
    /// this: see [`crate::ReplyInboxState`] for the guarantee that order
    /// gives a waiting caller.
    pub fn drain(&self) {
        let mut state = self.state();
        let drained: Vec<RequestId> = state.slots.keys().copied().collect();
        state.slots.clear();
        for request_id in drained {
            state.retired.record(request_id, SlotRetirement::Abandoned);
        }
    }

    /// Fail every pending call and refuse further registrations.
    ///
    /// Sets the closed flag and clears the slot map under the same lock,
    /// so this is atomic with respect to [`Self::register`]: whichever of
    /// the two runs first, the outcome is either a slot that survives
    /// (registered strictly before `close`) or none at all, never an
    /// orphan inserted after the map was cleared. Every waiting caller
    /// observes a closed channel, the same signal [`Self::drain`] produces,
    /// but here no further registration ever succeeds again.
    ///
    /// Every slot cleared here is recorded `SlotRetirement::Abandoned`,
    /// for the same reason [`Self::drain`] records it.
    pub fn close(&self) {
        let mut state = self.state();
        state.closed = true;
        let closed: Vec<RequestId> = state.slots.keys().copied().collect();
        state.slots.clear();
        for request_id in closed {
            state.retired.record(request_id, SlotRetirement::Abandoned);
        }
    }

    /// Whether the registry has been closed and refuses further
    /// registrations.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.state().closed
    }

    /// Number of in-flight slots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state().slots.len()
    }

    /// Whether no slot is in flight.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove `request_id`, recording why it left.
    ///
    /// Records `SlotRetirement::Abandoned` only when the slot was still
    /// present. A slot that [`Self::resolve`] already removed has been
    /// recorded `SlotRetirement::Resolved`, and overwriting that would
    /// make every duplicate count as `late`.
    fn remove(&self, request_id: RequestId) {
        let mut state = self.state();
        if state.slots.remove(&request_id).is_some() {
            state.retired.record(request_id, SlotRetirement::Abandoned);
        }
    }

    /// Identities of every slot currently in flight.
    ///
    /// Test-only: lets a test at the `RequestClient` level find the request
    /// id its own call registered, without widening this crate's public
    /// surface.
    #[cfg(test)]
    pub(crate) fn in_flight_ids(&self) -> Vec<RequestId> {
        self.state().slots.keys().copied().collect()
    }
}

/// RAII guard for one in-flight request slot.
///
/// Dropping a `PendingReply` removes its slot from the registry, whatever the
/// exit path (success, timeout, cancellation, panic): no slot ever leaks.
#[derive(Debug)]
pub struct PendingReply<'a> {
    request_id: RequestId,
    receiver: oneshot::Receiver<(BusEnvelope, ReplyAuthentication)>,
    registry: &'a RequestRegistry,
}

impl PendingReply<'_> {
    /// The request identity this slot waits on.
    #[must_use]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// The last reason a delivery bearing this identity was refused.
    ///
    /// Read it before the guard is dropped: dropping removes the slot.
    /// Renders the neutral `None` once the slot itself has disappeared,
    /// exactly as if nothing had ever been refused.
    #[must_use]
    pub fn last_rejection(&self) -> Option<ReplyRejection> {
        self.registry
            .state()
            .slots
            .get(&self.request_id)
            .and_then(|slot| slot.last_rejection)
    }

    /// How many deliveries bearing this identity were refused.
    ///
    /// One reason alone does not separate a misconfigured responder, which
    /// produces a handful, from a flood, which produces thousands. This
    /// count is what replaces the per-delivery warning that used to carry
    /// that signal. Renders the neutral `0` once the slot itself has
    /// disappeared.
    #[must_use]
    pub fn rejected_deliveries(&self) -> u32 {
        self.registry
            .state()
            .slots
            .get(&self.request_id)
            .map_or(0, |slot| slot.rejected_deliveries)
    }

    /// Await the reply envelope, alongside what this transport established
    /// about the identity that signed it. Borrows `self` so the RAII guard
    /// stays live and cleans the slot when the caller drops the
    /// [`PendingReply`].
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
    pub async fn wait(
        &mut self,
    ) -> Result<(BusEnvelope, ReplyAuthentication), oneshot::error::RecvError> {
        (&mut self.receiver).await
    }
}

impl Drop for PendingReply<'_> {
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
    use crate::envelope_security::identity::{Audience, Issuer, KeyId, SignatureAlgorithm};
    use crate::envelope_security::principal::VerifiedPrincipal;
    use crate::reply_authentication::ReplyAuthentication;
    use crate::rpc_protocol::REQUEST_ID_HEADER;
    use crate::{BusEnvelope, Message};

    /// A verified principal built the same way every test in this module
    /// needs one: an issuer, an audience and a key that a real verification
    /// would have established. The exact values never matter here, only
    /// that the same principal handed to `resolve` is the one observed
    /// through `wait()`.
    fn principal() -> VerifiedPrincipal {
        VerifiedPrincipal::new(
            Issuer::new("billing-service").expect("valid issuer"),
            Audience::new("ledger-service").expect("valid audience"),
            KeyId::new("2026-09").expect("valid key id"),
            SignatureAlgorithm::Ed25519,
        )
    }

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
        envelope.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        envelope.insert_protocol_header(
            crate::rpc_protocol::PROTOCOL_VERSION_HEADER,
            crate::rpc_protocol::PROTOCOL_VERSION.to_string(),
        );
        envelope.insert_protocol_header(
            crate::rpc_protocol::REPLY_STATUS_HEADER,
            crate::rpc_protocol::REPLY_STATUS_OK.to_owned(),
        );
        envelope
    }

    fn pong_expectation() -> ReplyExpectation {
        ReplyExpectation::new(Pong::MESSAGE_TYPE)
    }

    fn ok_reply(message_type: &str) -> BusEnvelope {
        let mut envelope = BusEnvelope::restore(
            Uuid::now_v7(),
            message_type.to_owned(),
            Vec::new(),
            Uuid::now_v7(),
            None,
            HashMap::default(),
            std::time::SystemTime::now(),
        );
        envelope.insert_protocol_header(
            crate::rpc_protocol::PROTOCOL_VERSION_HEADER,
            crate::rpc_protocol::PROTOCOL_VERSION.to_string(),
        );
        envelope.insert_protocol_header(
            crate::rpc_protocol::REPLY_STATUS_HEADER,
            crate::rpc_protocol::REPLY_STATUS_OK.to_owned(),
        );
        envelope
    }

    fn tagged(mut envelope: BusEnvelope, request_id: RequestId) -> BusEnvelope {
        envelope.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        envelope
    }

    const EXPECTED_REPLY: &str = "test.reply";

    fn expectation() -> ReplyExpectation {
        ReplyExpectation::new(EXPECTED_REPLY)
    }

    #[tokio::test]
    async fn concurrent_calls_in_one_causal_chain_do_not_cross_replies() {
        let registry = Arc::new(RequestRegistry::default());
        let shared_chain = Uuid::now_v7();

        let mut first = registry
            .register(RequestId::new(), pong_expectation())
            .expect("first registration succeeds");
        let mut second = registry
            .register(RequestId::new(), pong_expectation())
            .expect("second registration succeeds");
        assert_ne!(first.request_id(), second.request_id());

        registry.resolve(
            reply_for(second.request_id(), shared_chain, 2),
            Ok(ReplyAuthentication::NotEnforced),
        );
        registry.resolve(
            reply_for(first.request_id(), shared_chain, 1),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (first_envelope, _first_authentication) = first.wait().await.expect("first reply");
        let (second_envelope, _second_authentication) = second.wait().await.expect("second reply");
        let first_reply: Pong = first_envelope.decode().expect("decode");
        let second_reply: Pong = second_envelope.decode().expect("decode");
        assert_eq!(first_reply.seq, 1);
        assert_eq!(second_reply.seq, 2);
    }

    #[test]
    fn dropping_a_pending_reply_frees_its_slot() {
        let registry = Arc::new(RequestRegistry::default());
        let pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        assert_eq!(registry.len(), 1);
        drop(pending);
        assert!(registry.is_empty());
    }

    #[test]
    fn a_reply_without_a_request_id_header_is_dropped() {
        let registry = Arc::new(RequestRegistry::default());
        let _pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let envelope =
            BusEnvelope::new(Uuid::now_v7(), &Pong { seq: 7 }).expect("pong must serialize");
        registry.resolve(envelope, Ok(ReplyAuthentication::NotEnforced));
        assert_eq!(registry.len(), 1, "the slot must stay in flight");
    }

    #[test]
    fn an_unparsable_request_id_is_dropped() {
        let registry = Arc::new(RequestRegistry::default());
        let _pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let mut envelope =
            BusEnvelope::new(Uuid::now_v7(), &Pong { seq: 7 }).expect("pong must serialize");
        envelope.insert_protocol_header(REQUEST_ID_HEADER, "not-a-uuid".to_owned());
        registry.resolve(envelope, Ok(ReplyAuthentication::NotEnforced));
        assert_eq!(registry.len(), 1, "the slot must stay in flight");
        assert_eq!(registry.counters().orphaned, 1);
    }

    #[test]
    fn an_application_reserved_header_cannot_change_request_resolution() {
        let registry = Arc::new(RequestRegistry::default());
        let request_id = RequestId::new();
        let _pending = registry
            .register(request_id, expectation())
            .expect("registration succeeds");
        let mut envelope = ok_reply(EXPECTED_REPLY);
        envelope.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        envelope.headers.insert(
            "X-Hexeract-Request-Id".to_owned(),
            RequestId::new().to_string(),
        );

        registry.resolve(envelope, Ok(ReplyAuthentication::NotEnforced));

        assert!(
            registry.is_empty(),
            "the private request identity must win over a public-map forgery"
        );
    }

    #[tokio::test]
    async fn the_first_valid_reply_wins_and_duplicates_are_dropped() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), pong_expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();
        let chain = Uuid::now_v7();

        registry.resolve(
            reply_for(request_id, chain, 1),
            Ok(ReplyAuthentication::NotEnforced),
        );
        registry.resolve(
            reply_for(request_id, chain, 2),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (envelope, _authentication) = pending.wait().await.expect("reply");
        let reply: Pong = envelope.decode().expect("decode");
        assert_eq!(reply.seq, 1);
        let counters = registry.counters();
        assert_eq!(
            counters.duplicate, 1,
            "the second valid delivery names an identity this registry resolved itself, which is what tells a duplicate apart from an orphan"
        );
        assert_eq!(
            counters.orphaned, 0,
            "counting the same delivery twice would break the sum of the six counters, which the metrics built on them assume"
        );
    }

    #[tokio::test]
    async fn drain_closes_every_in_flight_slot() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
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

        let registry = Arc::new(RequestRegistry::default());
        let chain = Uuid::now_v7();
        let mut pendings: Vec<_> = (0..SLOT_COUNT)
            .map(|_| {
                registry
                    .register(RequestId::new(), pong_expectation())
                    .expect("registration succeeds")
            })
            .collect();
        let ids: Vec<RequestId> = pendings.iter().map(PendingReply::request_id).collect();

        let resolver = Arc::clone(&registry);
        let resolver_ids = ids.clone();
        let task = tokio::spawn(async move {
            for i in 0..SLOT_COUNT {
                let resolve_index = (i * 7) % SLOT_COUNT;
                let seq = u64::try_from(resolve_index).expect("seq fits in u64");
                resolver.resolve(
                    reply_for(resolver_ids[resolve_index], chain, seq),
                    Ok(ReplyAuthentication::NotEnforced),
                );
            }
        });

        for (index, pending) in pendings.iter_mut().enumerate() {
            let expected_seq = u64::try_from(index).expect("seq fits in u64");
            let (envelope, _authentication) =
                tokio::time::timeout(std::time::Duration::from_secs(5), pending.wait())
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
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();

        registry.resolve(
            tagged(ok_reply("attacker.reply"), request_id),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(registry.len(), 1, "the slot must survive an invalid reply");
        assert_eq!(registry.counters().invalid, 1);

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (reply, _authentication) = pending.wait().await.expect("the legitimate reply must win");
        assert_eq!(reply.message_type, EXPECTED_REPLY);
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn a_reply_with_an_unknown_identity_is_counted_orphaned() {
        let registry = Arc::new(RequestRegistry::default());
        let _pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), RequestId::new()),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(registry.len(), 1);
        assert_eq!(registry.counters().orphaned, 1);
        assert_eq!(registry.counters().invalid, 0);
    }

    #[tokio::test]
    async fn a_reply_without_an_identity_header_is_counted_orphaned() {
        let registry = Arc::new(RequestRegistry::default());
        let _pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");

        registry.resolve(
            ok_reply(EXPECTED_REPLY),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(registry.counters().orphaned, 1);
    }

    #[test]
    fn register_beyond_max_in_flight_is_rejected_and_inserts_nothing() {
        let registry = Arc::new(RequestRegistry::new(1));
        let _first = registry
            .register(RequestId::new(), expectation())
            .expect("the first slot fits within the bound");

        let rejection = registry
            .register(RequestId::new(), expectation())
            .expect_err("the second slot exceeds max_in_flight");

        assert_eq!(rejection, RegisterRejection::AtCapacity);
        assert_eq!(registry.len(), 1, "the refused call must not be inserted");
    }

    #[test]
    fn capacity_freed_by_a_dropped_pending_reply_lets_a_refused_call_pass_after() {
        let registry = Arc::new(RequestRegistry::new(1));
        let first = registry
            .register(RequestId::new(), expectation())
            .expect("the first slot fits within the bound");
        let rejection = registry
            .register(RequestId::new(), expectation())
            .expect_err("no capacity is available yet");
        assert_eq!(rejection, RegisterRejection::AtCapacity);

        drop(first);

        let second = registry.register(RequestId::new(), expectation());
        assert!(
            second.is_ok(),
            "capacity freed by the drop must admit the next call"
        );
    }

    #[tokio::test]
    async fn registering_an_occupied_identity_is_rejected_and_the_first_caller_still_gets_its_reply()
     {
        let registry = Arc::new(RequestRegistry::new(4));
        let request_id = RequestId::new();
        let mut first = registry
            .register(request_id, pong_expectation())
            .expect("the first registration on a fresh identity succeeds");

        let rejection = registry
            .register(request_id, pong_expectation())
            .expect_err("the identity is already in flight");
        assert_eq!(rejection, RegisterRejection::SlotOccupied);
        assert_eq!(registry.len(), 1, "only the first caller's slot exists");

        let chain = Uuid::now_v7();
        registry.resolve(
            reply_for(request_id, chain, 11),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (envelope, _authentication) = first
            .wait()
            .await
            .expect("the first caller still receives its reply");
        let reply: Pong = envelope.decode().expect("decode");
        assert_eq!(reply.seq, 11);
    }

    /// Capacity is freed on exactly two distinct code paths: `resolve`
    /// frees it as soon as a valid reply is consumed, and dropping an
    /// unresolved `PendingReply` frees it otherwise. The drop path is
    /// exercised once here, not once per reason a caller might drop it: a
    /// timed-out wait and an outright abandonment both end by dropping the
    /// identical value through the identical `Drop` impl, so a second drop
    /// phase would not distinguish anything the first does not already
    /// prove. Genuine coverage of an actual elapsed deadline, driven for
    /// real rather than simulated by a bare drop, lives in
    /// `request_client::tests::silent_responder_times_out`, which asserts
    /// `registry.len() == 0` after `RequestClient::request` times out.
    #[test]
    fn capacity_is_freed_on_resolve_and_on_drop() {
        let registry = Arc::new(RequestRegistry::new(1));

        // Resolve: `resolve` consumes and removes the slot immediately,
        // before the caller ever awaits its `PendingReply`.
        let pending = registry
            .register(RequestId::new(), expectation())
            .expect("first slot fits");
        let request_id = pending.request_id();
        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::NotEnforced),
        );
        assert!(
            registry.is_empty(),
            "a resolved reply must free its slot immediately"
        );
        drop(pending);

        // Drop: nothing ever resolves the slot. Dropping the
        // `PendingReply` frees it, whatever the caller's reason for
        // dropping it (timeout elapsed, cancellation, panic unwinding).
        let pending = registry
            .register(RequestId::new(), expectation())
            .expect("capacity was freed by the previous phase");
        drop(pending);
        assert!(registry.is_empty(), "an unresolved slot must free on drop");
    }

    #[test]
    fn a_fresh_registry_is_not_closed() {
        let registry = RequestRegistry::default();
        assert!(!registry.is_closed());
    }

    #[tokio::test]
    async fn close_drops_every_pending_slot_and_the_waiting_caller_observes_a_closed_channel() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");

        registry.close();

        assert!(registry.is_closed());
        assert!(registry.is_empty(), "close must drop every pending slot");
        assert!(
            pending.wait().await.is_err(),
            "a caller waiting on a closed slot observes a closed channel"
        );
    }

    #[test]
    fn register_after_close_is_rejected() {
        let registry = RequestRegistry::default();
        registry.close();

        let rejection = registry
            .register(RequestId::new(), expectation())
            .expect_err("a closed registry refuses new registrations");
        assert_eq!(rejection, RegisterRejection::Closed);
        assert!(registry.is_empty(), "the refused call must not be inserted");
    }

    #[test]
    fn close_called_twice_does_not_panic_and_stays_closed() {
        let registry = RequestRegistry::default();
        registry.close();
        registry.close();
        assert!(registry.is_closed());
        assert!(registry.is_empty());
    }

    /// Races `close` against many concurrent `register` calls on real OS
    /// threads, not tokio tasks: the `std::sync::Mutex` guarding both the
    /// closed flag and the slot map is synchronous, so a genuine thread
    /// race is the faithful way to exercise it.
    ///
    /// Successful registrations are kept alive in `successes` rather than
    /// dropped immediately: dropping a [`PendingReply`] removes its own
    /// slot, which would silently clean up an orphan left by a buggy
    /// implementation before this test ever gets to observe it. Holding
    /// them until after the assertion is what makes an orphan visible.
    ///
    /// The whole race is repeated many times with a fresh registry each
    /// time, to raise the odds of hitting the narrow window a wrong
    /// implementation would open: one that reads a `closed` flag before
    /// taking the slots lock lets a `register` observe `closed == false`,
    /// get preempted, let `close` flip the flag and clear the (still
    /// empty) map, then resume and insert its slot regardless, orphaning
    /// it in a map declared closed. With the flag and the map under the
    /// very same lock, `close` and every `register` are strictly ordered
    /// by the mutex and that window cannot exist: the map must be empty
    /// here whichever one wins the race, every time.
    #[test]
    fn close_concurrent_with_register_never_leaves_an_orphan_slot() {
        for _ in 0..50 {
            let registry = RequestRegistry::new(10_000);
            let successes: Mutex<Vec<PendingReply<'_>>> = Mutex::new(Vec::new());

            std::thread::scope(|scope| {
                for _ in 0..8 {
                    scope.spawn(|| {
                        for _ in 0..300 {
                            if let Ok(pending) = registry.register(RequestId::new(), expectation())
                            {
                                successes
                                    .lock()
                                    .unwrap_or_else(PoisonError::into_inner)
                                    .push(pending);
                            }
                        }
                    });
                }
                scope.spawn(|| registry.close());
            });

            assert!(registry.is_closed());
            assert!(
                registry.is_empty(),
                "no slot may survive a close racing concurrent registrations"
            );
            drop(successes);
        }
    }

    /// Mirrors `many_concurrent_calls_each_receive_their_own_reply` at a
    /// larger scale, `1_000` slots instead of 64, and additionally asserts
    /// the registry returns to zero once every call has completed: the
    /// property `#471` exists to guarantee under sustained concurrent load,
    /// not just for one call at a time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_thousand_concurrent_calls_never_cross_replies_and_the_registry_returns_to_zero() {
        const SLOT_COUNT: usize = 1_000;

        // Large enough that this test never observes `AtCapacity`: capacity
        // rejection has its own dedicated tests above, and this test's
        // purpose is reply routing under concurrency, not capacity.
        let registry = Arc::new(RequestRegistry::new(SLOT_COUNT * 2));
        let chain = Uuid::now_v7();
        let ids: Vec<RequestId> = (0..SLOT_COUNT).map(|_| RequestId::new()).collect();
        let mut pendings: Vec<PendingReply<'_>> = ids
            .iter()
            .map(|&id| {
                registry
                    .register(id, pong_expectation())
                    .expect("capacity is not the point of this test")
            })
            .collect();

        let resolver = Arc::clone(&registry);
        let resolver_ids = ids.clone();
        let task = tokio::spawn(async move {
            for i in 0..SLOT_COUNT {
                let resolve_index = (i * 7) % SLOT_COUNT;
                let seq = u64::try_from(resolve_index).expect("seq fits in u64");
                resolver.resolve(
                    reply_for(resolver_ids[resolve_index], chain, seq),
                    Ok(ReplyAuthentication::NotEnforced),
                );
            }
        });

        for (index, pending) in pendings.iter_mut().enumerate() {
            let expected_seq = u64::try_from(index).expect("seq fits in u64");
            let (envelope, _authentication) =
                tokio::time::timeout(std::time::Duration::from_secs(5), pending.wait())
                    .await
                    .unwrap_or_else(|_| panic!("caller at index {index} never received a reply"))
                    .expect("each caller gets a reply");
            let reply: Pong = envelope.decode().expect("decode");
            assert_eq!(reply.seq, expected_seq, "reply routed to the wrong caller");
        }
        task.await.expect("resolver task must finish");

        drop(pendings);
        assert!(
            registry.is_empty(),
            "the registry must return to zero once every call has completed"
        );
    }

    /// A reply resolved with [`ReplyAuthentication::Authenticated`] hands
    /// that exact principal to the caller waiting on [`PendingReply::wait`].
    /// A degenerate `resolve` that dropped the authentication and always
    /// handed back [`ReplyAuthentication::NotEnforced`] would still let
    /// every reply-routing test above pass, since none of them inspect
    /// anything beyond the envelope; this is the one that would catch it.
    #[tokio::test]
    async fn a_reply_resolved_as_authenticated_hands_the_verified_principal_to_the_waiting_caller()
    {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();
        let principal = principal();

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::Authenticated(principal.clone())),
        );

        let (envelope, authentication) = pending
            .wait()
            .await
            .expect("the resolved reply must be delivered");
        assert_eq!(envelope.message_type, EXPECTED_REPLY);
        assert_eq!(
            authentication,
            ReplyAuthentication::Authenticated(principal)
        );
    }

    /// A reply resolved with [`ReplyAuthentication::NotEnforced`] hands back
    /// exactly `NotEnforced`, never [`ReplyAuthentication::WaivedUnsigned`].
    /// Paired with
    /// [`a_reply_resolved_as_waived_unsigned_hands_back_waived_unsigned_never_not_enforced`]:
    /// each asserts exact equality with its own variant rather than merely
    /// rejecting the other one, because a `resolve` that always handed back
    /// `NotEnforced` regardless of what it was given would satisfy that
    /// negative assertion in its twin just as well as this one.
    #[tokio::test]
    async fn a_reply_resolved_as_not_enforced_hands_back_not_enforced_never_waived_unsigned() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (_envelope, authentication) = pending
            .wait()
            .await
            .expect("the resolved reply must be delivered");
        assert_eq!(authentication, ReplyAuthentication::NotEnforced);
    }

    /// A reply resolved with [`ReplyAuthentication::WaivedUnsigned`] hands
    /// back exactly `WaivedUnsigned`, never
    /// [`ReplyAuthentication::NotEnforced`]. The symmetric half of
    /// [`a_reply_resolved_as_not_enforced_hands_back_not_enforced_never_waived_unsigned`]:
    /// without this test, a `resolve` that always handed back
    /// `WaivedUnsigned` regardless of what it was given would pass its
    /// twin's assertion trivially, since that test never checks this
    /// variant is reachable at all.
    #[tokio::test]
    async fn a_reply_resolved_as_waived_unsigned_hands_back_waived_unsigned_never_not_enforced() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::WaivedUnsigned),
        );

        let (_envelope, authentication) = pending
            .wait()
            .await
            .expect("the resolved reply must be delivered");
        assert_eq!(authentication, ReplyAuthentication::WaivedUnsigned);
    }

    /// A reply refused by `reply_acceptance` must leave the slot pending and
    /// emit nothing, exactly as it does today: the authentication passed
    /// alongside the rejected reply is abandoned with it, never surfacing
    /// through a later `wait()`. Only the legitimate reply that eventually
    /// wins may hand its own authentication to the caller.
    #[tokio::test]
    async fn a_reply_rejected_by_reply_acceptance_leaves_the_slot_pending_and_drops_its_authentication()
     {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();

        registry.resolve(
            tagged(ok_reply("attacker.reply"), request_id),
            Ok(ReplyAuthentication::Authenticated(principal())),
        );

        assert_eq!(
            registry.len(),
            1,
            "an invalid reply must leave the slot pending"
        );
        assert_eq!(registry.counters().invalid, 1);

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (envelope, authentication) = pending
            .wait()
            .await
            .expect("the legitimate reply must still resolve the slot");
        assert_eq!(envelope.message_type, EXPECTED_REPLY);
        assert_eq!(
            authentication,
            ReplyAuthentication::NotEnforced,
            "the legitimate reply's own authentication must win, never the rejected one"
        );
    }

    #[test]
    fn a_delivery_the_transport_refused_is_counted_unauthenticated() {
        let registry = Arc::new(RequestRegistry::default());
        let request_id = RequestId::new();
        let _pending = registry
            .register(request_id, expectation())
            .expect("registration succeeds");

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Err(ReplyRejection::Unauthenticated),
        );

        assert_eq!(registry.counters().unauthenticated, 1);
    }

    #[tokio::test]
    async fn an_unauthenticated_delivery_leaves_the_slot_pending() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Err(ReplyRejection::Unauthenticated),
        );

        assert_eq!(
            registry.len(),
            1,
            "a delivery the transport refused must leave the slot pending"
        );

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (envelope, _authentication) = pending
            .wait()
            .await
            .expect("the legitimate reply must still resolve the slot");
        assert_eq!(envelope.message_type, EXPECTED_REPLY);
    }

    #[tokio::test]
    async fn a_second_valid_reply_is_counted_duplicate_not_orphaned() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), pong_expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();
        let chain = Uuid::now_v7();

        registry.resolve(
            reply_for(request_id, chain, 1),
            Ok(ReplyAuthentication::NotEnforced),
        );
        pending.wait().await.expect("the first reply must resolve");

        registry.resolve(
            reply_for(request_id, chain, 2),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(
            registry.counters().duplicate,
            1,
            "a second valid reply for an already-resolved identity must be counted duplicate"
        );
        assert_eq!(
            registry.counters().orphaned,
            0,
            "a duplicate must never inflate the orphaned counter"
        );
    }

    #[test]
    fn a_reply_arriving_after_the_caller_gave_up_is_counted_late() {
        let registry = Arc::new(RequestRegistry::default());
        let pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();
        drop(pending);

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(
            registry.counters().late,
            1,
            "a reply for an identity the caller abandoned must be counted late"
        );
        assert_eq!(
            registry.counters().orphaned,
            0,
            "a late reply must never inflate the orphaned counter"
        );
    }

    #[test]
    fn a_reply_for_an_identity_never_seen_is_counted_orphaned() {
        let registry = Arc::new(RequestRegistry::default());

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), RequestId::new()),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(registry.counters().orphaned, 1);
    }

    #[test]
    fn an_undecodable_delivery_is_counted_undecodable() {
        let registry = RequestRegistry::default();

        registry.record_undecodable();

        assert_eq!(registry.counters().undecodable, 1);
    }

    #[tokio::test]
    async fn resolving_a_slot_records_it_as_resolved_not_abandoned() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), pong_expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();
        let chain = Uuid::now_v7();

        registry.resolve(
            reply_for(request_id, chain, 1),
            Ok(ReplyAuthentication::NotEnforced),
        );
        pending
            .wait()
            .await
            .expect("the resolved reply must be delivered");
        drop(pending);

        registry.resolve(
            reply_for(request_id, chain, 2),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(
            registry.counters().duplicate,
            1,
            "a slot resolved before the guard drops must still be counted duplicate"
        );
        assert_eq!(
            registry.counters().late,
            0,
            "resolving must record the retirement before the guard drops, never letting the \
             drop overwrite it as abandoned"
        );
    }

    #[test]
    fn the_last_rejection_is_readable_from_the_pending_reply() {
        let registry = Arc::new(RequestRegistry::default());
        let pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Err(ReplyRejection::MissingVersion),
        );
        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), request_id),
            Err(ReplyRejection::UnsupportedVersion { version: 7 }),
        );

        assert_eq!(
            pending.last_rejection(),
            Some(ReplyRejection::UnsupportedVersion { version: 7 }),
            "the last rejection must be the most recent one, not the first"
        );
    }

    #[test]
    fn each_refused_delivery_increments_the_slot_counter() {
        let registry = Arc::new(RequestRegistry::default());
        let pending = registry
            .register(RequestId::new(), expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();

        for _ in 0..3 {
            registry.resolve(
                tagged(ok_reply(EXPECTED_REPLY), request_id),
                Err(ReplyRejection::Unauthenticated),
            );
        }

        assert_eq!(pending.rejected_deliveries(), 3);
    }

    #[tokio::test]
    async fn a_valid_reply_after_rejections_still_wins() {
        let registry = Arc::new(RequestRegistry::default());
        let mut pending = registry
            .register(RequestId::new(), pong_expectation())
            .expect("registration succeeds");
        let request_id = pending.request_id();
        let chain = Uuid::now_v7();

        registry.resolve(
            tagged(ok_reply("attacker.reply"), request_id),
            Err(ReplyRejection::Unauthenticated),
        );
        registry.resolve(
            tagged(ok_reply("attacker.reply"), request_id),
            Err(ReplyRejection::MissingVersion),
        );

        registry.resolve(
            reply_for(request_id, chain, 42),
            Ok(ReplyAuthentication::NotEnforced),
        );

        let (envelope, _authentication) = pending
            .wait()
            .await
            .expect("the legitimate reply must still win after prior rejections");
        let reply: Pong = envelope.decode().expect("decode");
        assert_eq!(reply.seq, 42);
    }

    #[test]
    fn beyond_capacity_a_late_reply_is_counted_orphaned() {
        let registry = Arc::new(RequestRegistry::new(1));

        let first = registry
            .register(RequestId::new(), expectation())
            .expect("the first slot fits within the bound");
        let first_id = first.request_id();
        drop(first);

        let second = registry
            .register(RequestId::new(), expectation())
            .expect("capacity freed by the previous drop");
        drop(second);

        registry.resolve(
            tagged(ok_reply(EXPECTED_REPLY), first_id),
            Ok(ReplyAuthentication::NotEnforced),
        );

        assert_eq!(
            registry.counters().orphaned,
            1,
            "a straggler beyond the documented retirement capacity must be orphaned again"
        );
        assert_eq!(registry.counters().late, 0);
    }
}
