//! Point-in-time totals of calls issued through a [`crate::RequestClient`].

use std::sync::atomic::AtomicU64;

use crate::request_registry::ReplyCountersSnapshot;

/// Point-in-time totals of calls issued through a request client.
///
/// Each field but `in_flight` and `replies` is one leaf of the closed-set
/// outcome taxonomy [`crate::request_outcome::RequestOutcome`] names:
/// at rest, once no call is in flight, `started` equals the sum of the eight
/// outcome totals below it. Every field is monotonic and exact on its own,
/// but the ten fields are read one after another, not atomically, so their
/// sum is exact only when no call is being observed concurrently (the same
/// reserve [`ReplyCountersSnapshot`] documents for its own six fields).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RequestClientCountersSnapshot {
    /// Calls admitted for publication and not yet finished. Rises only
    /// after a successful registration, so it never exceeds the registry's
    /// `max_in_flight` bound, and every admitted call brings it back down
    /// exactly once, on every exit path, cancellation included.
    pub in_flight: u64,
    /// Every call this client has attempted, admitted or refused alike.
    pub started: u64,
    /// Calls that returned a typed reply.
    pub succeeded: u64,
    /// Calls that did not complete within their one end-to-end local
    /// deadline, on either side of publication.
    pub timed_out: u64,
    /// Calls the responder reported a failure for.
    pub remote_failed: u64,
    /// Calls whose request could not be published, or whose reply channel
    /// was lost.
    pub transport_failed: u64,
    /// Calls this client refused before anything was published:
    /// [`crate::RequestError::AtCapacity`], [`crate::RequestError::Closed`],
    /// or a [`crate::ProtocolViolation::IdentityCollision`].
    pub refused: u64,
    /// Calls where shutdown began after the request was admitted for
    /// publication, so whether the responder acted on it is unknown.
    pub publication_unknown: u64,
    /// Calls whose reply violated the protocol, or could not be decoded
    /// into the expected reply type.
    pub invalid_reply: u64,
    /// Calls whose future was dropped before an outcome was established.
    pub cancelled: u64,
    /// Refused-delivery totals from this client's [`crate::RequestRegistry`],
    /// carried alongside these call-level totals rather than duplicated:
    /// see [`ReplyCountersSnapshot`] for its own fields and their own
    /// non-atomic reading reserve.
    pub replies: ReplyCountersSnapshot,
}

/// Shared, atomically updated totals behind [`RequestClientCountersSnapshot`].
#[derive(Debug, Default)]
pub(crate) struct RequestClientCounters {
    in_flight: AtomicU64,
    started: AtomicU64,
    succeeded: AtomicU64,
    timed_out: AtomicU64,
    remote_failed: AtomicU64,
    transport_failed: AtomicU64,
    refused: AtomicU64,
    publication_unknown: AtomicU64,
    invalid_reply: AtomicU64,
    cancelled: AtomicU64,
}

impl RequestClientCounters {
    /// Return a point-in-time snapshot of every call-level total, alongside
    /// `replies` carried through unchanged.
    ///
    /// Inert in this revision: every call-level field renders `0`
    /// regardless of what this handle has observed. The real reads land
    /// with the implementation.
    pub(crate) fn snapshot(&self, replies: ReplyCountersSnapshot) -> RequestClientCountersSnapshot {
        RequestClientCountersSnapshot {
            in_flight: 0,
            started: 0,
            succeeded: 0,
            timed_out: 0,
            remote_failed: 0,
            transport_failed: 0,
            refused: 0,
            publication_unknown: 0,
            invalid_reply: 0,
            cancelled: 0,
            replies,
        }
    }
}
