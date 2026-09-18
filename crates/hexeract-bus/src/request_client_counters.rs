//! Point-in-time totals of calls issued through a [`crate::RequestClient`].

use std::sync::atomic::{AtomicU64, Ordering};

use crate::request_outcome::RequestOutcome;
use crate::request_registry::ReplyCountersSnapshot;

/// Point-in-time totals of calls issued through a request client.
///
/// Each field but `in_flight` and `replies` is one leaf of a closed-set
/// outcome taxonomy: every call this client finishes lands in exactly one of
/// them, so at rest, once no call is in flight, `started` equals the sum of the eight
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
    /// Each field but `replies` is loaded independently, not atomically: see
    /// the struct-level doc for what that means for their sum while a call
    /// is in flight.
    pub(crate) fn snapshot(&self, replies: ReplyCountersSnapshot) -> RequestClientCountersSnapshot {
        RequestClientCountersSnapshot {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            started: self.started.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            remote_failed: self.remote_failed.load(Ordering::Relaxed),
            transport_failed: self.transport_failed.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
            publication_unknown: self.publication_unknown.load(Ordering::Relaxed),
            invalid_reply: self.invalid_reply.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            replies,
        }
    }

    /// Count one more call attempted, admitted or refused alike.
    pub(crate) fn count_started(&self) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    /// Raise the `in_flight` gauge for a call whose registration just
    /// succeeded.
    pub(crate) fn raise_in_flight(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    /// Bring the `in_flight` gauge back down for a call that was admitted
    /// and has now left this client's observation, whatever its outcome.
    pub(crate) fn release_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Count one more call whose future was dropped before an outcome was
    /// established.
    pub(crate) fn count_cancelled(&self) {
        self.cancelled.fetch_add(1, Ordering::Relaxed);
    }

    /// Count `outcome` against its own leaf of the taxonomy.
    pub(crate) fn count_outcome(&self, outcome: RequestOutcome) {
        let counter = match outcome {
            RequestOutcome::Succeeded => &self.succeeded,
            RequestOutcome::TimedOut => &self.timed_out,
            RequestOutcome::RemoteFailed => &self.remote_failed,
            RequestOutcome::TransportFailed => &self.transport_failed,
            RequestOutcome::Refused => &self.refused,
            RequestOutcome::PublicationUnknown => &self.publication_unknown,
            RequestOutcome::InvalidReply => &self.invalid_reply,
            RequestOutcome::Cancelled => &self.cancelled,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}
