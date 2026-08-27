use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counts of inbound requests a responder refused on the RPC envelope
/// contract, before the domain handler ran.
///
/// Clone this handle and retain one copy when registering a request handler
/// to inspect the responder-side rejection totals later. The counters are
/// monotonic and shared by every clone.
///
/// The scope is the envelope contract itself, not every rejection
/// [`RepliedHandler`](crate::RepliedHandler) can make before dispatch: its
/// reply destination, its request identity, its protocol version. A request
/// whose *payload* fails to decode is deliberately absent, and is reported
/// to the caller as [`RemoteErrorType::Malformed`](crate::RemoteErrorType):
/// that is a schema mismatch on one message type between two peers, not a
/// signal about the responder's inbound protocol health, which is what
/// these three measure.
#[derive(Debug, Clone, Default)]
pub struct ResponderCounters {
    inner: Arc<ResponderCountersInner>,
}

#[derive(Debug, Default)]
struct ResponderCountersInner {
    invalid_reply_to: AtomicU64,
    invalid_request_id: AtomicU64,
    unsupported_protocol_version: AtomicU64,
}

/// Point-in-time responder-side rejection totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResponderCountersSnapshot {
    /// Requests with an absent or policy-rejected reply destination.
    ///
    /// The two are counted as one: either way the responder has nowhere it
    /// is allowed to publish, and the caller sees nothing but a timeout.
    /// The `warn` event emitted on the same path names which of the two it
    /// was, and on which message type.
    pub invalid_reply_to: u64,
    /// Requests with an absent or unparsable request identity.
    ///
    /// Counted as one for the same reason, though the two point at
    /// different peer defects: a missing header at a non-conforming client
    /// library, an unreadable one at an encoding bug in a peer that
    /// otherwise believes it speaks the protocol. That distinction is a
    /// diagnosis, not a rate, so it stays in the `warn` event.
    pub invalid_request_id: u64,
    /// Requests announcing a missing or unsupported protocol version.
    ///
    /// The one counted rejection that answers its caller, with
    /// [`RemoteErrorType::Unsupported`](crate::RemoteErrorType). Should that
    /// reply fail to publish, the delivery fails with it, so a transport
    /// that redelivers counts the retry again: this is a count of
    /// rejections, not of distinct requests.
    pub unsupported_protocol_version: u64,
}

impl ResponderCounters {
    /// Return a point-in-time snapshot of all responder rejection totals.
    #[must_use]
    pub fn snapshot(&self) -> ResponderCountersSnapshot {
        ResponderCountersSnapshot {
            invalid_reply_to: self.inner.invalid_reply_to.load(Ordering::Relaxed),
            invalid_request_id: self.inner.invalid_request_id.load(Ordering::Relaxed),
            unsupported_protocol_version: self
                .inner
                .unsupported_protocol_version
                .load(Ordering::Relaxed),
        }
    }

    pub(crate) fn count_invalid_reply_to(&self) {
        self.inner.invalid_reply_to.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_invalid_request_id(&self) {
        self.inner
            .invalid_request_id
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_unsupported_protocol_version(&self) {
        self.inner
            .unsupported_protocol_version
            .fetch_add(1, Ordering::Relaxed);
    }
}

