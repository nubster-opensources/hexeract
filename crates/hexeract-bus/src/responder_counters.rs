use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counts of inbound requests a responder refused on the RPC envelope
/// contract, before the domain handler ran.
///
/// Clone this handle and retain one copy when registering a request handler
/// to inspect the responder-side rejection totals later. The counters are
/// monotonic and shared by every clone.
///
/// The scope is the envelope contract itself: its reply destination, its
/// request identity, its protocol version. It is not every rejection
/// [`RepliedHandler`](crate::RepliedHandler) can make before dispatch. A
/// request whose *payload* fails to decode is deliberately absent, and is
/// reported to the caller as
/// [`RemoteErrorType::Malformed`](crate::RemoteErrorType): that is a schema
/// mismatch on one message type between two peers, not a signal about the
/// responder's inbound protocol health, which is what these three measure.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_handle_counts_nothing() {
        assert_eq!(
            ResponderCounters::default().snapshot(),
            ResponderCountersSnapshot {
                invalid_reply_to: 0,
                invalid_request_id: 0,
                unsupported_protocol_version: 0,
            }
        );
    }

    #[test]
    fn clones_share_one_set_of_counters() {
        let counters = ResponderCounters::default();
        let retained = counters.clone();

        counters.count_invalid_reply_to();
        retained.count_invalid_request_id();

        let expected = ResponderCountersSnapshot {
            invalid_reply_to: 1,
            invalid_request_id: 1,
            unsupported_protocol_version: 0,
        };
        assert_eq!(
            counters.snapshot(),
            expected,
            "the handle handed to a responder must see what its clone counted"
        );
        assert_eq!(
            retained.snapshot(),
            expected,
            "the clone an operator retains must see what the responder counted"
        );
    }

    /// Guards the one defect the categorizing test in `replied_handler`
    /// cannot see on its own: a `count_*` method wired to the wrong field
    /// would still move a counter on every rejection, and only a per-kind
    /// assertion tells the three apart.
    #[test]
    fn each_kind_increments_only_its_own_counter() {
        let counters = ResponderCounters::default();
        counters.count_invalid_reply_to();
        counters.count_invalid_request_id();
        counters.count_invalid_request_id();
        counters.count_unsupported_protocol_version();
        counters.count_unsupported_protocol_version();
        counters.count_unsupported_protocol_version();

        assert_eq!(
            counters.snapshot(),
            ResponderCountersSnapshot {
                invalid_reply_to: 1,
                invalid_request_id: 2,
                unsupported_protocol_version: 3,
            }
        );
    }
}
