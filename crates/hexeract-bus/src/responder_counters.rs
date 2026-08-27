use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counts of inbound requests rejected before a responder handler runs.
///
/// Clone this handle and retain one copy when registering a request handler
/// to inspect the responder-side rejection totals later. The counters are
/// monotonic and shared by every clone.
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
    pub invalid_reply_to: u64,
    /// Requests with an absent or unparsable request identity.
    pub invalid_request_id: u64,
    /// Requests announcing a missing or unsupported protocol version.
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
