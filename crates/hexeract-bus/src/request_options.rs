use std::time::Duration;

use hexeract_core::CorrelationId;

/// Per-call overrides for a request issued through a [`crate::RequestClient`].
///
/// Every field defaults to "inherit the caller's usual behavior": an absent
/// `timeout` falls back to the client's own default timeout, an absent
/// `destination` falls back to the request type's own
/// [`crate::Request::DESTINATION`], and an absent `correlation_id` opens a
/// fresh causal chain rather than joining one already in progress. Marked
/// `#[non_exhaustive]` so a later release can grow another per-call
/// override, such as protocol metadata, without breaking existing callers
/// that build one through [`RequestOptions::new`].
///
/// # Example
///
/// ```
/// use std::time::Duration;
///
/// use hexeract_bus::RequestOptions;
///
/// let options = RequestOptions::new()
///     .with_timeout(Duration::from_secs(2))
///     .with_destination("accounts.priority");
///
/// assert_eq!(options.timeout, Some(Duration::from_secs(2)));
/// assert_eq!(options.destination.as_deref(), Some("accounts.priority"));
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// Overrides the client's default end-to-end local call timeout.
    ///
    /// The budget covers both publishing the request and waiting for its
    /// reply; publication latency does not start a fresh reply-wait budget.
    pub timeout: Option<Duration>,
    /// Overrides `Request::DESTINATION` for this call only.
    pub destination: Option<String>,
    /// Joins an existing causal chain instead of opening a new one.
    pub correlation_id: Option<CorrelationId>,
}

impl RequestOptions {
    /// Build an empty [`RequestOptions`] that overrides nothing.
    ///
    /// Equivalent to [`RequestOptions::default`]; provided as the
    /// idiomatic entry point into the builder chain.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the client's default end-to-end local timeout for this call.
    ///
    /// A `timeout` beyond the one hour horizon a responder enforces is
    /// still honored locally: this call keeps waiting for the full
    /// duration. What changes is the wire: the published request carries
    /// no `x-hexeract-deadline` header at all, since a responder would
    /// only refuse a deadline built past that horizon. This lets the call
    /// run exactly as it would without the deadline feature; a responder
    /// cannot refuse it early.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Override `Request::DESTINATION` for this call only.
    #[must_use]
    pub fn with_destination(mut self, destination: impl Into<String>) -> Self {
        self.destination = Some(destination.into());
        self
    }

    /// Join the causal chain identified by `correlation_id` instead of
    /// opening a fresh one for this call.
    ///
    /// `correlation_id` labels the causal chain and may be shared by
    /// several calls; it is never the identity of this call itself. Never
    /// derive one from the other: pass the `correlation_id` an inbound
    /// [`hexeract_core::HandlerContext`] carried, not a `RequestId`, and
    /// let `RequestClient` mint its own fresh `RequestId` for this call as
    /// it always does.
    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hexeract_core::CorrelationId;

    use super::*;

    #[test]
    fn default_overrides_nothing() {
        let options = RequestOptions::default();
        assert_eq!(options.timeout, None);
        assert_eq!(options.destination, None);
        assert_eq!(options.correlation_id, None);
    }

    #[test]
    fn new_is_equivalent_to_default() {
        let built = RequestOptions::new();
        assert_eq!(built.timeout, None);
        assert_eq!(built.destination, None);
        assert_eq!(built.correlation_id, None);
    }

    #[test]
    fn with_correlation_id_sets_only_the_correlation_override() {
        let correlation_id = CorrelationId::new();
        let options = RequestOptions::new().with_correlation_id(correlation_id);
        assert_eq!(options.correlation_id, Some(correlation_id));
        assert_eq!(options.timeout, None);
        assert_eq!(options.destination, None);
    }

    #[test]
    fn with_timeout_sets_only_the_timeout_override() {
        let options = RequestOptions::new().with_timeout(Duration::from_millis(50));
        assert_eq!(options.timeout, Some(Duration::from_millis(50)));
        assert_eq!(options.destination, None);
    }

    #[test]
    fn with_destination_sets_only_the_destination_override() {
        let options = RequestOptions::new().with_destination("accounts.priority");
        assert_eq!(options.destination.as_deref(), Some("accounts.priority"));
        assert_eq!(options.timeout, None);
    }

    #[test]
    fn chained_constructors_set_both_overrides() {
        let options = RequestOptions::new()
            .with_timeout(Duration::from_secs(1))
            .with_destination("accounts.priority");
        assert_eq!(options.timeout, Some(Duration::from_secs(1)));
        assert_eq!(options.destination.as_deref(), Some("accounts.priority"));
    }
}
