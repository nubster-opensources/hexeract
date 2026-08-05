use std::time::Duration;

/// Per-call overrides for a request issued through a [`crate::RequestClient`].
///
/// Every field defaults to "inherit the caller's usual behavior": an absent
/// `timeout` falls back to the client's own default timeout, and an absent
/// `destination` falls back to the request type's own
/// [`crate::Request::DESTINATION`]. Marked `#[non_exhaustive]` so a later
/// release can grow another per-call override, such as a deadline distinct
/// from a timeout, without breaking existing callers that build one through
/// [`RequestOptions::new`].
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
    /// Overrides the client's default call timeout.
    pub timeout: Option<Duration>,
    /// Overrides `Request::DESTINATION` for this call only.
    pub destination: Option<String>,
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

    /// Override the client's default call timeout for this call only.
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn default_overrides_nothing() {
        let options = RequestOptions::default();
        assert_eq!(options.timeout, None);
        assert_eq!(options.destination, None);
    }

    #[test]
    fn new_is_equivalent_to_default() {
        let built = RequestOptions::new();
        assert_eq!(built.timeout, None);
        assert_eq!(built.destination, None);
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
