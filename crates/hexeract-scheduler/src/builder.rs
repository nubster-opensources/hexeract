//! Fluent, validated assembly of a [`SchedulerWorker`].
//!
//! # Quick start
//!
//! ```
//! use std::time::Duration;
//! use hexeract_scheduler::{SchedulerBuilder, InMemoryScheduleStore, ScheduleSink, ScheduledMessage, SchedulerError};
//!
//! struct NoopSink;
//!
//! impl ScheduleSink for NoopSink {
//!     async fn dispatch(&self, _message: &ScheduledMessage) -> Result<(), SchedulerError> {
//!         Ok(())
//!     }
//! }
//!
//! let worker = SchedulerBuilder::new(InMemoryScheduleStore::default(), NoopSink)
//!     .batch_size(50)
//!     .lease(Duration::from_secs(60))
//!     .build()?;
//! assert_eq!(worker.config().batch_size, 50);
//! # Ok::<(), SchedulerError>(())
//! ```

use std::time::Duration;

use crate::error::SchedulerError;
use crate::sink::ScheduleSink;
use crate::store::ScheduleStore;
use crate::worker::{SchedulerWorker, SchedulerWorkerConfig};

/// Fluent builder that validates worker configuration before constructing a
/// [`SchedulerWorker`].
///
/// Start with [`SchedulerBuilder::new`], apply per-field overrides with the
/// chainable setters, then call [`SchedulerBuilder::build`] to validate and
/// obtain the worker. Any invalid combination is rejected with a typed
/// [`SchedulerError::InvalidConfiguration`] rather than silently producing an
/// incoherent worker.
pub struct SchedulerBuilder<S, K>
where
    S: ScheduleStore,
    K: ScheduleSink,
{
    store: S,
    sink: K,
    config: SchedulerWorkerConfig,
}

impl<S, K> SchedulerBuilder<S, K>
where
    S: ScheduleStore,
    K: ScheduleSink,
{
    /// Create a builder over `store` and `sink`.
    ///
    /// The initial configuration is [`SchedulerWorkerConfig::default`].
    #[must_use]
    pub fn new(store: S, sink: K) -> Self {
        Self {
            store,
            sink,
            config: SchedulerWorkerConfig::default(),
        }
    }

    /// Override the poll interval.
    #[must_use]
    pub fn poll_interval(mut self, value: Duration) -> Self {
        self.config.poll_interval = value;
        self
    }

    /// Override the maximum number of occurrences claimed per cycle.
    #[must_use]
    pub fn batch_size(mut self, value: usize) -> Self {
        self.config.batch_size = value;
        self
    }

    /// Override the lease duration granted to each claimed occurrence.
    #[must_use]
    pub fn lease(mut self, value: Duration) -> Self {
        self.config.lease = value;
        self
    }

    /// Override the base delay of the exponential retry backoff.
    #[must_use]
    pub fn retry_base_delay(mut self, value: Duration) -> Self {
        self.config.retry_base_delay = value;
        self
    }

    /// Override the upper bound of the exponential retry backoff.
    #[must_use]
    pub fn retry_max_delay(mut self, value: Duration) -> Self {
        self.config.retry_max_delay = value;
        self
    }

    /// Override whether full jitter is applied to the retry backoff.
    #[must_use]
    pub fn jitter(mut self, value: bool) -> Self {
        self.config.jitter = value;
        self
    }

    /// Override the delay between consecutive non-empty cycles.
    ///
    /// Zero disables inter-cycle pacing (occurrences are drained as fast as
    /// possible).
    #[must_use]
    pub fn min_cycle_delay(mut self, value: Duration) -> Self {
        self.config.min_cycle_delay = value;
        self
    }

    /// Override the hard dispatch timeout per occurrence.
    #[must_use]
    pub fn dispatch_timeout(mut self, value: Duration) -> Self {
        self.config.dispatch_timeout = value;
        self
    }

    /// Validate the configuration and build the [`SchedulerWorker`].
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::InvalidConfiguration`] when any constraint is
    /// violated:
    ///
    /// - `batch_size` must be at least 1.
    /// - `poll_interval`, `lease`, `dispatch_timeout` and `retry_base_delay`
    ///   must be non-zero.
    /// - `retry_max_delay` must be >= `retry_base_delay`.
    pub fn build(self) -> Result<SchedulerWorker<S, K>, SchedulerError> {
        if self.config.batch_size == 0 {
            return Err(SchedulerError::invalid_configuration(
                "batch_size must be at least 1",
            ));
        }
        if self.config.poll_interval.is_zero() {
            return Err(SchedulerError::invalid_configuration(
                "poll_interval must be non-zero",
            ));
        }
        if self.config.lease.is_zero() {
            return Err(SchedulerError::invalid_configuration(
                "lease must be non-zero",
            ));
        }
        if self.config.dispatch_timeout.is_zero() {
            return Err(SchedulerError::invalid_configuration(
                "dispatch_timeout must be non-zero",
            ));
        }
        if self.config.retry_base_delay.is_zero() {
            return Err(SchedulerError::invalid_configuration(
                "retry_base_delay must be non-zero",
            ));
        }
        if self.config.retry_max_delay < self.config.retry_base_delay {
            return Err(SchedulerError::invalid_configuration(
                "retry_max_delay must be >= retry_base_delay",
            ));
        }
        Ok(SchedulerWorker::new(self.store, self.sink, self.config))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::SchedulerBuilder;
    use crate::error::SchedulerError;
    use crate::memory::InMemoryScheduleStore;
    use crate::schedule::ScheduledMessage;
    use crate::sink::ScheduleSink;
    use crate::worker::SchedulerWorkerConfig;

    struct NoopSink;

    impl ScheduleSink for NoopSink {
        async fn dispatch(&self, _message: &ScheduledMessage) -> Result<(), SchedulerError> {
            Ok(())
        }
    }

    fn store() -> InMemoryScheduleStore {
        InMemoryScheduleStore::default()
    }

    fn builder() -> SchedulerBuilder<InMemoryScheduleStore, NoopSink> {
        SchedulerBuilder::new(store(), NoopSink)
    }

    #[test]
    fn defaults_build_successfully() {
        let worker = builder().build().expect("default config is valid");
        let defaults = SchedulerWorkerConfig::default();
        let config = worker.config();
        assert_eq!(config.batch_size, defaults.batch_size);
        assert_eq!(config.poll_interval, defaults.poll_interval);
        assert_eq!(config.lease, defaults.lease);
        assert_eq!(config.retry_base_delay, defaults.retry_base_delay);
        assert_eq!(config.retry_max_delay, defaults.retry_max_delay);
        assert_eq!(config.jitter, defaults.jitter);
        assert_eq!(config.min_cycle_delay, defaults.min_cycle_delay);
        assert_eq!(config.dispatch_timeout, defaults.dispatch_timeout);
    }

    #[test]
    fn batch_size_setter_is_applied() {
        let worker = builder().batch_size(50).build().expect("valid config");
        assert_eq!(worker.config().batch_size, 50);
    }

    #[test]
    fn lease_setter_is_applied() {
        let lease = Duration::from_secs(120);
        let worker = builder().lease(lease).build().expect("valid config");
        assert_eq!(worker.config().lease, lease);
    }

    #[test]
    fn poll_interval_setter_is_applied() {
        let interval = Duration::from_millis(500);
        let worker = builder()
            .poll_interval(interval)
            .build()
            .expect("valid config");
        assert_eq!(worker.config().poll_interval, interval);
    }

    #[test]
    fn jitter_setter_is_applied() {
        let worker = builder().jitter(false).build().expect("valid config");
        assert!(!worker.config().jitter);
    }

    #[test]
    fn zero_batch_size_is_rejected() {
        let err = builder()
            .batch_size(0)
            .build()
            .err()
            .expect("build must fail");
        assert!(matches!(err, SchedulerError::InvalidConfiguration { .. }));
        assert!(err.to_string().contains("batch_size"));
    }

    #[test]
    fn zero_poll_interval_is_rejected() {
        let err = builder()
            .poll_interval(Duration::ZERO)
            .build()
            .err()
            .expect("build must fail");
        assert!(matches!(err, SchedulerError::InvalidConfiguration { .. }));
        assert!(err.to_string().contains("poll_interval"));
    }

    #[test]
    fn zero_lease_is_rejected() {
        let err = builder()
            .lease(Duration::ZERO)
            .build()
            .err()
            .expect("build must fail");
        assert!(matches!(err, SchedulerError::InvalidConfiguration { .. }));
        assert!(err.to_string().contains("lease"));
    }

    #[test]
    fn zero_dispatch_timeout_is_rejected() {
        let err = builder()
            .dispatch_timeout(Duration::ZERO)
            .build()
            .err()
            .expect("build must fail");
        assert!(matches!(err, SchedulerError::InvalidConfiguration { .. }));
        assert!(err.to_string().contains("dispatch_timeout"));
    }

    #[test]
    fn zero_retry_base_delay_is_rejected() {
        let err = builder()
            .retry_base_delay(Duration::ZERO)
            .build()
            .err()
            .expect("build must fail");
        assert!(matches!(err, SchedulerError::InvalidConfiguration { .. }));
        assert!(err.to_string().contains("retry_base_delay"));
    }

    #[test]
    fn retry_max_delay_less_than_base_is_rejected() {
        let err = builder()
            .retry_base_delay(Duration::from_secs(10))
            .retry_max_delay(Duration::from_secs(5))
            .build()
            .err()
            .expect("build must fail");
        assert!(matches!(err, SchedulerError::InvalidConfiguration { .. }));
        assert!(err.to_string().contains("retry_max_delay"));
    }

    #[test]
    fn zero_min_cycle_delay_is_accepted() {
        let result = builder().min_cycle_delay(Duration::ZERO).build();
        assert!(
            result.is_ok(),
            "zero min_cycle_delay disables pacing and is valid"
        );
    }
}
