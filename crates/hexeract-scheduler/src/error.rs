use uuid::Uuid;

use thiserror::Error;

/// Errors raised by the scheduler primitives, stores and sinks.
///
/// Marked `#[non_exhaustive]` so new variants can be added without a
/// breaking change. Variants carrying data are built through the
/// constructors on this type rather than by literal, so their internals can
/// evolve without breaking callers in other crates.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchedulerError {
    /// The event payload could not be serialized or deserialized as JSON.
    #[error("failed to (de)serialize event payload as JSON")]
    Serialization(#[from] serde_json::Error),

    /// A trigger was rejected because it is structurally invalid.
    #[error("invalid trigger: {reason}")]
    #[non_exhaustive]
    InvalidTrigger {
        /// Human-readable explanation of why the trigger was rejected.
        reason: String,
    },

    /// The backend reported a storage-level failure.
    ///
    /// The original error is preserved as a boxed source so callers can
    /// downcast if they need typed access to the underlying driver error.
    #[error("schedule store error")]
    Database(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// No schedule exists for the requested identifier.
    #[error("no schedule found for id {schedule_id}")]
    #[non_exhaustive]
    ScheduleNotFound {
        /// Identifier that did not match any schedule.
        schedule_id: Uuid,
    },

    /// An invariant of the scheduler machinery was violated.
    ///
    /// Signals a bug in the framework itself, not a recoverable error.
    #[error("internal scheduler error: {0}")]
    Internal(String),
}

impl SchedulerError {
    /// Build an [`SchedulerError::InvalidTrigger`] from a reason.
    #[must_use]
    pub fn invalid_trigger(reason: impl Into<String>) -> Self {
        Self::InvalidTrigger {
            reason: reason.into(),
        }
    }

    /// Build an [`SchedulerError::Database`] from a backend error.
    #[must_use]
    pub fn database(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Database(Box::new(source))
    }

    /// Build an [`SchedulerError::ScheduleNotFound`] for `schedule_id`.
    #[must_use]
    pub fn schedule_not_found(schedule_id: Uuid) -> Self {
        Self::ScheduleNotFound { schedule_id }
    }

    /// Build an [`SchedulerError::Internal`] from a message.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::SchedulerError;

    #[test]
    fn invalid_trigger_carries_reason_in_message() {
        let error = SchedulerError::invalid_trigger("cron expression must not be empty");
        assert!(
            error
                .to_string()
                .contains("cron expression must not be empty")
        );
    }

    #[test]
    fn serialization_error_is_built_from_serde_json() {
        let serde_error = serde_json::from_slice::<serde_json::Value>(b"not json").unwrap_err();
        let error: SchedulerError = serde_error.into();
        assert!(matches!(error, SchedulerError::Serialization(_)));
    }

    #[test]
    fn database_error_preserves_source_chain() {
        let inner = std::io::Error::other("disk on fire");
        let error = SchedulerError::database(inner);
        let source = std::error::Error::source(&error).expect("source must be set");
        assert_eq!(source.to_string(), "disk on fire");
    }

    #[test]
    fn schedule_not_found_message_includes_the_identifier() {
        let schedule_id = uuid::Uuid::from_u128(42);
        let error = SchedulerError::schedule_not_found(schedule_id);
        assert!(error.to_string().contains(&schedule_id.to_string()));
        assert!(matches!(error, SchedulerError::ScheduleNotFound { .. }));
    }
}
