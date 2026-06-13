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
}

impl SchedulerError {
    /// Build an [`SchedulerError::InvalidTrigger`] from a reason.
    #[must_use]
    pub fn invalid_trigger(reason: impl Into<String>) -> Self {
        Self::InvalidTrigger {
            reason: reason.into(),
        }
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
}
