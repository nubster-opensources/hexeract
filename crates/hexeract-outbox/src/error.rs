use std::time::Duration;

use thiserror::Error;
use uuid::Uuid;

/// Errors raised by the outbox primitives, publishers and workers.
///
/// Marked `#[non_exhaustive]` so new variants can be added without a
/// breaking change.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OutboxError {
    /// The event payload could not be serialized or deserialized as JSON.
    #[error("failed to (de)serialize event payload as JSON")]
    Serialization(#[from] serde_json::Error),

    /// The backend reported a database-level failure.
    ///
    /// The original error is preserved as a boxed source so callers can
    /// downcast if they need typed access to the underlying driver error.
    #[error("database error")]
    Database(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The worker polled an envelope whose `event_type` has no registered handler.
    #[error("no handler registered for event type `{event_type}`")]
    MissingHandler {
        /// The unrouted event type read from the envelope.
        event_type: String,
    },

    /// The envelope was retried more times than the configured maximum.
    #[error("event {event_id} reached max retries after {attempts} attempts")]
    MaxRetries {
        /// Identifier of the event that exhausted its retry budget.
        event_id: Uuid,
        /// Number of attempts already consumed.
        attempts: u32,
    },

    /// An envelope was decoded into the wrong event type.
    ///
    /// Returned when a caller invokes [`crate::OutboxEnvelope::decode`]
    /// with a type whose [`crate::Event::EVENT_TYPE`] does not match the
    /// envelope's `event_type` field. Typically the sign of a
    /// router or registry misconfiguration on the caller side.
    #[error("envelope carries event_type `{actual}` but decode requested `{expected}`")]
    TypeMismatch {
        /// Event type requested by the caller (`E::EVENT_TYPE`).
        expected: &'static str,
        /// Event type actually stored in the envelope.
        actual: String,
    },

    /// The connection pool did not yield a connection within the configured
    /// timeout.
    ///
    /// This is a transient condition: the pool is under pressure but the
    /// database itself may be healthy. The outbox worker retries automatically
    /// after [`OutboxWorkerConfig::poll_interval`]. Application code that
    /// observes this variant can implement back-pressure or circuit-breaking.
    ///
    /// To prevent indefinite blocking, configure an acquire timeout on the
    /// pool (e.g. `sqlx::pool::PoolOptions::acquire_timeout`).
    ///
    /// [`OutboxWorkerConfig::poll_interval`]: crate::OutboxWorkerConfig::poll_interval
    #[error("connection pool acquire timed out")]
    PoolTimeout,

    /// The handler did not complete within
    /// [`OutboxWorkerConfig::dispatch_timeout`].
    ///
    /// The worker enforces `dispatch_timeout` as a hard deadline around each
    /// handler invocation. When it elapses the dispatch is treated as a failed
    /// attempt (recorded via [`crate::OutboxStore::mark_failed`] and retried or
    /// dead-lettered like any other error) and the handler's cancellation token
    /// is signalled so cooperative handlers can unwind.
    ///
    /// [`OutboxWorkerConfig::dispatch_timeout`]: crate::OutboxWorkerConfig::dispatch_timeout
    #[error("handler for event {event_id} ({event_type}) timed out after {timeout:?}")]
    DispatchTimeout {
        /// Identifier of the event whose handler timed out.
        event_id: Uuid,
        /// The unrouted event type read from the envelope.
        event_type: String,
        /// The configured dispatch timeout that elapsed.
        timeout: Duration,
    },

    /// An invariant of the outbox machinery was violated.
    ///
    /// Signals a bug in the framework itself, not a recoverable error.
    /// Report occurrences upstream.
    #[error("internal outbox error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_error_is_built_from_serde_json() {
        let invalid_json = b"not json";
        let serde_error: serde_json::Error =
            serde_json::from_slice::<serde_json::Value>(invalid_json).unwrap_err();
        let error: OutboxError = serde_error.into();
        assert!(matches!(error, OutboxError::Serialization(_)));
    }

    #[test]
    fn database_error_preserves_source_chain() {
        let inner = std::io::Error::other("disk on fire");
        let error = OutboxError::Database(Box::new(inner));
        let source = std::error::Error::source(&error).expect("source must be set");
        assert_eq!(source.to_string(), "disk on fire");
    }

    #[test]
    fn missing_handler_message_includes_event_type() {
        let error = OutboxError::MissingHandler {
            event_type: "users.registered".to_owned(),
        };
        assert!(error.to_string().contains("users.registered"));
    }

    #[test]
    fn max_retries_message_includes_event_id_and_count() {
        let event_id = Uuid::from_u128(7);
        let error = OutboxError::MaxRetries {
            event_id,
            attempts: 5,
        };
        let message = error.to_string();
        assert!(message.contains(&event_id.to_string()));
        assert!(message.contains('5'));
    }

    #[test]
    fn pool_timeout_has_descriptive_message() {
        let error = OutboxError::PoolTimeout;
        assert!(error.to_string().contains("timed out"));
    }
}
