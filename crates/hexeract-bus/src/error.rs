use thiserror::Error;

/// Errors raised by the bus primitives, transports and workers.
///
/// Marked `#[non_exhaustive]` so new variants can be added without a
/// breaking change.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BusError {
    /// The message payload could not be serialized or deserialized as JSON.
    #[error("failed to (de)serialize message payload as JSON")]
    Serialization(#[from] serde_json::Error),

    /// The transport layer reported a publish or consume failure.
    ///
    /// The original error is preserved as a boxed source so callers can
    /// downcast if they need typed access to the underlying driver error.
    #[error("transport error")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The transport could not establish or maintain a connection to the broker.
    #[error("connection error")]
    Connection(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The worker consumed an envelope whose `message_type` has no registered handler.
    #[error("no handler registered for message type `{message_type}`")]
    MissingHandler {
        /// The unrouted message type read from the envelope.
        message_type: String,
    },

    /// An envelope was decoded into the wrong message type.
    ///
    /// Returned when a caller invokes [`crate::BusEnvelope::decode`]
    /// with a type whose [`crate::Message::MESSAGE_TYPE`] does not match
    /// the envelope's `message_type` field.
    #[error("envelope carries message_type `{actual}` but decode requested `{expected}`")]
    TypeMismatch {
        /// Message type requested by the caller (`M::MESSAGE_TYPE`).
        expected: &'static str,
        /// Message type actually stored in the envelope.
        actual: String,
    },

    /// A topology declaration (exchange, queue, binding or routing key)
    /// failed validation.
    #[error("invalid topology: {reason}")]
    InvalidTopology {
        /// Human-readable explanation of the rejection.
        reason: String,
    },

    /// An invariant of the bus machinery was violated.
    ///
    /// Signals a bug in the framework itself, not a recoverable error.
    /// Report occurrences upstream.
    #[error("internal bus error: {0}")]
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
        let error: BusError = serde_error.into();
        assert!(matches!(error, BusError::Serialization(_)));
    }

    #[test]
    fn transport_error_preserves_source_chain() {
        let inner = std::io::Error::other("broker exploded");
        let error = BusError::Transport(Box::new(inner));
        let source = std::error::Error::source(&error).expect("source must be set");
        assert_eq!(source.to_string(), "broker exploded");
    }

    #[test]
    fn connection_error_preserves_source_chain() {
        let inner = std::io::Error::other("amqp handshake failed");
        let error = BusError::Connection(Box::new(inner));
        let source = std::error::Error::source(&error).expect("source must be set");
        assert_eq!(source.to_string(), "amqp handshake failed");
    }

    #[test]
    fn missing_handler_message_includes_message_type() {
        let error = BusError::MissingHandler {
            message_type: "orders.placed".to_owned(),
        };
        assert!(error.to_string().contains("orders.placed"));
    }

    #[test]
    fn invalid_topology_message_includes_reason() {
        let error = BusError::InvalidTopology {
            reason: "exchange name cannot be empty".to_owned(),
        };
        assert!(error.to_string().contains("exchange name cannot be empty"));
    }

    #[test]
    fn type_mismatch_message_includes_expected_and_actual() {
        let error = BusError::TypeMismatch {
            expected: "users.registered",
            actual: "orders.placed".to_owned(),
        };
        let message = error.to_string();
        assert!(message.contains("users.registered"));
        assert!(message.contains("orders.placed"));
    }
}
