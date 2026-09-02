use thiserror::Error;

/// Metadata dimension subject to a transport limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataLimit {
    /// Number of headers carried by a message.
    HeaderCount,
    /// Byte length of one header key.
    KeyBytes,
    /// Byte length of one header value.
    ValueBytes,
    /// Total byte length of all header keys and values.
    TotalBytes,
}

impl std::fmt::Display for MetadataLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::HeaderCount => "header count",
            Self::KeyBytes => "key bytes",
            Self::ValueBytes => "value bytes",
            Self::TotalBytes => "total bytes",
        };
        f.write_str(reason)
    }
}

/// Reason a transport-provided metadata value was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidMetadataReason {
    /// An AMQP long string could not be decoded as UTF-8.
    NonUtf8LongString,
    /// A reserved protocol header was not transmitted in canonical form.
    NonCanonicalReservedHeader,
}

impl std::fmt::Display for InvalidMetadataReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::NonUtf8LongString => "non-utf8 long string",
            Self::NonCanonicalReservedHeader => "non-canonical reserved header",
        };
        f.write_str(reason)
    }
}

/// Errors raised by the bus primitives, transports and workers.
///
/// Marked `#[non_exhaustive]` so new variants can be added without a
/// breaking change.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BusError {
    /// An application attempted to set a framework-reserved header.
    #[error("application header uses the reserved x-hexeract-* namespace")]
    ReservedHeaderNamespace,

    /// Metadata exceeded a configured transport limit.
    #[error("metadata {limit} limit exceeded: observed {actual}, maximum {max}")]
    MetadataLimitExceeded {
        /// Metadata dimension whose limit was exceeded.
        limit: MetadataLimit,
        /// Measured size of the metadata.
        actual: usize,
        /// Maximum permitted size of the metadata.
        max: usize,
    },

    /// Metadata could not be represented safely by the transport.
    #[error("invalid metadata: {reason}")]
    InvalidMetadata {
        /// Stable category of invalid metadata.
        reason: InvalidMetadataReason,
    },

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
    ///
    /// `retryable` records whether the failure is transient (a retry or an
    /// automatic reconnection may succeed) or permanent (bad credentials,
    /// an unsupported protocol version): a permanent failure must not be
    /// hammered by a reconnect loop.
    #[error("connection error")]
    Connection {
        /// The underlying driver error, preserved as a boxed source.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// Whether the failure is transient and worth retrying.
        retryable: bool,
    },

    /// A mandatory publish was returned by the broker as unroutable.
    ///
    /// The broker accepted the message but found no queue bound to the
    /// routing key, so the message was returned instead of being
    /// enqueued. Raised by transport backends that publish with
    /// publisher confirms enabled; declare the missing queue or
    /// binding, or fix the routing key, before retrying.
    #[error(
        "publish to routing key `{routing_key}` returned as unroutable: {reply_text} (code {reply_code})"
    )]
    Unroutable {
        /// Routing key the publish targeted.
        routing_key: String,
        /// Human-readable reply text sent by the broker.
        reply_text: String,
        /// AMQP reply code sent by the broker (typically `312`).
        reply_code: u16,
    },

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

    /// A consumed payload exceeds the transport's configured size limit.
    ///
    /// Returned by transport backends before the payload is copied or
    /// deserialized, so an oversize delivery from an untrusted producer
    /// bounds the consumer's memory and CPU instead of exhausting them.
    #[error("payload of {size} bytes exceeds the configured limit of {max} bytes")]
    PayloadTooLarge {
        /// Size of the rejected payload in bytes.
        size: usize,
        /// Configured maximum payload size in bytes.
        max: usize,
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

impl BusError {
    /// Build a [`BusError::Connection`] from a boxed source and a
    /// transience hint.
    ///
    /// `retryable` is `true` when the failure is transient (a retry or an
    /// automatic reconnection may succeed) and `false` when it is permanent
    /// (bad credentials, an unsupported protocol version).
    pub fn connection(
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
        retryable: bool,
    ) -> Self {
        Self::Connection {
            source: source.into(),
            retryable,
        }
    }

    /// Whether a [`BusError::Connection`] was classified as transient.
    ///
    /// Returns `None` for every other variant, so a caller can distinguish
    /// "not a connection error" from "a connection error that is permanent".
    #[must_use]
    pub fn is_retryable_connection(&self) -> Option<bool> {
        match self {
            Self::Connection { retryable, .. } => Some(*retryable),
            _ => None,
        }
    }
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
        let error = BusError::connection(inner, true);
        let source = std::error::Error::source(&error).expect("source must be set");
        assert_eq!(source.to_string(), "amqp handshake failed");
    }

    #[test]
    fn connection_carries_retryable_flag() {
        let permanent = BusError::connection(std::io::Error::other("access refused"), false);
        assert_eq!(permanent.is_retryable_connection(), Some(false));
        let transient = BusError::connection(std::io::Error::other("reset"), true);
        assert_eq!(transient.is_retryable_connection(), Some(true));
        assert_eq!(
            BusError::Internal("x".to_owned()).is_retryable_connection(),
            None
        );
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
    fn unroutable_message_includes_routing_key_and_broker_reply() {
        let error = BusError::Unroutable {
            routing_key: "orders.unknown".to_owned(),
            reply_text: "NO_ROUTE".to_owned(),
            reply_code: 312,
        };
        let message = error.to_string();
        assert!(message.contains("orders.unknown"));
        assert!(message.contains("NO_ROUTE"));
        assert!(message.contains("312"));
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

    #[test]
    fn metadata_limit_error_includes_only_limit_dimension_and_sizes() {
        let message = BusError::MetadataLimitExceeded {
            limit: MetadataLimit::KeyBytes,
            actual: 257,
            max: 256,
        }
        .to_string();

        assert!(message.contains("key bytes"));
        assert!(message.contains("257"));
        assert!(message.contains("256"));
        assert!(!message.contains("tenant-secret"));
    }

    #[test]
    fn invalid_metadata_error_includes_stable_reason_name() {
        let message = BusError::InvalidMetadata {
            reason: InvalidMetadataReason::NonUtf8LongString,
        }
        .to_string();

        assert!(message.contains("non-utf8 long string"));
        assert!(!message.contains("tenant-secret"));
    }
}
