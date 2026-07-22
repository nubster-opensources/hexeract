//! Sanitized failure payload published on the request-reply error channel.
//!
//! The protocol never carries a failure message. A responder publishes only
//! a category from a closed set plus the request identity, which is the key
//! to the full trace recorded on the responder side. The mapping below is
//! deliberately coarse: it collapses distinct internal causes under one
//! public label so no internal detail crosses the boundary.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BusError;

/// Public category of a remote failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteErrorType {
    /// The responder failed while handling the request.
    Internal,
    /// The request could not be decoded, or its payload was rejected.
    Malformed,
    /// A connection or transport failure occurred, or the request could
    /// not be routed to any queue.
    Unavailable,
    /// The announced protocol version is not supported by the responder.
    Unsupported,
    /// The request deadline had already passed. Reserved by this version.
    Expired,
}

impl RemoteErrorType {
    /// Collapse an internal [`BusError`] into its public category.
    #[must_use]
    pub fn from_bus_error(error: &BusError) -> Self {
        match error {
            BusError::Serialization(_)
            | BusError::TypeMismatch { .. }
            | BusError::PayloadTooLarge { .. } => Self::Malformed,
            BusError::Connection { .. } | BusError::Transport(_) | BusError::Unroutable { .. } => {
                Self::Unavailable
            }
            BusError::Internal(_)
            | BusError::MissingHandler { .. }
            | BusError::InvalidTopology { .. } => Self::Internal,
        }
    }
}

/// Wire payload of a failed reply.
///
/// A protocol type, deliberately not a [`crate::Message`]: a remote fault is
/// not a domain message. It travels in the envelope `payload` and carries no
/// free-form text, by design.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteErrorPayload {
    /// Public category of the failure.
    pub error_type: RemoteErrorType,
    /// Identity of the call this failure answers, for correlation with the
    /// responder-side trace. `Uuid::nil()` when the request carried none.
    pub request_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BusError;

    #[test]
    fn serializes_the_category_as_a_bare_name() {
        let payload = RemoteErrorPayload {
            error_type: RemoteErrorType::Internal,
            request_id: uuid::Uuid::nil(),
        };
        let json = serde_json::to_string(&payload).expect("payload must serialize");
        assert!(json.contains("\"error_type\":\"Internal\""), "got {json}");
    }

    #[test]
    fn transport_failures_map_to_unavailable() {
        let error = BusError::connection("broker unreachable", true);
        assert_eq!(
            RemoteErrorType::from_bus_error(&error),
            RemoteErrorType::Unavailable
        );
    }

    #[test]
    fn payload_failures_map_to_malformed() {
        let error = BusError::TypeMismatch {
            expected: "a",
            actual: "b".to_owned(),
        };
        assert_eq!(
            RemoteErrorType::from_bus_error(&error),
            RemoteErrorType::Malformed
        );
    }

    #[test]
    fn unclassified_failures_map_to_internal() {
        let error = BusError::Internal("anything at all".to_owned());
        assert_eq!(
            RemoteErrorType::from_bus_error(&error),
            RemoteErrorType::Internal
        );
    }
}
