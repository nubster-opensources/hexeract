use std::time::Duration;

use hexeract_core::RequestId;

use crate::BusError;
use crate::remote_error::RemoteErrorType;

/// A reply that does not honor the request-reply protocol.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolViolation {
    /// A required protocol header is absent or unparsable.
    #[error("reply is missing a usable {header} header")]
    MissingHeader {
        /// Name of the offending header.
        header: &'static str,
    },
    /// The reply announces a protocol version this crate does not implement.
    #[error("reply announces unsupported protocol version {version}")]
    UnsupportedVersion {
        /// Version announced by the peer.
        version: u32,
    },
    /// The reply carries a message type other than the expected one.
    #[error("reply has message type {actual}, expected {expected}")]
    UnexpectedReplyType {
        /// Message type the caller expected.
        expected: &'static str,
        /// Message type actually received.
        actual: String,
    },
}

/// Failure of a request-reply round trip observed by the caller.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RequestError {
    /// No reply arrived within the deadline.
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    /// The responder reported a failure.
    ///
    /// The category is deliberately coarse and carries no detail: the full
    /// trace lives on the responder side, indexed by `request_id`.
    #[error("remote responder failed [{error_type:?}] for request {request_id}")]
    Remote {
        /// Public category of the remote failure.
        error_type: RemoteErrorType,
        /// Identity of the call, to correlate with the responder trace.
        request_id: RequestId,
    },
    /// The reply does not honor the protocol.
    #[error("protocol violation")]
    Protocol(#[source] ProtocolViolation),
    /// The request could not be published or the reply channel was lost.
    #[error("transport failure")]
    Transport(#[source] BusError),
    /// The reply arrived and was well-formed, but its payload could not be
    /// decoded into the expected type.
    #[error("failed to decode reply")]
    Decode(#[source] BusError),
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn timeout_renders_duration() {
        let err = RequestError::Timeout(Duration::from_millis(250));
        assert!(err.to_string().contains("250ms"));
    }

    #[test]
    fn remote_renders_category_and_request_id() {
        let request_id = RequestId::new();
        let err = RequestError::Remote {
            error_type: RemoteErrorType::Internal,
            request_id,
        };
        assert!(err.to_string().contains("Internal"));
        assert!(err.to_string().contains(&request_id.to_string()));
    }

    #[test]
    fn protocol_violation_wraps_its_source() {
        let violation = ProtocolViolation::UnsupportedVersion { version: 7 };
        let err = RequestError::Protocol(violation.clone());
        let source = err.source().expect("source must be set");
        assert_eq!(source.to_string(), violation.to_string());
    }

    #[test]
    fn transport_preserves_source_chain() {
        let inner = std::io::Error::other("broker exploded");
        let bus_error = BusError::Transport(Box::new(inner));
        let err = RequestError::Transport(bus_error);
        let source = err.source().expect("source must be set");
        assert_eq!(source.to_string(), "transport error");
        let inner_source = source.source().expect("inner source must be set");
        assert_eq!(inner_source.to_string(), "broker exploded");
    }

    #[test]
    fn decode_preserves_source_chain() {
        let invalid_json = b"not json";
        let serde_error: serde_json::Error =
            serde_json::from_slice::<serde_json::Value>(invalid_json).unwrap_err();
        let bus_error: BusError = serde_error.into();
        let err = RequestError::Decode(bus_error);
        let source = err.source().expect("source must be set");
        assert_eq!(
            source.to_string(),
            "failed to (de)serialize message payload as JSON"
        );
    }
}
