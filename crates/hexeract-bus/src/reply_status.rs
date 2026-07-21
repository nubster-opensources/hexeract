use serde::{Deserialize, Serialize};

/// Header carrying the reply status on a request-reply response envelope.
pub const REPLY_STATUS_HEADER: &str = "x-hexeract-reply-status";
/// Value of [`REPLY_STATUS_HEADER`] for a successful reply.
pub const REPLY_STATUS_OK: &str = "ok";
/// Value of [`REPLY_STATUS_HEADER`] for a failed reply.
pub const REPLY_STATUS_ERROR: &str = "error";
/// Sentinel `message_type` stamped on an error reply envelope.
pub const REPLY_ERROR_MESSAGE_TYPE: &str = "hexeract.reply.error";

/// Wire payload of a failed reply.
///
/// This is a protocol type, deliberately NOT a [`crate::Message`]: a remote
/// fault is not a domain message. It travels in the envelope `payload` (so it
/// is masked by [`crate::BusEnvelope`]'s `Debug`) and is decoded directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteErrorPayload {
    /// Stable-ish category of the failure (a `BusError` variant name).
    pub error_type: String,
    /// Human-readable failure message.
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_error_payload_round_trips_as_json() {
        let payload = RemoteErrorPayload {
            error_type: "Internal".to_owned(),
            message: "store unavailable".to_owned(),
        };
        let bytes = serde_json::to_vec(&payload).unwrap();
        let back: RemoteErrorPayload = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, payload);
    }
}
