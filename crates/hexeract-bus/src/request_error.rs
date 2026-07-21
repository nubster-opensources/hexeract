use std::time::Duration;

use crate::BusError;

/// Failure of a request-reply round trip observed by the caller.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RequestError {
    /// No reply arrived within the deadline.
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    /// The remote handler returned an error, decoded from the reply.
    #[error("remote handler failed [{error_type}]: {message}")]
    Remote {
        /// Category of the remote failure.
        error_type: String,
        /// Human-readable remote failure message.
        message: String,
    },
    /// The request could not be published or the reply channel was lost.
    #[error("transport failure")]
    Transport(#[source] BusError),
    /// The reply arrived but could not be decoded into the expected type.
    #[error("failed to decode reply")]
    Decode(#[source] BusError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_renders_duration() {
        let err = RequestError::Timeout(Duration::from_millis(250));
        assert!(err.to_string().contains("250ms"));
    }

    #[test]
    fn remote_renders_type_and_message() {
        let err = RequestError::Remote {
            error_type: "Internal".to_owned(),
            message: "boom".to_owned(),
        };
        assert!(err.to_string().contains("Internal"));
        assert!(err.to_string().contains("boom"));
    }
}
