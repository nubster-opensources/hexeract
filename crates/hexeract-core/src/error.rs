use std::time::Duration;
use thiserror::Error;

/// Top-level error type for the Hexeract framework.
///
/// This enum is marked `#[non_exhaustive]` so that new variants can be added
/// in minor versions without breaking downstream `match` arms.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HexeractError {
    /// No handler was registered for the given command or query type.
    #[error("no handler registered for `{command_type}`")]
    HandlerNotFound {
        /// The fully-qualified type name of the unregistered command or query.
        command_type: &'static str,
    },

    /// A handler returned an error. The original error is preserved as source.
    #[error("handler failed: {source}")]
    HandlerFailed {
        /// The original error returned by the handler.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A dispatch exceeded its configured deadline.
    #[error("dispatch of `{type_name}` timed out after {duration:?}")]
    #[non_exhaustive]
    Timeout {
        /// Fully-qualified type name of the message being dispatched.
        type_name: &'static str,
        /// Configured timeout that was exceeded.
        duration: Duration,
    },

    /// A generic dispatch-level error with a human-readable message.
    #[error("dispatch error: {0}")]
    Dispatch(String),
}

impl HexeractError {
    /// Wraps any `Send + Sync` error as a [`HexeractError::HandlerFailed`].
    pub fn handler_failed(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::HandlerFailed {
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_not_found_display() {
        let err = HexeractError::HandlerNotFound {
            command_type: "RegisterUser",
        };
        assert_eq!(err.to_string(), "no handler registered for `RegisterUser`");
    }

    #[test]
    fn timeout_display_shows_type_name_and_duration() {
        let err = HexeractError::Timeout {
            type_name: "my::RegisterUser",
            duration: Duration::from_secs(5),
        };
        let rendered = err.to_string();
        assert!(rendered.contains("RegisterUser"));
        assert!(rendered.contains("5s"));
    }

    #[test]
    fn handler_failed_preserves_source() {
        let original = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = HexeractError::handler_failed(original);
        assert!(err.to_string().contains("handler failed"));
        assert!(std::error::Error::source(&err).is_some());
    }
}
