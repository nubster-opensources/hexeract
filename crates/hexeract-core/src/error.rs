use std::time::Duration;
use thiserror::Error;

/// Top-level error type for the Hexeract framework.
///
/// This enum is marked `#[non_exhaustive]` so that new variants can be added
/// in minor versions without breaking downstream `match` arms.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HexeractError {
    /// No handler was registered for the given message type.
    #[error("no handler registered for `{message_type}`")]
    #[non_exhaustive]
    HandlerNotFound {
        /// The fully-qualified type name of the unregistered command, query or
        /// notification.
        message_type: &'static str,
    },

    /// A handler returned an error. The original error is preserved as source.
    #[error("handler failed: {source}")]
    #[non_exhaustive]
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

    /// A dispatch produced a value that could not be downcast to the expected
    /// output type.
    ///
    /// This indicates a short-circuiting [`Middleware`](crate::middleware::Middleware)
    /// boxed a value whose type is not the message's `Output`. A correct
    /// short-circuit must box exactly the dispatched message's output type.
    #[error("dispatch produced a value that is not the expected output type `{expected}`")]
    #[non_exhaustive]
    DowncastFailed {
        /// Fully-qualified name of the output type the dispatch expected.
        expected: &'static str,
    },

    /// A dispatch was cancelled before the handler produced a result because
    /// its [`HandlerContext`](crate::HandlerContext) cancellation token fired.
    ///
    /// The dispatch pipeline observes the token before each middleware and
    /// before the terminal handler, so a middleware that cancels the token
    /// short-circuits the rest of the chain. Handlers and middlewares may
    /// also raise this variant themselves via [`HexeractError::cancelled`].
    #[error("dispatch of `{type_name}` was cancelled")]
    #[non_exhaustive]
    Cancelled {
        /// Fully-qualified type name of the message whose dispatch was cancelled.
        type_name: &'static str,
    },

    /// One or more notification handlers failed during a `publish` fan-out.
    ///
    /// Every handler runs regardless of its siblings; the failures are
    /// collected here in registration order, each retaining the handler's
    /// typed error and its `source` chain. Prefer matching this variant and
    /// inspecting [`NotificationFailure`] over parsing the message when a
    /// caller needs to recover an individual handler's error.
    #[error("publish: {} of {total} handlers failed: {}", failures.len(), render_publish_failures(failures))]
    #[non_exhaustive]
    PublishFailed {
        /// Fully-qualified type name of the published notification.
        notification_type: &'static str,
        /// Total number of handlers the notification fanned out to.
        total: usize,
        /// Per-handler failures, in registration order.
        failures: Vec<NotificationFailure>,
    },

    /// A generic dispatch-level error with a human-readable message.
    ///
    /// Reserved as a last resort for cases that have no dedicated structured
    /// variant, such as aggregating several notification handler failures into
    /// one message, or reporting a framework invariant violation. Prefer a
    /// specific variant ([`HandlerNotFound`](Self::HandlerNotFound),
    /// [`DowncastFailed`](Self::DowncastFailed), [`Cancelled`](Self::Cancelled),
    /// ...) whenever one applies.
    #[error("dispatch error: {0}")]
    Dispatch(String),
}

/// One notification handler that failed during a notification `publish`
/// fan-out, paired with the typed error it returned.
///
/// The [`error`](Self::error) field keeps the full [`HexeractError`], so its
/// `source` chain stays intact for callers that need to recover the original
/// failure rather than a flattened string. Values are exposed through
/// [`HexeractError::PublishFailed`].
#[derive(Debug)]
pub struct NotificationFailure {
    /// Fully-qualified type name of the handler that failed.
    pub handler: &'static str,
    /// Typed error the handler returned, with its `source` chain intact.
    pub error: HexeractError,
}

/// Renders aggregated notification failures as `handler: error` segments
/// joined with `; `, used by the [`HexeractError::PublishFailed`] `Display`.
fn render_publish_failures(failures: &[NotificationFailure]) -> String {
    failures
        .iter()
        .map(|failure| format!("{}: {}", failure.handler, failure.error))
        .collect::<Vec<_>>()
        .join("; ")
}

impl HexeractError {
    /// Builds a [`HexeractError::HandlerNotFound`] from the fully-qualified
    /// type name of the unregistered message. This is the only way to
    /// construct the variant from outside this crate, since it is marked
    /// `#[non_exhaustive]`.
    #[must_use]
    pub fn handler_not_found(message_type: &'static str) -> Self {
        Self::HandlerNotFound { message_type }
    }

    /// Wraps any `Send + Sync` error as a [`HexeractError::HandlerFailed`].
    pub fn handler_failed(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::HandlerFailed {
            source: Box::new(source),
        }
    }

    /// Builds a [`HexeractError::Timeout`] from the dispatched message type
    /// name and the timeout that was exceeded. This is the only way to
    /// construct the variant from outside this crate, since it is marked
    /// `#[non_exhaustive]`.
    #[must_use]
    pub fn timeout(type_name: &'static str, duration: Duration) -> Self {
        Self::Timeout {
            type_name,
            duration,
        }
    }

    /// Builds a [`HexeractError::DowncastFailed`] from the fully-qualified name
    /// of the output type the dispatch expected. This is the only way to
    /// construct the variant from outside this crate, since it is marked
    /// `#[non_exhaustive]`.
    #[must_use]
    pub fn downcast_failed(expected: &'static str) -> Self {
        Self::DowncastFailed { expected }
    }

    /// Builds a [`HexeractError::Cancelled`] from the fully-qualified name of
    /// the message whose dispatch was cancelled. This is the only way to
    /// construct the variant from outside this crate, since it is marked
    /// `#[non_exhaustive]`.
    #[must_use]
    pub fn cancelled(type_name: &'static str) -> Self {
        Self::Cancelled { type_name }
    }

    /// Builds a [`HexeractError::PublishFailed`] from the published
    /// notification's type name, the total number of handlers it fanned out
    /// to, and the per-handler failures collected in registration order. This
    /// is the only way to construct the variant from outside this crate, since
    /// it is marked `#[non_exhaustive]`.
    #[must_use]
    pub fn publish_failed(
        notification_type: &'static str,
        total: usize,
        failures: Vec<NotificationFailure>,
    ) -> Self {
        Self::PublishFailed {
            notification_type,
            total,
            failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_not_found_display() {
        let err = HexeractError::HandlerNotFound {
            message_type: "RegisterUser",
        };
        assert_eq!(err.to_string(), "no handler registered for `RegisterUser`");
    }

    #[test]
    fn cancelled_names_the_message_type() {
        let err = HexeractError::cancelled("my::RegisterUser");
        let rendered = err.to_string();
        assert!(rendered.contains("RegisterUser"));
        assert!(rendered.contains("cancelled"));
        assert!(
            matches!(err, HexeractError::Cancelled { type_name } if type_name == "my::RegisterUser")
        );
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

    #[test]
    fn handler_not_found_names_the_message_type() {
        let err = HexeractError::handler_not_found("my::RegisterUser");
        assert!(
            matches!(err, HexeractError::HandlerNotFound { message_type } if message_type == "my::RegisterUser")
        );
        assert!(err.to_string().contains("RegisterUser"));
    }

    #[test]
    fn downcast_failed_names_the_expected_output_type() {
        let err = HexeractError::downcast_failed("u32");
        let rendered = err.to_string();
        assert!(rendered.contains("u32"));
        assert!(matches!(err, HexeractError::DowncastFailed { expected } if expected == "u32"));
    }

    #[test]
    fn publish_failed_aggregates_typed_handler_errors() {
        let failures = vec![
            NotificationFailure {
                handler: "my::AuditHandler",
                error: HexeractError::cancelled("my::UserCreated"),
            },
            NotificationFailure {
                handler: "my::EmailHandler",
                error: HexeractError::Dispatch("smtp down".into()),
            },
        ];
        let err = HexeractError::publish_failed("my::UserCreated", 3, failures);
        let HexeractError::PublishFailed {
            notification_type,
            total,
            failures,
        } = err
        else {
            panic!("expected PublishFailed variant");
        };
        assert_eq!(notification_type, "my::UserCreated");
        assert_eq!(total, 3);
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].handler, "my::AuditHandler");
        assert!(matches!(failures[1].error, HexeractError::Dispatch(_)));
    }

    #[test]
    fn publish_failed_display_summarizes_count_and_handlers() {
        let failures = vec![NotificationFailure {
            handler: "my::AuditHandler",
            error: HexeractError::Dispatch("boom".into()),
        }];
        let err = HexeractError::publish_failed("my::UserCreated", 3, failures);
        let rendered = err.to_string();
        assert!(rendered.starts_with("publish: 1 of 3 handlers failed"));
        assert!(rendered.contains("my::AuditHandler"));
        assert!(rendered.contains("boom"));
    }

    #[test]
    fn publish_failed_preserves_underlying_handler_source() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let failures = vec![NotificationFailure {
            handler: "my::PersistHandler",
            error: HexeractError::handler_failed(io),
        }];
        let err = HexeractError::publish_failed("my::UserCreated", 1, failures);
        let HexeractError::PublishFailed { failures, .. } = err else {
            panic!("expected PublishFailed variant");
        };
        assert!(std::error::Error::source(&failures[0].error).is_some());
    }
}
