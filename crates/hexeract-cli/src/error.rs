//! Typed CLI error that carries a process exit code.
//!
//! Using a typed error instead of `std::process::exit` inside async paths
//! ensures all destructors run before the process terminates, and gives the
//! binary a single place to translate domain errors into exit codes.

use std::fmt;

use crate::terminal_text::escape_unsafe_characters;

/// A CLI-level error with an associated exit code.
///
/// All command `run` methods return `Result<(), CliError>`. The binary's
/// `main` inspects the variant and calls `std::process::exit` once the
/// async runtime has fully shut down.
#[derive(Debug)]
pub(crate) enum CliError {
    /// A required safety flag was absent. The error message has already been
    /// printed to stderr. Produces exit code **2**.
    SafetyFlagMissing(String),
    /// Any other failure (connection error, missing table, ...).
    /// Produces exit code **1**.
    Fatal(Box<dyn std::error::Error>),
}

impl CliError {
    /// Render this error with unsafe terminal characters escaped.
    pub(crate) fn terminal_message(&self) -> String {
        escape_unsafe_characters(&self.to_string()).into_owned()
    }

    /// Process exit code that corresponds to this error variant.
    pub(crate) fn exit_code(&self) -> i32 {
        match self {
            Self::SafetyFlagMissing(_) => 2,
            Self::Fatal(_) => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SafetyFlagMissing(msg) => write!(f, "{msg}"),
            Self::Fatal(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Fatal(err) => Some(err.as_ref()),
            Self::SafetyFlagMissing(_) => None,
        }
    }
}

impl From<Box<dyn std::error::Error>> for CliError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Self::Fatal(err)
    }
}

impl From<&str> for CliError {
    fn from(s: &str) -> Self {
        Self::Fatal(s.into())
    }
}

impl From<String> for CliError {
    fn from(s: String) -> Self {
        Self::Fatal(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::CliError;

    #[test]
    fn terminal_message_escapes_terminal_sequences() {
        let error = CliError::from("broker: \u{1b}[31mfailed\u{1b}[0m");
        assert_eq!(
            error.terminal_message(),
            "broker: \\u{1b}[31mfailed\\u{1b}[0m"
        );
    }

    #[test]
    fn terminal_message_escapes_line_breaks() {
        let error = CliError::from("failed\nforged success\r\t");
        assert_eq!(error.terminal_message(), "failed\\nforged success\\r\\t");
    }

    #[test]
    fn terminal_message_escapes_invisible_formatting() {
        let error = CliError::from("queue\u{202e}\u{200b}\u{2066}\u{feff}");
        assert_eq!(
            error.terminal_message(),
            "queue\\u{202e}\\u{200b}\\u{2066}\\u{feff}"
        );
    }

    #[test]
    fn terminal_message_distinguishes_literal_escape_sequences() {
        let error = CliError::from("literal \\n, newline \n");
        assert_eq!(error.terminal_message(), "literal \\\\n, newline \\n");
    }

    #[test]
    fn terminal_message_preserves_readable_unicode() {
        let message = "Échec de connexion : café e\u{301} 👩\u{200d}💻 ❤️";
        assert_eq!(CliError::from(message).terminal_message(), message);
    }

    #[test]
    fn terminal_message_preserves_original_error_and_exit_code() {
        let original = "broker\nfailed";
        let error = CliError::from(original);
        assert_eq!(error.terminal_message(), "broker\\nfailed");
        assert_eq!(error.to_string(), original);
        assert_eq!(
            std::error::Error::source(&error).unwrap().to_string(),
            original
        );
        assert_eq!(error.exit_code(), 1);
    }

    #[test]
    fn terminal_message_escapes_safety_errors_without_changing_exit_code() {
        let original = "queue\nrequires confirmation";
        let error = CliError::SafetyFlagMissing(original.into());
        assert_eq!(error.terminal_message(), "queue\\nrequires confirmation");
        assert_eq!(error.to_string(), original);
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(error.exit_code(), 2);
    }
}
