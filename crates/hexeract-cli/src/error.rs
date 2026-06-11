//! Typed CLI error that carries a process exit code.
//!
//! Using a typed error instead of `std::process::exit` inside async paths
//! ensures all destructors run before the process terminates, and gives the
//! binary a single place to translate domain errors into exit codes.

use std::fmt;

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
