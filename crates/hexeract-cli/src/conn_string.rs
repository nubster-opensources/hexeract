//! Redacting newtype for connection strings supplied on the command line.

use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

use hexeract_bus_rabbitmq::redact_uri;

/// Placeholder rendered instead of the raw connection string.
const REDACTED: &str = "<redacted>";

/// A connection string (AMQP or PostgreSQL) supplied via `--conn` or its
/// backing environment variable.
///
/// The raw value embeds broker or database credentials in its userinfo
/// component (`scheme://user:password@host/...`). [`ConnString`] never
/// renders that value through [`fmt::Debug`] or [`fmt::Display`]: both
/// print the fixed string `<redacted>`, so a stray `{:?}` on the parsed
/// CLI arguments (clap derives `Debug` on every `Args` struct) cannot
/// leak the password, and neither can an accidental `{}` interpolation
/// into a log line.
///
/// Prefer sourcing the value from an environment variable (or, for
/// PostgreSQL, a `.pgpass` file) rather than passing it literally on the
/// command line: argv is visible to every local user via `/proc/<pid>/cmdline`
/// or `ps aux`, and most shells persist it in history.
///
/// Use [`ConnString::as_str`] to obtain the raw value only at the point
/// where it is handed to the connection driver, and [`ConnString::redacted`]
/// to build a credential-safe string for logs and error messages.
#[derive(Clone)]
pub(crate) struct ConnString(String);

impl ConnString {
    /// Borrow the raw connection string.
    ///
    /// Callers must pass the result straight to the driver that
    /// consumes it and must not log, print or otherwise echo it.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Render a credential-redacted form suitable for logs and error
    /// messages, keeping only the scheme and host for diagnosis.
    #[must_use]
    pub(crate) fn redacted(&self) -> String {
        redact_uri(&self.0)
    }
}

impl FromStr for ConnString {
    type Err = Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Debug for ConnString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl fmt::Display for ConnString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_renders_the_secret() {
        let conn: ConnString = "postgres://user:hunter2@host/db".parse().unwrap();
        let rendered = format!("{conn:?}");
        assert_eq!(rendered, REDACTED);
        assert!(!rendered.contains("hunter2"));
    }

    #[test]
    fn display_never_renders_the_secret() {
        let conn: ConnString = "amqp://user:hunter2@host/vhost".parse().unwrap();
        let rendered = format!("{conn}");
        assert_eq!(rendered, REDACTED);
        assert!(!rendered.contains("hunter2"));
    }

    #[test]
    fn redacted_keeps_the_host_but_drops_the_credentials() {
        let conn: ConnString = "postgres://user:hunter2@db.example.com:5432/app"
            .parse()
            .unwrap();
        let redacted = conn.redacted();
        assert!(redacted.contains("db.example.com:5432/app"));
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("user"));
    }

    #[test]
    fn as_str_returns_the_raw_value_for_the_driver() {
        let conn: ConnString = "amqp://localhost:5672".parse().unwrap();
        assert_eq!(conn.as_str(), "amqp://localhost:5672");
    }
}
