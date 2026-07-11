//! Shared PostgreSQL connection establishment for the `outbox` commands.

use std::str::FromStr;

use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::Config;
use tokio_postgres::NoTls;
use tokio_postgres::config::SslMode;

use crate::conn_string::ConnString;
use crate::error::CliError;

/// Connect to PostgreSQL, honoring the connection string's `sslmode`.
///
/// The URL is parsed into a [`tokio_postgres::Config`] so `sslmode` is
/// interpreted the way libpq defines it, instead of by a naive substring
/// search over the raw string (a password or an unrelated query
/// parameter that merely contains `sslmode=disable` used to be enough to
/// flip this decision).
///
/// Only an explicitly parsed `SslMode::Disable` selects a plaintext
/// [`NoTls`] connection. Every other value, including the driver's own
/// default of `prefer` when the URL omits `sslmode` entirely, is
/// upgraded to `SslMode::Require` before connecting: a server that
/// declines TLS then fails the connect attempt instead of silently
/// falling back to a cleartext session, which is the downgrade a
/// network-position attacker could otherwise force unnoticed.
///
/// # Errors
///
/// Returns [`CliError::Fatal`] if the URL cannot be parsed, the TLS
/// connector cannot be built, or the connect attempt itself fails. The
/// error message never contains the connection string, only its
/// credential-redacted form: the raw `tokio_postgres` error is
/// deliberately not chained as the source, because a config-parse
/// failure can embed the offending connection string (password
/// included) in its cause chain.
pub(crate) async fn connect(conn: &ConnString) -> Result<tokio_postgres::Client, CliError> {
    let mut config = Config::from_str(conn.as_str()).map_err(|_err| connect_error(conn))?;

    if config.get_ssl_mode() == SslMode::Disable {
        tracing::warn!(
            conn = %conn.redacted(),
            "TLS disabled via sslmode=disable; credentials will be sent in cleartext"
        );
        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|_err| connect_error(conn))?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "PostgreSQL connection task error");
            }
        });
        return Ok(client);
    }

    config.ssl_mode(SslMode::Require);
    let builder = native_tls::TlsConnector::builder()
        .build()
        .map_err(|e| CliError::Fatal(Box::new(e)))?;
    let connector = MakeTlsConnector::new(builder);
    let (client, connection) = config
        .connect(connector)
        .await
        .map_err(|_err| connect_error(conn))?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::error!(error = %err, "PostgreSQL connection task error");
        }
    });
    Ok(client)
}

/// Build a credential-safe fatal error for a failed PostgreSQL connect.
fn connect_error(conn: &ConnString) -> CliError {
    CliError::Fatal(format!("failed to connect to PostgreSQL at {}", conn.redacted()).into())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use tokio_postgres::Config;
    use tokio_postgres::config::SslMode;

    #[test]
    fn default_url_without_sslmode_parses_as_prefer_before_being_upgraded() {
        // Documents the driver default this module must not trust as-is:
        // `connect` upgrades this to `Require` rather than connecting
        // with it verbatim.
        let config = Config::from_str("postgres://user:pass@host/db").expect("must parse");
        assert_eq!(config.get_ssl_mode(), SslMode::Prefer);
    }

    #[test]
    fn explicit_disable_parses_as_disable() {
        let config =
            Config::from_str("postgres://user:pass@host/db?sslmode=disable").expect("must parse");
        assert_eq!(config.get_ssl_mode(), SslMode::Disable);
    }

    #[test]
    fn explicit_require_parses_as_require() {
        let config =
            Config::from_str("postgres://user:pass@host/db?sslmode=require").expect("must parse");
        assert_eq!(config.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn sslmode_substring_inside_the_password_does_not_disable_tls() {
        // Regression for #365: a password that merely contains the text
        // "sslmode=disable" (e.g. chosen by an attacker, or coincidentally
        // by a user) must not be mistaken for the query parameter.
        let config =
            Config::from_str("postgres://user:sslmode=disable@host/db").expect("must parse");
        assert_ne!(
            config.get_ssl_mode(),
            SslMode::Disable,
            "a password containing the substring must not disable TLS"
        );
    }
}
