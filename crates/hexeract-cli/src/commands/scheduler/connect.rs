//! TLS policy for the scheduler admin pools.

use std::str::FromStr;

use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{MySqlPool, PgPool, SqlitePool};

use crate::conn_string::ConnString;
use crate::error::CliError;

use super::open::DialectKind;

/// What the connection string asked of the transport, once its SSL mode is parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportChoice {
    /// TLS is enforced and the server certificate is verified.
    Enforced,
    /// Plaintext was explicitly requested and will be used.
    PlaintextRequested,
}

/// Parse a connection string into PostgreSQL options carrying the enforced mode.
///
/// Only an explicitly parsed [`PgSslMode::Disable`] is treated as an
/// opt-out. [`PgSslMode::VerifyCa`] and [`PgSslMode::VerifyFull`] are kept
/// as chosen: both already verify the server's certificate authority.
/// Every other mode (no `sslmode` at all, or an explicit `allow`, `prefer`
/// or `require`) is raised to `VerifyFull`, because `require` alone only
/// verifies the certificate when a root CA file happens to be present,
/// which is a weaker guarantee than the outbox commands provide.
///
/// The decision is read from [`PgConnectOptions::get_ssl_mode`], the mode
/// libpq itself parsed, never from a substring search over the raw URL.
fn postgres_options(conn: &ConnString) -> Result<(PgConnectOptions, TransportChoice), CliError> {
    let options = PgConnectOptions::from_str(conn.as_str())
        .map_err(|_err| connect_error(conn, DialectKind::Postgres))?;
    let (options, choice) = match options.get_ssl_mode() {
        PgSslMode::Disable => (options, TransportChoice::PlaintextRequested),
        PgSslMode::VerifyCa | PgSslMode::VerifyFull => (options, TransportChoice::Enforced),
        PgSslMode::Allow | PgSslMode::Prefer | PgSslMode::Require => (
            options.ssl_mode(PgSslMode::VerifyFull),
            TransportChoice::Enforced,
        ),
    };
    Ok((options, choice))
}

/// Parse a connection string into MySQL options carrying the enforced mode.
///
/// Mirrors [`postgres_options`]: only an explicitly parsed
/// [`MySqlSslMode::Disabled`] opts out, [`MySqlSslMode::VerifyCa`] and
/// [`MySqlSslMode::VerifyIdentity`] are kept as chosen, and every other
/// mode (absent, `preferred` or `required`) is raised to
/// `VerifyIdentity`, MySQL's strongest verification level.
fn mysql_options(conn: &ConnString) -> Result<(MySqlConnectOptions, TransportChoice), CliError> {
    let options = MySqlConnectOptions::from_str(conn.as_str())
        .map_err(|_err| connect_error(conn, DialectKind::MySql))?;
    let (options, choice) = match options.get_ssl_mode() {
        MySqlSslMode::Disabled => (options, TransportChoice::PlaintextRequested),
        MySqlSslMode::VerifyCa | MySqlSslMode::VerifyIdentity => {
            (options, TransportChoice::Enforced)
        }
        MySqlSslMode::Preferred | MySqlSslMode::Required => (
            options.ssl_mode(MySqlSslMode::VerifyIdentity),
            TransportChoice::Enforced,
        ),
    };
    Ok((options, choice))
}

/// Open a PostgreSQL pool, never downgrading to plaintext unless asked.
///
/// # Errors
///
/// Returns [`CliError::Fatal`] if the connection string cannot be parsed
/// or the pool fails to connect.
pub(crate) async fn postgres_pool(conn: &ConnString) -> Result<PgPool, CliError> {
    let (options, choice) = postgres_options(conn)?;
    if choice == TransportChoice::PlaintextRequested {
        warn_plaintext_selected(conn);
    }
    PgPool::connect_with(options)
        .await
        .map_err(|_err| connect_error(conn, DialectKind::Postgres))
}

/// Open a MySQL pool, never downgrading to plaintext unless asked.
///
/// # Errors
///
/// Returns [`CliError::Fatal`] if the connection string cannot be parsed
/// or the pool fails to connect.
pub(crate) async fn mysql_pool(conn: &ConnString) -> Result<MySqlPool, CliError> {
    let (options, choice) = mysql_options(conn)?;
    if choice == TransportChoice::PlaintextRequested {
        warn_plaintext_selected(conn);
    }
    MySqlPool::connect_with(options)
        .await
        .map_err(|_err| connect_error(conn, DialectKind::MySql))
}

/// Open a SQLite pool; a local file has no transport to protect, so no
/// TLS policy applies here.
///
/// # Errors
///
/// Returns [`CliError::Fatal`] if the pool fails to connect.
pub(crate) async fn sqlite_pool(conn: &ConnString) -> Result<SqlitePool, CliError> {
    SqlitePool::connect(conn.as_str())
        .await
        .map_err(|_err| connect_error(conn, DialectKind::Sqlite))
}

/// Build a fatal error naming the backend and the redacted target only.
///
/// The underlying `sqlx` error is deliberately not chained as the source:
/// a configuration-parse failure can embed the offending connection
/// string, password included, in its cause chain.
///
/// The target renders through [`ConnString::redacted`], which drops the
/// userinfo but keeps the scheme and host. An operator who cannot reach
/// a database needs to know which one was dialled; the credentials are
/// what must not appear, not the address. This matches what the outbox
/// commands already print, so both families of commands fail the same
/// way.
fn connect_error(conn: &ConnString, backend: DialectKind) -> CliError {
    let backend = match backend {
        DialectKind::Postgres => "PostgreSQL",
        DialectKind::MySql => "MySQL",
        DialectKind::Sqlite => "SQLite",
    };
    CliError::Fatal(format!("failed to connect to {backend} at {}", conn.redacted()).into())
}

/// Warn that plaintext was explicitly selected, without echoing credentials.
fn warn_plaintext_selected(conn: &ConnString) {
    tracing::warn!(
        conn = %conn.redacted(),
        "TLS disabled by an explicit opt-out; credentials will be sent in cleartext"
    );
}

#[cfg(test)]
mod tests {
    use sqlx::mysql::MySqlSslMode;
    use sqlx::postgres::PgSslMode;

    use super::*;

    #[test]
    fn postgres_url_without_ssl_mode_is_raised_to_verify_full() {
        let conn: ConnString = "postgres://sentinel_user:sentinel_password@host/db"
            .parse()
            .unwrap();
        let (options, choice) = postgres_options(&conn).unwrap();
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyFull));
        assert_eq!(choice, TransportChoice::Enforced);
    }

    #[test]
    fn mysql_url_without_ssl_mode_is_raised_to_verify_identity() {
        let conn: ConnString = "mysql://sentinel_user:sentinel_password@host/db"
            .parse()
            .unwrap();
        let (options, choice) = mysql_options(&conn).unwrap();
        assert!(matches!(
            options.get_ssl_mode(),
            MySqlSslMode::VerifyIdentity
        ));
        assert_eq!(choice, TransportChoice::Enforced);
    }

    #[test]
    fn postgres_explicit_disable_is_kept_and_marked_plaintext_requested() {
        let conn: ConnString = "postgres://sentinel_user:sentinel_password@host/db?sslmode=disable"
            .parse()
            .unwrap();
        let (options, choice) = postgres_options(&conn).unwrap();
        assert!(matches!(options.get_ssl_mode(), PgSslMode::Disable));
        assert_eq!(choice, TransportChoice::PlaintextRequested);
    }

    #[test]
    fn mysql_explicit_disabled_is_kept_and_marked_plaintext_requested() {
        let conn: ConnString = "mysql://sentinel_user:sentinel_password@host/db?ssl-mode=disabled"
            .parse()
            .unwrap();
        let (options, choice) = mysql_options(&conn).unwrap();
        assert!(matches!(options.get_ssl_mode(), MySqlSslMode::Disabled));
        assert_eq!(choice, TransportChoice::PlaintextRequested);
    }

    #[test]
    fn postgres_explicit_prefer_and_require_are_raised_to_verify_full() {
        for mode in ["prefer", "require"] {
            let conn: ConnString =
                format!("postgres://sentinel_user:sentinel_password@host/db?sslmode={mode}")
                    .parse()
                    .unwrap();
            let (options, choice) = postgres_options(&conn).unwrap();
            assert!(
                matches!(options.get_ssl_mode(), PgSslMode::VerifyFull),
                "sslmode={mode} must not be treated as an opt-out"
            );
            assert_eq!(choice, TransportChoice::Enforced);
        }
    }

    #[test]
    fn mysql_explicit_preferred_and_required_are_raised_to_verify_identity() {
        for mode in ["preferred", "required"] {
            let conn: ConnString =
                format!("mysql://sentinel_user:sentinel_password@host/db?ssl-mode={mode}")
                    .parse()
                    .unwrap();
            let (options, choice) = mysql_options(&conn).unwrap();
            assert!(
                matches!(options.get_ssl_mode(), MySqlSslMode::VerifyIdentity),
                "ssl-mode={mode} must not be treated as an opt-out"
            );
            assert_eq!(choice, TransportChoice::Enforced);
        }
    }

    #[test]
    fn postgres_explicit_verify_ca_is_kept_as_chosen() {
        let conn: ConnString =
            "postgres://sentinel_user:sentinel_password@host/db?sslmode=verify-ca"
                .parse()
                .unwrap();
        let (options, choice) = postgres_options(&conn).unwrap();
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyCa));
        assert_eq!(choice, TransportChoice::Enforced);
    }

    #[test]
    fn mysql_explicit_verify_ca_is_kept_as_chosen() {
        let conn: ConnString = "mysql://sentinel_user:sentinel_password@host/db?ssl-mode=verify_ca"
            .parse()
            .unwrap();
        let (options, choice) = mysql_options(&conn).unwrap();
        assert!(matches!(options.get_ssl_mode(), MySqlSslMode::VerifyCa));
        assert_eq!(choice, TransportChoice::Enforced);
    }

    #[test]
    fn disable_substring_in_password_does_not_disable_tls() {
        let conn: ConnString = "postgres://sentinel_user:sslmode=disable@host/db"
            .parse()
            .unwrap();
        let (options, choice) = postgres_options(&conn).unwrap();
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyFull));
        assert_eq!(choice, TransportChoice::Enforced);
    }

    // The two tests below guard an assumption about `sqlx`, not about this
    // module: that its configuration errors never quote the connection
    // string they failed to parse. They stay green even if this module
    // chains the driver error, because `sqlx` does not leak it today.
    // Not chaining it remains the correct precaution, and the day `sqlx`
    // starts quoting the input, these two turn red.
    #[test]
    fn a_parse_failure_does_not_echo_the_credentials_it_choked_on() {
        let conn: ConnString = "postgres://sentinel_user:sentinel_password@host:99999/db"
            .parse()
            .unwrap();
        // Deliberately not `unwrap_err`: it would render the `Ok` side with
        // `Debug` on failure, and those options carry the password.
        let Err(error) = postgres_options(&conn) else {
            panic!("a port outside the u16 range must fail to parse");
        };
        let message = error.to_string();
        assert!(!message.contains("sentinel_user"));
        assert!(!message.contains("sentinel_password"));
    }

    #[test]
    fn a_mysql_parse_failure_does_not_echo_the_credentials_it_choked_on() {
        let conn: ConnString = "mysql://sentinel_user:sentinel_password@host:99999/db"
            .parse()
            .unwrap();
        let Err(error) = mysql_options(&conn) else {
            panic!("a port outside the u16 range must fail to parse");
        };
        let message = error.to_string();
        assert!(!message.contains("sentinel_user"));
        assert!(!message.contains("sentinel_password"));
    }

    #[test]
    fn connect_error_redacts_credentials_but_keeps_the_host() {
        let conn: ConnString = "postgres://sentinel_user:sentinel_password@db.example.com/app"
            .parse()
            .unwrap();
        let error = connect_error(&conn, DialectKind::Postgres);
        let message = error.to_string();
        assert!(
            message.contains("db.example.com"),
            "the host must stay readable for diagnosis: {message}"
        );
        assert!(!message.contains("sentinel_user"));
        assert!(!message.contains("sentinel_password"));
    }
}
