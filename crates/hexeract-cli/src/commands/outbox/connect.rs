//! Shared PostgreSQL connection establishment for the `outbox` commands.

use std::str::FromStr;
use std::sync::Arc;

use rustls::ClientConfig;
use rustls::RootCertStore;
use tokio_postgres::Config;
use tokio_postgres::NoTls;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

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
/// The TLS stack is `rustls` with the `ring` crypto provider, matching
/// the workspace `sqlx` configuration (`tls-rustls-ring`) so the CLI
/// binary carries a single TLS implementation rather than pulling in
/// `native-tls`/OpenSSL alongside it. Server certificates are validated
/// against the operating system trust store loaded via
/// [`rustls_native_certs`], which honors internal and enterprise CAs;
/// certificate-chain and hostname verification are `rustls`'s default
/// behavior and are never disabled.
///
/// # Errors
///
/// Returns [`CliError::Fatal`] if the URL cannot be parsed, the TLS
/// client configuration cannot be built, or the connect attempt itself
/// fails. The error message never contains the connection string, only
/// its credential-redacted form: the raw `tokio_postgres` error is
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
        spawn_connection(connection);
        return Ok(client);
    }

    config.ssl_mode(SslMode::Require);
    let connector = MakeRustlsConnect::new(build_tls_config()?);
    let (client, connection) = config
        .connect(connector)
        .await
        .map_err(|_err| connect_error(conn))?;
    spawn_connection(connection);
    Ok(client)
}

/// Build the `rustls` [`ClientConfig`] used for `sslmode != disable`.
///
/// Pins the `ring` crypto provider explicitly through
/// [`ClientConfig::builder_with_provider`] rather than relying on a
/// process-level default provider: with two providers reachable in the
/// dependency graph, `rustls` refuses to guess and a builder that omits
/// the provider panics at runtime with "no process-level `CryptoProvider`
/// available". Trust anchors come from the OS certificate store, and the
/// resulting config performs `rustls`'s default full chain and hostname
/// verification.
fn build_tls_config() -> Result<ClientConfig, CliError> {
    let root_store = load_root_store()?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_err| tls_config_error())?
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(config)
}

/// Load the operating system trust store into a [`RootCertStore`].
///
/// `add_parsable_certificates` tolerates individually malformed entries
/// (a few bad certificates in a large OS bundle must not sink the whole
/// store), but an empty store would silently accept no server, so an
/// empty result is treated as a hard configuration error.
fn load_root_store() -> Result<RootCertStore, CliError> {
    let loaded = rustls_native_certs::load_native_certs();
    let mut root_store = RootCertStore::empty();
    let (added, _ignored) = root_store.add_parsable_certificates(loaded.certs);
    if added == 0 {
        return Err(tls_config_error());
    }
    Ok(root_store)
}

/// Build a credential-safe fatal error for a failed PostgreSQL connect.
fn connect_error(conn: &ConnString) -> CliError {
    CliError::Fatal(format!("failed to connect to PostgreSQL at {}", conn.redacted()).into())
}

/// Build a fatal error for an unusable TLS configuration.
fn tls_config_error() -> CliError {
    CliError::Fatal(
        "failed to build the TLS client configuration: no usable CA certificates were found \
         in the operating system trust store"
            .into(),
    )
}

/// Spawn the background driver task that services a `tokio_postgres`
/// connection until it closes.
fn spawn_connection<S, T>(connection: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio_postgres::tls::TlsStream + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::error!(error = %err, "PostgreSQL connection task error");
        }
    });
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use tokio_postgres::Config;
    use tokio_postgres::config::SslMode;

    use super::build_tls_config;
    use super::load_root_store;

    #[test]
    fn os_trust_store_yields_a_non_empty_root_store() {
        let root_store =
            load_root_store().expect("the OS trust store must provide CA certificates");
        assert!(
            !root_store.is_empty(),
            "the root store must hold at least one trust anchor for server verification"
        );
    }

    #[test]
    fn tls_config_builds_with_the_ring_provider() {
        // Exercises the explicit `ring` provider wiring: a builder that
        // relied on a process-level default provider would panic here
        // ("no process-level CryptoProvider available") rather than return.
        build_tls_config().expect("TLS client config must build from the OS trust store");
    }

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
