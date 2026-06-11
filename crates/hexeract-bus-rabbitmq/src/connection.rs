use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hexeract_bus::BusError;
use lapin::Channel;
use lapin::Connection;
use lapin::ConnectionProperties;

/// Redact the credentials of an AMQP URI for safe logging.
///
/// Returns `scheme://***@host[:port][/vhost]` when a userinfo component
/// is present, dropping the user and password entirely. When the URI is
/// malformed enough that the host cannot be isolated, returns the static
/// `"<redacted AMQP URI>"` so a raw, password-bearing string is never
/// surfaced. The function never echoes the password under any input.
pub(crate) fn redact_uri(uri: &str) -> String {
    // Split off the scheme (everything up to and including "://" or ":").
    let (scheme, rest) = match uri.split_once("://") {
        Some((scheme, rest)) => (scheme, rest),
        None => match uri.split_once(':') {
            Some((scheme, rest)) => (scheme, rest.trim_start_matches("//")),
            None => return "<redacted AMQP URI>".to_owned(),
        },
    };
    // Drop any userinfo (everything up to and including the last '@').
    let host_and_path = match rest.rsplit_once('@') {
        Some((_userinfo, host)) => host,
        None => rest,
    };
    if host_and_path.is_empty() {
        return "<redacted AMQP URI>".to_owned();
    }
    format!("{scheme}://***@{host_and_path}")
}

/// Build a credential-safe [`BusError::Connection`] for a failed connect.
///
/// The underlying `lapin` error is deliberately not chained as the
/// source: for a malformed URI the `amq-protocol-uri` error embeds the
/// raw URI (password included), and any error-chain formatter would
/// expose it. The message carries only the redacted form.
fn connection_error(uri: &str) -> BusError {
    BusError::Connection(
        format!(
            "failed to connect to rabbitmq broker at {}",
            redact_uri(uri)
        )
        .into(),
    )
}

/// Default number of attempts used by [`RabbitMqConnection::connect_with_retry`].
pub const DEFAULT_RETRY_ATTEMPTS: u32 = 5;

/// Default base delay used by [`RabbitMqConnection::connect_with_retry`].
pub const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// Thin wrapper over a shared [`lapin::Connection`].
///
/// The wrapper centralises connection establishment so the rest of the
/// crate does not need to depend on `lapin` directly. Cloning the
/// wrapper clones the underlying [`Arc`], so every clone keeps pointing
/// at the same broker session.
///
/// # Transport
///
/// Both [`Self::connect`] and [`Self::connect_with_retry`] take an AMQP
/// URI and select the transport from its scheme:
///
/// - `amqp://` is plaintext AMQP 0.9.1 and offers no confidentiality.
///   Use it only for local development against a broker on `localhost`.
/// - `amqps://` is AMQP over TLS. Production deployments should always
///   use `amqps://` so credentials and message payloads are encrypted
///   in transit. Server certificate validation is performed by the
///   platform trust store; point the broker at a certificate chain that
///   the host already trusts.
///
/// # Security
///
/// The URI embeds the broker credentials in its userinfo component
/// (`amqps://user:password@host:5671/vhost`). Treat the whole URI as a
/// secret:
///
/// - Source it from an environment variable or a secrets manager, never
///   hard-code it.
/// - Never log the URI or interpolate it into error messages. This type
///   derives [`Debug`] only over the opaque shared [`lapin::Connection`]
///   handle, which does not render the originating URI, so logging a
///   [`RabbitMqConnection`] cannot leak credentials.
/// - Connection failures surface as [`BusError::Connection`] wrapping a
///   sanitized message. The crate never logs the raw `lapin` error on a
///   connect failure, because for one class of malformed input (a URI
///   that parses but `cannot_be_a_base`, e.g. the typo `amqps:user:pass@host`
///   with the `//` missing) the underlying `amq-protocol-uri` error echoes
///   the entire URI back, password included. The worker logs only a
///   credential-redacted form (`scheme://***@host:port/vhost`) and the
///   returned error suppresses the leaking source chain.
/// - Prefer per-environment credentials with least-privilege vhost
///   permissions so a leaked development URI cannot reach production.
#[derive(Clone, Debug)]
pub struct RabbitMqConnection {
    inner: Arc<Connection>,
}

impl RabbitMqConnection {
    /// Connect to the broker described by `uri`, single attempt.
    ///
    /// Pass an `amqps://` URI in production so the session is encrypted
    /// with TLS; `amqp://` is plaintext and intended for local
    /// development only.
    ///
    /// # Security
    ///
    /// `uri` carries the broker credentials and must be treated as a
    /// secret: do not log it or place it in error messages. See the
    /// [type-level security notes](RabbitMqConnection#security).
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if `lapin` fails to negotiate
    /// the AMQP handshake. The error never includes `uri` or its
    /// credentials: the raw `lapin` error (which can echo a malformed
    /// URI back) is dropped in favour of a credential-redacted message.
    pub async fn connect(uri: &str) -> Result<Self, BusError> {
        let inner = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|_err| connection_error(uri))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Connect to the broker with a bounded exponential-backoff loop.
    ///
    /// Tries up to `attempts` times, doubling the wait between
    /// attempts starting from `base_delay`. Each failing attempt is
    /// logged via `tracing::warn`. Use an `amqps://` URI in production
    /// for a TLS-encrypted session.
    ///
    /// # Security
    ///
    /// `uri` carries the broker credentials and must be treated as a
    /// secret. Only the attempt counter and a credential-redacted form
    /// of the URI are logged on failure; the raw URI and the `lapin`
    /// error (which can echo it back) are never logged. See the
    /// [type-level security notes](RabbitMqConnection#security).
    ///
    /// `attempts` is clamped to at least 1: a caller-supplied `0` would
    /// otherwise make the loop a no-op, so it is treated as a single
    /// attempt rather than panicking on untrusted input.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] after the final attempt. The
    /// error never includes `uri` or its credentials: the raw `lapin`
    /// error (which can echo a malformed URI back, password included)
    /// is dropped in favour of a credential-redacted message, and only
    /// the attempt counter and the redacted URI are logged.
    pub async fn connect_with_retry(
        uri: &str,
        attempts: u32,
        base_delay: Duration,
    ) -> Result<Self, BusError> {
        let attempts = attempts.max(1);
        for attempt in 1..=attempts {
            match Connection::connect(uri, ConnectionProperties::default()).await {
                Ok(conn) => {
                    return Ok(Self {
                        inner: Arc::new(conn),
                    });
                }
                Err(_err) => {
                    tracing::warn!(
                        attempt,
                        uri = %redact_uri(uri),
                        "rabbitmq connect failed"
                    );
                    if attempt < attempts {
                        let shift = attempt.saturating_sub(1).min(8);
                        let delay = base_delay.checked_mul(1u32 << shift).unwrap_or(base_delay);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        Err(connection_error(uri))
    }

    /// Open a fresh AMQP channel on the underlying connection.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if the channel cannot be opened.
    pub async fn create_channel(&self) -> Result<Channel, BusError> {
        self.inner
            .create_channel()
            .await
            .map_err(|err| BusError::Connection(Box::new(err)))
    }

    /// Open a short-lived channel, hand it to `f` and drop it when the
    /// future completes.
    ///
    /// Useful for admin operations (topology declarations, one-shot
    /// queries) that do not warrant adding a long-lived channel to a
    /// [`crate::ChannelPool`]. The closure receives the channel by
    /// value; the channel is closed by lapin on drop after the inner
    /// future resolves.
    ///
    /// # Errors
    ///
    /// Propagates [`BusError::Connection`] if the channel cannot be
    /// opened, or whatever error the closure returns.
    pub async fn with_channel<F, Fut, T>(&self, f: F) -> Result<T, BusError>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: Future<Output = Result<T, BusError>>,
    {
        let channel = self.create_channel().await?;
        f(channel).await
    }

    /// Borrow the underlying [`lapin::Connection`].
    #[must_use]
    pub fn inner(&self) -> &Connection {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_with_retry_returns_connection_error_after_max_attempts() {
        let result = RabbitMqConnection::connect_with_retry(
            "amqp://127.0.0.1:1",
            2,
            Duration::from_millis(1),
        )
        .await;
        let err = result.expect_err("must fail to connect");
        assert!(matches!(err, BusError::Connection(_)));
    }

    #[tokio::test]
    async fn connect_returns_connection_error_on_unreachable_broker() {
        let err = RabbitMqConnection::connect("amqp://127.0.0.1:1")
            .await
            .expect_err("must fail to connect");
        assert!(matches!(err, BusError::Connection(_)));
    }

    #[test]
    fn defaults_are_sane() {
        assert_eq!(DEFAULT_RETRY_ATTEMPTS, 5);
        assert!(DEFAULT_RETRY_BASE_DELAY >= Duration::from_millis(1));
    }

    #[test]
    fn redact_uri_strips_userinfo_credentials() {
        let redacted = redact_uri("amqps://user:s3cr3t@broker.example.com:5671/vhost");
        assert!(!redacted.contains("s3cr3t"), "password must not appear");
        assert!(!redacted.contains("user"), "username must not appear");
        assert!(redacted.contains("broker.example.com:5671/vhost"));
        assert!(redacted.starts_with("amqps://***@"));
    }

    #[test]
    fn redact_uri_handles_cannot_be_a_base_uri_with_password() {
        // The classic typo: `//` missing after the scheme. amq-protocol-uri
        // echoes the whole string back in its error; redaction must not.
        let redacted = redact_uri("amqps:user:s3cr3t@host/vhost");
        assert!(
            !redacted.contains("s3cr3t"),
            "password must never survive redaction, got {redacted}"
        );
    }

    #[test]
    fn redact_uri_without_userinfo_keeps_host() {
        let redacted = redact_uri("amqp://localhost:5672/%2f");
        assert!(!redacted.contains("s3cr3t"));
        assert!(redacted.contains("localhost:5672"));
    }

    #[tokio::test]
    async fn connect_with_retry_never_leaks_password_in_error() {
        let result = RabbitMqConnection::connect_with_retry(
            "amqps://user:s3cr3t@127.0.0.1:1/vhost",
            1,
            Duration::from_millis(1),
        )
        .await;
        let err = result.expect_err("must fail to connect");
        let rendered = format!("{err:?} {err}");
        // Walk the whole source chain too.
        let mut source = std::error::Error::source(&err);
        let mut chain = rendered;
        while let Some(inner) = source {
            chain.push_str(&inner.to_string());
            source = inner.source();
        }
        assert!(
            !chain.contains("s3cr3t"),
            "password must not appear anywhere in the error chain: {chain}"
        );
    }

    #[tokio::test]
    async fn connect_with_retry_treats_zero_attempts_as_one() {
        // Must not panic on a caller-supplied 0; it returns a connection
        // error after a single clamped attempt against the dead broker.
        let result = RabbitMqConnection::connect_with_retry(
            "amqp://127.0.0.1:1",
            0,
            Duration::from_millis(1),
        )
        .await;
        assert!(matches!(result, Err(BusError::Connection(_))));
    }
}
