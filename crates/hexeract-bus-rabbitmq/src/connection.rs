use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hexeract_bus::BusError;
use lapin::Channel;
use lapin::Connection;
use lapin::ConnectionProperties;

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
///   [`RabbitMqConnection`] cannot leak credentials. Connection failures
///   surface as [`BusError::Connection`] wrapping the `lapin` error,
///   which likewise does not echo the URI back.
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
    /// the AMQP handshake. The error does not include `uri`.
    pub async fn connect(uri: &str) -> Result<Self, BusError> {
        let inner = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|err| BusError::Connection(Box::new(err)))?;
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
    /// secret. Only the attempt counter and the `lapin` error are
    /// logged on failure; the URI is never logged. See the
    /// [type-level security notes](RabbitMqConnection#security).
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] wrapping the last `lapin`
    /// error after the final attempt. The error does not include `uri`.
    pub async fn connect_with_retry(
        uri: &str,
        attempts: u32,
        base_delay: Duration,
    ) -> Result<Self, BusError> {
        assert!(attempts >= 1, "attempts must be at least 1");
        let mut last_error: Option<lapin::Error> = None;
        for attempt in 1..=attempts {
            match Connection::connect(uri, ConnectionProperties::default()).await {
                Ok(conn) => {
                    return Ok(Self {
                        inner: Arc::new(conn),
                    });
                }
                Err(err) => {
                    tracing::warn!(attempt, error = %err, "rabbitmq connect failed");
                    last_error = Some(err);
                    if attempt < attempts {
                        let shift = attempt.saturating_sub(1).min(8);
                        let delay = base_delay.checked_mul(1u32 << shift).unwrap_or(base_delay);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        match last_error {
            Some(err) => Err(BusError::Connection(Box::new(err))),
            None => Err(BusError::Internal(
                "connect_with_retry exited without recording an error".to_owned(),
            )),
        }
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
}
