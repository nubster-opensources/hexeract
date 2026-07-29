use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hexeract_bus::BusError;
use lapin::Channel;
use lapin::Connection;
use lapin::ConnectionProperties;
use lapin::ErrorKind;

/// Redact the credentials of a connection URI for safe logging.
///
/// Returns `scheme://***@host[:port][/vhost]` when a userinfo component
/// is present, dropping the user and password entirely. When the URI is
/// malformed enough that the host cannot be isolated, returns the static
/// `"<redacted AMQP URI>"` so a raw, password-bearing string is never
/// surfaced. The function never echoes the password under any input.
///
/// The parsing is scheme-agnostic (it only looks for `scheme://` and a
/// trailing `user:pass@` userinfo component), so it is reused by the
/// `hexeract-cli` crate to redact PostgreSQL connection strings as well
/// as AMQP ones.
#[must_use]
pub fn redact_uri(uri: &str) -> String {
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

/// Connection properties for the supervised consumer/worker path.
///
/// Auto-recovery is deliberately left off. A [`RabbitMqWorker`] does not
/// reconnect on its own: recovery belongs to its supervisor, which rebuilds
/// every piece of broker state (queues, bindings, consumer) from scratch on
/// each iteration. That contract requires a lost connection to end the
/// consumer stream so [`RabbitMqWorker::run`] returns
/// [`BusError::Connection`] and the supervisor can restart it. lapin's
/// auto-recovery exists to keep that stream alive across a drop, which is
/// exactly what would defeat the supervisor: the stream would never end, the
/// worker would never surface a dead broker, and its run loop would block
/// forever. The consumer path therefore stays on plain, single-attempt
/// connection properties.
///
/// [`RabbitMqWorker`]: crate::RabbitMqWorker
/// [`RabbitMqWorker::run`]: crate::RabbitMqWorker::run
fn supervised_properties() -> ConnectionProperties {
    ConnectionProperties::default()
}

/// Connection properties for the auto-recovering publisher path.
///
/// Auto-recovery is enabled so the lapin IO loop transparently reconnects
/// on a network drop and replays the topology (exchanges, queues, bindings)
/// on the new connection. A long-lived publisher therefore stays usable
/// across a broker blip instead of failing forever (#334). This is reserved
/// for the publisher: a publisher has no supervisor to rebuild it, so it
/// must heal itself, whereas a consumer is rebuilt by its supervisor (see
/// [`supervised_properties`]).
///
/// The recovery backoff is deliberately tightened, but note what it does and
/// does not buy. lapin applies this same backoff to the initial connection
/// attempt as well as to reconnections, and its `global_backoff` budget is
/// rebuilt on every successful connect, so `with_max_times(3)` means three
/// consecutive failed reconnections before recovery is abandoned. That is the
/// right budget for a live session (#334) and the wrong one for a first
/// connect, where it multiplies every attempt by four.
///
/// These properties are therefore no longer used for the initial connect.
/// [`RabbitMqConnection::connect_with_retry_recovering`] probes the broker
/// first with [`supervised_properties`], under a bound, and only opens an
/// auto-recovering session once the broker has answered.
///
/// `with_max_times(3)` also bounds something outside our own control: if the
/// session bound expires while a connect built from these properties is
/// in flight, `tokio::time::timeout` drops only our future, not lapin's
/// io-loop thread, which keeps knocking on the broker on its own budget.
/// Measured, that abandoned thread exhausts a `with_max_times(3)` budget and
/// exits in roughly 8.5 s; raising this value lengthens how long the
/// abandoned thread keeps knocking after we have already given up, so it
/// must stay small.
fn recovering_properties() -> ConnectionProperties {
    ConnectionProperties::default()
        .enable_auto_recover()
        .configure_backoff(|builder| {
            builder
                .with_max_times(3)
                .with_min_delay(Duration::from_millis(50))
                .with_max_delay(Duration::from_millis(500))
        })
}

/// Build a credential-safe [`BusError::Connection`] for a failed connect.
///
/// The underlying `lapin` error is deliberately not chained as the source:
/// for a malformed URI the `amq-protocol-uri` error embeds the raw URI
/// (password included), and any error-chain formatter would expose it. The
/// message carries only the redacted form. `retryable` is the classification
/// from [`is_transient`].
fn connection_error(uri: &str, retryable: bool) -> BusError {
    BusError::connection(
        format!(
            "failed to connect to rabbitmq broker at {}",
            redact_uri(uri)
        ),
        retryable,
    )
}

/// Classify a lapin failure as transient (worth retrying) or permanent.
///
/// Reads only the error *shape* through [`lapin::Error::kind`], never its
/// formatted content, so it is safe on the credential-bearing connect path
/// where the content can echo the URI. Transient failures are transport
/// level (TCP refused, reset, timed out); a permanent failure is an AMQP
/// handshake rejection (bad credentials surface as `ACCESS_REFUSED`, an
/// unsupported protocol version, or an authentication provider error) that
/// would fail identically on every retry and must not be hammered (#340).
///
/// lapin's own `Error::can_be_recovered` is deliberately not used: it
/// classifies every `ProtocolError` as recoverable, so it would retry an
/// `ACCESS_REFUSED`, which is exactly the bug this classifier prevents.
pub(crate) fn is_transient(error: &lapin::Error) -> bool {
    // Permanent kinds are enumerated; everything else (transport-level
    // IOError, and any future `#[non_exhaustive]` variant such as a
    // channel/connection state during a recovery gap or a missing
    // heartbeat) is transient, so a retry or auto-recovery is given a
    // chance to heal it, bounded by the caller's attempt budget. Expressed
    // as a negated match so the shared `true` outcomes do not trip
    // clippy::match_same_arms (pedantic, denied at the workspace level).
    !matches!(
        error.kind(),
        ErrorKind::InvalidProtocolVersion(_)
            | ErrorKind::AuthProviderError(_)
            | ErrorKind::RuntimeShutdownError(_)
            | ErrorKind::ProtocolError(_)
    )
}

/// Default number of attempts used by [`RabbitMqConnection::connect_with_retry`].
pub const DEFAULT_RETRY_ATTEMPTS: u32 = 5;

/// Default base delay used by [`RabbitMqConnection::connect_with_retry`].
pub const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// Default bound on the probe phase of the publisher connect, used by
/// [`crate::RabbitMqTransport::new`] and
/// [`crate::RabbitMqTransport::with_exchange`].
///
/// Caps how long the publisher path may spend proving the broker answers
/// before it gives up. Measured against a closed loopback port, one probe
/// attempt costs about 2 s, so this budget covers roughly two attempts. A
/// broker that refuses the handshake locally (bad credentials, unsupported
/// protocol version) returns `ACCESS_REFUSED` and the permanent-failure
/// early-break (#340) fires long before the bound is reached; a broker whose
/// authentication depends on a slow or hanging external backend can instead
/// leave the bound to fire first, and an expired bound always classifies as
/// transient.
pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default bound on opening the auto-recovering session once the probe has
/// succeeded.
///
/// The probe proves the broker answers, so this phase normally completes in
/// milliseconds. The bound exists for the case the probe cannot rule out: a
/// broker that accepts the connection and then never sends a frame, which
/// produces no error for lapin to classify and would otherwise hang forever.
/// It is more generous than the probe bound because a slow but healthy broker
/// must not be cut off here.
///
/// If this bound fires mid-attempt, the abandoned lapin io-loop thread
/// survives the dropped future and keeps retrying on its own
/// `recovering_properties` backoff budget before it gives up and exits;
/// raising that budget's `with_max_times` lengthens the thread's residual
/// knocking on the broker, so it must stay small.
// No unit test covers this bound: reaching phase two needs a peer that
// completes the AMQP handshake and then stalls, which a local listener
// cannot simulate. The Docker integration suites exercise the success path.
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(10);

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
        let inner = Connection::connect(uri, supervised_properties())
            .await
            .map_err(|err| connection_error(uri, is_transient(&err)))?;
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
        Self::connect_with_retry_inner(uri, attempts, base_delay, false).await
    }

    /// Like [`Self::connect_with_retry`] but enables lapin auto-recovery on
    /// the resulting connection, for the long-lived publisher path that must
    /// heal itself across a broker blip (#334).
    ///
    /// Reserved for [`crate::RabbitMqTransport`]: a publisher has no
    /// supervisor to rebuild it. The consumer/worker path must not use this,
    /// because auto-recovery keeps the consumer stream alive across a drop
    /// and would stop [`crate::RabbitMqWorker::run`] from ever detecting a
    /// dead broker (the run loop would block forever instead of returning a
    /// connection error for the supervisor to act on).
    pub(crate) async fn connect_with_retry_recovering(
        uri: &str,
        attempts: u32,
        base_delay: Duration,
    ) -> Result<Self, BusError> {
        Self::connect_recovering_within(
            uri,
            attempts,
            base_delay,
            DEFAULT_PROBE_TIMEOUT,
            DEFAULT_SESSION_TIMEOUT,
        )
        .await
    }

    /// Establish the publisher connection, giving up once the relevant bound
    /// elapses.
    ///
    /// Split in two phases, both driven by [`Self::connect_with_retry_inner`]
    /// so the same retry policy and the same [`is_transient`]
    /// permanent-failure early-break (#340) govern the connection this
    /// function actually returns, not just a probe that gets thrown away.
    /// lapin exposes a single backoff for both the initial connect and later
    /// reconnections, so no single setting can be fast on the first and
    /// patient on the rest; interrupting an auto-recovering connect can also
    /// leave lapin's io-loop thread alive on its own OS thread, so neither
    /// phase can be abandoned and simply forgotten. Both therefore carry a
    /// bound, but not the same one.
    ///
    /// Phase one probes with auto-recovery off, bounded by `probe_timeout`.
    /// That path is inert once dropped, so bounding it tightly is honest, and
    /// it keeps the retry policy and the early-break clear of lapin's own
    /// reconnection backoff before the broker has even proven it answers.
    ///
    /// Phase two retries again, this time with auto-recovery on (#334) so the
    /// returned connection can heal itself across a later broker blip,
    /// bounded by `session_timeout`. The probe has just proven the broker
    /// answers, so this phase normally completes on its first attempt; the
    /// bound exists for the broker that accepts the connection and then never
    /// sends a frame, which produces no error for lapin to classify.
    /// `session_timeout` is deliberately more generous than `probe_timeout`,
    /// since a slow but healthy broker must not be cut off here.
    async fn connect_recovering_within(
        uri: &str,
        attempts: u32,
        base_delay: Duration,
        probe_timeout: Duration,
        session_timeout: Duration,
    ) -> Result<Self, BusError> {
        // An expired bound carries no classification of its own; an
        // unreachable broker may heal, so it is reported as transient.
        let probe = tokio::time::timeout(
            probe_timeout,
            Self::connect_with_retry_inner(uri, attempts, base_delay, false),
        )
        .await
        .map_err(|_elapsed| connection_error(uri, true))??;
        drop(probe);

        let inner = tokio::time::timeout(
            session_timeout,
            Self::connect_with_retry_inner(uri, attempts, base_delay, true),
        )
        .await
        .map_err(|_elapsed| connection_error(uri, true))??;
        Ok(inner)
    }

    /// Shared retry loop backing both connect-with-retry variants.
    ///
    /// `recovering` selects the connection properties rebuilt on each
    /// attempt: [`recovering_properties`] enables auto-recovery for the
    /// publisher, [`supervised_properties`] leaves it off for the worker.
    /// The properties are rebuilt per attempt because lapin consumes them
    /// by value.
    async fn connect_with_retry_inner(
        uri: &str,
        attempts: u32,
        base_delay: Duration,
        recovering: bool,
    ) -> Result<Self, BusError> {
        let attempts = attempts.max(1);
        let mut retryable = true;
        for attempt in 1..=attempts {
            let properties = if recovering {
                recovering_properties()
            } else {
                supervised_properties()
            };
            match Connection::connect(uri, properties).await {
                Ok(conn) => {
                    return Ok(Self {
                        inner: Arc::new(conn),
                    });
                }
                Err(err) => {
                    retryable = is_transient(&err);
                    tracing::warn!(
                        attempt,
                        retryable,
                        uri = %redact_uri(uri),
                        "rabbitmq connect failed"
                    );
                    // A permanent failure (bad credentials -> ACCESS_REFUSED,
                    // unsupported protocol version) fails identically on every
                    // retry: stop early instead of burning the whole budget
                    // hammering a broker that refuses the handshake (#340).
                    if !retryable {
                        break;
                    }
                    if attempt < attempts {
                        let shift = attempt.saturating_sub(1).min(8);
                        let delay = base_delay.checked_mul(1u32 << shift).unwrap_or(base_delay);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        Err(connection_error(uri, retryable))
    }

    /// Open a fresh AMQP channel on the underlying connection.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if the channel cannot be opened.
    pub async fn create_channel(&self) -> Result<Channel, BusError> {
        self.inner.create_channel().await.map_err(|err| {
            let retryable = is_transient(&err);
            BusError::connection(err, retryable)
        })
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
    use lapin::protocol::{AMQPError, AMQPErrorKind, AMQPSoftError};

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
        assert!(matches!(err, BusError::Connection { .. }));
    }

    #[tokio::test]
    async fn connect_returns_connection_error_on_unreachable_broker() {
        let started = std::time::Instant::now();
        let err = RabbitMqConnection::connect("amqp://127.0.0.1:1")
            .await
            .expect_err("must fail to connect");
        assert!(matches!(err, BusError::Connection { .. }));
        // Regression guard on the supervised connect path. A refused connect
        // to a closed local port returns instantly at the TCP layer because
        // `supervised_properties` leaves auto-recovery off: there is no lapin
        // reconnection loop to turn that instant refusal into a minutes-long
        // retry. Ten seconds is far below that yet tolerant of CI noise.
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a refused connect must fail fast, took {:?}",
            started.elapsed()
        );
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
        assert!(matches!(result, Err(BusError::Connection { .. })));
    }

    /// The bound must cut short a probe against an unreachable broker. Without
    /// it, `DEFAULT_RETRY_ATTEMPTS` attempts each carrying lapin's own retry
    /// budget take tens of seconds to give up (#474). Measured on a closed
    /// loopback port: 2.04 s per supervised attempt, 8.53 s per recovering one.
    #[tokio::test]
    async fn connect_recovering_gives_up_once_its_bound_elapses() {
        let probe_timeout = Duration::from_millis(100);
        let started = std::time::Instant::now();
        let err = RabbitMqConnection::connect_recovering_within(
            "amqp://127.0.0.1:1",
            DEFAULT_RETRY_ATTEMPTS,
            DEFAULT_RETRY_BASE_DELAY,
            probe_timeout,
            DEFAULT_SESSION_TIMEOUT,
        )
        .await
        .expect_err("a closed port must not yield a connection");
        let elapsed = started.elapsed();
        assert!(matches!(err, BusError::Connection { .. }));
        assert!(
            elapsed < Duration::from_millis(500),
            "the bound must cut the probe short, took {elapsed:?} for a {probe_timeout:?} bound"
        );
    }

    /// An unreachable broker may come back. A caller that inspects the
    /// classification before deciding to retry must not read the give-up as a
    /// permanent refusal.
    #[tokio::test]
    async fn giving_up_on_the_bound_stays_retryable() {
        let err = RabbitMqConnection::connect_recovering_within(
            "amqp://127.0.0.1:1",
            DEFAULT_RETRY_ATTEMPTS,
            DEFAULT_RETRY_BASE_DELAY,
            Duration::from_millis(50),
            DEFAULT_SESSION_TIMEOUT,
        )
        .await
        .expect_err("a closed port must not yield a connection");
        assert_eq!(
            err.is_retryable_connection(),
            Some(true),
            "an unreachable broker may heal, so the give-up must classify as transient"
        );
    }

    #[test]
    fn properties_split_auto_recovery_by_role() {
        // Locks the role split at the source without a broker. The publisher
        // path must carry auto-recovery, otherwise a dropped publisher
        // connection would never heal (#334). The consumer/worker path must
        // NOT, otherwise auto-recovery keeps the consumer stream alive across
        // a drop and the worker never detects a dead broker, blocking its run
        // loop forever. ConnectionProperties has no getter, so assert through
        // its Debug rendering.
        let recovering = format!("{:?}", recovering_properties());
        assert!(
            recovering.contains("auto_recover: true"),
            "the publisher path must enable auto-recovery, got {recovering}"
        );
        let supervised = format!("{:?}", supervised_properties());
        assert!(
            supervised.contains("auto_recover: false"),
            "the consumer/worker path must leave auto-recovery off, got {supervised}"
        );
    }

    /// Every kind the retry loop must refuse to hammer (#340), asserted
    /// without a broker: lapin exposes `From<ErrorKind> for Error`, so each
    /// classification can be pinned at the unit level rather than inferred
    /// from one live handshake.
    ///
    /// `ErrorKind::InvalidProtocolVersion` is deliberately absent: its
    /// payload type lives in `amq_protocol::frame`, which lapin does not
    /// re-export, so covering it would mean pinning a second version of
    /// `amq-protocol` alongside the one lapin resolves.
    #[test]
    fn is_transient_is_false_for_permanent_kinds() {
        let permanent: [(&str, lapin::Error); 3] = [
            (
                "ACCESS_REFUSED, the shape bad credentials take",
                ErrorKind::ProtocolError(AMQPError::new(
                    AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED),
                    "ACCESS_REFUSED".into(),
                ))
                .into(),
            ),
            (
                "an authentication provider failure",
                ErrorKind::AuthProviderError("provider rejected the mechanism".to_owned()).into(),
            ),
            (
                "a runtime shutdown",
                ErrorKind::RuntimeShutdownError(Arc::new(std::io::Error::other("runtime gone")))
                    .into(),
            ),
        ];

        for (label, error) in permanent {
            assert!(
                !is_transient(&error),
                "{label} would fail identically on every retry, so it must classify as permanent"
            );
        }
    }

    /// The complement of the permanent list. The unlisted kind is the load
    /// bearing case: it locks the polarity of the negated match, so inverting
    /// the classifier turns a healthy retry into a refusal to connect.
    #[test]
    fn is_transient_is_true_for_transport_failures_and_unlisted_kinds() {
        let transient: [(&str, lapin::Error); 2] = [
            (
                "a TCP failure (refused, reset, timed out)",
                std::io::Error::other("connection reset").into(),
            ),
            (
                "a kind absent from the permanent list",
                ErrorKind::ChannelsLimitReached.into(),
            ),
        ];

        for (label, error) in transient {
            assert!(
                is_transient(&error),
                "{label} may heal, so a retry or auto-recovery must be given a chance"
            );
        }
    }
}
