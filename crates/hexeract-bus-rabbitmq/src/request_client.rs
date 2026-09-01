//! Facade assembling a ready-to-use RabbitMQ request-reply client.
//!
//! [`connect_request_client`] wires together an auto-recovering
//! publisher transport ([`RabbitMqTransport::new`]) with a reply inbox
//! consumed on a distinct, supervised connection
//! ([`RabbitMqConnection::connect_with_retry`]). The two connections
//! must stay separate: see the `reply_inbox` module docs for why
//! lapin's native auto-recovery on the publisher side would mask a
//! broker outage from the inbox supervisor.
//!
//! # Supervisor contract
//!
//! A background task drives `run_reply_inbox` in a loop:
//!
//! - On cancellation, or a plain `Ok(())` return, the task stops.
//! - On `Err` (the broker connection was lost), the task first marks the
//!   shared [`hexeract_bus::ReplyInboxState`] as
//!   [`hexeract_bus::ReplyInboxState::Reconnecting`], then
//!   [`hexeract_bus::RequestRegistry::drain`]s every in-flight slot so a
//!   caller waiting on a reply observes
//!   [`hexeract_bus::RequestError::Transport`] immediately instead of
//!   waiting out its timeout. That order, mark before drain, is what
//!   closes the reconnect race for a call that has not registered yet;
//!   see [`hexeract_bus::ReplyInboxState`] for the exact guarantee it
//!   gives such a call. The task then reconnects over a fresh supervised
//!   connection, declares a fresh exclusive inbox (the previous one died
//!   with its connection), publishes the new name into the
//!   `Arc<Mutex<ReplyInboxState>>` the [`hexeract_bus::RequestClient`]
//!   reads on every request, and resumes consuming.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hexeract_bus::{
    BusError, DEFAULT_MAX_IN_FLIGHT, ReplyInboxState, RequestClient, RequestClientSupervisor,
    RequestRegistry,
};
use lapin::Channel;
use tokio_util::sync::CancellationToken;

use crate::connection::{
    DEFAULT_RETRY_ATTEMPTS, DEFAULT_RETRY_BASE_DELAY, RabbitMqConnection, RabbitMqConnectionConfig,
};
use crate::metadata::AmqpMetadataLimits;
use crate::reply_inbox::{declare_reply_inbox, run_reply_inbox_with_limits};
use crate::transport::RabbitMqTransport;

/// Maximum time spent closing a connection from a failed reply-inbox setup.
///
/// This close is best-effort cleanup: it must never turn a broker that has
/// stopped responding into an unbounded pause in the reconnect loop. Bounded
/// by [`DEFAULT_RETRY_BASE_DELAY`] so a broker that never answers the close
/// costs the same order of wall-clock as the backoff that follows it, rather
/// than several times that backoff.
const FAILED_REPLY_INBOX_SETUP_CLOSE_TIMEOUT: Duration = DEFAULT_RETRY_BASE_DELAY;

/// Tuning parameters for a RabbitMQ request client.
///
/// Marked `#[non_exhaustive]` so new tuning fields can be added in a minor
/// version: construct it through [`RabbitMqRequestClientConfigBuilder`]
/// rather than a struct literal.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RabbitMqRequestClientConfig {
    /// Maximum number of concurrent requests awaiting a reply.
    ///
    /// Reaching this limit returns [`hexeract_bus::RequestError::AtCapacity`]
    /// immediately. It is caller-side backpressure, never an added queue or
    /// latency: callers can shed, retry, or route the rejected request using
    /// their own policy. Rejecting is preferable to waiting for a free slot
    /// because it keeps a saturated client distinguishable from a slow
    /// responder: queuing would report both as latency, and the caller would
    /// lose the one signal that tells it to shed load rather than wait.
    ///
    /// Zero is a legitimate setting, not a misconfiguration guard: it admits
    /// no request at all, which turns request-reply off without stopping the
    /// process. See [`hexeract_bus::RequestRegistry::new`].
    pub max_in_flight: usize,
    /// TLS settings used by the publisher and supervised reply-inbox sessions.
    pub connection_config: RabbitMqConnectionConfig,
    /// Bounds applied to AMQP metadata on both legs of a request-reply call.
    ///
    /// One value governs the publisher transport and the supervised reply
    /// inbox, including every inbox rebuilt after a reconnect. A reply inbox
    /// running on weaker limits than the requests it answers would be the
    /// bypass: it is the path that feeds an RPC correlation slot.
    pub metadata_limits: AmqpMetadataLimits,
}

impl Default for RabbitMqRequestClientConfig {
    fn default() -> Self {
        Self {
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            connection_config: RabbitMqConnectionConfig::default(),
            metadata_limits: AmqpMetadataLimits::default(),
        }
    }
}

/// Builder for [`RabbitMqRequestClientConfig`].
///
/// Mirrors the construction style of [`crate::RabbitMqWorkerBuilder`]: every
/// setter consumes and returns the builder, and [`Self::build`] yields the
/// configuration. An untouched builder produces exactly
/// [`RabbitMqRequestClientConfig::default`], so naming only the settings that
/// matter is always safe.
#[derive(Debug, Clone, Default)]
pub struct RabbitMqRequestClientConfigBuilder {
    config: RabbitMqRequestClientConfig,
}

impl RabbitMqRequestClientConfigBuilder {
    /// Build a fresh builder carrying the default tuning.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the maximum number of concurrent requests awaiting a reply
    /// (default [`DEFAULT_MAX_IN_FLIGHT`]).
    ///
    /// See [`RabbitMqRequestClientConfig::max_in_flight`] for what reaching
    /// the bound does, and why zero is a meaningful value.
    #[must_use]
    pub fn max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.config.max_in_flight = max_in_flight;
        self
    }

    /// Use the supplied TLS settings for every connection owned by the client.
    #[must_use]
    pub fn connection_config(mut self, connection_config: RabbitMqConnectionConfig) -> Self {
        self.config.connection_config = connection_config;
        self
    }

    /// Bound the AMQP metadata of both legs of a request-reply call.
    ///
    /// The same value reaches the publisher transport and every reply inbox
    /// the supervisor runs, including those rebuilt after a reconnect. See
    /// [`RabbitMqRequestClientConfig::metadata_limits`].
    #[must_use]
    pub fn metadata_limits(mut self, metadata_limits: AmqpMetadataLimits) -> Self {
        self.config.metadata_limits = metadata_limits;
        self
    }

    /// Finish the builder into a [`RabbitMqRequestClientConfig`].
    #[must_use]
    pub fn build(self) -> RabbitMqRequestClientConfig {
        self.config
    }
}

/// Assemble a ready RabbitMQ request client.
///
/// Requests are published through a recovering publisher connection
/// (healed transparently, see [`RabbitMqTransport::new`]). Replies are
/// received on a dedicated exclusive inbox consumed over a separate
/// supervised connection that detects broker loss; a background
/// supervisor drains the in-flight registry and re-declares the inbox
/// on reconnect. See the [module docs](self) for the full contract.
///
/// The client admits [`DEFAULT_MAX_IN_FLIGHT`] concurrent requests. Use
/// [`connect_request_client_with_config`] to select another bound.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if either the recovering publisher
/// connection or the initial supervised inbox connection cannot be
/// established, and a permanent one, before any socket opens, when the
/// transport would carry the session in cleartext: `uri` selecting `amqp://`
/// against a host outside loopback, or TLS material an `amqp://` URI would
/// discard.
pub async fn connect_request_client(
    uri: &str,
    default_timeout: Duration,
    cancel: CancellationToken,
) -> Result<RequestClient<RabbitMqTransport>, BusError> {
    connect_request_client_with_config(
        uri,
        default_timeout,
        cancel,
        RabbitMqRequestClientConfig::default(),
    )
    .await
}

/// Assemble a ready RabbitMQ request client with caller-selected limits.
///
/// See [`RabbitMqRequestClientConfig::max_in_flight`] for the immediate
/// backpressure applied when too many requests are already awaiting replies.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if either the recovering publisher
/// connection or the initial supervised inbox connection cannot be
/// established, and a permanent one, before any socket opens, when the
/// transport would carry the session in cleartext: `uri` selecting `amqp://`
/// against a host outside loopback, or TLS material an `amqp://` URI would
/// discard.
pub async fn connect_request_client_with_config(
    uri: &str,
    default_timeout: Duration,
    cancel: CancellationToken,
    config: RabbitMqRequestClientConfig,
) -> Result<RequestClient<RabbitMqTransport>, BusError> {
    let transport = Arc::new(
        RabbitMqTransport::new_with_config(uri, &config.connection_config)
            .await?
            .with_metadata_limits(config.metadata_limits),
    );
    let registry = Arc::new(RequestRegistry::new(config.max_in_flight));

    // Supervised connection for the inbox consumer: NOT the recovering
    // connection used by the publisher above.
    let connection = RabbitMqConnection::connect_with_retry_with_config(
        uri,
        DEFAULT_RETRY_ATTEMPTS,
        DEFAULT_RETRY_BASE_DELAY,
        &config.connection_config,
    )
    .await?;
    // Same contract as the reconnect path: a setup that fails after this
    // connect is live closes it before the error escapes. A caller that
    // retries this constructor while its broker is still refusing inbox
    // declaration would otherwise stack one abandoned session per attempt.
    let (channel, inbox_name) = setup_reply_inbox_or_close(
        &cancel,
        FAILED_REPLY_INBOX_SETUP_CLOSE_TIMEOUT,
        || async {
            let channel = connection.create_channel().await?;
            let inbox_name = declare_reply_inbox(&channel).await?;
            Ok::<_, BusError>((channel, inbox_name))
        },
        || close_reply_inbox_connection(&connection),
    )
    .await?;
    let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Ready(inbox_name.clone())));

    let supervisor = spawn_reply_inbox_supervisor(
        uri,
        (channel, inbox_name),
        Arc::clone(&registry),
        Arc::clone(&reply_inbox),
        cancel,
        config.connection_config,
        config.metadata_limits,
    );

    Ok(RequestClient::new(
        transport,
        registry,
        reply_inbox,
        default_timeout,
        supervisor,
    ))
}

/// Drive the reply inbox consumer, rebuilding it across a broker drop.
///
/// Runs until `cancel` fires or [`run_reply_inbox`] returns `Ok(())`.
/// On `Err` (connection lost), drains `registry` so in-flight callers
/// fail fast, then hands off to [`reconnect_reply_inbox`] for a fresh
/// connection and inbox before resuming.
///
/// Returns an opaque [`RequestClientSupervisor`] rather than detaching the
/// task: [`connect_request_client`] hands that handle to the
/// [`RequestClient`] it assembles, so `RequestClient::close` can await
/// this task's actual termination instead of merely cancelling it.
///
/// `active_inbox` pairs the consuming channel with the exclusive inbox it
/// declared, the same `ActiveInbox` value [`supervise_reply_inbox`] replaces
/// wholesale on every reconnect.
fn spawn_reply_inbox_supervisor(
    uri: &str,
    active_inbox: (Channel, String),
    registry: Arc<RequestRegistry>,
    reply_inbox: Arc<Mutex<ReplyInboxState>>,
    cancel: CancellationToken,
    connection_config: RabbitMqConnectionConfig,
    metadata_limits: AmqpMetadataLimits,
) -> RequestClientSupervisor {
    let reconnect_uri = uri.to_owned();
    let reconnect_state = Arc::clone(&reply_inbox);
    let run_registry = Arc::clone(&registry);

    RequestClientSupervisor::spawn(cancel, move |cancel| async move {
        supervise_reply_inbox(
            active_inbox,
            registry,
            reply_inbox,
            cancel,
            // `metadata_limits` is captured once and reused by every run the
            // supervisor drives, so an inbox rebuilt after a reconnect keeps
            // the configured bound instead of silently falling back to the
            // defaults.
            move |(channel, inbox), cancel| {
                run_reply_inbox_with_limits(
                    channel,
                    inbox,
                    Arc::clone(&run_registry),
                    cancel,
                    metadata_limits,
                )
            },
            move |cancel| {
                let reconnect_uri = reconnect_uri.clone();
                let reconnect_state = Arc::clone(&reconnect_state);
                let connection_config = connection_config.clone();
                async move {
                    reconnect_reply_inbox(
                        &reconnect_uri,
                        &reconnect_state,
                        &cancel,
                        &connection_config,
                    )
                    .await
                }
            },
        )
        .await;
    })
}

/// Run the reply-inbox lifecycle independently from its RabbitMQ plumbing.
///
/// Keeping the loop here makes the lifecycle contract testable with a failed
/// receive and a blocked reconnect, while production supplies the concrete
/// channel operations at the call site above.
///
/// `ActiveInbox` is whatever a run consumes and a reconnect replaces:
/// production pairs a `Channel` with the exclusive inbox it declared, and a
/// test supplies a unit so the loop runs without a broker. Owning that value
/// outright keeps "a run always has an inbox to consume" a fact the compiler
/// checks, rather than a convention two closures have to agree to keep.
async fn supervise_reply_inbox<ActiveInbox, Run, RunFuture, Reconnect, ReconnectFuture>(
    mut active_inbox: ActiveInbox,
    registry: Arc<RequestRegistry>,
    reply_inbox: Arc<Mutex<ReplyInboxState>>,
    cancel: CancellationToken,
    mut run_once: Run,
    mut reconnect: Reconnect,
) where
    Run: FnMut(ActiveInbox, CancellationToken) -> RunFuture,
    RunFuture: Future<Output = Result<(), BusError>>,
    Reconnect: FnMut(CancellationToken) -> ReconnectFuture,
    ReconnectFuture: Future<Output = Option<ActiveInbox>>,
{
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let outcome = run_once(active_inbox, cancel.clone()).await;
        if cancel.is_cancelled() || outcome.is_ok() {
            return;
        }

        // Connection lost: fail every in-flight request fast rather than let
        // it run out its timeout against a dead inbox, and refuse calls that
        // have not registered yet. The order is non-negotiable.
        on_connection_lost(&reply_inbox, &registry);

        match reconnect(cancel.clone()).await {
            Some(next_inbox) => active_inbox = next_inbox,
            None => return,
        }
    }
}

/// Handle connection loss: mark `reply_inbox` [`ReplyInboxState::Reconnecting`]
/// and drain `registry`.
///
/// This is the single named site the composition runs from, and the
/// single site a test can call to exercise both halves together without
/// a broker: [`RequestRegistry::drain`] stays `pub` and stays callable on
/// its own, this function does not close that off, it just gives the two
/// halves one testable home instead of leaving the composition implicit
/// at the call site.
fn on_connection_lost(reply_inbox: &Mutex<ReplyInboxState>, registry: &RequestRegistry) {
    mark_reconnecting_then_drain(reply_inbox, || registry.drain());
}

/// Marks `reply_inbox` as [`ReplyInboxState::Reconnecting`], then runs
/// `drain`.
///
/// This order, mark before drain, is the non-negotiable invariant the
/// reconnect door depends on: it is what lets a call that registers
/// after the drain still observe `Reconnecting` rather than a stale
/// `Ready`, closing the window a caller could otherwise wait its full
/// timeout in. See [`ReplyInboxState`] for the exact guarantee this
/// gives such a call, and the module docs for the full supervisor
/// contract.
///
/// `drain` is a closure rather than a direct `registry.drain()` call
/// specifically so a test can observe, at the instant it runs, that the
/// mark has already landed: a production edit that swapped the two
/// operations would make that observation fail, not merely a comment
/// stop matching the code.
fn mark_reconnecting_then_drain(reply_inbox: &Mutex<ReplyInboxState>, drain: impl FnOnce()) {
    *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner) = ReplyInboxState::Reconnecting;
    drain();
}

/// Reconnect over a fresh supervised connection and declare a fresh
/// exclusive reply inbox, retrying until it succeeds or `cancel` fires.
///
/// Publishes the new inbox name into `reply_inbox` (read by the
/// [`RequestClient`] on every request) before returning it. Every failure
/// that did not pay a connect backoff of its own waits
/// [`DEFAULT_RETRY_BASE_DELAY`] before the next attempt: failed channel or
/// inbox setup, and a connect that gave up without sleeping. Only a connect
/// that actually spent its own attempt budget skips that wait, since it has
/// already waited. A failed setup additionally spends up to
/// [`FAILED_REPLY_INBOX_SETUP_CLOSE_TIMEOUT`] closing the connection it was
/// handed; that is cleanup, and it does not stand in for the wait. Both the
/// attempt and that wait stop as soon as `cancel` fires.
async fn reconnect_reply_inbox(
    uri: &str,
    reply_inbox: &Mutex<ReplyInboxState>,
    cancel: &CancellationToken,
    connection_config: &RabbitMqConnectionConfig,
) -> Option<(Channel, String)> {
    let next_inbox = retry_reply_inbox_after_failures(cancel, DEFAULT_RETRY_BASE_DELAY, || async {
        let connection = RabbitMqConnection::connect_with_retry_with_config(
            uri,
            DEFAULT_RETRY_ATTEMPTS,
            DEFAULT_RETRY_BASE_DELAY,
            connection_config,
        )
        .await
        .map_err(|error| {
            tracing::warn!(
                phase = "connect",
                retryable = ?error.is_retryable_connection(),
                "rpc reply inbox reconnect failed"
            );
            classify_connect_failure(&error)
        })?;
        setup_reply_inbox_or_close(
            cancel,
            FAILED_REPLY_INBOX_SETUP_CLOSE_TIMEOUT,
            || async {
                let channel = connection.create_channel().await.map_err(|error| {
                    tracing::warn!(phase = "channel", error = %error, "rpc reply inbox reconnect failed");
                    ReconnectFailure::NeedsBackoff
                })?;
                let inbox = declare_reply_inbox(&channel).await.map_err(|error| {
                    tracing::warn!(phase = "inbox", error = %error, "rpc reply inbox reconnect failed");
                    ReconnectFailure::NeedsBackoff
                })?;
                Ok((channel, inbox))
            },
            || close_reply_inbox_connection(&connection),
        )
        .await
    })
    .await?;

    *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner) =
        ReplyInboxState::Ready(next_inbox.1.clone());
    Some(next_inbox)
}

/// Run one reply-inbox setup over an already live connection, closing that
/// connection when setup fails.
///
/// `setup` owns the steps that can only fail once a connection exists:
/// opening the channel and declaring the inbox. Either failure closes the
/// connection before the error reaches the caller, so the next attempt never
/// opens its own connection alongside one the broker still holds.
///
/// lapin does send `Connection.Close` when the last handle drops, but it does
/// so fire-and-forget through its internal RPC handle, with no ordering
/// against the connect that follows. Awaiting the close here is what supplies
/// that ordering; it is not what supplies the close.
///
/// The cleanup stays subordinate to the failure: `setup`'s own error is always
/// the one returned, and a close that stalls is bounded by `close_timeout`.
/// Diagnostics for the failure belong inside `setup`, which emits them before
/// returning so an operator reads the cause without waiting out the cleanup.
async fn setup_reply_inbox_or_close<Ready, Failure, Setup, SetupFuture, Close, CloseFuture>(
    cancel: &CancellationToken,
    close_timeout: Duration,
    setup: Setup,
    close: Close,
) -> Result<Ready, Failure>
where
    Setup: FnOnce() -> SetupFuture,
    SetupFuture: Future<Output = Result<Ready, Failure>>,
    Close: FnOnce() -> CloseFuture,
    CloseFuture: Future<Output = ()>,
{
    match setup().await {
        Ok(ready) => Ok(ready),
        Err(failure) => {
            close_failed_reply_inbox_connection(cancel, close_timeout, close()).await;
            Err(failure)
        }
    }
}

/// Close a connection that successfully opened but could not finish reply
/// inbox setup. Failure is deliberately diagnostic-only: setup's original
/// error remains the one that drives the reconnect policy.
async fn close_reply_inbox_connection(connection: &RabbitMqConnection) {
    if let Err(error) = connection.inner().close(200, "OK".into()).await {
        tracing::debug!(%error, "failed reply inbox connection close did not complete");
    }
}

/// Await a best-effort connection close without letting it hold cancellation
/// or reconnect forever.
///
/// Returns `true` when the close future completed, including a broker-side
/// error that [`close_reply_inbox_connection`] recorded. `false` means
/// cancellation or the supplied deadline won first, and says so in the log:
/// the session was abandoned rather than closed, which is exactly what an
/// operator needs when broker connection counts drift during an outage.
async fn close_failed_reply_inbox_connection<Close>(
    cancel: &CancellationToken,
    timeout: Duration,
    close: Close,
) -> bool
where
    Close: Future<Output = ()>,
{
    // `biased` so cancellation wins deterministically over a cleanup that is,
    // by that point, no longer worth waiting for.
    let completed = tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        result = tokio::time::timeout(timeout, close) => result.is_ok(),
    };

    if !completed {
        tracing::warn!(
            ?timeout,
            "failed reply inbox connection abandoned before its close completed"
        );
    }

    completed
}

/// Retry a reply-inbox setup attempt until it succeeds or cancellation fires.
///
/// Failures that did not already consume a connection backoff are followed by
/// exactly one cancellable delay. The attempt itself is also cancellable, so
/// dropping a blocked broker operation cannot hold [`RequestClient::close`]
/// past the caller's cancellation.
async fn retry_reply_inbox_after_failures<Inbox, Attempt, AttemptFuture>(
    cancel: &CancellationToken,
    retry_delay: Duration,
    mut attempt: Attempt,
) -> Option<Inbox>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<Inbox, ReconnectFailure>>,
{
    loop {
        // `biased` so an already-cancelled token wins deterministically:
        // the attempt future is built but never polled, which is what keeps
        // "no attempt starts after cancellation" a guarantee rather than a
        // coin flip on `select!`'s random poll order.
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            result = attempt() => result,
        };

        match result {
            Ok(inbox) => return Some(inbox),
            Err(ReconnectFailure::AlreadyBackedOff) => continue,
            Err(ReconnectFailure::NeedsBackoff) => {}
        }

        tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            () = tokio::time::sleep(retry_delay) => {}
        }
    }
}

/// Records whether a failed reconnect setup already consumed a backoff.
#[derive(Debug, PartialEq, Eq)]
enum ReconnectFailure {
    /// [`RabbitMqConnection::connect_with_retry`] spent its attempt budget,
    /// so the wait it applied stands in for this loop's own.
    AlreadyBackedOff,
    /// The failure did not pay a connect backoff of its own: channel setup or
    /// inbox declaration after a successful connect, or a connect that gave up
    /// without ever sleeping.
    ///
    /// A setup failure does spend up to
    /// [`FAILED_REPLY_INBOX_SETUP_CLOSE_TIMEOUT`] closing the connection it
    /// was handed. That is cleanup, not a backoff, so it does not stand in for
    /// the loop's own wait.
    NeedsBackoff,
}

/// Decide whether a failed connect already paid for a delay.
///
/// [`RabbitMqConnection::connect_with_retry`] only spends its attempt budget
/// while the failure looks transient. A permanent one (refused credentials,
/// an unsupported protocol version) breaks out on the first attempt without
/// sleeping at all, which is deliberate: burning the budget against a broker
/// that has already refused the handshake helps nobody (#340). Reading that
/// early exit as "a delay was applied" is what would let this loop retry with
/// no delay whatsoever, so only an explicitly retryable connection failure
/// gets the benefit of the doubt. Anything else earns a wait.
fn classify_connect_failure(error: &BusError) -> ReconnectFailure {
    match error.is_retryable_connection() {
        Some(true) => ReconnectFailure::AlreadyBackedOff,
        _ => ReconnectFailure::NeedsBackoff,
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use hexeract_bus::ReplyExpectation;
    use hexeract_core::RequestId;
    use lapin::tcp::OwnedTLSConfig;
    use tokio::sync::Notify;

    use super::*;

    #[test]
    fn request_client_config_preserves_the_default_in_flight_bound() {
        let config = RabbitMqRequestClientConfig::default();

        assert_eq!(config.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert!(!config.connection_config.has_custom_tls_config());
    }

    #[test]
    fn request_client_builder_keeps_the_selected_connection_configuration() {
        let config = RabbitMqRequestClientConfigBuilder::new()
            .connection_config(RabbitMqConnectionConfig::default().with_tls_config(
                OwnedTLSConfig {
                    cert_chain: Some("private-ca-pem".to_owned()),
                    identity: None,
                },
            ))
            .build();

        assert!(config.connection_config.has_custom_tls_config());
    }

    #[test]
    fn an_untouched_builder_yields_the_default_metadata_limits() {
        assert_eq!(
            RabbitMqRequestClientConfigBuilder::new()
                .build()
                .metadata_limits,
            AmqpMetadataLimits::default()
        );
    }

    #[test]
    fn the_builder_carries_custom_metadata_limits_into_the_config() {
        let limits = AmqpMetadataLimits {
            max_headers: 2,
            ..AmqpMetadataLimits::default()
        };
        assert_eq!(
            RabbitMqRequestClientConfigBuilder::new()
                .metadata_limits(limits)
                .build()
                .metadata_limits,
            limits
        );
    }

    /// The supervisor rebuilds the inbox on reconnect, so the configured
    /// limits must survive that rebuild. A run closure that captured its
    /// bound once, the way production does, must hand the same value to every
    /// run: falling back to the defaults on the second one would leave the
    /// reply path quietly more permissive than the caller asked for.
    #[tokio::test]
    async fn every_run_after_a_reconnect_keeps_the_configured_metadata_limits() {
        let configured = AmqpMetadataLimits {
            max_headers: 3,
            ..AmqpMetadataLimits::default()
        };
        let observed: Arc<Mutex<Vec<AmqpMetadataLimits>>> = Arc::new(Mutex::new(Vec::new()));
        let cancel = CancellationToken::new();
        let mut reconnects_left = 1;

        supervise_reply_inbox(
            (),
            Arc::new(RequestRegistry::default()),
            Arc::new(Mutex::new(ReplyInboxState::Ready("inbox-1".to_owned()))),
            cancel,
            {
                let observed = Arc::clone(&observed);
                move |(), _| {
                    observed
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(configured);
                    async { Err(BusError::connection(io::Error::other("lost"), true)) }
                }
            },
            move |_| {
                let another_inbox = reconnects_left > 0;
                reconnects_left -= 1;
                async move { another_inbox.then_some(()) }
            },
        )
        .await;

        assert_eq!(
            *observed.lock().unwrap_or_else(PoisonError::into_inner),
            vec![configured, configured],
            "the run before and the run after the reconnect must share one bound"
        );
    }

    #[test]
    fn an_untouched_builder_yields_the_default_bound() {
        assert_eq!(
            RabbitMqRequestClientConfigBuilder::new()
                .build()
                .max_in_flight,
            DEFAULT_MAX_IN_FLIGHT
        );
    }

    #[test]
    fn the_builder_carries_the_selected_bound_into_the_config() {
        assert_eq!(
            RabbitMqRequestClientConfigBuilder::new()
                .max_in_flight(7)
                .build()
                .max_in_flight,
            7
        );
    }

    /// A bound of zero is a documented way to refuse every request without
    /// stopping the process, so the builder must carry it through untouched
    /// rather than treating it as "unset" and silently restoring the default.
    #[test]
    fn the_builder_carries_a_zero_bound_rather_than_restoring_the_default() {
        assert_eq!(
            RabbitMqRequestClientConfigBuilder::new()
                .max_in_flight(0)
                .build()
                .max_in_flight,
            0
        );
    }

    /// The invariant of resolution 2, attested rather than merely
    /// documented: `mark_reconnecting_then_drain` must have already
    /// written `Reconnecting` by the time `drain` runs, not after.
    ///
    /// The `drain` closure reads `reply_inbox` itself, synchronously, at
    /// the exact instant it is invoked: this is what makes the test
    /// discriminate the two orders. A test that only inspected the final
    /// state after the call returned would pass whichever order the
    /// production code used, since both writes happen before either the
    /// closure captures anything at all: catching that pitfall is the
    /// whole point of routing `drain` through a closure instead of
    /// letting this function call `registry.drain()` directly.
    ///
    /// The trailing assertion is a postcondition, checked after the call
    /// returns rather than from inside the closure: it catches a
    /// production edit that wrote `Reconnecting`, drained, then wrote
    /// `Ready` back, which the in-closure assertion alone would never
    /// see since it only inspects the single instant `drain` runs.
    ///
    /// Runs with no broker, no tokio runtime at all: `mark_reconnecting_then_drain`
    /// is plain synchronous code.
    #[test]
    fn marks_reconnecting_before_draining() {
        let reply_inbox = Mutex::new(ReplyInboxState::Ready("inbox-1".to_owned()));
        let mut drain_observed_reconnecting = false;

        mark_reconnecting_then_drain(&reply_inbox, || {
            drain_observed_reconnecting = matches!(
                *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner),
                ReplyInboxState::Reconnecting
            );
        });

        assert!(
            drain_observed_reconnecting,
            "drain must observe the inbox already marked Reconnecting"
        );
        assert_eq!(
            *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner),
            ReplyInboxState::Reconnecting,
            "the mark must still hold once the function has returned, not just at the \
             instant drain ran"
        );
    }

    /// Closes I1/I2: the composition used to be implicit at the call
    /// site, where dropping the drain half left every other test green.
    /// This registers a slot in a real `RequestRegistry`, calls
    /// `on_connection_lost` the same way the supervisor loop does, and
    /// checks both halves actually ran: the state is `Reconnecting`, and
    /// the registered slot is gone, its waiting caller observing a
    /// closed channel rather than merely an absent entry.
    ///
    /// The state check alone would not catch a call site that dropped
    /// the drain: that is exactly the mutation this test exists to
    /// catch, and it is deliberately called out in `on_connection_lost`
    /// proof below.
    ///
    /// No broker and no tokio runtime: `pending.wait()` is polled once by
    /// hand with a no-op waker, which is enough since a closed channel
    /// resolves on its very first poll.
    #[test]
    fn on_connection_lost_marks_reconnecting_and_closes_the_waiting_callers_channel() {
        let registry = RequestRegistry::default();
        let reply_inbox = Mutex::new(ReplyInboxState::Ready("inbox-1".to_owned()));
        let mut pending = registry
            .register(RequestId::new(), ReplyExpectation::new("test.reply"))
            .expect("registration succeeds");

        on_connection_lost(&reply_inbox, &registry);

        assert_eq!(
            *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner),
            ReplyInboxState::Reconnecting
        );
        assert!(
            registry.is_empty(),
            "on_connection_lost must remove the registered slot, not just mark the state"
        );

        let mut future = pin!(pending.wait());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(
            matches!(future.as_mut().poll(&mut context), Poll::Ready(Err(_))),
            "the caller still waiting on its PendingReply must observe a closed channel"
        );
    }

    #[tokio::test]
    async fn supervisor_marks_and_drains_before_it_starts_reconnecting() {
        let registry = Arc::new(RequestRegistry::default());
        let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Ready("inbox-1".to_owned())));
        let mut pending = registry
            .register(RequestId::new(), ReplyExpectation::new("test.reply"))
            .expect("registration succeeds");
        let cancel = CancellationToken::new();
        let reconnect_started = Arc::new(Notify::new());
        let supervisor_finished = Arc::new(Notify::new());

        let _supervisor = RequestClientSupervisor::spawn(cancel.clone(), {
            let registry = Arc::clone(&registry);
            let reply_inbox = Arc::clone(&reply_inbox);
            let reconnect_started = Arc::clone(&reconnect_started);
            let supervisor_finished = Arc::clone(&supervisor_finished);
            move |cancel| async move {
                supervise_reply_inbox(
                    (),
                    registry,
                    reply_inbox,
                    cancel,
                    |(), _| async { Err(BusError::connection(io::Error::other("lost"), true)) },
                    move |cancel| {
                        let reconnect_started = Arc::clone(&reconnect_started);
                        async move {
                            reconnect_started.notify_one();
                            cancel.cancelled().await;
                            None
                        }
                    },
                )
                .await;
                supervisor_finished.notify_one();
            }
        });

        reconnect_started.notified().await;
        assert_eq!(
            *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner),
            ReplyInboxState::Reconnecting,
            "the real supervisor must mark Reconnecting before reconnecting"
        );
        assert!(
            registry.is_empty(),
            "the real supervisor must drain pending calls before reconnecting"
        );
        assert!(
            pending.wait().await.is_err(),
            "the pending caller must be released before reconnecting starts"
        );

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), supervisor_finished.notified())
            .await
            .expect("supervisor must stop once cancellation reaches reconnecting");
    }

    #[tokio::test]
    async fn reconnect_policy_delays_after_a_failure_and_cancellation_interrupts_the_wait() {
        let cancel = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let first_attempt = Arc::new(Notify::new());
        let second_attempt = Arc::new(Notify::new());

        let reconnect = tokio::spawn({
            let cancel = cancel.clone();
            let attempts = Arc::clone(&attempts);
            let first_attempt = Arc::clone(&first_attempt);
            let second_attempt = Arc::clone(&second_attempt);
            async move {
                retry_reply_inbox_after_failures(&cancel, Duration::from_secs(60), move || {
                    let attempts = Arc::clone(&attempts);
                    let first_attempt = Arc::clone(&first_attempt);
                    let second_attempt = Arc::clone(&second_attempt);
                    async move {
                        match attempts.fetch_add(1, Ordering::SeqCst) {
                            0 => first_attempt.notify_one(),
                            _ => second_attempt.notify_one(),
                        }
                        Err::<(), _>(ReconnectFailure::NeedsBackoff)
                    }
                })
                .await
            }
        });

        first_attempt.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), second_attempt.notified())
                .await
                .is_err(),
            "a failed reconnect must wait before starting another attempt"
        );

        cancel.cancel();
        assert_eq!(
            reconnect.await.expect("reconnect task must not panic"),
            None,
            "cancellation must interrupt the reconnect backoff"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "cancellation during the delay must not start a new attempt"
        );
    }

    #[tokio::test]
    async fn reconnect_policy_cancellation_interrupts_an_attempt_in_progress() {
        let cancel = CancellationToken::new();
        let attempt_started = Arc::new(Notify::new());

        let reconnect = tokio::spawn({
            let cancel = cancel.clone();
            let attempt_started = Arc::clone(&attempt_started);
            async move {
                retry_reply_inbox_after_failures(&cancel, Duration::from_secs(60), move || {
                    let attempt_started = Arc::clone(&attempt_started);
                    async move {
                        attempt_started.notify_one();
                        std::future::pending::<Result<(), ReconnectFailure>>().await
                    }
                })
                .await
            }
        });

        attempt_started.notified().await;
        cancel.cancel();
        assert_eq!(
            reconnect.await.expect("reconnect task must not panic"),
            None,
            "cancellation must interrupt a broker operation that has not returned"
        );
    }

    /// [`RabbitMqConnection::connect_with_retry`] only spends its attempt
    /// budget on a transient failure. A permanent one breaks out of that
    /// budget on the first attempt without sleeping at all (#340), so
    /// reading every connect failure as "already delayed" leaves the
    /// reconnect loop hammering a broker that has already refused the
    /// handshake (#495), which is exactly what a rotated credential or a
    /// revoked vhost permission produces.
    #[test]
    fn a_permanent_connect_failure_still_earns_a_backoff() {
        assert_eq!(
            classify_connect_failure(&BusError::connection("ACCESS_REFUSED", false)),
            ReconnectFailure::NeedsBackoff
        );
    }

    #[test]
    fn a_transient_connect_failure_has_already_spent_its_budget() {
        assert_eq!(
            classify_connect_failure(&BusError::connection("broker unreachable", true)),
            ReconnectFailure::AlreadyBackedOff
        );
    }

    /// Only [`BusError::Connection`] carries the transience flag. Any other
    /// variant proves nothing about a delay having been spent, so it earns
    /// one rather than being given the benefit of the doubt.
    #[test]
    fn a_failure_that_is_not_a_connection_error_earns_a_backoff() {
        assert_eq!(
            classify_connect_failure(&BusError::Internal("unexpected".to_owned())),
            ReconnectFailure::NeedsBackoff
        );
    }

    /// The other half of the contract: a failure that did consume a backoff
    /// must not be delayed a second time, or every broker blip would be
    /// waited out twice over.
    ///
    /// The attempt yields before failing, the way a real connect yields on
    /// its socket. Without that, a loop with no delay would starve the
    /// current-thread runtime instead of reaching the third attempt.
    #[tokio::test]
    async fn an_already_backed_off_failure_retries_without_a_further_delay() {
        let cancel = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let third_attempt = Arc::new(Notify::new());

        let reconnect = tokio::spawn({
            let cancel = cancel.clone();
            let attempts = Arc::clone(&attempts);
            let third_attempt = Arc::clone(&third_attempt);
            async move {
                retry_reply_inbox_after_failures(&cancel, Duration::from_secs(60), move || {
                    let attempts = Arc::clone(&attempts);
                    let third_attempt = Arc::clone(&third_attempt);
                    async move {
                        tokio::task::yield_now().await;
                        if attempts.fetch_add(1, Ordering::SeqCst) >= 2 {
                            third_attempt.notify_one();
                        }
                        Err::<(), _>(ReconnectFailure::AlreadyBackedOff)
                    }
                })
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), third_attempt.notified())
            .await
            .expect("a failure that already backed off must retry without waiting again");

        cancel.cancel();
        assert_eq!(
            reconnect.await.expect("reconnect task must not panic"),
            None,
            "cancellation must still stop the loop"
        );
    }

    /// The guard this fix exists for. A setup failure after a successful
    /// connect still owns a live AMQP session, and lapin's own close on drop
    /// is fire-and-forget: nothing orders it against the next connect. Setup
    /// must therefore await its own close before the error reaches the
    /// caller. Deleting the close from [`setup_reply_inbox_or_close`] fails
    /// here.
    #[tokio::test]
    async fn a_failed_reply_inbox_setup_closes_its_connection() {
        let cancel = CancellationToken::new();
        let closed = Arc::new(AtomicBool::new(false));

        let outcome: Result<(), &str> = setup_reply_inbox_or_close(
            &cancel,
            Duration::from_secs(1),
            || async { Err("channel setup refused") },
            || {
                let closed = Arc::clone(&closed);
                async move {
                    closed.store(true, Ordering::SeqCst);
                }
            },
        )
        .await;

        assert_eq!(
            outcome,
            Err("channel setup refused"),
            "the setup error must reach the caller unchanged"
        );
        assert!(
            closed.load(Ordering::SeqCst),
            "a failed setup must close the connection it was handed"
        );
    }

    /// The mirror guard: the close belongs to the failure path only. Closing
    /// on success would tear down the very connection carrying the inbox that
    /// was just declared.
    #[tokio::test]
    async fn a_successful_reply_inbox_setup_keeps_its_connection() {
        let cancel = CancellationToken::new();
        let closed = Arc::new(AtomicBool::new(false));

        let outcome: Result<&str, &str> = setup_reply_inbox_or_close(
            &cancel,
            Duration::from_secs(1),
            || async { Ok("inbox") },
            || {
                let closed = Arc::clone(&closed);
                async move {
                    closed.store(true, Ordering::SeqCst);
                }
            },
        )
        .await;

        assert_eq!(
            outcome,
            Ok("inbox"),
            "a successful setup must hand its inbox back"
        );
        assert!(
            !closed.load(Ordering::SeqCst),
            "a successful setup must not close the connection it just used"
        );
    }

    /// A close that answers within its bound reports completion. That report
    /// is what separates a session known to be closed from one merely
    /// abandoned, which is the distinction the warning at the deadline reads.
    #[tokio::test]
    async fn a_close_that_answers_in_time_reports_completion() {
        let cancel = CancellationToken::new();
        let close_started = Arc::new(AtomicBool::new(false));

        let closed = close_failed_reply_inbox_connection(&cancel, Duration::from_secs(1), {
            let close_started = Arc::clone(&close_started);
            async move {
                close_started.store(true, Ordering::SeqCst);
            }
        })
        .await;

        assert!(closed, "the failed setup must await its close attempt");
        assert!(
            close_started.load(Ordering::SeqCst),
            "the connection close future must be polled"
        );
    }

    /// The close handshake uses the broker, so it may itself stop making
    /// progress during an outage. Cancellation must win over that handshake.
    ///
    /// On today's single call path [`retry_reply_inbox_after_failures`]
    /// already drops the whole attempt on cancellation, so this arm is
    /// defence in depth rather than the reason `RequestClient::close`
    /// terminates. It is kept so the helper stays correct on its own, for a
    /// future caller that is not wrapped in that outer select.
    #[tokio::test]
    async fn cancellation_interrupts_a_failed_setup_connection_close() {
        let cancel = CancellationToken::new();
        let close_started = Arc::new(Notify::new());

        let close = tokio::spawn({
            let cancel = cancel.clone();
            let close_started = Arc::clone(&close_started);
            async move {
                close_failed_reply_inbox_connection(&cancel, Duration::from_secs(1), async move {
                    close_started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
            }
        });

        close_started.notified().await;
        cancel.cancel();

        assert!(
            !tokio::time::timeout(Duration::from_secs(2), close)
                .await
                .expect("cancellation must interrupt the close handshake")
                .expect("close task must not panic"),
            "an interrupted close must report that it did not finish"
        );
    }

    /// A healthy but unresponsive peer can accept the connection then never
    /// answer `Connection.Close-Ok`. The cleanup is best-effort, so its own
    /// bound must release the reconnect loop even without cancellation.
    #[tokio::test]
    async fn failed_reply_inbox_setup_close_stops_at_its_deadline() {
        let cancel = CancellationToken::new();

        let closed = close_failed_reply_inbox_connection(
            &cancel,
            Duration::from_millis(50),
            std::future::pending(),
        )
        .await;

        assert!(
            !closed,
            "an unresponsive close must give up at its configured bound"
        );
    }
}
