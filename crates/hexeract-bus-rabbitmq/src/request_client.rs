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

use crate::connection::{DEFAULT_RETRY_ATTEMPTS, DEFAULT_RETRY_BASE_DELAY, RabbitMqConnection};
use crate::reply_inbox::{declare_reply_inbox, run_reply_inbox};
use crate::transport::RabbitMqTransport;

/// Assemble a ready RabbitMQ request client.
///
/// Requests are published through a recovering publisher connection
/// (healed transparently, see [`RabbitMqTransport::new`]). Replies are
/// received on a dedicated exclusive inbox consumed over a separate
/// supervised connection that detects broker loss; a background
/// supervisor drains the in-flight registry and re-declares the inbox
/// on reconnect. See the [module docs](self) for the full contract.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if either the recovering publisher
/// connection or the initial supervised inbox connection cannot be
/// established.
pub async fn connect_request_client(
    uri: &str,
    default_timeout: Duration,
    cancel: CancellationToken,
) -> Result<RequestClient<RabbitMqTransport>, BusError> {
    let transport = Arc::new(RabbitMqTransport::new(uri).await?);
    let registry = Arc::new(RequestRegistry::new(DEFAULT_MAX_IN_FLIGHT));

    // Supervised connection for the inbox consumer: NOT the recovering
    // connection used by the publisher above.
    let connection = RabbitMqConnection::connect_with_retry(
        uri,
        DEFAULT_RETRY_ATTEMPTS,
        DEFAULT_RETRY_BASE_DELAY,
    )
    .await?;
    let channel = connection.create_channel().await?;
    let inbox_name = declare_reply_inbox(&channel).await?;
    let reply_inbox = Arc::new(Mutex::new(ReplyInboxState::Ready(inbox_name.clone())));

    let supervisor = spawn_reply_inbox_supervisor(
        uri,
        channel,
        inbox_name,
        Arc::clone(&registry),
        Arc::clone(&reply_inbox),
        cancel,
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
fn spawn_reply_inbox_supervisor(
    uri: &str,
    channel: Channel,
    inbox: String,
    registry: Arc<RequestRegistry>,
    reply_inbox: Arc<Mutex<ReplyInboxState>>,
    cancel: CancellationToken,
) -> RequestClientSupervisor {
    let active_inbox = Arc::new(Mutex::new(Some((channel, inbox))));
    let run_inbox = Arc::clone(&active_inbox);
    let reconnect_inbox = Arc::clone(&active_inbox);
    let reconnect_uri = uri.to_owned();
    let reconnect_state = Arc::clone(&reply_inbox);
    let run_registry = Arc::clone(&registry);

    RequestClientSupervisor::spawn(cancel, move |cancel| async move {
        supervise_reply_inbox(
            registry,
            reply_inbox,
            cancel,
            move |cancel| {
                let (channel, inbox) = run_inbox
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                    .expect("every reply-inbox run has an active channel and inbox");
                run_reply_inbox(channel, inbox, Arc::clone(&run_registry), cancel)
            },
            move |cancel| {
                let reconnect_inbox = Arc::clone(&reconnect_inbox);
                let reconnect_uri = reconnect_uri.clone();
                let reconnect_state = Arc::clone(&reconnect_state);
                async move {
                    let Some((channel, inbox)) =
                        reconnect_reply_inbox(&reconnect_uri, &reconnect_state, &cancel).await
                    else {
                        return false;
                    };
                    *reconnect_inbox
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = Some((channel, inbox));
                    true
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
async fn supervise_reply_inbox<Run, RunFuture, Reconnect, ReconnectFuture>(
    registry: Arc<RequestRegistry>,
    reply_inbox: Arc<Mutex<ReplyInboxState>>,
    cancel: CancellationToken,
    mut run_once: Run,
    mut reconnect: Reconnect,
) where
    Run: FnMut(CancellationToken) -> RunFuture,
    RunFuture: Future<Output = Result<(), BusError>>,
    Reconnect: FnMut(CancellationToken) -> ReconnectFuture,
    ReconnectFuture: Future<Output = bool>,
{
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let outcome = run_once(cancel.clone()).await;
        if cancel.is_cancelled() || outcome.is_ok() {
            return;
        }

        // Connection lost: fail every in-flight request fast rather than let
        // it run out its timeout against a dead inbox, and refuse calls that
        // have not registered yet. The order is non-negotiable.
        on_connection_lost(&reply_inbox, &registry);

        if !reconnect(cancel.clone()).await {
            return;
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
/// [`RequestClient`] on every request) before returning it. Each
/// reconnect attempt is bounded by
/// [`RabbitMqConnection::connect_with_retry`]'s own attempt budget; a
/// failed attempt loops back for another rather than giving up.
async fn reconnect_reply_inbox(
    uri: &str,
    reply_inbox: &Mutex<ReplyInboxState>,
    cancel: &CancellationToken,
) -> Option<(Channel, String)> {
    loop {
        if cancel.is_cancelled() {
            return None;
        }
        let Ok(connection) = RabbitMqConnection::connect_with_retry(
            uri,
            DEFAULT_RETRY_ATTEMPTS,
            DEFAULT_RETRY_BASE_DELAY,
        )
        .await
        else {
            continue;
        };
        let Ok(channel) = connection.create_channel().await else {
            continue;
        };
        let Ok(inbox) = declare_reply_inbox(&channel).await else {
            continue;
        };
        *reply_inbox.lock().unwrap_or_else(PoisonError::into_inner) =
            ReplyInboxState::Ready(inbox.clone());
        return Some((channel, inbox));
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use hexeract_bus::ReplyExpectation;
    use hexeract_core::RequestId;
    use tokio::sync::Notify;

    use super::*;

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
                    registry,
                    reply_inbox,
                    cancel,
                    |_| async { Err(BusError::connection(io::Error::other("lost"), true)) },
                    move |cancel| {
                        let reconnect_started = Arc::clone(&reconnect_started);
                        async move {
                            reconnect_started.notify_one();
                            cancel.cancelled().await;
                            false
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
}
