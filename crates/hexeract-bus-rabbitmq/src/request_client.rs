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
//! - On `Err` (the broker connection was lost), the task
//!   [`hexeract_bus::RequestRegistry::drain`]s every in-flight slot
//!   so a caller waiting on a reply observes
//!   [`hexeract_bus::RequestError::Transport`] immediately instead of
//!   waiting out its timeout, reconnects over a fresh supervised
//!   connection, declares a fresh exclusive inbox (the previous one
//!   died with its connection), publishes the new name into the
//!   `Arc<Mutex<String>>` the [`hexeract_bus::RequestClient`] reads on
//!   every request, and resumes consuming.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hexeract_bus::{BusError, DEFAULT_MAX_IN_FLIGHT, RequestClient, RequestRegistry};
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
    let reply_inbox = Arc::new(Mutex::new(inbox_name.clone()));

    let finished = CancellationToken::new();

    let supervisor = spawn_reply_inbox_supervisor(
        uri.to_owned(),
        channel,
        inbox_name,
        Arc::clone(&registry),
        Arc::clone(&reply_inbox),
        cancel.clone(),
        finished.clone(),
    );

    Ok(RequestClient::new(
        transport,
        registry,
        reply_inbox,
        default_timeout,
        cancel,
        Some((supervisor, finished)),
    ))
}

/// Cancels its token on drop: the last thing that happens to this guard,
/// on every exit path of the task that owns it, panic included.
///
/// [`spawn_reply_inbox_supervisor`]'s task body has three `return` points
/// (cancellation observed at the top of the loop, a plain `Ok(())` or an
/// already-observed cancellation after [`run_reply_inbox`], and a failed
/// reconnect). Cancelling `finished` by hand at each of them would work
/// today, but silently stops working the moment a fourth exit point is
/// added and its author forgets the cancellation. Tying the cancellation
/// to this guard's `Drop` instead makes forgetting it impossible: the
/// token is cancelled exactly when the task's stack unwinds, regardless
/// of which `return` triggered it.
struct FinishedGuard(CancellationToken);

impl Drop for FinishedGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Drive the reply inbox consumer, rebuilding it across a broker drop.
///
/// Runs until `cancel` fires or [`run_reply_inbox`] returns `Ok(())`.
/// On `Err` (connection lost), drains `registry` so in-flight callers
/// fail fast, then hands off to [`reconnect_reply_inbox`] for a fresh
/// connection and inbox before resuming.
///
/// Returns the spawned task's [`tokio::task::JoinHandle`] rather than
/// detaching it: [`connect_request_client`] hands that handle to the
/// [`RequestClient`] it assembles, so `RequestClient::close` can await
/// this task's actual termination instead of merely cancelling it.
///
/// `finished` is cancelled, through a [`FinishedGuard`], right before the
/// task actually returns, on every exit path: this is the signal a
/// `RequestClient::close` caller that finds the join handle already taken
/// by a concurrent caller waits on instead, so it too observes genuine
/// termination rather than returning early.
fn spawn_reply_inbox_supervisor(
    uri: String,
    channel: Channel,
    inbox: String,
    registry: Arc<RequestRegistry>,
    reply_inbox: Arc<Mutex<String>>,
    cancel: CancellationToken,
    finished: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        let _finished_guard = FinishedGuard(finished);
        let mut channel = channel;
        let mut inbox = inbox;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let outcome =
                run_reply_inbox(channel, inbox, Arc::clone(&registry), cancel.clone()).await;
            if cancel.is_cancelled() || outcome.is_ok() {
                return;
            }

            // Connection lost: fail every in-flight request fast rather
            // than let it run out its timeout against a dead inbox.
            registry.drain();

            match reconnect_reply_inbox(&uri, &reply_inbox, &cancel).await {
                Some((new_channel, new_inbox)) => {
                    channel = new_channel;
                    inbox = new_inbox;
                }
                None => return,
            }
        }
    })
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
    reply_inbox: &Mutex<String>,
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
        reply_inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone_from(&inbox);
        return Some((channel, inbox));
    }
}
