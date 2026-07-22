//! Consumer for the exclusive, auto-delete reply inbox used by the
//! request-reply client path.
//!
//! [`declare_reply_inbox`] declares a server-named exclusive queue that
//! dies with the connection; [`run_reply_inbox`] consumes it with
//! `no_ack` and routes every delivery to a
//! [`hexeract_bus::RequestRegistry`] by request id, so a caller
//! waiting on a [`hexeract_bus::PendingReply`] is woken as soon as its
//! reply arrives.
//!
//! # Design
//!
//! The inbox consumer must run on a connection distinct from the
//! auto-recovering publisher connection: lapin's native auto-recovery
//! would keep the consumer stream alive across a broker drop and mask
//! the outage from the supervisor that owns this consumer's lifecycle.
//! See [`run_reply_inbox`] for the resulting contract on connection loss.

use std::sync::Arc;

use futures_util::StreamExt;
use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::RequestRegistry;
use lapin::Channel;
use lapin::options::BasicConsumeOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use tokio_util::sync::CancellationToken;

use crate::transport::to_short_string;
use crate::worker::DEFAULT_MAX_PAYLOAD_BYTES;
use crate::worker::delivery_to_envelope;

/// Declare an exclusive, auto-delete, server-named reply inbox and
/// return its generated name.
///
/// The queue dies with the connection: on reconnect the caller must
/// declare a fresh inbox and mint a new name, since a stale inbox name
/// is never delivered to again once its owning connection is gone.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if the broker rejects the
/// declaration.
pub async fn declare_reply_inbox(channel: &Channel) -> Result<String, BusError> {
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                durable: false,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|err| BusError::connection(Box::new(err), true))?;
    Ok(queue.name().as_str().to_owned())
}

/// Decode one AMQP delivery from the reply inbox into a [`BusEnvelope`].
///
/// Delegates to [`crate::worker::delivery_to_envelope`] so the reply
/// inbox and the regular consumer worker share exactly the same
/// AMQP-property-to-envelope reconstruction: `message_type` from the
/// `type` property, `correlation_id` and `reply_to` from their
/// respective properties, and free-form headers.
fn decode_delivery(delivery: &lapin::message::Delivery) -> Result<BusEnvelope, BusError> {
    delivery_to_envelope(
        &delivery.properties,
        &delivery.data,
        DEFAULT_MAX_PAYLOAD_BYTES,
    )
}

/// Consume the reply inbox with `no_ack` and route each delivery to
/// `registry` by request id.
///
/// Runs until `cancel` fires, returning `Ok(())`. A delivery that fails
/// to decode into a [`BusEnvelope`] is logged and dropped rather than
/// tearing down the consumer, since a foreign or malformed message on the
/// inbox is untrusted input, not a framework bug.
///
/// # Errors
///
/// Returns [`BusError::Connection`] (always `retryable: true`) if the
/// consumer cannot be established or if the delivery stream ends before
/// cancellation, so a supervisor can drain in-flight requests and
/// re-declare a fresh inbox on a new connection.
pub async fn run_reply_inbox(
    channel: Channel,
    inbox: String,
    registry: Arc<RequestRegistry>,
    cancel: CancellationToken,
) -> Result<(), BusError> {
    let mut consumer = channel
        .basic_consume(
            to_short_string(inbox.as_str(), "reply inbox queue name")?,
            to_short_string("hexeract-reply-inbox", "consumer tag")?,
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|err| BusError::connection(Box::new(err), true))?;

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            next = consumer.next() => match next {
                Some(Ok(delivery)) => match decode_delivery(&delivery) {
                    Ok(envelope) => registry.resolve(envelope),
                    Err(error) => {
                        tracing::warn!(%error, "undecodable reply delivery, dropping");
                    }
                },
                Some(Err(error)) => {
                    return Err(BusError::connection(Box::new(error), true));
                }
                None => {
                    return Err(BusError::connection(
                        "reply inbox consumer stream ended: connection or channel lost",
                        true,
                    ));
                }
            }
        }
    }
}
