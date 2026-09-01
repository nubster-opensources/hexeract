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
use lapin::BasicProperties;
use lapin::Channel;
use lapin::options::BasicConsumeOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use tokio_util::sync::CancellationToken;

use crate::metadata::AmqpMetadataLimits;
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
fn decode_delivery(
    properties: &BasicProperties,
    payload: &[u8],
    metadata_limits: AmqpMetadataLimits,
) -> Result<BusEnvelope, BusError> {
    delivery_to_envelope(
        properties,
        payload,
        DEFAULT_MAX_PAYLOAD_BYTES,
        metadata_limits,
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
    run_reply_inbox_with_limits(
        channel,
        inbox,
        registry,
        cancel,
        AmqpMetadataLimits::default(),
    )
    .await
}

/// Consume the reply inbox under caller-selected metadata limits.
///
/// The reply path applies exactly the same limits, through exactly the same
/// decoder, as the normal worker: a reply inbox that accepted metadata the
/// worker refuses would be a complete bypass of the worker's bound, and it is
/// the path that feeds an RPC correlation slot.
///
/// # Errors
///
/// Same contract as [`run_reply_inbox`].
pub(crate) async fn run_reply_inbox_with_limits(
    channel: Channel,
    inbox: String,
    registry: Arc<RequestRegistry>,
    cancel: CancellationToken,
    metadata_limits: AmqpMetadataLimits,
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
                Some(Ok(delivery)) => match decode_delivery(
                    &delivery.properties,
                    &delivery.data,
                    metadata_limits,
                ) {
                    Ok(envelope) => registry.resolve(envelope),
                    // The typed error carries a reason and sizes only, never a
                    // header key or value, and the delivery is dropped under
                    // the existing no_ack contract before it can take a
                    // correlation slot.
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

#[cfg(test)]
mod tests {
    use hexeract_bus::REQUEST_ID_HEADER;
    use lapin::types::AMQPValue;

    use super::*;

    /// Build reply properties carrying `headers` and the AMQP `type` a
    /// delivery needs to decode into an envelope.
    fn reply_properties<'a>(
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> BasicProperties {
        let mut table = FieldTable::default();
        for (key, value) in headers {
            table.insert(key.into(), AMQPValue::LongString(value.as_bytes().into()));
        }
        BasicProperties::default()
            .with_type("orders.replied".into())
            .with_headers(table)
    }

    #[test]
    fn a_bounded_reply_decodes_and_keeps_its_protocol_header() {
        let properties = reply_properties([(REQUEST_ID_HEADER, "request-1")]);
        let envelope = decode_delivery(&properties, b"{}", AmqpMetadataLimits::default())
            .expect("a bounded reply must decode");
        assert_eq!(envelope.header(REQUEST_ID_HEADER), Some("request-1"));
    }

    #[test]
    fn oversized_reply_metadata_fails_before_resolution() {
        let properties = reply_properties([(REQUEST_ID_HEADER, "request-1")]);
        let limits = AmqpMetadataLimits {
            max_headers: 0,
            ..AmqpMetadataLimits::default()
        };
        assert!(
            matches!(
                decode_delivery(&properties, b"{}", limits),
                Err(BusError::MetadataLimitExceeded {
                    limit: hexeract_bus::MetadataLimit::HeaderCount,
                    actual: 1,
                    max: 0,
                })
            ),
            "an oversized reply must fail decoding, never reach RequestRegistry::resolve"
        );
    }
}
