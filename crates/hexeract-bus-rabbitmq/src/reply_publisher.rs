//! RabbitMQ reply publisher: replies always transit the default exchange.
//!
//! `reply_to` is caller-supplied, so a reply must never be routed by the
//! responder's application bindings. This publisher targets the AMQP default
//! exchange and its namespace policy restricts a reply destination to a
//! server-named inbox, which the broker generates under the reserved
//! `amq.gen-` prefix.

use hexeract_bus::{
    BoxFuture, BusEnvelope, BusError, ReplyDestination, ReplyDestinationError, ReplyPublisher,
    Transport,
};

use crate::RabbitMqConnection;
use crate::transport::RabbitMqTransport;

/// The prefix RabbitMQ reserves for server-generated queue names.
const SERVER_NAMED_INBOX_PREFIX: &str = "amq.gen-";

/// Apply the AMQP reply-destination policy to a caller-supplied `reply_to`.
///
/// Runs the transport-neutral rules first, then requires the server-named
/// inbox prefix. A name that is well formed but not a generated inbox (an
/// application queue, or a reserved name such as `amq.direct`) is rejected as
/// [`ReplyDestinationError::OutsideReplyNamespace`].
///
/// # Errors
///
/// Returns the [`ReplyDestinationError`] describing the first rule violated.
pub(crate) fn accept_reply_destination(
    raw: &str,
) -> Result<ReplyDestination, ReplyDestinationError> {
    let destination = ReplyDestination::parse(raw)?;
    if destination.as_str().starts_with(SERVER_NAMED_INBOX_PREFIX) {
        Ok(destination)
    } else {
        Err(ReplyDestinationError::OutsideReplyNamespace)
    }
}

/// Publishes replies to the AMQP default exchange.
///
/// Built over a [`RabbitMqTransport`] that targets the default exchange; no
/// constructor exposes another exchange, so a reply cannot be routed through
/// an application exchange.
#[derive(Debug)]
pub struct RabbitMqReplyPublisher {
    transport: RabbitMqTransport,
}

impl RabbitMqReplyPublisher {
    /// Build a reply publisher over `connection`, always targeting the AMQP
    /// default exchange.
    #[must_use]
    pub fn new(connection: RabbitMqConnection, pool_size: usize) -> Self {
        Self {
            transport: RabbitMqTransport::from_connection(connection, pool_size),
        }
    }
}

impl ReplyPublisher for RabbitMqReplyPublisher {
    fn publish_reply<'a>(
        &'a self,
        destination: &'a ReplyDestination,
        envelope: &'a BusEnvelope,
    ) -> BoxFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            self.transport
                .publish_envelope(destination.as_str(), envelope)
                .await
                .map(drop)
        })
    }

    fn accept_destination(&self, raw: &str) -> Result<ReplyDestination, ReplyDestinationError> {
        accept_reply_destination(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_bus::ReplyDestinationError;

    #[test]
    fn the_backend_accepts_a_server_named_inbox() {
        assert!(accept_reply_destination("amq.gen-Xa8sK2p_QwertY").is_ok());
    }

    #[test]
    fn the_backend_rejects_an_application_queue_name() {
        assert_eq!(
            accept_reply_destination("orders.inbox"),
            Err(ReplyDestinationError::OutsideReplyNamespace)
        );
    }

    #[test]
    fn the_backend_rejects_a_reserved_name_that_is_not_a_generated_inbox() {
        assert_eq!(
            accept_reply_destination("amq.direct"),
            Err(ReplyDestinationError::OutsideReplyNamespace)
        );
    }

    #[test]
    fn the_backend_still_applies_the_neutral_rules_first() {
        // An empty name fails the neutral rule before the namespace rule is
        // ever reached, proving parse runs first.
        assert_eq!(
            accept_reply_destination(""),
            Err(ReplyDestinationError::Empty)
        );
    }
}
