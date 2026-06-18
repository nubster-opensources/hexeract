use async_trait::async_trait;
use uuid::Uuid;

use crate::BusError;

/// Contract for publishing a pre-serialized message with a caller-supplied
/// `message_id`.
///
/// Unlike [`Transport`](crate::Transport), which takes a typed
/// [`Message`](crate::Message), serializes it and mints a fresh `message_id`,
/// this trait publishes the raw `message_type` and `payload` already produced
/// upstream under an identifier the caller chooses. It exists for producers
/// that re-emit a message they did not originate and need the published
/// identity to be stable rather than freshly minted.
///
/// # Stable identity and deduplication
///
/// The caller owns the meaning of `message_id`. A producer that may republish
/// the same logical message (for example a scheduler redelivering a due
/// occurrence after a crash) should pass a stable, content-addressed
/// identifier. Delivery across the broker is at-least-once, so a redelivery
/// reaches consumers more than once; propagating a stable `message_id` lets
/// consumers deduplicate on it.
#[async_trait]
pub trait RawBusPublish: Send + Sync + 'static {
    /// Publish a raw, pre-serialized message under `routing_key` with the
    /// caller-supplied `message_id`.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Serialization`] if the payload is malformed for the
    /// backend, [`BusError::Connection`] if the broker is unreachable, or
    /// [`BusError::Transport`] if the broker rejected the publish.
    async fn publish_raw(
        &self,
        routing_key: &str,
        message_id: Uuid,
        message_type: &str,
        payload: &[u8],
    ) -> Result<(), BusError>;
}
