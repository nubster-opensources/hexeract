use uuid::Uuid;

use crate::OutboxError;

/// Contract for inserting a pre-serialized event into the outbox idempotently.
///
/// Unlike [`OutboxPublisher`](crate::OutboxPublisher), which takes a typed
/// [`Event`](crate::Event) and serializes it, this trait accepts the raw
/// `event_type` and `payload` already produced upstream, together with a
/// caller-supplied `event_id`. The insert is keyed on `event_id` and is a
/// no-op when a row with that identifier already exists, so the same logical
/// event can be enqueued more than once without producing a duplicate row.
///
/// # Idempotency key
///
/// The caller owns the meaning of `event_id`. A producer that may replay the
/// same logical event (for example a scheduler redelivering a due occurrence
/// after a crash) should derive a stable, content-addressed identifier so the
/// retries collapse onto a single row. The backend relies on the unique index
/// over `event_id` to reject the duplicate silently.
///
/// # Ordering
///
/// The outbox worker polls by ascending `event_id` (a `UUIDv7` carries an
/// embedded timestamp). A caller that supplies an identifier of another kind
/// trades that insertion-order property for deterministic idempotency; the
/// rows are still delivered, only their relative poll order is unspecified.
#[trait_variant::make(Send)]
pub trait IdempotentOutboxEnqueue: Send + Sync + 'static {
    /// Insert a raw event keyed idempotently on `event_id`.
    ///
    /// Returns `true` when a new row was inserted, `false` when a row with
    /// `event_id` already existed and the insert was skipped.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Database`] if the backend fails to insert.
    async fn enqueue_idempotent(
        &self,
        event_id: Uuid,
        event_type: &str,
        payload: &[u8],
    ) -> Result<bool, OutboxError>;
}
