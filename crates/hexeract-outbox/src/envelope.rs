use std::time::SystemTime;

use uuid::Uuid;

use crate::Event;
use crate::OutboxError;

/// Persisted representation of an event awaiting dispatch.
///
/// An envelope carries the serialized payload plus every column needed by
/// the worker to poll, dispatch and retry the event. Backend crates map
/// this struct to and from their physical schema.
///
/// The `Debug` implementation masks the payload bytes to avoid leaking
/// potentially sensitive event data into logs and tracing output.
#[derive(Clone)]
#[non_exhaustive]
pub struct OutboxEnvelope {
    /// Stable identifier of the event, set by the caller (typically a `UUIDv7`).
    pub event_id: Uuid,
    /// Routing key matching [`Event::EVENT_TYPE`] of the original event.
    pub event_type: String,
    /// JSON-serialized payload of the original event.
    pub payload: Vec<u8>,
    /// Optional aggregate identifier used for partial ordering by subject.
    pub subject_id: Option<Uuid>,
    /// Instant at which the envelope was created.
    pub created_at: SystemTime,
    /// Number of dispatch attempts already consumed.
    pub attempts: u32,
    /// Error message captured during the last failed attempt, if any.
    pub last_error: Option<String>,
    /// Earliest instant at which the next attempt is allowed.
    pub next_retry_at: Option<SystemTime>,
    /// Instant at which the event was successfully dispatched.
    pub delivered_at: Option<SystemTime>,
}

impl std::fmt::Debug for OutboxEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboxEnvelope")
            .field("event_id", &self.event_id)
            .field("event_type", &self.event_type)
            .field("payload", &format_args!("<{} bytes>", self.payload.len()))
            .field("subject_id", &self.subject_id)
            .field("created_at", &self.created_at)
            .field("attempts", &self.attempts)
            .field("last_error", &self.last_error)
            .field("next_retry_at", &self.next_retry_at)
            .field("delivered_at", &self.delivered_at)
            .finish()
    }
}

impl OutboxEnvelope {
    /// Build a fresh envelope from a domain event.
    ///
    /// The payload is serialized as JSON. The envelope starts with zero
    /// attempts, no recorded error and no delivery timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Serialization`] if the event payload cannot
    /// be encoded as JSON.
    pub fn new<E: Event>(event_id: Uuid, event: &E) -> Result<Self, OutboxError> {
        let payload = serde_json::to_vec(event)?;
        Ok(Self {
            event_id,
            event_type: E::EVENT_TYPE.to_owned(),
            payload,
            subject_id: None,
            created_at: SystemTime::now(),
            attempts: 0,
            last_error: None,
            next_retry_at: None,
            delivered_at: None,
        })
    }

    /// Build a fresh envelope tagged with an aggregate subject.
    ///
    /// Use this constructor when partial ordering matters: the worker can
    /// guarantee that events sharing a `subject_id` are dispatched in
    /// insertion order.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Serialization`] if the event payload cannot
    /// be encoded as JSON.
    pub fn with_subject<E: Event>(
        event_id: Uuid,
        subject_id: Uuid,
        event: &E,
    ) -> Result<Self, OutboxError> {
        let mut envelope = Self::new(event_id, event)?;
        envelope.subject_id = Some(subject_id);
        Ok(envelope)
    }

    /// Deserialize the payload back into the strongly-typed event.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::TypeMismatch`] if the envelope's `event_type`
    /// does not match [`Event::EVENT_TYPE`] of the requested type, or
    /// [`OutboxError::Serialization`] if the payload cannot be decoded as
    /// JSON of the target type.
    pub fn decode<E: Event>(&self) -> Result<E, OutboxError> {
        if self.event_type != E::EVENT_TYPE {
            return Err(OutboxError::TypeMismatch {
                expected: E::EVENT_TYPE,
                actual: self.event_type.clone(),
            });
        }
        serde_json::from_slice(&self.payload).map_err(OutboxError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct UserRegistered {
        user_id: Uuid,
    }

    impl Event for UserRegistered {
        const EVENT_TYPE: &'static str = "users.registered";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: Uuid,
    }

    impl Event for OrderPlaced {
        const EVENT_TYPE: &'static str = "orders.placed";
    }

    fn sample_event() -> UserRegistered {
        UserRegistered {
            user_id: Uuid::nil(),
        }
    }

    #[test]
    fn new_records_event_type_and_zero_attempts() {
        let envelope = OutboxEnvelope::new(Uuid::nil(), &sample_event()).unwrap();
        assert_eq!(envelope.event_type, "users.registered");
        assert_eq!(envelope.attempts, 0);
        assert!(envelope.last_error.is_none());
        assert!(envelope.next_retry_at.is_none());
        assert!(envelope.delivered_at.is_none());
        assert!(envelope.subject_id.is_none());
    }

    #[test]
    fn new_serializes_payload_as_json() {
        let envelope = OutboxEnvelope::new(Uuid::nil(), &sample_event()).unwrap();
        let raw = std::str::from_utf8(&envelope.payload).unwrap();
        assert!(raw.contains("\"user_id\""));
    }

    #[test]
    fn with_subject_records_subject_id() {
        let subject = Uuid::from_u128(42);
        let envelope = OutboxEnvelope::with_subject(Uuid::nil(), subject, &sample_event()).unwrap();
        assert_eq!(envelope.subject_id, Some(subject));
    }

    #[test]
    fn decode_round_trip_returns_original_event() {
        let original = sample_event();
        let envelope = OutboxEnvelope::new(Uuid::nil(), &original).unwrap();
        let decoded: UserRegistered = envelope.decode().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_preserves_subject_id_alongside_payload() {
        let subject = Uuid::from_u128(99);
        let envelope = OutboxEnvelope::with_subject(Uuid::nil(), subject, &sample_event()).unwrap();
        let decoded: UserRegistered = envelope.decode().unwrap();
        assert_eq!(decoded, sample_event());
        assert_eq!(envelope.subject_id, Some(subject));
    }

    #[test]
    fn debug_masks_payload_bytes() {
        let envelope = OutboxEnvelope::new(Uuid::nil(), &sample_event()).unwrap();
        let debug_output = format!("{envelope:?}");
        assert!(debug_output.contains('<'));
        assert!(debug_output.contains("bytes>"));
        assert!(!debug_output.contains("user_id"));
    }

    #[test]
    fn decode_rejects_mismatched_event_type() {
        let envelope = OutboxEnvelope::new(Uuid::nil(), &sample_event()).unwrap();
        let err = envelope.decode::<OrderPlaced>().unwrap_err();
        match err {
            OutboxError::TypeMismatch { expected, actual } => {
                assert_eq!(expected, "orders.placed");
                assert_eq!(actual, "users.registered");
            }
            other => panic!("expected OutboxError::TypeMismatch, got {other:?}"),
        }
    }
}
