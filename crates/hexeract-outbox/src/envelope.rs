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
/// # Timestamp precision
///
/// The precision of [`Self::created_at`], [`Self::next_retry_at`] and
/// [`Self::delivered_at`] depends on the storage backend:
///
/// - **PostgreSQL** stores `TIMESTAMPTZ` with microsecond precision.
/// - **MySQL** stores `DATETIME(6)` with microsecond precision.
/// - **SQLite** stores UTC RFC 3339 strings at **millisecond** precision.
///   Sub-millisecond components are **truncated** (not rounded) on write.
///   Round-tripping a `SystemTime` with nanosecond precision through the
///   SQLite backend yields a value truncated to the nearest millisecond.
///
/// # Identifier limits (hexeract-outbox-sql)
///
/// The SQL backends enforce the following column widths:
///
/// - `event_type` is limited to **64 bytes**.
/// - Table-name identifiers passed to the builder are limited to **63 bytes**.
///
/// # Observability note
///
/// The `Debug` implementation never prints the bytes of `payload` or the text
/// of `last_error`. It renders `payload` as `<N bytes>`, and `last_error` as
/// its presence and its length in bytes alone, `Some(<N bytes redacted>)` or
/// `None`. An error message is free-form text a dispatcher captured from a
/// failure, and this type cannot tell a harmless message from one that
/// carries a datum.
///
/// To read the text of an error, ask for it: [`Self::last_error`] stays a
/// public field, and operators read it through the `check` command and the SQL
/// views. That path is deliberate. It makes the disclosure a decision at the
/// call site instead of a side effect of formatting, and the formatter does
/// not pretend to redact what the field exposes.
///
/// The limit: `event_type` is printed verbatim. The SQL backends constrain it
/// on their write paths, but [`Self::restore`] validates none of its fields,
/// so a row written outside the framework can carry arbitrary text into this
/// output. The type guarantees no more than that.
#[derive(Clone)]
#[non_exhaustive]
pub struct OutboxEnvelope {
    /// Stable identifier of the event, set by the caller (typically a `UUIDv7`).
    pub event_id: Uuid,
    /// Routing key matching [`Event::EVENT_TYPE`] of the original event.
    pub event_type: String,
    /// JSON-serialized payload of the original event.
    pub payload: Vec<u8>,
    /// Optional aggregate identifier used for partition routing.
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

/// Renders the byte length of a withheld error message in place of its text.
struct WithheldErrorLength(usize);

impl std::fmt::Debug for WithheldErrorLength {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} bytes redacted>", self.0)
    }
}

impl std::fmt::Debug for OutboxEnvelope {
    /// Destructures `Self` without a rest pattern on purpose: adding a field
    /// to the envelope breaks this function, which forces whoever adds it to
    /// decide whether the new field is safe to print. A sensitive field
    /// silently swept in by a `..` would be invisible to review.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            event_id,
            event_type,
            payload,
            subject_id,
            created_at,
            attempts,
            last_error,
            next_retry_at,
            delivered_at,
        } = self;

        f.debug_struct("OutboxEnvelope")
            .field("event_id", event_id)
            .field("event_type", event_type)
            .field("payload", &format_args!("<{} bytes>", payload.len()))
            .field("subject_id", subject_id)
            .field("created_at", created_at)
            .field("attempts", attempts)
            .field(
                "last_error",
                &last_error
                    .as_ref()
                    .map(|message| WithheldErrorLength(message.len())),
            )
            .field("next_retry_at", next_retry_at)
            .field("delivered_at", delivered_at)
            .finish()
    }
}

impl OutboxEnvelope {
    /// Build a fresh envelope from a domain event.
    ///
    /// The payload is serialized as JSON. The envelope starts with zero
    /// attempts, no recorded error and no delivery timestamp. Backends
    /// typically mint the `event_id` as a `UUIDv7` right before calling
    /// this constructor.
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
    /// The `subject_id` is informational and intended for partition routing
    /// (for example sharding dispatch across workers). Dispatch ordering is
    /// best-effort and not guaranteed: stores that claim rows with
    /// `FOR UPDATE SKIP LOCKED` let several workers relay concurrently, so
    /// envelopes sharing a `subject_id` may be dispatched out of insertion
    /// order or in parallel.
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

    /// Reconstruct a persisted envelope from its stored fields.
    ///
    /// Intended for backend implementations of
    /// [`crate::OutboxStore`] that read rows back from the storage
    /// layer. Application code should use [`Self::new`] or
    /// [`Self::with_subject`] instead.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        event_id: Uuid,
        event_type: String,
        payload: Vec<u8>,
        subject_id: Option<Uuid>,
        created_at: SystemTime,
        attempts: u32,
        last_error: Option<String>,
        next_retry_at: Option<SystemTime>,
        delivered_at: Option<SystemTime>,
    ) -> Self {
        Self {
            event_id,
            event_type,
            payload,
            subject_id,
            created_at,
            attempts,
            last_error,
            next_retry_at,
            delivered_at,
        }
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

    fn envelope_with_last_error(last_error: Option<&str>) -> OutboxEnvelope {
        OutboxEnvelope::restore(
            Uuid::nil(),
            "users.registered".to_owned(),
            Vec::new(),
            None,
            SystemTime::UNIX_EPOCH,
            1,
            last_error.map(str::to_owned),
            None,
            None,
        )
    }

    #[test]
    fn debug_never_prints_last_error_text() {
        let message = "postgres://svc:hunter2@db.internal:5432/app?sslmode=require";
        let envelope = envelope_with_last_error(Some(message));
        let debug_output = format!("{envelope:?}");
        assert!(
            !debug_output.contains(message),
            "full error text leaked in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("hunter2"),
            "credential fragment leaked in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("db.internal"),
            "host fragment leaked in Debug output: {debug_output}"
        );
    }

    #[test]
    fn debug_reveals_last_error_presence_and_byte_length_only() {
        let message = "connection refused by upstream";
        let envelope = envelope_with_last_error(Some(message));
        let debug_output = format!("{envelope:?}");
        let expected = format!("last_error: Some(<{} bytes redacted>)", message.len());
        assert!(
            debug_output.contains(&expected),
            "expected `{expected}` in Debug output: {debug_output}"
        );
        assert!(
            debug_output.contains("redacted"),
            "missing `redacted` marker in Debug output: {debug_output}"
        );
    }

    #[test]
    fn debug_renders_none_when_no_error_is_recorded() {
        let envelope = envelope_with_last_error(None);
        let debug_output = format!("{envelope:?}");
        assert!(
            debug_output.contains("last_error: None"),
            "expected `last_error: None` in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("redacted"),
            "unexpected `redacted` marker in Debug output: {debug_output}"
        );
    }

    #[test]
    fn debug_distinguishes_an_empty_error_from_no_error() {
        let envelope = envelope_with_last_error(Some(""));
        let debug_output = format!("{envelope:?}");
        assert!(
            debug_output.contains("last_error: Some(<0 bytes redacted>)"),
            "an empty recorded error must stay distinguishable from None: {debug_output}"
        );
    }

    #[test]
    fn debug_reports_last_error_length_in_bytes_not_characters() {
        let message = "échec à la connexion près de zoé";
        assert_ne!(message.len(), message.chars().count());
        let envelope = envelope_with_last_error(Some(message));
        let debug_output = format!("{envelope:?}");
        let expected_bytes = format!("<{} bytes redacted>", message.len());
        let wrong_characters = format!("<{} bytes redacted>", message.chars().count());
        assert!(
            debug_output.contains(&expected_bytes),
            "expected `{expected_bytes}` in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains(&wrong_characters),
            "character count used instead of byte length in Debug output: {debug_output}"
        );
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
