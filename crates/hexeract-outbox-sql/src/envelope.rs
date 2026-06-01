use std::time::SystemTime;

use hexeract_outbox::OutboxEnvelope;
use time::OffsetDateTime;
#[cfg(feature = "mysql")]
use time::PrimitiveDateTime;
use uuid::Uuid;

/// Convert a [`SystemTime`] into the `sqlx`-encodable [`OffsetDateTime`].
pub(crate) fn to_offset(t: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(t)
}

/// Convert an [`OffsetDateTime`] decoded from the database back into a [`SystemTime`].
pub(crate) fn to_system_time(o: OffsetDateTime) -> SystemTime {
    SystemTime::from(o)
}

/// Convert a [`SystemTime`] into a UTC [`PrimitiveDateTime`] for a MySQL `DATETIME`.
///
/// MySQL `DATETIME` carries no time zone, so the value is normalized to UTC
/// before the offset is dropped. The store reads it back with
/// [`primitive_utc_to_system_time`].
#[cfg(feature = "mysql")]
pub(crate) fn to_primitive_utc(t: SystemTime) -> PrimitiveDateTime {
    let odt = OffsetDateTime::from(t);
    PrimitiveDateTime::new(odt.date(), odt.time())
}

/// Interpret a [`PrimitiveDateTime`] read from a MySQL `DATETIME` as UTC and
/// convert it back into a [`SystemTime`].
#[cfg(feature = "mysql")]
pub(crate) fn primitive_utc_to_system_time(p: PrimitiveDateTime) -> SystemTime {
    SystemTime::from(p.assume_utc())
}

/// Build an [`OutboxEnvelope`] from the scalar columns a backend store
/// decodes from a polled row.
///
/// Centralizes the column-to-envelope mapping so every backend shares it.
/// A polled row is never delivered yet, so `delivered_at` is always `None`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble_envelope(
    event_id: Uuid,
    event_type: String,
    payload: Vec<u8>,
    subject_id: Option<Uuid>,
    created_at: SystemTime,
    attempts: u32,
    last_error: Option<String>,
    next_retry_at: Option<SystemTime>,
) -> OutboxEnvelope {
    OutboxEnvelope::restore(
        event_id,
        event_type,
        payload,
        subject_id,
        created_at,
        attempts,
        last_error,
        next_retry_at,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn system_time_round_trips_through_offset_date_time() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_456_789);
        let restored = to_system_time(to_offset(original));
        assert_eq!(restored, original);
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn system_time_round_trips_through_primitive_utc() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_456_000);
        let restored = primitive_utc_to_system_time(to_primitive_utc(original));
        assert_eq!(restored, original);
    }

    #[test]
    fn assemble_envelope_maps_columns_and_leaves_undelivered() {
        let event_id = Uuid::from_u128(1);
        let subject = Uuid::from_u128(2);
        let created = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let envelope = assemble_envelope(
            event_id,
            "users.registered".to_owned(),
            b"{\"user_id\":\"x\"}".to_vec(),
            Some(subject),
            created,
            3,
            Some("boom".to_owned()),
            None,
        );

        assert_eq!(envelope.event_id, event_id);
        assert_eq!(envelope.event_type, "users.registered");
        assert_eq!(envelope.payload, b"{\"user_id\":\"x\"}".to_vec());
        assert_eq!(envelope.subject_id, Some(subject));
        assert_eq!(envelope.created_at, created);
        assert_eq!(envelope.attempts, 3);
        assert_eq!(envelope.last_error.as_deref(), Some("boom"));
        assert!(envelope.next_retry_at.is_none());
        assert!(envelope.delivered_at.is_none());
    }
}
