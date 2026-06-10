use std::time::SystemTime;

use hexeract_outbox::OutboxEnvelope;
#[cfg(feature = "sqlite")]
use hexeract_outbox::OutboxError;
use time::OffsetDateTime;
#[cfg(any(feature = "mysql", feature = "sqlite"))]
use time::PrimitiveDateTime;
use uuid::Uuid;

/// Convert a [`SystemTime`] into the `sqlx`-encodable [`OffsetDateTime`].
#[cfg(feature = "postgres")]
pub(crate) fn to_offset(t: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(t)
}

/// Convert an [`OffsetDateTime`] decoded from the database back into a [`SystemTime`].
#[cfg(feature = "postgres")]
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

/// Format a [`SystemTime`] as a UTC RFC 3339 string with millisecond precision
/// for a SQLite `TEXT` column.
///
/// The layout matches the dialect's `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`
/// expression so lexicographic comparisons against the stored timestamps stay
/// correct.
///
/// SQLite stores outbox timestamps at millisecond resolution. Any
/// sub-millisecond component of `t` is **truncated**, not rounded, when it is
/// formatted, so it does not survive a round-trip through [`parse_sqlite_utc`].
/// PostgreSQL (`TIMESTAMPTZ`) and MySQL (`DATETIME(6)`) keep finer,
/// microsecond resolution. The outbox tolerates the SQLite granularity: poll
/// ordering and `next_retry_at` scheduling only need lexicographically sortable
/// timestamps, and millisecond granularity is sufficient for both.
///
/// # Errors
///
/// Returns [`OutboxError::Internal`] if the value cannot be formatted.
#[cfg(feature = "sqlite")]
pub(crate) fn format_sqlite_utc(t: SystemTime) -> Result<String, OutboxError> {
    let fmt = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    );
    OffsetDateTime::from(t)
        .format(fmt)
        .map_err(|e| OutboxError::Internal(format!("sqlite timestamp format failed: {e}")))
}

/// Parse a SQLite `TEXT` timestamp written by [`format_sqlite_utc`] (or by the
/// `strftime` column default) back into a [`SystemTime`], interpreting it as UTC.
///
/// The stored text carries millisecond precision, so the returned
/// [`SystemTime`] is aligned to a whole millisecond: any sub-millisecond part of
/// the original value was already truncated by [`format_sqlite_utc`].
///
/// # Errors
///
/// Returns [`OutboxError::Internal`] if the text does not match the expected
/// `YYYY-MM-DDTHH:MM:SS.mmmZ` layout.
#[cfg(feature = "sqlite")]
pub(crate) fn parse_sqlite_utc(s: &str) -> Result<SystemTime, OutboxError> {
    let fmt = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    );
    let parsed = PrimitiveDateTime::parse(s, fmt)
        .map_err(|e| OutboxError::Internal(format!("sqlite timestamp parse failed: {e}")))?;
    Ok(SystemTime::from(parsed.assume_utc()))
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

    #[cfg(feature = "postgres")]
    #[test]
    fn system_time_round_trips_through_offset_date_time() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_456_789);
        let restored = to_system_time(to_offset(original));
        assert_eq!(restored, original);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn system_time_round_trips_through_sqlite_text() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_000_000);
        let text = format_sqlite_utc(original).unwrap();
        assert!(text.contains('T'));
        assert!(text.ends_with('Z'));
        let restored = parse_sqlite_utc(&text).unwrap();
        assert_eq!(restored, original);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_text_truncates_sub_millisecond_precision() {
        // 123_900_000 ns is 123.9 ms: the 900_000 ns tail sits past the
        // millisecond boundary. A value above the half-millisecond mark would
        // round up to 124 ms, so this case distinguishes truncation from
        // rounding rather than just precision loss.
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_900_000);
        let truncated = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_000_000);

        let restored = parse_sqlite_utc(&format_sqlite_utc(original).unwrap()).unwrap();

        assert_ne!(
            restored, original,
            "sub-millisecond precision must not survive a SQLite round-trip"
        );
        assert_eq!(
            restored, truncated,
            "the sub-millisecond tail must be truncated, not rounded"
        );
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
