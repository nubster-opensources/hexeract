//! Per-dialect conversions between [`SystemTime`] and the timestamp
//! representation each backend binds and decodes.
//!
//! PostgreSQL keeps `TIMESTAMPTZ` (microsecond precision), MySQL keeps
//! `DATETIME(6)` holding UTC, and SQLite keeps RFC 3339 `TEXT` at millisecond
//! precision. The string layout matches the dialect's
//! `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')` expression so lexicographic
//! comparisons against the stored timestamps stay correct.

use std::time::SystemTime;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use time::OffsetDateTime;
#[cfg(any(feature = "mysql", feature = "sqlite"))]
use time::PrimitiveDateTime;

#[cfg(feature = "sqlite")]
use hexeract_scheduler::SchedulerError;

/// Convert a [`SystemTime`] into the `OffsetDateTime` bound to a PostgreSQL
/// `TIMESTAMPTZ`.
#[cfg(feature = "postgres")]
pub(crate) fn to_offset_date_time(t: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(t)
}

/// Convert an `OffsetDateTime` decoded from a PostgreSQL `TIMESTAMPTZ` back
/// into a [`SystemTime`].
#[cfg(feature = "postgres")]
pub(crate) fn from_offset_date_time(o: OffsetDateTime) -> SystemTime {
    SystemTime::from(o)
}

/// Convert a [`SystemTime`] into the UTC `PrimitiveDateTime` bound to a MySQL
/// `DATETIME(6)`.
#[cfg(feature = "mysql")]
pub(crate) fn to_primitive_utc(t: SystemTime) -> PrimitiveDateTime {
    let utc = OffsetDateTime::from(t);
    PrimitiveDateTime::new(utc.date(), utc.time())
}

/// Interpret a `PrimitiveDateTime` decoded from a MySQL `DATETIME(6)` as UTC
/// and convert it back into a [`SystemTime`].
#[cfg(feature = "mysql")]
pub(crate) fn from_primitive_utc(p: PrimitiveDateTime) -> SystemTime {
    SystemTime::from(p.assume_utc())
}

/// Format a [`SystemTime`] as a UTC RFC 3339 string with millisecond precision
/// for a SQLite `TEXT` column.
///
/// The layout matches the dialect's `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`
/// expression so lexicographic comparisons against the stored timestamps stay
/// correct. SQLite keeps millisecond resolution: any sub-millisecond component
/// is truncated.
///
/// # Errors
///
/// Returns [`SchedulerError::Internal`] if the value cannot be formatted.
#[cfg(feature = "sqlite")]
pub(crate) fn format_sqlite_utc(t: SystemTime) -> Result<String, SchedulerError> {
    let fmt = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    );
    OffsetDateTime::from(t)
        .format(fmt)
        .map_err(|e| SchedulerError::internal(format!("sqlite timestamp format failed: {e}")))
}

/// Parse a SQLite `TEXT` timestamp into a [`SystemTime`], interpreting it as
/// UTC.
///
/// Two layouts are accepted so the store keeps reading rows written by
/// something other than [`format_sqlite_utc`]:
///
/// - the canonical millisecond RFC 3339 form this crate writes,
///   `YYYY-MM-DDTHH:MM:SS.mmmZ`;
/// - the SQLite native `datetime('now')` form `YYYY-MM-DD HH:MM:SS` (space
///   separator, no fractional seconds), which a hand-written migration or an
///   external writer commonly produces.
///
/// # Errors
///
/// Returns [`SchedulerError::Internal`] if the text matches neither accepted
/// layout.
#[cfg(feature = "sqlite")]
pub(crate) fn parse_sqlite_utc(s: &str) -> Result<SystemTime, SchedulerError> {
    let rfc3339_millis = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    );
    if let Ok(parsed) = PrimitiveDateTime::parse(s, rfc3339_millis) {
        return Ok(SystemTime::from(parsed.assume_utc()));
    }

    let sqlite_canonical =
        time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    let parsed = PrimitiveDateTime::parse(s, sqlite_canonical).map_err(|e| {
        SchedulerError::internal(format!("sqlite timestamp parse failed for {s:?}: {e}"))
    })?;
    Ok(SystemTime::from(parsed.assume_utc()))
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use std::time::Duration;

    #[cfg(feature = "postgres")]
    #[test]
    fn system_time_round_trips_through_offset_date_time() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_456_789);
        assert_eq!(
            from_offset_date_time(to_offset_date_time(original)),
            original
        );
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn system_time_round_trips_through_primitive_utc() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_456_000);
        assert_eq!(from_primitive_utc(to_primitive_utc(original)), original);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn system_time_round_trips_through_sqlite_text_at_millisecond_precision() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_000_000);
        let text = format_sqlite_utc(original).unwrap();
        assert!(text.contains('T'));
        assert!(text.ends_with('Z'));
        assert_eq!(parse_sqlite_utc(&text).unwrap(), original);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_text_truncates_sub_millisecond_precision() {
        let original = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_900_000);
        let truncated = SystemTime::UNIX_EPOCH + Duration::new(1_750_000_000, 123_000_000);
        let restored = parse_sqlite_utc(&format_sqlite_utc(original).unwrap()).unwrap();
        assert_eq!(restored, truncated);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn parse_sqlite_utc_accepts_the_canonical_datetime_now_form() {
        let parsed = parse_sqlite_utc("2024-01-01 12:00:00").unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_secs(1_704_110_400);
        assert_eq!(parsed, expected);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn parse_sqlite_utc_rejects_garbage() {
        assert!(parse_sqlite_utc("not a timestamp").is_err());
    }
}
