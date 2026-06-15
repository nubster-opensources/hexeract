use std::time::{Duration, SystemTime, UNIX_EPOCH};

use time::OffsetDateTime;

use crate::error::SchedulerError;

/// Nanoseconds in one second, used to convert between `SystemTime` and
/// `time::OffsetDateTime` without relying on a panicking conversion.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// A fully validated cron expression evaluated in UTC.
///
/// [`CronExpression::parse`] validates the expression with the `isochron`
/// cron engine: field count, ranges, steps, lists, named months and days,
/// and macros such as `@daily` are all checked up front, so an accepted
/// expression is guaranteed to re-parse when computing occurrences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronExpression(String);

impl CronExpression {
    /// Parse and validate a cron expression.
    ///
    /// Surrounding whitespace is trimmed. The expression must be a valid five
    /// field cron expression (`minute hour day-of-month month day-of-week`),
    /// a six field expression with a leading seconds field, or a supported
    /// macro such as `@daily`. Validation is delegated to the `isochron`
    /// engine, so field-level semantics (ranges, steps, lists, named months
    /// and days) are checked here.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::InvalidTrigger`] if the expression is not a
    /// valid cron expression.
    pub fn parse(expression: &str) -> Result<Self, SchedulerError> {
        let trimmed = expression.trim();
        isochron::CronSchedule::parse(trimmed)
            .map_err(|error| SchedulerError::invalid_trigger(error.to_string()))?;
        Ok(Self(trimmed.to_owned()))
    }

    /// Return the validated expression text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The next occurrence strictly after `after`, evaluated in UTC.
    ///
    /// Returns `None` when no occurrence exists within the engine's bounded
    /// search horizon (for example an expression that can never match).
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Internal`] if `after` or the computed
    /// occurrence falls outside the representable date range.
    pub fn next_occurrence(&self, after: SystemTime) -> Result<Option<SystemTime>, SchedulerError> {
        let schedule = isochron::CronSchedule::parse(&self.0).map_err(|error| {
            SchedulerError::internal(format!(
                "validated cron expression failed to re-parse: {error}"
            ))
        })?;
        let anchor = to_offset_date_time(after)?;
        match schedule.next_after(anchor) {
            Some(occurrence) => Ok(Some(to_system_time(occurrence)?)),
            None => Ok(None),
        }
    }

    /// The next due instant under the fire-once misfire policy.
    ///
    /// The search is anchored on `max(now, previous_due)`, so occurrences
    /// missed while the worker was down collapse into a single fire before
    /// the schedule realigns on the future.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Internal`] if an instant falls outside the
    /// representable date range.
    pub fn next_due(
        &self,
        now: SystemTime,
        previous_due: SystemTime,
    ) -> Result<Option<SystemTime>, SchedulerError> {
        self.next_occurrence(now.max(previous_due))
    }
}

/// Convert a `SystemTime` to a UTC `OffsetDateTime` without panicking.
fn to_offset_date_time(instant: SystemTime) -> Result<OffsetDateTime, SchedulerError> {
    let nanos = match instant.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => i128::try_from(elapsed.as_nanos())
            .map_err(|_| SchedulerError::internal("instant too far in the future to represent"))?,
        Err(before_epoch) => {
            let magnitude = i128::try_from(before_epoch.duration().as_nanos()).map_err(|_| {
                SchedulerError::internal("instant too far in the past to represent")
            })?;
            -magnitude
        }
    };
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|_| SchedulerError::internal("instant outside the supported date range"))
}

/// Convert a UTC `OffsetDateTime` back to a `SystemTime` without panicking.
fn to_system_time(moment: OffsetDateTime) -> Result<SystemTime, SchedulerError> {
    let nanos = moment.unix_timestamp_nanos();
    let seconds = nanos.div_euclid(NANOS_PER_SECOND);
    let subsec_nanos = u32::try_from(nanos.rem_euclid(NANOS_PER_SECOND))
        .map_err(|_| SchedulerError::internal("sub-second component out of range"))?;
    if seconds >= 0 {
        let seconds = u64::try_from(seconds).map_err(|_| {
            SchedulerError::internal("occurrence too far in the future to represent")
        })?;
        Ok(UNIX_EPOCH + Duration::new(seconds, subsec_nanos))
    } else {
        let seconds = u64::try_from(-seconds)
            .map_err(|_| SchedulerError::internal("occurrence too far in the past to represent"))?;
        Ok(
            (UNIX_EPOCH - Duration::new(seconds, 0))
                + Duration::from_nanos(u64::from(subsec_nanos)),
        )
    }
}

/// When a scheduled message fires.
///
/// Marked `#[non_exhaustive]`: build instances through the constructors so
/// new trigger kinds can be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Trigger {
    /// Fire once at the given instant.
    Delay(SystemTime),
    /// Fire repeatedly on a UTC cron schedule.
    Cron(CronExpression),
}

impl Trigger {
    /// Fire once at `at`.
    #[must_use]
    pub fn delay(at: SystemTime) -> Self {
        Self::Delay(at)
    }

    /// Fire repeatedly on the cron `expression`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::InvalidTrigger`] if `expression` is not a
    /// structurally valid cron expression (see [`CronExpression::parse`]).
    pub fn cron(expression: &str) -> Result<Self, SchedulerError> {
        Ok(Self::Cron(CronExpression::parse(expression)?))
    }

    /// Whether the trigger fires more than once.
    #[must_use]
    pub fn is_recurring(&self) -> bool {
        matches!(self, Self::Cron(_))
    }
}

#[cfg(test)]
mod tests {
    use super::{CronExpression, Trigger};
    use crate::error::SchedulerError;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use time::OffsetDateTime;
    use time::macros::datetime;

    fn at(moment: OffsetDateTime) -> SystemTime {
        let secs = moment.unix_timestamp();
        if secs >= 0 {
            UNIX_EPOCH + Duration::from_secs(u64::try_from(secs).expect("non-negative"))
        } else {
            UNIX_EPOCH - Duration::from_secs(u64::try_from(-secs).expect("magnitude"))
        }
    }

    fn unix_secs(instant: SystemTime) -> i64 {
        match instant.duration_since(UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_secs()).expect("fits"),
            Err(before) => -i64::try_from(before.duration().as_secs()).expect("fits"),
        }
    }

    #[test]
    fn delay_holds_the_instant_and_is_not_recurring() {
        let at = UNIX_EPOCH + Duration::from_secs(60);
        let trigger = Trigger::delay(at);
        assert_eq!(trigger, Trigger::Delay(at));
        assert!(!trigger.is_recurring());
    }

    #[test]
    fn cron_accepts_a_five_field_expression() {
        let trigger = Trigger::cron("0 0 * * *").expect("valid cron");
        assert!(trigger.is_recurring());
        match trigger {
            Trigger::Cron(expr) => assert_eq!(expr.as_str(), "0 0 * * *"),
            other => panic!("expected Trigger::Cron, got {other:?}"),
        }
    }

    #[test]
    fn cron_accepts_a_six_field_expression_with_seconds() {
        assert!(Trigger::cron("0 0 0 * * *").is_ok());
    }

    #[test]
    fn cron_trims_surrounding_whitespace() {
        let expr = CronExpression::parse("  0 0 * * *  ").expect("valid cron");
        assert_eq!(expr.as_str(), "0 0 * * *");
    }

    #[test]
    fn cron_rejects_an_empty_expression() {
        let error = Trigger::cron("   ").unwrap_err();
        assert!(matches!(error, SchedulerError::InvalidTrigger { .. }));
    }

    #[test]
    fn cron_rejects_a_wrong_field_count() {
        let error = Trigger::cron("* * *").unwrap_err();
        assert!(matches!(error, SchedulerError::InvalidTrigger { .. }));
    }

    #[test]
    fn cron_rejects_an_out_of_range_minute() {
        let error = Trigger::cron("99 * * * *").unwrap_err();
        assert!(matches!(error, SchedulerError::InvalidTrigger { .. }));
    }

    #[test]
    fn cron_rejects_an_out_of_range_day_of_month() {
        assert!(Trigger::cron("* * 32 * *").is_err());
    }

    #[test]
    fn next_occurrence_crosses_month_end() {
        let expr = CronExpression::parse("0 0 * * *").expect("valid cron");
        let after = at(datetime!(2026-02-28 12:00:00 UTC));
        let next = expr
            .next_occurrence(after)
            .expect("conversion succeeds")
            .expect("occurrence exists");
        assert_eq!(
            unix_secs(next),
            datetime!(2026-03-01 00:00:00 UTC).unix_timestamp()
        );
    }

    #[test]
    fn next_occurrence_handles_the_leap_day() {
        let expr = CronExpression::parse("0 0 29 2 *").expect("valid cron");
        let next = expr
            .next_occurrence(at(datetime!(2024-02-01 00:00:00 UTC)))
            .expect("conversion succeeds")
            .expect("occurrence exists");
        assert_eq!(
            unix_secs(next),
            datetime!(2024-02-29 00:00:00 UTC).unix_timestamp()
        );
        let after_leap = expr
            .next_occurrence(at(datetime!(2024-02-29 00:00:00 UTC)))
            .expect("conversion succeeds")
            .expect("occurrence exists");
        assert_eq!(
            unix_secs(after_leap),
            datetime!(2028-02-29 00:00:00 UTC).unix_timestamp()
        );
    }

    #[test]
    fn next_occurrence_is_strictly_after_the_anchor() {
        let expr = CronExpression::parse("0 0 * * *").expect("valid cron");
        let next = expr
            .next_occurrence(at(datetime!(2026-06-15 00:00:00 UTC)))
            .expect("conversion succeeds")
            .expect("occurrence exists");
        assert_eq!(
            unix_secs(next),
            datetime!(2026-06-16 00:00:00 UTC).unix_timestamp()
        );
    }

    #[test]
    fn next_due_collapses_missed_occurrences() {
        let expr = CronExpression::parse("0 * * * *").expect("valid cron");
        let now = at(datetime!(2026-06-15 10:30:00 UTC));
        let previous_due = at(datetime!(2026-06-14 09:00:00 UTC));
        let due = expr
            .next_due(now, previous_due)
            .expect("conversion succeeds")
            .expect("occurrence exists");
        assert_eq!(
            unix_secs(due),
            datetime!(2026-06-15 11:00:00 UTC).unix_timestamp()
        );
    }

    #[test]
    fn next_due_anchors_on_a_future_previous_due() {
        let expr = CronExpression::parse("*/15 * * * *").expect("valid cron");
        let now = at(datetime!(2026-06-15 10:05:00 UTC));
        let previous_due = at(datetime!(2026-06-15 10:20:00 UTC));
        let due = expr
            .next_due(now, previous_due)
            .expect("conversion succeeds")
            .expect("occurrence exists");
        assert_eq!(
            unix_secs(due),
            datetime!(2026-06-15 10:30:00 UTC).unix_timestamp()
        );
    }
}
