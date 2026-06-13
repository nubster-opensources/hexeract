use std::time::SystemTime;

use crate::error::SchedulerError;

/// Minimum number of whitespace-separated fields in a cron expression.
const MIN_CRON_FIELDS: usize = 5;
/// Maximum number of whitespace-separated fields in a cron expression: the
/// optional leading seconds field brings the count to six.
const MAX_CRON_FIELDS: usize = 6;

/// A structurally validated cron expression evaluated in UTC.
///
/// [`CronExpression::parse`] checks that the expression is non-empty and
/// carries five or six whitespace-separated fields. Full field-level
/// semantics (ranges, steps, named months) are resolved by the cron engine
/// when computing the next occurrence, not here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronExpression(String);

impl CronExpression {
    /// Parse and validate a cron expression.
    ///
    /// Surrounding whitespace is trimmed. The expression must carry five
    /// fields (`minute hour day-of-month month day-of-week`) or six when a
    /// leading seconds field is present.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::InvalidTrigger`] if the expression is empty
    /// or does not carry five or six fields.
    pub fn parse(expression: &str) -> Result<Self, SchedulerError> {
        let trimmed = expression.trim();
        if trimmed.is_empty() {
            return Err(SchedulerError::invalid_trigger(
                "cron expression must not be empty",
            ));
        }
        let field_count = trimmed.split_whitespace().count();
        if !(MIN_CRON_FIELDS..=MAX_CRON_FIELDS).contains(&field_count) {
            return Err(SchedulerError::invalid_trigger(format!(
                "cron expression must carry {MIN_CRON_FIELDS} or {MAX_CRON_FIELDS} fields, got {field_count}"
            )));
        }
        Ok(Self(trimmed.to_owned()))
    }

    /// Return the validated expression text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
    use std::time::{Duration, UNIX_EPOCH};

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
}
