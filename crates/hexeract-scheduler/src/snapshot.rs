use std::time::SystemTime;

use uuid::Uuid;

use crate::trigger::Trigger;

/// Lifecycle state of a schedule.
///
/// Marked `#[non_exhaustive]` so new states can be added without a breaking
/// change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScheduleStatus {
    /// Eligible to fire once due and not leased.
    Pending,
    /// Intentionally suspended; excluded from claiming until resumed.
    Paused,
    /// A one-shot schedule that has been dispatched.
    Delivered,
    /// Cancelled before completion; excluded from claiming.
    Cancelled,
    /// Moved to the dead-letter state after exhausting its attempts.
    DeadLettered,
}

/// A read-only view of a schedule's current state.
///
/// Returned by [`crate::ScheduleStore::inspect`]. Marked
/// `#[non_exhaustive]`: backends build it through [`ScheduleSnapshot::new`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ScheduleSnapshot {
    /// Identifier of the schedule.
    pub schedule_id: Uuid,
    /// Current lifecycle state.
    pub status: ScheduleStatus,
    /// Instant of the current occurrence.
    pub scheduled_for: SystemTime,
    /// Attempts consumed for the current occurrence.
    pub attempts: u32,
    /// Maximum attempts allowed before dead-lettering.
    pub max_attempts: u32,
    /// The recurrence rule.
    pub trigger: Trigger,
    /// Error recorded for the last failed attempt, if any.
    pub last_error: Option<String>,
}

impl ScheduleSnapshot {
    /// Build a snapshot from its parts.
    ///
    /// Intended for backend implementations of [`crate::ScheduleStore`].
    #[must_use]
    pub fn new(
        schedule_id: Uuid,
        status: ScheduleStatus,
        scheduled_for: SystemTime,
        attempts: u32,
        max_attempts: u32,
        trigger: Trigger,
        last_error: Option<String>,
    ) -> Self {
        Self {
            schedule_id,
            status,
            scheduled_for,
            attempts,
            max_attempts,
            trigger,
            last_error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ScheduleSnapshot, ScheduleStatus};
    use crate::trigger::Trigger;
    use std::time::{Duration, UNIX_EPOCH};
    use uuid::Uuid;

    #[test]
    fn new_round_trips_every_field() {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        let snapshot = ScheduleSnapshot::new(
            Uuid::from_u128(7),
            ScheduleStatus::Pending,
            at,
            1,
            5,
            Trigger::delay(at),
            Some("boom".to_owned()),
        );
        assert_eq!(snapshot.schedule_id, Uuid::from_u128(7));
        assert_eq!(snapshot.status, ScheduleStatus::Pending);
        assert_eq!(snapshot.scheduled_for, at);
        assert_eq!(snapshot.attempts, 1);
        assert_eq!(snapshot.max_attempts, 5);
        assert_eq!(snapshot.trigger, Trigger::delay(at));
        assert_eq!(snapshot.last_error.as_deref(), Some("boom"));
    }
}
