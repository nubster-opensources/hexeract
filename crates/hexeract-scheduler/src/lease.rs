use std::time::SystemTime;

use crate::occurrence::OccurrenceId;
use crate::schedule::ScheduledMessage;

/// A due occurrence claimed under a soft lease, ready to dispatch.
///
/// Returned by [`crate::ScheduleStore::claim_due`]. The claim has already
/// advanced the attempt counter and stamped the lease, so the worker can
/// dispatch outside any transaction: a competing worker skips this
/// occurrence until [`Self::leased_until`] elapses. If the worker crashes
/// between claim and acknowledgement, the lease expires and the occurrence
/// is reclaimed, which is what makes delivery at-least-once.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LeasedOccurrence {
    /// The scheduled message to dispatch.
    pub message: ScheduledMessage,
    /// Attempts consumed so far, including the one this claim represents.
    pub attempts: u32,
    /// Maximum attempts allowed before the occurrence is dead-lettered.
    pub max_attempts: u32,
    /// Instant until which the claim holds the lease.
    pub leased_until: SystemTime,
}

impl LeasedOccurrence {
    /// Build a leased occurrence from its parts.
    ///
    /// Intended for backend implementations of [`crate::ScheduleStore`].
    #[must_use]
    pub fn new(
        message: ScheduledMessage,
        attempts: u32,
        max_attempts: u32,
        leased_until: SystemTime,
    ) -> Self {
        Self {
            message,
            attempts,
            max_attempts,
            leased_until,
        }
    }

    /// The stable identity of this occurrence, the deduplication key for
    /// at-least-once delivery.
    #[must_use]
    pub fn occurrence_id(&self) -> OccurrenceId {
        self.message.occurrence_id()
    }

    /// Whether the occurrence has consumed its entire attempt budget.
    ///
    /// The worker dead-letters the schedule when a dispatch fails and this
    /// returns `true`.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.attempts >= self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::LeasedOccurrence;
    use crate::schedule::ScheduledMessage;
    use crate::target::Target;
    use hexeract_outbox::Event;
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue;

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    fn leased(attempts: u32, max_attempts: u32) -> LeasedOccurrence {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        let message = ScheduledMessage::delay(Target::mediator(), at, &ReminderDue)
            .expect("serializes the payload");
        LeasedOccurrence::new(
            message,
            attempts,
            max_attempts,
            at + Duration::from_secs(30),
        )
    }

    #[test]
    fn is_exhausted_is_false_below_the_budget() {
        assert!(!leased(2, 5).is_exhausted());
    }

    #[test]
    fn is_exhausted_is_true_at_the_budget() {
        assert!(leased(5, 5).is_exhausted());
    }

    #[test]
    fn occurrence_id_delegates_to_the_message() {
        let occurrence = leased(1, 5);
        assert_eq!(
            occurrence.occurrence_id(),
            occurrence.message.occurrence_id()
        );
    }
}
