use std::time::SystemTime;

use hexeract_outbox::Event;
use uuid::Uuid;

use crate::error::SchedulerError;
use crate::occurrence::OccurrenceId;
use crate::target::Target;
use crate::trigger::Trigger;

/// A message persisted for future delivery.
///
/// A scheduled message pairs the serialized event with its dispatch
/// [`Target`] and its [`Trigger`], plus the instant of the current
/// occurrence ([`Self::scheduled_for`]). Backends map this struct to and
/// from their physical schema; the worker reads it back to dispatch a due
/// occurrence.
///
/// # Occurrence instant
///
/// [`Self::scheduled_for`] is the UTC instant of the firing the message
/// currently represents. For a [`Trigger::Delay`] it equals the delay
/// instant. For a [`Trigger::Cron`] it is the next occurrence computed by
/// the cron engine; the trigger itself carries only the recurrence rule.
///
/// The `Debug` implementation masks the payload bytes to avoid leaking
/// potentially sensitive event data into logs and tracing output.
#[derive(Clone)]
#[non_exhaustive]
pub struct ScheduledMessage {
    /// Stable identifier of the schedule, minted as a `UUIDv7` on creation.
    pub schedule_id: Uuid,
    /// Routing key matching [`Event::EVENT_TYPE`] of the original event.
    pub event_type: String,
    /// JSON-serialized payload of the original event.
    pub payload: Vec<u8>,
    /// Where the due occurrence is dispatched.
    pub target: Target,
    /// When the message fires.
    pub trigger: Trigger,
    /// UTC instant of the current occurrence.
    pub scheduled_for: SystemTime,
}

impl ScheduledMessage {
    /// Build a message that fires once at `at`.
    ///
    /// The schedule identifier is minted as a `UUIDv7` and
    /// [`Self::scheduled_for`] is set to `at`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Serialization`] if the event payload cannot
    /// be encoded as JSON.
    pub fn delay<E: Event>(
        target: Target,
        at: SystemTime,
        event: &E,
    ) -> Result<Self, SchedulerError> {
        Self::from_parts(target, Trigger::delay(at), at, event)
    }

    /// Build a message that fires repeatedly on `expression`, with its first
    /// occurrence at `first_occurrence`.
    ///
    /// The schedule identifier is minted as a `UUIDv7`. Subsequent
    /// occurrences are computed by the cron engine when the message is
    /// rescheduled.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::InvalidTrigger`] if `expression` is not a
    /// structurally valid cron expression, or
    /// [`SchedulerError::Serialization`] if the event payload cannot be
    /// encoded as JSON.
    pub fn cron<E: Event>(
        target: Target,
        expression: &str,
        first_occurrence: SystemTime,
        event: &E,
    ) -> Result<Self, SchedulerError> {
        Self::from_parts(target, Trigger::cron(expression)?, first_occurrence, event)
    }

    /// Reconstruct a persisted message from its stored fields.
    ///
    /// Intended for backend implementations that read rows back from the
    /// storage layer. Application code should use [`Self::delay`] or
    /// [`Self::cron`] instead.
    #[must_use]
    pub fn restore(
        schedule_id: Uuid,
        event_type: String,
        payload: Vec<u8>,
        target: Target,
        trigger: Trigger,
        scheduled_for: SystemTime,
    ) -> Self {
        Self {
            schedule_id,
            event_type,
            payload,
            target,
            trigger,
            scheduled_for,
        }
    }

    /// The stable identity of the occurrence this message currently
    /// represents.
    ///
    /// Derived from [`Self::schedule_id`] and [`Self::scheduled_for`], so it
    /// is the deduplication key under the at-least-once delivery contract.
    #[must_use]
    pub fn occurrence_id(&self) -> OccurrenceId {
        OccurrenceId::derive(self.schedule_id, self.scheduled_for)
    }

    fn from_parts<E: Event>(
        target: Target,
        trigger: Trigger,
        scheduled_for: SystemTime,
        event: &E,
    ) -> Result<Self, SchedulerError> {
        let payload = serde_json::to_vec(event)?;
        Ok(Self {
            schedule_id: Uuid::now_v7(),
            event_type: E::EVENT_TYPE.to_owned(),
            payload,
            target,
            trigger,
            scheduled_for,
        })
    }
}

impl std::fmt::Debug for ScheduledMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledMessage")
            .field("schedule_id", &self.schedule_id)
            .field("event_type", &self.event_type)
            .field("payload", &format_args!("<{} bytes>", self.payload.len()))
            .field("target", &self.target)
            .field("trigger", &self.trigger)
            .field("scheduled_for", &self.scheduled_for)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::ScheduledMessage;
    use crate::error::SchedulerError;
    use crate::occurrence::OccurrenceId;
    use crate::target::Target;
    use crate::trigger::Trigger;
    use hexeract_outbox::Event;
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, UNIX_EPOCH};
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue {
        user_id: Uuid,
    }

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    fn sample_event() -> ReminderDue {
        ReminderDue {
            user_id: Uuid::nil(),
        }
    }

    fn instant() -> std::time::SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_000)
    }

    #[test]
    fn delay_sets_trigger_scheduled_for_and_event_type() {
        let at = instant();
        let message = ScheduledMessage::delay(Target::mediator(), at, &sample_event())
            .expect("serializes the payload");
        assert_eq!(message.trigger, Trigger::Delay(at));
        assert_eq!(message.scheduled_for, at);
        assert_eq!(message.event_type, "reminders.due");
        assert_eq!(message.target, Target::Mediator);
        assert_ne!(message.schedule_id, Uuid::nil());
    }

    #[test]
    fn delay_serializes_the_payload_as_json() {
        let message = ScheduledMessage::delay(Target::mediator(), instant(), &sample_event())
            .expect("serializes the payload");
        let raw = std::str::from_utf8(&message.payload).expect("utf8 json");
        assert!(raw.contains("\"user_id\""));
    }

    #[test]
    fn cron_sets_a_recurring_trigger_and_first_occurrence() {
        let first = instant();
        let message = ScheduledMessage::cron(Target::outbox(), "0 0 * * *", first, &sample_event())
            .expect("valid cron and payload");
        assert!(message.trigger.is_recurring());
        assert_eq!(message.scheduled_for, first);
        assert_eq!(message.target, Target::Outbox);
    }

    #[test]
    fn cron_rejects_an_invalid_expression() {
        let error = ScheduledMessage::cron(Target::outbox(), "   ", instant(), &sample_event())
            .unwrap_err();
        assert!(matches!(error, SchedulerError::InvalidTrigger { .. }));
    }

    #[test]
    fn occurrence_id_is_derived_from_schedule_id_and_scheduled_for() {
        let message = ScheduledMessage::delay(Target::mediator(), instant(), &sample_event())
            .expect("serializes the payload");
        assert_eq!(
            message.occurrence_id(),
            OccurrenceId::derive(message.schedule_id, message.scheduled_for),
        );
    }

    #[test]
    fn restore_round_trips_every_field() {
        let at = instant();
        let message = ScheduledMessage::restore(
            Uuid::from_u128(5),
            "reminders.due".to_owned(),
            b"{}".to_vec(),
            Target::outbox(),
            Trigger::delay(at),
            at,
        );
        assert_eq!(message.schedule_id, Uuid::from_u128(5));
        assert_eq!(message.event_type, "reminders.due");
        assert_eq!(message.payload, b"{}");
        assert_eq!(message.target, Target::Outbox);
        assert_eq!(message.trigger, Trigger::Delay(at));
        assert_eq!(message.scheduled_for, at);
    }

    #[test]
    fn debug_masks_the_payload_bytes() {
        let message = ScheduledMessage::delay(Target::mediator(), instant(), &sample_event())
            .expect("serializes the payload");
        let debug_output = format!("{message:?}");
        assert!(debug_output.contains("bytes>"));
        assert!(!debug_output.contains("user_id"));
    }
}
