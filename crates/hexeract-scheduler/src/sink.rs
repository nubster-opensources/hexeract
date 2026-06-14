use crate::error::SchedulerError;
use crate::schedule::ScheduledMessage;

/// Contract for dispatching a due occurrence to its destination.
///
/// The worker routes each due occurrence to the sink matching its
/// [`Target`](crate::Target). Implementations carry the message to the
/// mediator, the outbox or the bus.
///
/// # At-least-once delivery
///
/// A sink may receive the same occurrence more than once: the claim and
/// lease protocol of [`ScheduleStore`](crate::ScheduleStore) guarantees
/// delivery at least once, not exactly once. A worker that crashes after
/// dispatching but before acknowledging lets the lease expire, and the
/// occurrence is reclaimed and dispatched again. Implementations must
/// therefore tolerate redelivery, deduplicating on
/// [`ScheduledMessage::occurrence_id`] when an effect must happen only once.
///
/// # Errors
///
/// A failed dispatch is surfaced as an error (typically
/// [`SchedulerError::Dispatch`]). The worker treats it as a failed attempt
/// and retries or dead-letters the occurrence according to its remaining
/// attempt budget.
#[trait_variant::make(Send)]
pub trait ScheduleSink: Send + Sync + 'static {
    /// Dispatch a due occurrence to its destination.
    ///
    /// # Errors
    ///
    /// Returns an error if the occurrence could not be delivered. The caller
    /// retries or dead-letters it.
    async fn dispatch(&self, message: &ScheduledMessage) -> Result<(), SchedulerError>;
}

#[cfg(test)]
mod tests {
    use super::ScheduleSink;
    use crate::error::SchedulerError;
    use crate::occurrence::OccurrenceId;
    use crate::schedule::ScheduledMessage;
    use crate::target::Target;
    use hexeract_outbox::Event;
    use serde::{Deserialize, Serialize};
    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue;

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    #[derive(Default)]
    struct RecordingSink {
        dispatched: Mutex<Vec<OccurrenceId>>,
    }

    impl ScheduleSink for RecordingSink {
        async fn dispatch(&self, message: &ScheduledMessage) -> Result<(), SchedulerError> {
            self.dispatched
                .lock()
                .expect("not poisoned")
                .push(message.occurrence_id());
            Ok(())
        }
    }

    struct FailingSink;

    impl ScheduleSink for FailingSink {
        async fn dispatch(&self, _message: &ScheduledMessage) -> Result<(), SchedulerError> {
            Err(SchedulerError::dispatch(std::io::Error::other("sink down")))
        }
    }

    fn message() -> ScheduledMessage {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        ScheduledMessage::delay(Target::mediator(), at, &ReminderDue).expect("serializes")
    }

    #[tokio::test]
    async fn dispatch_receives_the_occurrence() {
        let sink = RecordingSink::default();
        let message = message();
        sink.dispatch(&message).await.expect("dispatch succeeds");
        let recorded = sink.dispatched.lock().expect("not poisoned");
        assert_eq!(recorded.as_slice(), &[message.occurrence_id()]);
    }

    #[tokio::test]
    async fn dispatch_errors_propagate_to_the_caller() {
        let sink = FailingSink;
        let error = sink.dispatch(&message()).await.unwrap_err();
        assert!(matches!(error, SchedulerError::Dispatch(_)));
    }
}
