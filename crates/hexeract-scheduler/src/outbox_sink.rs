use hexeract_outbox::IdempotentOutboxEnqueue;

use crate::error::SchedulerError;
use crate::schedule::ScheduledMessage;
use crate::sink::ScheduleSink;

/// [`ScheduleSink`] that enqueues a due occurrence into the transactional
/// outbox, keyed idempotently on its occurrence id.
///
/// When an occurrence targeting [`Target::Outbox`](crate::Target::Outbox) is
/// due, the worker hands it to this sink, which forwards the stored
/// `event_type` and `payload` to the outbox as a raw row whose `event_id` is
/// the occurrence's stable identity. Downstream delivery is then owned by the
/// outbox worker and inherits all of its guarantees: claim and lease, bounded
/// backoff and dead-letter.
///
/// # Exactly-once across the boundary
///
/// The schedule row and the outbox row are not written in one transaction.
/// Safety comes instead from the idempotent insert: the `event_id` is the
/// occurrence id ([`ScheduledMessage::occurrence_id`]), so a redelivery after
/// a crash re-runs the enqueue as a no-op rather than producing a second
/// outbox row. The occurrence reaches the outbox exactly once even though the
/// sink may be invoked more than once under the at-least-once claim contract.
pub struct OutboxSink<Q> {
    enqueue: Q,
}

impl<Q> OutboxSink<Q>
where
    Q: IdempotentOutboxEnqueue,
{
    /// Build a sink that enqueues due occurrences through `enqueue`.
    pub fn new(enqueue: Q) -> Self {
        Self { enqueue }
    }
}

impl<Q> ScheduleSink for OutboxSink<Q>
where
    Q: IdempotentOutboxEnqueue,
{
    /// Enqueue the occurrence into the outbox, keyed on its occurrence id.
    ///
    /// Succeeds whether the row was newly inserted or already present, so a
    /// redelivered occurrence is a no-op rather than an error.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Dispatch`] if the outbox fails to insert. The
    /// worker then retries or dead-letters the occurrence.
    async fn dispatch(&self, message: &ScheduledMessage) -> Result<(), SchedulerError> {
        self.enqueue
            .enqueue_idempotent(
                message.occurrence_id().as_uuid(),
                &message.event_type,
                &message.payload,
            )
            .await
            .map(drop)
            .map_err(SchedulerError::dispatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};

    use hexeract_outbox::{Event, OutboxError};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::target::Target;

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue {
        label: String,
    }

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    /// Records every enqueue and deduplicates on `event_id`, mirroring the
    /// unique-index behaviour of a real backend.
    #[derive(Default)]
    struct RecordingEnqueue {
        seen: Mutex<HashSet<Uuid>>,
        calls: Mutex<Vec<(Uuid, String, Vec<u8>)>>,
    }

    impl IdempotentOutboxEnqueue for RecordingEnqueue {
        async fn enqueue_idempotent(
            &self,
            event_id: Uuid,
            event_type: &str,
            payload: &[u8],
        ) -> Result<bool, OutboxError> {
            self.calls.lock().expect("not poisoned").push((
                event_id,
                event_type.to_owned(),
                payload.to_vec(),
            ));
            let inserted = self.seen.lock().expect("not poisoned").insert(event_id);
            Ok(inserted)
        }
    }

    struct FailingEnqueue;

    impl IdempotentOutboxEnqueue for FailingEnqueue {
        async fn enqueue_idempotent(
            &self,
            _event_id: Uuid,
            _event_type: &str,
            _payload: &[u8],
        ) -> Result<bool, OutboxError> {
            Err(OutboxError::Internal("outbox down".to_owned()))
        }
    }

    fn reminder_message(label: &str) -> ScheduledMessage {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        ScheduledMessage::delay(
            Target::outbox(),
            at,
            &ReminderDue {
                label: label.into(),
            },
        )
        .expect("serializes")
    }

    #[tokio::test]
    async fn dispatch_enqueues_under_the_occurrence_id() {
        let message = reminder_message("morning-standup");
        let sink = OutboxSink::new(RecordingEnqueue::default());

        sink.dispatch(&message).await.expect("dispatch succeeds");

        let calls = sink.enqueue.calls.lock().expect("not poisoned");
        assert_eq!(calls.len(), 1);
        let (event_id, event_type, payload) = &calls[0];
        assert_eq!(*event_id, message.occurrence_id().as_uuid());
        assert_eq!(event_type, "reminders.due");
        assert_eq!(payload.as_slice(), message.payload.as_slice());
    }

    /// The sink may be invoked more than once for the same occurrence under the
    /// at-least-once claim contract, but the idempotent enqueue collapses the
    /// retries onto a single outbox row.
    #[tokio::test]
    async fn dispatch_twice_produces_a_single_outbox_row() {
        let message = reminder_message("redelivered");
        let sink = OutboxSink::new(RecordingEnqueue::default());

        sink.dispatch(&message).await.expect("first dispatch");
        sink.dispatch(&message).await.expect("second dispatch");

        let seen = sink.enqueue.seen.lock().expect("not poisoned");
        assert_eq!(seen.len(), 1, "exactly one outbox row for the occurrence");
    }

    #[tokio::test]
    async fn dispatch_maps_enqueue_failure_to_dispatch_error() {
        let sink = OutboxSink::new(FailingEnqueue);

        let err = sink
            .dispatch(&reminder_message("trigger"))
            .await
            .unwrap_err();

        assert!(
            matches!(err, SchedulerError::Dispatch(_)),
            "expected Dispatch, got {err:?}"
        );
    }
}
