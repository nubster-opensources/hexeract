use hexeract_bus::RawBusPublish;
use thiserror::Error;

use crate::error::SchedulerError;
use crate::schedule::ScheduledMessage;
use crate::sink::ScheduleSink;
use crate::target::Target;

/// [`ScheduleSink`] that publishes a due occurrence on the message bus.
///
/// When an occurrence targeting [`Target::Bus`](crate::Target::Bus) is due,
/// the worker hands it to this sink, which publishes the stored `event_type`
/// and `payload` under the target's routing key. The occurrence id
/// ([`ScheduledMessage::occurrence_id`]) is propagated as the message id, so
/// broker-side consumers can deduplicate the redeliveries that the
/// at-least-once claim contract may produce.
///
/// Delivery across the broker is at-least-once: the sink does not deduplicate
/// itself, it only stamps a stable message id for consumers to deduplicate on.
pub struct BusSink<T> {
    transport: T,
}

impl<T> BusSink<T>
where
    T: RawBusPublish,
{
    /// Build a sink that publishes due occurrences through `transport`.
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

impl<T> ScheduleSink for BusSink<T>
where
    T: RawBusPublish,
{
    /// Publish the occurrence on the bus under its target routing key, stamped
    /// with the occurrence id as message id.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Dispatch`] if the occurrence does not target
    /// the bus or the publish fails. The worker then retries or dead-letters
    /// the occurrence.
    async fn dispatch(&self, message: &ScheduledMessage) -> Result<(), SchedulerError> {
        let Target::Bus { routing_key } = &message.target else {
            return Err(SchedulerError::dispatch(NonBusTarget));
        };
        self.transport
            .publish_raw(
                routing_key,
                message.occurrence_id().as_uuid(),
                &message.event_type,
                &message.payload,
            )
            .await
            .map_err(SchedulerError::dispatch)
    }
}

/// Returned when [`BusSink::dispatch`] receives an occurrence whose target is
/// not [`Target::Bus`].
#[derive(Debug, Error)]
#[error("bus sink received an occurrence whose target is not the bus")]
struct NonBusTarget;

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};

    use async_trait::async_trait;
    use hexeract_bus::BusError;
    use hexeract_outbox::Event;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue {
        label: String,
    }

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    type PublishedCall = (String, Uuid, String, Vec<u8>);

    #[derive(Default)]
    struct RecordingTransport {
        published: Mutex<Vec<PublishedCall>>,
    }

    #[async_trait]
    impl RawBusPublish for RecordingTransport {
        async fn publish_raw(
            &self,
            routing_key: &str,
            message_id: Uuid,
            message_type: &str,
            payload: &[u8],
        ) -> Result<(), BusError> {
            self.published.lock().expect("not poisoned").push((
                routing_key.to_owned(),
                message_id,
                message_type.to_owned(),
                payload.to_vec(),
            ));
            Ok(())
        }
    }

    struct FailingTransport;

    #[async_trait]
    impl RawBusPublish for FailingTransport {
        async fn publish_raw(
            &self,
            _routing_key: &str,
            _message_id: Uuid,
            _message_type: &str,
            _payload: &[u8],
        ) -> Result<(), BusError> {
            Err(BusError::Internal("broker down".to_owned()))
        }
    }

    fn bus_message(routing_key: &str, label: &str) -> ScheduledMessage {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        ScheduledMessage::delay(
            Target::bus(routing_key),
            at,
            &ReminderDue {
                label: label.into(),
            },
        )
        .expect("serializes")
    }

    #[tokio::test]
    async fn dispatch_publishes_under_routing_key_with_occurrence_id() {
        let message = bus_message("reminders.due", "morning-standup");
        let sink = BusSink::new(RecordingTransport::default());

        sink.dispatch(&message).await.expect("dispatch succeeds");

        let published = sink.transport.published.lock().expect("not poisoned");
        assert_eq!(published.len(), 1);
        let (routing_key, message_id, message_type, payload) = &published[0];
        assert_eq!(routing_key, "reminders.due");
        assert_eq!(*message_id, message.occurrence_id().as_uuid());
        assert_eq!(message_type, "reminders.due");
        assert_eq!(payload.as_slice(), message.payload.as_slice());
    }

    #[tokio::test]
    async fn dispatch_rejects_a_non_bus_target() {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        let message = ScheduledMessage::delay(
            Target::outbox(),
            at,
            &ReminderDue {
                label: "wrong-target".into(),
            },
        )
        .expect("serializes");
        let sink = BusSink::new(RecordingTransport::default());

        let err = sink.dispatch(&message).await.unwrap_err();

        assert!(
            matches!(err, SchedulerError::Dispatch(_)),
            "expected Dispatch, got {err:?}"
        );
        assert!(
            sink.transport
                .published
                .lock()
                .expect("not poisoned")
                .is_empty(),
            "nothing is published for a non-bus target"
        );
    }

    #[tokio::test]
    async fn dispatch_maps_publish_failure_to_dispatch_error() {
        let sink = BusSink::new(FailingTransport);

        let err = sink
            .dispatch(&bus_message("reminders.due", "trigger"))
            .await
            .unwrap_err();

        assert!(
            matches!(err, SchedulerError::Dispatch(_)),
            "expected Dispatch, got {err:?}"
        );
    }
}
