use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use hexeract_core::Notification;
use hexeract_mediator::Mediator;
use hexeract_outbox::Event;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::error::SchedulerError;
use crate::schedule::ScheduledMessage;
use crate::sink::ScheduleSink;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// In-process [`ScheduleSink`] that republishes a due occurrence through the
/// mediator.
///
/// When an occurrence targeting [`Target::Mediator`](crate::Target::Mediator)
/// is due, the worker hands it to this sink, which decodes the payload back
/// into its concrete notification type and publishes it through the
/// [`Mediator`]. The registered [`NotificationHandler`](hexeract_core::NotificationHandler)s
/// then run in-process, with no external transport.
///
/// The schedule row remains the source of truth: the dispatch itself is not
/// persisted. A crash between publishing and acknowledgement lets the lease
/// expire, so the occurrence is reclaimed and republished. Handlers must
/// therefore tolerate redelivery (deduplicate on
/// [`ScheduledMessage::occurrence_id`] when an effect must happen once).
pub struct MediatorSink {
    mediator: Arc<Mediator>,
    publishers: HashMap<&'static str, Box<dyn ErasedPublisher>>,
}

/// Builder that records which notification types a [`MediatorSink`] can decode
/// and publish.
///
/// Each [`Self::register`] call binds an event type to the machinery that
/// deserializes its payload and publishes it through the mediator. The handler
/// itself is registered separately on the [`MediatorBuilder`](hexeract_mediator::MediatorBuilder);
/// the sink only bridges the stored bytes back to the typed notification.
pub struct MediatorSinkBuilder {
    mediator: Arc<Mediator>,
    publishers: HashMap<&'static str, Box<dyn ErasedPublisher>>,
}

impl MediatorSink {
    /// Start building a sink that dispatches through `mediator`.
    #[must_use]
    pub fn builder(mediator: Arc<Mediator>) -> MediatorSinkBuilder {
        MediatorSinkBuilder {
            mediator,
            publishers: HashMap::new(),
        }
    }
}

impl MediatorSinkBuilder {
    /// Register the notification type `N`, keyed on its
    /// [`Event::EVENT_TYPE`].
    ///
    /// Registering the same event type twice keeps the last registration.
    #[must_use]
    pub fn register<N>(mut self) -> Self
    where
        N: Notification + Event + DeserializeOwned + 'static,
    {
        self.publishers
            .insert(N::EVENT_TYPE, Box::new(TypedPublisher::<N>(PhantomData)));
        self
    }

    /// Finish building the sink.
    #[must_use]
    pub fn build(self) -> MediatorSink {
        MediatorSink {
            mediator: self.mediator,
            publishers: self.publishers,
        }
    }
}

impl ScheduleSink for MediatorSink {
    /// Decode the occurrence into its notification type and publish it through
    /// the mediator.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Dispatch`] if the event type has no
    /// registered notification or the mediator reports a handler failure, and
    /// [`SchedulerError::Serialization`] if the stored payload cannot be
    /// decoded.
    async fn dispatch(&self, message: &ScheduledMessage) -> Result<(), SchedulerError> {
        let publisher = self
            .publishers
            .get(message.event_type.as_str())
            .ok_or_else(|| {
                SchedulerError::dispatch(UnregisteredEventType {
                    event_type: message.event_type.clone(),
                })
            })?;
        publisher.publish(&self.mediator, &message.payload).await
    }
}

/// Type-erased bridge from a stored payload to a mediator publish.
///
/// One implementation exists per registered notification type, hidden behind
/// this trait so the sink can hold a heterogeneous registry keyed on
/// `event_type`.
trait ErasedPublisher: Send + Sync + 'static {
    /// Decode `payload` into the concrete notification and publish it through
    /// `mediator`.
    fn publish<'a>(
        &'a self,
        mediator: &'a Mediator,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(), SchedulerError>>;
}

/// Adapter that lifts a concrete notification type `N` into an
/// [`ErasedPublisher`].
struct TypedPublisher<N>(PhantomData<fn() -> N>);

impl<N> ErasedPublisher for TypedPublisher<N>
where
    N: Notification + Event + DeserializeOwned + 'static,
{
    fn publish<'a>(
        &'a self,
        mediator: &'a Mediator,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(), SchedulerError>> {
        Box::pin(async move {
            let notification: N = serde_json::from_slice(payload)?;
            mediator
                .publish(notification)
                .await
                .map_err(SchedulerError::dispatch)
        })
    }
}

/// Returned when [`MediatorSink::dispatch`] receives a message whose event type
/// has no registered [`TypedPublisher`].
#[derive(Debug, Error)]
#[error("no notification registered for event type `{event_type}`")]
struct UnregisteredEventType {
    event_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};

    use hexeract_core::{HandlerContext, HexeractError, NotificationHandler};
    use hexeract_mediator::MediatorBuilder;
    use serde::{Deserialize, Serialize};

    use crate::schedule::ScheduledMessage;
    use crate::target::Target;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ReminderDue {
        label: String,
    }

    impl hexeract_outbox::Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    impl Notification for ReminderDue {}

    #[derive(Default)]
    struct RecordingHandler {
        received: Arc<Mutex<Vec<String>>>,
    }

    impl NotificationHandler<ReminderDue> for RecordingHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            notification: Arc<ReminderDue>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.received
                .lock()
                .expect("not poisoned")
                .push(notification.label.clone());
            Ok(())
        }
    }

    struct FailingHandler;

    #[derive(Debug, Error)]
    #[error("handler exploded")]
    struct HandlerBoom;

    impl From<HandlerBoom> for HexeractError {
        fn from(e: HandlerBoom) -> Self {
            HexeractError::handler_failed(e)
        }
    }

    impl NotificationHandler<ReminderDue> for FailingHandler {
        type Error = HandlerBoom;

        async fn handle(
            &self,
            _notification: Arc<ReminderDue>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Err(HandlerBoom)
        }
    }

    fn reminder_message(label: &str) -> ScheduledMessage {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        ScheduledMessage::delay(
            Target::mediator(),
            at,
            &ReminderDue {
                label: label.into(),
            },
        )
        .expect("serializes")
    }

    #[tokio::test]
    async fn dispatch_delivers_decoded_notification_to_handler() {
        let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let handler = RecordingHandler {
            received: Arc::clone(&received),
        };
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<ReminderDue, _>(handler)
            .build()
            .expect("valid mediator");

        let sink = MediatorSink::builder(Arc::new(mediator))
            .register::<ReminderDue>()
            .build();

        let message = reminder_message("morning-standup");
        sink.dispatch(&message).await.expect("dispatch succeeds");

        let labels = received.lock().expect("not poisoned");
        assert_eq!(labels.as_slice(), ["morning-standup"]);
    }

    #[tokio::test]
    async fn dispatch_maps_handler_failure_to_dispatch_error() {
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<ReminderDue, _>(FailingHandler)
            .build()
            .expect("valid mediator");

        let sink = MediatorSink::builder(Arc::new(mediator))
            .register::<ReminderDue>()
            .build();

        let err = sink
            .dispatch(&reminder_message("trigger"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, SchedulerError::Dispatch(_)),
            "expected Dispatch, got {err:?}"
        );
    }

    #[tokio::test]
    async fn dispatch_unknown_event_type_returns_dispatch_error() {
        let mediator = MediatorBuilder::new().build().expect("valid mediator");

        // Sink built without registering ReminderDue.
        let sink = MediatorSink::builder(Arc::new(mediator)).build();

        let err = sink.dispatch(&reminder_message("ghost")).await.unwrap_err();
        assert!(
            matches!(err, SchedulerError::Dispatch(_)),
            "expected Dispatch for unknown event type, got {err:?}"
        );
    }

    /// The sink is stateless, so the same message dispatched twice reaches the
    /// handler twice: idempotence is the handler's responsibility under the
    /// at-least-once contract.
    #[tokio::test]
    async fn dispatch_twice_delivers_notification_twice() {
        let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let handler = RecordingHandler {
            received: Arc::clone(&received),
        };
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<ReminderDue, _>(handler)
            .build()
            .expect("valid mediator");

        let sink = MediatorSink::builder(Arc::new(mediator))
            .register::<ReminderDue>()
            .build();

        let message = reminder_message("redelivered");
        sink.dispatch(&message).await.expect("first dispatch");
        sink.dispatch(&message).await.expect("second dispatch");

        let labels = received.lock().expect("not poisoned");
        assert_eq!(labels.len(), 2, "handler must receive the occurrence twice");
        assert!(
            labels.iter().all(|l| l == "redelivered"),
            "both occurrences carry the correct payload"
        );
    }

    #[tokio::test]
    async fn dispatch_invalid_payload_returns_serialization_error() {
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<ReminderDue, _>(RecordingHandler::default())
            .build()
            .expect("valid mediator");

        let sink = MediatorSink::builder(Arc::new(mediator))
            .register::<ReminderDue>()
            .build();

        // Build a message and corrupt the payload after construction.
        let mut message = reminder_message("ok");
        message.payload = b"not valid json {{{".to_vec();

        let err = sink.dispatch(&message).await.unwrap_err();
        assert!(
            matches!(err, SchedulerError::Serialization(_)),
            "expected Serialization, got {err:?}"
        );
    }
}
