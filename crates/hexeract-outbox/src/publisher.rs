use uuid::Uuid;

use crate::Event;
use crate::OutboxError;

/// Contract for inserting events into the outbox storage.
///
/// Implementors persist a fresh [`crate::OutboxEnvelope`] for each
/// published event. The associated [`Self::Tx`] generic associated type
/// lets the trait stay backend-agnostic while still allowing backends to
/// expose lifetime-bound transaction handles (e.g.
/// `deadpool_postgres::Transaction<'a>` borrows the connection it was
/// opened from).
///
/// Callers reuse their existing business transaction so the outbox
/// insert and the state mutation commit together.
///
/// # Idempotency
///
/// Backends should enforce uniqueness on `event_id` (e.g. `UNIQUE INDEX`)
/// so retried calls with the same identifier are no-ops rather than
/// duplicates. The trait itself does not impose this contract, but every
/// shipped backend will.
///
/// # Example
///
/// ```
/// use hexeract_outbox::{Event, OutboxPublisher, OutboxError};
/// use serde::{Deserialize, Serialize};
/// use uuid::Uuid;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct UserRegistered {
///     user_id: Uuid,
/// }
///
/// impl Event for UserRegistered {
///     const EVENT_TYPE: &'static str = "users.registered";
/// }
///
/// struct InMemoryTx;
/// struct InMemoryPublisher;
///
/// impl OutboxPublisher for InMemoryPublisher {
///     type Tx<'tx> = InMemoryTx;
///
///     async fn publish_in_tx<E: Event>(
///         &self,
///         _tx: &mut Self::Tx<'_>,
///         _event_id: Uuid,
///         _event: &E,
///     ) -> Result<(), OutboxError> {
///         Ok(())
///     }
///
///     async fn publish_in_tx_with_subject<E: Event>(
///         &self,
///         _tx: &mut Self::Tx<'_>,
///         _event_id: Uuid,
///         _subject_id: Uuid,
///         _event: &E,
///     ) -> Result<(), OutboxError> {
///         Ok(())
///     }
///
///     async fn publish<E: Event>(
///         &self,
///         _event_id: Uuid,
///         _event: &E,
///     ) -> Result<(), OutboxError> {
///         Ok(())
///     }
/// }
/// ```
#[trait_variant::make(Send)]
pub trait OutboxPublisher: Send + Sync + 'static {
    /// Backend-specific transaction handle borrowed by `publish_in_tx`.
    ///
    /// Parameterized by the transaction's lifetime so backends can expose
    /// types that borrow from the connection pool (e.g.
    /// `deadpool_postgres::Transaction<'tx>`).
    type Tx<'tx>: Send;

    /// Insert an event using an existing business transaction.
    ///
    /// The transaction is borrowed mutably so the call enrols the
    /// outbox insert in the caller's unit of work. The caller is
    /// responsible for committing or rolling back the transaction.
    async fn publish_in_tx<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        event_id: Uuid,
        event: &E,
    ) -> Result<(), OutboxError>;

    /// Insert an event tagged with a subject for partial ordering.
    ///
    /// Use this variant when downstream handlers need to observe events
    /// sharing the same aggregate identifier in insertion order.
    async fn publish_in_tx_with_subject<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        event_id: Uuid,
        subject_id: Uuid,
        event: &E,
    ) -> Result<(), OutboxError>;

    /// Insert an event using a transaction opened by the publisher itself.
    ///
    /// Useful for health checks, admin scripts and callers that do not
    /// own a business transaction. Backends commit immediately.
    async fn publish<E: Event>(&self, event_id: Uuid, event: &E) -> Result<(), OutboxError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OutboxEnvelope;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct UserRegistered {
        user_id: Uuid,
    }

    impl Event for UserRegistered {
        const EVENT_TYPE: &'static str = "users.registered";
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: Uuid,
    }

    impl Event for OrderPlaced {
        const EVENT_TYPE: &'static str = "orders.placed";
    }

    struct MockTx;

    #[derive(Clone)]
    struct MockPublisher {
        inserted: Arc<Mutex<Vec<OutboxEnvelope>>>,
    }

    impl MockPublisher {
        fn new() -> Self {
            Self {
                inserted: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn snapshot(&self) -> Vec<OutboxEnvelope> {
            self.inserted.lock().unwrap().clone()
        }
    }

    impl OutboxPublisher for MockPublisher {
        type Tx<'tx> = MockTx;

        async fn publish_in_tx<E: Event>(
            &self,
            _tx: &mut Self::Tx<'_>,
            event_id: Uuid,
            event: &E,
        ) -> Result<(), OutboxError> {
            let envelope = OutboxEnvelope::new(event_id, event)?;
            self.inserted.lock().unwrap().push(envelope);
            Ok(())
        }

        async fn publish_in_tx_with_subject<E: Event>(
            &self,
            _tx: &mut Self::Tx<'_>,
            event_id: Uuid,
            subject_id: Uuid,
            event: &E,
        ) -> Result<(), OutboxError> {
            let envelope = OutboxEnvelope::with_subject(event_id, subject_id, event)?;
            self.inserted.lock().unwrap().push(envelope);
            Ok(())
        }

        async fn publish<E: Event>(&self, event_id: Uuid, event: &E) -> Result<(), OutboxError> {
            let envelope = OutboxEnvelope::new(event_id, event)?;
            self.inserted.lock().unwrap().push(envelope);
            Ok(())
        }
    }

    fn sample_user() -> UserRegistered {
        UserRegistered {
            user_id: Uuid::from_u128(1),
        }
    }

    fn assert_send<T: Send>(_: &T) {}

    #[tokio::test]
    async fn publish_in_tx_inserts_envelope_without_subject() {
        let publisher = MockPublisher::new();
        let mut tx = MockTx;
        let event_id = Uuid::from_u128(7);

        publisher
            .publish_in_tx(&mut tx, event_id, &sample_user())
            .await
            .expect("publish must succeed");

        let store = publisher.snapshot();
        assert_eq!(store.len(), 1);
        assert_eq!(store[0].event_id, event_id);
        assert_eq!(store[0].event_type, "users.registered");
        assert!(store[0].subject_id.is_none());
    }

    #[tokio::test]
    async fn publish_in_tx_with_subject_tags_the_envelope() {
        let publisher = MockPublisher::new();
        let mut tx = MockTx;
        let event_id = Uuid::from_u128(8);
        let subject = Uuid::from_u128(42);

        publisher
            .publish_in_tx_with_subject(&mut tx, event_id, subject, &sample_user())
            .await
            .expect("publish must succeed");

        let store = publisher.snapshot();
        assert_eq!(store.len(), 1);
        assert_eq!(store[0].subject_id, Some(subject));
    }

    #[tokio::test]
    async fn publish_without_tx_inserts_envelope() {
        let publisher = MockPublisher::new();
        let event_id = Uuid::from_u128(9);

        publisher
            .publish(event_id, &sample_user())
            .await
            .expect("publish must succeed");

        let store = publisher.snapshot();
        assert_eq!(store.len(), 1);
        assert_eq!(store[0].event_id, event_id);
    }

    #[tokio::test]
    async fn published_envelope_decodes_back_to_original_event() {
        let publisher = MockPublisher::new();
        let mut tx = MockTx;
        let original = sample_user();

        publisher
            .publish_in_tx(&mut tx, Uuid::nil(), &original)
            .await
            .expect("publish must succeed");

        let store = publisher.snapshot();
        let decoded: UserRegistered = store[0].decode().expect("decode must succeed");
        assert_eq!(decoded, original);
    }

    #[tokio::test]
    async fn publisher_accepts_multiple_event_types() {
        let publisher = MockPublisher::new();
        let mut tx = MockTx;

        publisher
            .publish_in_tx(&mut tx, Uuid::from_u128(1), &sample_user())
            .await
            .unwrap();
        publisher
            .publish_in_tx(
                &mut tx,
                Uuid::from_u128(2),
                &OrderPlaced {
                    order_id: Uuid::from_u128(99),
                },
            )
            .await
            .unwrap();

        let store = publisher.snapshot();
        assert_eq!(store.len(), 2);
        assert_eq!(store[0].event_type, "users.registered");
        assert_eq!(store[1].event_type, "orders.placed");
    }

    #[tokio::test]
    async fn publish_future_is_send() {
        let publisher = MockPublisher::new();
        let mut tx = MockTx;
        let event = sample_user();
        let future = publisher.publish_in_tx(&mut tx, Uuid::nil(), &event);
        assert_send(&future);
        let _ = future.await;
    }

    #[tokio::test]
    async fn publisher_is_shareable_via_arc() {
        let publisher: Arc<MockPublisher> = Arc::new(MockPublisher::new());
        let p1 = Arc::clone(&publisher);
        let p2 = Arc::clone(&publisher);

        let t1 = tokio::spawn(async move {
            p1.publish(Uuid::from_u128(1), &sample_user())
                .await
                .unwrap();
        });
        let t2 = tokio::spawn(async move {
            p2.publish(Uuid::from_u128(2), &sample_user())
                .await
                .unwrap();
        });

        let _ = tokio::join!(t1, t2);
        assert_eq!(publisher.snapshot().len(), 2);
    }
}
