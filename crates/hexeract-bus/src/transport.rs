use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::BusError;
use crate::Message;

/// Backend-agnostic publish contract for the bus.
///
/// A [`Transport`] mints a fresh [`crate::BusEnvelope`] for each
/// message and hands it to the underlying broker driver. The
/// `routing_key` is interpreted by the backend: AMQP routing key,
/// NATS subject, Kafka topic or SQS queue URL. A single transport
/// publishes to multiple destinations on the same logical exchange
/// or stream.
///
/// Implementors are typically held by long-lived workers and
/// handlers; they should be cheap to share through an
/// [`std::sync::Arc`].
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
///
/// use hexeract_bus::Message;
/// use hexeract_bus::Transport;
/// use serde::Deserialize;
/// use serde::Serialize;
/// use uuid::Uuid;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct OrderPlaced {
///     order_id: Uuid,
/// }
///
/// impl Message for OrderPlaced {
///     const MESSAGE_TYPE: &'static str = "orders.placed";
/// }
///
/// async fn publish_one<T: Transport>(transport: Arc<T>) {
///     let event = OrderPlaced {
///         order_id: Uuid::now_v7(),
///     };
///     transport.publish("orders.placed", &event).await.unwrap();
/// }
/// ```
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Publish `message` to the broker under the given `routing_key`.
    ///
    /// Mints a fresh [`crate::BusEnvelope`] and returns its
    /// `message_id` (`UUIDv7`). The `correlation_id` is minted by
    /// the transport. Callers that need to propagate a known
    /// correlation identifier across a publish should use
    /// [`Self::publish_with_correlation_id`] instead.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Serialization`] if `message` cannot be
    /// encoded as JSON, [`BusError::Connection`] if the broker is
    /// unreachable, or [`BusError::Transport`] if the broker
    /// rejected the publish.
    async fn publish<M: Message>(&self, routing_key: &str, message: &M) -> Result<Uuid, BusError>;

    /// Publish `message` with extra `headers` attached to the envelope.
    ///
    /// Headers carry free-form metadata: W3C trace context
    /// (`traceparent`), tenancy (`tenant_id`) or backend-specific
    /// hints exposed verbatim to the broker. The `correlation_id`
    /// is minted by the transport; combine with
    /// [`Self::publish_with_correlation_id`] semantics when both a
    /// caller-supplied correlation_id and custom headers are needed.
    ///
    /// # Errors
    ///
    /// Same conditions as [`Self::publish`].
    async fn publish_with_headers<M: Message>(
        &self,
        routing_key: &str,
        headers: HashMap<String, String>,
        message: &M,
    ) -> Result<Uuid, BusError>;

    /// Publish `message` and propagate a caller-supplied `correlation_id`.
    ///
    /// Use this variant when the publish is part of a chain of
    /// messages that must share a correlation identifier for
    /// distributed tracing or request-reply patterns. Handlers
    /// typically read the inbound `correlation_id` from their
    /// [`hexeract_core::HandlerContext`] and forward it here when
    /// they re-emit downstream messages.
    ///
    /// Returns the freshly minted `message_id` (`UUIDv7`) of the
    /// outgoing envelope; the supplied `correlation_id` is carried
    /// verbatim through the bus envelope and surfaces on the
    /// consumer side.
    ///
    /// # Errors
    ///
    /// Same conditions as [`Self::publish`].
    async fn publish_with_correlation_id<M: Message>(
        &self,
        routing_key: &str,
        correlation_id: Uuid,
        message: &M,
    ) -> Result<Uuid, BusError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use serde::Deserialize;
    use serde::Serialize;

    use super::*;
    use crate::BusEnvelope;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: Uuid,
    }

    impl Message for OrderPlaced {
        const MESSAGE_TYPE: &'static str = "orders.placed";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct UserRegistered {
        user_id: Uuid,
    }

    impl Message for UserRegistered {
        const MESSAGE_TYPE: &'static str = "users.registered";
    }

    struct MockTransport {
        recorded: Arc<Mutex<Vec<(String, BusEnvelope)>>>,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                recorded: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn snapshot(&self) -> Vec<(String, BusEnvelope)> {
            self.recorded.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn publish<M: Message>(
            &self,
            routing_key: &str,
            message: &M,
        ) -> Result<Uuid, BusError> {
            let envelope = BusEnvelope::new(Uuid::now_v7(), message)?;
            let message_id = envelope.message_id;
            self.recorded
                .lock()
                .unwrap()
                .push((routing_key.to_owned(), envelope));
            Ok(message_id)
        }

        async fn publish_with_headers<M: Message>(
            &self,
            routing_key: &str,
            headers: HashMap<String, String>,
            message: &M,
        ) -> Result<Uuid, BusError> {
            let envelope = BusEnvelope::with_headers(Uuid::now_v7(), headers, message)?;
            let message_id = envelope.message_id;
            self.recorded
                .lock()
                .unwrap()
                .push((routing_key.to_owned(), envelope));
            Ok(message_id)
        }

        async fn publish_with_correlation_id<M: Message>(
            &self,
            routing_key: &str,
            correlation_id: Uuid,
            message: &M,
        ) -> Result<Uuid, BusError> {
            let envelope = BusEnvelope::new(correlation_id, message)?;
            let message_id = envelope.message_id;
            self.recorded
                .lock()
                .unwrap()
                .push((routing_key.to_owned(), envelope));
            Ok(message_id)
        }
    }

    fn sample_order() -> OrderPlaced {
        OrderPlaced {
            order_id: Uuid::from_u128(1),
        }
    }

    fn assert_send<T: Send>(_: &T) {}

    #[tokio::test]
    async fn publish_records_envelope_under_routing_key() {
        let transport = MockTransport::new();

        let message_id = transport
            .publish("orders.placed", &sample_order())
            .await
            .expect("publish must succeed");

        let recorded = transport.snapshot();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, "orders.placed");
        assert_eq!(recorded[0].1.message_id, message_id);
        assert_eq!(recorded[0].1.message_type, "orders.placed");
    }

    #[tokio::test]
    async fn publish_serializes_payload_as_json() {
        let transport = MockTransport::new();
        transport
            .publish("orders.placed", &sample_order())
            .await
            .unwrap();

        let recorded = transport.snapshot();
        let payload = std::str::from_utf8(&recorded[0].1.payload).unwrap();
        assert!(payload.contains("\"order_id\""));
    }

    #[tokio::test]
    async fn publish_with_headers_attaches_headers() {
        let transport = MockTransport::new();
        let mut headers = HashMap::new();
        headers.insert("tenant".to_owned(), "acme".to_owned());

        transport
            .publish_with_headers("orders.placed", headers.clone(), &sample_order())
            .await
            .unwrap();

        let recorded = transport.snapshot();
        assert_eq!(recorded[0].1.headers, headers);
    }

    #[tokio::test]
    async fn publish_with_correlation_id_propagates_caller_supplied_value() {
        let transport = MockTransport::new();
        let correlation_id = Uuid::from_u128(0x42);

        let message_id = transport
            .publish_with_correlation_id("orders.placed", correlation_id, &sample_order())
            .await
            .expect("publish must succeed");

        let recorded = transport.snapshot();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].1.correlation_id, correlation_id);
        assert_eq!(recorded[0].1.message_id, message_id);
        assert_ne!(recorded[0].1.message_id, correlation_id);
    }

    #[tokio::test]
    async fn publish_mints_distinct_message_ids() {
        let transport = MockTransport::new();
        let id1 = transport
            .publish("orders.placed", &sample_order())
            .await
            .unwrap();
        let id2 = transport
            .publish("orders.placed", &sample_order())
            .await
            .unwrap();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn publish_accepts_multiple_message_types() {
        let transport = MockTransport::new();
        transport
            .publish("orders.placed", &sample_order())
            .await
            .unwrap();
        transport
            .publish(
                "users.registered",
                &UserRegistered {
                    user_id: Uuid::from_u128(7),
                },
            )
            .await
            .unwrap();

        let recorded = transport.snapshot();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].1.message_type, "orders.placed");
        assert_eq!(recorded[1].1.message_type, "users.registered");
    }

    #[tokio::test]
    async fn publish_future_is_send() {
        let transport = MockTransport::new();
        let event = sample_order();
        let future = transport.publish("orders.placed", &event);
        assert_send(&future);
        let _ = future.await;
    }

    #[tokio::test]
    async fn transport_is_shareable_via_arc() {
        let transport: Arc<MockTransport> = Arc::new(MockTransport::new());
        let t1 = Arc::clone(&transport);
        let t2 = Arc::clone(&transport);

        let h1 = tokio::spawn(async move {
            t1.publish("orders.placed", &sample_order()).await.unwrap();
        });
        let h2 = tokio::spawn(async move {
            t2.publish("orders.placed", &sample_order()).await.unwrap();
        });

        let _ = tokio::join!(h1, h2);
        assert_eq!(transport.snapshot().len(), 2);
    }
}
