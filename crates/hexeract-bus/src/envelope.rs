use std::collections::HashMap;
use std::time::SystemTime;

use uuid::Uuid;

use crate::BusError;
use crate::Message;

/// In-flight representation of a message crossing the bus.
///
/// An envelope carries the serialized payload plus the routing and
/// observability metadata the bus needs: a stable `message_id`, the
/// `correlation_id` for distributed tracing, an optional `reply_to`
/// queue for request-reply patterns, and free-form `headers`.
///
/// The `Debug` implementation masks the payload bytes to avoid leaking
/// potentially sensitive event data into logs and tracing output.
#[derive(Clone)]
#[non_exhaustive]
pub struct BusEnvelope {
    /// Stable identifier of this message (`UUIDv7`), minted by the publisher.
    pub message_id: Uuid,
    /// Routing key matching [`Message::MESSAGE_TYPE`] of the original message.
    pub message_type: String,
    /// JSON-serialized payload of the original message.
    pub payload: Vec<u8>,
    /// Identifier shared by every message belonging to the same causal chain.
    pub correlation_id: Uuid,
    /// Optional reply queue or routing key for request-reply patterns.
    pub reply_to: Option<String>,
    /// Free-form metadata propagated alongside the message.
    pub headers: HashMap<String, String>,
    /// Instant at which the envelope was created by the publisher.
    pub published_at: SystemTime,
}

impl std::fmt::Debug for BusEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusEnvelope")
            .field("message_id", &self.message_id)
            .field("message_type", &self.message_type)
            .field("payload", &format_args!("<{} bytes>", self.payload.len()))
            .field("correlation_id", &self.correlation_id)
            .field("reply_to", &self.reply_to)
            .field("headers", &self.headers)
            .field("published_at", &self.published_at)
            .finish()
    }
}

impl BusEnvelope {
    /// Build a fresh envelope from a domain message.
    ///
    /// Mints a new `message_id` (`UUIDv7`), serializes the payload as
    /// JSON and stamps the current time as `published_at`.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Serialization`] if the message cannot be
    /// encoded as JSON.
    pub fn new<M: Message>(correlation_id: Uuid, message: &M) -> Result<Self, BusError> {
        let payload = serde_json::to_vec(message)?;
        Ok(Self {
            message_id: Uuid::now_v7(),
            message_type: M::MESSAGE_TYPE.to_owned(),
            payload,
            correlation_id,
            reply_to: None,
            headers: HashMap::new(),
            published_at: SystemTime::now(),
        })
    }

    /// Build a fresh envelope with the given headers attached.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Serialization`] if the message cannot be
    /// encoded as JSON.
    pub fn with_headers<M: Message>(
        correlation_id: Uuid,
        headers: HashMap<String, String>,
        message: &M,
    ) -> Result<Self, BusError> {
        let mut envelope = Self::new(correlation_id, message)?;
        envelope.headers = headers;
        Ok(envelope)
    }

    /// Build a fresh envelope with the given reply-to queue.
    ///
    /// Use this when publishing a request that expects a reply, in
    /// preparation for the request-reply pattern shipped in a later
    /// milestone.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Serialization`] if the message cannot be
    /// encoded as JSON.
    pub fn with_reply_to<M: Message>(
        correlation_id: Uuid,
        reply_to: impl Into<String>,
        message: &M,
    ) -> Result<Self, BusError> {
        let mut envelope = Self::new(correlation_id, message)?;
        envelope.reply_to = Some(reply_to.into());
        Ok(envelope)
    }

    /// Reconstruct an envelope from its stored fields.
    ///
    /// Intended for backend implementations that read envelopes back
    /// from the broker. Application code should use [`Self::new`],
    /// [`Self::with_headers`] or [`Self::with_reply_to`] instead.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        message_id: Uuid,
        message_type: String,
        payload: Vec<u8>,
        correlation_id: Uuid,
        reply_to: Option<String>,
        headers: HashMap<String, String>,
        published_at: SystemTime,
    ) -> Self {
        Self {
            message_id,
            message_type,
            payload,
            correlation_id,
            reply_to,
            headers,
            published_at,
        }
    }

    /// Deserialize the payload back into the strongly-typed message.
    ///
    /// # Trust boundary
    ///
    /// The payload may originate from any producer allowed to publish
    /// to the broker, so it is untrusted input. Deeply nested JSON is
    /// bounded by the `serde_json` recursion limit (128 levels by
    /// default), which turns a potential stack exhaustion into a
    /// [`BusError::Serialization`]. Payload size is not bounded here:
    /// the bytes are already in memory by the time an envelope exists,
    /// so transport backends must enforce their size cap before
    /// constructing the envelope, the way `hexeract-bus-rabbitmq` does
    /// with its `max_payload_bytes` worker setting.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::TypeMismatch`] if the envelope's
    /// `message_type` does not match [`Message::MESSAGE_TYPE`] of the
    /// requested type, or [`BusError::Serialization`] if the payload
    /// cannot be decoded as JSON of the target type.
    pub fn decode<M: Message>(&self) -> Result<M, BusError> {
        if self.message_type != M::MESSAGE_TYPE {
            return Err(BusError::TypeMismatch {
                expected: M::MESSAGE_TYPE,
                actual: self.message_type.clone(),
            });
        }
        serde_json::from_slice(&self.payload).map_err(BusError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: Uuid,
        amount_cents: u64,
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

    fn sample_order() -> OrderPlaced {
        OrderPlaced {
            order_id: Uuid::from_u128(1),
            amount_cents: 9999,
        }
    }

    #[test]
    fn new_records_message_type_and_correlation_id() {
        let correlation = Uuid::from_u128(42);
        let envelope = BusEnvelope::new(correlation, &sample_order()).unwrap();
        assert_eq!(envelope.message_type, "orders.placed");
        assert_eq!(envelope.correlation_id, correlation);
        assert!(envelope.reply_to.is_none());
        assert!(envelope.headers.is_empty());
        assert_ne!(envelope.message_id, Uuid::nil());
    }

    #[test]
    fn new_serializes_payload_as_json() {
        let envelope = BusEnvelope::new(Uuid::nil(), &sample_order()).unwrap();
        let raw = std::str::from_utf8(&envelope.payload).unwrap();
        assert!(raw.contains("\"order_id\""));
        assert!(raw.contains("\"amount_cents\""));
    }

    #[test]
    fn with_headers_records_headers() {
        let mut headers = HashMap::new();
        headers.insert("tenant".to_owned(), "acme".to_owned());
        let envelope =
            BusEnvelope::with_headers(Uuid::nil(), headers.clone(), &sample_order()).unwrap();
        assert_eq!(envelope.headers, headers);
    }

    #[test]
    fn with_reply_to_records_reply_to() {
        let envelope =
            BusEnvelope::with_reply_to(Uuid::nil(), "q.replies", &sample_order()).unwrap();
        assert_eq!(envelope.reply_to.as_deref(), Some("q.replies"));
    }

    #[test]
    fn decode_round_trip_returns_original_message() {
        let original = sample_order();
        let envelope = BusEnvelope::new(Uuid::nil(), &original).unwrap();
        let decoded: OrderPlaced = envelope.decode().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_rejects_mismatched_message_type() {
        let envelope = BusEnvelope::new(Uuid::nil(), &sample_order()).unwrap();
        let err = envelope.decode::<UserRegistered>().unwrap_err();
        match err {
            BusError::TypeMismatch { expected, actual } => {
                assert_eq!(expected, "users.registered");
                assert_eq!(actual, "orders.placed");
            }
            other => panic!("expected BusError::TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn debug_masks_payload_bytes() {
        let envelope = BusEnvelope::new(Uuid::nil(), &sample_order()).unwrap();
        let debug_output = format!("{envelope:?}");
        assert!(debug_output.contains('<'));
        assert!(debug_output.contains("bytes>"));
        assert!(!debug_output.contains("order_id"));
    }

    #[test]
    fn successive_news_mint_distinct_message_ids() {
        let e1 = BusEnvelope::new(Uuid::nil(), &sample_order()).unwrap();
        let e2 = BusEnvelope::new(Uuid::nil(), &sample_order()).unwrap();
        assert_ne!(e1.message_id, e2.message_id);
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct FreeForm {
        data: serde_json::Value,
    }

    impl Message for FreeForm {
        const MESSAGE_TYPE: &'static str = "tests.free_form";
    }

    /// Pins the `serde_json` recursion limit this crate relies on to
    /// guard [`BusEnvelope::decode`] against stack exhaustion from
    /// deeply nested untrusted payloads. If a dependency change ever
    /// lifts that limit, this test fails instead of the guard silently
    /// disappearing.
    #[test]
    fn decode_rejects_deeply_nested_payload() {
        let depth = 200;
        let nested = format!("{{\"data\":{}{}}}", "[".repeat(depth), "]".repeat(depth));
        let envelope = BusEnvelope::restore(
            Uuid::nil(),
            FreeForm::MESSAGE_TYPE.to_owned(),
            nested.into_bytes(),
            Uuid::nil(),
            None,
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
        );

        let err = envelope.decode::<FreeForm>().unwrap_err();
        match err {
            BusError::Serialization(source) => {
                assert!(
                    source.to_string().contains("recursion limit"),
                    "expected the recursion limit to trip, got: {source}"
                );
            }
            other => panic!("expected BusError::Serialization, got {other:?}"),
        }
    }
}
