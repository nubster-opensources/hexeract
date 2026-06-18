use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::Message;
use hexeract_bus::RawBusPublish;
use hexeract_bus::Transport;
use lapin::BasicProperties;
use lapin::options::BasicPublishOptions;
use lapin::options::ExchangeDeclareOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use uuid::Uuid;

use crate::connection::DEFAULT_RETRY_ATTEMPTS;
use crate::connection::DEFAULT_RETRY_BASE_DELAY;
use crate::connection::RabbitMqConnection;
use crate::pool::ChannelPool;
use crate::pool::DEFAULT_POOL_MAX_SIZE;

/// MIME type used for the JSON payloads carried by [`BusEnvelope`].
const JSON_CONTENT_TYPE: &str = "application/json";

/// Convert a user-supplied string into an AMQP [`ShortString`] without
/// panicking.
///
/// `ShortString::from`/`try_new` cap names at 255 bytes. The blanket
/// `From<&str>` impl panics via `.expect(...)` on an oversize input, and
/// on the publish / consume hot path that input is attacker-influenced
/// (routing keys, header keys, queue names). A panic there aborts the
/// async task and drops the whole connection, taking down every consumer
/// sharing it. This helper maps the overflow to a graceful
/// [`BusError::InvalidTopology`] instead.
///
/// `field` names the offending value for the error message.
///
/// # Errors
///
/// Returns [`BusError::InvalidTopology`] when `value` exceeds the
/// AMQP `ShortString` limit of 255 bytes.
pub(crate) fn to_short_string(value: &str, field: &str) -> Result<ShortString, BusError> {
    ShortString::try_new(value).map_err(|err| BusError::InvalidTopology {
        reason: format!("{field} exceeds the AMQP short-string limit of 255 bytes: {err}"),
    })
}

/// [`Transport`] implementation backed by RabbitMQ via [`lapin`].
///
/// A transport instance is bound to one logical AMQP exchange. The
/// [`Self::new`] constructor targets the AMQP default exchange (the
/// empty string), which routes each message directly to the queue
/// whose name matches the publish `routing_key`. The
/// [`Self::with_exchange`] constructor declares and uses a typed
/// [`Exchange`] for routing.
///
/// Each publish acquires a [`lapin::Channel`] from an internal
/// [`ChannelPool`] (default capacity `DEFAULT_POOL_MAX_SIZE`),
/// `basic_publish`es the JSON-encoded payload, and returns the
/// envelope's `message_id`. The AMQP `correlation_id` is minted by
/// the transport when the caller does not provide one.
///
/// By default every publish is hardened: the message is persistent
/// (`delivery_mode` 2), the publish is `mandatory`, and the transport
/// awaits a publisher confirm before returning. `Ok` therefore proves
/// the broker stored the message in at least one queue; an unroutable
/// routing key surfaces as [`BusError::Unroutable`] instead of
/// silently dropping the message. Opt out of the confirm round-trip
/// with [`Self::fire_and_forget`].
#[derive(Debug)]
pub struct RabbitMqTransport {
    pool: Arc<ChannelPool>,
    exchange: String,
}

impl RabbitMqTransport {
    /// Connect to `connection_string` and target the AMQP default exchange.
    ///
    /// Uses the bounded reconnect loop from
    /// [`RabbitMqConnection::connect_with_retry`] with the crate-wide
    /// [`DEFAULT_RETRY_ATTEMPTS`] and [`DEFAULT_RETRY_BASE_DELAY`].
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if the broker remains
    /// unreachable after the retry loop exits.
    pub async fn new(connection_string: &str) -> Result<Self, BusError> {
        let connection = RabbitMqConnection::connect_with_retry(
            connection_string,
            DEFAULT_RETRY_ATTEMPTS,
            DEFAULT_RETRY_BASE_DELAY,
        )
        .await?;
        let pool = Arc::new(ChannelPool::new(connection, DEFAULT_POOL_MAX_SIZE));
        Ok(Self {
            pool,
            exchange: String::new(),
        })
    }

    /// Connect to `connection_string`, declare `exchange` and target it.
    ///
    /// The exchange is declared as durable / auto-delete according to
    /// the flags carried by the [`Exchange`] declaration. Subsequent
    /// publishes on the resulting transport route through this
    /// exchange.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if the broker is unreachable
    /// or [`BusError::Transport`] if the exchange declaration is
    /// rejected (typically a mismatch with a pre-existing exchange).
    pub async fn with_exchange(
        connection_string: &str,
        exchange: Exchange,
    ) -> Result<Self, BusError> {
        let connection = RabbitMqConnection::connect_with_retry(
            connection_string,
            DEFAULT_RETRY_ATTEMPTS,
            DEFAULT_RETRY_BASE_DELAY,
        )
        .await?;
        let exchange_kind = exchange_kind_to_lapin(exchange.kind)?;
        let options = ExchangeDeclareOptions {
            durable: exchange.durable,
            auto_delete: exchange.auto_delete,
            ..ExchangeDeclareOptions::default()
        };
        let exchange_name = exchange.name;
        connection
            .with_channel(|channel| async move {
                let exchange_short = to_short_string(exchange_name.as_str(), "exchange name")?;
                channel
                    .exchange_declare(
                        exchange_short,
                        exchange_kind,
                        options,
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|err| BusError::Transport(Box::new(err)))?;
                Ok(exchange_name)
            })
            .await
            .map(|exchange_name| Self {
                pool: Arc::new(ChannelPool::new(connection, DEFAULT_POOL_MAX_SIZE)),
                exchange: exchange_name,
            })
    }

    /// Build a transport from an already-established [`RabbitMqConnection`].
    ///
    /// Useful when several transports share the same broker session
    /// or when the caller wants to drive the reconnect policy.
    /// Targets the AMQP default exchange.
    #[must_use]
    pub fn from_connection(connection: RabbitMqConnection, pool_size: usize) -> Self {
        Self {
            pool: Arc::new(ChannelPool::new(connection, pool_size)),
            exchange: String::new(),
        }
    }

    /// Switch the transport to fire-and-forget publishing.
    ///
    /// Disables publisher confirms and the `mandatory` flag: a publish
    /// returns as soon as the frame is written, without waiting for a
    /// broker acknowledgement. `Ok` no longer proves delivery, and an
    /// unroutable routing key drops the message silently instead of
    /// raising [`BusError::Unroutable`]. Messages are still published
    /// as persistent (`delivery_mode` 2).
    ///
    /// Reserve this mode for flows where loss is acceptable and
    /// throughput matters more than the delivery guarantee, such as
    /// metrics or non-critical fan-out, mirroring the consume-side
    /// trade-off of `AckMode::Unacknowledged`.
    #[must_use]
    pub fn fire_and_forget(mut self) -> Self {
        let pool = ChannelPool::new(self.pool.connection().clone(), self.pool.max_size())
            .without_confirms();
        self.pool = Arc::new(pool);
        self
    }

    /// Borrow the [`ChannelPool`] this transport publishes through.
    #[must_use]
    pub fn pool(&self) -> &ChannelPool {
        &self.pool
    }

    /// Borrow the AMQP exchange name this transport targets.
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    async fn publish_envelope(
        &self,
        routing_key: &str,
        envelope: &BusEnvelope,
    ) -> Result<Uuid, BusError> {
        let pooled = self.pool.acquire().await?;
        let properties = envelope_to_properties(envelope)?;
        let exchange = to_short_string(self.exchange.as_str(), "exchange name")?;
        let routing_key_short = to_short_string(routing_key, "routing key")?;
        let confirms = self.pool.confirms();
        let confirmation = pooled
            .channel()
            .basic_publish(
                exchange,
                routing_key_short,
                BasicPublishOptions {
                    mandatory: confirms,
                    ..BasicPublishOptions::default()
                },
                &envelope.payload,
                properties,
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;
        if confirms {
            crate::confirm::confirmation_to_result(confirmation, "publish", routing_key)?;
        }
        Ok(envelope.message_id)
    }
}

#[async_trait]
impl Transport for RabbitMqTransport {
    async fn publish<M: Message>(&self, routing_key: &str, message: &M) -> Result<Uuid, BusError> {
        let envelope = BusEnvelope::new(Uuid::now_v7(), message)?;
        self.publish_envelope(routing_key, &envelope).await
    }

    async fn publish_with_headers<M: Message>(
        &self,
        routing_key: &str,
        headers: HashMap<String, String>,
        message: &M,
    ) -> Result<Uuid, BusError> {
        let envelope = BusEnvelope::with_headers(Uuid::now_v7(), headers, message)?;
        self.publish_envelope(routing_key, &envelope).await
    }

    async fn publish_with_correlation_id<M: Message>(
        &self,
        routing_key: &str,
        correlation_id: Uuid,
        message: &M,
    ) -> Result<Uuid, BusError> {
        let envelope = BusEnvelope::new(correlation_id, message)?;
        self.publish_envelope(routing_key, &envelope).await
    }
}

#[async_trait]
impl RawBusPublish for RabbitMqTransport {
    async fn publish_raw(
        &self,
        routing_key: &str,
        message_id: Uuid,
        message_type: &str,
        payload: &[u8],
    ) -> Result<(), BusError> {
        let envelope = BusEnvelope::restore(
            message_id,
            message_type.to_owned(),
            payload.to_vec(),
            message_id,
            None,
            HashMap::new(),
            SystemTime::now(),
        );
        self.publish_envelope(routing_key, &envelope)
            .await
            .map(drop)
    }
}

pub(crate) fn exchange_kind_to_lapin(kind: ExchangeKind) -> Result<lapin::ExchangeKind, BusError> {
    // `ExchangeKind` is `#[non_exhaustive]`; an unknown future variant
    // must not be silently declared as `direct` (which would route every
    // message wrong). Reject it as invalid topology so the mismatch
    // surfaces at declare time instead of corrupting routing at runtime.
    match kind {
        ExchangeKind::Direct => Ok(lapin::ExchangeKind::Direct),
        ExchangeKind::Topic => Ok(lapin::ExchangeKind::Topic),
        ExchangeKind::Fanout => Ok(lapin::ExchangeKind::Fanout),
        ExchangeKind::Headers => Ok(lapin::ExchangeKind::Headers),
        other => Err(BusError::InvalidTopology {
            reason: format!("unsupported exchange kind {other:?}"),
        }),
    }
}

fn envelope_to_properties(envelope: &BusEnvelope) -> Result<BasicProperties, BusError> {
    let mut amqp_headers = FieldTable::default();
    for (k, v) in &envelope.headers {
        amqp_headers.insert(
            to_short_string(k.as_str(), "header key")?,
            AMQPValue::LongString(v.as_str().into()),
        );
    }
    let published_at_secs = envelope
        .published_at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut properties = BasicProperties::default()
        .with_message_id(envelope.message_id.to_string().into())
        .with_correlation_id(envelope.correlation_id.to_string().into())
        .with_content_type(JSON_CONTENT_TYPE.into())
        .with_type(to_short_string(
            envelope.message_type.as_str(),
            "message type",
        )?)
        .with_delivery_mode(2)
        .with_timestamp(published_at_secs)
        .with_headers(amqp_headers);
    if let Some(reply_to) = &envelope.reply_to {
        properties = properties.with_reply_to(to_short_string(reply_to.as_str(), "reply_to")?);
    }
    Ok(properties)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::Deserialize;
    use serde::Serialize;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: Uuid,
    }

    impl Message for OrderPlaced {
        const MESSAGE_TYPE: &'static str = "orders.placed";
    }

    #[test]
    fn exchange_kind_maps_to_lapin_variants() {
        assert!(matches!(
            exchange_kind_to_lapin(ExchangeKind::Direct).unwrap(),
            lapin::ExchangeKind::Direct
        ));
        assert!(matches!(
            exchange_kind_to_lapin(ExchangeKind::Topic).unwrap(),
            lapin::ExchangeKind::Topic
        ));
        assert!(matches!(
            exchange_kind_to_lapin(ExchangeKind::Fanout).unwrap(),
            lapin::ExchangeKind::Fanout
        ));
        assert!(matches!(
            exchange_kind_to_lapin(ExchangeKind::Headers).unwrap(),
            lapin::ExchangeKind::Headers
        ));
    }

    #[test]
    fn to_short_string_rejects_oversize_value() {
        let oversize = "a".repeat(256);
        let err = to_short_string(&oversize, "routing key")
            .expect_err("a 256-byte value must be rejected, not panic");
        match err {
            BusError::InvalidTopology { reason } => assert!(reason.contains("routing key")),
            other => panic!("expected InvalidTopology, got {other:?}"),
        }
    }

    #[test]
    fn to_short_string_accepts_value_at_limit() {
        let at_limit = "a".repeat(255);
        assert!(to_short_string(&at_limit, "routing key").is_ok());
    }

    #[test]
    fn envelope_to_properties_rejects_oversize_header_key() {
        let mut headers = HashMap::new();
        headers.insert("k".repeat(256), "v".to_owned());
        let envelope = BusEnvelope::with_headers(
            Uuid::from_u128(1),
            headers,
            &OrderPlaced {
                order_id: Uuid::from_u128(9),
            },
        )
        .unwrap();
        // Must return an error rather than panic inside ShortString::from.
        let result = envelope_to_properties(&envelope);
        assert!(
            matches!(result, Err(BusError::InvalidTopology { .. })),
            "an oversize header key must surface as InvalidTopology"
        );
    }

    #[test]
    fn envelope_to_properties_carries_core_fields() {
        let envelope = BusEnvelope::new(
            Uuid::from_u128(1),
            &OrderPlaced {
                order_id: Uuid::from_u128(2),
            },
        )
        .unwrap();
        let properties = envelope_to_properties(&envelope).unwrap();
        assert_eq!(
            properties.content_type().as_ref().map(ShortString::as_str),
            Some(JSON_CONTENT_TYPE)
        );
        assert_eq!(
            properties.kind().as_ref().map(ShortString::as_str),
            Some("orders.placed")
        );
        assert_eq!(
            properties.message_id().as_ref().map(ShortString::as_str),
            Some(envelope.message_id.to_string().as_str())
        );
        assert_eq!(
            properties
                .correlation_id()
                .as_ref()
                .map(ShortString::as_str),
            Some(envelope.correlation_id.to_string().as_str())
        );
        assert!(properties.reply_to().is_none());
    }

    #[test]
    fn envelope_to_properties_includes_headers_and_reply_to() {
        let mut headers = HashMap::new();
        headers.insert("tenant".to_owned(), "acme".to_owned());
        let envelope = BusEnvelope::with_headers(
            Uuid::from_u128(1),
            headers,
            &OrderPlaced {
                order_id: Uuid::from_u128(3),
            },
        )
        .unwrap();
        let properties = envelope_to_properties(&envelope).unwrap();
        let table = properties.headers().as_ref().expect("headers must be set");
        let value = table.inner().get(&ShortString::from("tenant"));
        match value {
            Some(AMQPValue::LongString(s)) => {
                let decoded = std::str::from_utf8(s.as_bytes()).expect("header must be UTF-8");
                assert_eq!(decoded, "acme");
            }
            other => panic!("expected LongString header, got {other:?}"),
        }
    }

    #[test]
    fn envelope_to_properties_sets_persistent_delivery_mode() {
        let envelope = BusEnvelope::new(
            Uuid::from_u128(1),
            &OrderPlaced {
                order_id: Uuid::from_u128(4),
            },
        )
        .unwrap();
        let properties = envelope_to_properties(&envelope).unwrap();
        assert_eq!(*properties.delivery_mode(), Some(2));
    }

    #[test]
    fn envelope_to_properties_propagates_published_at_as_timestamp() {
        let envelope = BusEnvelope::new(
            Uuid::from_u128(1),
            &OrderPlaced {
                order_id: Uuid::from_u128(5),
            },
        )
        .unwrap();
        let expected = envelope
            .published_at
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let properties = envelope_to_properties(&envelope).unwrap();
        assert_eq!(*properties.timestamp(), Some(expected));
    }

    #[tokio::test]
    async fn new_returns_connection_error_on_unreachable_broker() {
        let err = RabbitMqTransport::new("amqp://127.0.0.1:1")
            .await
            .expect_err("must fail to connect");
        assert!(matches!(err, BusError::Connection(_)));
    }

    #[tokio::test]
    async fn with_exchange_returns_connection_error_on_unreachable_broker() {
        let exchange = Exchange::new("orders", ExchangeKind::Topic).unwrap();
        let err = RabbitMqTransport::with_exchange("amqp://127.0.0.1:1", exchange)
            .await
            .expect_err("must fail to connect");
        assert!(matches!(err, BusError::Connection(_)));
    }

    #[test]
    fn default_constants_are_sane() {
        assert!(DEFAULT_RETRY_BASE_DELAY <= Duration::from_secs(1));
    }
}
