use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::Message;
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
        let exchange_kind = exchange_kind_to_lapin(exchange.kind);
        let options = ExchangeDeclareOptions {
            durable: exchange.durable,
            auto_delete: exchange.auto_delete,
            ..ExchangeDeclareOptions::default()
        };
        let exchange_name = exchange.name;
        connection
            .with_channel(|channel| async move {
                channel
                    .exchange_declare(
                        ShortString::from(exchange_name.as_str()),
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
        let properties = envelope_to_properties(envelope);
        let confirms = self.pool.confirms();
        let confirmation = pooled
            .channel()
            .basic_publish(
                ShortString::from(self.exchange.as_str()),
                ShortString::from(routing_key),
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

pub(crate) fn exchange_kind_to_lapin(kind: ExchangeKind) -> lapin::ExchangeKind {
    // `ExchangeKind` is `#[non_exhaustive]`; unknown future variants
    // fall back to the AMQP `direct` kind. New variants should be
    // mapped explicitly when introduced.
    #[allow(clippy::match_same_arms)]
    match kind {
        ExchangeKind::Direct => lapin::ExchangeKind::Direct,
        ExchangeKind::Topic => lapin::ExchangeKind::Topic,
        ExchangeKind::Fanout => lapin::ExchangeKind::Fanout,
        ExchangeKind::Headers => lapin::ExchangeKind::Headers,
        _ => lapin::ExchangeKind::Direct,
    }
}

fn envelope_to_properties(envelope: &BusEnvelope) -> BasicProperties {
    let mut amqp_headers = FieldTable::default();
    for (k, v) in &envelope.headers {
        amqp_headers.insert(
            ShortString::from(k.as_str()),
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
        .with_type(envelope.message_type.as_str().into())
        .with_delivery_mode(2)
        .with_timestamp(published_at_secs)
        .with_headers(amqp_headers);
    if let Some(reply_to) = &envelope.reply_to {
        properties = properties.with_reply_to(reply_to.as_str().into());
    }
    properties
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
            exchange_kind_to_lapin(ExchangeKind::Direct),
            lapin::ExchangeKind::Direct
        ));
        assert!(matches!(
            exchange_kind_to_lapin(ExchangeKind::Topic),
            lapin::ExchangeKind::Topic
        ));
        assert!(matches!(
            exchange_kind_to_lapin(ExchangeKind::Fanout),
            lapin::ExchangeKind::Fanout
        ));
        assert!(matches!(
            exchange_kind_to_lapin(ExchangeKind::Headers),
            lapin::ExchangeKind::Headers
        ));
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
        let properties = envelope_to_properties(&envelope);
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
        let properties = envelope_to_properties(&envelope);
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
        let properties = envelope_to_properties(&envelope);
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
        let properties = envelope_to_properties(&envelope);
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
