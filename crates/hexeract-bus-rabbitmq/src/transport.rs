use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::RawBusPublish;
use hexeract_bus::SecurityHeaders;
use hexeract_bus::Transport;
use lapin::BasicProperties;
use lapin::Confirmation;
use lapin::options::BasicPublishOptions;
use lapin::options::ExchangeDeclareOptions;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use uuid::Uuid;

use crate::connection::{RabbitMqConnection, RabbitMqConnectionConfig};
use crate::envelope_security::OutboundEnvelopeSecurity;
use crate::metadata::AmqpMetadataLimits;
use crate::metadata::encode_headers;
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
/// [`RabbitMqTransportConfig::with_exchange`] setting, passed to
/// [`Self::new_with_config`], declares and uses a typed [`Exchange`] for
/// routing.
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
/// with [`RabbitMqTransportConfig::fire_and_forget`].
#[derive(Debug)]
pub struct RabbitMqTransport {
    pool: Arc<ChannelPool>,
    exchange: String,
    metadata_limits: AmqpMetadataLimits,
    envelope_security: Option<Arc<OutboundEnvelopeSecurity>>,
}

/// Configuration for [`RabbitMqTransport::new_with_config`].
///
/// Bundles every setting an application supplies when it opens a transport:
/// the connection settings (TLS and retry), the exchange to declare, the
/// publish mode, the outbound metadata bounds and the outbound envelope
/// security. Private fields, a `with_*` method per setting that consumes and
/// returns `Self`, and an untouched value producing exactly the defaults of
/// [`RabbitMqTransport::new`]: the same construction style as
/// [`RabbitMqConnectionConfig`].
#[derive(Clone, Default)]
pub struct RabbitMqTransportConfig {
    connection_config: RabbitMqConnectionConfig,
    metadata_limits: AmqpMetadataLimits,
    envelope_security: Option<Arc<OutboundEnvelopeSecurity>>,
    exchange: Option<Exchange>,
    is_fire_and_forget: bool,
}

impl RabbitMqTransportConfig {
    /// Use the supplied connection settings (TLS and retry) for the
    /// connection this transport opens.
    #[must_use]
    pub fn with_connection_config(mut self, connection_config: RabbitMqConnectionConfig) -> Self {
        self.connection_config = connection_config;
        self
    }

    /// Bound the AMQP metadata this transport is willing to publish.
    ///
    /// Applies to the combined application and framework protocol headers of
    /// every publish. Validation happens before a channel is acquired, so a
    /// rejected envelope never reaches the broker.
    #[must_use]
    pub fn with_metadata_limits(mut self, metadata_limits: AmqpMetadataLimits) -> Self {
        self.metadata_limits = metadata_limits;
        self
    }

    /// Sign every envelope the resulting transport publishes with `security`.
    ///
    /// Each envelope is signed once for the routing key it is actually
    /// published to, and the resulting headers are merged into the AMQP
    /// properties alongside the application and protocol metadata.
    #[must_use]
    pub fn with_outbound_envelope_security(
        mut self,
        security: Arc<OutboundEnvelopeSecurity>,
    ) -> Self {
        self.envelope_security = Some(security);
        self
    }

    /// Declare `exchange` at connection time and publish to it (default: the
    /// AMQP default exchange).
    #[must_use]
    pub fn with_exchange(mut self, exchange: Exchange) -> Self {
        self.exchange = Some(exchange);
        self
    }

    /// Publish without publisher confirms and without `mandatory` (default:
    /// confirmed and mandatory).
    ///
    /// A publish returns as soon as the frame is written, without waiting for
    /// a broker acknowledgement. `Ok` no longer proves delivery, and an
    /// unroutable routing key drops the message silently instead of raising
    /// [`BusError::Unroutable`]. Messages are still published as persistent
    /// (`delivery_mode` 2).
    ///
    /// Reserve this mode for flows where loss is acceptable and throughput
    /// matters more than the delivery guarantee, such as metrics or
    /// non-critical fan-out, mirroring the consume-side trade-off of
    /// `AckMode::Unacknowledged`.
    #[must_use]
    pub fn fire_and_forget(mut self) -> Self {
        self.is_fire_and_forget = true;
        self
    }

    pub(crate) fn connection_config(&self) -> &RabbitMqConnectionConfig {
        &self.connection_config
    }

    pub(crate) fn metadata_limits(&self) -> AmqpMetadataLimits {
        self.metadata_limits
    }

    pub(crate) fn outbound_envelope_security(&self) -> Option<Arc<OutboundEnvelopeSecurity>> {
        self.envelope_security.clone()
    }

    pub(crate) fn exchange(&self) -> Option<&Exchange> {
        self.exchange.as_ref()
    }

    pub(crate) fn is_fire_and_forget(&self) -> bool {
        self.is_fire_and_forget
    }
}

impl RabbitMqTransport {
    /// Connect to `connection_string` with the default configuration and
    /// target the AMQP default exchange.
    ///
    /// Equivalent to [`Self::new_with_config`] with
    /// [`RabbitMqTransportConfig::default`]. The resulting publisher
    /// connection has lapin auto-recovery enabled, so it heals itself across
    /// a broker blip instead of failing forever (#334): a publisher is long
    /// lived and has no supervisor to rebuild it, unlike a
    /// [`crate::RabbitMqWorker`], whose supervisor owns its recovery.
    ///
    /// # Errors
    ///
    /// Same as [`Self::new_with_config`].
    pub async fn new(connection_string: &str) -> Result<Self, BusError> {
        Self::new_with_config(connection_string, &RabbitMqTransportConfig::default()).await
    }

    /// Connect to `connection_string`, applying every setting `config`
    /// carries.
    ///
    /// Connects with the retry settings of the connection configuration
    /// ([`RabbitMqConnectionConfig::with_retry_attempts`] and
    /// [`RabbitMqConnectionConfig::with_retry_base_delay`]), declares the
    /// exchange when [`RabbitMqTransportConfig::with_exchange`] was set, and
    /// builds the channel pool with or without publisher confirms according to
    /// [`RabbitMqTransportConfig::fire_and_forget`]. The exchange is declared
    /// as durable / auto-delete according to the flags carried by the
    /// [`Exchange`] declaration.
    ///
    /// # Errors
    ///
    /// Returns a credential-redacted [`BusError::Connection`] when the broker
    /// cannot be reached within the connect bounds, and a permanent one when
    /// the transport would carry the session in cleartext: either `config`
    /// holds TLS material that `connection_string` would discard, or
    /// `connection_string` selects `amqp://` against a host outside loopback.
    /// [`RabbitMqConnectionConfig::allow_insecure_plaintext_transport`] opts
    /// out of both. Returns [`BusError::Transport`] if the exchange
    /// declaration is rejected (typically a mismatch with a pre-existing
    /// exchange).
    ///
    /// Against an unreachable broker the constructor retries up to the
    /// configured number of attempts in each phase before giving up, or
    /// earlier when a failure classifies as permanent (#340): first the
    /// probe, then, once the broker has proven it answers, the real
    /// auto-recovering session. The retry settings therefore govern the
    /// connection this function actually returns, not just the probe. Each
    /// phase also carries a time bound for the broker that accepts a
    /// connection and then never completes it: the probe by
    /// [`crate::connection::DEFAULT_PROBE_TIMEOUT`], the session by
    /// [`crate::connection::DEFAULT_SESSION_TIMEOUT`]. No path blocks
    /// forever.
    pub async fn new_with_config(
        connection_string: &str,
        config: &RabbitMqTransportConfig,
    ) -> Result<Self, BusError> {
        let connection = RabbitMqConnection::connect_with_retry_recovering_with_config(
            connection_string,
            config.connection_config(),
        )
        .await?;
        let exchange_name = match config.exchange() {
            Some(exchange) => declare_exchange(&connection, exchange).await?,
            None => String::new(),
        };
        let pool = ChannelPool::new(connection, DEFAULT_POOL_MAX_SIZE);
        let pool = if config.is_fire_and_forget() {
            pool.without_confirms()
        } else {
            pool
        };
        let transport = Self {
            pool: Arc::new(pool),
            exchange: exchange_name,
            metadata_limits: AmqpMetadataLimits::default(),
            envelope_security: None,
        }
        .with_metadata_limits(config.metadata_limits());
        Ok(match config.outbound_envelope_security() {
            Some(security) => transport.with_outbound_envelope_security(security),
            None => transport,
        })
    }

    /// Build a transport from an already-established [`RabbitMqConnection`].
    ///
    /// Targets the AMQP default exchange.
    #[must_use]
    pub(crate) fn from_connection(connection: RabbitMqConnection, pool_size: usize) -> Self {
        Self {
            pool: Arc::new(ChannelPool::new(connection, pool_size)),
            exchange: String::new(),
            metadata_limits: AmqpMetadataLimits::default(),
            envelope_security: None,
        }
    }

    /// Bound the AMQP metadata this transport is willing to publish.
    #[must_use]
    pub(crate) fn with_metadata_limits(mut self, metadata_limits: AmqpMetadataLimits) -> Self {
        self.metadata_limits = metadata_limits;
        self
    }

    /// Borrow the metadata limits applied to outbound publishes.
    #[must_use]
    pub fn metadata_limits(&self) -> AmqpMetadataLimits {
        self.metadata_limits
    }

    /// Sign every envelope this transport publishes with `security`.
    ///
    /// [`Self::publish_envelope`] signs each envelope for the routing key it
    /// is actually publishing to, once, before entering its channel-recovery
    /// loop, and merges the resulting headers into the AMQP properties
    /// alongside the application and protocol metadata.
    #[must_use]
    pub(crate) fn with_outbound_envelope_security(
        mut self,
        security: Arc<OutboundEnvelopeSecurity>,
    ) -> Self {
        self.envelope_security = Some(security);
        self
    }

    /// The outbound envelope security configured for this transport, if any.
    #[must_use]
    pub fn outbound_envelope_security(&self) -> Option<Arc<OutboundEnvelopeSecurity>> {
        self.envelope_security.clone()
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
}

/// Declare `exchange` on a fresh channel of `connection` and return its name.
///
/// # Errors
///
/// Returns [`BusError::Transport`] if the broker rejects the declaration,
/// and [`BusError::InvalidTopology`] if the name exceeds the AMQP
/// short-string limit.
async fn declare_exchange(
    connection: &RabbitMqConnection,
    exchange: &Exchange,
) -> Result<String, BusError> {
    let exchange_kind = exchange_kind_to_lapin(exchange.kind)?;
    let options = ExchangeDeclareOptions {
        durable: exchange.durable,
        auto_delete: exchange.auto_delete,
        ..ExchangeDeclareOptions::default()
    };
    let exchange_name = exchange.name.clone();
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
}

#[async_trait]
impl Transport for RabbitMqTransport {
    async fn publish_envelope(
        &self,
        routing_key: &str,
        envelope: &BusEnvelope,
    ) -> Result<Uuid, BusError> {
        // Signing happens exactly once here, before the properties are built
        // and before the channel-recovery loop below runs, so a transient
        // channel failure retries only the publish, never the signature: the
        // canonical representation is bound to `routing_key`, the routing key
        // actually observed at publication, never to a value read back from
        // the envelope.
        let security_headers = self
            .envelope_security
            .as_ref()
            .map(|security| security.sign(envelope, routing_key))
            .transpose()
            .map_err(BusError::EnvelopeSecurity)?;
        let broker_user_id = self
            .envelope_security
            .as_ref()
            .and_then(|security| security.broker_user_id());

        let properties = envelope_to_properties(
            envelope,
            self.metadata_limits,
            security_headers.as_ref(),
            broker_user_id,
        )?;
        let exchange = to_short_string(self.exchange.as_str(), "exchange name")?;
        let routing_key_short = to_short_string(routing_key, "routing key")?;
        let confirms = self.pool.confirms();

        let confirmation = publish_with_recovery(
            &self.pool,
            exchange,
            routing_key_short,
            &envelope.payload,
            properties,
            confirms,
        )
        .await?;
        if confirms {
            crate::confirm::confirmation_to_result(confirmation, "publish", routing_key)?;
        }
        Ok(envelope.message_id)
    }
}

/// Publish one already-built message, retrying once across a channel recovery.
///
/// When the connection is mid auto-recovery the first publish can fail with a
/// recoverable lapin error; await the channel recovery and retry once on a
/// fresh channel, so a transient broker blip does not surface as a publish
/// failure. This matters for a synchronous request/reply waiting on the reply
/// (#334).
///
/// A free function rather than a method on [`RabbitMqTransport`], and
/// deliberately so. A signature covers one publication, never one attempt: a
/// retry must re-send the properties it was handed, and must not ask the key
/// source for a second signature, which would both waste the work and make
/// recovery from a broker blip depend on the key source still being
/// reachable. Taking the pool and the finished properties, and nothing else,
/// puts every signing input out of scope in this body, so re-signing inside
/// the retry stops being a mistake to remember not to make and becomes a line
/// that does not compile.
async fn publish_with_recovery(
    pool: &ChannelPool,
    exchange: ShortString,
    routing_key: ShortString,
    payload: &[u8],
    properties: BasicProperties,
    mandatory: bool,
) -> Result<Confirmation, BusError> {
    let mut recovered = false;
    loop {
        let pooled = pool.acquire().await?;
        let outcome = async {
            pooled
                .channel()
                .basic_publish(
                    exchange.clone(),
                    routing_key.clone(),
                    BasicPublishOptions {
                        mandatory,
                        ..BasicPublishOptions::default()
                    },
                    payload,
                    properties.clone(),
                )
                .await?
                .await
        }
        .await;
        match outcome {
            Ok(confirmation) => return Ok(confirmation),
            Err(err) => {
                if recovered {
                    return Err(BusError::Transport(Box::new(err)));
                }
                match pooled.channel().wait_for_recovery(err).await {
                    // Recovered: loop once more on a freshly acquired channel.
                    // Falling through to the end of the loop body re-enters the
                    // attempt, so no explicit continue.
                    Ok(()) => recovered = true,
                    Err(err) => return Err(BusError::Transport(Box::new(err))),
                }
            }
        }
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

/// Assemble the AMQP properties published for `envelope`.
///
/// `security_headers`, when present, is bound to the same
/// [`AmqpMetadataLimits`] as every other header: see [`encode_headers`].
/// `broker_user_id`, when present, is published as the AMQP `user-id`
/// property alongside the signed `x-hexeract-issuer` header.
fn envelope_to_properties(
    envelope: &BusEnvelope,
    metadata_limits: AmqpMetadataLimits,
    security_headers: Option<&SecurityHeaders>,
    broker_user_id: Option<&str>,
) -> Result<BasicProperties, BusError> {
    let amqp_headers = encode_headers(envelope, metadata_limits, security_headers)?;
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
    if let Some(user_id) = broker_user_id {
        properties = properties.with_user_id(to_short_string(user_id, "user id")?);
    }
    Ok(properties)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ed25519_dalek::SigningKey;
    use hexeract_bus::Message;
    use lapin::types::AMQPValue;
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;

    /// The wire names of the six security headers, in no particular order.
    const SECURITY_HEADER_NAMES: [&str; 6] = [
        "x-hexeract-signature",
        "x-hexeract-key-id",
        "x-hexeract-issuer",
        "x-hexeract-audience",
        "x-hexeract-algorithm",
        "x-hexeract-destination",
    ];

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

        // Under the defaults the metadata limit is stricter than the AMQP
        // short-string bound, so the key is rejected by dimension.
        assert!(
            matches!(
                envelope_to_properties(&envelope, AmqpMetadataLimits::default(), None, None),
                Err(BusError::MetadataLimitExceeded {
                    limit: hexeract_bus::MetadataLimit::KeyBytes,
                    actual: 256,
                    max: 128,
                })
            ),
            "an oversize header key must surface as a key-bytes limit violation"
        );

        // Raising the limit past the AMQP short-string bound hands the key to
        // `to_short_string`, which must still return an error rather than
        // panic inside `ShortString::from`.
        let permissive = AmqpMetadataLimits {
            max_key_bytes: 1024,
            ..AmqpMetadataLimits::default()
        };
        assert!(
            matches!(
                envelope_to_properties(&envelope, permissive, None, None),
                Err(BusError::InvalidTopology { .. })
            ),
            "a key past the AMQP short-string bound must surface as InvalidTopology"
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
        let properties =
            envelope_to_properties(&envelope, AmqpMetadataLimits::default(), None, None).unwrap();
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
        let properties =
            envelope_to_properties(&envelope, AmqpMetadataLimits::default(), None, None).unwrap();
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
        let properties =
            envelope_to_properties(&envelope, AmqpMetadataLimits::default(), None, None).unwrap();
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
        let properties =
            envelope_to_properties(&envelope, AmqpMetadataLimits::default(), None, None).unwrap();
        assert_eq!(*properties.timestamp(), Some(expected));
    }

    // The connect-failure path of `new_with_config` is covered by
    // `connection::tests::connect_with_config_returns_connection_error_after_max_attempts`
    // and by `connection::tests::connect_recovering_gives_up_once_its_bound_elapses`.
    // The constructor only forwards `connect_with_retry_recovering_with_config`
    // through `?`, so exercising it here would re-test the same mapping.

    #[test]
    fn default_constants_are_sane() {
        assert!(crate::connection::DEFAULT_RETRY_BASE_DELAY <= Duration::from_secs(1));
    }

    fn outbound_security() -> Arc<OutboundEnvelopeSecurity> {
        let keys: Arc<dyn hexeract_bus::SigningKeySource> =
            Arc::new(hexeract_bus::StaticKeySource::builder().build());
        Arc::new(OutboundEnvelopeSecurity::new(
            hexeract_bus::Issuer::new("billing-service").expect("valid issuer"),
            hexeract_bus::Audience::new("ledger-service").expect("valid audience"),
            keys,
        ))
    }

    /// An [`OutboundEnvelopeSecurity`] carrying an actual signing key, unlike
    /// [`outbound_security`], which is only ever used to prove configuration
    /// carries through unsigned wiring paths.
    fn signed_security() -> OutboundEnvelopeSecurity {
        let keys: Arc<dyn hexeract_bus::SigningKeySource> = Arc::new(
            hexeract_bus::StaticKeySource::builder()
                .with_signing_key(
                    hexeract_bus::KeyId::new("2026-09").expect("valid key id"),
                    hexeract_bus::SigningKeyHandle::from(SigningKey::from_bytes(&[3; 32])),
                )
                .build(),
        );
        OutboundEnvelopeSecurity::new(
            hexeract_bus::Issuer::new("billing-service").expect("valid issuer"),
            hexeract_bus::Audience::new("ledger-service").expect("valid audience"),
            keys,
        )
    }

    fn order_placed(seed: u128) -> BusEnvelope {
        BusEnvelope::new(
            Uuid::from_u128(1),
            &OrderPlaced {
                order_id: Uuid::from_u128(seed),
            },
        )
        .unwrap()
    }

    // ------------------------------------------------------ security headers

    #[test]
    fn signing_puts_the_six_security_headers_on_the_wire() {
        let security = signed_security();
        let envelope = order_placed(101);
        let headers = security
            .sign(&envelope, "orders.placed")
            .expect("signing must succeed");

        let properties = envelope_to_properties(
            &envelope,
            AmqpMetadataLimits::default(),
            Some(&headers),
            None,
        )
        .unwrap();
        let table = properties.headers().as_ref().expect("headers must be set");

        for name in SECURITY_HEADER_NAMES {
            assert!(
                table.inner().contains_key(&ShortString::from(name)),
                "missing security header {name}"
            );
        }
    }

    #[test]
    fn an_unsigned_transport_publishes_no_security_header() {
        let envelope = order_placed(102);

        let properties =
            envelope_to_properties(&envelope, AmqpMetadataLimits::default(), None, None).unwrap();
        let table = properties.headers().as_ref().expect("headers must be set");

        for name in SECURITY_HEADER_NAMES {
            assert!(
                !table.inner().contains_key(&ShortString::from(name)),
                "unexpected security header {name} on an unsigned publish"
            );
        }
    }

    #[test]
    fn the_published_timestamp_is_the_second_the_signature_covers() {
        let published_at =
            std::time::UNIX_EPOCH + Duration::from_secs(1_757_000_000) + Duration::from_millis(999);
        let envelope = BusEnvelope::restore_from_transport(
            Uuid::from_u128(1),
            "orders.placed".to_owned(),
            b"{}".to_vec(),
            Uuid::from_u128(2),
            None,
            HashMap::new(),
            HashMap::new(),
            published_at,
        );
        let security = signed_security();
        let headers = security
            .sign(&envelope, "orders.placed")
            .expect("signing must succeed");

        let properties = envelope_to_properties(
            &envelope,
            AmqpMetadataLimits::default(),
            Some(&headers),
            None,
        )
        .unwrap();

        assert_eq!(
            *properties.timestamp(),
            Some(1_757_000_000),
            "the AMQP timestamp must be the whole second the canonical representation covers, \
             not a value that also carries the sub-second remainder the signature ignores"
        );
    }

    #[test]
    fn user_id_is_absent_unless_the_publisher_binds_it() {
        let security = signed_security();
        let envelope = order_placed(103);

        let properties = envelope_to_properties(
            &envelope,
            AmqpMetadataLimits::default(),
            None,
            security.broker_user_id(),
        )
        .unwrap();

        assert!(properties.user_id().is_none());
    }

    #[test]
    fn user_id_carries_the_issuer_when_bound() {
        let security = signed_security().bind_issuer_to_broker_user();
        let envelope = order_placed(104);

        let properties = envelope_to_properties(
            &envelope,
            AmqpMetadataLimits::default(),
            None,
            security.broker_user_id(),
        )
        .unwrap();

        assert_eq!(
            properties.user_id().as_ref().map(ShortString::as_str),
            Some("billing-service")
        );
    }

    #[test]
    fn transport_config_default_matches_new_with_config_defaults() {
        let config = RabbitMqTransportConfig::default();

        assert!(!config.connection_config().allows_plaintext_transport());
        assert_eq!(config.metadata_limits(), AmqpMetadataLimits::default());
        assert!(config.outbound_envelope_security().is_none());
    }

    #[test]
    fn transport_config_builder_carries_every_setting() {
        let limits = AmqpMetadataLimits {
            max_headers: 3,
            ..AmqpMetadataLimits::default()
        };
        let security = outbound_security();

        let config = RabbitMqTransportConfig::default()
            .with_connection_config(
                RabbitMqConnectionConfig::default().allow_insecure_plaintext_transport(),
            )
            .with_metadata_limits(limits)
            .with_outbound_envelope_security(Arc::clone(&security));

        assert!(config.connection_config().allows_plaintext_transport());
        assert_eq!(config.metadata_limits(), limits);
        assert!(config.outbound_envelope_security().is_some());
    }
}
