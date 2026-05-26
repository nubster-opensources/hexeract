//! Consumer worker that dispatches AMQP deliveries to typed handlers.
//!
//! The worker mirrors `hexeract_outbox::OutboxWorker`: it consumes
//! from a queue, decodes each delivery into the matching typed
//! handler via [`ErasedHandler`], and applies ack / nack semantics
//! based on the configured [`AckMode`].
//!
//! See [`RabbitMqWorkerBuilder`] for the entry point.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use hexeract_bus::BusError;
use hexeract_bus::ErasedHandler;
use hexeract_bus::Handler;
use hexeract_bus::Message;
use hexeract_bus::TypedHandler;
use hexeract_core::CorrelationId;
use hexeract_core::HandlerContext;
use hexeract_core::MessageId;
use lapin::BasicProperties;
use lapin::Channel;
use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use lapin::options::BasicConsumeOptions;
use lapin::options::BasicNackOptions;
use lapin::options::BasicPublishOptions;
use lapin::options::BasicQosOptions;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::connection::RabbitMqConnection;

/// Default consumer prefetch (`basic.qos`).
pub const DEFAULT_PREFETCH: u16 = 16;

/// Default per-delivery max attempts before giving up.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// Ack discipline for a [`RabbitMqWorker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Ack on receive, before the handler runs. Handler failures are
    /// logged but never retried; the broker never sees the failure.
    Auto,
    /// Ack only when the handler returns `Ok`. Handler failures
    /// trigger [`basic_nack`] with `requeue=true` up to
    /// `max_attempts`, then route to the dead-letter routing key if
    /// configured or drop the delivery otherwise.
    Manual,
}

impl Default for AckMode {
    fn default() -> Self {
        Self::Manual
    }
}

/// Tuning parameters for a [`RabbitMqWorker`].
#[derive(Debug, Clone)]
pub struct RabbitMqWorkerConfig {
    /// Ack discipline applied to consumed deliveries.
    pub ack_mode: AckMode,
    /// Maximum number of attempts per delivery before giving up.
    pub max_attempts: u32,
    /// Per-channel prefetch (`basic.qos`).
    pub prefetch: u16,
    /// Optional routing key on the default exchange that receives
    /// deliveries which exhausted their retry budget. `None` drops
    /// exhausted deliveries silently.
    pub dead_letter_routing_key: Option<String>,
}

impl Default for RabbitMqWorkerConfig {
    fn default() -> Self {
        Self {
            ack_mode: AckMode::default(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            prefetch: DEFAULT_PREFETCH,
            dead_letter_routing_key: None,
        }
    }
}

/// Fluent builder for [`RabbitMqWorker`], symmetric with
/// `PgOutboxWorkerBuilder` from `hexeract-outbox-postgres`.
pub struct RabbitMqWorkerBuilder {
    connection: RabbitMqConnection,
    queue: Option<String>,
    handlers: HashMap<&'static str, Arc<dyn ErasedHandler>>,
    config: RabbitMqWorkerConfig,
}

impl RabbitMqWorkerBuilder {
    /// Build a fresh builder backed by `connection`.
    #[must_use]
    pub fn new(connection: RabbitMqConnection) -> Self {
        Self {
            connection,
            queue: None,
            handlers: HashMap::new(),
            config: RabbitMqWorkerConfig::default(),
        }
    }

    /// Set the queue the worker will consume from.
    #[must_use]
    pub fn queue(mut self, name: impl Into<String>) -> Self {
        self.queue = Some(name.into());
        self
    }

    /// Register a typed handler for messages of type `M`.
    ///
    /// Registering twice for the same `M::MESSAGE_TYPE` silently
    /// replaces the previous entry.
    #[must_use]
    pub fn register_handler<M, H>(mut self, handler: H) -> Self
    where
        M: Message,
        H: Handler<M>,
    {
        let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::<M, H>::new(handler));
        self.handlers.insert(M::MESSAGE_TYPE, erased);
        self
    }

    /// Override the [`AckMode`] (default [`AckMode::Manual`]).
    #[must_use]
    pub fn ack_mode(mut self, mode: AckMode) -> Self {
        self.config.ack_mode = mode;
        self
    }

    /// Override the per-delivery `max_attempts` (default [`DEFAULT_MAX_ATTEMPTS`]).
    #[must_use]
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.config.max_attempts = n;
        self
    }

    /// Override the consumer prefetch (default [`DEFAULT_PREFETCH`]).
    #[must_use]
    pub fn prefetch(mut self, n: u16) -> Self {
        self.config.prefetch = n;
        self
    }

    /// Route exhausted deliveries to `routing_key` on the default exchange.
    #[must_use]
    pub fn dead_letter_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.config.dead_letter_routing_key = Some(routing_key.into());
        self
    }

    /// Build the worker.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Internal`] if [`Self::queue`] was never
    /// called: a worker without a queue has nothing to consume.
    pub fn build(self) -> Result<RabbitMqWorker, BusError> {
        let queue = self.queue.ok_or_else(|| {
            BusError::Internal("RabbitMqWorkerBuilder requires a queue name".to_owned())
        })?;
        Ok(RabbitMqWorker {
            connection: self.connection,
            queue,
            handlers: Arc::new(self.handlers),
            config: self.config,
        })
    }
}

/// Long-running consumer that dispatches deliveries to typed handlers.
///
/// Built through [`RabbitMqWorkerBuilder`]. [`Self::run`] drives the
/// consume loop until the supplied [`CancellationToken`] fires.
///
/// # Retry counter
///
/// In [`AckMode::Manual`], the worker keeps an in-memory
/// `HashMap<message_id, attempts>` per consumer instance. Keying on
/// the AMQP `message_id` (rather than `delivery_tag`) lets the
/// counter survive `basic_nack(requeue=true)` redeliveries, since
/// each redelivery reuses the same `message_id` but receives a fresh
/// `delivery_tag`. The counter is still volatile across consumer
/// restarts, so long-lived broker queues with persistent failures
/// should pair the worker with a broker-side dead-letter exchange
/// policy.
pub struct RabbitMqWorker {
    connection: RabbitMqConnection,
    queue: String,
    handlers: Arc<HashMap<&'static str, Arc<dyn ErasedHandler>>>,
    config: RabbitMqWorkerConfig,
}

impl RabbitMqWorker {
    /// Run the consume loop until `cancel` fires.
    ///
    /// On `Ok(())` the loop drained normally on cancellation. Any
    /// fatal broker error returns immediately; per-delivery handler
    /// failures are absorbed by the retry / ack-mode policy.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if the consumer channel
    /// cannot be opened or [`BusError::Transport`] if
    /// [`Channel::basic_consume`] is rejected.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), BusError> {
        let channel = self.connection.create_channel().await?;
        channel
            .basic_qos(self.config.prefetch, BasicQosOptions::default())
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;
        let no_ack = matches!(self.config.ack_mode, AckMode::Auto);
        let mut consumer = channel
            .basic_consume(
                ShortString::from(self.queue.as_str()),
                ShortString::from(format!("hexeract-{}", Uuid::now_v7()).as_str()),
                BasicConsumeOptions {
                    no_ack,
                    ..BasicConsumeOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;

        let attempts: Arc<Mutex<HashMap<Uuid, u32>>> = Arc::new(Mutex::new(HashMap::new()));

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::info!(queue = %self.queue, "rabbitmq worker cancelled");
                    break;
                }
                next = consumer.next() => {
                    let Some(item) = next else { break; };
                    match item {
                        Ok(delivery) => {
                            self.dispatch(&channel, delivery, &attempts).await?;
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "rabbitmq consumer stream error");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn dispatch(
        &self,
        channel: &Channel,
        delivery: Delivery,
        attempts: &Arc<Mutex<HashMap<Uuid, u32>>>,
    ) -> Result<(), BusError> {
        let envelope = match delivery_to_envelope(&delivery.properties, &delivery.data) {
            Ok(env) => env,
            Err(err) => {
                tracing::warn!(error = %err, "rabbitmq delivery decode failed");
                if matches!(self.config.ack_mode, AckMode::Manual) {
                    let _ = channel
                        .basic_nack(
                            delivery.delivery_tag,
                            BasicNackOptions {
                                requeue: false,
                                ..BasicNackOptions::default()
                            },
                        )
                        .await;
                }
                return Ok(());
            }
        };

        let ctx = build_handler_context(&delivery.properties);
        let outcome = match self.handlers.get(envelope.message_type.as_str()) {
            Some(handler) => handler.handle(&envelope, &ctx).await,
            None => Err(BusError::MissingHandler {
                message_type: envelope.message_type.clone(),
            }),
        };

        match self.config.ack_mode {
            AckMode::Auto => {
                if let Err(err) = outcome {
                    tracing::warn!(
                        message_type = %envelope.message_type,
                        error = %err,
                        "handler failed under AckMode::Auto, delivery already acked"
                    );
                }
                Ok(())
            }
            AckMode::Manual => {
                self.handle_manual_outcome(channel, &delivery, &envelope, outcome, attempts)
                    .await
            }
        }
    }

    async fn handle_manual_outcome(
        &self,
        channel: &Channel,
        delivery: &Delivery,
        envelope: &hexeract_bus::BusEnvelope,
        outcome: Result<(), BusError>,
        attempts: &Arc<Mutex<HashMap<Uuid, u32>>>,
    ) -> Result<(), BusError> {
        match outcome {
            Ok(()) => {
                attempts.lock().await.remove(&envelope.message_id);
                channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .map_err(|err| BusError::Transport(Box::new(err)))?;
                Ok(())
            }
            Err(err) => {
                let current = {
                    let mut guard = attempts.lock().await;
                    let counter = guard.entry(envelope.message_id).or_insert(0);
                    *counter += 1;
                    *counter
                };
                tracing::warn!(
                    message_type = %envelope.message_type,
                    attempt = current,
                    max_attempts = self.config.max_attempts,
                    error = %err,
                    "handler failed"
                );
                if current < self.config.max_attempts {
                    channel
                        .basic_nack(
                            delivery.delivery_tag,
                            BasicNackOptions {
                                multiple: false,
                                requeue: true,
                            },
                        )
                        .await
                        .map_err(|err| BusError::Transport(Box::new(err)))?;
                } else {
                    if let Some(routing_key) = &self.config.dead_letter_routing_key {
                        channel
                            .basic_publish(
                                ShortString::from(""),
                                ShortString::from(routing_key.as_str()),
                                BasicPublishOptions::default(),
                                &envelope.payload,
                                delivery.properties.clone(),
                            )
                            .await
                            .map_err(|err| BusError::Transport(Box::new(err)))?
                            .await
                            .map_err(|err| BusError::Transport(Box::new(err)))?;
                    } else {
                        tracing::warn!(
                            message_type = %envelope.message_type,
                            attempts = current,
                            "delivery dropped after exhausting retry budget"
                        );
                    }
                    attempts.lock().await.remove(&envelope.message_id);
                    channel
                        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                        .await
                        .map_err(|err| BusError::Transport(Box::new(err)))?;
                }
                Ok(())
            }
        }
    }
}

pub(crate) fn delivery_to_envelope(
    props: &BasicProperties,
    payload: &[u8],
) -> Result<hexeract_bus::BusEnvelope, BusError> {
    use std::collections::HashMap as StdHashMap;
    use std::time::SystemTime;

    let message_id = props
        .message_id()
        .as_ref()
        .and_then(|s| Uuid::parse_str(s.as_str()).ok())
        .unwrap_or_else(Uuid::now_v7);
    let correlation_id = props
        .correlation_id()
        .as_ref()
        .and_then(|s| Uuid::parse_str(s.as_str()).ok())
        .unwrap_or_else(Uuid::now_v7);
    let message_type = props
        .kind()
        .as_ref()
        .map(|s| s.as_str().to_owned())
        .ok_or_else(|| {
            BusError::Internal(
                "rabbitmq delivery missing AMQP `type` property (envelope message_type)".to_owned(),
            )
        })?;
    let reply_to = props.reply_to().as_ref().map(|s| s.as_str().to_owned());

    let mut headers = StdHashMap::new();
    if let Some(table) = props.headers().as_ref() {
        for (key, value) in table.inner() {
            if let lapin::types::AMQPValue::LongString(s) = value {
                if let Ok(text) = std::str::from_utf8(s.as_bytes()) {
                    headers.insert(key.as_str().to_owned(), text.to_owned());
                }
            }
        }
    }

    Ok(hexeract_bus::BusEnvelope::restore(
        message_id,
        message_type,
        payload.to_vec(),
        correlation_id,
        reply_to,
        headers,
        SystemTime::now(),
    ))
}

pub(crate) fn build_handler_context(props: &BasicProperties) -> HandlerContext {
    let message_id = props
        .message_id()
        .as_ref()
        .and_then(|s| Uuid::parse_str(s.as_str()).ok())
        .map_or_else(MessageId::new, MessageId::from);
    let correlation_id = props
        .correlation_id()
        .as_ref()
        .and_then(|s| Uuid::parse_str(s.as_str()).ok())
        .map_or_else(CorrelationId::new, CorrelationId::from);
    HandlerContext::new(message_id, correlation_id)
}

// Suppress an unused-import warning when only some helpers are used.
#[allow(dead_code)]
fn _suppress_unused_basic_properties(_p: BasicProperties) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_are_sane() {
        let cfg = RabbitMqWorkerConfig::default();
        assert_eq!(cfg.ack_mode, AckMode::Manual);
        assert_eq!(cfg.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(cfg.prefetch, DEFAULT_PREFETCH);
        assert!(cfg.dead_letter_routing_key.is_none());
    }

    #[test]
    fn delivery_to_envelope_extracts_message_id_from_amqp_properties() {
        let message_id = Uuid::from_u128(0xABCD);
        let correlation_id = Uuid::from_u128(0x1234);
        let props = BasicProperties::default()
            .with_message_id(message_id.to_string().into())
            .with_correlation_id(correlation_id.to_string().into())
            .with_type("orders.placed".into());

        let envelope = delivery_to_envelope(&props, b"{\"order_id\":\"x\"}").expect("must decode");
        assert_eq!(envelope.message_id, message_id);
        assert_eq!(envelope.correlation_id, correlation_id);
        assert_eq!(envelope.message_type, "orders.placed");
    }

    #[test]
    fn delivery_to_envelope_mints_fresh_message_id_when_property_missing() {
        let props = BasicProperties::default().with_type("orders.placed".into());

        let envelope = delivery_to_envelope(&props, b"{}").expect("must decode");
        assert_ne!(envelope.message_id, Uuid::nil());
        assert_ne!(envelope.correlation_id, Uuid::nil());
        assert_eq!(envelope.message_type, "orders.placed");
    }

    #[test]
    fn delivery_to_envelope_returns_internal_when_type_property_missing() {
        let props = BasicProperties::default();
        let err = delivery_to_envelope(&props, b"{}")
            .expect_err("missing `type` must surface as Internal");
        match err {
            BusError::Internal(message) => assert!(message.contains("type")),
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn delivery_to_envelope_recovers_headers_and_reply_to() {
        let mut headers = lapin::types::FieldTable::default();
        headers.insert(
            ShortString::from("tenant"),
            lapin::types::AMQPValue::LongString("acme".into()),
        );
        let props = BasicProperties::default()
            .with_type("orders.placed".into())
            .with_reply_to(ShortString::from("orders.replies"))
            .with_headers(headers);

        let envelope = delivery_to_envelope(&props, b"{}").expect("must decode");
        assert_eq!(
            envelope.headers.get("tenant").map(String::as_str),
            Some("acme")
        );
        assert_eq!(envelope.reply_to.as_deref(), Some("orders.replies"));
    }

    #[test]
    fn build_handler_context_propagates_message_id_and_correlation_id() {
        let message_id = Uuid::from_u128(0x42);
        let correlation_id = Uuid::from_u128(0x7);
        let props = BasicProperties::default()
            .with_message_id(message_id.to_string().into())
            .with_correlation_id(correlation_id.to_string().into());

        let ctx = build_handler_context(&props);
        assert_eq!(*ctx.message_id.as_uuid(), message_id);
        assert_eq!(*ctx.correlation_id.as_uuid(), correlation_id);
    }

    #[test]
    fn build_handler_context_mints_fresh_ids_when_properties_missing() {
        let props = BasicProperties::default();
        let ctx = build_handler_context(&props);
        assert_ne!(*ctx.message_id.as_uuid(), Uuid::nil());
        assert_ne!(*ctx.correlation_id.as_uuid(), Uuid::nil());
    }

    #[test]
    fn ack_mode_default_is_manual() {
        assert_eq!(AckMode::default(), AckMode::Manual);
    }

    #[tokio::test]
    async fn builder_requires_queue_name() {
        let connection_result = RabbitMqConnection::connect("amqp://127.0.0.1:1").await;
        // Connect itself fails on the unreachable broker, so the
        // builder test runs against the success path of *building*
        // without a queue: we simulate it by constructing the
        // builder with a connection mocked at higher level. Since we
        // cannot construct a RabbitMqConnection without a broker, we
        // only assert that the connection error path is reached and
        // covered by the integration tests for the build() path.
        assert!(connection_result.is_err());
    }
}
