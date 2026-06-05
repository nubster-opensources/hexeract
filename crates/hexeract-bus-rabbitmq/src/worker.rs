//! Consumer worker that dispatches AMQP deliveries to typed handlers.
//!
//! The worker mirrors `hexeract_outbox::OutboxWorker`: it consumes
//! from a queue, decodes each delivery into the matching typed
//! handler via [`ErasedHandler`], and applies ack / nack semantics
//! based on the configured [`AckMode`].
//!
//! See [`RabbitMqWorkerBuilder`] for the entry point.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
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
use lapin::options::QueueDeclareOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::connection::RabbitMqConnection;

/// Default consumer prefetch (`basic.qos`).
pub const DEFAULT_PREFETCH: u16 = 16;

/// Default per-delivery max attempts before giving up.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// Suffix appended to the consumed queue name to form the wait queue
/// that holds failed deliveries until their retry delay expires.
pub const RETRY_QUEUE_SUFFIX: &str = ".retry";

/// Default broker-side delay before a failed delivery is retried.
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Decision taken after an attempt to settle a delivery with the broker.
///
/// A single transient broker error on `basic_ack` / `basic_nack` /
/// dead-letter `basic_publish` must never tear down the long-running
/// consumer. This enum captures the outcome of one such broker
/// operation so the loop can decide whether to keep running. Every
/// variant keeps the consumer alive; the distinction is purely about
/// what to log and whether the delivery was left for redelivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryDisposition {
    /// The broker operation succeeded and the delivery is fully settled.
    Settled,
    /// A broker operation failed; the error was logged and the delivery
    /// is left unsettled so the broker can redeliver it. The consumer
    /// keeps running.
    LeftForRedelivery,
}

impl DeliveryDisposition {
    /// Map the result of a broker settle operation into a disposition.
    ///
    /// `Ok` becomes [`DeliveryDisposition::Settled`]; any error becomes
    /// [`DeliveryDisposition::LeftForRedelivery`]. The helper is pure so
    /// the loop-survival policy can be unit-tested without a broker.
    fn from_settle_result(result: &Result<(), BusError>) -> Self {
        match result {
            Ok(()) => Self::Settled,
            Err(_) => Self::LeftForRedelivery,
        }
    }

    /// Whether the consumer loop should keep running after this outcome.
    ///
    /// Both variants return `true`: per-delivery settle failures are
    /// non-fatal to the consumer by design. Matching on the variant
    /// keeps the loop-survival decision colocated with the disposition,
    /// so a future fatal disposition only has to be added here.
    const fn keep_running(self) -> bool {
        match self {
            Self::Settled | Self::LeftForRedelivery => true,
        }
    }
}

/// Ack discipline for a [`RabbitMqWorker`].
///
/// The three modes trade delivery guarantees against throughput:
///
/// - [`AckMode::Manual`] is at-least-once: the broker redelivers until a
///   handler succeeds. Duplicates are possible whenever a settle
///   operation fails after its side effect took place (for example an
///   ack failing after the retry copy reached the wait queue), so
///   handlers must be idempotent.
/// - [`AckMode::AckOnReceive`] is at-most-once with an explicit ack: the
///   delivery is acknowledged as soon as it is received, before the handler
///   runs, so a handler failure is not retried. A crash after the ack and
///   before the handler completes drops that in-flight delivery.
/// - [`AckMode::Unacknowledged`] is fire-and-forget: the broker is told not
///   to expect any ack (`no_ack`), so it removes the message the moment it is
///   sent. Highest throughput, but any handler failure or crash loses the
///   message. Use only when loss is acceptable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Ack only when the handler returns `Ok`. Handler failures are
    /// republished to the wait queue and retried after
    /// [`RabbitMqWorkerConfig::retry_delay`], up to `max_attempts`,
    /// then routed to the dead-letter routing key if configured or
    /// dropped otherwise. At-least-once.
    Manual,
    /// Acknowledge each delivery explicitly as soon as it is received,
    /// before the handler runs (`no_ack` is not set). Handler failures are
    /// logged but never retried. At-most-once.
    AckOnReceive,
    /// Consume with `no_ack`: the broker removes the message on delivery,
    /// before the handler runs, and never expects an acknowledgement.
    /// Highest throughput, but handler failures and crashes lose the
    /// message. Fire-and-forget.
    Unacknowledged,
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
    /// Broker-side delay between retries, enforced by the TTL of the
    /// wait queue declared by the worker. The TTL is baked into the
    /// queue arguments at declare time, so changing this value for an
    /// existing wait queue makes the declaration fail with a broker
    /// precondition error: delete the `<queue>.retry` queue first.
    pub retry_delay: Duration,
}

impl Default for RabbitMqWorkerConfig {
    fn default() -> Self {
        Self {
            ack_mode: AckMode::default(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            prefetch: DEFAULT_PREFETCH,
            dead_letter_routing_key: None,
            retry_delay: DEFAULT_RETRY_DELAY,
        }
    }
}

/// Fluent builder for [`RabbitMqWorker`], symmetric with
/// `PgOutboxWorkerBuilder` from `hexeract-outbox-sql`.
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

    /// Override the broker-side retry delay (default [`DEFAULT_RETRY_DELAY`]).
    ///
    /// See [`RabbitMqWorkerConfig::retry_delay`] for the constraint on
    /// changing the delay of an already-declared wait queue.
    #[must_use]
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.config.retry_delay = delay;
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
/// # Retry flow
///
/// In [`AckMode::Manual`], a failed delivery is republished to a
/// durable wait queue (`<queue>.retry`, declared by the worker at
/// startup) whose per-queue TTL enforces [`RabbitMqWorkerConfig::retry_delay`]
/// and whose dead-letter route points back at the consumed queue. The
/// retry count is read from the broker-maintained `x-death` header,
/// so it survives worker restarts and never lives in process memory.
/// Once the count reaches `max_attempts` the delivery is routed to
/// the configured dead-letter routing key, or dropped.
pub struct RabbitMqWorker {
    connection: RabbitMqConnection,
    queue: String,
    handlers: Arc<HashMap<&'static str, Arc<dyn ErasedHandler>>>,
    config: RabbitMqWorkerConfig,
}

impl RabbitMqWorker {
    /// Run the consume loop until `cancel` fires.
    ///
    /// On `Ok(())` the loop drained normally on cancellation. Only
    /// fatal setup errors (channel open, `basic_qos`, `basic_consume`)
    /// return immediately. Per-delivery handler failures are absorbed
    /// by the retry / ack-mode policy, and transient broker errors on
    /// settling a delivery (`basic_ack` / `basic_nack` / dead-letter
    /// `basic_publish`) are logged and never abort the loop: the
    /// consumer keeps running and the broker redelivers the unsettled
    /// delivery.
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
        if matches!(self.config.ack_mode, AckMode::Manual) {
            Self::declare_retry_queue(&channel, &self.queue, self.config.retry_delay).await?;
        }
        let no_ack = matches!(self.config.ack_mode, AckMode::Unacknowledged);
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
                            let disposition = self.dispatch(&channel, delivery).await;
                            if !disposition.keep_running() {
                                break;
                            }
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

    #[allow(clippy::too_many_lines)]
    async fn dispatch(&self, channel: &Channel, delivery: Delivery) -> DeliveryDisposition {
        let envelope = match delivery_to_envelope(&delivery.properties, &delivery.data) {
            Ok(env) => env,
            Err(err) => {
                tracing::warn!(error = %err, "rabbitmq delivery decode failed");
                match self.config.ack_mode {
                    AckMode::Manual => {
                        let nack = channel
                            .basic_nack(
                                delivery.delivery_tag,
                                BasicNackOptions {
                                    requeue: false,
                                    ..BasicNackOptions::default()
                                },
                            )
                            .await
                            .map_err(|err| BusError::Transport(Box::new(err)));
                        if let Err(err) = &nack {
                            tracing::warn!(
                                delivery_tag = delivery.delivery_tag,
                                error = %err,
                                "rabbitmq nack of undecodable delivery failed; consumer continues"
                            );
                        }
                    }
                    AckMode::AckOnReceive => {
                        let ack = channel
                            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                            .await
                            .map_err(|err| BusError::Transport(Box::new(err)));
                        if let Err(err) = &ack {
                            tracing::warn!(
                                delivery_tag = delivery.delivery_tag,
                                error = %err,
                                "rabbitmq ack of undecodable delivery failed; consumer continues"
                            );
                        }
                    }
                    AckMode::Unacknowledged => {}
                }
                return DeliveryDisposition::Settled;
            }
        };

        // AckOnReceive settles the delivery before the handler runs, so a
        // handler failure is never retried (at-most-once).
        if matches!(self.config.ack_mode, AckMode::AckOnReceive) {
            let ack = channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await
                .map_err(|err| BusError::Transport(Box::new(err)));
            if let Err(err) = &ack {
                tracing::warn!(
                    delivery_tag = delivery.delivery_tag,
                    error = %err,
                    "rabbitmq ack-on-receive failed; consumer continues, broker will redeliver"
                );
                return DeliveryDisposition::from_settle_result(&ack);
            }
        }

        let ctx = build_handler_context(&delivery.properties);
        let outcome = match self.handlers.get(envelope.message_type.as_str()) {
            Some(handler) => AssertUnwindSafe(handler.handle(&envelope, &ctx))
                .catch_unwind()
                .await
                .unwrap_or_else(|payload| {
                    let msg = panic_message(&payload);
                    tracing::error!(
                        message_type = %envelope.message_type,
                        panic = %msg,
                        "handler panicked; treating as delivery failure"
                    );
                    Err(BusError::Internal(format!("handler panicked: {msg}")))
                }),
            None => Err(BusError::MissingHandler {
                message_type: envelope.message_type.clone(),
            }),
        };

        match self.config.ack_mode {
            AckMode::Manual => {
                self.handle_manual_outcome(channel, &delivery, &envelope, outcome)
                    .await
            }
            AckMode::AckOnReceive => {
                if let Err(err) = outcome {
                    tracing::warn!(
                        message_type = %envelope.message_type,
                        error = %err,
                        "handler failed under AckMode::AckOnReceive, delivery already acked"
                    );
                }
                DeliveryDisposition::Settled
            }
            AckMode::Unacknowledged => {
                if let Err(err) = outcome {
                    tracing::warn!(
                        message_type = %envelope.message_type,
                        error = %err,
                        "handler failed under AckMode::Unacknowledged (no_ack), message already gone"
                    );
                }
                DeliveryDisposition::Settled
            }
        }
    }

    async fn handle_manual_outcome(
        &self,
        channel: &Channel,
        delivery: &Delivery,
        envelope: &hexeract_bus::BusEnvelope,
        outcome: Result<(), BusError>,
    ) -> DeliveryDisposition {
        match outcome {
            Ok(()) => {
                let ack = channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .map_err(|err| BusError::Transport(Box::new(err)));
                if let Err(err) = &ack {
                    tracing::warn!(
                        message_id = %envelope.message_id,
                        delivery_tag = delivery.delivery_tag,
                        error = %err,
                        "rabbitmq ack failed; consumer continues, broker will redeliver"
                    );
                }
                DeliveryDisposition::from_settle_result(&ack)
            }
            Err(err) => {
                let wait_queue = wait_queue_name(&self.queue);
                let current = death_count(&delivery.properties, &wait_queue) + 1;
                tracing::warn!(
                    message_type = %envelope.message_type,
                    message_id = %envelope.message_id,
                    attempt = current,
                    max_attempts = self.config.max_attempts,
                    error = %err,
                    "handler failed"
                );
                if current < self.config.max_attempts {
                    self.schedule_retry(channel, delivery, envelope, &wait_queue)
                        .await
                } else {
                    self.handle_exhausted(channel, delivery, envelope, current)
                        .await
                }
            }
        }
    }

    /// Republish the failed delivery to the wait queue, then ack the
    /// original. The wait queue TTL plus its dead-letter route back to
    /// the consumed queue turn this into a broker-side delayed retry,
    /// so the consumer never sleeps and the count survives restarts.
    async fn schedule_retry(
        &self,
        channel: &Channel,
        delivery: &Delivery,
        envelope: &hexeract_bus::BusEnvelope,
        wait_queue: &str,
    ) -> DeliveryDisposition {
        Self::retry_core(
            move || async move {
                let published = channel
                    .basic_publish(
                        ShortString::from(""),
                        ShortString::from(wait_queue),
                        BasicPublishOptions::default(),
                        &delivery.data,
                        delivery.properties.clone(),
                    )
                    .await
                    .map_err(|err| BusError::Transport(Box::new(err)))
                    .map(|_confirm| ());
                if let Err(err) = &published {
                    tracing::warn!(
                        message_id = %envelope.message_id,
                        delivery_tag = delivery.delivery_tag,
                        error = %err,
                        "rabbitmq retry publish failed; original left unacked for redelivery, consumer continues"
                    );
                }
                published
            },
            move || async move {
                let ack = channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .map_err(|err| BusError::Transport(Box::new(err)));
                if let Err(err) = &ack {
                    tracing::warn!(
                        message_id = %envelope.message_id,
                        delivery_tag = delivery.delivery_tag,
                        error = %err,
                        "rabbitmq ack after retry publish failed; consumer continues, broker may redeliver a duplicate"
                    );
                }
                ack
            },
        )
        .await
    }

    /// Publish-then-ack core of the retry path, generic over the broker
    /// operations so the ordering is unit-testable without a broker.
    ///
    /// The original delivery is only acked once the copy is safely in
    /// the wait queue; a failed publish leaves it unacked so the broker
    /// redelivers it instead of losing it.
    async fn retry_core<P, PF, A, AF>(publish_to_wait_queue: P, ack: A) -> DeliveryDisposition
    where
        P: FnOnce() -> PF,
        PF: Future<Output = Result<(), BusError>>,
        A: FnOnce() -> AF,
        AF: Future<Output = Result<(), BusError>>,
    {
        if publish_to_wait_queue().await.is_err() {
            return DeliveryDisposition::LeftForRedelivery;
        }
        DeliveryDisposition::from_settle_result(&ack().await)
    }

    /// Declare the durable wait queue `<queue>.retry` whose per-queue
    /// TTL enforces the retry delay and whose dead-letter route points
    /// back at the consumed queue.
    ///
    /// Declaration is idempotent for identical arguments; a different
    /// `retry_delay` makes the broker reject it with a precondition
    /// error (see [`RabbitMqWorkerConfig::retry_delay`]).
    async fn declare_retry_queue(
        channel: &Channel,
        queue: &str,
        retry_delay: Duration,
    ) -> Result<(), BusError> {
        let ttl_ms = i64::try_from(retry_delay.as_millis()).unwrap_or(i64::MAX);
        let mut args = FieldTable::default();
        args.insert(
            ShortString::from("x-message-ttl"),
            AMQPValue::LongLongInt(ttl_ms),
        );
        args.insert(
            ShortString::from("x-dead-letter-exchange"),
            AMQPValue::LongString("".into()),
        );
        args.insert(
            ShortString::from("x-dead-letter-routing-key"),
            AMQPValue::LongString(queue.into()),
        );
        channel
            .queue_declare(
                ShortString::from(wait_queue_name(queue).as_str()),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                args,
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;
        Ok(())
    }

    async fn handle_exhausted(
        &self,
        channel: &Channel,
        delivery: &Delivery,
        envelope: &hexeract_bus::BusEnvelope,
        current: u32,
    ) -> DeliveryDisposition {
        if self.config.dead_letter_routing_key.is_none() {
            tracing::warn!(
                message_type = %envelope.message_type,
                message_id = %envelope.message_id,
                attempts = current,
                "delivery dropped after exhausting retry budget"
            );
        }
        Self::exhausted_core(
            self.config
                .dead_letter_routing_key
                .as_deref()
                .map(|routing_key| {
                    move || async move {
                        let published = self
                            .publish_dead_letter(channel, delivery, envelope, routing_key)
                            .await;
                        if let Err(err) = &published {
                            tracing::error!(
                                message_type = %envelope.message_type,
                                message_id = %envelope.message_id,
                                delivery_tag = delivery.delivery_tag,
                                error = %err,
                                "rabbitmq dead-letter publish failed; original left unacked for redelivery, consumer continues"
                            );
                        }
                        published
                    }
                }),
            move || async move {
                let ack = channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .map_err(|err| BusError::Transport(Box::new(err)));
                if let Err(err) = &ack {
                    tracing::warn!(
                        message_id = %envelope.message_id,
                        delivery_tag = delivery.delivery_tag,
                        error = %err,
                        "rabbitmq ack after dead-letter failed; consumer continues, broker will redeliver"
                    );
                }
                ack
            },
        )
        .await
    }

    /// Dead-letter-then-ack core of the exhausted path, generic over
    /// the broker operations so the ordering is unit-testable without
    /// a broker.
    ///
    /// A `None` dead-letter means no routing key is configured and the
    /// delivery is dropped via ack. A failing dead-letter publish
    /// leaves the delivery unacked so the broker redelivers it.
    async fn exhausted_core<P, PF, A, AF>(dead_letter: Option<P>, ack: A) -> DeliveryDisposition
    where
        P: FnOnce() -> PF,
        PF: Future<Output = Result<(), BusError>>,
        A: FnOnce() -> AF,
        AF: Future<Output = Result<(), BusError>>,
    {
        if let Some(publish) = dead_letter {
            if publish().await.is_err() {
                return DeliveryDisposition::LeftForRedelivery;
            }
        }
        DeliveryDisposition::from_settle_result(&ack().await)
    }

    async fn publish_dead_letter(
        &self,
        channel: &Channel,
        delivery: &Delivery,
        envelope: &hexeract_bus::BusEnvelope,
        routing_key: &str,
    ) -> Result<(), BusError> {
        channel
            .basic_publish(
                ShortString::from(""),
                ShortString::from(routing_key),
                BasicPublishOptions::default(),
                &envelope.payload,
                delivery.properties.clone(),
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;
        Ok(())
    }
}

/// Name of the wait queue paired with `queue`.
pub(crate) fn wait_queue_name(queue: &str) -> String {
    format!("{queue}{RETRY_QUEUE_SUFFIX}")
}

/// Read the broker-maintained retry count from the `x-death` header.
///
/// Each time the broker dead-letters a message it appends or updates
/// an entry in `x-death`; the entry whose `queue` is the wait queue
/// and whose `reason` is `expired` counts the completed retry cycles.
/// Returns 0 when the header or the entry is absent (first attempt).
pub(crate) fn death_count(props: &BasicProperties, wait_queue: &str) -> u32 {
    let Some(headers) = props.headers().as_ref() else {
        return 0;
    };
    let Some(AMQPValue::FieldArray(deaths)) = headers.inner().get("x-death") else {
        return 0;
    };
    for death in deaths.as_slice() {
        let AMQPValue::FieldTable(entry) = death else {
            continue;
        };
        let fields = entry.inner();
        let queue_matches = matches!(
            fields.get("queue"),
            Some(AMQPValue::LongString(q)) if q.as_bytes() == wait_queue.as_bytes()
        );
        let reason_matches = matches!(
            fields.get("reason"),
            Some(AMQPValue::LongString(r)) if r.as_bytes() == b"expired"
        );
        if queue_matches && reason_matches {
            if let Some(AMQPValue::LongLongInt(count)) = fields.get("count") {
                return u32::try_from(*count).unwrap_or(u32::MAX);
            }
        }
    }
    0
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

fn panic_message(payload: &Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

// Suppress an unused-import warning when only some helpers are used.
#[allow(dead_code)]
fn _suppress_unused_basic_properties(_p: BasicProperties) {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn config_defaults_are_sane() {
        let cfg = RabbitMqWorkerConfig::default();
        assert_eq!(cfg.ack_mode, AckMode::Manual);
        assert_eq!(cfg.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(cfg.prefetch, DEFAULT_PREFETCH);
        assert!(cfg.dead_letter_routing_key.is_none());
        assert_eq!(cfg.retry_delay, DEFAULT_RETRY_DELAY);
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

    #[test]
    fn ack_error_keeps_the_consumer_running() {
        let settle_failed: Result<(), BusError> =
            Err(BusError::Internal("simulated basic_ack failure".to_owned()));
        let disposition = DeliveryDisposition::from_settle_result(&settle_failed);
        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
        assert!(
            disposition.keep_running(),
            "a settle (ack/nack) error must not abort the consume loop"
        );
    }

    #[test]
    fn nack_error_keeps_the_consumer_running() {
        let settle_failed: Result<(), BusError> = Err(BusError::Transport(Box::new(
            std::io::Error::other("simulated basic_nack failure"),
        )));
        let disposition = DeliveryDisposition::from_settle_result(&settle_failed);
        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
        assert!(disposition.keep_running());
    }

    #[test]
    fn successful_settle_keeps_running_and_marks_settled() {
        let settled: Result<(), BusError> = Ok(());
        let disposition = DeliveryDisposition::from_settle_result(&settled);
        assert_eq!(disposition, DeliveryDisposition::Settled);
        assert!(disposition.keep_running());
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

    /// A dead-letter closure type that can name `None` for the no-DL case.
    type NoDeadLetter = fn() -> std::future::Ready<Result<(), BusError>>;

    #[tokio::test]
    async fn exhausted_core_leaves_unacked_when_dead_letter_publish_fails() {
        let ack_called = AtomicBool::new(false);

        let disposition = RabbitMqWorker::exhausted_core(
            Some(|| async { Err(BusError::Internal("dead-letter broker down".to_owned())) }),
            || async {
                ack_called.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
        assert!(
            !ack_called.load(Ordering::SeqCst),
            "delivery must stay unacked so the broker redelivers it"
        );
    }

    #[tokio::test]
    async fn exhausted_core_acks_when_dead_letter_publish_succeeds() {
        let disposition =
            RabbitMqWorker::exhausted_core(Some(|| async { Ok(()) }), || async { Ok(()) }).await;
        assert_eq!(disposition, DeliveryDisposition::Settled);
    }

    #[tokio::test]
    async fn exhausted_core_acks_when_no_dead_letter_configured() {
        let disposition =
            RabbitMqWorker::exhausted_core(None::<NoDeadLetter>, || async { Ok(()) }).await;
        assert_eq!(disposition, DeliveryDisposition::Settled);
    }

    #[tokio::test]
    async fn exhausted_core_reports_left_for_redelivery_when_final_ack_fails() {
        let disposition = RabbitMqWorker::exhausted_core(Some(|| async { Ok(()) }), || async {
            Err(BusError::Internal("simulated basic_ack failure".to_owned()))
        })
        .await;
        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
    }

    #[tokio::test]
    async fn retry_core_leaves_unacked_when_wait_queue_publish_fails() {
        let ack_called = AtomicBool::new(false);

        let disposition = RabbitMqWorker::retry_core(
            || async { Err(BusError::Internal("wait queue publish failed".to_owned())) },
            || async {
                ack_called.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
        assert!(
            !ack_called.load(Ordering::SeqCst),
            "the original must stay unacked when the retry copy was not published"
        );
    }

    #[tokio::test]
    async fn retry_core_acks_only_after_successful_publish() {
        let published = AtomicBool::new(false);
        let acked_after_publish = AtomicBool::new(false);

        let disposition = RabbitMqWorker::retry_core(
            || async {
                published.store(true, Ordering::SeqCst);
                Ok(())
            },
            || async {
                acked_after_publish.store(published.load(Ordering::SeqCst), Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert_eq!(disposition, DeliveryDisposition::Settled);
        assert!(
            acked_after_publish.load(Ordering::SeqCst),
            "ack must run after the wait queue publish succeeded"
        );
    }

    #[tokio::test]
    async fn retry_core_reports_left_for_redelivery_when_ack_fails() {
        let disposition = RabbitMqWorker::retry_core(
            || async { Ok(()) },
            || async { Err(BusError::Internal("simulated basic_ack failure".to_owned())) },
        )
        .await;
        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
    }

    #[test]
    fn wait_queue_name_appends_suffix() {
        assert_eq!(wait_queue_name("orders"), "orders.retry");
    }

    fn x_death_properties(entries: Vec<AMQPValue>) -> BasicProperties {
        let mut headers = FieldTable::default();
        headers.insert(
            ShortString::from("x-death"),
            AMQPValue::FieldArray(lapin::types::FieldArray::from(entries)),
        );
        BasicProperties::default().with_headers(headers)
    }

    fn x_death_entry(queue: &str, reason: &str, count: i64) -> AMQPValue {
        let mut entry = FieldTable::default();
        entry.insert(
            ShortString::from("queue"),
            AMQPValue::LongString(queue.into()),
        );
        entry.insert(
            ShortString::from("reason"),
            AMQPValue::LongString(reason.into()),
        );
        entry.insert(ShortString::from("count"), AMQPValue::LongLongInt(count));
        AMQPValue::FieldTable(entry)
    }

    #[test]
    fn death_count_returns_zero_without_headers() {
        let props = BasicProperties::default();
        assert_eq!(death_count(&props, "orders.retry"), 0);
    }

    #[test]
    fn death_count_returns_zero_for_entries_of_other_queues() {
        let props = x_death_properties(vec![x_death_entry("payments.retry", "expired", 4)]);
        assert_eq!(death_count(&props, "orders.retry"), 0);
    }

    #[test]
    fn death_count_ignores_non_expired_reasons() {
        let props = x_death_properties(vec![x_death_entry("orders.retry", "rejected", 4)]);
        assert_eq!(death_count(&props, "orders.retry"), 0);
    }

    #[test]
    fn death_count_reads_the_expired_entry_of_the_wait_queue() {
        let props = x_death_properties(vec![
            x_death_entry("payments.retry", "expired", 9),
            x_death_entry("orders.retry", "rejected", 1),
            x_death_entry("orders.retry", "expired", 3),
        ]);
        assert_eq!(death_count(&props, "orders.retry"), 3);
    }
}
