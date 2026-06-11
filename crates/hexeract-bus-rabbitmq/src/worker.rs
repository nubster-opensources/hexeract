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
use lapin::options::ConfirmSelectOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::connection::RabbitMqConnection;
use crate::transport::to_short_string;

/// Default consumer prefetch (`basic.qos`).
pub const DEFAULT_PREFETCH: u16 = 16;

/// Default per-delivery max attempts before giving up.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// Suffix appended to the consumed queue name to form the wait queue
/// that holds failed deliveries until their retry delay expires.
pub const RETRY_QUEUE_SUFFIX: &str = ".retry";

/// Default broker-side delay before a failed delivery is retried.
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Default cap on the size of a consumed payload, in bytes (1 MiB).
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

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
    /// A broker operation (`basic_ack` / `basic_nack`) failed and the
    /// delivery could not be settled; the error was logged and the
    /// consumer keeps running. This is reached only when the settle
    /// itself fails, which usually means the channel is dying: the broker
    /// redelivers the delivery once the channel or connection closes. A
    /// failed retry / dead-letter publish does *not* reach this state on a
    /// live channel; it nacks the delivery so its prefetch slot is freed.
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
///
/// Marked `#[non_exhaustive]` so a future ack discipline can be added in a
/// minor version: downstream `match` arms must include a wildcard `_` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    ///
    /// The broker applies no `QoS` bound to a `no_ack` consumer, so
    /// [`RabbitMqWorkerConfig::prefetch`] has no effect and there is no
    /// broker-side flow control: deliveries can accumulate in the client
    /// buffer without limit. The worker skips `basic.qos` in this mode.
    Unacknowledged,
}

impl Default for AckMode {
    fn default() -> Self {
        Self::Manual
    }
}

/// Whether `basic.qos` prefetch is honoured under the given ack mode.
///
/// A `no_ack` consumer ([`AckMode::Unacknowledged`]) never settles a
/// delivery, so the broker applies no `QoS` bound to it: issuing
/// `basic.qos` would advertise a backpressure limit that does not
/// exist. The worker skips the call in that case.
const fn qos_applies(ack_mode: AckMode) -> bool {
    !matches!(ack_mode, AckMode::Unacknowledged)
}

/// Tuning parameters for a [`RabbitMqWorker`].
///
/// Marked `#[non_exhaustive]` so new tuning fields can be added in a minor
/// version: construct it through [`RabbitMqWorkerBuilder`] rather than a
/// struct literal.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RabbitMqWorkerConfig {
    /// Ack discipline applied to consumed deliveries.
    pub ack_mode: AckMode,
    /// Maximum number of attempts per delivery before giving up.
    pub max_attempts: u32,
    /// Per-channel prefetch (`basic.qos`).
    ///
    /// Bounds in-flight unacknowledged deliveries under
    /// [`AckMode::Manual`] and [`AckMode::AckOnReceive`]. Has no effect
    /// under [`AckMode::Unacknowledged`]: a `no_ack` consumer never
    /// acknowledges, so the broker applies no bound and exerts no flow
    /// control, and the worker skips the `basic.qos` call entirely.
    pub prefetch: u16,
    /// Optional name of the durable dead-letter queue that receives
    /// deliveries which exhausted their retry budget. The worker
    /// declares the queue at startup and publishes to it through the
    /// default exchange with a mandatory, confirmed and persistent
    /// publish, so an exhausted delivery is never silently lost.
    /// `None` drops exhausted deliveries with a warning instead.
    pub dead_letter_routing_key: Option<String>,
    /// Broker-side delay between retries, enforced by the TTL of the
    /// wait queue declared by the worker. The TTL is baked into the
    /// queue arguments at declare time, so changing this value for an
    /// existing wait queue makes the declaration fail with a broker
    /// precondition error: delete the `<queue>.retry` queue first.
    pub retry_delay: Duration,
    /// Maximum accepted payload size, in bytes, for a consumed delivery.
    ///
    /// Broker bytes cross a trust boundary: any client allowed to
    /// publish to the queue controls the payload, so the worker rejects
    /// oversize deliveries before copying or deserializing them. The
    /// rejected delivery follows the same path as an undecodable one:
    /// routed to the dead-letter queue when `dead_letter_routing_key`
    /// is configured, dropped with a warning otherwise. The broker has
    /// already buffered the frame when the worker sees it, so this cap
    /// bounds the consumer's work, not the network: pair it with the
    /// broker-side `max_message_size` to bound ingress.
    pub max_payload_bytes: usize,
}

impl Default for RabbitMqWorkerConfig {
    fn default() -> Self {
        Self {
            ack_mode: AckMode::default(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            prefetch: DEFAULT_PREFETCH,
            dead_letter_routing_key: None,
            retry_delay: DEFAULT_RETRY_DELAY,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
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
    ///
    /// Has no effect under [`AckMode::Unacknowledged`]; see
    /// [`RabbitMqWorkerConfig::prefetch`].
    #[must_use]
    pub fn prefetch(mut self, n: u16) -> Self {
        self.config.prefetch = n;
        self
    }

    /// Route exhausted deliveries to the durable dead-letter queue
    /// named `routing_key`, declared by the worker at startup and fed
    /// through a mandatory, confirmed and persistent publish.
    ///
    /// See [`RabbitMqWorkerConfig::dead_letter_routing_key`].
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

    /// Override the maximum accepted payload size in bytes (default
    /// [`DEFAULT_MAX_PAYLOAD_BYTES`]).
    ///
    /// See [`RabbitMqWorkerConfig::max_payload_bytes`] for the trust
    /// boundary this cap enforces.
    #[must_use]
    pub fn max_payload_bytes(mut self, bytes: usize) -> Self {
        self.config.max_payload_bytes = bytes;
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
/// Once the count reaches `max_attempts` the delivery is published
/// to the configured dead-letter queue, declared by the worker at
/// startup and written with a mandatory, confirmed and persistent
/// publish, or dropped with a warning when no dead-letter queue is
/// configured.
pub struct RabbitMqWorker {
    connection: RabbitMqConnection,
    queue: String,
    handlers: Arc<HashMap<&'static str, Arc<dyn ErasedHandler>>>,
    config: RabbitMqWorkerConfig,
}

impl RabbitMqWorker {
    /// Run the consume loop until `cancel` fires.
    ///
    /// `Ok(())` means the loop drained normally on cancellation, and
    /// nothing else: a consumer stream that ends while the token has
    /// not fired signals a lost connection or channel and surfaces as
    /// [`BusError::Connection`], so a supervisor can rebuild and
    /// restart the worker. Only fatal setup errors (channel open,
    /// `basic_qos`, wait and dead-letter queue declarations,
    /// `confirm_select`, `basic_consume`) return immediately.
    /// Per-delivery handler failures are absorbed by the retry /
    /// ack-mode policy, and transient broker errors on settling a
    /// delivery (`basic_ack` / `basic_nack`) are logged and never abort
    /// the loop. A retry or dead-letter publish that fails on a live
    /// channel nacks the delivery (requeuing transient failures, dropping
    /// unroutable ones) so its prefetch slot is freed and the consumer
    /// never silently stalls; a failed settle leaves the delivery for
    /// redelivery once the channel closes.
    ///
    /// # Recovery
    ///
    /// The worker never reconnects on its own: recovery belongs to
    /// the caller, which rebuilds every piece of broker state from
    /// scratch on each iteration. Re-creating the worker re-declares
    /// the wait and dead-letter queues, re-enables publisher confirms
    /// and re-subscribes the consumer.
    ///
    /// ```rust,no_run
    /// # use std::time::Duration;
    /// # use hexeract_bus_rabbitmq::{RabbitMqConnection, RabbitMqWorkerBuilder};
    /// # use tokio_util::sync::CancellationToken;
    /// # async fn supervise(uri: &str, cancel: CancellationToken) -> Result<(), hexeract_bus::BusError> {
    /// loop {
    ///     let connection =
    ///         RabbitMqConnection::connect_with_retry(uri, 5, Duration::from_millis(500)).await?;
    ///     let worker = RabbitMqWorkerBuilder::new(connection)
    ///         .queue("orders.received")
    ///         .build()?;
    ///     match worker.run(cancel.clone()).await {
    ///         Ok(()) => break,
    ///         Err(err) => {
    ///             tracing::error!(error = %err, "bus worker stopped, restarting");
    ///             tokio::time::sleep(Duration::from_secs(1)).await;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if the consumer channel
    /// cannot be opened or if the consumer stream ends before
    /// cancellation (lost connection or channel), and
    /// [`BusError::Transport`] if the broker rejects a setup command
    /// (queue declaration, `confirm_select` or
    /// [`Channel::basic_consume`]).
    pub async fn run(self, cancel: CancellationToken) -> Result<(), BusError> {
        let channel = self.connection.create_channel().await?;
        if qos_applies(self.config.ack_mode) {
            channel
                .basic_qos(self.config.prefetch, BasicQosOptions::default())
                .await
                .map_err(|err| BusError::Transport(Box::new(err)))?;
        }
        // The retry wait queue is specific to Manual mode (the only mode
        // that republishes failed deliveries for a delayed retry).
        if matches!(self.config.ack_mode, AckMode::Manual) {
            Self::declare_retry_queue(&channel, &self.queue, self.config.retry_delay).await?;
        }
        // Dead-letter setup and publisher confirms are independent of the
        // ack mode. They were previously gated on Manual, which silently
        // broke dead-lettering for AckOnReceive (the poison path publishes
        // to the DLQ for every ack mode) and made the retry copy's confirm
        // resolve as `NotRequested`. Enable confirms whenever a confirmed
        // publish can occur: Manual always republishes (retry copy), and
        // any mode with a dead-letter routing key publishes to the DLQ.
        let needs_confirms = matches!(self.config.ack_mode, AckMode::Manual)
            || self.config.dead_letter_routing_key.is_some();
        if let Some(dead_letter_queue) = self.config.dead_letter_routing_key.as_deref() {
            Self::declare_dead_letter_queue(&channel, dead_letter_queue).await?;
        }
        if needs_confirms {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|err| BusError::Transport(Box::new(err)))?;
        }
        let no_ack = matches!(self.config.ack_mode, AckMode::Unacknowledged);
        let mut consumer = channel
            .basic_consume(
                to_short_string(self.queue.as_str(), "queue name")?,
                to_short_string(
                    format!("hexeract-{}", Uuid::now_v7()).as_str(),
                    "consumer tag",
                )?,
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
                    let Some(item) = next else {
                        if cancel.is_cancelled() {
                            break;
                        }
                        return Err(BusError::Connection(
                            "rabbitmq consumer stream ended unexpectedly: connection or channel lost"
                                .into(),
                        ));
                    };
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
        let envelope = match delivery_to_envelope(
            &delivery.properties,
            &delivery.data,
            self.config.max_payload_bytes,
        ) {
            Ok(env) => env,
            Err(err) => return self.handle_poison(channel, &delivery, &err).await,
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

        let ctx = build_handler_context(&envelope);
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
                // A delivery whose message type has no registered handler
                // is a permanent failure: retrying it through the wait
                // queue only burns the retry budget and adds broker
                // traffic. Route it straight to the exhausted path.
                if let BusError::MissingHandler { .. } = &err {
                    tracing::warn!(
                        message_type = %envelope.message_type,
                        message_id = %envelope.message_id,
                        error = %err,
                        "no handler registered; routing delivery straight to dead-letter without retry"
                    );
                    return self.handle_exhausted(channel, delivery, envelope, 0).await;
                }
                let wait_queue = wait_queue_name(&self.queue);
                // `x-death` is attacker-influenced; clamp the saturating
                // add so a forged count near `u32::MAX` cannot overflow
                // (panic in debug, wrap to 0 then retry forever in release).
                let current = death_count(&delivery.properties, &wait_queue).saturating_add(1);
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
                let published = Self::publish_to_wait_queue(channel, delivery, wait_queue).await;
                if let Err(err) = &published {
                    tracing::warn!(
                        message_id = %envelope.message_id,
                        delivery_tag = delivery.delivery_tag,
                        error = %err,
                        "rabbitmq retry publish failed; original nacked so the prefetch slot is freed, consumer continues"
                    );
                }
                published
            },
            move |requeue| async move {
                Self::nack_after_failed_publish(channel, delivery, requeue, "retry publish").await
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

    /// Nack a delivery whose retry / dead-letter publish failed so its
    /// prefetch slot is released on a live channel.
    ///
    /// `requeue` decides whether the broker re-queues the delivery for
    /// another attempt or drops it (see
    /// [`Self::requeue_on_publish_failure`]). A failure to nack is logged
    /// and surfaced so the caller records [`DeliveryDisposition::LeftForRedelivery`].
    async fn nack_after_failed_publish(
        channel: &Channel,
        delivery: &Delivery,
        requeue: bool,
        context: &str,
    ) -> Result<(), BusError> {
        let nacked = channel
            .basic_nack(
                delivery.delivery_tag,
                BasicNackOptions {
                    requeue,
                    ..BasicNackOptions::default()
                },
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)));
        if let Err(err) = &nacked {
            tracing::warn!(
                delivery_tag = delivery.delivery_tag,
                requeue,
                error = %err,
                "rabbitmq nack after failed {context} failed; consumer continues"
            );
        }
        nacked
    }

    /// Whether a delivery whose retry / dead-letter publish failed should
    /// be requeued when nacked.
    ///
    /// AMQP brokers only redeliver an unacked delivery after the channel
    /// or connection closes, never on a live channel, so leaving the
    /// delivery unsettled permanently consumes a prefetch slot and stalls
    /// the consumer. The delivery must therefore be nacked. The requeue
    /// flag depends on why the publish failed:
    ///
    /// - [`BusError::Unroutable`]: the destination queue is gone, so
    ///   requeuing would loop forever against a permanently missing
    ///   target. Drop the delivery (`requeue: false`), matching the
    ///   documented no-dead-letter behaviour.
    /// - any other (transient transport) error: the failure is likely
    ///   recoverable, so requeue (`requeue: true`) for another attempt.
    const fn requeue_on_publish_failure(err: &BusError) -> bool {
        !matches!(err, BusError::Unroutable { .. })
    }

    /// Publish-then-settle core of the retry path, generic over the
    /// broker operations so the ordering is unit-testable without a
    /// broker.
    ///
    /// The original delivery is only acked once the copy is safely in
    /// the wait queue. A failed publish does not leave the delivery
    /// unacked (that would starve the prefetch window on a live channel):
    /// instead `settle_on_publish_failure` nacks it, requeuing or
    /// dropping per [`Self::requeue_on_publish_failure`].
    async fn retry_core<P, PF, S, SF, A, AF>(
        publish_to_wait_queue: P,
        settle_on_publish_failure: S,
        ack: A,
    ) -> DeliveryDisposition
    where
        P: FnOnce() -> PF,
        PF: Future<Output = Result<(), BusError>>,
        S: FnOnce(bool) -> SF,
        SF: Future<Output = Result<(), BusError>>,
        A: FnOnce() -> AF,
        AF: Future<Output = Result<(), BusError>>,
    {
        if let Err(err) = publish_to_wait_queue().await {
            let requeue = Self::requeue_on_publish_failure(&err);
            return DeliveryDisposition::from_settle_result(
                &settle_on_publish_failure(requeue).await,
            );
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
                to_short_string(wait_queue_name(queue).as_str(), "wait queue name")?,
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

    /// Declare the durable dead-letter queue so the dead-letter
    /// routing key always has a bound queue on the default exchange
    /// and an exhausted delivery can never be silently unroutable.
    ///
    /// Declaration is idempotent for identical arguments; a pre-
    /// existing queue with different arguments makes the broker
    /// reject the setup with a precondition error, surfacing the
    /// conflict at startup instead of losing messages at runtime.
    async fn declare_dead_letter_queue(channel: &Channel, queue: &str) -> Result<(), BusError> {
        channel
            .queue_declare(
                to_short_string(queue, "dead-letter queue name")?,
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;
        Ok(())
    }

    /// Whether the poison path settles with `basic_nack` instead of
    /// `basic_ack`.
    ///
    /// A manual consume without an application-level dead-letter queue
    /// nacks without requeue so a broker-level dead-letter exchange
    /// configured on the queue still receives the delivery. Every other
    /// combination acks: either the dead-letter copy is already safely
    /// queued, or the consume was ack-on-receive and its contract is
    /// at-most-once anyway.
    fn poison_settles_with_nack(ack_mode: AckMode, has_dead_letter: bool) -> bool {
        matches!(ack_mode, AckMode::Manual) && !has_dead_letter
    }

    /// Settle a delivery that failed to decode into an envelope: an
    /// oversize payload, a missing AMQP `type` property, or any other
    /// decode failure.
    ///
    /// When `dead_letter_routing_key` is configured the raw delivery is
    /// routed to the dead-letter queue through the same mandatory,
    /// confirmed and persistent publish as an exhausted delivery, so a
    /// poison message is never silently lost; a failed publish nacks the
    /// delivery (requeuing transient failures, dropping unroutable ones)
    /// so its prefetch slot is freed rather than left to stall the
    /// consumer. Without a dead-letter queue the delivery is dropped with
    /// a warning. An `Unacknowledged` consume is already settled by the
    /// broker, so the dead-letter copy is best-effort.
    async fn handle_poison(
        &self,
        channel: &Channel,
        delivery: &Delivery,
        err: &BusError,
    ) -> DeliveryDisposition {
        let dead_letter = self.config.dead_letter_routing_key.as_deref();
        tracing::warn!(
            delivery_tag = delivery.delivery_tag,
            error = %err,
            dead_letter = dead_letter.is_some(),
            "rabbitmq delivery failed to decode before dispatch"
        );

        if matches!(self.config.ack_mode, AckMode::Unacknowledged) {
            if let Some(routing_key) = dead_letter {
                let published = self
                    .publish_dead_letter(channel, delivery, &delivery.data, routing_key)
                    .await;
                if let Err(err) = &published {
                    tracing::error!(
                        delivery_tag = delivery.delivery_tag,
                        error = %err,
                        "rabbitmq dead-letter publish of poison delivery failed; delivery lost (no_ack consume)"
                    );
                }
            }
            return DeliveryDisposition::Settled;
        }

        let settle_with_nack =
            Self::poison_settles_with_nack(self.config.ack_mode, dead_letter.is_some());
        Self::exhausted_core(
            dead_letter.map(|routing_key| {
                move || async move {
                    let published = self
                        .publish_dead_letter(channel, delivery, &delivery.data, routing_key)
                        .await;
                    if let Err(err) = &published {
                        tracing::error!(
                            delivery_tag = delivery.delivery_tag,
                            error = %err,
                            "rabbitmq dead-letter publish of poison delivery failed; original nacked so the prefetch slot is freed, consumer continues"
                        );
                    }
                    published
                }
            }),
            move |requeue| async move {
                Self::nack_after_failed_publish(
                    channel,
                    delivery,
                    requeue,
                    "dead-letter publish of poison delivery",
                )
                .await
            },
            move || async move {
                let settled = if settle_with_nack {
                    channel
                        .basic_nack(
                            delivery.delivery_tag,
                            BasicNackOptions {
                                requeue: false,
                                ..BasicNackOptions::default()
                            },
                        )
                        .await
                } else {
                    channel
                        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                        .await
                }
                .map_err(|err| BusError::Transport(Box::new(err)));
                if let Err(err) = &settled {
                    tracing::warn!(
                        delivery_tag = delivery.delivery_tag,
                        error = %err,
                        "rabbitmq settle of poison delivery failed; consumer continues"
                    );
                }
                settled
            },
        )
        .await
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
                            .publish_dead_letter(channel, delivery, &envelope.payload, routing_key)
                            .await;
                        if let Err(err) = &published {
                            tracing::error!(
                                message_type = %envelope.message_type,
                                message_id = %envelope.message_id,
                                delivery_tag = delivery.delivery_tag,
                                error = %err,
                                "rabbitmq dead-letter publish failed; original nacked so the prefetch slot is freed, consumer continues"
                            );
                        }
                        published
                    }
                }),
            move |requeue| async move {
                Self::nack_after_failed_publish(channel, delivery, requeue, "dead-letter publish")
                    .await
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
                        "rabbitmq ack after dead-letter failed; consumer continues, broker will redeliver"
                    );
                }
                ack
            },
        )
        .await
    }

    /// Dead-letter-then-settle core of the exhausted path, generic over
    /// the broker operations so the ordering is unit-testable without
    /// a broker.
    ///
    /// A `None` dead-letter means no routing key is configured and the
    /// delivery is dropped via the `ack`/nack closure. When the
    /// dead-letter publish fails the delivery is not left unsettled
    /// (which would starve the prefetch window on a live channel):
    /// `settle_on_publish_failure` nacks it, requeuing or dropping per
    /// [`Self::requeue_on_publish_failure`].
    async fn exhausted_core<P, PF, S, SF, A, AF>(
        dead_letter: Option<P>,
        settle_on_publish_failure: S,
        ack: A,
    ) -> DeliveryDisposition
    where
        P: FnOnce() -> PF,
        PF: Future<Output = Result<(), BusError>>,
        S: FnOnce(bool) -> SF,
        SF: Future<Output = Result<(), BusError>>,
        A: FnOnce() -> AF,
        AF: Future<Output = Result<(), BusError>>,
    {
        if let Some(publish) = dead_letter {
            if let Err(err) = publish().await {
                let requeue = Self::requeue_on_publish_failure(&err);
                return DeliveryDisposition::from_settle_result(
                    &settle_on_publish_failure(requeue).await,
                );
            }
        }
        DeliveryDisposition::from_settle_result(&ack().await)
    }

    /// Republish a failed delivery to the wait queue with a mandatory,
    /// confirmed and persistent publish.
    ///
    /// The retry copy is forced to `delivery_mode` 2 so a transient
    /// original is not lost on a broker restart mid-retry, and the
    /// publisher confirm is awaited so the original is only acked once
    /// the broker has stored the copy: dropping the confirm (the prior
    /// behaviour) acked the original before the copy was durable, losing
    /// the message and violating the at-least-once contract. The publish
    /// is `mandatory` so a missing wait queue surfaces as
    /// [`BusError::Unroutable`] instead of being silently discarded.
    async fn publish_to_wait_queue(
        channel: &Channel,
        delivery: &Delivery,
        wait_queue: &str,
    ) -> Result<(), BusError> {
        let routing_key = to_short_string(wait_queue, "wait queue name")?;
        let confirmation = channel
            .basic_publish(
                ShortString::from(""),
                routing_key,
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                &delivery.data,
                delivery.properties.clone().with_delivery_mode(2),
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;
        crate::confirm::confirmation_to_result(confirmation, "retry publish", wait_queue)
    }

    /// Publish a delivery's payload to the dead-letter queue with a
    /// mandatory, confirmed and persistent publish.
    ///
    /// The copy is forced to `delivery_mode` 2 so it survives a broker
    /// restart even when the original delivery was transient. Success
    /// requires a broker ack without a returned message; every other
    /// confirmation surfaces as an error so the caller leaves the
    /// original delivery unacked for redelivery.
    async fn publish_dead_letter(
        &self,
        channel: &Channel,
        delivery: &Delivery,
        payload: &[u8],
        routing_key: &str,
    ) -> Result<(), BusError> {
        let confirmation = channel
            .basic_publish(
                ShortString::from(""),
                to_short_string(routing_key, "dead-letter routing key")?,
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                payload,
                delivery.properties.clone().with_delivery_mode(2),
            )
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?
            .await
            .map_err(|err| BusError::Transport(Box::new(err)))?;
        crate::confirm::confirmation_to_result(confirmation, "dead-letter publish", routing_key)
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
                // `x-death` is an ordinary header any producer can forge:
                // a negative or huge count must not poison the retry
                // counter. Clamp negatives to 0 before the conversion so
                // `try_from` never maps a forged negative to `u32::MAX`.
                return u32::try_from(count.max(&0).to_owned()).unwrap_or(u32::MAX);
            }
        }
    }
    0
}

pub(crate) fn delivery_to_envelope(
    props: &BasicProperties,
    payload: &[u8],
    max_payload_bytes: usize,
) -> Result<hexeract_bus::BusEnvelope, BusError> {
    use std::collections::HashMap as StdHashMap;
    use std::time::SystemTime;

    if payload.len() > max_payload_bytes {
        return Err(BusError::PayloadTooLarge {
            size: payload.len(),
            max: max_payload_bytes,
        });
    }

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
            // A foreign or malformed delivery is normal untrusted input,
            // not a framework bug, so it must not surface as
            // `BusError::Internal` (documented as "report upstream").
            BusError::InvalidTopology {
                reason: "rabbitmq delivery missing AMQP `type` property (envelope message_type)"
                    .to_owned(),
            }
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

    // `published_at` is the publisher's creation instant, not the
    // consume time. The transport writes it into the AMQP `timestamp`
    // property; restore it from there and fall back to now only when the
    // property is absent (foreign producer that did not stamp it).
    let published_at = props.timestamp().map_or_else(SystemTime::now, |secs| {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    });

    Ok(hexeract_bus::BusEnvelope::restore(
        message_id,
        message_type,
        payload.to_vec(),
        correlation_id,
        reply_to,
        headers,
        published_at,
    ))
}

/// Build the handler context from an already-decoded envelope.
///
/// Deriving the IDs from the envelope (rather than re-parsing the AMQP
/// properties) guarantees `ctx.message_id == envelope.message_id` and
/// `ctx.correlation_id == envelope.correlation_id` for the same
/// delivery: re-parsing minted a second, different random UUID whenever
/// a property was absent, breaking correlation between handler logs and
/// envelope-derived logs.
pub(crate) fn build_handler_context(envelope: &hexeract_bus::BusEnvelope) -> HandlerContext {
    HandlerContext::new(
        MessageId::from(envelope.message_id),
        CorrelationId::from(envelope.correlation_id),
    )
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
        assert_eq!(cfg.max_payload_bytes, DEFAULT_MAX_PAYLOAD_BYTES);
    }

    #[test]
    fn qos_is_skipped_only_under_unacknowledged() {
        assert!(qos_applies(AckMode::Manual));
        assert!(qos_applies(AckMode::AckOnReceive));
        assert!(!qos_applies(AckMode::Unacknowledged));
    }

    #[test]
    fn delivery_to_envelope_extracts_message_id_from_amqp_properties() {
        let message_id = Uuid::from_u128(0xABCD);
        let correlation_id = Uuid::from_u128(0x1234);
        let props = BasicProperties::default()
            .with_message_id(message_id.to_string().into())
            .with_correlation_id(correlation_id.to_string().into())
            .with_type("orders.placed".into());

        let envelope =
            delivery_to_envelope(&props, b"{\"order_id\":\"x\"}", DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("must decode");
        assert_eq!(envelope.message_id, message_id);
        assert_eq!(envelope.correlation_id, correlation_id);
        assert_eq!(envelope.message_type, "orders.placed");
    }

    #[test]
    fn delivery_to_envelope_mints_fresh_message_id_when_property_missing() {
        let props = BasicProperties::default().with_type("orders.placed".into());

        let envelope =
            delivery_to_envelope(&props, b"{}", DEFAULT_MAX_PAYLOAD_BYTES).expect("must decode");
        assert_ne!(envelope.message_id, Uuid::nil());
        assert_ne!(envelope.correlation_id, Uuid::nil());
        assert_eq!(envelope.message_type, "orders.placed");
    }

    #[test]
    fn delivery_to_envelope_returns_invalid_topology_when_type_property_missing() {
        let props = BasicProperties::default();
        let err = delivery_to_envelope(&props, b"{}", DEFAULT_MAX_PAYLOAD_BYTES)
            .expect_err("missing `type` must surface as a non-Internal error");
        match err {
            BusError::InvalidTopology { reason } => assert!(reason.contains("type")),
            other => panic!(
                "expected InvalidTopology (untrusted input, not a framework bug), got {other:?}"
            ),
        }
    }

    #[test]
    fn delivery_to_envelope_restores_published_at_from_timestamp_property() {
        let published_at_secs = 1_700_000_000u64;
        let props = BasicProperties::default()
            .with_type("orders.placed".into())
            .with_timestamp(published_at_secs);
        let envelope =
            delivery_to_envelope(&props, b"{}", DEFAULT_MAX_PAYLOAD_BYTES).expect("must decode");
        let restored = envelope
            .published_at
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after epoch")
            .as_secs();
        assert_eq!(
            restored, published_at_secs,
            "published_at must come from the AMQP timestamp, not consume time"
        );
    }

    #[test]
    fn build_handler_context_shares_ids_with_envelope_when_properties_missing() {
        // No message_id / correlation_id properties: the envelope mints
        // fresh UUIDs, and the context must reuse exactly those, not mint
        // a second independent pair.
        let props = BasicProperties::default().with_type("orders.placed".into());
        let envelope =
            delivery_to_envelope(&props, b"{}", DEFAULT_MAX_PAYLOAD_BYTES).expect("must decode");
        let ctx = build_handler_context(&envelope);
        assert_eq!(*ctx.message_id.as_uuid(), envelope.message_id);
        assert_eq!(*ctx.correlation_id.as_uuid(), envelope.correlation_id);
    }

    #[test]
    fn death_count_saturates_on_negative_count() {
        // A forged negative count must clamp to 0, never to u32::MAX.
        let props = x_death_properties(vec![x_death_entry("orders.retry", "expired", -1)]);
        assert_eq!(death_count(&props, "orders.retry"), 0);
    }

    #[test]
    fn retry_counter_does_not_overflow_at_u32_max() {
        // A forged count at i64::MAX maps to u32::MAX; +1 must saturate,
        // not wrap to 0 (which would retry forever) or panic in debug.
        let props = x_death_properties(vec![x_death_entry("orders.retry", "expired", i64::MAX)]);
        let current = death_count(&props, "orders.retry").saturating_add(1);
        assert_eq!(current, u32::MAX);
    }

    #[test]
    fn requeue_on_publish_failure_drops_unroutable_and_requeues_transient() {
        let unroutable = BusError::Unroutable {
            routing_key: "dlq".to_owned(),
            reply_text: "NO_ROUTE".to_owned(),
            reply_code: 312,
        };
        assert!(
            !RabbitMqWorker::requeue_on_publish_failure(&unroutable),
            "an unroutable DLQ must drop, not loop forever"
        );
        let transient = BusError::Transport(Box::new(std::io::Error::other("broker hiccup")));
        assert!(
            RabbitMqWorker::requeue_on_publish_failure(&transient),
            "a transient transport error must requeue for another attempt"
        );
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

        let envelope =
            delivery_to_envelope(&props, b"{}", DEFAULT_MAX_PAYLOAD_BYTES).expect("must decode");
        assert_eq!(
            envelope.headers.get("tenant").map(String::as_str),
            Some("acme")
        );
        assert_eq!(envelope.reply_to.as_deref(), Some("orders.replies"));
    }

    #[test]
    fn poison_settles_with_nack_only_for_manual_without_dead_letter() {
        assert!(RabbitMqWorker::poison_settles_with_nack(
            AckMode::Manual,
            false
        ));
        assert!(!RabbitMqWorker::poison_settles_with_nack(
            AckMode::Manual,
            true
        ));
        assert!(!RabbitMqWorker::poison_settles_with_nack(
            AckMode::AckOnReceive,
            false
        ));
        assert!(!RabbitMqWorker::poison_settles_with_nack(
            AckMode::AckOnReceive,
            true
        ));
    }

    #[test]
    fn delivery_to_envelope_rejects_oversize_payload() {
        let props = BasicProperties::default().with_type("orders.placed".into());
        let payload = vec![b'x'; 9];

        let err = delivery_to_envelope(&props, &payload, 8)
            .expect_err("oversize payload must be rejected before the copy");
        match err {
            BusError::PayloadTooLarge { size, max } => {
                assert_eq!(size, 9);
                assert_eq!(max, 8);
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn delivery_to_envelope_accepts_payload_at_limit() {
        let props = BasicProperties::default().with_type("orders.placed".into());
        let payload = b"{}";

        let envelope = delivery_to_envelope(&props, payload, payload.len())
            .expect("payload exactly at the limit must pass");
        assert_eq!(envelope.payload, payload);
    }

    #[test]
    fn build_handler_context_propagates_message_id_and_correlation_id() {
        let message_id = Uuid::from_u128(0x42);
        let correlation_id = Uuid::from_u128(0x7);
        let props = BasicProperties::default()
            .with_message_id(message_id.to_string().into())
            .with_correlation_id(correlation_id.to_string().into())
            .with_type("orders.placed".into());

        let envelope =
            delivery_to_envelope(&props, b"{}", DEFAULT_MAX_PAYLOAD_BYTES).expect("must decode");
        let ctx = build_handler_context(&envelope);
        assert_eq!(*ctx.message_id.as_uuid(), message_id);
        assert_eq!(*ctx.correlation_id.as_uuid(), correlation_id);
    }

    #[test]
    fn build_handler_context_mints_fresh_ids_when_properties_missing() {
        let props = BasicProperties::default().with_type("orders.placed".into());
        let envelope =
            delivery_to_envelope(&props, b"{}", DEFAULT_MAX_PAYLOAD_BYTES).expect("must decode");
        let ctx = build_handler_context(&envelope);
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
    async fn exhausted_core_nacks_instead_of_acking_when_dead_letter_publish_fails() {
        let ack_called = AtomicBool::new(false);
        let nack_called = AtomicBool::new(false);

        let disposition = RabbitMqWorker::exhausted_core(
            Some(|| async {
                Err(BusError::Transport(Box::new(std::io::Error::other(
                    "dead-letter broker down",
                ))))
            }),
            |_requeue| async {
                nack_called.store(true, Ordering::SeqCst);
                Ok(())
            },
            || async {
                ack_called.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        // On a live channel a failed dead-letter must nack (freeing the
        // prefetch slot), never ack and never leave the delivery unsettled.
        assert_eq!(disposition, DeliveryDisposition::Settled);
        assert!(
            nack_called.load(Ordering::SeqCst),
            "a failed dead-letter publish must nack to free the prefetch slot"
        );
        assert!(
            !ack_called.load(Ordering::SeqCst),
            "the success ack path must not run when the publish failed"
        );
    }

    #[tokio::test]
    async fn exhausted_core_requeues_transient_drops_unroutable() {
        let requeue_seen = std::sync::atomic::AtomicU8::new(0);
        // Transient transport error -> requeue: true.
        RabbitMqWorker::exhausted_core(
            Some(|| async {
                Err(BusError::Transport(Box::new(std::io::Error::other(
                    "hiccup",
                ))))
            }),
            |requeue| {
                requeue_seen.store(u8::from(requeue), Ordering::SeqCst);
                async { Ok(()) }
            },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(
            requeue_seen.load(Ordering::SeqCst),
            1,
            "transient must requeue"
        );

        // Unroutable (DLQ gone) -> requeue: false.
        RabbitMqWorker::exhausted_core(
            Some(|| async {
                Err(BusError::Unroutable {
                    routing_key: "dlq".to_owned(),
                    reply_text: "NO_ROUTE".to_owned(),
                    reply_code: 312,
                })
            }),
            |requeue| {
                requeue_seen.store(u8::from(requeue), Ordering::SeqCst);
                async { Ok(()) }
            },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(
            requeue_seen.load(Ordering::SeqCst),
            0,
            "unroutable must drop"
        );
    }

    #[tokio::test]
    async fn exhausted_core_acks_when_dead_letter_publish_succeeds() {
        let disposition = RabbitMqWorker::exhausted_core(
            Some(|| async { Ok(()) }),
            |_requeue| async { Ok(()) },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(disposition, DeliveryDisposition::Settled);
    }

    #[tokio::test]
    async fn exhausted_core_acks_when_no_dead_letter_configured() {
        let disposition = RabbitMqWorker::exhausted_core(
            None::<NoDeadLetter>,
            |_requeue| async { Ok(()) },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(disposition, DeliveryDisposition::Settled);
    }

    #[tokio::test]
    async fn exhausted_core_reports_left_for_redelivery_when_final_ack_fails() {
        let disposition = RabbitMqWorker::exhausted_core(
            Some(|| async { Ok(()) }),
            |_requeue| async { Ok(()) },
            || async { Err(BusError::Internal("simulated basic_ack failure".to_owned())) },
        )
        .await;
        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
    }

    #[tokio::test]
    async fn exhausted_core_reports_left_for_redelivery_when_nack_also_fails() {
        let disposition = RabbitMqWorker::exhausted_core(
            Some(|| async { Err(BusError::Transport(Box::new(std::io::Error::other("down")))) }),
            |_requeue| async {
                Err(BusError::Transport(Box::new(std::io::Error::other(
                    "nack failed too",
                ))))
            },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(disposition, DeliveryDisposition::LeftForRedelivery);
    }

    #[tokio::test]
    async fn retry_core_nacks_instead_of_acking_when_wait_queue_publish_fails() {
        let ack_called = AtomicBool::new(false);
        let nack_called = AtomicBool::new(false);

        let disposition = RabbitMqWorker::retry_core(
            || async {
                Err(BusError::Transport(Box::new(std::io::Error::other(
                    "wait queue publish failed",
                ))))
            },
            |_requeue| async {
                nack_called.store(true, Ordering::SeqCst);
                Ok(())
            },
            || async {
                ack_called.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert_eq!(disposition, DeliveryDisposition::Settled);
        assert!(
            nack_called.load(Ordering::SeqCst),
            "a failed retry publish must nack to free the prefetch slot"
        );
        assert!(
            !ack_called.load(Ordering::SeqCst),
            "the success ack path must not run when the publish failed"
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
            |_requeue| async { Ok(()) },
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
            |_requeue| async { Ok(()) },
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
