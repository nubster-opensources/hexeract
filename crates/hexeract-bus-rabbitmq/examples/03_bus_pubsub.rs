//! End-to-end RabbitMQ pub/sub example.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example 03_bus_pubsub -p hexeract-bus-rabbitmq
//! ```
//!
//! The example spins up a RabbitMQ container via `testcontainers`,
//! declares an exchange, a queue and a binding through
//! [`hexeract_bus_rabbitmq::ensure_topology`], spawns a
//! [`RabbitMqWorker`](hexeract_bus_rabbitmq::RabbitMqWorker) with a
//! counting handler, publishes five messages through a
//! [`RabbitMqTransport`](hexeract_bus_rabbitmq::RabbitMqTransport),
//! and asserts every delivery is acknowledged in under one second.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use hexeract_bus::Binding;
use hexeract_bus::BusError;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::Handler;
use hexeract_bus::Message;
use hexeract_bus::Queue;
use hexeract_bus::RoutingKey;
use hexeract_bus::Transport;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::RabbitMqTransport;
use hexeract_bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract_bus_rabbitmq::ensure_topology;
use hexeract_core::HandlerContext;
use serde::Deserialize;
use serde::Serialize;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const TOTAL_MESSAGES: usize = 5;
const LATENCY_BUDGET: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: Uuid,
}

impl Message for OrderPlaced {
    const MESSAGE_TYPE: &'static str = "orders.placed";
}

#[derive(Debug)]
struct CountingHandler {
    seen: Arc<AtomicUsize>,
}

impl Handler<OrderPlaced> for CountingHandler {
    type Error = BusError;

    async fn handle(&self, message: OrderPlaced, ctx: &HandlerContext) -> Result<(), Self::Error> {
        let total = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(
            order_id = %message.order_id,
            message_id = ?ctx.message_id,
            correlation_id = ?ctx.correlation_id,
            total,
            "consume"
        );
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tracing::info!("starting rabbitmq container");
    let container = RabbitMq::default().start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5672).await?;
    let uri = format!("amqp://{host}:{port}");
    tracing::info!(%uri, "rabbitmq ready");

    let exchange = Exchange::new("orders.exchange", ExchangeKind::Topic)?
        .durable(false)
        .auto_delete(true);
    let queue = Queue::new("orders.received")?
        .durable(false)
        .auto_delete(true);
    let routing_key = RoutingKey::new("orders.placed")?;
    let binding = Binding::new(&queue.name, &exchange.name, routing_key.clone())?;

    let admin = RabbitMqConnection::connect(&uri).await?;
    ensure_topology(
        &admin,
        std::slice::from_ref(&exchange),
        std::slice::from_ref(&queue),
        std::slice::from_ref(&binding),
    )
    .await?;
    tracing::info!(
        exchange = %exchange.name,
        queue = %queue.name,
        routing_key = %routing_key,
        "topology declared"
    );

    let seen = Arc::new(AtomicUsize::new(0));
    let worker_conn = RabbitMqConnection::connect(&uri).await?;
    let worker = RabbitMqWorkerBuilder::new(worker_conn)
        .queue(queue.name.as_str())
        .register_handler::<OrderPlaced, _>(CountingHandler {
            seen: Arc::clone(&seen),
        })
        .build()?;
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    // Give the worker enough time to settle its `basic_consume`
    // before we start publishing, so the first messages do not race
    // the consumer registration.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let publisher = RabbitMqTransport::with_exchange(&uri, exchange).await?;
    let started = Instant::now();
    for i in 0..TOTAL_MESSAGES {
        let order = OrderPlaced {
            order_id: Uuid::from_u128(u128::from(i as u64 + 1)),
        };
        let message_id = publisher.publish(routing_key.as_str(), &order).await?;
        tracing::info!(%message_id, index = i, "publish");
    }

    // Wait until every published message has been observed by the
    // handler, or fail when the latency budget is exhausted.
    while seen.load(Ordering::SeqCst) < TOTAL_MESSAGES {
        if started.elapsed() > LATENCY_BUDGET {
            cancel.cancel();
            let _ = worker_handle.await;
            return Err(format!(
                "only {}/{} messages consumed within {:?}",
                seen.load(Ordering::SeqCst),
                TOTAL_MESSAGES,
                LATENCY_BUDGET
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let elapsed = started.elapsed();
    tracing::info!(
        total = TOTAL_MESSAGES,
        elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        "all messages consumed"
    );

    cancel.cancel();
    worker_handle.await??;
    Ok(())
}
