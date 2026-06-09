//! End-to-end pipeline: transactional outbox -> RabbitMQ bus -> CQRS mediator.
//!
//! An order and its `OrderPlaced` event are written in one Postgres
//! transaction. The outbox worker relays the event to RabbitMQ; a bus
//! consumer bridges it into a `ProcessPayment` command dispatched by the
//! mediator. This is the whole framework wired together.
//!
//! Run with (requires a running Docker daemon):
//!
//! ```bash
//! cargo run --example 05_orders_to_payments -p hexeract-examples
//! ```

#![allow(
    clippy::unused_async,
    reason = "the mediator handler stays async to match the trait the #[handler] macro expands to"
)]

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use hexeract::bus::Binding;
use hexeract::bus::BusError;
use hexeract::bus::Exchange;
use hexeract::bus::ExchangeKind;
use hexeract::bus::Handler as BusHandler;
use hexeract::bus::Message;
use hexeract::bus::Queue;
use hexeract::bus::RoutingKey;
use hexeract::bus::Transport;
use hexeract::bus_rabbitmq::RabbitMqConnection;
use hexeract::bus_rabbitmq::RabbitMqTransport;
use hexeract::bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract::bus_rabbitmq::ensure_topology;
use hexeract::core::Command;
use hexeract::core::HandlerContext;
use hexeract::core::HexeractError;
use hexeract::macros::handler;
use hexeract::mediator::Mediator;
use hexeract::mediator::MediatorBuilder;
use hexeract::outbox::ErasedHandler;
use hexeract::outbox::Event;
use hexeract::outbox::Handler as OutboxHandler;
use hexeract::outbox::OutboxError;
use hexeract::outbox::OutboxPublisher;
use hexeract::outbox::OutboxWorker;
use hexeract::outbox::OutboxWorkerConfig;
use hexeract::outbox::TypedHandler;
use hexeract::outbox_sql::PgOutboxPublisher;
use hexeract::outbox_sql::PgOutboxStore;
use hexeract::outbox_sql::postgres::ensure_schema;
use serde::Deserialize;
use serde::Serialize;
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const OUTBOX_TABLE: &str = "orders_outbox";
const PIPELINE_BUDGET: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: Uuid,
    amount_cents: i64,
}

impl Event for OrderPlaced {
    const EVENT_TYPE: &'static str = "orders.placed";
}

impl Message for OrderPlaced {
    const MESSAGE_TYPE: &'static str = "orders.placed";
}

/// Outbox drain handler that forwards each relayed event onto the bus.
struct BusForwarder {
    transport: RabbitMqTransport,
    routing_key: String,
}

impl OutboxHandler<OrderPlaced> for BusForwarder {
    type Error = OutboxError;

    async fn handle(&self, event: OrderPlaced, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        self.transport
            .publish(&self.routing_key, &event)
            .await
            .map_err(|err| OutboxError::Internal(err.to_string()))?;
        Ok(())
    }
}

/// Bus consumer that bridges the wire message into a mediator command.
struct PaymentBridge {
    mediator: Mediator,
}

impl BusHandler<OrderPlaced> for PaymentBridge {
    type Error = BusError;

    async fn handle(&self, message: OrderPlaced, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        self.mediator
            .send(ProcessPayment {
                order_id: message.order_id,
                amount_cents: message.amount_cents,
            })
            .await
            .map_err(|err| BusError::Internal(err.to_string()))?;
        Ok(())
    }
}

struct ProcessPayment {
    order_id: Uuid,
    amount_cents: i64,
}

impl Command for ProcessPayment {
    type Output = Uuid;
}

struct PaymentBook {
    recorded: Arc<Mutex<Vec<Uuid>>>,
}

#[handler(command)]
impl PaymentBook {
    async fn handle(
        &self,
        cmd: ProcessPayment,
        _ctx: &HandlerContext,
    ) -> Result<Uuid, HexeractError> {
        let payment_id = Uuid::now_v7();
        tracing::info!(
            order_id = %cmd.order_id,
            amount_cents = cmd.amount_cents,
            %payment_id,
            "payment processed"
        );
        self.recorded.lock().expect("poisoned").push(cmd.order_id);
        Ok(payment_id)
    }
}

fn outbox_registry(handler: BusForwarder) -> HashMap<&'static str, Arc<dyn ErasedHandler>> {
    let mut map = HashMap::new();
    let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(handler));
    map.insert(erased.event_type(), erased);
    map
}

async fn setup_postgres() -> Result<(ContainerAsync<Postgres>, PgPool), Box<dyn Error>> {
    let container = Postgres::default().start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPool::connect(&url).await?;
    ensure_schema(&pool, OUTBOX_TABLE).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (order_id UUID PRIMARY KEY, amount_cents BIGINT NOT NULL)",
    )
    .execute(&pool)
    .await?;
    Ok((container, pool))
}

async fn setup_rabbit() -> Result<(ContainerAsync<RabbitMq>, String), Box<dyn Error>> {
    let container = RabbitMq::default().start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5672).await?;
    let uri = format!("amqp://{host}:{port}");
    Ok((container, uri))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let (_pg, pool) = setup_postgres().await?;
    let (_rabbit, uri) = setup_rabbit().await?;

    // Bus topology.
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

    // Mediator + bus consumer.
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let mediator = MediatorBuilder::new()
        .register_command_handler::<ProcessPayment, _>(PaymentBook {
            recorded: Arc::clone(&recorded),
        })
        .build()?;
    let worker_conn = RabbitMqConnection::connect(&uri).await?;
    let bus_worker = RabbitMqWorkerBuilder::new(worker_conn)
        .queue(queue.name.as_str())
        .register_handler::<OrderPlaced, _>(PaymentBridge { mediator })
        .build()?;
    let bus_cancel = CancellationToken::new();
    let bus_cancel_task = bus_cancel.clone();
    let bus_handle = tokio::spawn(async move { bus_worker.run(bus_cancel_task).await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Outbox worker forwarding to the bus.
    let transport = RabbitMqTransport::with_exchange(&uri, exchange).await?;
    let forwarder = BusForwarder {
        transport,
        routing_key: routing_key.as_str().to_owned(),
    };
    let store = PgOutboxStore::new(pool.clone(), OUTBOX_TABLE)?;
    let outbox_worker = OutboxWorker::new(
        store,
        outbox_registry(forwarder),
        OutboxWorkerConfig::default(),
    );
    let outbox_cancel = CancellationToken::new();
    let outbox_handle = tokio::spawn(outbox_worker.run(outbox_cancel.clone()));

    // Atomic write: order + event in one transaction.
    let publisher = PgOutboxPublisher::new(pool.clone(), OUTBOX_TABLE)?;
    let order = OrderPlaced {
        order_id: Uuid::now_v7(),
        amount_cents: 4_200,
    };
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO orders (order_id, amount_cents) VALUES ($1, $2)")
        .bind(order.order_id)
        .bind(order.amount_cents)
        .execute(&mut *tx)
        .await?;
    publisher.publish_in_tx(&mut tx, &order).await?;
    tx.commit().await?;

    // Await the full pipeline.
    let started = Instant::now();
    while recorded.lock().expect("poisoned").is_empty() {
        if started.elapsed() > PIPELINE_BUDGET {
            outbox_cancel.cancel();
            bus_cancel.cancel();
            let _ = outbox_handle.await;
            let _ = bus_handle.await;
            return Err("pipeline did not complete within the budget".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    outbox_cancel.cancel();
    bus_cancel.cancel();
    outbox_handle.await??;
    bus_handle.await??;

    assert_eq!(
        recorded.lock().expect("poisoned").as_slice(),
        &[order.order_id]
    );
    println!(
        "order {} flowed through outbox -> bus -> mediator",
        order.order_id
    );
    Ok(())
}
