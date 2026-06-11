//! Bus consumption feeding the in-process CQRS mediator.
//!
//! A message consumed from RabbitMQ triggers a `ProcessPayment` command
//! dispatched through the mediator, showing how transport and in-process
//! dispatch compose.
//!
//! Run with (requires a running Docker daemon):
//!
//! ```bash
//! cargo run --example 04_bus_mediator -p hexeract-examples
//! ```

#![allow(
    clippy::unused_async,
    reason = "the handler stays async to match the trait the #[handler] macro expands to"
)]

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
use serde::Deserialize;
use serde::Serialize;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const PROCESSED_BUDGET: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: Uuid,
    amount_cents: i64,
}

impl Message for OrderPlaced {
    const MESSAGE_TYPE: &'static str = "orders.placed";
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
        ctx: &HandlerContext,
    ) -> Result<Uuid, HexeractError> {
        let payment_id = Uuid::now_v7();
        tracing::info!(
            order_id = %cmd.order_id,
            %payment_id,
            amount_cents = cmd.amount_cents,
            correlation_id = %ctx.correlation_id,
            "payment processed"
        );
        self.recorded.lock().expect("poisoned").push(cmd.order_id);
        Ok(payment_id)
    }
}

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

    let (_rabbit, uri) = setup_rabbit().await?;

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

    let recorded = Arc::new(Mutex::new(Vec::new()));
    let builder =
        MediatorBuilder::new().register_command_handler::<ProcessPayment, _>(PaymentBook {
            recorded: Arc::clone(&recorded),
        });
    builder.verify_handlers()?;
    let mediator = builder.build()?;

    let worker_conn = RabbitMqConnection::connect(&uri).await?;
    let worker = RabbitMqWorkerBuilder::new(worker_conn)
        .queue(queue.name.as_str())
        .register_handler::<OrderPlaced, _>(PaymentBridge { mediator })
        .build()?;
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(cancel_for_task).await });

    let order_id = Uuid::now_v7();
    let publisher = RabbitMqTransport::with_exchange(&uri, exchange).await?;
    publisher
        .publish(
            routing_key.as_str(),
            &OrderPlaced {
                order_id,
                amount_cents: 4_200,
            },
        )
        .await?;

    let started = Instant::now();
    while recorded.lock().expect("poisoned").is_empty() {
        if started.elapsed() > PROCESSED_BUDGET {
            cancel.cancel();
            let _ = worker_handle.await;
            return Err("payment was not processed within the budget".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cancel.cancel();
    worker_handle.await??;

    assert_eq!(recorded.lock().expect("poisoned").as_slice(), &[order_id]);
    println!("processed payment for order {order_id}");
    Ok(())
}
