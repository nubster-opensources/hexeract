//! Scheduled reminders dispatched through the bus sink.
//!
//! The scheduler is the outbox plus time: a message is persisted together
//! with the instant it is due, then relayed once that instant is reached.
//! This example schedules a one-shot reminder two seconds ahead and a
//! recurring cron reminder firing every two seconds, both dispatched to
//! RabbitMQ through [`BusSink`]. The live cron schedule is then inspected
//! and cancelled through [`SchedulerControl`].
//!
//! Run with (requires a running Docker daemon):
//!
//! ```bash
//! cargo run --example 06_scheduled_reminder -p hexeract-examples
//! ```

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use hexeract::bus::Binding;
use hexeract::bus::BusError;
use hexeract::bus::Exchange;
use hexeract::bus::ExchangeKind;
use hexeract::bus::Handler;
use hexeract::bus::Message;
use hexeract::bus::Queue;
use hexeract::bus::RoutingKey;
use hexeract::bus_rabbitmq::RabbitMqConnection;
use hexeract::bus_rabbitmq::RabbitMqTransport;
use hexeract::bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract::bus_rabbitmq::ensure_topology;
use hexeract::core::HandlerContext;
use hexeract::outbox::Event;
use hexeract::scheduler::BusSink;
use hexeract::scheduler::SchedulerBuilder;
use hexeract::scheduler::SchedulerControl;
use hexeract::scheduler_sql::Dialect;
use hexeract::scheduler_sql::PgScheduleStore;
use hexeract::scheduler_sql::schema::schema_ddl;
use serde::Deserialize;
use serde::Serialize;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const TABLE: &str = "scheduled_messages";
const ROUTING_KEY: &str = "reminders.due";
#[allow(dead_code)]
const MAX_ATTEMPTS: u32 = 5;
const POLL_INTERVAL: Duration = Duration::from_millis(500);
#[allow(dead_code)]
const ONE_SHOT_BUDGET: Duration = Duration::from_secs(10);
#[allow(dead_code)]
const CRON_BUDGET: Duration = Duration::from_secs(15);
#[allow(dead_code)]
const GRACE_WINDOW: Duration = Duration::from_secs(5);

/// Reminder payload scheduled for future delivery on the bus.
#[derive(Debug, Serialize, Deserialize)]
struct ReminderDue {
    reminder_id: Uuid,
    note: String,
}

impl Event for ReminderDue {
    const EVENT_TYPE: &'static str = "reminders.due";
}

impl Message for ReminderDue {
    const MESSAGE_TYPE: &'static str = "reminders.due";
}

/// Bus handler counting every reminder observed on the queue.
#[derive(Debug)]
struct CountingHandler {
    seen: Arc<AtomicUsize>,
}

impl Handler<ReminderDue> for CountingHandler {
    type Error = BusError;

    async fn handle(&self, message: ReminderDue, ctx: &HandlerContext) -> Result<(), Self::Error> {
        let total = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(
            reminder_id = %message.reminder_id,
            note = %message.note,
            message_id = ?ctx.message_id,
            total,
            "reminder received"
        );
        Ok(())
    }
}

/// Wait until `seen` reaches `target`, failing once `budget` is exhausted.
#[allow(dead_code)]
async fn wait_for_count(
    seen: &AtomicUsize,
    target: usize,
    budget: Duration,
    what: &str,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    while seen.load(Ordering::SeqCst) < target {
        if started.elapsed() > budget {
            return Err(format!(
                "{what}: only {}/{target} reminders received within {budget:?}",
                seen.load(Ordering::SeqCst)
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tracing::info!("starting postgres container");
    let postgres = Postgres::default().start().await?;
    let postgres_host = postgres.get_host().await?;
    let postgres_port = postgres.get_host_port_ipv4(5432).await?;
    let postgres_url =
        format!("postgres://postgres:postgres@{postgres_host}:{postgres_port}/postgres");
    let pool = PgPool::connect(&postgres_url).await?;
    let ddl = schema_ddl(Dialect::Postgres, TABLE)?;
    sqlx::raw_sql(&ddl).execute(&pool).await?;
    let store = PgScheduleStore::new(pool, TABLE)?;
    tracing::info!(table = TABLE, "schedule store ready");

    tracing::info!("starting rabbitmq container");
    let rabbitmq = RabbitMq::default().start().await?;
    let rabbitmq_host = rabbitmq.get_host().await?;
    let rabbitmq_port = rabbitmq.get_host_port_ipv4(5672).await?;
    let rabbitmq_uri = format!("amqp://{rabbitmq_host}:{rabbitmq_port}");

    let exchange = Exchange::new("reminders.exchange", ExchangeKind::Topic)?
        .durable(false)
        .auto_delete(true);
    let queue = Queue::new("reminders.received")?
        .durable(false)
        .auto_delete(true);
    let routing_key = RoutingKey::new(ROUTING_KEY)?;
    let binding = Binding::new(&queue.name, &exchange.name, routing_key.clone())?;

    let admin = RabbitMqConnection::connect(&rabbitmq_uri).await?;
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
    let consumer_conn = RabbitMqConnection::connect(&rabbitmq_uri).await?;
    let consumer = RabbitMqWorkerBuilder::new(consumer_conn)
        .queue(queue.name.as_str())
        .register_handler::<ReminderDue, _>(CountingHandler {
            seen: Arc::clone(&seen),
        })
        .build()?;
    let cancel = CancellationToken::new();
    let consumer_cancel = cancel.clone();
    let consumer_handle = tokio::spawn(async move { consumer.run(consumer_cancel).await });

    let transport = RabbitMqTransport::with_exchange(&rabbitmq_uri, exchange).await?;
    let scheduler = SchedulerBuilder::new(store.clone(), BusSink::new(transport))
        .poll_interval(POLL_INTERVAL)
        .build()?;
    let scheduler_cancel = cancel.clone();
    let scheduler_handle = tokio::spawn(async move { scheduler.run(scheduler_cancel).await });

    let control = SchedulerControl::new(Arc::new(store.clone()));
    tracing::info!("scheduler worker and bus consumer running");

    let _ = &control;

    cancel.cancel();
    scheduler_handle.await??;
    consumer_handle.await??;
    tracing::info!("scheduled reminder example completed");
    Ok(())
}
