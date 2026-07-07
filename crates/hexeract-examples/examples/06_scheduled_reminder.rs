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
use std::time::SystemTime;

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
use hexeract::scheduler::ScheduleStatus;
use hexeract::scheduler::ScheduleStore;
use hexeract::scheduler::ScheduledMessage;
use hexeract::scheduler::SchedulerBuilder;
use hexeract::scheduler::SchedulerControl;
use hexeract::scheduler::Target;
use hexeract::scheduler_sql::Dialect;
use hexeract::scheduler_sql::PgScheduleStore;
use hexeract::scheduler_sql::schema::schema_ddl;
use serde::Deserialize;
use serde::Serialize;
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const TABLE: &str = "scheduled_messages";
const ROUTING_KEY: &str = "reminders.due";
const MAX_ATTEMPTS: u32 = 5;
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const ONE_SHOT_BUDGET: Duration = Duration::from_secs(10);
const CRON_BUDGET: Duration = Duration::from_secs(15);
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

/// Start a Postgres container and prepare the schedule store table.
async fn setup_postgres() -> Result<(ContainerAsync<Postgres>, PgScheduleStore), Box<dyn Error>> {
    let container = Postgres::default().start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPool::connect(&url).await?;
    let ddl = schema_ddl(Dialect::Postgres, TABLE)?;
    sqlx::raw_sql(&ddl).execute(&pool).await?;
    let store = PgScheduleStore::new(pool, TABLE)?;
    Ok((container, store))
}

/// Start a RabbitMQ container and return its AMQP connection uri.
async fn setup_rabbit() -> Result<(ContainerAsync<RabbitMq>, String), Box<dyn Error>> {
    let container = RabbitMq::default().start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5672).await?;
    let uri = format!("amqp://{host}:{port}");
    Ok((container, uri))
}

/// Declare the reminders exchange, queue and binding on the running broker.
async fn declare_topology(rabbitmq_uri: &str) -> Result<(Exchange, Queue), Box<dyn Error>> {
    let exchange = Exchange::new("reminders.exchange", ExchangeKind::Topic)?
        .durable(false)
        .auto_delete(true);
    let queue = Queue::new("reminders.received")?
        .durable(false)
        .auto_delete(true);
    let routing_key = RoutingKey::new(ROUTING_KEY)?;
    let binding = Binding::new(&queue.name, &exchange.name, routing_key.clone())?;

    let admin = RabbitMqConnection::connect(rabbitmq_uri).await?;
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
    Ok((exchange, queue))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let (_postgres, store) = setup_postgres().await?;
    tracing::info!(table = TABLE, "schedule store ready");

    let (_rabbitmq, rabbitmq_uri) = setup_rabbit().await?;
    let (exchange, queue) = declare_topology(&rabbitmq_uri).await?;

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

    tracing::info!("act 1: one-shot reminder due in two seconds");
    let one_shot = ReminderDue {
        reminder_id: Uuid::new_v4(),
        note: "one-shot reminder".to_owned(),
    };
    let message = ScheduledMessage::delay(
        Target::bus(ROUTING_KEY),
        SystemTime::now() + Duration::from_secs(2),
        &one_shot,
    )?;
    store.insert(&message, MAX_ATTEMPTS).await?;
    tracing::info!(schedule_id = %message.schedule_id, "one-shot reminder scheduled");
    wait_for_count(&seen, 1, ONE_SHOT_BUDGET, "act 1 one-shot reminder").await?;

    tracing::info!("act 2: recurring cron reminder every two seconds");
    let recurring = ReminderDue {
        reminder_id: Uuid::new_v4(),
        note: "recurring reminder".to_owned(),
    };
    let message = ScheduledMessage::cron(
        Target::bus(ROUTING_KEY),
        "*/2 * * * * *",
        SystemTime::now() + Duration::from_secs(2),
        &recurring,
    )?;
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await?;
    tracing::info!(%schedule_id, "recurring reminder scheduled");

    // Two occurrences prove the full recurring path: claim, dispatch and
    // reschedule to the next cron instant.
    wait_for_count(&seen, 3, CRON_BUDGET, "act 2 recurring reminder").await?;

    let snapshot = control
        .inspect(schedule_id)
        .await?
        .ok_or("recurring schedule not found on inspect")?;
    tracing::info!(
        status = ?snapshot.status,
        scheduled_for = ?snapshot.scheduled_for,
        attempts = snapshot.attempts,
        "live schedule inspected"
    );
    if !matches!(snapshot.status, ScheduleStatus::Pending) {
        return Err(format!("expected a pending schedule, got {:?}", snapshot.status).into());
    }

    control.cancel(schedule_id).await?;
    let snapshot = control
        .inspect(schedule_id)
        .await?
        .ok_or("recurring schedule not found after cancel")?;
    if !matches!(snapshot.status, ScheduleStatus::Cancelled) {
        return Err(format!("expected a cancelled schedule, got {:?}", snapshot.status).into());
    }
    tracing::info!("recurring reminder cancelled");

    // Let any in-flight occurrence land before taking the baseline, then
    // assert the cancelled schedule never fires again.
    tokio::time::sleep(POLL_INTERVAL + Duration::from_secs(2)).await;
    let baseline = seen.load(Ordering::SeqCst);
    tokio::time::sleep(GRACE_WINDOW).await;
    let after_grace = seen.load(Ordering::SeqCst);
    if after_grace != baseline {
        return Err(format!(
            "cancelled schedule kept firing: {after_grace} reminders after a baseline of {baseline}"
        )
        .into());
    }
    tracing::info!(
        total = after_grace,
        "no reminder after cancel, grace window clean"
    );

    cancel.cancel();
    scheduler_handle.await??;
    consumer_handle.await??;
    tracing::info!("scheduled reminder example completed");
    Ok(())
}
