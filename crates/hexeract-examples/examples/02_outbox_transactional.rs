//! Transactional outbox over PostgreSQL.
//!
//! Demonstrates atomicity: a business row and its `OrderPlaced` event are
//! committed in the same transaction, then an [`OutboxWorker`] relays the
//! event to a handler. If the transaction rolled back, neither the order nor
//! the event would exist.
//!
//! Run with (requires a running Docker daemon):
//!
//! ```bash
//! cargo run --example 02_outbox_transactional -p hexeract-examples
//! ```

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use hexeract::core::HandlerContext;
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
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const OUTBOX_TABLE: &str = "orders_outbox";
const RELAY_BUDGET: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: Uuid,
    amount_cents: i64,
}

impl Event for OrderPlaced {
    const EVENT_TYPE: &'static str = "orders.placed";
}

struct RecordingDrain {
    seen: Arc<Mutex<Vec<Uuid>>>,
}

impl OutboxHandler<OrderPlaced> for RecordingDrain {
    type Error = OutboxError;

    async fn handle(&self, event: OrderPlaced, ctx: &HandlerContext) -> Result<(), Self::Error> {
        tracing::info!(order_id = %event.order_id, message_id = %ctx.message_id, "relayed");
        self.seen.lock().expect("poisoned").push(event.order_id);
        Ok(())
    }
}

fn registry(handler: RecordingDrain) -> HashMap<&'static str, Arc<dyn ErasedHandler>> {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let (_pg, pool) = setup_postgres().await?;
    let publisher = PgOutboxPublisher::new(pool.clone(), OUTBOX_TABLE)?;
    let store = PgOutboxStore::new(pool.clone(), OUTBOX_TABLE)?;

    // Atomic unit of work: business row and outbox event commit together.
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
    let event_id = publisher.publish_in_tx(&mut tx, &order).await?;
    tx.commit().await?;
    tracing::info!(%event_id, "committed order and event atomically");

    // Drain the outbox to a handler.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = OutboxWorker::new(
        store,
        registry(RecordingDrain {
            seen: Arc::clone(&seen),
        }),
        OutboxWorkerConfig::default(),
    );
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(worker.run(cancel.clone()));

    let started = Instant::now();
    while seen.lock().expect("poisoned").is_empty() {
        if started.elapsed() > RELAY_BUDGET {
            cancel.cancel();
            let _ = handle.await;
            return Err("outbox event was not relayed within the budget".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cancel.cancel();
    handle.await??;

    assert_eq!(seen.lock().expect("poisoned").as_slice(), &[order.order_id]);
    println!("relayed order {}", order.order_id);
    Ok(())
}
