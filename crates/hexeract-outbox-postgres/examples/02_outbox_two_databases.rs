//! End-to-end demonstration of the Outbox MVP with two-database isolation.
//!
//! This example spins up two PostgreSQL containers via `testcontainers`:
//!
//! - `ops_db`: hosts the business state and the outbox table
//!   (`audit_outbox` using the canonical schema exposed by
//!   [`hexeract_outbox_postgres::POSTGRES_SCHEMA_SQL`]).
//! - `audit_db`: hosts the immutable audit log written by the worker's
//!   handler.
//!
//! The pattern enforces defense-in-depth: the operational database
//! cannot tamper with audit history, and the audit handler cannot
//! corrupt business state. The outbox bridges the two with
//! at-least-once dispatch semantics.
//!
//! Flow:
//!
//! 1. A simulated business use case opens a transaction on `ops_db`,
//!    publishes a `UserRegistered` event via
//!    [`hexeract_outbox::OutboxPublisher::publish_in_tx`] and commits.
//! 2. A [`hexeract_outbox::OutboxWorker`] polls `ops_db.audit_outbox`,
//!    dispatches the envelope to a handler that writes a derived audit
//!    record into `audit_db.audit_log`, and marks the envelope
//!    delivered.
//! 3. The example asserts the audit row lands within 500 ms.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example 02_outbox_two_databases -p hexeract-outbox-postgres
//! ```
//!
//! Requires a running Docker daemon.

use std::error::Error;
use std::time::Duration;
use std::time::Instant;

use deadpool_postgres::Config;
use deadpool_postgres::Pool;
use deadpool_postgres::Runtime;
use hexeract_core::HandlerContext;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox_postgres::PgOutboxPublisher;
use hexeract_outbox_postgres::PgOutboxWorkerBuilder;
use hexeract_outbox_postgres::ensure_schema;
use serde::Deserialize;
use serde::Serialize;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const OUTBOX_TABLE: &str = "audit_outbox";
const DELIVERY_DEADLINE: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserRegistered {
    user_id: Uuid,
    email: String,
}

impl Event for UserRegistered {
    const EVENT_TYPE: &'static str = "users.registered";
}

/// Handler that writes a derived audit record into the audit database.
///
/// The handler owns its own `audit_pool`, demonstrating that the worker
/// does not pilot the handler's database connection. Handlers are free
/// to talk to any backend (DB, broker, HTTP endpoint, ...).
#[derive(Clone)]
struct AuditWriter {
    audit_pool: Pool,
}

impl Handler<UserRegistered> for AuditWriter {
    type Error = OutboxError;

    async fn handle(
        &self,
        event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        let client = self
            .audit_pool
            .get()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        let payload = serde_json::to_string(&event)?;
        client
            .execute(
                "INSERT INTO audit_log (event_type, payload) VALUES ($1, $2::jsonb)",
                &[&UserRegistered::EVENT_TYPE, &payload],
            )
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,hexeract_outbox=debug")),
        )
        .with_target(true)
        .init();

    tracing::info!("starting Outbox two-database isolation example");

    let ops_container = Postgres::default()
        .start()
        .await
        .map_err(|e| format!("failed to start ops container: {e}"))?;
    let audit_container = Postgres::default()
        .start()
        .await
        .map_err(|e| format!("failed to start audit container: {e}"))?;

    let ops_pool = make_pool(&ops_container).await?;
    let audit_pool = make_pool(&audit_container).await?;

    ensure_schema(&ops_pool, OUTBOX_TABLE).await?;
    create_audit_table(&audit_pool).await?;

    tracing::info!("both databases initialised");

    let publisher = PgOutboxPublisher::new(ops_pool.clone(), OUTBOX_TABLE)?;

    let cancel = CancellationToken::new();
    let worker = PgOutboxWorkerBuilder::new(ops_pool.clone())
        .table_name(OUTBOX_TABLE)
        .register_handler::<UserRegistered, _>(AuditWriter {
            audit_pool: audit_pool.clone(),
        })
        .poll_interval(Duration::from_millis(50))
        .build()?;
    let worker_handle = tokio::spawn(worker.run(cancel.clone()));

    let event_id = Uuid::new_v4();
    let event = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "alice@example.com".to_owned(),
    };

    let start = Instant::now();
    tracing::info!(event_id = %event_id, "publishing UserRegistered event in business transaction");
    {
        let mut client = ops_pool.get().await?;
        let mut tx = client.transaction().await?;
        publisher.publish_in_tx(&mut tx, event_id, &event).await?;
        tx.commit().await?;
    }

    tracing::info!("event committed, waiting for worker dispatch");

    wait_for_audit_row(&audit_pool, start).await?;

    cancel.cancel();
    worker_handle.await??;

    tracing::info!("example completed successfully");
    Ok(())
}

async fn make_pool(container: &ContainerAsync<Postgres>) -> Result<Pool, Box<dyn Error>> {
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432).await?;

    let mut cfg = Config::new();
    cfg.host = Some(host);
    cfg.port = Some(port);
    cfg.user = Some("postgres".to_owned());
    cfg.password = Some("postgres".to_owned());
    cfg.dbname = Some("postgres".to_owned());

    Ok(cfg.create_pool(Some(Runtime::Tokio1), NoTls)?)
}

async fn create_audit_table(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await?;
    client
        .batch_execute(
            "CREATE TABLE audit_log (
                 id         BIGSERIAL    PRIMARY KEY,
                 event_type VARCHAR(64)  NOT NULL,
                 payload    JSONB        NOT NULL,
                 written_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
             )",
        )
        .await?;
    Ok(())
}

async fn wait_for_audit_row(audit_pool: &Pool, start: Instant) -> Result<(), Box<dyn Error>> {
    let deadline = start + DELIVERY_DEADLINE;
    loop {
        let client = audit_pool.get().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM audit_log", &[])
            .await?;
        let count: i64 = row.get(0);
        if count >= 1 {
            let elapsed = start.elapsed();
            tracing::info!(
                elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                "event delivered to audit database"
            );
            if elapsed > DELIVERY_DEADLINE {
                return Err(format!(
                    "delivery latency {elapsed:?} exceeded target {DELIVERY_DEADLINE:?}"
                )
                .into());
            }
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(
                format!("event not delivered to audit_db within {DELIVERY_DEADLINE:?}").into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
