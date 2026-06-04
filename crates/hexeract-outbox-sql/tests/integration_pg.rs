//! Integration tests for the PostgreSQL backend of `hexeract-outbox-sql`.
//!
//! These tests start a PostgreSQL container via `testcontainers` and are
//! marked `#[ignore]` so they run in the dedicated integration workflow.
//!
//! ```sh
//! cargo test -p hexeract-outbox-sql --features postgres --test integration_pg -- --ignored
//! ```
#![cfg(feature = "postgres")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use hexeract_core::HandlerContext;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;
use hexeract_outbox_sql::PgOutboxPublisher;
use hexeract_outbox_sql::PgOutboxStore;
use hexeract_outbox_sql::postgres::ensure_schema;
use serde::Deserialize;
use serde::Serialize;
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TABLE: &str = "audit_outbox";

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct UserRegistered {
    user_id: Uuid,
    email: String,
}

impl Event for UserRegistered {
    const EVENT_TYPE: &'static str = "users.registered";
}

async fn setup() -> (ContainerAsync<Postgres>, PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    ensure_schema(&pool, TABLE).await.expect("schema apply");
    (container, pool)
}

struct RecordingHandler {
    seen: Arc<Mutex<Vec<UserRegistered>>>,
}

impl Handler<UserRegistered> for RecordingHandler {
    type Error = OutboxError;
    async fn handle(
        &self,
        event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.seen.lock().unwrap().push(event);
        Ok(())
    }
}

struct FailingHandler;
impl Handler<UserRegistered> for FailingHandler {
    type Error = OutboxError;
    async fn handle(
        &self,
        _event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        Err(OutboxError::Internal("forced failure".to_owned()))
    }
}

fn registry_with<H>(handler: H) -> HashMap<&'static str, Arc<dyn ErasedHandler>>
where
    H: Handler<UserRegistered>,
{
    let mut map = HashMap::new();
    let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(handler));
    map.insert(erased.event_type(), erased);
    map
}

fn sample(email: &str) -> UserRegistered {
    UserRegistered {
        user_id: Uuid::now_v7(),
        email: email.to_owned(),
    }
}

async fn delivered_count(pool: &PgPool, event_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_outbox WHERE event_id = $1 AND delivered_at IS NOT NULL",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn publish_in_tx_rollback_discards_the_insert() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let mut tx = pool.begin().await.unwrap();
    let event_id = publisher
        .publish_in_tx(&mut tx, &sample("rollback@example.com"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn worker_dispatches_published_event_and_marks_delivered() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = PgOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = publisher
        .publish(&sample("alice@example.com"))
        .await
        .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = OutboxWorker::new(
        store,
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen),
        }),
        OutboxWorkerConfig::default(),
    );
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(600)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(delivered_count(&pool, event_id).await, 1);
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn worker_marks_failed_and_increments_attempts_on_handler_error() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = PgOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = publisher.publish(&sample("bob@example.com")).await.unwrap();

    let config = OutboxWorkerConfig {
        poll_interval: Duration::from_millis(20),
        retry_base_delay: Duration::from_secs(60),
        retry_max_delay: Duration::from_secs(60),
        jitter: false,
        ..OutboxWorkerConfig::default()
    };
    let worker = OutboxWorker::new(store, registry_with(FailingHandler), config);
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(delivered_count(&pool, event_id).await, 0);
    let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM audit_outbox WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        attempts >= 1,
        "attempts should be incremented, got {attempts}"
    );
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn multi_worker_skip_locked_prevents_double_dispatch() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event_count = 20usize;
    for i in 0..event_count {
        publisher
            .publish(&sample(&format!("user{i}@example.com")))
            .await
            .unwrap();
    }

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));
    let cfg = || OutboxWorkerConfig {
        poll_interval: Duration::from_millis(20),
        batch_size: 5,
        ..OutboxWorkerConfig::default()
    };
    let worker_a = OutboxWorker::new(
        PgOutboxStore::new(pool.clone(), TABLE).unwrap(),
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen_a),
        }),
        cfg(),
    );
    let worker_b = OutboxWorker::new(
        PgOutboxStore::new(pool.clone(), TABLE).unwrap(),
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen_b),
        }),
        cfg(),
    );

    let cancel = CancellationToken::new();
    let ja = tokio::spawn(worker_a.run(cancel.clone()));
    let jb = tokio::spawn(worker_b.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(1500)).await;
    cancel.cancel();
    ja.await.unwrap().unwrap();
    jb.await.unwrap().unwrap();

    let total = seen_a.lock().unwrap().len() + seen_b.lock().unwrap().len();
    assert_eq!(
        total, event_count,
        "each event must be dispatched exactly once"
    );

    let delivered: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox WHERE delivered_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(delivered, i64::try_from(event_count).unwrap());
}
