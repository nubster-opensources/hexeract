//! Integration tests for the SQLite backend of `hexeract-outbox-sql`.
//!
//! These tests use a temporary file database (no container needed) and are
//! marked `#[ignore]` so they run in the dedicated integration workflow.
//!
//! ```sh
//! cargo test -p hexeract-outbox-sql --features sqlite --test integration_sqlite -- --ignored
//! ```
#![cfg(feature = "sqlite")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use hexeract_core::HandlerContext;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::IdempotentOutboxEnqueue;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;
use hexeract_outbox_sql::SqliteOutboxPublisher;
use hexeract_outbox_sql::SqliteOutboxStore;
use hexeract_outbox_sql::sqlite::ensure_schema;
use serde::Deserialize;
use serde::Serialize;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use tempfile::NamedTempFile;
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

async fn setup() -> (NamedTempFile, SqlitePool) {
    let file = NamedTempFile::new().expect("temp file");
    let options = SqliteConnectOptions::new()
        .filename(file.path())
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(options).await.expect("connect");
    ensure_schema(&pool, TABLE).await.expect("schema apply");
    (file, pool)
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

async fn delivered_count(pool: &SqlitePool, event_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_outbox WHERE event_id = ? AND delivered_at IS NOT NULL",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn publish_in_tx_rollback_discards_the_insert() {
    let (_file, pool) = setup().await;
    let publisher = SqliteOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let mut tx = pool.begin().await.unwrap();
    let event_id = publisher
        .publish_in_tx(&mut tx, &sample("rollback@example.com"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox WHERE event_id = ?")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn worker_dispatches_published_event_and_marks_delivered() {
    let (_file, pool) = setup().await;
    let publisher = SqliteOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = SqliteOutboxStore::new(pool.clone(), TABLE).unwrap();

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

    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(delivered_count(&pool, event_id).await, 1);
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn worker_marks_failed_and_increments_attempts_on_handler_error() {
    let (_file, pool) = setup().await;
    let publisher = SqliteOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = SqliteOutboxStore::new(pool.clone(), TABLE).unwrap();

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

    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(delivered_count(&pool, event_id).await, 0);
    let attempts: i64 = sqlx::query_scalar("SELECT attempts FROM audit_outbox WHERE event_id = ?")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        attempts >= 1,
        "attempts should be incremented, got {attempts}"
    );
    let next_retry: Option<String> =
        sqlx::query_scalar("SELECT next_retry_at FROM audit_outbox WHERE event_id = ?")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        next_retry.is_some(),
        "next_retry_at should be set after a failure"
    );
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn future_next_retry_at_excludes_event_from_poll() {
    let (_file, pool) = setup().await;
    let publisher = SqliteOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = SqliteOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = publisher
        .publish(&sample("carol@example.com"))
        .await
        .unwrap();

    sqlx::query(
        "UPDATE audit_outbox \
         SET attempts = 1, next_retry_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+1 day') \
         WHERE event_id = ?",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = OutboxWorker::new(
        store,
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen),
        }),
        OutboxWorkerConfig {
            poll_interval: Duration::from_millis(20),
            ..OutboxWorkerConfig::default()
        },
    );
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(
        seen.lock().unwrap().len(),
        0,
        "event scheduled in the future must not be dispatched"
    );
    assert_eq!(delivered_count(&pool, event_id).await, 0);
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn undecodable_row_is_skipped_and_the_rest_of_the_batch_drains() {
    // #214: a row whose created_at is in the SQLite canonical datetime('now')
    // form (space separator, no millis) used to be parseable, but a truly
    // garbage timestamp must not abort the whole poll. Insert one poison row
    // ahead of a valid one and assert the valid one is still delivered.
    let (_file, pool) = setup().await;
    let publisher = SqliteOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = SqliteOutboxStore::new(pool.clone(), TABLE).unwrap();

    // Poison row: an unparseable created_at. event_id is a valid blob so the
    // skip path can still log its id.
    let poison_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO audit_outbox (event_id, event_type, payload, created_at) \
         VALUES (?, ?, ?, ?)",
    )
    .bind(poison_id)
    .bind(UserRegistered::EVENT_TYPE)
    .bind("{\"user_id\":\"00000000-0000-0000-0000-000000000000\",\"email\":\"x\"}")
    .bind("totally-not-a-timestamp")
    .execute(&pool)
    .await
    .unwrap();

    let good_id = publisher
        .publish(&sample("dora@example.com"))
        .await
        .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = OutboxWorker::new(
        store,
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen),
        }),
        OutboxWorkerConfig {
            poll_interval: Duration::from_millis(20),
            ..OutboxWorkerConfig::default()
        },
    );
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(
        delivered_count(&pool, good_id).await,
        1,
        "the valid row behind the poison row must still be delivered"
    );
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn canonical_datetime_now_created_at_is_accepted() {
    // #214: rows written with the SQLite native datetime('now') form (space
    // separator, no fractional seconds) must be polled, not rejected.
    let (_file, pool) = setup().await;
    let store = SqliteOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO audit_outbox (event_id, event_type, payload, created_at) \
         VALUES (?, ?, ?, datetime('now'))",
    )
    .bind(event_id)
    .bind(UserRegistered::EVENT_TYPE)
    .bind("{\"user_id\":\"00000000-0000-0000-0000-000000000000\",\"email\":\"y\"}")
    .execute(&pool)
    .await
    .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = OutboxWorker::new(
        store,
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen),
        }),
        OutboxWorkerConfig {
            poll_interval: Duration::from_millis(20),
            ..OutboxWorkerConfig::default()
        },
    );
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(
        delivered_count(&pool, event_id).await,
        1,
        "a datetime('now') created_at must be parseable and the row delivered"
    );
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn enqueue_idempotent_twice_inserts_one_row() {
    let (_file, pool) = setup().await;
    let publisher = SqliteOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event_id = Uuid::now_v7();
    let inserted = publisher
        .enqueue_idempotent(event_id, "x.due", b"{\"k\":1}")
        .await
        .unwrap();
    assert!(inserted, "first enqueue must insert a new row");

    let duplicate = publisher
        .enqueue_idempotent(event_id, "x.due", b"{\"k\":1}")
        .await
        .unwrap();
    assert!(
        !duplicate,
        "second enqueue with same event_id must be a no-op"
    );

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox WHERE event_id = ?")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "exactly one row must exist after two enqueues");
}

#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn claim_consumes_a_retry_slot_even_without_a_clean_failure() {
    // #213: claiming a batch must increment attempts. We simulate a crash
    // between claim and acknowledgement by claiming directly (no dispatch) and
    // asserting attempts advanced from 0 to 1.
    use hexeract_outbox::OutboxStore;

    let (_file, pool) = setup().await;
    let publisher = SqliteOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = SqliteOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = publisher
        .publish(&sample("erin@example.com"))
        .await
        .unwrap();

    let mut client = store.acquire().await.unwrap();
    let mut tx = store.begin(&mut client).await.unwrap();
    store
        .claim(&mut tx, &[event_id], Duration::from_secs(30))
        .await
        .unwrap();
    store.commit(tx).await.unwrap();

    let attempts: i64 = sqlx::query_scalar("SELECT attempts FROM audit_outbox WHERE event_id = ?")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        attempts, 1,
        "claim alone must consume one retry slot (crash safety, #213)"
    );
}
