//! Integration tests for `hexeract-outbox-postgres`.
//!
//! # Running
//!
//! These tests are marked `#[ignore]` by default because they start a
//! PostgreSQL container via `testcontainers` and therefore require a
//! running Docker daemon.
//!
//! ```sh
//! cargo test -p hexeract-outbox-postgres -- --ignored
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use deadpool_postgres::Config;
use deadpool_postgres::Pool;
use deadpool_postgres::Runtime;
use hexeract_core::HandlerContext;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;
use hexeract_outbox_postgres::PgOutboxPublisher;
use hexeract_outbox_postgres::PgOutboxStore;
use hexeract_outbox_postgres::ensure_schema;
use serde::Deserialize;
use serde::Serialize;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct UserRegistered {
    user_id: Uuid,
    email: String,
}

impl Event for UserRegistered {
    const EVENT_TYPE: &'static str = "users.registered";
}

const TABLE: &str = "audit_outbox";

async fn setup() -> (ContainerAsync<Postgres>, Pool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let mut cfg = Config::new();
    cfg.host = Some(host);
    cfg.port = Some(port);
    cfg.user = Some("postgres".to_owned());
    cfg.password = Some("postgres".to_owned());
    cfg.dbname = Some("postgres".to_owned());

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    ensure_schema(&pool, TABLE).await.expect("schema apply");
    (container, pool)
}

async fn count_pending(pool: &Pool, event_id: Uuid) -> i64 {
    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM audit_outbox WHERE event_id = $1 AND delivered_at IS NULL",
            &[&event_id],
        )
        .await
        .unwrap();
    row.get(0)
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn ensure_schema_creates_columns_and_indexes() {
    let (_container, pool) = setup().await;
    let client = pool.get().await.unwrap();
    let rows = client
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = $1 ORDER BY ordinal_position",
            &[&TABLE],
        )
        .await
        .unwrap();
    let columns: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    for expected in [
        "id",
        "event_id",
        "event_type",
        "payload",
        "subject_id",
        "created_at",
        "attempts",
        "last_error",
        "next_retry_at",
        "delivered_at",
    ] {
        assert!(
            columns.iter().any(|c| c == expected),
            "missing column `{expected}` in {columns:?}"
        );
    }

    let indexes = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE tablename = $1",
            &[&TABLE],
        )
        .await
        .unwrap();
    let names: Vec<String> = indexes.iter().map(|r| r.get(0)).collect();
    assert!(names.iter().any(|n| n == "idx_audit_outbox_pending"));
    assert!(names.iter().any(|n| n == "idx_audit_outbox_subject"));
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn publish_in_tx_inserts_a_row_in_the_calling_transaction() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "alice@example.com".to_owned(),
    };
    let event_id = Uuid::new_v4();

    let mut client = pool.get().await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    publisher
        .publish_in_tx(&mut tx, event_id, &event)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT event_type, payload::text, subject_id FROM audit_outbox WHERE event_id = $1",
            &[&event_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "users.registered");
    let payload: String = row.get(1);
    assert!(payload.contains("\"user_id\""));
    assert!(payload.contains("alice@example.com"));
    let subject: Option<Uuid> = row.get(2);
    assert!(subject.is_none());
    assert_eq!(count_pending(&pool, event_id).await, 1);
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn publish_in_tx_with_subject_records_subject_id() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "bob@example.com".to_owned(),
    };
    let event_id = Uuid::new_v4();
    let subject = Uuid::from_u128(424_242);

    let mut client = pool.get().await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    publisher
        .publish_in_tx_with_subject(&mut tx, event_id, subject, &event)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let client = pool.get().await.unwrap();
    let stored_subject: Option<Uuid> = client
        .query_one(
            "SELECT subject_id FROM audit_outbox WHERE event_id = $1",
            &[&event_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_subject, Some(subject));
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn publish_without_tx_inserts_and_commits() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "carol@example.com".to_owned(),
    };
    let event_id = Uuid::new_v4();
    publisher.publish(event_id, &event).await.unwrap();

    assert_eq!(count_pending(&pool, event_id).await, 1);
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn rollback_in_business_tx_discards_the_outbox_insert() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "dave@example.com".to_owned(),
    };
    let event_id = Uuid::new_v4();

    let mut client = pool.get().await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    publisher
        .publish_in_tx(&mut tx, event_id, &event)
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let client = pool.get().await.unwrap();
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM audit_outbox WHERE event_id = $1",
            &[&event_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
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

async fn delivered_count(pool: &Pool, event_id: Uuid) -> i64 {
    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM audit_outbox WHERE event_id = $1 AND delivered_at IS NOT NULL",
            &[&event_id],
        )
        .await
        .unwrap();
    row.get(0)
}

async fn attempts_for(pool: &Pool, event_id: Uuid) -> i32 {
    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT attempts FROM audit_outbox WHERE event_id = $1",
            &[&event_id],
        )
        .await
        .unwrap();
    row.get(0)
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn worker_dispatches_published_event_and_marks_delivered() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = PgOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = Uuid::new_v4();
    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "alice@example.com".to_owned(),
    };
    publisher.publish(event_id, &event).await.unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handlers = registry_with(RecordingHandler {
        seen: Arc::clone(&seen),
    });
    let worker = OutboxWorker::new(store, handlers, OutboxWorkerConfig::default());
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(delivered_count(&pool, event_id).await, 1);
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn worker_marks_failed_and_increments_attempts_on_handler_error() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = PgOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = Uuid::new_v4();
    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "bob@example.com".to_owned(),
    };
    publisher.publish(event_id, &event).await.unwrap();

    let handlers = registry_with(FailingHandler);
    let config = OutboxWorkerConfig {
        poll_interval: Duration::from_millis(20),
        retry_delay: Duration::from_secs(60),
        max_attempts: 5,
        batch_size: 10,
    };
    let worker = OutboxWorker::new(store, handlers, config);
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(delivered_count(&pool, event_id).await, 0);
    assert!(attempts_for(&pool, event_id).await >= 1);
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn worker_excludes_events_past_max_attempts() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();
    let store = PgOutboxStore::new(pool.clone(), TABLE).unwrap();

    let event_id = Uuid::new_v4();
    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "carol@example.com".to_owned(),
    };
    publisher.publish(event_id, &event).await.unwrap();

    let client = pool.get().await.unwrap();
    client
        .execute(
            "UPDATE audit_outbox SET attempts = 5 WHERE event_id = $1",
            &[&event_id],
        )
        .await
        .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handlers = registry_with(RecordingHandler {
        seen: Arc::clone(&seen),
    });
    let config = OutboxWorkerConfig {
        max_attempts: 5,
        ..OutboxWorkerConfig::default()
    };
    let worker = OutboxWorker::new(store, handlers, config);
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel();
    join.await.unwrap().unwrap();

    assert_eq!(seen.lock().unwrap().len(), 0);
    assert_eq!(delivered_count(&pool, event_id).await, 0);
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn multi_worker_skip_locked_prevents_double_dispatch() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event_count = 20;
    let mut ids = Vec::with_capacity(event_count);
    for i in 0..event_count {
        let event_id = Uuid::new_v4();
        let event = UserRegistered {
            user_id: Uuid::from_u128(i as u128),
            email: format!("user{i}@example.com"),
        };
        publisher.publish(event_id, &event).await.unwrap();
        ids.push(event_id);
    }

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));

    let store_a = PgOutboxStore::new(pool.clone(), TABLE).unwrap();
    let store_b = PgOutboxStore::new(pool.clone(), TABLE).unwrap();
    let worker_a = OutboxWorker::new(
        store_a,
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen_a),
        }),
        OutboxWorkerConfig {
            poll_interval: Duration::from_millis(20),
            batch_size: 5,
            ..OutboxWorkerConfig::default()
        },
    );
    let worker_b = OutboxWorker::new(
        store_b,
        registry_with(RecordingHandler {
            seen: Arc::clone(&seen_b),
        }),
        OutboxWorkerConfig {
            poll_interval: Duration::from_millis(20),
            batch_size: 5,
            ..OutboxWorkerConfig::default()
        },
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
        total,
        event_count,
        "expected {event_count} unique dispatches, got {total} (a={}, b={})",
        seen_a.lock().unwrap().len(),
        seen_b.lock().unwrap().len()
    );

    let client = pool.get().await.unwrap();
    let delivered: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM audit_outbox WHERE delivered_at IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(delivered, i64::try_from(event_count).unwrap());
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn worker_stops_gracefully_on_cancellation() {
    let (_container, pool) = setup().await;
    let store = PgOutboxStore::new(pool, TABLE).unwrap();
    let worker = OutboxWorker::new(store, HashMap::new(), OutboxWorkerConfig::default());

    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    cancel.cancel();
    let started = std::time::Instant::now();
    join.await.unwrap().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "worker took {:?} to stop",
        started.elapsed()
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn duplicate_event_id_is_rejected_by_unique_constraint() {
    let (_container, pool) = setup().await;
    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    let event = UserRegistered {
        user_id: Uuid::nil(),
        email: "erin@example.com".to_owned(),
    };
    let event_id = Uuid::new_v4();

    publisher.publish(event_id, &event).await.unwrap();
    let err = publisher.publish(event_id, &event).await.unwrap_err();
    assert!(matches!(err, hexeract_outbox::OutboxError::Database(_)));
}
