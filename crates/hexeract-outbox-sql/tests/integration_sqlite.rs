//! Integration tests for the SQLite backend of `hexeract-outbox-sql`.
//!
//! These tests use a temporary file database, so they need no container and
//! run in the regular test job on every supported operating system.
//!
//! ```sh
//! cargo test -p hexeract-outbox-sql --features sqlite --test integration_sqlite
//! ```
//!
//! Behaviour shared with the other dialects lives in [`common`]. Only what
//! SQLite alone can express stays here.
#![cfg(feature = "sqlite")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use common::Backend;
use common::TABLE;
use common::UserRegistered;
use hexeract_core::HandlerContext;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;
use hexeract_outbox_sql::SqliteOutboxPublisher;
use hexeract_outbox_sql::SqliteOutboxStore;
use hexeract_outbox_sql::sqlite::ensure_schema;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// A temporary SQLite database, kept alive for the duration of a scenario.
struct SqliteBackend {
    _file: NamedTempFile,
    pool: SqlitePool,
}

async fn setup() -> SqliteBackend {
    let file = NamedTempFile::new().expect("temp file");
    let options = SqliteConnectOptions::new()
        .filename(file.path())
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(options).await.expect("connect");
    ensure_schema(&pool, TABLE).await.expect("schema apply");
    SqliteBackend { _file: file, pool }
}

impl Backend for SqliteBackend {
    type Store = SqliteOutboxStore;
    type Publisher = SqliteOutboxPublisher;

    fn store(&self) -> Self::Store {
        SqliteOutboxStore::new(self.pool.clone(), TABLE).expect("store")
    }

    fn publisher(&self) -> Self::Publisher {
        SqliteOutboxPublisher::new(self.pool.clone(), TABLE).expect("publisher")
    }

    async fn publish_then_rollback(&self, event: &UserRegistered) -> Uuid {
        let publisher = self.publisher();
        let mut tx = self.pool.begin().await.expect("begin");
        let event_id = publisher
            .publish_in_tx(&mut tx, event)
            .await
            .expect("publish in tx");
        tx.rollback().await.expect("rollback");
        event_id
    }

    async fn row_count(&self, event_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox WHERE event_id = ?")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .expect("row count")
    }

    async fn delivered_count(&self, event_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_outbox \
             WHERE event_id = ? AND delivered_at IS NOT NULL",
        )
        .bind(event_id)
        .fetch_one(&self.pool)
        .await
        .expect("delivered count")
    }

    async fn total_delivered(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox WHERE delivered_at IS NOT NULL")
            .fetch_one(&self.pool)
            .await
            .expect("total delivered")
    }

    async fn attempts(&self, event_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT attempts FROM audit_outbox WHERE event_id = ?")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .expect("attempts")
    }

    async fn has_next_retry_at(&self, event_id: Uuid) -> bool {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_outbox \
             WHERE event_id = ? AND next_retry_at IS NOT NULL",
        )
        .bind(event_id)
        .fetch_one(&self.pool)
        .await
        .expect("next retry probe");
        count == 1
    }

    async fn defer_next_retry_by_one_day(&self, event_id: Uuid) {
        sqlx::query(
            "UPDATE audit_outbox \
             SET attempts = 1, next_retry_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+1 day') \
             WHERE event_id = ?",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await
        .expect("defer retry");
    }
}

macro_rules! backend_scenarios {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let backend = setup().await;
                common::$name(&backend).await;
            }
        )*
    };
}

backend_scenarios!(
    publish_in_tx_rollback_discards_the_insert,
    worker_dispatches_published_event_and_marks_delivered,
    worker_marks_failed_and_increments_attempts_on_handler_error,
    future_next_retry_at_excludes_event_from_poll,
    enqueue_idempotent_twice_inserts_one_row,
    idempotent_enqueue_delivered_once_by_worker,
    claim_consumes_a_retry_slot_even_without_a_clean_failure,
);

// The two scenarios below have no equivalent on Postgres or MySQL: both store
// timestamps in native column types, so neither an unparseable `created_at`
// nor the SQLite-specific `datetime('now')` form can ever reach the store.

/// Handler recording the addresses it saw, local to the SQLite-only scenarios.
#[derive(Clone, Default)]
struct Recorder {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Handler<UserRegistered> for Recorder {
    type Error = OutboxError;

    async fn handle(
        &self,
        event: UserRegistered,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.seen.lock().expect("recorder mutex").push(event.email);
        Ok(())
    }
}

/// Run a worker until `probe` reports true, then stop it.
///
/// # Panics
///
/// Panics when `probe` never reports true, naming `expectation`.
async fn drain_until<P, F>(backend: &SqliteBackend, expectation: &str, mut probe: P)
where
    P: FnMut() -> F,
    F: std::future::Future<Output = bool>,
{
    let mut registry = HashMap::new();
    let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler::new(Recorder::default()));
    registry.insert(erased.event_type(), erased);

    let worker = OutboxWorker::new(
        backend.store(),
        registry,
        OutboxWorkerConfig {
            poll_interval: Duration::from_millis(20),
            ..OutboxWorkerConfig::default()
        },
    );
    let cancel = CancellationToken::new();
    let join = tokio::spawn(worker.run(cancel.clone()));

    let wait = async {
        loop {
            if probe().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let outcome = tokio::time::timeout(Duration::from_secs(10), wait).await;

    cancel.cancel();
    join.await.expect("worker task").expect("worker run");
    assert!(outcome.is_ok(), "timed out waiting for {expectation}");
}

#[tokio::test]
async fn undecodable_row_is_skipped_and_the_rest_of_the_batch_drains() {
    // #214: a truly garbage timestamp must not abort the whole poll. Only
    // SQLite can hold one, since it stores timestamps as TEXT. Insert a poison
    // row ahead of a valid one and assert the valid one is still delivered.
    let backend = setup().await;

    // The event_id stays a valid blob so the skip path can log its identifier.
    let poison_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO audit_outbox (event_id, event_type, payload, created_at) \
         VALUES (?, ?, ?, ?)",
    )
    .bind(poison_id)
    .bind(UserRegistered::EVENT_TYPE)
    .bind("{\"user_id\":\"00000000-0000-0000-0000-000000000000\",\"email\":\"x\"}")
    .bind("totally-not-a-timestamp")
    .execute(&backend.pool)
    .await
    .expect("poison row insert");

    let good_id = backend
        .publisher()
        .publish(&UserRegistered {
            user_id: Uuid::now_v7(),
            email: "dora@example.com".to_owned(),
        })
        .await
        .expect("publish");

    drain_until(
        &backend,
        "the row behind the poison row to drain",
        || async { backend.delivered_count(good_id).await == 1 },
    )
    .await;

    assert_eq!(
        backend.delivered_count(poison_id).await,
        0,
        "the poison row must stay undelivered"
    );
}

#[tokio::test]
async fn canonical_datetime_now_created_at_is_accepted() {
    // #214: rows written with the SQLite native datetime('now') form (space
    // separator, no fractional seconds) must be polled, not rejected.
    let backend = setup().await;

    let event_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO audit_outbox (event_id, event_type, payload, created_at) \
         VALUES (?, ?, ?, datetime('now'))",
    )
    .bind(event_id)
    .bind(UserRegistered::EVENT_TYPE)
    .bind("{\"user_id\":\"00000000-0000-0000-0000-000000000000\",\"email\":\"y\"}")
    .execute(&backend.pool)
    .await
    .expect("row insert");

    drain_until(
        &backend,
        "a datetime('now') created_at to be parsed and delivered",
        || async { backend.delivered_count(event_id).await == 1 },
    )
    .await;
}
