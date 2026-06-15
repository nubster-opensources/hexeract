//! Integration tests for the SQLite backend of `hexeract-scheduler-sql`.
//!
//! These tests use a temporary file database (no container needed) and are
//! marked `#[ignore]` so they run in the dedicated integration workflow.
//!
//! ```sh
//! cargo test -p hexeract-scheduler-sql --features sqlite --test integration_sqlite -- --ignored
//! ```
#![cfg(feature = "sqlite")]

use hexeract_scheduler_sql::Dialect;
use hexeract_scheduler_sql::SqliteScheduleStore;
use hexeract_scheduler_sql::schema::schema_ddl;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use tempfile::NamedTempFile;

mod common;

const TABLE: &str = "scheduled_messages";

async fn setup() -> (NamedTempFile, SqliteScheduleStore) {
    let file = NamedTempFile::new().expect("temp file");
    let options = SqliteConnectOptions::new()
        .filename(file.path())
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(options).await.expect("connect");
    let ddl = schema_ddl(Dialect::Sqlite, TABLE).expect("schema ddl");
    sqlx::raw_sql(&ddl)
        .execute(&pool)
        .await
        .expect("schema apply");
    let store = SqliteScheduleStore::new(pool, TABLE).expect("store");
    (file, store)
}

macro_rules! backend_scenarios {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            #[ignore = "runs in the integration workflow"]
            async fn $name() {
                let (_guard, store) = setup().await;
                common::$name(&store).await;
            }
        )*
    };
}

backend_scenarios!(
    insert_then_inspect_reports_pending,
    claim_increments_then_excludes_active_lease,
    expired_lease_reclaimed_exactly_once,
    excludes_not_yet_due,
    reschedule_advances_resets_and_reclaims,
    cancel_excludes_and_rejects_unknown,
    pause_excludes_then_resume_reenables,
    dead_letter_excludes_and_records_error,
    mark_delivered_excludes,
    mark_failed_defers_reclaim_until_retry_at,
);
