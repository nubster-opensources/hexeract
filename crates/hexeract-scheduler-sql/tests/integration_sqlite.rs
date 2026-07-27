//! Integration tests for the SQLite backend of `hexeract-scheduler-sql`.
//!
//! These tests use a temporary file database, so they need no container and
//! run in the regular test job on every supported operating system.
//!
//! ```sh
//! cargo test -p hexeract-scheduler-sql --features sqlite --test integration_sqlite
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
    cancel_does_not_clobber_a_terminal_status,
    pause_excludes_then_resume_reenables,
    dead_letter_excludes_and_records_error,
    mark_delivered_excludes,
    mark_failed_defers_reclaim_until_retry_in_elapses,
    resume_realigns_paused_and_rejects_unknown,
    list_pending_orders_and_limits,
    list_dead_letter_reports_errors,
    list_dead_letter_orders_most_recently_dead_lettered_first,
    replay_requeues_dead_letter,
    replay_rejects_non_dead_lettered,
    ack_round_trips_the_claimed_lease,
    ack_with_a_stale_lease_is_rejected_after_reclaim,
    dead_letter_exhausted_sweeps_crash_exhausted_schedules,
);
