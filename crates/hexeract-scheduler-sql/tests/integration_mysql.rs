//! Integration tests for the MySQL backend of `hexeract-scheduler-sql`.
//!
//! These tests start a MySQL 8 container via `testcontainers` and are marked
//! `#[ignore]` so they run in the dedicated integration workflow.
//!
//! ```sh
//! cargo test -p hexeract-scheduler-sql --features mysql --test integration_mysql -- --ignored
//! ```
#![cfg(feature = "mysql")]

use std::time::Duration;
use std::time::SystemTime;

use hexeract_scheduler::ScheduleStore;
use hexeract_scheduler_sql::Dialect;
use hexeract_scheduler_sql::MySqlScheduleStore;
use hexeract_scheduler_sql::schema::schema_ddl;
use sqlx::MySqlPool;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;
use uuid::Uuid;

mod common;

const TABLE: &str = "scheduled_messages";

async fn setup() -> (ContainerAsync<Mysql>, MySqlScheduleStore) {
    let container = Mysql::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let url = format!("mysql://root@{host}:{port}/test");
    let pool = MySqlPool::connect(&url).await.expect("connect");
    let ddl = schema_ddl(Dialect::MySql, TABLE).expect("schema ddl");
    sqlx::raw_sql(&ddl)
        .execute(&pool)
        .await
        .expect("schema apply");
    let store = MySqlScheduleStore::new(pool, TABLE).expect("store");
    (container, store)
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
);

/// Two workers claiming concurrently partition the due occurrences via the
/// internal `FOR UPDATE SKIP LOCKED` transaction: every occurrence is claimed
/// exactly once even without `UPDATE ... RETURNING`.
#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn concurrent_claims_dispatch_each_occurrence_once() {
    let (_guard, store) = setup().await;
    let count = 20usize;
    let mut expected = common::insert_due_batch(&store, count).await;
    expected.sort();

    let worker_a = store.clone();
    let worker_b = store.clone();
    let lease = Duration::from_secs(30);
    let (claimed_a, claimed_b) = tokio::join!(
        async move {
            worker_a
                .claim_due(SystemTime::now(), count, lease)
                .await
                .expect("claim a")
        },
        async move {
            worker_b
                .claim_due(SystemTime::now(), count, lease)
                .await
                .expect("claim b")
        },
    );

    let mut claimed: Vec<Uuid> = claimed_a
        .iter()
        .chain(claimed_b.iter())
        .map(|occurrence| occurrence.message.schedule_id)
        .collect();
    claimed.sort();
    let mut deduped = claimed.clone();
    deduped.dedup();
    assert_eq!(deduped.len(), claimed.len(), "no occurrence claimed twice");
    assert_eq!(claimed, expected, "every occurrence claimed exactly once");
}
