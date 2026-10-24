//! Integration tests for the MySQL backend of `hexeract-outbox-sql`.
//!
//! These tests start a MySQL 8 container via `testcontainers` and are marked
//! `#[ignore]` so they run in the dedicated integration workflow.
//!
//! ```sh
//! cargo test -p hexeract-outbox-sql --features mysql --test integration_mysql -- --ignored
//! ```
//!
//! Every scenario lives in [`common`]: MySQL expresses the whole shared
//! contract, competing-consumer scenario included.
#![cfg(feature = "mysql")]

mod common;

use common::Backend;
use common::TABLE;
use common::UserRegistered;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxStore;
use hexeract_outbox_sql::MySqlOutboxPublisher;
use hexeract_outbox_sql::MySqlOutboxStore;
use hexeract_outbox_sql::mysql::ensure_dead_letter_schema;
use hexeract_outbox_sql::mysql::ensure_schema;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;
use uuid::Uuid;

/// A MySQL container, kept alive for the duration of a scenario.
struct MySqlBackend {
    _container: ContainerAsync<Mysql>,
    pool: MySqlPool,
}

async fn setup() -> MySqlBackend {
    let container = Mysql::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("container port");
    let url = format!("mysql://root@{host}:{port}/test");
    let pool = MySqlPool::connect(&url).await.expect("connect");
    ensure_schema(&pool, TABLE).await.expect("schema apply");
    ensure_dead_letter_schema(&pool, TABLE)
        .await
        .expect("dead-letter schema apply");
    MySqlBackend {
        _container: container,
        pool,
    }
}

impl Backend for MySqlBackend {
    type Store = MySqlOutboxStore;
    type Publisher = MySqlOutboxPublisher;

    fn store(&self) -> Self::Store {
        MySqlOutboxStore::new(self.pool.clone(), TABLE).expect("store")
    }

    fn publisher(&self) -> Self::Publisher {
        MySqlOutboxPublisher::new(self.pool.clone(), TABLE).expect("publisher")
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
        // `attempts` is an INT, which sqlx decodes as `i32` on MySQL.
        let attempts: i32 =
            sqlx::query_scalar("SELECT attempts FROM audit_outbox WHERE event_id = ?")
                .bind(event_id)
                .fetch_one(&self.pool)
                .await
                .expect("attempts");
        i64::from(attempts)
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
        // The poll compares against `UTC_TIMESTAMP`, so the deferral is
        // anchored to the same clock rather than to the session time zone.
        sqlx::query(
            "UPDATE audit_outbox \
             SET attempts = 1, next_retry_at = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 1 DAY) \
             WHERE event_id = ?",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await
        .expect("defer retry");
    }

    fn store_with_dead_letter(&self) -> Self::Store {
        MySqlOutboxStore::new(self.pool.clone(), TABLE)
            .expect("store")
            .with_dead_letter("audit_outbox_dead_letter")
            .expect("dead-letter store")
    }

    async fn dead_letter_count(&self, event_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox_dead_letter WHERE event_id = ?")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .expect("dead letter count")
    }

    async fn last_error(&self, event_id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT last_error FROM audit_outbox WHERE event_id = ?")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .expect("last error")
    }

    async fn is_retry_deferred(&self, event_id: Uuid) -> bool {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_outbox              WHERE event_id = ? AND next_retry_at > UTC_TIMESTAMP(6)",
        )
        .bind(event_id)
        .fetch_one(&self.pool)
        .await
        .expect("deferred probe");
        count == 1
    }
}

macro_rules! backend_scenarios {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            #[ignore = "runs in the integration workflow"]
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
    claim_serves_only_one_of_two_claimants_that_read_the_same_attempts,
    stale_delivery_is_refused_and_leaves_the_current_owner_outcome_intact,
    stale_failure_is_refused_and_leaves_the_current_owner_outcome_intact,
    stale_dead_lettering_is_refused_and_leaves_the_row_in_place,
    competing_workers_dispatch_each_event_once,
    fence_beyond_the_integer_column_is_an_error_not_a_saturation,
);

/// Criterion 10. `mark_failed` called twice with the same error and the same
/// database instant changes no column the second time, yet the lease is valid.
///
/// MySQL reports *changed* rows by default and *matched* rows only when the
/// client sets `CLIENT_FOUND_ROWS`. If the driver counted changed rows, the
/// second call would report `false` and the worker would log a lost lease that
/// never happened. The database clock is frozen on a single connection so the
/// computed `next_retry_at` is identical across both calls.
#[tokio::test]
#[ignore = "runs in the integration workflow"]
async fn mark_failed_with_identical_values_under_a_valid_lease_still_reports_applied() {
    let backend = setup().await;
    let event_id = backend
        .publisher()
        .publish(&common::sample("identical@example.com"))
        .await
        .expect("publish");

    let options = (*backend.pool.connect_options()).clone();
    let frozen_pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("single connection pool");
    sqlx::query("SET timestamp = 1700000000.123456")
        .execute(&frozen_pool)
        .await
        .expect("freeze the session clock");
    let store = MySqlOutboxStore::new(frozen_pool, TABLE).expect("store");

    let mut client = store.acquire().await.expect("acquire");
    let mut tx = store.begin(&mut client).await.expect("begin");
    let claimed = store
        .claim(&mut tx, &[(event_id, 0)], Duration::from_secs(30))
        .await
        .expect("claim");
    store.commit(tx).await.expect("commit");
    assert_eq!(claimed, vec![event_id]);

    for call in ["first", "second"] {
        let mut client = store.acquire().await.expect("acquire");
        let mut tx = store.begin(&mut client).await.expect("begin");
        let applied = store
            .mark_failed(&mut tx, event_id, "same error", Duration::from_secs(60), 1)
            .await
            .expect("mark_failed must not be an error");
        store.commit(tx).await.expect("commit");
        assert!(
            applied,
            "the {call} mark_failed matched the row under a valid lease and must report true"
        );
    }
}
