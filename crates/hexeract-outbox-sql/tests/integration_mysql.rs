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
//! contract, plus the competing-consumer scenario that needs
//! `FOR UPDATE SKIP LOCKED`.
#![cfg(feature = "mysql")]

mod common;

use common::Backend;
use common::TABLE;
use common::UserRegistered;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox_sql::MySqlOutboxPublisher;
use hexeract_outbox_sql::MySqlOutboxStore;
use hexeract_outbox_sql::mysql::ensure_schema;
use sqlx::MySqlPool;
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
    multi_worker_skip_locked_prevents_double_dispatch,
);
