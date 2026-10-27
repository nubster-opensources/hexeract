//! Integration tests for the Postgres backend of `hexeract-outbox-sql`.
//!
//! These tests start a Postgres container via `testcontainers` and are marked
//! `#[ignore]` so they run in the dedicated integration workflow.
//!
//! ```sh
//! cargo test -p hexeract-outbox-sql --features postgres --test integration_pg -- --ignored
//! ```
//!
//! Every scenario lives in [`common`]: Postgres expresses the whole shared
//! contract, competing-consumer scenario included.
#![cfg(feature = "postgres")]

mod common;

use common::Backend;
use common::TABLE;
use common::UserRegistered;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox_sql::PgOutboxPublisher;
use hexeract_outbox_sql::PgOutboxStore;
use hexeract_outbox_sql::postgres::ensure_dead_letter_schema;
use hexeract_outbox_sql::postgres::ensure_schema;
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

/// A Postgres container, kept alive for the duration of a scenario.
struct PgBackend {
    _container: ContainerAsync<Postgres>,
    pool: PgPool,
}

async fn setup() -> PgBackend {
    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    ensure_schema(&pool, TABLE).await.expect("schema apply");
    ensure_dead_letter_schema(&pool, TABLE)
        .await
        .expect("dead-letter schema apply");
    PgBackend {
        _container: container,
        pool,
    }
}

impl Backend for PgBackend {
    type Store = PgOutboxStore;
    type Publisher = PgOutboxPublisher;

    fn store(&self) -> Self::Store {
        PgOutboxStore::new(self.pool.clone(), TABLE).expect("store")
    }

    fn publisher(&self) -> Self::Publisher {
        PgOutboxPublisher::new(self.pool.clone(), TABLE).expect("publisher")
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
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .expect("row count")
    }

    async fn delivered_count(&self, event_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_outbox \
             WHERE event_id = $1 AND delivered_at IS NOT NULL",
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
        // `attempts` is an INTEGER, which sqlx decodes as `i32` on Postgres.
        let attempts: i32 =
            sqlx::query_scalar("SELECT attempts FROM audit_outbox WHERE event_id = $1")
                .bind(event_id)
                .fetch_one(&self.pool)
                .await
                .expect("attempts");
        i64::from(attempts)
    }

    async fn has_next_retry_at(&self, event_id: Uuid) -> bool {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_outbox \
             WHERE event_id = $1 AND next_retry_at IS NOT NULL",
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
             SET attempts = 1, next_retry_at = NOW() + INTERVAL '1 day' \
             WHERE event_id = $1",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await
        .expect("defer retry");
    }

    fn store_with_dead_letter(&self) -> Self::Store {
        PgOutboxStore::new(self.pool.clone(), TABLE)
            .expect("store")
            .with_dead_letter("audit_outbox_dead_letter")
            .expect("dead-letter store")
    }

    async fn dead_letter_count(&self, event_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_outbox_dead_letter WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .expect("dead letter count")
    }

    async fn last_error(&self, event_id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT last_error FROM audit_outbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .expect("last error")
    }

    async fn is_retry_deferred(&self, event_id: Uuid) -> bool {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_outbox              WHERE event_id = $1 AND next_retry_at > NOW()",
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
