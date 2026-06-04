use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::OutboxEnvelope;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxStore;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;
use sqlx::Acquire;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::pool::PoolConnection;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::DEFAULT_TABLE_NAME;
use crate::dialect::Dialect;
use crate::envelope::assemble_envelope;
use crate::envelope::to_offset;
use crate::envelope::to_system_time;
use crate::validate::validate_table_name;

const DIALECT: Dialect = Dialect::Postgres;

fn database_error(error: impl std::error::Error + Send + Sync + 'static) -> OutboxError {
    OutboxError::Database(Box::new(error))
}

fn pool_error(error: sqlx::Error) -> OutboxError {
    if matches!(error, sqlx::Error::PoolTimedOut) {
        OutboxError::PoolTimeout
    } else {
        OutboxError::Database(Box::new(error))
    }
}

#[derive(Debug, Clone)]
struct DeadLetterSql {
    insert_sql: Arc<str>,
    delete_sql: Arc<str>,
}

/// Apply the canonical Postgres outbox schema to the target database.
///
/// **Intended for POCs, integration tests and local development.**
/// Production deployments should run their own migration tooling against the
/// SQL rendered by [`Dialect::schema_ddl`]. Applying DDL from the running
/// application typically requires elevated privileges that the runtime role
/// should not own, and clashes with versioned migration workflows.
///
/// # Errors
///
/// - [`OutboxError::Internal`] if `table_name` is not a valid identifier.
/// - [`OutboxError::Database`] if the connection or the DDL statement fails.
pub async fn ensure_schema(pool: &PgPool, table_name: &str) -> Result<(), OutboxError> {
    let ddl = DIALECT.schema_ddl(table_name)?;
    sqlx::raw_sql(&ddl)
        .execute(pool)
        .await
        .map_err(database_error)?;
    Ok(())
}

/// PostgreSQL implementation of [`OutboxStore`] backed by `sqlx::PgPool`.
///
/// Cheap to clone (the pool and the cached SQL strings are reference-counted).
#[derive(Debug, Clone)]
pub struct PgOutboxStore {
    pool: PgPool,
    table_name: Arc<str>,
    poll_sql: Arc<str>,
    mark_delivered_sql: Arc<str>,
    mark_failed_sql: Arc<str>,
    dead_letter: Option<Arc<DeadLetterSql>>,
}

impl PgOutboxStore {
    /// Build a store for the given pool and table.
    ///
    /// SQL statements are templated and cached at construction so each poll
    /// cycle re-uses the same strings.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: PgPool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        let poll_sql = DIALECT.poll_sql(&table_name);
        let mark_delivered_sql = DIALECT.mark_delivered_sql(&table_name);
        let mark_failed_sql = DIALECT.mark_failed_sql(&table_name);
        Ok(Self {
            pool,
            table_name: Arc::from(table_name),
            poll_sql: Arc::from(poll_sql),
            mark_delivered_sql: Arc::from(mark_delivered_sql),
            mark_failed_sql: Arc::from(mark_failed_sql),
            dead_letter: None,
        })
    }

    /// Underlying pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Activate dead-letter persistence for this store.
    ///
    /// When enabled, envelopes that exhaust `max_attempts` are atomically
    /// moved to `dlq_table` by [`OutboxStore::mark_dead_lettered`].
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `dlq_table` is not a valid
    /// identifier.
    pub fn with_dead_letter(mut self, dlq_table: impl Into<String>) -> Result<Self, OutboxError> {
        let dlq = dlq_table.into();
        validate_table_name(&dlq)?;
        let insert_sql = DIALECT.insert_dead_letter_sql(&self.table_name, &dlq);
        let delete_sql = DIALECT.delete_from_main_sql(&self.table_name);
        self.dead_letter = Some(Arc::new(DeadLetterSql {
            insert_sql: Arc::from(insert_sql),
            delete_sql: Arc::from(delete_sql),
        }));
        Ok(self)
    }
}

#[async_trait]
impl OutboxStore for PgOutboxStore {
    type Client = PoolConnection<Postgres>;
    type Tx<'tx> = Transaction<'tx, Postgres>;

    async fn acquire(&self) -> Result<Self::Client, OutboxError> {
        self.pool.acquire().await.map_err(pool_error)
    }

    async fn begin<'a>(&self, client: &'a mut Self::Client) -> Result<Self::Tx<'a>, OutboxError> {
        client.begin().await.map_err(database_error)
    }

    async fn poll<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        batch_size: usize,
        max_attempts: u32,
    ) -> Result<Vec<OutboxEnvelope>, OutboxError> {
        let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let max = i32::try_from(max_attempts).unwrap_or(i32::MAX);
        let rows = sqlx::query(&self.poll_sql)
            .bind(max)
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
            .map_err(database_error)?;

        let mut envelopes = Vec::with_capacity(rows.len());
        for row in rows {
            let event_id: Uuid = row.try_get("event_id").map_err(database_error)?;
            let event_type: String = row.try_get("event_type").map_err(database_error)?;
            let payload: serde_json::Value = row.try_get("payload").map_err(database_error)?;
            let subject_id: Option<Uuid> = row.try_get("subject_id").map_err(database_error)?;
            let created_at: OffsetDateTime = row.try_get("created_at").map_err(database_error)?;
            let attempts: i32 = row.try_get("attempts").map_err(database_error)?;
            let last_error: Option<String> = row.try_get("last_error").map_err(database_error)?;
            let next_retry_at: Option<OffsetDateTime> =
                row.try_get("next_retry_at").map_err(database_error)?;

            let payload = serde_json::to_vec(&payload)?;

            envelopes.push(assemble_envelope(
                event_id,
                event_type,
                payload,
                subject_id,
                to_system_time(created_at),
                u32::try_from(attempts.max(0)).unwrap_or(u32::MAX),
                last_error,
                next_retry_at.map(to_system_time),
            ));
        }
        Ok(envelopes)
    }

    async fn mark_delivered<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
    ) -> Result<(), OutboxError> {
        sqlx::query(&self.mark_delivered_sql)
            .bind(event_id)
            .execute(&mut **tx)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn mark_failed<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
        error: &str,
        next_retry_at: SystemTime,
    ) -> Result<(), OutboxError> {
        sqlx::query(&self.mark_failed_sql)
            .bind(error)
            .bind(to_offset(next_retry_at))
            .bind(event_id)
            .execute(&mut **tx)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn commit<'a>(&self, tx: Self::Tx<'a>) -> Result<(), OutboxError> {
        tx.commit().await.map_err(database_error)
    }

    async fn mark_dead_lettered<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
        _error: &str,
    ) -> Result<(), OutboxError> {
        let Some(dlq) = &self.dead_letter else {
            return Ok(());
        };
        sqlx::query(&dlq.insert_sql)
            .bind(event_id)
            .execute(&mut **tx)
            .await
            .map_err(database_error)?;
        sqlx::query(&dlq.delete_sql)
            .bind(event_id)
            .execute(&mut **tx)
            .await
            .map_err(database_error)?;
        Ok(())
    }
}

/// PostgreSQL implementation of [`OutboxPublisher`] backed by `sqlx::PgPool`.
///
/// Cheap to clone (the pool and the cached insert statement are reference-counted).
#[derive(Debug, Clone)]
pub struct PgOutboxPublisher {
    pool: PgPool,
    table_name: Arc<str>,
    insert_sql: Arc<str>,
}

impl PgOutboxPublisher {
    /// Create a new publisher for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: PgPool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        let insert_sql = DIALECT.insert_sql(&table_name);
        Ok(Self {
            pool,
            table_name: Arc::from(table_name),
            insert_sql: Arc::from(insert_sql),
        })
    }

    /// Underlying pool, exposed for callers that open their own transactions.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl OutboxPublisher for PgOutboxPublisher {
    type Tx<'tx> = Transaction<'tx, Postgres>;

    async fn publish_in_tx<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        event: &E,
    ) -> Result<Uuid, OutboxError> {
        let event_id = Uuid::now_v7();
        let payload = serde_json::to_value(event)?;
        sqlx::query(&self.insert_sql)
            .bind(event_id)
            .bind(E::EVENT_TYPE)
            .bind(payload)
            .bind(Option::<Uuid>::None)
            .execute(&mut **tx)
            .await
            .map_err(database_error)?;
        Ok(event_id)
    }

    async fn publish_in_tx_with_subject<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        subject_id: Uuid,
        event: &E,
    ) -> Result<Uuid, OutboxError> {
        let event_id = Uuid::now_v7();
        let payload = serde_json::to_value(event)?;
        sqlx::query(&self.insert_sql)
            .bind(event_id)
            .bind(E::EVENT_TYPE)
            .bind(payload)
            .bind(Some(subject_id))
            .execute(&mut **tx)
            .await
            .map_err(database_error)?;
        Ok(event_id)
    }

    async fn publish<E: Event>(&self, event: &E) -> Result<Uuid, OutboxError> {
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let event_id = self.publish_in_tx(&mut tx, event).await?;
        tx.commit().await.map_err(database_error)?;
        Ok(event_id)
    }
}

/// Fluent builder for an [`OutboxWorker`] backed by [`PgOutboxStore`].
///
/// # Pool sizing and acquire timeout
///
/// The worker and every concurrent publisher draw connections from the same
/// pool. To avoid indefinite blocking under pressure, configure an acquire
/// timeout on the `PgPool` before passing it here:
///
/// ```rust,ignore
/// use sqlx::postgres::PgPoolOptions;
/// use std::time::Duration;
///
/// let pool = PgPoolOptions::new()
///     // 1 connection for the claim cycle + 1 per concurrent publisher + 2 headroom
///     .max_connections(batch_size + num_publishers + 2)
///     // surface PoolTimeout instead of blocking indefinitely
///     .acquire_timeout(Duration::from_secs(5))
///     .connect("postgres://...")
///     .await?;
///
/// let worker = PgOutboxWorkerBuilder::new(pool)
///     .batch_size(batch_size)
///     .build()?;
/// ```
///
/// When `acquire_timeout` expires, [`OutboxStore::acquire`] returns
/// [`OutboxError::PoolTimeout`] instead of hanging. The worker logs the
/// error and retries after [`OutboxWorkerConfig::poll_interval`].
///
/// [`OutboxError::PoolTimeout`]: hexeract_outbox::OutboxError::PoolTimeout
/// [`OutboxWorkerConfig::poll_interval`]: hexeract_outbox::OutboxWorkerConfig::poll_interval
pub struct PgOutboxWorkerBuilder {
    pool: PgPool,
    table_name: String,
    dead_letter_table: Option<String>,
    handlers: HashMap<&'static str, Arc<dyn ErasedHandler>>,
    config: OutboxWorkerConfig,
}

impl PgOutboxWorkerBuilder {
    /// Start a new builder for the given pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            table_name: DEFAULT_TABLE_NAME.to_owned(),
            dead_letter_table: None,
            handlers: HashMap::new(),
            config: OutboxWorkerConfig::default(),
        }
    }

    /// Override the outbox table name (default `"audit_outbox"`).
    #[must_use]
    pub fn table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = name.into();
        self
    }

    /// Enable dead-letter persistence for poison messages.
    ///
    /// Envelopes that exhaust their retry budget are atomically moved to
    /// `dlq_table` (INSERT + DELETE in the same transaction). When not set,
    /// exhausted envelopes are logged via `tracing::error!` but not moved.
    #[must_use]
    pub fn dead_letter_table(mut self, name: impl Into<String>) -> Self {
        self.dead_letter_table = Some(name.into());
        self
    }

    /// Register a typed handler for the event type `E`.
    ///
    /// Registering twice for the same event type silently replaces the
    /// previous handler.
    #[must_use]
    pub fn register_handler<E, H>(mut self, handler: H) -> Self
    where
        E: Event,
        H: Handler<E>,
    {
        let typed = TypedHandler::<E, H>::new(handler);
        let erased: Arc<dyn ErasedHandler> = Arc::new(typed);
        self.handlers.insert(E::EVENT_TYPE, erased);
        self
    }

    /// Register a handler already shared behind an `Arc`.
    #[must_use]
    pub fn shared_handler<E, H>(mut self, handler: Arc<H>) -> Self
    where
        E: Event,
        H: Handler<E>,
    {
        let typed = TypedHandler::<E, H>::shared(handler);
        let erased: Arc<dyn ErasedHandler> = Arc::new(typed);
        self.handlers.insert(E::EVENT_TYPE, erased);
        self
    }

    /// Override the poll interval (default 100 ms).
    #[must_use]
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.config.poll_interval = d;
        self
    }

    /// Override the batch size per poll (default 10).
    #[must_use]
    pub fn batch_size(mut self, n: usize) -> Self {
        self.config.batch_size = n;
        self
    }

    /// Override the maximum number of attempts per envelope (default 5).
    #[must_use]
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.config.max_attempts = n;
        self
    }

    /// Override the constant retry delay between failed attempts (default 5 s).
    #[must_use]
    pub fn retry_delay(mut self, d: Duration) -> Self {
        self.config.retry_delay = d;
        self
    }

    /// Consume the builder and produce an [`OutboxWorker`] ready to spawn.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if the configured `table_name`
    /// is not a valid identifier.
    pub fn build(self) -> Result<OutboxWorker<PgOutboxStore>, OutboxError> {
        let mut store = PgOutboxStore::new(self.pool, self.table_name)?;
        if let Some(dlq) = self.dead_letter_table {
            store = store.with_dead_letter(dlq)?;
        }
        Ok(OutboxWorker::new(store, self.handlers, self.config))
    }
}

/// Apply the dead-letter schema to the target Postgres database.
///
/// **Intended for POCs, integration tests and local development.**
///
/// # Errors
///
/// - [`OutboxError::Internal`] if `table_name` is not a valid identifier.
/// - [`OutboxError::Database`] if the connection or the DDL statement fails.
pub async fn ensure_dead_letter_schema(pool: &PgPool, table_name: &str) -> Result<(), OutboxError> {
    let ddl = DIALECT.dead_letter_schema_ddl(table_name)?;
    sqlx::raw_sql(&ddl)
        .execute(pool)
        .await
        .map_err(database_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::HandlerContext;
    use serde::Deserialize;
    use serde::Serialize;

    fn lazy_pool() -> PgPool {
        PgPool::connect_lazy("postgres://nobody:nobody@127.0.0.1:1/nobody")
            .expect("lazy pool must build from a valid URL")
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct UserRegistered {
        user_id: Uuid,
    }

    impl Event for UserRegistered {
        const EVENT_TYPE: &'static str = "users.registered";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: Uuid,
    }

    impl Event for OrderPlaced {
        const EVENT_TYPE: &'static str = "orders.placed";
    }

    struct NoopHandler;

    impl Handler<UserRegistered> for NoopHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: UserRegistered,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    impl Handler<OrderPlaced> for NoopHandler {
        type Error = OutboxError;
        async fn handle(
            &self,
            _event: OrderPlaced,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn pool_error_maps_pool_timed_out_to_pool_timeout_variant() {
        let err = pool_error(sqlx::Error::PoolTimedOut);
        assert!(
            matches!(err, OutboxError::PoolTimeout),
            "PoolTimedOut must map to OutboxError::PoolTimeout, got {err:?}"
        );
    }

    #[test]
    fn pool_error_wraps_other_errors_as_database_error() {
        let err = pool_error(sqlx::Error::RowNotFound);
        assert!(
            matches!(err, OutboxError::Database(_)),
            "non-timeout errors must map to OutboxError::Database, got {err:?}"
        );
    }

    #[tokio::test]
    async fn store_new_rejects_invalid_table_name() {
        let err = PgOutboxStore::new(lazy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[tokio::test]
    async fn store_new_caches_postgres_sql_with_validated_table_name() {
        let store = PgOutboxStore::new(lazy_pool(), "audit_outbox").unwrap();
        assert_eq!(store.table_name(), "audit_outbox");
        assert!(store.poll_sql.contains("FROM audit_outbox"));
        assert!(store.poll_sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(store.mark_delivered_sql.contains("UPDATE audit_outbox"));
        assert!(store.mark_failed_sql.contains("attempts = attempts + 1"));
    }

    #[tokio::test]
    async fn publisher_new_rejects_invalid_table_name() {
        let err = PgOutboxPublisher::new(lazy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[tokio::test]
    async fn publisher_new_caches_insert_sql_with_validated_table_name() {
        let publisher = PgOutboxPublisher::new(lazy_pool(), "audit_outbox").unwrap();
        assert_eq!(publisher.table_name(), "audit_outbox");
        assert!(publisher.insert_sql.contains("INSERT INTO audit_outbox"));
        assert!(publisher.insert_sql.contains("$1, $2, $3, $4"));
    }

    #[tokio::test]
    async fn builder_starts_with_default_table_and_empty_handlers() {
        let builder = PgOutboxWorkerBuilder::new(lazy_pool());
        assert_eq!(builder.table_name, DEFAULT_TABLE_NAME);
        assert!(builder.handlers.is_empty());
        let default_cfg = OutboxWorkerConfig::default();
        assert_eq!(builder.config.batch_size, default_cfg.batch_size);
        assert_eq!(builder.config.max_attempts, default_cfg.max_attempts);
    }

    #[tokio::test]
    async fn builder_table_name_can_be_customized() {
        let builder = PgOutboxWorkerBuilder::new(lazy_pool()).table_name("my_outbox");
        assert_eq!(builder.table_name, "my_outbox");
    }

    #[tokio::test]
    async fn builder_register_handler_records_event_types() {
        let builder = PgOutboxWorkerBuilder::new(lazy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler)
            .register_handler::<OrderPlaced, _>(NoopHandler);
        assert_eq!(builder.handlers.len(), 2);
        assert!(builder.handlers.contains_key("users.registered"));
        assert!(builder.handlers.contains_key("orders.placed"));
    }

    #[tokio::test]
    async fn builder_register_handler_twice_replaces_silently() {
        let builder = PgOutboxWorkerBuilder::new(lazy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler)
            .register_handler::<UserRegistered, _>(NoopHandler);
        assert_eq!(builder.handlers.len(), 1);
    }

    #[tokio::test]
    async fn builder_build_rejects_invalid_table_name() {
        let result = PgOutboxWorkerBuilder::new(lazy_pool())
            .table_name("bad name; DROP TABLE")
            .build();
        assert!(matches!(result, Err(OutboxError::Internal(_))));
    }

    #[tokio::test]
    async fn builder_build_with_default_table_name_succeeds() {
        let worker = PgOutboxWorkerBuilder::new(lazy_pool()).build();
        assert!(worker.is_ok());
    }

    #[tokio::test]
    async fn store_with_dead_letter_caches_sql_for_dlq() {
        let store = PgOutboxStore::new(lazy_pool(), "audit_outbox")
            .unwrap()
            .with_dead_letter("audit_outbox_dead_letter")
            .unwrap();
        let dlq = store.dead_letter.as_ref().unwrap();
        assert!(
            dlq.insert_sql
                .contains("INSERT INTO audit_outbox_dead_letter")
        );
        assert!(dlq.insert_sql.contains("FROM audit_outbox"));
        assert!(dlq.insert_sql.contains("$1"));
        assert!(dlq.delete_sql.contains("DELETE FROM audit_outbox"));
        assert!(dlq.delete_sql.contains("$1"));
    }

    #[tokio::test]
    async fn store_with_dead_letter_rejects_invalid_dlq_name() {
        let err = PgOutboxStore::new(lazy_pool(), "audit_outbox")
            .unwrap()
            .with_dead_letter("bad name; DROP")
            .unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[tokio::test]
    async fn builder_dead_letter_table_propagates_to_store() {
        let worker = PgOutboxWorkerBuilder::new(lazy_pool())
            .dead_letter_table("audit_outbox_dead_letter")
            .build()
            .unwrap();
        drop(worker);
    }
}
