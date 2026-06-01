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
use sqlx::MySql;
use sqlx::MySqlPool;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::pool::PoolConnection;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::DEFAULT_TABLE_NAME;
use crate::dialect::Dialect;
use crate::envelope::assemble_envelope;
use crate::envelope::primitive_utc_to_system_time;
use crate::envelope::to_primitive_utc;
use crate::validate::validate_table_name;

const DIALECT: Dialect = Dialect::MySql;

fn database_error(error: impl std::error::Error + Send + Sync + 'static) -> OutboxError {
    OutboxError::Database(Box::new(error))
}

/// Apply the canonical MySQL outbox schema to the target database.
///
/// **Intended for POCs, integration tests and local development.**
/// Production deployments should run their own migration tooling against the
/// SQL rendered by [`Dialect::schema_ddl`].
///
/// # Errors
///
/// - [`OutboxError::Internal`] if `table_name` is not a valid identifier.
/// - [`OutboxError::Database`] if the connection or the DDL statement fails.
pub async fn ensure_schema(pool: &MySqlPool, table_name: &str) -> Result<(), OutboxError> {
    let ddl = DIALECT.schema_ddl(table_name)?;
    sqlx::raw_sql(&ddl)
        .execute(pool)
        .await
        .map_err(database_error)?;
    Ok(())
}

/// MySQL implementation of [`OutboxStore`] backed by `sqlx::MySqlPool`.
///
/// Cheap to clone (the pool and the cached SQL strings are reference-counted).
#[derive(Debug, Clone)]
pub struct MySqlOutboxStore {
    pool: MySqlPool,
    table_name: Arc<str>,
    poll_sql: Arc<str>,
    mark_delivered_sql: Arc<str>,
    mark_failed_sql: Arc<str>,
}

impl MySqlOutboxStore {
    /// Build a store for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: MySqlPool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
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
        })
    }

    /// Underlying pool.
    #[must_use]
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[async_trait]
impl OutboxStore for MySqlOutboxStore {
    type Client = PoolConnection<MySql>;
    type Tx<'tx> = Transaction<'tx, MySql>;

    async fn acquire(&self) -> Result<Self::Client, OutboxError> {
        self.pool.acquire().await.map_err(database_error)
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
            let created_at: PrimitiveDateTime =
                row.try_get("created_at").map_err(database_error)?;
            let attempts: i32 = row.try_get("attempts").map_err(database_error)?;
            let last_error: Option<String> = row.try_get("last_error").map_err(database_error)?;
            let next_retry_at: Option<PrimitiveDateTime> =
                row.try_get("next_retry_at").map_err(database_error)?;

            let payload = serde_json::to_vec(&payload)?;

            envelopes.push(assemble_envelope(
                event_id,
                event_type,
                payload,
                subject_id,
                primitive_utc_to_system_time(created_at),
                u32::try_from(attempts.max(0)).unwrap_or(u32::MAX),
                last_error,
                next_retry_at.map(primitive_utc_to_system_time),
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
            .bind(to_primitive_utc(next_retry_at))
            .bind(event_id)
            .execute(&mut **tx)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn commit<'a>(&self, tx: Self::Tx<'a>) -> Result<(), OutboxError> {
        tx.commit().await.map_err(database_error)
    }
}

/// MySQL implementation of [`OutboxPublisher`] backed by `sqlx::MySqlPool`.
///
/// Cheap to clone (the pool and the cached insert statement are reference-counted).
#[derive(Debug, Clone)]
pub struct MySqlOutboxPublisher {
    pool: MySqlPool,
    table_name: Arc<str>,
    insert_sql: Arc<str>,
}

impl MySqlOutboxPublisher {
    /// Create a new publisher for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: MySqlPool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
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
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl OutboxPublisher for MySqlOutboxPublisher {
    type Tx<'tx> = Transaction<'tx, MySql>;

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

/// Fluent builder for an [`OutboxWorker`] backed by [`MySqlOutboxStore`].
pub struct MySqlOutboxWorkerBuilder {
    pool: MySqlPool,
    table_name: String,
    handlers: HashMap<&'static str, Arc<dyn ErasedHandler>>,
    config: OutboxWorkerConfig,
}

impl MySqlOutboxWorkerBuilder {
    /// Start a new builder for the given pool.
    #[must_use]
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            table_name: DEFAULT_TABLE_NAME.to_owned(),
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
    pub fn build(self) -> Result<OutboxWorker<MySqlOutboxStore>, OutboxError> {
        let store = MySqlOutboxStore::new(self.pool, self.table_name)?;
        Ok(OutboxWorker::new(store, self.handlers, self.config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::HandlerContext;
    use serde::Deserialize;
    use serde::Serialize;

    fn lazy_pool() -> MySqlPool {
        MySqlPool::connect_lazy("mysql://nobody:nobody@127.0.0.1:1/nobody")
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

    #[tokio::test]
    async fn store_new_rejects_invalid_table_name() {
        let err = MySqlOutboxStore::new(lazy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[tokio::test]
    async fn store_new_caches_mysql_sql_with_validated_table_name() {
        let store = MySqlOutboxStore::new(lazy_pool(), "audit_outbox").unwrap();
        assert_eq!(store.table_name(), "audit_outbox");
        assert!(store.poll_sql.contains("FROM audit_outbox"));
        assert!(store.poll_sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(store.poll_sql.contains("UTC_TIMESTAMP()"));
        assert!(store.mark_delivered_sql.contains("UPDATE audit_outbox"));
        assert!(store.mark_failed_sql.contains("attempts = attempts + 1"));
    }

    #[tokio::test]
    async fn publisher_new_caches_insert_sql_with_question_marks() {
        let publisher = MySqlOutboxPublisher::new(lazy_pool(), "audit_outbox").unwrap();
        assert_eq!(publisher.table_name(), "audit_outbox");
        assert!(publisher.insert_sql.contains("INSERT INTO audit_outbox"));
        assert!(publisher.insert_sql.contains("?, ?, ?, ?"));
    }

    #[tokio::test]
    async fn builder_starts_with_default_table_and_empty_handlers() {
        let builder = MySqlOutboxWorkerBuilder::new(lazy_pool());
        assert_eq!(builder.table_name, DEFAULT_TABLE_NAME);
        assert!(builder.handlers.is_empty());
    }

    #[tokio::test]
    async fn builder_register_handler_records_event_types() {
        let builder = MySqlOutboxWorkerBuilder::new(lazy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler)
            .register_handler::<OrderPlaced, _>(NoopHandler);
        assert_eq!(builder.handlers.len(), 2);
        assert!(builder.handlers.contains_key("users.registered"));
        assert!(builder.handlers.contains_key("orders.placed"));
    }

    #[tokio::test]
    async fn builder_build_rejects_invalid_table_name() {
        let result = MySqlOutboxWorkerBuilder::new(lazy_pool())
            .table_name("bad name; DROP TABLE")
            .build();
        assert!(matches!(result, Err(OutboxError::Internal(_))));
    }

    #[tokio::test]
    async fn builder_build_with_default_table_name_succeeds() {
        let worker = MySqlOutboxWorkerBuilder::new(lazy_pool()).build();
        assert!(worker.is_ok());
    }
}
