//! SQLite backend for the Hexeract outbox.
//!
//! # Concurrency
//!
//! SQLite has no `FOR UPDATE SKIP LOCKED`, so this backend assumes a
//! **single [`OutboxWorker`] per database**. Running several workers against
//! the same SQLite database can dispatch an envelope more than once, because
//! concurrent pollers may read the same pending rows before either marks them
//! delivered. For competing-consumers fan-out across many workers, use the
//! PostgreSQL or MySQL backend instead. Configuring `busy_timeout` on the pool
//! is recommended so writes wait rather than fail under contention.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::IdempotentOutboxEnqueue;
use hexeract_outbox::OutboxEnvelope;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox::OutboxStore;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;
use sqlx::Acquire;
use sqlx::Row;
use sqlx::Sqlite;
use sqlx::SqlitePool;
use sqlx::Transaction;
use sqlx::pool::PoolConnection;
use uuid::Uuid;

use crate::DEFAULT_TABLE_NAME;
use crate::dialect::Dialect;
use crate::envelope::assemble_envelope;
use crate::envelope::parse_sqlite_utc;
use crate::validate::validate_event_type;
use crate::validate::validate_table_name;

const DIALECT: Dialect = Dialect::Sqlite;

/// Maximum interval in seconds that can be safely passed to SQLite's strftime
/// modifier (`"+N seconds"`).
///
/// SQLite's strftime modifier must parse to a finite value. Durations near
/// [`Duration::MAX`] produce `"+inf seconds"` via [`f64::INFINITY`], which
/// SQLite ignores silently, leaving `next_retry_at` as `NULL`. Capping at
/// this value (roughly 292 years) keeps the result well within SQLite's
/// datetime range while being far beyond any practical retry interval.
const MAX_SQLITE_INTERVAL_SECS: u64 = 9_223_372_036; // i64::MAX seconds

/// Render a backoff/lease [`Duration`] as a SQLite `strftime` modifier, e.g.
/// `"+1.500 seconds"`, so `next_retry_at` is computed from the database clock.
///
/// The duration is capped at [`MAX_SQLITE_INTERVAL_SECS`] before conversion
/// so that pathologically large values do not produce an `"+inf seconds"`
/// modifier that SQLite would silently ignore.
fn sqlite_seconds_modifier(d: Duration) -> String {
    let capped = d.min(Duration::from_secs(MAX_SQLITE_INTERVAL_SECS));
    format!("+{:.3} seconds", capped.as_secs_f64())
}

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

/// Decode one polled row into an [`OutboxEnvelope`].
///
/// Kept separate from the poll loop so a decode failure (notably a timestamp
/// that does not match either accepted SQLite layout) can be isolated to the
/// offending row (logged and skipped) instead of aborting the whole batch.
fn decode_sqlite_row(row: &sqlx::sqlite::SqliteRow) -> Result<OutboxEnvelope, OutboxError> {
    let event_id: Uuid = row.try_get("event_id").map_err(database_error)?;
    let event_type: String = row.try_get("event_type").map_err(database_error)?;
    let payload: serde_json::Value = row.try_get("payload").map_err(database_error)?;
    let subject_id: Option<Uuid> = row.try_get("subject_id").map_err(database_error)?;
    let created_at: String = row.try_get("created_at").map_err(database_error)?;
    let attempts: i64 = row.try_get("attempts").map_err(database_error)?;
    let last_error: Option<String> = row.try_get("last_error").map_err(database_error)?;
    let next_retry_at: Option<String> = row.try_get("next_retry_at").map_err(database_error)?;

    let payload = serde_json::to_vec(&payload)?;
    let next_retry_at = next_retry_at.as_deref().map(parse_sqlite_utc).transpose()?;

    Ok(assemble_envelope(
        event_id,
        event_type,
        payload,
        subject_id,
        parse_sqlite_utc(&created_at)?,
        u32::try_from(attempts.max(0)).unwrap_or(u32::MAX),
        last_error,
        next_retry_at,
    ))
}

#[derive(Debug, Clone)]
struct DeadLetterSql {
    insert_sql: Arc<str>,
    delete_sql: Arc<str>,
}

/// Apply the canonical SQLite outbox schema to the target database.
///
/// **Intended for POCs, integration tests and local development.**
/// Production deployments should run their own migration tooling against the
/// SQL rendered by [`Dialect::schema_ddl`].
///
/// # Errors
///
/// - [`OutboxError::Internal`] if `table_name` is not a valid identifier.
/// - [`OutboxError::Database`] if the connection or the DDL statement fails.
pub async fn ensure_schema(pool: &SqlitePool, table_name: &str) -> Result<(), OutboxError> {
    let ddl = DIALECT.schema_ddl(table_name)?;
    sqlx::raw_sql(&ddl)
        .execute(pool)
        .await
        .map_err(database_error)?;
    Ok(())
}

/// SQLite implementation of [`OutboxStore`] backed by `sqlx::SqlitePool`.
///
/// See the [module documentation](self) for the single-worker concurrency model.
/// Cheap to clone (the pool and the cached SQL strings are reference-counted).
#[derive(Debug, Clone)]
pub struct SqliteOutboxStore {
    pool: SqlitePool,
    table_name: Arc<str>,
    poll_sql: Arc<str>,
    mark_delivered_sql: Arc<str>,
    mark_failed_sql: Arc<str>,
    dead_letter: Option<Arc<DeadLetterSql>>,
}

impl SqliteOutboxStore {
    /// Build a store for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: SqlitePool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
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
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Activate dead-letter persistence for poison messages.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `dlq_table` is not a valid identifier.
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
impl OutboxStore for SqliteOutboxStore {
    type Client = PoolConnection<Sqlite>;
    type Tx<'tx> = Transaction<'tx, Sqlite>;

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
        let max = i64::from(max_attempts);
        let rows = sqlx::query(&self.poll_sql)
            .bind(max)
            .bind(limit)
            .fetch_all(&mut **tx)
            .await
            .map_err(database_error)?;

        let mut envelopes = Vec::with_capacity(rows.len());
        for row in rows {
            // A single undecodable row (e.g. an externally written timestamp in
            // an unexpected layout) must not abort the whole poll: that
            // head-of-line poisons the queue forever (#214). Log it and skip so
            // the rest of the batch keeps draining.
            match decode_sqlite_row(&row) {
                Ok(envelope) => envelopes.push(envelope),
                Err(error) => {
                    let event_id = row.try_get::<Uuid, _>("event_id").ok();
                    tracing::error!(
                        ?event_id,
                        error = %error,
                        "skipping undecodable outbox row; the rest of the batch continues"
                    );
                }
            }
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
        retry_in: Duration,
    ) -> Result<(), OutboxError> {
        // next_retry_at is computed as strftime('now', ?modifier) against the
        // DB clock (#230); bind the backoff as a strftime seconds modifier.
        sqlx::query(&self.mark_failed_sql)
            .bind(error)
            .bind(sqlite_seconds_modifier(retry_in))
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

    /// Set the soft lease and consume one retry slot on the claimed batch.
    ///
    /// SQLite has no `FOR UPDATE SKIP LOCKED`, so this does **not** provide a
    /// competing-consumer claim: the store remains single-writer (see the
    /// [module documentation](self)). The override exists so that `attempts`
    /// is incremented at claim time on SQLite too. Without it, a worker that
    /// crashed between claim and acknowledgement would never advance
    /// `attempts` and would redeliver a poison row forever (#213).
    async fn claim<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_ids: &[Uuid],
        lease_for: Duration,
    ) -> Result<(), OutboxError> {
        if event_ids.is_empty() {
            return Ok(());
        }
        // Lease anchored to the DB clock via strftime('now', ?modifier) (#230).
        let sql = DIALECT.claim_sql(&self.table_name, event_ids.len());
        let mut query = sqlx::query(&sql).bind(sqlite_seconds_modifier(lease_for));
        for id in event_ids {
            query = query.bind(*id);
        }
        query.execute(&mut **tx).await.map_err(database_error)?;
        Ok(())
    }
}

/// SQLite implementation of [`OutboxPublisher`] backed by `sqlx::SqlitePool`.
///
/// Cheap to clone (the pool and the cached insert statement are reference-counted).
#[derive(Debug, Clone)]
pub struct SqliteOutboxPublisher {
    pool: SqlitePool,
    table_name: Arc<str>,
    insert_sql: Arc<str>,
    idempotent_insert_sql: Arc<str>,
}

impl SqliteOutboxPublisher {
    /// Create a new publisher for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: SqlitePool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        let insert_sql = DIALECT.insert_sql(&table_name);
        let idempotent_insert_sql = DIALECT.insert_idempotent_sql(&table_name);
        Ok(Self {
            pool,
            table_name: Arc::from(table_name),
            insert_sql: Arc::from(insert_sql),
            idempotent_insert_sql: Arc::from(idempotent_insert_sql),
        })
    }

    /// Underlying pool, exposed for callers that open their own transactions.
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl OutboxPublisher for SqliteOutboxPublisher {
    type Tx<'tx> = Transaction<'tx, Sqlite>;

    async fn publish_in_tx<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        event: &E,
    ) -> Result<Uuid, OutboxError> {
        validate_event_type(E::EVENT_TYPE)?;
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
        validate_event_type(E::EVENT_TYPE)?;
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

impl IdempotentOutboxEnqueue for SqliteOutboxPublisher {
    async fn enqueue_idempotent(
        &self,
        event_id: Uuid,
        event_type: &str,
        payload: &[u8],
    ) -> Result<bool, OutboxError> {
        validate_event_type(event_type)?;
        let payload_value = serde_json::from_slice::<serde_json::Value>(payload)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let result = sqlx::query(&self.idempotent_insert_sql)
            .bind(event_id)
            .bind(event_type)
            .bind(payload_value)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        Ok(result.rows_affected() > 0)
    }
}

/// Fluent builder for an [`OutboxWorker`] backed by [`SqliteOutboxStore`].
///
/// See the [module documentation](self) for the single-worker concurrency model.
///
/// # Pool sizing and acquire timeout
///
/// SQLite uses a single-writer model. A pool size of 1–2 connections is
/// typical: the worker holds one connection per poll cycle while publishers
/// take the other. To prevent a stuck writer from blocking `acquire()`
/// indefinitely, set an acquire timeout on the pool:
///
/// ```rust,ignore
/// use sqlx::sqlite::SqlitePoolOptions;
/// use std::time::Duration;
///
/// let pool = SqlitePoolOptions::new()
///     .max_connections(2)
///     // surface PoolTimeout instead of blocking indefinitely
///     .acquire_timeout(Duration::from_secs(5))
///     .connect("sqlite:outbox.db")
///     .await?;
///
/// let worker = SqliteOutboxWorkerBuilder::new(pool).build()?;
/// ```
///
/// When `acquire_timeout` expires, [`OutboxStore::acquire`] returns
/// [`OutboxError::PoolTimeout`] instead of hanging. The worker logs the
/// error and retries after [`OutboxWorkerConfig::poll_interval`].
///
/// [`OutboxError::PoolTimeout`]: hexeract_outbox::OutboxError::PoolTimeout
/// [`OutboxWorkerConfig::poll_interval`]: hexeract_outbox::OutboxWorkerConfig::poll_interval
pub struct SqliteOutboxWorkerBuilder {
    pool: SqlitePool,
    table_name: String,
    dead_letter_table: Option<String>,
    handlers: HashMap<&'static str, Arc<dyn ErasedHandler>>,
    config: OutboxWorkerConfig,
}

impl SqliteOutboxWorkerBuilder {
    /// Start a new builder for the given pool.
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
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

    /// Override the base delay for exponential backoff (default 1 s).
    #[must_use]
    pub fn retry_base_delay(mut self, d: Duration) -> Self {
        self.config.retry_base_delay = d;
        self
    }

    /// Override the maximum backoff delay (default 5 min).
    #[must_use]
    pub fn retry_max_delay(mut self, d: Duration) -> Self {
        self.config.retry_max_delay = d;
        self
    }

    /// Enable or disable full jitter on the backoff delay (default `true`).
    #[must_use]
    pub fn jitter(mut self, enabled: bool) -> Self {
        self.config.jitter = enabled;
        self
    }

    /// Override the soft-lease duration for claimed envelopes (default 30 s).
    ///
    /// SQLite has no `FOR UPDATE SKIP LOCKED` and therefore no
    /// competing-consumer claim, so this store is meant for a **single
    /// worker** (see the [module documentation](self)); running several workers
    /// against one database can still double-dispatch. The lease is recorded on
    /// claim alongside the attempt increment, but with a single writer it only
    /// affects when a crashed-and-restarted worker re-picks an in-flight row.
    #[must_use]
    pub fn dispatch_timeout(mut self, d: Duration) -> Self {
        self.config.dispatch_timeout = d;
        self
    }

    /// Consume the builder and produce an [`OutboxWorker`] ready to spawn.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if the configured `table_name`
    /// is not a valid identifier.
    pub fn build(self) -> Result<OutboxWorker<SqliteOutboxStore>, OutboxError> {
        let mut store = SqliteOutboxStore::new(self.pool, self.table_name)?;
        if let Some(dlq) = self.dead_letter_table {
            store = store.with_dead_letter(dlq)?;
        }
        Ok(OutboxWorker::new(store, self.handlers, self.config))
    }
}

/// Apply the dead-letter schema to the target SQLite database.
///
/// **Intended for POCs, integration tests and local development.**
///
/// # Errors
///
/// - [`OutboxError::Internal`] if `table_name` is not a valid identifier.
/// - [`OutboxError::Database`] if the connection or the DDL statement fails.
pub async fn ensure_dead_letter_schema(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<(), OutboxError> {
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

    fn lazy_pool() -> SqlitePool {
        SqlitePool::connect_lazy("sqlite::memory:").expect("lazy pool must build from a valid URL")
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
        let err = SqliteOutboxStore::new(lazy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[tokio::test]
    async fn store_new_caches_sqlite_sql_without_skip_locked() {
        let store = SqliteOutboxStore::new(lazy_pool(), "audit_outbox").unwrap();
        assert_eq!(store.table_name(), "audit_outbox");
        assert!(store.poll_sql.contains("FROM \"audit_outbox\""));
        assert!(!store.poll_sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(store.poll_sql.contains("strftime"));
        // The attempt increment lives in claim_sql now (see #213), not in
        // mark_failed.
        assert!(!store.mark_failed_sql.contains("attempts = attempts + 1"));
    }

    #[tokio::test]
    async fn publisher_new_caches_insert_sql_with_question_marks() {
        let publisher = SqliteOutboxPublisher::new(lazy_pool(), "audit_outbox").unwrap();
        assert_eq!(publisher.table_name(), "audit_outbox");
        assert!(
            publisher
                .insert_sql
                .contains("INSERT INTO \"audit_outbox\"")
        );
        assert!(publisher.insert_sql.contains("?, ?, ?, ?"));
    }

    #[test]
    fn sqlite_seconds_modifier_caps_huge_duration() {
        // Duration::MAX produces inf via as_secs_f64(); capping prevents an
        // "+inf seconds" modifier that SQLite would silently ignore (#240).
        let modifier = sqlite_seconds_modifier(Duration::MAX);
        assert!(
            !modifier.contains("inf"),
            "Duration::MAX must not produce an inf modifier, got: {modifier}"
        );
        assert!(modifier.starts_with('+'), "modifier must start with '+'");
        assert!(
            modifier.ends_with(" seconds"),
            "modifier must end with ' seconds'"
        );
    }

    #[test]
    fn sqlite_seconds_modifier_preserves_ordinary_values() {
        let modifier = sqlite_seconds_modifier(Duration::from_millis(1_500));
        assert_eq!(modifier, "+1.500 seconds");
    }

    #[tokio::test]
    async fn builder_register_handler_records_event_types() {
        let builder = SqliteOutboxWorkerBuilder::new(lazy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler)
            .register_handler::<OrderPlaced, _>(NoopHandler);
        assert_eq!(builder.handlers.len(), 2);
        assert!(builder.handlers.contains_key("users.registered"));
        assert!(builder.handlers.contains_key("orders.placed"));
    }

    #[tokio::test]
    async fn builder_build_rejects_invalid_table_name() {
        let result = SqliteOutboxWorkerBuilder::new(lazy_pool())
            .table_name("bad name; DROP TABLE")
            .build();
        assert!(matches!(result, Err(OutboxError::Internal(_))));
    }

    #[tokio::test]
    async fn builder_build_with_default_table_name_succeeds() {
        let worker = SqliteOutboxWorkerBuilder::new(lazy_pool()).build();
        assert!(worker.is_ok());
    }
}
