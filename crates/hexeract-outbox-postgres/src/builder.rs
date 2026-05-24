use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use hexeract_outbox::ErasedHandler;
use hexeract_outbox::Event;
use hexeract_outbox::Handler;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxWorker;
use hexeract_outbox::OutboxWorkerConfig;
use hexeract_outbox::TypedHandler;

use crate::PgOutboxStore;

/// Default outbox table name used when [`PgOutboxWorkerBuilder::table_name`] is not called.
pub const DEFAULT_TABLE_NAME: &str = "audit_outbox";

/// Fluent builder for an [`OutboxWorker`] backed by [`PgOutboxStore`].
///
/// The builder owns a [`Pool`], a registry of typed handlers and an
/// [`OutboxWorkerConfig`]. The pool is mandatory and supplied at
/// construction. Every other knob has a sensible default that mirrors
/// [`OutboxWorkerConfig::default`].
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
///
/// use deadpool_postgres::Config;
/// use deadpool_postgres::Runtime;
/// use hexeract_core::HandlerContext;
/// use hexeract_outbox::{Event, Handler, OutboxError};
/// use hexeract_outbox_postgres::PgOutboxWorkerBuilder;
/// use serde::{Deserialize, Serialize};
/// use tokio_postgres::NoTls;
/// use tokio_util::sync::CancellationToken;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct UserRegistered { user_id: uuid::Uuid }
///
/// impl Event for UserRegistered {
///     const EVENT_TYPE: &'static str = "users.registered";
/// }
///
/// struct AuditWriter;
///
/// impl Handler<UserRegistered> for AuditWriter {
///     type Error = OutboxError;
///     async fn handle(&self, _event: UserRegistered, _ctx: &HandlerContext) -> Result<(), Self::Error> {
///         Ok(())
///     }
/// }
///
/// # async fn run(pool: deadpool_postgres::Pool) -> Result<(), Box<dyn std::error::Error>> {
/// let worker = PgOutboxWorkerBuilder::new(pool)
///     .table_name("audit_outbox")
///     .register_handler::<UserRegistered, _>(AuditWriter)
///     .poll_interval(Duration::from_millis(50))
///     .build()?;
///
/// let cancel = CancellationToken::new();
/// let join = tokio::spawn(worker.run(cancel.clone()));
/// cancel.cancel();
/// join.await??;
/// # Ok(())
/// # }
/// ```
pub struct PgOutboxWorkerBuilder {
    pool: Pool,
    table_name: String,
    handlers: HashMap<&'static str, Arc<dyn ErasedHandler>>,
    config: OutboxWorkerConfig,
}

impl PgOutboxWorkerBuilder {
    /// Start a new builder for the given pool.
    #[must_use]
    pub fn new(pool: Pool) -> Self {
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
    /// previous handler. Useful for dynamic reconfiguration but be
    /// explicit in production code.
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
    ///
    /// Useful when the same handler instance must be reused outside the
    /// worker (e.g. for direct invocation in tests).
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
    /// is not a valid PostgreSQL identifier matching
    /// `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn build(self) -> Result<OutboxWorker<PgOutboxStore>, OutboxError> {
        let store = PgOutboxStore::new(self.pool, self.table_name)?;
        Ok(OutboxWorker::new(store, self.handlers, self.config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deadpool_postgres::Config;
    use deadpool_postgres::Runtime;
    use hexeract_core::HandlerContext;
    use serde::Deserialize;
    use serde::Serialize;
    use tokio_postgres::NoTls;
    use uuid::Uuid;

    fn dummy_pool() -> Pool {
        let mut cfg = Config::new();
        cfg.host = Some("127.0.0.1".to_string());
        cfg.port = Some(1);
        cfg.user = Some("nobody".to_string());
        cfg.dbname = Some("nobody".to_string());
        cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
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
    fn new_uses_audit_outbox_as_default_table_name() {
        let builder = PgOutboxWorkerBuilder::new(dummy_pool());
        assert_eq!(builder.table_name, DEFAULT_TABLE_NAME);
        assert_eq!(builder.table_name, "audit_outbox");
    }

    #[test]
    fn new_starts_with_default_config_and_empty_handlers() {
        let builder = PgOutboxWorkerBuilder::new(dummy_pool());
        assert!(builder.handlers.is_empty());
        let default_cfg = OutboxWorkerConfig::default();
        assert_eq!(builder.config.poll_interval, default_cfg.poll_interval);
        assert_eq!(builder.config.batch_size, default_cfg.batch_size);
        assert_eq!(builder.config.max_attempts, default_cfg.max_attempts);
        assert_eq!(builder.config.retry_delay, default_cfg.retry_delay);
    }

    #[test]
    fn table_name_can_be_customized() {
        let builder = PgOutboxWorkerBuilder::new(dummy_pool()).table_name("my_outbox");
        assert_eq!(builder.table_name, "my_outbox");
    }

    #[test]
    fn register_handler_records_event_type_in_registry() {
        let builder = PgOutboxWorkerBuilder::new(dummy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler);
        assert!(builder.handlers.contains_key("users.registered"));
        assert_eq!(builder.handlers.len(), 1);
    }

    #[test]
    fn register_handler_supports_multiple_event_types() {
        let builder = PgOutboxWorkerBuilder::new(dummy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler)
            .register_handler::<OrderPlaced, _>(NoopHandler);
        assert_eq!(builder.handlers.len(), 2);
        assert!(builder.handlers.contains_key("users.registered"));
        assert!(builder.handlers.contains_key("orders.placed"));
    }

    #[test]
    fn register_handler_twice_for_same_event_replaces_silently() {
        let builder = PgOutboxWorkerBuilder::new(dummy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler)
            .register_handler::<UserRegistered, _>(NoopHandler);
        assert_eq!(builder.handlers.len(), 1);
    }

    #[test]
    fn shared_handler_records_event_type_in_registry() {
        let handler = Arc::new(NoopHandler);
        let builder =
            PgOutboxWorkerBuilder::new(dummy_pool()).shared_handler::<UserRegistered, _>(handler);
        assert!(builder.handlers.contains_key("users.registered"));
    }

    #[test]
    fn tuning_methods_update_config() {
        let builder = PgOutboxWorkerBuilder::new(dummy_pool())
            .poll_interval(Duration::from_millis(50))
            .batch_size(20)
            .max_attempts(10)
            .retry_delay(Duration::from_secs(30));
        assert_eq!(builder.config.poll_interval, Duration::from_millis(50));
        assert_eq!(builder.config.batch_size, 20);
        assert_eq!(builder.config.max_attempts, 10);
        assert_eq!(builder.config.retry_delay, Duration::from_secs(30));
    }

    #[test]
    fn build_with_default_table_name_succeeds() {
        let result = PgOutboxWorkerBuilder::new(dummy_pool()).build();
        assert!(result.is_ok());
    }

    #[test]
    fn build_rejects_invalid_table_name() {
        let result = PgOutboxWorkerBuilder::new(dummy_pool())
            .table_name("bad name; DROP TABLE")
            .build();
        assert!(matches!(result, Err(OutboxError::Internal(_))));
    }

    #[test]
    fn build_with_handlers_carries_them_through() {
        let worker = PgOutboxWorkerBuilder::new(dummy_pool())
            .register_handler::<UserRegistered, _>(NoopHandler)
            .register_handler::<OrderPlaced, _>(NoopHandler)
            .build()
            .expect("build must succeed");
        // Type-check only: ensure we obtain the expected concrete worker.
        let _: OutboxWorker<PgOutboxStore> = worker;
    }
}
