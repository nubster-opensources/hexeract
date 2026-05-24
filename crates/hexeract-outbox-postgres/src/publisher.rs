use deadpool_postgres::Pool;
use deadpool_postgres::Transaction;
use hexeract_outbox::Event;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxPublisher;
use uuid::Uuid;

use crate::schema::validate_table_name;

/// PostgreSQL implementation of [`OutboxPublisher`] backed by `deadpool_postgres`.
///
/// Cheap to clone (the underlying `Pool` is reference-counted).
#[derive(Debug, Clone)]
pub struct PgOutboxPublisher {
    pool: Pool,
    table_name: String,
}

impl PgOutboxPublisher {
    /// Create a new publisher for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// PostgreSQL identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`. The
    /// table name is validated up front because it is concatenated into
    /// the prepared insert statement.
    pub fn new(pool: Pool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        Ok(Self { pool, table_name })
    }

    /// Underlying pool, exposed for callers that need to open their own transactions.
    #[must_use]
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    fn insert_sql(&self) -> String {
        format!(
            "INSERT INTO {} (event_id, event_type, payload, subject_id) \
             VALUES ($1, $2, $3, $4)",
            self.table_name
        )
    }

    async fn execute_insert(
        tx: &Transaction<'_>,
        sql: &str,
        event_id: Uuid,
        event_type: &'static str,
        payload: &serde_json::Value,
        subject_id: Option<Uuid>,
    ) -> Result<(), OutboxError> {
        tx.execute(sql, &[&event_id, &event_type, payload, &subject_id])
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        Ok(())
    }
}

impl OutboxPublisher for PgOutboxPublisher {
    type Tx<'tx> = Transaction<'tx>;

    async fn publish_in_tx<E: Event>(
        &self,
        tx: &mut Self::Tx<'_>,
        event: &E,
    ) -> Result<Uuid, OutboxError> {
        let event_id = Uuid::now_v7();
        let payload = serde_json::to_value(event)?;
        let sql = self.insert_sql();
        Self::execute_insert(tx, &sql, event_id, E::EVENT_TYPE, &payload, None).await?;
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
        let sql = self.insert_sql();
        Self::execute_insert(
            tx,
            &sql,
            event_id,
            E::EVENT_TYPE,
            &payload,
            Some(subject_id),
        )
        .await?;
        Ok(event_id)
    }

    async fn publish<E: Event>(&self, event: &E) -> Result<Uuid, OutboxError> {
        let event_id = Uuid::now_v7();
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;

        let payload = serde_json::to_value(event)?;
        let sql = self.insert_sql();
        Self::execute_insert(&tx, &sql, event_id, E::EVENT_TYPE, &payload, None).await?;

        tx.commit()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        Ok(event_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deadpool_postgres::Config;
    use deadpool_postgres::Runtime;
    use tokio_postgres::NoTls;

    fn dummy_pool() -> Pool {
        let mut cfg = Config::new();
        cfg.host = Some("127.0.0.1".to_string());
        cfg.port = Some(1);
        cfg.user = Some("nobody".to_string());
        cfg.dbname = Some("nobody".to_string());
        cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
    }

    #[test]
    fn new_rejects_invalid_table_name() {
        let pool = dummy_pool();
        let err = PgOutboxPublisher::new(pool, "bad name; DROP TABLE").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn new_accepts_safe_table_name() {
        let pool = dummy_pool();
        let publisher = PgOutboxPublisher::new(pool, "audit_outbox").unwrap();
        assert_eq!(publisher.table_name(), "audit_outbox");
    }

    #[test]
    fn insert_sql_embeds_validated_table_name() {
        let pool = dummy_pool();
        let publisher = PgOutboxPublisher::new(pool, "audit_outbox").unwrap();
        let sql = publisher.insert_sql();
        assert!(sql.contains("INSERT INTO audit_outbox"));
        assert!(sql.contains("event_id"));
        assert!(sql.contains("VALUES ($1, $2, $3, $4)"));
    }
}
