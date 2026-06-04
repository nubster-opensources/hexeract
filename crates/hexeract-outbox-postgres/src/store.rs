use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use deadpool_postgres::Object;
use deadpool_postgres::Pool;
use deadpool_postgres::Transaction;
use hexeract_outbox::OutboxEnvelope;
use hexeract_outbox::OutboxError;
use hexeract_outbox::OutboxStore;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use crate::schema::validate_table_name;

/// PostgreSQL implementation of [`OutboxStore`] backed by `deadpool_postgres`.
///
/// Cheap to clone (the underlying [`Pool`] and the cached SQL strings
/// are reference-counted).
#[deprecated(
    since = "0.4.0",
    note = "use the hexeract-outbox-sql crate with the postgres feature; this crate is removed in 0.5.0"
)]
#[derive(Debug, Clone)]
pub struct PgOutboxStore {
    pool: Pool,
    table_name: Arc<String>,
    poll_sql: Arc<String>,
    mark_delivered_sql: Arc<String>,
    mark_failed_sql: Arc<String>,
}

impl PgOutboxStore {
    /// Build a store for the given pool and table.
    ///
    /// SQL statements are templated and cached at construction so each
    /// poll cycle re-uses the same prepared strings.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table_name` is not a valid
    /// PostgreSQL identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: Pool, table_name: impl Into<String>) -> Result<Self, OutboxError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        let poll_sql = format!(
            "SELECT event_id, event_type, payload, subject_id, created_at, \
                    attempts, last_error, next_retry_at \
             FROM {table_name} \
             WHERE delivered_at IS NULL \
               AND attempts < $1 \
               AND (next_retry_at IS NULL OR next_retry_at <= NOW()) \
             ORDER BY id \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED"
        );
        let mark_delivered_sql =
            format!("UPDATE {table_name} SET delivered_at = NOW() WHERE event_id = $1");
        let mark_failed_sql = format!(
            "UPDATE {table_name} \
             SET attempts = attempts + 1, last_error = $1, next_retry_at = $2 \
             WHERE event_id = $3"
        );
        Ok(Self {
            pool,
            table_name: Arc::new(table_name),
            poll_sql: Arc::new(poll_sql),
            mark_delivered_sql: Arc::new(mark_delivered_sql),
            mark_failed_sql: Arc::new(mark_failed_sql),
        })
    }

    /// Underlying pool.
    #[must_use]
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[async_trait]
impl OutboxStore for PgOutboxStore {
    type Client = Object;
    type Tx<'tx>
        = Transaction<'tx>
    where
        Self: 'tx;

    async fn acquire(&self) -> Result<Self::Client, OutboxError> {
        self.pool
            .get()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))
    }

    async fn begin<'a>(&self, client: &'a mut Self::Client) -> Result<Self::Tx<'a>, OutboxError> {
        client
            .transaction()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))
    }

    async fn poll<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        batch_size: usize,
        max_attempts: u32,
    ) -> Result<Vec<OutboxEnvelope>, OutboxError> {
        let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let max = i32::try_from(max_attempts).unwrap_or(i32::MAX);
        let rows = tx
            .query(self.poll_sql.as_str(), &[&max, &limit])
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;

        let mut envelopes = Vec::with_capacity(rows.len());
        for row in rows {
            let event_id: Uuid = row.get(0);
            let event_type: String = row.get(1);
            let payload: serde_json::Value = row.get(2);
            let subject_id: Option<Uuid> = row.get(3);
            let created_at: SystemTime = row.get(4);
            let attempts: i32 = row.get(5);
            let last_error: Option<String> = row.get(6);
            let next_retry_at: Option<SystemTime> = row.get(7);

            let payload = serde_json::to_vec(&payload)?;

            envelopes.push(OutboxEnvelope::restore(
                event_id,
                event_type,
                payload,
                subject_id,
                created_at,
                u32::try_from(attempts.max(0)).unwrap_or(u32::MAX),
                last_error,
                next_retry_at,
                None,
            ));
        }
        Ok(envelopes)
    }

    async fn mark_delivered<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
    ) -> Result<(), OutboxError> {
        tx.execute(self.mark_delivered_sql.as_str(), &[&event_id])
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn mark_failed<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_id: Uuid,
        error: &str,
        next_retry_at: SystemTime,
    ) -> Result<(), OutboxError> {
        tx.execute(
            self.mark_failed_sql.as_str(),
            &[&error, &next_retry_at, &event_id],
        )
        .await
        .map_err(|e| OutboxError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn claim<'a>(
        &self,
        tx: &mut Self::Tx<'a>,
        event_ids: &[Uuid],
        lease_until: SystemTime,
    ) -> Result<(), OutboxError> {
        if event_ids.is_empty() {
            return Ok(());
        }
        let n = event_ids.len();
        let placeholders = (2..=n + 1)
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "UPDATE {} SET next_retry_at = $1 WHERE event_id IN ({placeholders})",
            self.table_name
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n + 1);
        params.push(&lease_until);
        for id in event_ids {
            params.push(id);
        }
        tx.execute(sql.as_str(), &params)
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn commit<'a>(&self, tx: Self::Tx<'a>) -> Result<(), OutboxError> {
        tx.commit()
            .await
            .map_err(|e| OutboxError::Database(Box::new(e)))
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
        let err = PgOutboxStore::new(dummy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn new_caches_sql_statements_with_validated_table_name() {
        let store = PgOutboxStore::new(dummy_pool(), "audit_outbox").unwrap();
        assert_eq!(store.table_name(), "audit_outbox");
        assert!(store.poll_sql.contains("FROM audit_outbox"));
        assert!(store.poll_sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(store.mark_delivered_sql.contains("UPDATE audit_outbox"));
        assert!(store.mark_failed_sql.contains("attempts = attempts + 1"));
    }
}
