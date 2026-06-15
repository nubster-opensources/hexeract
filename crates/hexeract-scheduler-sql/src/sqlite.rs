//! SQLite backend for the Hexeract scheduler.
//!
//! [`SqliteScheduleStore`] implements [`ScheduleStore`] on `sqlx::SqlitePool`.
//!
//! # Concurrency
//!
//! SQLite has no `FOR UPDATE SKIP LOCKED`, so this backend assumes a **single
//! worker per database**: it serializes writes through one writer instead of
//! locking due rows for competing consumers. The claim still increments the
//! attempt counter and stamps the lease, so a worker that crashes between
//! claim and acknowledgement does not redeliver forever. For competing-consumer
//! fan-out, use the PostgreSQL or MySQL backend.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use hexeract_outbox_sql::Dialect;
use hexeract_scheduler::LeasedOccurrence;
use hexeract_scheduler::ScheduleSnapshot;
use hexeract_scheduler::ScheduleStore;
use hexeract_scheduler::ScheduledMessage;
use hexeract_scheduler::SchedulerError;
use sqlx::Row;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::mapping;
use crate::statements;
use crate::timestamp;
use crate::validate::validate_table_name;

const DIALECT: Dialect = Dialect::Sqlite;

/// Maximum interval in seconds passed to SQLite's `strftime` modifier.
/// Durations near [`Duration::MAX`] would render as `"+inf seconds"`, which
/// SQLite silently ignores; capping at roughly 292 years stays well within
/// range while exceeding any practical lease.
const MAX_SQLITE_INTERVAL_SECS: u64 = 9_223_372_036;

/// Render a lease [`Duration`] as a SQLite `strftime` modifier, for example
/// `"+1.500 seconds"`, so the lease is computed from the database clock.
fn sqlite_seconds_modifier(d: Duration) -> String {
    let capped = d.min(Duration::from_secs(MAX_SQLITE_INTERVAL_SECS));
    format!("+{:.3} seconds", capped.as_secs_f64())
}

fn database_error(error: impl std::error::Error + Send + Sync + 'static) -> SchedulerError {
    SchedulerError::database(error)
}

/// Decode a claimed row into a [`LeasedOccurrence`].
fn decode_leased(row: &sqlx::sqlite::SqliteRow) -> Result<LeasedOccurrence, SchedulerError> {
    let schedule_id: Uuid = row.try_get("schedule_id").map_err(database_error)?;
    let event_type: String = row.try_get("event_type").map_err(database_error)?;
    let payload: serde_json::Value = row.try_get("payload").map_err(database_error)?;
    let trigger_kind: String = row.try_get("trigger_kind").map_err(database_error)?;
    let cron_expr: Option<String> = row.try_get("cron_expr").map_err(database_error)?;
    let scheduled_for: String = row.try_get("scheduled_for").map_err(database_error)?;
    let target_kind: String = row.try_get("target_kind").map_err(database_error)?;
    let routing_key: Option<String> = row.try_get("target_routing_key").map_err(database_error)?;
    let attempts: i64 = row.try_get("attempts").map_err(database_error)?;
    let max_attempts: i64 = row.try_get("max_attempts").map_err(database_error)?;
    let leased_until: String = row.try_get("leased_until").map_err(database_error)?;

    let payload = serde_json::to_vec(&payload)?;
    let scheduled_for = timestamp::parse_sqlite_utc(&scheduled_for)?;
    let message = mapping::build_message(
        schedule_id,
        event_type,
        payload,
        &trigger_kind,
        cron_expr,
        scheduled_for,
        &target_kind,
        routing_key,
    )?;
    Ok(LeasedOccurrence::new(
        message,
        mapping::attempts_from_i64(attempts),
        mapping::attempts_from_i64(max_attempts),
        timestamp::parse_sqlite_utc(&leased_until)?,
    ))
}

/// Decode an inspected row into a [`ScheduleSnapshot`].
fn decode_snapshot(row: &sqlx::sqlite::SqliteRow) -> Result<ScheduleSnapshot, SchedulerError> {
    let schedule_id: Uuid = row.try_get("schedule_id").map_err(database_error)?;
    let trigger_kind: String = row.try_get("trigger_kind").map_err(database_error)?;
    let cron_expr: Option<String> = row.try_get("cron_expr").map_err(database_error)?;
    let scheduled_for: String = row.try_get("scheduled_for").map_err(database_error)?;
    let attempts: i64 = row.try_get("attempts").map_err(database_error)?;
    let max_attempts: i64 = row.try_get("max_attempts").map_err(database_error)?;
    let paused: i64 = row.try_get("paused").map_err(database_error)?;
    let last_error: Option<String> = row.try_get("last_error").map_err(database_error)?;
    let delivered_at: Option<String> = row.try_get("delivered_at").map_err(database_error)?;
    let cancelled_at: Option<String> = row.try_get("cancelled_at").map_err(database_error)?;
    let dead_lettered_at: Option<String> =
        row.try_get("dead_lettered_at").map_err(database_error)?;

    let scheduled_for = timestamp::parse_sqlite_utc(&scheduled_for)?;
    let trigger = mapping::build_trigger(&trigger_kind, cron_expr, scheduled_for)?;
    let terminal = mapping::terminal_status(
        delivered_at.is_some(),
        cancelled_at.is_some(),
        dead_lettered_at.is_some(),
    );
    let status = mapping::status_with_paused(terminal, paused != 0);
    Ok(ScheduleSnapshot::new(
        schedule_id,
        status,
        scheduled_for,
        mapping::attempts_from_i64(attempts),
        mapping::attempts_from_i64(max_attempts),
        trigger,
        last_error,
    ))
}

/// SQLite implementation of [`ScheduleStore`] backed by `sqlx::SqlitePool`.
///
/// See the [module documentation](self) for the single-worker concurrency
/// model. Cheap to clone (the pool and the cached SQL strings are
/// reference-counted).
#[derive(Debug, Clone)]
pub struct SqliteScheduleStore {
    pool: SqlitePool,
    table_name: Arc<str>,
    insert_sql: Arc<str>,
    claim_sql: Arc<str>,
    mark_delivered_sql: Arc<str>,
    reschedule_sql: Arc<str>,
    mark_failed_sql: Arc<str>,
    mark_dead_lettered_sql: Arc<str>,
    cancel_sql: Arc<str>,
    set_paused_sql: Arc<str>,
    inspect_sql: Arc<str>,
    exists_sql: Arc<str>,
}

impl SqliteScheduleStore {
    /// Build a store for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: SqlitePool, table_name: impl Into<String>) -> Result<Self, SchedulerError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        Ok(Self {
            pool,
            insert_sql: Arc::from(statements::insert_sql(DIALECT, &table_name)),
            claim_sql: Arc::from(statements::claim_returning_sql(DIALECT, &table_name)),
            mark_delivered_sql: Arc::from(statements::mark_delivered_sql(DIALECT, &table_name)),
            reschedule_sql: Arc::from(statements::reschedule_sql(DIALECT, &table_name)),
            mark_failed_sql: Arc::from(statements::mark_failed_sql(DIALECT, &table_name)),
            mark_dead_lettered_sql: Arc::from(statements::mark_dead_lettered_sql(
                DIALECT,
                &table_name,
            )),
            cancel_sql: Arc::from(statements::cancel_sql(DIALECT, &table_name)),
            set_paused_sql: Arc::from(statements::set_paused_sql(DIALECT, &table_name)),
            inspect_sql: Arc::from(statements::inspect_sql(DIALECT, &table_name)),
            exists_sql: Arc::from(statements::exists_sql(DIALECT, &table_name)),
            table_name: Arc::from(table_name),
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

    /// Return [`SchedulerError::ScheduleNotFound`] when no schedule matches
    /// `schedule_id`, used to disambiguate a zero-row acknowledgement.
    async fn ensure_exists(&self, schedule_id: Uuid) -> Result<(), SchedulerError> {
        let row = sqlx::query(&self.exists_sql)
            .bind(schedule_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(database_error)?;
        if row.is_some() {
            Ok(())
        } else {
            Err(SchedulerError::schedule_not_found(schedule_id))
        }
    }
}

impl ScheduleStore for SqliteScheduleStore {
    async fn insert(
        &self,
        message: &ScheduledMessage,
        max_attempts: u32,
    ) -> Result<(), SchedulerError> {
        let (trigger_kind, cron_expr) = mapping::trigger_columns(&message.trigger)?;
        let (target_kind, routing_key) = mapping::target_columns(&message.target)?;
        let payload: serde_json::Value = serde_json::from_slice(&message.payload)?;
        sqlx::query(&self.insert_sql)
            .bind(message.schedule_id)
            .bind(&message.event_type)
            .bind(payload)
            .bind(trigger_kind)
            .bind(cron_expr)
            .bind(timestamp::format_sqlite_utc(message.scheduled_for)?)
            .bind(target_kind)
            .bind(routing_key)
            .bind(i64::from(mapping::max_attempts_to_i32(max_attempts)))
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn claim_due(
        &self,
        _now: SystemTime,
        batch_size: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedOccurrence>, SchedulerError> {
        let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let rows = sqlx::query(&self.claim_sql)
            .bind(sqlite_seconds_modifier(lease))
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(database_error)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in &rows {
            claimed.push(decode_leased(row)?);
        }
        Ok(claimed)
    }

    async fn mark_delivered(&self, schedule_id: Uuid) -> Result<(), SchedulerError> {
        sqlx::query(&self.mark_delivered_sql)
            .bind(schedule_id)
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn reschedule(&self, schedule_id: Uuid, next: SystemTime) -> Result<(), SchedulerError> {
        sqlx::query(&self.reschedule_sql)
            .bind(timestamp::format_sqlite_utc(next)?)
            .bind(schedule_id)
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn mark_failed(
        &self,
        schedule_id: Uuid,
        retry_at: SystemTime,
        error: &str,
    ) -> Result<(), SchedulerError> {
        sqlx::query(&self.mark_failed_sql)
            .bind(timestamp::format_sqlite_utc(retry_at)?)
            .bind(error)
            .bind(schedule_id)
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn mark_dead_lettered(
        &self,
        schedule_id: Uuid,
        error: &str,
    ) -> Result<(), SchedulerError> {
        sqlx::query(&self.mark_dead_lettered_sql)
            .bind(error)
            .bind(schedule_id)
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    async fn cancel(&self, schedule_id: Uuid) -> Result<(), SchedulerError> {
        let result = sqlx::query(&self.cancel_sql)
            .bind(schedule_id)
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        if result.rows_affected() == 0 {
            self.ensure_exists(schedule_id).await?;
        }
        Ok(())
    }

    async fn set_paused(&self, schedule_id: Uuid, paused: bool) -> Result<(), SchedulerError> {
        let result = sqlx::query(&self.set_paused_sql)
            .bind(paused)
            .bind(schedule_id)
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        if result.rows_affected() == 0 {
            self.ensure_exists(schedule_id).await?;
        }
        Ok(())
    }

    async fn inspect(&self, schedule_id: Uuid) -> Result<Option<ScheduleSnapshot>, SchedulerError> {
        let row = sqlx::query(&self.inspect_sql)
            .bind(schedule_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(database_error)?;
        row.as_ref().map(decode_snapshot).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lazy_pool() -> SqlitePool {
        SqlitePool::connect_lazy("sqlite::memory:").expect("lazy pool must build from a valid URL")
    }

    #[tokio::test]
    async fn new_rejects_an_invalid_table_name() {
        let error = SqliteScheduleStore::new(lazy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[tokio::test]
    async fn new_caches_sqlite_sql_without_skip_locked() {
        let store = SqliteScheduleStore::new(lazy_pool(), "scheduled_messages").unwrap();
        assert_eq!(store.table_name(), "scheduled_messages");
        assert!(
            store
                .insert_sql
                .contains("INSERT INTO \"scheduled_messages\"")
        );
        assert!(store.claim_sql.contains("RETURNING"));
        assert!(!store.claim_sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(store.claim_sql.contains("strftime"));
        assert!(
            store
                .exists_sql
                .contains("SELECT 1 FROM \"scheduled_messages\"")
        );
    }

    #[test]
    fn sqlite_seconds_modifier_caps_a_huge_lease() {
        let modifier = sqlite_seconds_modifier(Duration::MAX);
        assert!(!modifier.contains("inf"));
        assert!(modifier.starts_with('+'));
        assert!(modifier.ends_with(" seconds"));
    }

    #[test]
    fn sqlite_seconds_modifier_preserves_ordinary_values() {
        assert_eq!(
            sqlite_seconds_modifier(Duration::from_millis(1_500)),
            "+1.500 seconds"
        );
    }
}
