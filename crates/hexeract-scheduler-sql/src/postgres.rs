//! PostgreSQL backend for the Hexeract scheduler.
//!
//! [`PgScheduleStore`] implements [`ScheduleStore`] on `sqlx::PgPool`. The
//! claim is a single `UPDATE ... RETURNING` driven by a `FOR UPDATE SKIP
//! LOCKED` CTE, so competing workers never contend for the same due
//! occurrence.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use hexeract_outbox_sql::Dialect;
use hexeract_scheduler::LeasedOccurrence;
use hexeract_scheduler::ScheduleSnapshot;
use hexeract_scheduler::ScheduleStore;
use hexeract_scheduler::ScheduledMessage;
use hexeract_scheduler::SchedulerError;
use sqlx::PgPool;
use sqlx::Row;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::mapping;
use crate::statements;
use crate::timestamp;
use crate::validate::validate_table_name;

const DIALECT: Dialect = Dialect::Postgres;

/// Maximum interval in seconds that can be safely cast to `DOUBLE PRECISION`
/// and added to a PostgreSQL timestamp. A duration near [`Duration::MAX`]
/// overflows to `inf`, which PostgreSQL rejects; capping at roughly 292 years
/// stays far beyond any practical lease.
const MAX_PG_INTERVAL_SECS: f64 = 9_223_372_036.0;

/// Convert a lease [`Duration`] to seconds for binding as `DOUBLE PRECISION`,
/// capped so a pathologically large value does not produce `inf`.
fn duration_to_pg_secs(d: Duration) -> f64 {
    d.as_secs_f64().min(MAX_PG_INTERVAL_SECS)
}

fn database_error(error: impl std::error::Error + Send + Sync + 'static) -> SchedulerError {
    SchedulerError::database(error)
}

/// Decode a claimed row into a [`LeasedOccurrence`].
fn decode_leased(row: &sqlx::postgres::PgRow) -> Result<LeasedOccurrence, SchedulerError> {
    let schedule_id: Uuid = row.try_get("schedule_id").map_err(database_error)?;
    let event_type: String = row.try_get("event_type").map_err(database_error)?;
    let payload: serde_json::Value = row.try_get("payload").map_err(database_error)?;
    let trigger_kind: String = row.try_get("trigger_kind").map_err(database_error)?;
    let cron_expr: Option<String> = row.try_get("cron_expr").map_err(database_error)?;
    let scheduled_for: OffsetDateTime = row.try_get("scheduled_for").map_err(database_error)?;
    let target_kind: String = row.try_get("target_kind").map_err(database_error)?;
    let routing_key: Option<String> = row.try_get("target_routing_key").map_err(database_error)?;
    let attempts: i32 = row.try_get("attempts").map_err(database_error)?;
    let max_attempts: i32 = row.try_get("max_attempts").map_err(database_error)?;
    let leased_until: OffsetDateTime = row.try_get("leased_until").map_err(database_error)?;

    let payload = serde_json::to_vec(&payload)?;
    let scheduled_for = timestamp::from_offset_date_time(scheduled_for);
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
        mapping::attempts_from_i64(i64::from(attempts)),
        mapping::attempts_from_i64(i64::from(max_attempts)),
        timestamp::from_offset_date_time(leased_until),
    ))
}

/// Decode an inspected row into a [`ScheduleSnapshot`].
fn decode_snapshot(row: &sqlx::postgres::PgRow) -> Result<ScheduleSnapshot, SchedulerError> {
    let schedule_id: Uuid = row.try_get("schedule_id").map_err(database_error)?;
    let trigger_kind: String = row.try_get("trigger_kind").map_err(database_error)?;
    let cron_expr: Option<String> = row.try_get("cron_expr").map_err(database_error)?;
    let scheduled_for: OffsetDateTime = row.try_get("scheduled_for").map_err(database_error)?;
    let attempts: i32 = row.try_get("attempts").map_err(database_error)?;
    let max_attempts: i32 = row.try_get("max_attempts").map_err(database_error)?;
    let paused: bool = row.try_get("paused").map_err(database_error)?;
    let last_error: Option<String> = row.try_get("last_error").map_err(database_error)?;
    let delivered_at: Option<OffsetDateTime> =
        row.try_get("delivered_at").map_err(database_error)?;
    let cancelled_at: Option<OffsetDateTime> =
        row.try_get("cancelled_at").map_err(database_error)?;
    let dead_lettered_at: Option<OffsetDateTime> =
        row.try_get("dead_lettered_at").map_err(database_error)?;

    let scheduled_for = timestamp::from_offset_date_time(scheduled_for);
    let trigger = mapping::build_trigger(&trigger_kind, cron_expr, scheduled_for)?;
    let terminal = mapping::terminal_status(
        delivered_at.is_some(),
        cancelled_at.is_some(),
        dead_lettered_at.is_some(),
    );
    let status = mapping::status_with_paused(terminal, paused);
    Ok(ScheduleSnapshot::new(
        schedule_id,
        status,
        scheduled_for,
        mapping::attempts_from_i64(i64::from(attempts)),
        mapping::attempts_from_i64(i64::from(max_attempts)),
        trigger,
        last_error,
    ))
}

/// PostgreSQL implementation of [`ScheduleStore`] backed by `sqlx::PgPool`.
///
/// Cheap to clone (the pool and the cached SQL strings are reference-counted).
#[derive(Debug, Clone)]
pub struct PgScheduleStore {
    pool: PgPool,
    table_name: Arc<str>,
    insert_sql: Arc<str>,
    claim_sql: Arc<str>,
    mark_delivered_sql: Arc<str>,
    reschedule_sql: Arc<str>,
    mark_dead_lettered_sql: Arc<str>,
    cancel_sql: Arc<str>,
    set_paused_sql: Arc<str>,
    inspect_sql: Arc<str>,
    exists_sql: Arc<str>,
}

impl PgScheduleStore {
    /// Build a store for the given pool and table.
    ///
    /// SQL statements are templated and cached at construction so each cycle
    /// reuses the same strings.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: PgPool, table_name: impl Into<String>) -> Result<Self, SchedulerError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        Ok(Self {
            pool,
            insert_sql: Arc::from(statements::insert_sql(DIALECT, &table_name)),
            claim_sql: Arc::from(statements::claim_returning_sql(DIALECT, &table_name)),
            mark_delivered_sql: Arc::from(statements::mark_delivered_sql(DIALECT, &table_name)),
            reschedule_sql: Arc::from(statements::reschedule_sql(DIALECT, &table_name)),
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
    pub fn pool(&self) -> &PgPool {
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

impl ScheduleStore for PgScheduleStore {
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
            .bind(timestamp::to_offset_date_time(message.scheduled_for))
            .bind(target_kind)
            .bind(routing_key)
            .bind(mapping::max_attempts_to_i32(max_attempts))
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
            .bind(duration_to_pg_secs(lease))
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
            .bind(timestamp::to_offset_date_time(next))
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

    fn lazy_pool() -> PgPool {
        PgPool::connect_lazy("postgres://nobody:nobody@127.0.0.1:1/nobody")
            .expect("lazy pool must build from a valid URL")
    }

    #[tokio::test]
    async fn new_rejects_an_invalid_table_name() {
        let error = PgScheduleStore::new(lazy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[tokio::test]
    async fn new_caches_postgres_sql_with_the_validated_table_name() {
        let store = PgScheduleStore::new(lazy_pool(), "scheduled_messages").unwrap();
        assert_eq!(store.table_name(), "scheduled_messages");
        assert!(
            store
                .insert_sql
                .contains("INSERT INTO \"scheduled_messages\"")
        );
        assert!(store.claim_sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(store.claim_sql.contains("RETURNING"));
        assert!(store.inspect_sql.contains("FROM \"scheduled_messages\""));
        assert!(
            store
                .exists_sql
                .contains("SELECT 1 FROM \"scheduled_messages\"")
        );
    }

    #[test]
    fn duration_to_pg_secs_caps_a_huge_lease() {
        assert!(duration_to_pg_secs(Duration::MAX).is_finite());
        assert!((duration_to_pg_secs(Duration::from_secs(30)) - 30.0_f64).abs() < f64::EPSILON);
    }
}
