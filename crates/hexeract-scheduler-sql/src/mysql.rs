//! MySQL backend for the Hexeract scheduler.
//!
//! [`MySqlScheduleStore`] implements [`ScheduleStore`] on `sqlx::MySqlPool`.
//!
//! # Claiming without `RETURNING`
//!
//! MySQL supports neither `UPDATE ... RETURNING` nor a `FOR UPDATE SKIP
//! LOCKED` that also returns the updated rows, so the claim runs as a short
//! internal transaction: it selects and locks the due rows with `FOR UPDATE
//! SKIP LOCKED`, leases them and consumes one attempt, then reselects the
//! leased rows. The [`ScheduleStore`] contract stays free of any cross-method
//! transaction: the transaction is an implementation detail of this one
//! method.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use hexeract_outbox_sql::Dialect;
use hexeract_scheduler::LeasedOccurrence;
use hexeract_scheduler::ScheduleSnapshot;
use hexeract_scheduler::ScheduleStore;
use hexeract_scheduler::ScheduledMessage;
use hexeract_scheduler::SchedulerError;
use sqlx::MySqlPool;
use sqlx::Row;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::mapping;
use crate::statements;
use crate::timestamp;
use crate::validate::validate_table_name;

const DIALECT: Dialect = Dialect::MySql;

/// Convert a lease [`Duration`] into whole microseconds for binding to a MySQL
/// `INTERVAL ? MICROSECOND` expression, saturating at [`i64::MAX`].
fn duration_to_micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

fn database_error(error: impl std::error::Error + Send + Sync + 'static) -> SchedulerError {
    SchedulerError::database(error)
}

/// Decode a claimed row into a [`LeasedOccurrence`].
fn decode_leased(row: &sqlx::mysql::MySqlRow) -> Result<LeasedOccurrence, SchedulerError> {
    let schedule_id: Uuid = row.try_get("schedule_id").map_err(database_error)?;
    let event_type: String = row.try_get("event_type").map_err(database_error)?;
    let payload: serde_json::Value = row.try_get("payload").map_err(database_error)?;
    let trigger_kind: String = row.try_get("trigger_kind").map_err(database_error)?;
    let cron_expr: Option<String> = row.try_get("cron_expr").map_err(database_error)?;
    let scheduled_for: PrimitiveDateTime = row.try_get("scheduled_for").map_err(database_error)?;
    let target_kind: String = row.try_get("target_kind").map_err(database_error)?;
    let routing_key: Option<String> = row.try_get("target_routing_key").map_err(database_error)?;
    let attempts: i32 = row.try_get("attempts").map_err(database_error)?;
    let max_attempts: i32 = row.try_get("max_attempts").map_err(database_error)?;
    let leased_until: PrimitiveDateTime = row.try_get("leased_until").map_err(database_error)?;

    let payload = serde_json::to_vec(&payload)?;
    let scheduled_for = timestamp::from_primitive_utc(scheduled_for);
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
        timestamp::from_primitive_utc(leased_until),
    ))
}

/// Decode an inspected row into a [`ScheduleSnapshot`].
fn decode_snapshot(row: &sqlx::mysql::MySqlRow) -> Result<ScheduleSnapshot, SchedulerError> {
    let schedule_id: Uuid = row.try_get("schedule_id").map_err(database_error)?;
    let trigger_kind: String = row.try_get("trigger_kind").map_err(database_error)?;
    let cron_expr: Option<String> = row.try_get("cron_expr").map_err(database_error)?;
    let scheduled_for: PrimitiveDateTime = row.try_get("scheduled_for").map_err(database_error)?;
    let attempts: i32 = row.try_get("attempts").map_err(database_error)?;
    let max_attempts: i32 = row.try_get("max_attempts").map_err(database_error)?;
    let paused: bool = row.try_get("paused").map_err(database_error)?;
    let last_error: Option<String> = row.try_get("last_error").map_err(database_error)?;
    let delivered_at: Option<PrimitiveDateTime> =
        row.try_get("delivered_at").map_err(database_error)?;
    let cancelled_at: Option<PrimitiveDateTime> =
        row.try_get("cancelled_at").map_err(database_error)?;
    let dead_lettered_at: Option<PrimitiveDateTime> =
        row.try_get("dead_lettered_at").map_err(database_error)?;

    let scheduled_for = timestamp::from_primitive_utc(scheduled_for);
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

/// MySQL implementation of [`ScheduleStore`] backed by `sqlx::MySqlPool`.
///
/// Cheap to clone (the pool and the cached SQL strings are reference-counted).
#[derive(Debug, Clone)]
pub struct MySqlScheduleStore {
    pool: MySqlPool,
    table_name: Arc<str>,
    insert_sql: Arc<str>,
    claim_select_sql: Arc<str>,
    mark_delivered_sql: Arc<str>,
    reschedule_sql: Arc<str>,
    mark_dead_lettered_sql: Arc<str>,
    cancel_sql: Arc<str>,
    set_paused_sql: Arc<str>,
    inspect_sql: Arc<str>,
    exists_sql: Arc<str>,
}

impl MySqlScheduleStore {
    /// Build a store for the given pool and table.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Internal`] if `table_name` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn new(pool: MySqlPool, table_name: impl Into<String>) -> Result<Self, SchedulerError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        Ok(Self {
            pool,
            insert_sql: Arc::from(statements::insert_sql(DIALECT, &table_name)),
            claim_select_sql: Arc::from(statements::mysql_claim_select_sql(&table_name)),
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
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Configured table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Return [`SchedulerError::ScheduleNotFound`] when no schedule matches
    /// `schedule_id`.
    ///
    /// MySQL reports affected (changed) rows rather than matched rows, so a
    /// `set_paused` to an unchanged value updates nothing even when the
    /// schedule exists; the acknowledgement methods fall back to this probe
    /// before deciding the schedule is absent.
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

impl ScheduleStore for MySqlScheduleStore {
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
            .bind(timestamp::to_primitive_utc(message.scheduled_for))
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
        let mut tx = self.pool.begin().await.map_err(database_error)?;

        // Step 1: select and lock the due rows.
        let locked = sqlx::query(&self.claim_select_sql)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(database_error)?;
        let ids: Vec<Uuid> = locked
            .iter()
            .map(|row| row.try_get("schedule_id"))
            .collect::<Result<_, _>>()
            .map_err(database_error)?;
        if ids.is_empty() {
            tx.commit().await.map_err(database_error)?;
            return Ok(Vec::new());
        }

        // Step 2: lease the locked rows and consume one attempt.
        let update_sql = statements::mysql_claim_update_sql(&self.table_name, ids.len());
        let mut update = sqlx::query(&update_sql).bind(duration_to_micros(lease));
        for id in &ids {
            update = update.bind(*id);
        }
        update.execute(&mut *tx).await.map_err(database_error)?;

        // Step 3: read the leased rows back with their stamped lease and
        // incremented attempt counter.
        let reselect_sql = statements::mysql_claim_reselect_sql(&self.table_name, ids.len());
        let mut reselect = sqlx::query(&reselect_sql);
        for id in &ids {
            reselect = reselect.bind(*id);
        }
        let rows = reselect.fetch_all(&mut *tx).await.map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;

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
            .bind(timestamp::to_primitive_utc(next))
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

    fn lazy_pool() -> MySqlPool {
        MySqlPool::connect_lazy("mysql://nobody:nobody@127.0.0.1:1/nobody")
            .expect("lazy pool must build from a valid URL")
    }

    #[tokio::test]
    async fn new_rejects_an_invalid_table_name() {
        let error = MySqlScheduleStore::new(lazy_pool(), "bad name; DROP").unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[tokio::test]
    async fn new_caches_mysql_sql_with_backtick_quoting() {
        let store = MySqlScheduleStore::new(lazy_pool(), "scheduled_messages").unwrap();
        assert_eq!(store.table_name(), "scheduled_messages");
        assert!(
            store
                .insert_sql
                .contains("INSERT INTO `scheduled_messages`")
        );
        assert!(!store.insert_sql.contains('"'));
        assert!(store.claim_select_sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(
            store
                .exists_sql
                .contains("SELECT 1 FROM `scheduled_messages`")
        );
    }

    #[test]
    fn duration_to_micros_saturates_a_huge_lease() {
        assert_eq!(duration_to_micros(Duration::from_micros(1_500)), 1_500);
        assert_eq!(duration_to_micros(Duration::MAX), i64::MAX);
    }
}
