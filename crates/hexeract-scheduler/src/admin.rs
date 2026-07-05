//! Operator surface over scheduled work: read-only listing plus replay of
//! dead-lettered schedules, kept separate from [`ScheduleStore`] so the worker
//! hot path never depends on listing or administrative operations.

use uuid::Uuid;

use crate::memory::StoredSchedule;
use crate::{ScheduleSnapshot, ScheduleStore, SchedulerError};

/// Read and administrative operations an operator performs against the store.
#[trait_variant::make(Send)]
pub trait ScheduleAdmin: ScheduleStore {
    /// Non-terminal schedules (pending and paused), earliest `scheduled_for`
    /// first, capped at `limit`.
    async fn list_pending(&self, limit: usize) -> Result<Vec<ScheduleSnapshot>, SchedulerError>;

    /// Dead-lettered schedules, most recently dead-lettered first, capped at
    /// `limit`.
    async fn list_dead_letter(&self, limit: usize)
    -> Result<Vec<ScheduleSnapshot>, SchedulerError>;

    /// Return a dead-lettered schedule to pending: attempts reset to zero,
    /// `scheduled_for` set to now, `last_error` cleared. Refuses any schedule
    /// that is not currently dead-lettered.
    async fn replay(&self, schedule_id: Uuid) -> Result<(), SchedulerError>;
}

use crate::{InMemoryScheduleStore, ScheduleStatus};

impl ScheduleAdmin for InMemoryScheduleStore {
    async fn list_pending(&self, limit: usize) -> Result<Vec<ScheduleSnapshot>, SchedulerError> {
        let schedules = self.lock()?;
        let mut rows: Vec<ScheduleSnapshot> = schedules
            .values()
            .filter(|stored| {
                matches!(
                    stored.status,
                    ScheduleStatus::Pending | ScheduleStatus::Paused
                )
            })
            .map(StoredSchedule::to_snapshot)
            .collect();
        rows.sort_by_key(|snapshot| snapshot.scheduled_for);
        rows.truncate(limit);
        Ok(rows)
    }

    async fn list_dead_letter(
        &self,
        limit: usize,
    ) -> Result<Vec<ScheduleSnapshot>, SchedulerError> {
        let schedules = self.lock()?;
        let mut rows: Vec<ScheduleSnapshot> = schedules
            .values()
            .filter(|stored| stored.status == ScheduleStatus::DeadLettered)
            .map(StoredSchedule::to_snapshot)
            .collect();
        rows.sort_by(|a, b| b.scheduled_for.cmp(&a.scheduled_for));
        rows.truncate(limit);
        Ok(rows)
    }

    async fn replay(&self, schedule_id: Uuid) -> Result<(), SchedulerError> {
        let mut schedules = self.lock()?;
        let stored = schedules
            .get_mut(&schedule_id)
            .ok_or_else(|| SchedulerError::schedule_not_found(schedule_id))?;
        if stored.status != ScheduleStatus::DeadLettered {
            return Err(SchedulerError::not_replayable(schedule_id, stored.status));
        }
        stored.status = ScheduleStatus::Pending;
        stored.attempts = 0;
        stored.last_error = None;
        stored.leased_until = None;
        stored.message.scheduled_for = std::time::SystemTime::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::{
        InMemoryScheduleStore, ScheduleAdmin, ScheduleStatus, ScheduleStore, ScheduledMessage,
        Target,
    };
    use hexeract_outbox::Event;

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue;

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    fn base() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_000)
    }

    async fn insert_delay(
        store: &InMemoryScheduleStore,
        at: SystemTime,
        max_attempts: u32,
    ) -> Uuid {
        let message =
            ScheduledMessage::delay(Target::mediator(), at, &ReminderDue).expect("serializes");
        let schedule_id = message.schedule_id;
        store.insert(&message, max_attempts).await.expect("insert");
        schedule_id
    }

    #[tokio::test]
    async fn list_pending_orders_by_due_and_respects_limit() {
        let store = InMemoryScheduleStore::default();
        let later = insert_delay(&store, base() + Duration::from_secs(200), 5).await;
        let sooner = insert_delay(&store, base() + Duration::from_secs(100), 5).await;
        let listed = store.list_pending(10).await.expect("list");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].schedule_id, sooner);
        assert_eq!(listed[1].schedule_id, later);
        let capped = store.list_pending(1).await.expect("list");
        assert_eq!(capped.len(), 1);
        assert_eq!(capped[0].schedule_id, sooner);
    }

    #[tokio::test]
    async fn list_pending_includes_paused_excludes_terminal() {
        let store = InMemoryScheduleStore::default();
        let paused = insert_delay(&store, base(), 5).await;
        store.set_paused(paused, true).await.expect("pause");
        let cancelled = insert_delay(&store, base(), 5).await;
        store.cancel(cancelled).await.expect("cancel");
        let listed = store.list_pending(10).await.expect("list");
        let ids: Vec<Uuid> = listed.iter().map(|s| s.schedule_id).collect();
        assert!(ids.contains(&paused));
        assert!(!ids.contains(&cancelled));
    }

    #[tokio::test]
    async fn list_dead_letter_returns_only_dead_lettered() {
        let store = InMemoryScheduleStore::default();
        let dead = insert_delay(&store, base(), 5).await;
        store.mark_dead_lettered(dead, "boom").await.expect("dead");
        let alive = insert_delay(&store, base(), 5).await;
        let listed = store.list_dead_letter(10).await.expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].schedule_id, dead);
        assert_eq!(listed[0].last_error.as_deref(), Some("boom"));
        let _ = alive;
    }

    #[tokio::test]
    async fn replay_resets_dead_letter_to_pending() {
        let store = InMemoryScheduleStore::default();
        let dead = insert_delay(&store, base(), 5).await;
        store.mark_failed(dead, base(), "err").await.expect("fail");
        store.mark_dead_lettered(dead, "boom").await.expect("dead");
        store.replay(dead).await.expect("replay");
        let snapshot = store.inspect(dead).await.expect("inspect").expect("exists");
        assert_eq!(snapshot.status, ScheduleStatus::Pending);
        assert_eq!(snapshot.attempts, 0);
        assert_eq!(snapshot.last_error, None);
    }

    #[tokio::test]
    async fn replay_rejects_non_dead_lettered() {
        let store = InMemoryScheduleStore::default();
        let pending = insert_delay(&store, base(), 5).await;
        let error = store.replay(pending).await.expect_err("must reject");
        assert!(matches!(error, SchedulerError::NotReplayable { .. }));
    }

    use crate::SchedulerError;
}
