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
        let mut rows: Vec<&StoredSchedule> = schedules
            .values()
            .filter(|stored| stored.status == ScheduleStatus::DeadLettered)
            .collect();
        // Most recently dead-lettered first, matching the SQL backends'
        // `ORDER BY dead_lettered_at DESC`.
        rows.sort_by(|a, b| b.dead_lettered_at.cmp(&a.dead_lettered_at));
        rows.truncate(limit);
        Ok(rows.into_iter().map(StoredSchedule::to_snapshot).collect())
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
        stored.dead_lettered_at = None;
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

    const LEASE: Duration = Duration::from_secs(30);

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
        let delivered = insert_delay(&store, base(), 5).await;
        store
            .claim_due(base(), 10, LEASE)
            .await
            .expect("claim delivered");
        store.mark_delivered(delivered).await.expect("deliver");
        let listed = store.list_pending(10).await.expect("list");
        let ids: Vec<Uuid> = listed.iter().map(|s| s.schedule_id).collect();
        assert!(ids.contains(&paused));
        assert!(!ids.contains(&cancelled));
        assert!(
            !ids.contains(&delivered),
            "a delivered schedule is terminal"
        );
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
    async fn list_dead_letter_orders_most_recently_dead_lettered_first() {
        let store = InMemoryScheduleStore::default();
        let first = insert_delay(&store, base(), 5).await;
        store
            .mark_dead_lettered(first, "first")
            .await
            .expect("dead letter first");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let second = insert_delay(&store, base(), 5).await;
        store
            .mark_dead_lettered(second, "second")
            .await
            .expect("dead letter second");

        let listed = store.list_dead_letter(10).await.expect("list");
        assert_eq!(listed.len(), 2);
        assert_eq!(
            listed[0].schedule_id, second,
            "the most recently dead-lettered schedule must come first"
        );
        assert_eq!(listed[1].schedule_id, first);
    }

    #[tokio::test]
    async fn replay_resets_dead_letter_to_pending() {
        let store = InMemoryScheduleStore::default();
        let dead = insert_delay(&store, base(), 5).await;
        // Drive attempts above zero via a claim before dead-lettering, so the
        // reset asserted below actually exercises the reset rather than a
        // counter that was already zero.
        let claimed = store.claim_due(base(), 10, LEASE).await.expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempts, 1);
        store.mark_failed(dead, base(), "err").await.expect("fail");
        store.mark_dead_lettered(dead, "boom").await.expect("dead");
        store.replay(dead).await.expect("replay");
        let snapshot = store.inspect(dead).await.expect("inspect").expect("exists");
        assert_eq!(snapshot.status, ScheduleStatus::Pending);
        assert_eq!(
            snapshot.attempts, 0,
            "replay must reset the attempt counter"
        );
        assert_eq!(snapshot.last_error, None);
    }

    #[tokio::test]
    async fn replay_rejects_non_dead_lettered() {
        let store = InMemoryScheduleStore::default();
        let pending = insert_delay(&store, base(), 5).await;
        let error = store.replay(pending).await.expect_err("must reject");
        assert!(matches!(error, SchedulerError::NotReplayable { .. }));
    }

    #[test]
    fn not_replayable_display_includes_schedule_id_and_status() {
        let schedule_id = Uuid::from_u128(7);
        let error = SchedulerError::not_replayable(schedule_id, ScheduleStatus::Pending);
        let message = error.to_string();
        assert!(message.contains(&schedule_id.to_string()));
        assert!(message.contains("Pending"));
    }

    use crate::SchedulerError;
}
