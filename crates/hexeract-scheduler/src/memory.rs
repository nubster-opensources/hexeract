use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Duration;
use std::time::SystemTime;

use uuid::Uuid;

use crate::error::SchedulerError;
use crate::lease::LeasedOccurrence;
use crate::schedule::ScheduledMessage;
use crate::snapshot::ScheduleSnapshot;
use crate::snapshot::ScheduleStatus;
use crate::store::ScheduleStore;

/// Stored row backing one schedule in the in-memory store.
struct StoredSchedule {
    message: ScheduledMessage,
    max_attempts: u32,
    attempts: u32,
    leased_until: Option<SystemTime>,
    status: ScheduleStatus,
    last_error: Option<String>,
}

/// An in-memory [`ScheduleStore`] for tests and the worker test harness.
///
/// It implements the full claim and lease contract in process, with no
/// database, and is the reference the worker is exercised against. Every
/// operation is synchronous under a mutex, so the returned futures resolve
/// immediately; it is not tuned for production throughput.
#[derive(Default)]
pub struct InMemoryScheduleStore {
    schedules: Mutex<HashMap<Uuid, StoredSchedule>>,
}

impl InMemoryScheduleStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, HashMap<Uuid, StoredSchedule>>, SchedulerError> {
        self.schedules
            .lock()
            .map_err(|_| SchedulerError::internal("schedule store mutex poisoned"))
    }
}

impl ScheduleStore for InMemoryScheduleStore {
    async fn insert(
        &self,
        message: &ScheduledMessage,
        max_attempts: u32,
    ) -> Result<(), SchedulerError> {
        let mut schedules = self.lock()?;
        schedules.insert(
            message.schedule_id,
            StoredSchedule {
                message: message.clone(),
                max_attempts,
                attempts: 0,
                leased_until: None,
                status: ScheduleStatus::Pending,
                last_error: None,
            },
        );
        Ok(())
    }

    async fn claim_due(
        &self,
        now: SystemTime,
        batch_size: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedOccurrence>, SchedulerError> {
        let leased_until = now
            .checked_add(lease)
            .ok_or_else(|| SchedulerError::internal("lease deadline overflow"))?;
        let mut schedules = self.lock()?;
        let mut claimed = Vec::new();
        for stored in schedules.values_mut() {
            if claimed.len() >= batch_size {
                break;
            }
            let is_due = stored.message.scheduled_for <= now;
            let lease_free = stored.leased_until.is_none_or(|until| until <= now);
            let has_budget = stored.attempts < stored.max_attempts;
            if stored.status == ScheduleStatus::Pending && is_due && lease_free && has_budget {
                stored.attempts += 1;
                stored.leased_until = Some(leased_until);
                claimed.push(LeasedOccurrence::new(
                    stored.message.clone(),
                    stored.attempts,
                    stored.max_attempts,
                    leased_until,
                ));
            }
        }
        Ok(claimed)
    }

    async fn mark_delivered(&self, schedule_id: Uuid) -> Result<(), SchedulerError> {
        let mut schedules = self.lock()?;
        if let Some(stored) = schedules.get_mut(&schedule_id) {
            if stored.status == ScheduleStatus::Pending {
                stored.status = ScheduleStatus::Delivered;
                stored.leased_until = None;
            }
        }
        Ok(())
    }

    async fn reschedule(&self, schedule_id: Uuid, next: SystemTime) -> Result<(), SchedulerError> {
        let mut schedules = self.lock()?;
        if let Some(stored) = schedules.get_mut(&schedule_id) {
            if stored.status == ScheduleStatus::Pending {
                stored.message.scheduled_for = next;
                stored.attempts = 0;
                stored.leased_until = None;
                stored.last_error = None;
            }
        }
        Ok(())
    }

    async fn mark_dead_lettered(
        &self,
        schedule_id: Uuid,
        error: &str,
    ) -> Result<(), SchedulerError> {
        let mut schedules = self.lock()?;
        if let Some(stored) = schedules.get_mut(&schedule_id) {
            if stored.status == ScheduleStatus::Pending {
                stored.status = ScheduleStatus::DeadLettered;
                stored.leased_until = None;
                stored.last_error = Some(error.to_owned());
            }
        }
        Ok(())
    }

    async fn cancel(&self, schedule_id: Uuid) -> Result<(), SchedulerError> {
        let mut schedules = self.lock()?;
        let stored = schedules
            .get_mut(&schedule_id)
            .ok_or_else(|| SchedulerError::schedule_not_found(schedule_id))?;
        stored.status = ScheduleStatus::Cancelled;
        stored.leased_until = None;
        Ok(())
    }

    async fn set_paused(&self, schedule_id: Uuid, paused: bool) -> Result<(), SchedulerError> {
        let mut schedules = self.lock()?;
        let stored = schedules
            .get_mut(&schedule_id)
            .ok_or_else(|| SchedulerError::schedule_not_found(schedule_id))?;
        match (paused, stored.status) {
            (true, ScheduleStatus::Pending) => stored.status = ScheduleStatus::Paused,
            (false, ScheduleStatus::Paused) => stored.status = ScheduleStatus::Pending,
            _ => {}
        }
        Ok(())
    }

    async fn inspect(&self, schedule_id: Uuid) -> Result<Option<ScheduleSnapshot>, SchedulerError> {
        let schedules = self.lock()?;
        Ok(schedules.get(&schedule_id).map(|stored| {
            ScheduleSnapshot::new(
                schedule_id,
                stored.status,
                stored.message.scheduled_for,
                stored.attempts,
                stored.max_attempts,
                stored.message.trigger.clone(),
                stored.last_error.clone(),
            )
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryScheduleStore;
    use crate::error::SchedulerError;
    use crate::schedule::ScheduledMessage;
    use crate::snapshot::ScheduleStatus;
    use crate::store::ScheduleStore;
    use crate::target::Target;
    use hexeract_outbox::Event;
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue;

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    const LEASE: Duration = Duration::from_secs(30);

    fn base() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_000)
    }

    fn delay_message(at: SystemTime) -> ScheduledMessage {
        ScheduledMessage::delay(Target::mediator(), at, &ReminderDue).expect("serializes")
    }

    async fn insert_delay(
        store: &InMemoryScheduleStore,
        at: SystemTime,
        max_attempts: u32,
    ) -> Uuid {
        let message = delay_message(at);
        let schedule_id = message.schedule_id;
        store
            .insert(&message, max_attempts)
            .await
            .expect("insert succeeds");
        schedule_id
    }

    #[tokio::test]
    async fn insert_then_inspect_reports_pending() {
        let store = InMemoryScheduleStore::default();
        let schedule_id = insert_delay(&store, base(), 5).await;
        let snapshot = store
            .inspect(schedule_id)
            .await
            .expect("inspect succeeds")
            .expect("schedule exists");
        assert_eq!(snapshot.status, ScheduleStatus::Pending);
        assert_eq!(snapshot.scheduled_for, base());
        assert_eq!(snapshot.attempts, 0);
        assert_eq!(snapshot.max_attempts, 5);
    }

    #[tokio::test]
    async fn claim_due_returns_due_schedules_and_increments_attempts() {
        let store = InMemoryScheduleStore::default();
        let schedule_id = insert_delay(&store, base(), 5).await;
        let claimed = store
            .claim_due(base(), 10, LEASE)
            .await
            .expect("claim succeeds");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempts, 1);
        assert_eq!(claimed[0].leased_until, base() + LEASE);
        let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
        assert_eq!(snapshot.attempts, 1);
    }

    #[tokio::test]
    async fn claim_due_excludes_not_yet_due() {
        let store = InMemoryScheduleStore::default();
        insert_delay(&store, base() + Duration::from_secs(100), 5).await;
        let claimed = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert!(claimed.is_empty());
    }

    #[tokio::test]
    async fn claim_due_excludes_paused() {
        let store = InMemoryScheduleStore::default();
        let schedule_id = insert_delay(&store, base(), 5).await;
        store.set_paused(schedule_id, true).await.unwrap();
        let claimed = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert!(claimed.is_empty());
    }

    #[tokio::test]
    async fn claim_due_excludes_cancelled() {
        let store = InMemoryScheduleStore::default();
        let schedule_id = insert_delay(&store, base(), 5).await;
        store.cancel(schedule_id).await.unwrap();
        let claimed = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert!(claimed.is_empty());
    }

    #[tokio::test]
    async fn claim_due_excludes_an_active_lease() {
        let store = InMemoryScheduleStore::default();
        insert_delay(&store, base(), 5).await;
        let first = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert_eq!(first.len(), 1);
        let second = store
            .claim_due(base() + Duration::from_secs(10), 10, LEASE)
            .await
            .unwrap();
        assert!(second.is_empty());
    }

    #[tokio::test]
    async fn an_expired_lease_is_reclaimed_as_the_same_occurrence() {
        let store = InMemoryScheduleStore::default();
        insert_delay(&store, base(), 5).await;
        let first = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].attempts, 1);
        let blocked = store
            .claim_due(base() + Duration::from_secs(10), 10, LEASE)
            .await
            .unwrap();
        assert!(blocked.is_empty());
        let reclaimed = store
            .claim_due(base() + Duration::from_secs(31), 10, LEASE)
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].attempts, 2);
        assert_eq!(reclaimed[0].occurrence_id(), first[0].occurrence_id());
    }

    #[tokio::test]
    async fn claim_due_excludes_delivered_schedules() {
        let store = InMemoryScheduleStore::default();
        let schedule_id = insert_delay(&store, base(), 5).await;
        store.claim_due(base(), 10, LEASE).await.unwrap();
        store.mark_delivered(schedule_id).await.unwrap();
        let claimed = store
            .claim_due(base() + Duration::from_secs(60), 10, LEASE)
            .await
            .unwrap();
        assert!(claimed.is_empty());
        let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, ScheduleStatus::Delivered);
    }

    #[tokio::test]
    async fn claim_due_respects_the_batch_size() {
        let store = InMemoryScheduleStore::default();
        for _ in 0..3 {
            insert_delay(&store, base(), 5).await;
        }
        let claimed = store.claim_due(base(), 2, LEASE).await.unwrap();
        assert_eq!(claimed.len(), 2);
    }

    #[tokio::test]
    async fn reschedule_advances_and_resets_attempts() {
        let store = InMemoryScheduleStore::default();
        let message =
            ScheduledMessage::cron(Target::outbox(), "0 0 * * *", base(), &ReminderDue).unwrap();
        let schedule_id = message.schedule_id;
        store.insert(&message, 5).await.unwrap();
        store.claim_due(base(), 10, LEASE).await.unwrap();

        let next = base() + Duration::from_secs(86_400);
        store.reschedule(schedule_id, next).await.unwrap();
        let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, ScheduleStatus::Pending);
        assert_eq!(snapshot.scheduled_for, next);
        assert_eq!(snapshot.attempts, 0);

        let too_early = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert!(too_early.is_empty());
        let claimed = store.claim_due(next, 10, LEASE).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempts, 1);
    }

    #[tokio::test]
    async fn mark_dead_lettered_excludes_and_reports() {
        let store = InMemoryScheduleStore::default();
        let schedule_id = insert_delay(&store, base(), 5).await;
        store.mark_dead_lettered(schedule_id, "boom").await.unwrap();
        let claimed = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert!(claimed.is_empty());
        let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, ScheduleStatus::DeadLettered);
        assert_eq!(snapshot.last_error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn cancel_unknown_schedule_errors() {
        let store = InMemoryScheduleStore::default();
        let error = store.cancel(Uuid::from_u128(123)).await.unwrap_err();
        assert!(matches!(error, SchedulerError::ScheduleNotFound { .. }));
    }

    #[tokio::test]
    async fn set_paused_unknown_schedule_errors() {
        let store = InMemoryScheduleStore::default();
        let error = store
            .set_paused(Uuid::from_u128(123), true)
            .await
            .unwrap_err();
        assert!(matches!(error, SchedulerError::ScheduleNotFound { .. }));
    }

    #[tokio::test]
    async fn resume_makes_a_schedule_claimable_again() {
        let store = InMemoryScheduleStore::default();
        let schedule_id = insert_delay(&store, base(), 5).await;
        store.set_paused(schedule_id, true).await.unwrap();
        assert!(store.claim_due(base(), 10, LEASE).await.unwrap().is_empty());
        store.set_paused(schedule_id, false).await.unwrap();
        let claimed = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert_eq!(claimed.len(), 1);
    }

    #[tokio::test]
    async fn inspect_unknown_schedule_returns_none() {
        let store = InMemoryScheduleStore::default();
        let snapshot = store.inspect(Uuid::from_u128(123)).await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn claim_due_excludes_exhausted_schedules() {
        let store = InMemoryScheduleStore::default();
        insert_delay(&store, base(), 1).await;
        let first = store.claim_due(base(), 10, LEASE).await.unwrap();
        assert_eq!(first.len(), 1);
        assert!(first[0].is_exhausted());
        let after_expiry = store
            .claim_due(base() + Duration::from_secs(31), 10, LEASE)
            .await
            .unwrap();
        assert!(after_expiry.is_empty());
    }
}
