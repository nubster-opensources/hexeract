//! High-level lifecycle facade for a single schedule.
//!
//! [`SchedulerControl`] wraps a [`ScheduleStore`] and provides the four
//! operations a caller needs to drive a schedule through its lifecycle:
//! [`inspect`](SchedulerControl::inspect),
//! [`pause`](SchedulerControl::pause),
//! [`cancel`](SchedulerControl::cancel) and
//! [`resume`](SchedulerControl::resume).
//!
//! The facade adds two guards that are not present at the store level:
//!
//! - `cancel` is a no-op when the schedule is already in a terminal state
//!   (Delivered, Cancelled, DeadLettered), so callers do not need to check
//!   first.
//! - `resume` realigns a past-due paused cron schedule to the next strictly
//!   future occurrence instead of letting it fire immediately for every missed
//!   tick (no catch-up).

use std::sync::Arc;
use std::time::SystemTime;

use uuid::Uuid;

use crate::error::SchedulerError;
use crate::snapshot::ScheduleSnapshot;
use crate::snapshot::ScheduleStatus;
use crate::store::ScheduleStore;
use crate::trigger::Trigger;

/// Ergonomic facade for driving a single schedule through its lifecycle.
///
/// Wraps a shared [`ScheduleStore`] reference and exposes the four control
/// operations: [`inspect`](Self::inspect), [`pause`](Self::pause),
/// [`cancel`](Self::cancel) and [`resume`](Self::resume).
///
/// `SchedulerControl` is `Clone` whenever the underlying store is: the `Arc`
/// is cheap to duplicate.
pub struct SchedulerControl<S: ScheduleStore> {
    store: Arc<S>,
}

impl<S: ScheduleStore> SchedulerControl<S> {
    /// Wrap `store` in a new control handle.
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    /// Return a read-only snapshot of the schedule, or `None` if it does not
    /// exist.
    ///
    /// Delegates directly to [`ScheduleStore::inspect`].
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to read.
    pub async fn inspect(&self, id: Uuid) -> Result<Option<ScheduleSnapshot>, SchedulerError> {
        self.store.inspect(id).await
    }

    /// Pause the schedule, excluding it from future claims until resumed.
    ///
    /// Delegates to [`ScheduleStore::set_paused`] with `paused = true`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ScheduleNotFound`] if no schedule matches
    /// `id`, or [`SchedulerError::Database`] on a backend failure.
    pub async fn pause(&self, id: Uuid) -> Result<(), SchedulerError> {
        self.store.set_paused(id, true).await
    }

    /// Cancel the schedule, excluding it from future claims permanently.
    ///
    /// If the schedule is already in a terminal state (`Delivered`, `Cancelled`,
    /// `DeadLettered`) this is a silent no-op: the existing terminal status is
    /// preserved and `Ok(())` is returned without touching the store.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ScheduleNotFound`] if no schedule matches
    /// `id`, or [`SchedulerError::Database`] on a backend failure.
    pub async fn cancel(&self, id: Uuid) -> Result<(), SchedulerError> {
        let snapshot = self
            .store
            .inspect(id)
            .await?
            .ok_or_else(|| SchedulerError::schedule_not_found(id))?;
        if is_terminal(snapshot.status) {
            return Ok(());
        }
        self.store.cancel(id).await
    }

    /// Resume a paused schedule.
    ///
    /// For a recurring cron schedule whose stored occurrence is in the past,
    /// the next strictly future occurrence is computed and the store is asked
    /// to realign to it (no catch-up fire). If the cron expression is
    /// exhausted (no future occurrence), the schedule remains paused and
    /// `Ok(())` is returned.
    ///
    /// For a one-shot delay schedule, or a cron whose stored occurrence is
    /// still in the future, the schedule is simply unpaused with the existing
    /// occurrence intact.
    ///
    /// Idempotent: a no-op returning `Ok(())` if the schedule is not currently
    /// Paused.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ScheduleNotFound`] if no schedule matches
    /// `id`, or [`SchedulerError::Database`] on a backend failure.
    pub async fn resume(&self, id: Uuid) -> Result<(), SchedulerError> {
        let snap = self
            .store
            .inspect(id)
            .await?
            .ok_or_else(|| SchedulerError::schedule_not_found(id))?;
        if snap.status != ScheduleStatus::Paused {
            return Ok(());
        }
        let now = SystemTime::now();
        match &snap.trigger {
            Trigger::Cron(expr) if snap.scheduled_for <= now => {
                match expr.next_occurrence(now)? {
                    Some(next) => self.store.resume(id, Some(next)).await,
                    // The cron expression is exhausted: leave paused, no catch-up.
                    None => Ok(()),
                }
            }
            // Delay, or cron still in the future: just unpause.
            _ => self.store.resume(id, None).await,
        }
    }
}

/// Return `true` for states from which a schedule cannot be revived.
fn is_terminal(status: ScheduleStatus) -> bool {
    matches!(
        status,
        ScheduleStatus::Delivered | ScheduleStatus::Cancelled | ScheduleStatus::DeadLettered
    )
}

#[cfg(test)]
mod tests {
    use super::SchedulerControl;
    use crate::error::SchedulerError;
    use crate::memory::InMemoryScheduleStore;
    use crate::schedule::ScheduledMessage;
    use crate::snapshot::ScheduleStatus;
    use crate::store::ScheduleStore;
    use crate::target::Target;
    use hexeract_outbox::Event;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue {
        user_id: Uuid,
    }

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    fn event() -> ReminderDue {
        ReminderDue {
            user_id: Uuid::nil(),
        }
    }

    /// An instant safely in the past so `claim_due(SystemTime::now(), ...)` can pick it up.
    fn past(secs: u64) -> SystemTime {
        SystemTime::now()
            .checked_sub(Duration::from_secs(secs))
            .unwrap_or(UNIX_EPOCH)
    }

    fn delay_message(at: SystemTime) -> ScheduledMessage {
        ScheduledMessage::delay(Target::mediator(), at, &event()).expect("serializes")
    }

    fn cron_message(first: SystemTime) -> ScheduledMessage {
        // "* * * * *" fires every minute, always has future occurrences.
        ScheduledMessage::cron(Target::mediator(), "* * * * *", first, &event())
            .expect("valid cron")
    }

    fn make_control() -> (
        Arc<InMemoryScheduleStore>,
        SchedulerControl<InMemoryScheduleStore>,
    ) {
        let store = Arc::new(InMemoryScheduleStore::new());
        let control = SchedulerControl::new(Arc::clone(&store));
        (store, control)
    }

    // Insert a schedule and return its id.
    async fn insert(store: &InMemoryScheduleStore, message: &ScheduledMessage) -> Uuid {
        let id = message.schedule_id;
        store.insert(message, 5).await.expect("insert");
        id
    }

    #[tokio::test]
    async fn pause_then_inspect_shows_paused() {
        let (store, control) = make_control();
        let msg = delay_message(past(60));
        let id = insert(&store, &msg).await;

        control.pause(id).await.unwrap();

        let snap = control.inspect(id).await.unwrap().unwrap();
        assert_eq!(snap.status, ScheduleStatus::Paused);
    }

    #[tokio::test]
    async fn resume_of_past_due_cron_produces_strictly_future_occurrence() {
        let (store, control) = make_control();
        // Use a cron with an occurrence far in the past so the guard fires.
        let first = past(7_200); // two hours ago
        let msg = cron_message(first);
        let id = insert(&store, &msg).await;

        control.pause(id).await.unwrap();
        control.resume(id).await.unwrap();

        let snap = control.inspect(id).await.unwrap().unwrap();
        assert_eq!(
            snap.status,
            ScheduleStatus::Pending,
            "must be Pending after resume"
        );
        assert!(
            snap.scheduled_for > SystemTime::now(),
            "occurrence must be strictly in the future after realignment"
        );
    }

    #[tokio::test]
    async fn resume_of_paused_delay_unpauses_without_changing_scheduled_for() {
        let (store, control) = make_control();
        let at = past(60);
        let msg = delay_message(at);
        let id = insert(&store, &msg).await;

        control.pause(id).await.unwrap();
        control.resume(id).await.unwrap();

        let snap = control.inspect(id).await.unwrap().unwrap();
        assert_eq!(snap.status, ScheduleStatus::Pending);
        assert_eq!(
            snap.scheduled_for, at,
            "delay scheduled_for must be unchanged"
        );
    }

    #[tokio::test]
    async fn cancel_on_delivered_schedule_is_no_op() {
        let (store, control) = make_control();
        let msg = delay_message(past(60));
        let id = insert(&store, &msg).await;
        // Deliver the schedule directly via the store.
        store
            .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
            .await
            .unwrap();
        store.mark_delivered(id).await.unwrap();

        // Cancel must be a no-op (no ScheduleNotFound, no status change).
        control.cancel(id).await.unwrap();

        let snap = control.inspect(id).await.unwrap().unwrap();
        assert_eq!(
            snap.status,
            ScheduleStatus::Delivered,
            "cancel must not overwrite a terminal Delivered status"
        );
    }

    #[tokio::test]
    async fn cancel_on_pending_schedule_moves_it_to_cancelled() {
        let (store, control) = make_control();
        let msg = delay_message(past(60));
        let id = insert(&store, &msg).await;

        control.cancel(id).await.unwrap();

        let snap = control.inspect(id).await.unwrap().unwrap();
        assert_eq!(snap.status, ScheduleStatus::Cancelled);
    }

    #[tokio::test]
    async fn lifecycle_ops_on_unknown_id_return_not_found() {
        let (_, control) = make_control();
        let missing = Uuid::from_u128(0xdead);

        let e1 = control.pause(missing).await.unwrap_err();
        assert!(matches!(e1, SchedulerError::ScheduleNotFound { .. }));

        let e2 = control.cancel(missing).await.unwrap_err();
        assert!(matches!(e2, SchedulerError::ScheduleNotFound { .. }));

        let e3 = control.resume(missing).await.unwrap_err();
        assert!(matches!(e3, SchedulerError::ScheduleNotFound { .. }));

        let none = control.inspect(missing).await.unwrap();
        assert!(none.is_none());
    }
}
