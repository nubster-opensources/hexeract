//! The polling worker that drives schedules to their sink.

use core::fmt;
use std::time::Duration;
use std::time::SystemTime;

use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::lease::LeasedOccurrence;
use crate::schedule::ScheduledMessage;
use crate::sink::ScheduleSink;
use crate::store::ScheduleStore;
use crate::trigger::Trigger;

/// Tuning parameters for a [`SchedulerWorker`].
///
/// [`SchedulerWorkerConfig::default`] returns production-ready values.
#[derive(Debug, Clone)]
pub struct SchedulerWorkerConfig {
    /// How long to wait after an empty or failed poll cycle before polling
    /// again.
    pub poll_interval: Duration,
    /// Maximum number of occurrences claimed per cycle.
    pub batch_size: usize,
    /// Lease granted to each claimed occurrence: the window in which this
    /// worker must dispatch and acknowledge before another worker may reclaim.
    pub lease: Duration,
    /// Base delay of the exponential retry backoff.
    pub retry_base_delay: Duration,
    /// Upper bound of the exponential retry backoff.
    pub retry_max_delay: Duration,
    /// Whether to apply full jitter to the retry backoff.
    pub jitter: bool,
    /// Delay between consecutive non-empty cycles, to avoid a busy loop while
    /// still draining a backlog quickly.
    pub min_cycle_delay: Duration,
    /// Hard deadline for a single dispatch; a slower sink is treated as a
    /// failed attempt.
    pub dispatch_timeout: Duration,
}

impl Default for SchedulerWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            batch_size: 10,
            lease: Duration::from_secs(30),
            retry_base_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(300),
            jitter: true,
            min_cycle_delay: Duration::from_millis(5),
            dispatch_timeout: Duration::from_secs(30),
        }
    }
}

/// A polling worker that claims due occurrences from a [`ScheduleStore`],
/// dispatches them to a [`ScheduleSink`], and settles each one.
///
/// On success a one-shot schedule is marked delivered and a recurring
/// schedule is advanced to its next UTC occurrence. On failure the occurrence
/// is retried with bounded exponential backoff, or dead-lettered once its
/// attempt budget is exhausted.
pub struct SchedulerWorker<S, K>
where
    S: ScheduleStore,
    K: ScheduleSink,
{
    store: S,
    sink: K,
    config: SchedulerWorkerConfig,
}

impl<S, K> SchedulerWorker<S, K>
where
    S: ScheduleStore,
    K: ScheduleSink,
{
    /// Build a worker over `store` and `sink` with the given configuration.
    #[must_use]
    pub fn new(store: S, sink: K, config: SchedulerWorkerConfig) -> Self {
        Self {
            store,
            sink,
            config,
        }
    }

    /// Borrow the worker configuration.
    #[must_use]
    pub fn config(&self) -> &SchedulerWorkerConfig {
        &self.config
    }

    /// Run the polling loop until `cancel` is triggered.
    ///
    /// Each iteration claims and settles one batch, then waits: `poll_interval`
    /// after an empty or failed cycle, or `min_cycle_delay` after a non-empty
    /// one. Cancellation is observed promptly between and during waits; the
    /// batch in flight when cancellation arrives is finished first.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError`] only on an unrecoverable internal failure;
    /// per-cycle and per-occurrence errors are logged and retried rather than
    /// propagated.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), SchedulerError> {
        while !cancel.is_cancelled() {
            let wait = match self.poll_cycle().await {
                Ok(0) => Some(self.config.poll_interval),
                Ok(_) => {
                    (!self.config.min_cycle_delay.is_zero()).then_some(self.config.min_cycle_delay)
                }
                Err(error) => {
                    tracing::error!(error = %error, "scheduler worker poll cycle failed, backing off");
                    Some(self.config.poll_interval)
                }
            };
            if let Some(delay) = wait {
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = cancel.cancelled() => break,
                }
            }
        }
        Ok(())
    }

    /// Claim and settle one batch, returning the number of occurrences claimed.
    #[tracing::instrument(name = "scheduler.tick", skip_all, fields(claimed = tracing::field::Empty))]
    async fn poll_cycle(&self) -> Result<usize, SchedulerError> {
        let now = SystemTime::now();
        let claimed = self
            .store
            .claim_due(now, self.config.batch_size, self.config.lease)
            .await?;
        let count = claimed.len();
        tracing::Span::current().record("claimed", count);
        if count > 0 {
            tracing::debug!(claimed = count, "scheduler claimed due occurrences");
        }
        for occurrence in claimed {
            let schedule_id = occurrence.message.schedule_id;
            if let Err(error) = self.settle(occurrence).await {
                tracing::error!(
                    schedule_id = %schedule_id,
                    error = %error,
                    "scheduler worker failed to settle an occurrence"
                );
            }
        }
        Ok(count)
    }

    /// Dispatch one occurrence and apply the success or failure outcome.
    #[tracing::instrument(
        name = "scheduler.dispatch",
        skip_all,
        fields(
            schedule_id = %occurrence.message.schedule_id,
            trigger = occurrence.message.trigger.kind(),
            attempt = occurrence.attempts,
            lag_ms = tracing::field::Empty
        )
    )]
    async fn settle(&self, occurrence: LeasedOccurrence) -> Result<(), SchedulerError> {
        let now = SystemTime::now();
        let lag = Self::dispatch_lag(occurrence.message.scheduled_for, now);
        tracing::Span::current()
            .record("lag_ms", u64::try_from(lag.as_millis()).unwrap_or(u64::MAX));
        match self.dispatch(&occurrence.message).await {
            Ok(()) => {
                tracing::debug!(
                    schedule_id = %occurrence.message.schedule_id,
                    trigger = occurrence.message.trigger.kind(),
                    lag_ms = u64::try_from(lag.as_millis()).unwrap_or(u64::MAX),
                    "scheduled occurrence dispatched"
                );
                self.on_success(&occurrence.message).await
            }
            Err(error) => self.on_failure(&occurrence, &error).await,
        }
    }

    /// Duration from an occurrence's due time to `now`, clamped to zero when the
    /// occurrence is not yet due (clock skew). The scheduler's primary health
    /// signal: sustained growth means the worker is falling behind.
    fn dispatch_lag(scheduled_for: SystemTime, now: SystemTime) -> Duration {
        now.duration_since(scheduled_for).unwrap_or(Duration::ZERO)
    }

    /// Dispatch to the sink under the configured hard timeout.
    async fn dispatch(&self, message: &ScheduledMessage) -> Result<(), SchedulerError> {
        match tokio::time::timeout(self.config.dispatch_timeout, self.sink.dispatch(message)).await
        {
            Ok(result) => result,
            Err(_elapsed) => Err(SchedulerError::dispatch(DispatchTimedOut {
                after: self.config.dispatch_timeout,
            })),
        }
    }

    /// Acknowledge a successful dispatch: deliver a one-shot schedule, or
    /// advance a recurring one to its next occurrence.
    async fn on_success(&self, message: &ScheduledMessage) -> Result<(), SchedulerError> {
        match &message.trigger {
            Trigger::Delay(_) => self.store.mark_delivered(message.schedule_id).await,
            Trigger::Cron(expression) => {
                match expression.next_due(SystemTime::now(), message.scheduled_for)? {
                    Some(next) => {
                        tracing::debug!(
                            schedule_id = %message.schedule_id,
                            trigger = message.trigger.kind(),
                            "scheduled occurrence rescheduled"
                        );
                        self.store.reschedule(message.schedule_id, next).await
                    }
                    None => self.store.mark_delivered(message.schedule_id).await,
                }
            }
        }
    }

    /// Apply a failed dispatch: retry with backoff, or dead-letter once the
    /// attempt budget is exhausted.
    async fn on_failure(
        &self,
        occurrence: &LeasedOccurrence,
        error: &SchedulerError,
    ) -> Result<(), SchedulerError> {
        let schedule_id = occurrence.message.schedule_id;
        let reason = error.to_string();
        if occurrence.is_exhausted() {
            tracing::error!(
                schedule_id = %schedule_id,
                attempts = occurrence.attempts,
                error = %reason,
                "scheduled occurrence dead-lettered"
            );
            return self.store.mark_dead_lettered(schedule_id, &reason).await;
        }
        tracing::warn!(
            schedule_id = %schedule_id,
            attempts = occurrence.attempts,
            error = %reason,
            "scheduled occurrence retried"
        );
        let delay = self.next_retry_delay(occurrence.attempts);
        let retry_at = SystemTime::now()
            .checked_add(delay)
            .ok_or_else(|| SchedulerError::internal("retry deadline overflow"))?;
        self.store.mark_failed(schedule_id, retry_at, &reason).await
    }

    /// Compute the next retry delay: bounded exponential backoff with optional
    /// full jitter, overflow-safe.
    fn next_retry_delay(&self, attempts: u32) -> Duration {
        let factor = 1u32.checked_shl(attempts).unwrap_or(u32::MAX);
        let capped = self
            .config
            .retry_base_delay
            .saturating_mul(factor)
            .min(self.config.retry_max_delay);
        if self.config.jitter {
            let nanos = u64::try_from(capped.as_nanos()).unwrap_or(u64::MAX);
            Duration::from_nanos(fastrand::u64(0..=nanos))
        } else {
            capped
        }
    }
}

/// Error recorded when a dispatch exceeds the configured timeout.
#[derive(Debug)]
struct DispatchTimedOut {
    after: Duration,
}

impl fmt::Display for DispatchTimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dispatch timed out after {:?}", self.after)
    }
}

impl std::error::Error for DispatchTimedOut {}

#[cfg(test)]
mod tests {
    use super::{SchedulerWorker, SchedulerWorkerConfig};
    use crate::error::SchedulerError;
    use crate::memory::InMemoryScheduleStore;
    use crate::schedule::ScheduledMessage;
    use crate::sink::ScheduleSink;
    use crate::snapshot::ScheduleStatus;
    use crate::store::ScheduleStore;
    use crate::target::Target;
    use hexeract_outbox::Event;
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;
    use uuid::Uuid;

    #[derive(Debug, Serialize, Deserialize)]
    struct ReminderDue;

    impl Event for ReminderDue {
        const EVENT_TYPE: &'static str = "reminders.due";
    }

    struct SuccessSink;

    impl ScheduleSink for SuccessSink {
        async fn dispatch(&self, _message: &ScheduledMessage) -> Result<(), SchedulerError> {
            Ok(())
        }
    }

    struct FailingSink;

    impl ScheduleSink for FailingSink {
        async fn dispatch(&self, _message: &ScheduledMessage) -> Result<(), SchedulerError> {
            Err(SchedulerError::dispatch(std::io::Error::other("sink down")))
        }
    }

    /// An instant comfortably in the past so claims are always due.
    fn past() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_000)
    }

    fn test_config() -> SchedulerWorkerConfig {
        SchedulerWorkerConfig {
            poll_interval: Duration::from_millis(10),
            batch_size: 10,
            lease: Duration::from_secs(30),
            retry_base_delay: Duration::from_secs(3_600),
            retry_max_delay: Duration::from_secs(3_600),
            jitter: false,
            min_cycle_delay: Duration::ZERO,
            dispatch_timeout: Duration::from_secs(30),
        }
    }

    async fn insert_delay(store: &InMemoryScheduleStore, max_attempts: u32) -> Uuid {
        let message =
            ScheduledMessage::delay(Target::mediator(), past(), &ReminderDue).expect("serializes");
        let schedule_id = message.schedule_id;
        store
            .insert(&message, max_attempts)
            .await
            .expect("insert succeeds");
        schedule_id
    }

    #[tokio::test]
    async fn delivers_a_delay_schedule_on_success() {
        let store = InMemoryScheduleStore::new();
        let schedule_id = insert_delay(&store, 3).await;
        let worker = SchedulerWorker::new(store, SuccessSink, test_config());

        let claimed = worker.poll_cycle().await.expect("cycle succeeds");
        assert_eq!(claimed, 1);

        let snapshot = worker
            .store
            .inspect(schedule_id)
            .await
            .unwrap()
            .expect("schedule exists");
        assert_eq!(snapshot.status, ScheduleStatus::Delivered);
    }

    #[tokio::test]
    async fn reschedules_a_cron_schedule_on_success() {
        let store = InMemoryScheduleStore::new();
        let message =
            ScheduledMessage::cron(Target::outbox(), "0 0 * * *", past(), &ReminderDue).unwrap();
        let schedule_id = message.schedule_id;
        store.insert(&message, 3).await.unwrap();
        let worker = SchedulerWorker::new(store, SuccessSink, test_config());

        let claimed = worker.poll_cycle().await.expect("cycle succeeds");
        assert_eq!(claimed, 1);

        let snapshot = worker.store.inspect(schedule_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, ScheduleStatus::Pending);
        assert_eq!(snapshot.attempts, 0);
        assert!(snapshot.scheduled_for > past());
    }

    #[tokio::test]
    async fn retries_a_failed_dispatch_with_backoff() {
        let store = InMemoryScheduleStore::new();
        let schedule_id = insert_delay(&store, 3).await;
        let worker = SchedulerWorker::new(store, FailingSink, test_config());

        let claimed = worker.poll_cycle().await.expect("cycle succeeds");
        assert_eq!(claimed, 1);

        let snapshot = worker.store.inspect(schedule_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, ScheduleStatus::Pending);
        assert_eq!(snapshot.attempts, 1);
        assert!(snapshot.last_error.is_some());

        // The retry deadline is an hour out, so the occurrence is not reclaimed
        // on the next immediate cycle.
        let again = worker.poll_cycle().await.expect("cycle succeeds");
        assert_eq!(again, 0);
    }

    #[tokio::test]
    async fn dead_letters_an_exhausted_dispatch() {
        let store = InMemoryScheduleStore::new();
        let schedule_id = insert_delay(&store, 1).await;
        let worker = SchedulerWorker::new(store, FailingSink, test_config());

        let claimed = worker.poll_cycle().await.expect("cycle succeeds");
        assert_eq!(claimed, 1);

        let snapshot = worker.store.inspect(schedule_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, ScheduleStatus::DeadLettered);
        assert!(snapshot.last_error.is_some());
    }

    #[tokio::test]
    async fn run_returns_when_cancelled_before_start() {
        let worker = SchedulerWorker::new(InMemoryScheduleStore::new(), SuccessSink, test_config());
        let cancel = CancellationToken::new();
        cancel.cancel();
        worker.run(cancel).await.expect("graceful shutdown");
    }

    #[tokio::test]
    async fn run_shuts_down_gracefully_with_work_present() {
        let store = InMemoryScheduleStore::new();
        insert_delay(&store, 3).await;
        let worker = SchedulerWorker::new(store, SuccessSink, test_config());
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(worker.run(cancel.clone()));

        // Let the loop run a few cycles, then signal shutdown.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        handle
            .await
            .expect("task joins")
            .expect("graceful shutdown");
    }

    // --- dispatch_lag unit tests ---

    #[test]
    fn dispatch_lag_past_returns_positive_duration() {
        let scheduled_for = UNIX_EPOCH + Duration::from_secs(1_000);
        let now = scheduled_for + Duration::from_millis(250);
        let lag =
            SchedulerWorker::<InMemoryScheduleStore, SuccessSink>::dispatch_lag(scheduled_for, now);
        assert_eq!(lag, Duration::from_millis(250));
    }

    #[test]
    fn dispatch_lag_future_clamps_to_zero() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let scheduled_for = now + Duration::from_secs(5);
        let lag =
            SchedulerWorker::<InMemoryScheduleStore, SuccessSink>::dispatch_lag(scheduled_for, now);
        assert_eq!(lag, Duration::ZERO);
    }

    // --- tracing events tests ---

    #[tokio::test]
    #[traced_test]
    async fn delay_dispatch_emits_dispatched_event() {
        let store = InMemoryScheduleStore::new();
        insert_delay(&store, 3).await;
        let worker = SchedulerWorker::new(store, SuccessSink, test_config());
        worker.poll_cycle().await.expect("cycle succeeds");
        assert!(logs_contain("scheduled occurrence dispatched"));
    }

    #[tokio::test]
    #[traced_test]
    async fn exhausted_dispatch_emits_dead_lettered_event() {
        let store = InMemoryScheduleStore::new();
        insert_delay(&store, 1).await;
        let worker = SchedulerWorker::new(store, FailingSink, test_config());
        worker.poll_cycle().await.expect("cycle succeeds");
        assert!(logs_contain("scheduled occurrence dead-lettered"));
    }

    #[tokio::test]
    #[traced_test]
    async fn cron_success_emits_rescheduled_event() {
        let store = InMemoryScheduleStore::new();
        let message =
            ScheduledMessage::cron(Target::outbox(), "0 0 * * *", past(), &ReminderDue).unwrap();
        store.insert(&message, 3).await.unwrap();
        let worker = SchedulerWorker::new(store, SuccessSink, test_config());
        worker.poll_cycle().await.expect("cycle succeeds");
        assert!(logs_contain("scheduled occurrence rescheduled"));
    }
}
