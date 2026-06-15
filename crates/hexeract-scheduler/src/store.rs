use std::time::Duration;
use std::time::SystemTime;

use uuid::Uuid;

use crate::error::SchedulerError;
use crate::lease::LeasedOccurrence;
use crate::schedule::ScheduledMessage;
use crate::snapshot::ScheduleSnapshot;

/// Backend-agnostic contract for persisting and claiming scheduled messages.
///
/// A store keeps each schedule with the instant of its current occurrence
/// and the runtime state the worker needs: attempt counter, soft lease and
/// lifecycle status. Implementations map this contract onto their physical
/// schema; the worker drives them without knowing the backend.
///
/// # Claim and lease (crash safety)
///
/// [`Self::claim_due`] is the heart of the contract. In a single atomic
/// step it selects occurrences that are due, free of an active lease and
/// eligible (see below), then for each one it advances the attempt counter
/// and stamps a fresh lease ending at `now + lease`. The worker dispatches
/// the returned occurrences outside any transaction; a competing worker
/// skips them until their lease elapses.
///
/// Advancing the attempt counter at claim time, rather than only on
/// failure, is what makes a crash between claim and acknowledgement safe:
/// the attempt is already counted, so a poison occurrence eventually
/// reaches its attempt budget instead of being redelivered forever. If the
/// worker crashes before acknowledging, the lease simply expires and the
/// occurrence is reclaimed. Delivery is therefore at-least-once, and
/// consumers deduplicate on
/// [`OccurrenceId`](crate::OccurrenceId).
///
/// SQL backends should base both the due comparison and the lease deadline
/// on the database clock to stay immune to skew between the worker host and
/// the database host; `now` is provided for backends without a server-side
/// clock (such as the in-memory double) and for deterministic testing.
///
/// # Eligibility
///
/// [`Self::claim_due`] never returns an occurrence whose schedule is paused,
/// cancelled, already delivered, dead-lettered, not yet due, still leased,
/// or has exhausted its attempt budget. Pausing is intentional and distinct
/// from a missed firing: resuming a schedule does not backfill skipped
/// occurrences, it simply lets the next due occurrence be claimed.
///
/// # Acknowledgement
///
/// After a successful dispatch the worker either marks the occurrence
/// delivered ([`Self::mark_delivered`], for a one-shot schedule) or
/// reschedules it to the next occurrence ([`Self::reschedule`], for a
/// recurring schedule). These are mutually exclusive and each is atomic, so
/// the contract needs no cross-method transaction. The acknowledgement
/// methods are idempotent: applying them to an unknown schedule is a no-op,
/// which keeps redelivery safe.
#[trait_variant::make(Send)]
pub trait ScheduleStore: Send + Sync + 'static {
    /// Persist a new schedule with the given attempt budget.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to persist
    /// the schedule.
    async fn insert(
        &self,
        message: &ScheduledMessage,
        max_attempts: u32,
    ) -> Result<(), SchedulerError>;

    /// Atomically claim up to `batch_size` due occurrences, advancing their
    /// attempt counter and stamping a lease ending at `now + lease`.
    ///
    /// Occurrences that are paused, cancelled, terminal, not yet due, still
    /// leased or exhausted are excluded. See the trait-level documentation
    /// for the crash-safety contract.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to claim,
    /// or [`SchedulerError::Internal`] if the lease deadline overflows.
    async fn claim_due(
        &self,
        now: SystemTime,
        batch_size: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedOccurrence>, SchedulerError>;

    /// Mark a one-shot schedule as delivered and release its lease.
    ///
    /// Idempotent: a no-op when no schedule matches `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn mark_delivered(&self, schedule_id: Uuid) -> Result<(), SchedulerError>;

    /// Advance a recurring schedule to its `next` occurrence, resetting the
    /// attempt counter and releasing the lease.
    ///
    /// Idempotent: a no-op when no schedule matches `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn reschedule(&self, schedule_id: Uuid, next: SystemTime) -> Result<(), SchedulerError>;

    /// Record a failed delivery attempt and defer the next claim until
    /// `retry_at`, keeping the occurrence pending and its attempt counter
    /// untouched.
    ///
    /// The attempt is advanced at claim time, not here, so a failed
    /// occurrence keeps the attempt already consumed; this method only pushes
    /// the lease out to `retry_at` (the worker's backoff deadline) and records
    /// the error. The occurrence is reclaimed once `retry_at` has passed, as
    /// long as it still has attempt budget. Idempotent: a no-op when no
    /// schedule matches `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn mark_failed(
        &self,
        schedule_id: Uuid,
        retry_at: SystemTime,
        error: &str,
    ) -> Result<(), SchedulerError>;

    /// Move a schedule to the dead-letter state, recording the last error
    /// and releasing the lease.
    ///
    /// Idempotent: a no-op when no schedule matches `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn mark_dead_lettered(
        &self,
        schedule_id: Uuid,
        error: &str,
    ) -> Result<(), SchedulerError>;

    /// Cancel a schedule, excluding it from future claims.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ScheduleNotFound`] if no schedule matches
    /// `schedule_id`, or [`SchedulerError::Database`] on a backend failure.
    async fn cancel(&self, schedule_id: Uuid) -> Result<(), SchedulerError>;

    /// Pause or resume a schedule.
    ///
    /// Pausing excludes the schedule from claims. Resuming does not backfill
    /// occurrences missed while paused: the next due occurrence is claimed
    /// as usual.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ScheduleNotFound`] if no schedule matches
    /// `schedule_id`, or [`SchedulerError::Database`] on a backend failure.
    async fn set_paused(&self, schedule_id: Uuid, paused: bool) -> Result<(), SchedulerError>;

    /// Return a read-only snapshot of a schedule, or `None` if it does not
    /// exist.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to read.
    async fn inspect(&self, schedule_id: Uuid) -> Result<Option<ScheduleSnapshot>, SchedulerError>;
}
