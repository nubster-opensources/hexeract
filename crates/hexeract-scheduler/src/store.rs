use std::time::Duration;
use std::time::SystemTime;

use uuid::Uuid;

use crate::error::SchedulerError;
use crate::lease::LeasedOccurrence;
use crate::schedule::ScheduledMessage;
use crate::snapshot::ScheduleSnapshot;

/// Error message recorded by [`ScheduleStore::dead_letter_exhausted`] on every
/// schedule it sweeps.
pub const DEAD_LETTER_EXHAUSTED_MESSAGE: &str =
    "attempts exhausted by a crashed worker; dead-lettered by the sweeper";

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
///
/// # Fencing
///
/// Every acknowledgement ([`Self::mark_delivered`], [`Self::reschedule`],
/// [`Self::mark_failed`] and [`Self::mark_dead_lettered`]) takes the `lease`
/// a prior [`Self::claim_due`] stamped on the occurrence (carried on
/// [`LeasedOccurrence::leased_until`]) and only applies when the row still
/// carries exactly that lease. A worker whose lease has expired before it
/// gets around to acknowledging (a zombie: paused on I/O, descheduled by the
/// OS, or simply slower than the lease window) can no longer corrupt the
/// state written by whichever worker reclaimed the occurrence in the
/// meantime. `Ok(true)` means the acknowledgement was applied; `Ok(false)`
/// is not an error, it is the signal that another worker now owns this
/// occurrence, and the caller should move on without retrying locally.
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
    /// Applies only when the schedule is still `Pending` and `leased_until`
    /// still equals `lease`; see the trait-level fencing documentation.
    /// Idempotent: a no-op (returning `Ok(false)`) when no schedule matches
    /// `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn mark_delivered(
        &self,
        schedule_id: Uuid,
        lease: SystemTime,
    ) -> Result<bool, SchedulerError>;

    /// Advance a recurring schedule to its `next` occurrence, resetting the
    /// attempt counter and releasing the lease.
    ///
    /// Applies only when the schedule is still `Pending` and `leased_until`
    /// still equals `lease`; see the trait-level fencing documentation.
    /// Idempotent: a no-op (returning `Ok(false)`) when no schedule matches
    /// `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn reschedule(
        &self,
        schedule_id: Uuid,
        next: SystemTime,
        lease: SystemTime,
    ) -> Result<bool, SchedulerError>;

    /// Record a failed delivery attempt and defer the next claim by
    /// `retry_in`, keeping the occurrence pending and its attempt counter
    /// untouched.
    ///
    /// `retry_in` is a delay from now, not an absolute instant: the backend
    /// adds it to its own database clock when computing the new
    /// `leased_until`, so the retry deadline stays immune to skew between the
    /// worker host and the database host (mirroring
    /// [`Self::claim_due`]'s lease anchoring). The attempt is advanced at
    /// claim time, not here, so a failed occurrence keeps the attempt already
    /// consumed; this method only pushes the lease out and records the error.
    /// The occurrence is reclaimed once the new `leased_until` has passed, as
    /// long as it still has attempt budget.
    ///
    /// Applies only when the schedule is still `Pending` and `leased_until`
    /// still equals `lease`; see the trait-level fencing documentation.
    /// Idempotent: a no-op (returning `Ok(false)`) when no schedule matches
    /// `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn mark_failed(
        &self,
        schedule_id: Uuid,
        retry_in: Duration,
        error: &str,
        lease: SystemTime,
    ) -> Result<bool, SchedulerError>;

    /// Move a schedule to the dead-letter state, recording the last error
    /// and releasing the lease.
    ///
    /// Applies only when the schedule is still `Pending` and `leased_until`
    /// still equals `lease`; see the trait-level fencing documentation.
    /// Idempotent: a no-op (returning `Ok(false)`) when no schedule matches
    /// `schedule_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn mark_dead_lettered(
        &self,
        schedule_id: Uuid,
        error: &str,
        lease: SystemTime,
    ) -> Result<bool, SchedulerError>;

    /// Dead-letter every non-terminal, non-paused schedule whose attempt
    /// budget is exhausted and whose lease has expired or was never taken.
    ///
    /// A worker that crashes while handling its last attempt leaves a row
    /// with `attempts >= max_attempts`, still `Pending`, and no active lease:
    /// [`Self::claim_due`] excludes it because its budget is exhausted, so
    /// without this sweep it would sit forever, neither claimable nor
    /// dead-lettered. [`crate::worker::SchedulerWorker`] calls this once at
    /// the start of every poll cycle to close that gap; a non-zero count is
    /// worth logging as an operational signal of crashed workers. The last
    /// error recorded on each swept row is [`DEAD_LETTER_EXHAUSTED_MESSAGE`].
    ///
    /// Idempotent: a schedule already dead-lettered by a previous sweep is no
    /// longer non-terminal, so it is not matched again.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if the backend fails to update.
    async fn dead_letter_exhausted(&self) -> Result<u64, SchedulerError>;

    /// Cancel a schedule, excluding it from future claims.
    ///
    /// A no-op when the schedule is already in a terminal state (delivered,
    /// dead-lettered, or already cancelled): the stored status is never
    /// overwritten once it has reached a terminal outcome.
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

    /// Resume a paused schedule, optionally realigning its next occurrence.
    ///
    /// `Some(next)` unpauses AND sets `scheduled_for = next`, resetting the
    /// attempt counter, clearing the last recorded error and releasing any
    /// lease, atomically. `None` only unpauses, leaving the occurrence intact.
    ///
    /// Idempotent: a no-op returning `Ok(())` when the schedule is not Paused.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ScheduleNotFound`] if no schedule matches
    /// `schedule_id`, or [`SchedulerError::Database`] on a backend failure.
    async fn resume(
        &self,
        schedule_id: Uuid,
        next: Option<SystemTime>,
    ) -> Result<(), SchedulerError>;
}
