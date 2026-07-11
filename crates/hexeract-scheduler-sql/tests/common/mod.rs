//! Backend-agnostic scenarios exercised by every dialect integration test.
//!
//! Each scenario drives a freshly set up [`ScheduleStore`] through the
//! [`hexeract_scheduler::ScheduleStore`] contract only, so the same behaviour
//! is asserted identically against Postgres, MySQL and SQLite. The dialect
//! test files own the container or file setup and call these functions.

#![allow(dead_code)]

use std::time::Duration;
use std::time::SystemTime;

use hexeract_outbox::Event;
use hexeract_scheduler::ScheduleAdmin;
use hexeract_scheduler::ScheduleStatus;
use hexeract_scheduler::ScheduleStore;
use hexeract_scheduler::ScheduledMessage;
use hexeract_scheduler::SchedulerError;
use hexeract_scheduler::Target;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Sample event persisted by the scenarios.
#[derive(Debug, Serialize, Deserialize)]
struct ReminderDue {
    user_id: Uuid,
}

impl Event for ReminderDue {
    const EVENT_TYPE: &'static str = "reminders.due";
}

const MAX_ATTEMPTS: u32 = 5;

/// An instant `seconds` in the past, comfortably before the database clock.
fn past(seconds: u64) -> SystemTime {
    SystemTime::now() - Duration::from_secs(seconds)
}

/// An instant `seconds` in the future, comfortably after the database clock.
fn future(seconds: u64) -> SystemTime {
    SystemTime::now() + Duration::from_secs(seconds)
}

/// Build a one-shot message firing at `at`.
fn delay_message(at: SystemTime) -> ScheduledMessage {
    ScheduledMessage::delay(
        Target::mediator(),
        at,
        &ReminderDue {
            user_id: Uuid::nil(),
        },
    )
    .expect("serializes the payload")
}

/// Build a recurring message whose first occurrence is `at`.
fn cron_message(at: SystemTime) -> ScheduledMessage {
    ScheduledMessage::cron(
        Target::outbox(),
        "0 0 * * *",
        at,
        &ReminderDue {
            user_id: Uuid::nil(),
        },
    )
    .expect("valid cron and payload")
}

/// Insert a due one-shot schedule and report Pending with a zeroed attempt
/// counter.
pub(crate) async fn insert_then_inspect_reports_pending<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let snapshot = store
        .inspect(schedule_id)
        .await
        .expect("inspect")
        .expect("schedule exists");
    assert_eq!(snapshot.status, ScheduleStatus::Pending);
    assert_eq!(snapshot.attempts, 0);
    assert_eq!(snapshot.max_attempts, MAX_ATTEMPTS);
    assert!(
        store
            .inspect(Uuid::now_v7())
            .await
            .expect("inspect")
            .is_none(),
        "an unknown schedule must inspect to None"
    );
}

/// Claiming a due schedule consumes one attempt and stamps a lease that
/// excludes it from an immediate second claim.
pub(crate) async fn claim_increments_then_excludes_active_lease<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempts, 1);
    assert_eq!(claimed[0].message.schedule_id, schedule_id);

    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.attempts, 1);

    let again = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("second claim");
    assert!(
        again.is_empty(),
        "an active lease must exclude the schedule"
    );
}

/// An expired lease is reclaimed as the same occurrence exactly once, with the
/// attempt counter advanced. This is the crash-safety guarantee.
pub(crate) async fn expired_lease_reclaimed_exactly_once<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let first = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(1))
        .await
        .expect("first claim");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].attempts, 1);

    let blocked = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(1))
        .await
        .expect("blocked claim");
    assert!(blocked.is_empty(), "the lease must still be active");

    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let reclaimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("reclaim");
    assert_eq!(reclaimed.len(), 1, "the expired lease must be reclaimable");
    assert_eq!(reclaimed[0].attempts, 2);
    assert_eq!(
        reclaimed[0].occurrence_id(),
        first[0].occurrence_id(),
        "a reclaim is the same occurrence, not a new one"
    );
}

/// A schedule whose instant is in the future is not claimed.
pub(crate) async fn excludes_not_yet_due<S: ScheduleStore>(store: &S) {
    let message = delay_message(future(3_600));
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert!(claimed.is_empty(), "a future schedule must not be claimed");
}

/// Rescheduling a recurring schedule advances its instant, resets the attempt
/// counter, clears the lease and makes the new occurrence claimable.
pub(crate) async fn reschedule_advances_resets_and_reclaims<S: ScheduleStore>(store: &S) {
    let message = cron_message(past(120));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempts, 1);

    let applied = store
        .reschedule(schedule_id, past(30), claimed[0].leased_until)
        .await
        .expect("reschedule");
    assert!(applied, "the freshly claimed lease must still be valid");
    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Pending);
    assert_eq!(snapshot.attempts, 0);

    let reclaimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim after reschedule");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].attempts, 1);
}

/// Cancelling excludes a schedule from claims and reports it cancelled; an
/// unknown schedule is rejected.
pub(crate) async fn cancel_excludes_and_rejects_unknown<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    store.cancel(schedule_id).await.expect("cancel");
    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert!(
        claimed.is_empty(),
        "a cancelled schedule must not be claimed"
    );
    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Cancelled);

    let error = store.cancel(Uuid::now_v7()).await.unwrap_err();
    assert!(matches!(error, SchedulerError::ScheduleNotFound { .. }));
}

/// Cancelling a schedule that already reached a terminal state (delivered or
/// dead-lettered) is a no-op: the original terminal status is preserved, not
/// overwritten to `Cancelled`. Cancelling an already-cancelled schedule stays
/// idempotent.
pub(crate) async fn cancel_does_not_clobber_a_terminal_status<S: ScheduleStore>(store: &S) {
    let delivered_message = delay_message(past(60));
    let delivered_id = delivered_message.schedule_id;
    store
        .insert(&delivered_message, MAX_ATTEMPTS)
        .await
        .expect("insert delivered");
    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    store
        .mark_delivered(delivered_id, claimed[0].leased_until)
        .await
        .expect("mark delivered");

    store.cancel(delivered_id).await.expect("cancel delivered");
    let snapshot = store.inspect(delivered_id).await.unwrap().unwrap();
    assert_eq!(
        snapshot.status,
        ScheduleStatus::Delivered,
        "cancel must not clobber a delivered schedule"
    );

    let dead_lettered_message = delay_message(past(60));
    let dead_lettered_id = dead_lettered_message.schedule_id;
    store
        .insert(&dead_lettered_message, MAX_ATTEMPTS)
        .await
        .expect("insert dead-lettered");
    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    store
        .mark_dead_lettered(dead_lettered_id, "boom", claimed[0].leased_until)
        .await
        .expect("dead letter");

    store
        .cancel(dead_lettered_id)
        .await
        .expect("cancel dead-lettered");
    let snapshot = store.inspect(dead_lettered_id).await.unwrap().unwrap();
    assert_eq!(
        snapshot.status,
        ScheduleStatus::DeadLettered,
        "cancel must not clobber a dead-lettered schedule"
    );

    let cancelled_message = delay_message(past(60));
    let cancelled_id = cancelled_message.schedule_id;
    store
        .insert(&cancelled_message, MAX_ATTEMPTS)
        .await
        .expect("insert cancelled");
    store.cancel(cancelled_id).await.expect("first cancel");

    store.cancel(cancelled_id).await.expect("second cancel");
    let snapshot = store.inspect(cancelled_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Cancelled);
}

/// Pausing excludes a schedule; resuming makes it claimable again; an unknown
/// schedule is rejected.
pub(crate) async fn pause_excludes_then_resume_reenables<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    store.set_paused(schedule_id, true).await.expect("pause");
    assert!(
        store
            .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
            .await
            .unwrap()
            .is_empty(),
        "a paused schedule must not be claimed"
    );
    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Paused);

    store.set_paused(schedule_id, false).await.expect("resume");
    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim after resume");
    assert_eq!(claimed.len(), 1);

    let error = store.set_paused(Uuid::now_v7(), true).await.unwrap_err();
    assert!(matches!(error, SchedulerError::ScheduleNotFound { .. }));
}

/// Dead-lettering excludes a schedule from claims and records the last error.
pub(crate) async fn dead_letter_excludes_and_records_error<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    store
        .mark_dead_lettered(schedule_id, "boom", claimed[0].leased_until)
        .await
        .expect("dead letter");
    assert!(
        store
            .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
            .await
            .unwrap()
            .is_empty(),
        "a dead-lettered schedule must not be claimed"
    );
    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::DeadLettered);
    assert_eq!(snapshot.last_error.as_deref(), Some("boom"));
}

/// Marking a one-shot schedule delivered excludes it from claims and reports it
/// delivered.
pub(crate) async fn mark_delivered_excludes<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    store
        .mark_delivered(schedule_id, claimed[0].leased_until)
        .await
        .expect("deliver");
    assert!(
        store
            .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
            .await
            .unwrap()
            .is_empty(),
        "a delivered schedule must not be claimed"
    );
    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Delivered);
}

/// A failed occurrence is deferred by a relative `retry_in`, anchored on the
/// database clock rather than a caller-supplied absolute instant (#355): it
/// is not reclaimable before `retry_in` elapses, becomes reclaimable once it
/// has, and the snapshot retains the error string. `attempts` is not
/// incremented by `mark_failed` itself (it was already counted at claim
/// time).
pub(crate) async fn mark_failed_defers_reclaim_until_retry_in_elapses<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    // Claim the occurrence: attempts advances to 1.
    let first = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].attempts, 1);

    // Defer retry by 2 seconds, fenced on the lease this claim stamped.
    let retry_in = Duration::from_secs(2);
    let applied = store
        .mark_failed(
            schedule_id,
            retry_in,
            "connection refused",
            first[0].leased_until,
        )
        .await
        .expect("mark_failed");
    assert!(applied, "the freshly claimed lease must still be valid");

    // Before retry_in elapses: the occurrence must not be reclaimable.
    let too_early = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim before retry_in elapses");
    assert!(
        too_early.is_empty(),
        "a failed schedule must not be reclaimable before retry_in elapses"
    );

    // The snapshot must reflect the error but remain Pending.
    let snapshot = store
        .inspect(schedule_id)
        .await
        .expect("inspect")
        .expect("schedule exists");
    assert_eq!(snapshot.status, ScheduleStatus::Pending);
    assert_eq!(snapshot.last_error.as_deref(), Some("connection refused"));
    // attempts must NOT have changed (still 1 from the claim, not 2).
    assert_eq!(snapshot.attempts, 1);

    // Wait past retry_in.
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    // Once retry_in has elapsed: the occurrence is reclaimable and attempts advance.
    let reclaimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim after retry_in elapses");
    assert_eq!(
        reclaimed.len(),
        1,
        "must be reclaimable once retry_in has elapsed"
    );
    assert_eq!(reclaimed[0].attempts, 2, "attempts must advance on reclaim");
    assert_eq!(reclaimed[0].message.schedule_id, schedule_id);
}

/// Resuming a paused schedule with `next = Some(t)` sets the occurrence to `t`
/// and makes it claimable; resuming with `None` only unpauses. Resuming an
/// unknown id returns `ScheduleNotFound`.
pub(crate) async fn resume_realigns_paused_and_rejects_unknown<S: ScheduleStore>(store: &S) {
    // Resume with explicit next: occurrence is realigned and claimable.
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");
    store.set_paused(schedule_id, true).await.expect("pause");

    let next = future(3_600);
    store
        .resume(schedule_id, Some(next))
        .await
        .expect("resume with next");

    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Pending);
    assert_eq!(snapshot.attempts, 0, "attempts must reset after resume");
    // The occurrence is now in the future: not yet claimable.
    assert!(
        store
            .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
            .await
            .unwrap()
            .is_empty(),
        "a future-aligned schedule must not be claimable yet"
    );

    // Resume with None: only unpauses, occurrence unchanged.
    let message2 = delay_message(past(60));
    let id2 = message2.schedule_id;
    store
        .insert(&message2, MAX_ATTEMPTS)
        .await
        .expect("insert2");
    store.set_paused(id2, true).await.expect("pause2");
    store.resume(id2, None).await.expect("resume with none");
    let snap2 = store.inspect(id2).await.unwrap().unwrap();
    assert_eq!(snap2.status, ScheduleStatus::Pending);
    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "a past-due resumed schedule must be claimable"
    );

    // Unknown id returns ScheduleNotFound.
    let error = store.resume(Uuid::now_v7(), None).await.unwrap_err();
    assert!(matches!(error, SchedulerError::ScheduleNotFound { .. }));
}

/// Listing pending schedules orders by earliest occurrence first and honours
/// the caller's limit.
pub(crate) async fn list_pending_orders_and_limits<S: ScheduleAdmin>(store: &S) {
    let sooner = delay_message(future(100));
    let later = delay_message(future(200));
    store.insert(&sooner, MAX_ATTEMPTS).await.expect("insert");
    store.insert(&later, MAX_ATTEMPTS).await.expect("insert");
    let listed = store.list_pending(10).await.expect("list");
    assert_eq!(
        listed.first().map(|s| s.schedule_id),
        Some(sooner.schedule_id)
    );
    let capped = store.list_pending(1).await.expect("list");
    assert_eq!(capped.len(), 1);
    assert_eq!(capped[0].schedule_id, sooner.schedule_id);
}

/// Listing dead-lettered schedules reports the recorded error.
pub(crate) async fn list_dead_letter_reports_errors<S: ScheduleAdmin>(store: &S) {
    let message = delay_message(past(60));
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");
    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    store
        .mark_dead_lettered(message.schedule_id, "boom", claimed[0].leased_until)
        .await
        .expect("dead");
    let listed = store.list_dead_letter(10).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].schedule_id, message.schedule_id);
    assert_eq!(listed[0].last_error.as_deref(), Some("boom"));
}

/// Listing dead-lettered schedules orders the most recently dead-lettered
/// schedule first, matching the `ORDER BY dead_lettered_at DESC` contract.
pub(crate) async fn list_dead_letter_orders_most_recently_dead_lettered_first<S: ScheduleAdmin>(
    store: &S,
) {
    let first = delay_message(past(60));
    store
        .insert(&first, MAX_ATTEMPTS)
        .await
        .expect("insert first");
    let claimed_first = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim first");
    store
        .mark_dead_lettered(first.schedule_id, "first", claimed_first[0].leased_until)
        .await
        .expect("dead letter first");

    // Sleep past the coarsest timestamp precision among the backends (SQLite
    // stores milliseconds) so the two dead-letter instants are unambiguously
    // ordered.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let second = delay_message(past(60));
    store
        .insert(&second, MAX_ATTEMPTS)
        .await
        .expect("insert second");
    let claimed_second = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim second");
    store
        .mark_dead_lettered(second.schedule_id, "second", claimed_second[0].leased_until)
        .await
        .expect("dead letter second");

    let listed = store.list_dead_letter(10).await.expect("list");
    assert_eq!(listed.len(), 2);
    assert_eq!(
        listed[0].schedule_id, second.schedule_id,
        "the most recently dead-lettered schedule must come first"
    );
    assert_eq!(listed[1].schedule_id, first.schedule_id);
}

/// Replaying a dead-lettered schedule returns it to pending with its attempt
/// counter and last error cleared, and it no longer appears in the dead
/// letter listing.
pub(crate) async fn replay_requeues_dead_letter<S: ScheduleAdmin>(store: &S) {
    let message = delay_message(past(60));
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");
    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    store
        .mark_dead_lettered(message.schedule_id, "boom", claimed[0].leased_until)
        .await
        .expect("dead");
    store.replay(message.schedule_id).await.expect("replay");
    let snapshot = store.inspect(message.schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Pending);
    assert_eq!(snapshot.attempts, 0);
    assert_eq!(snapshot.last_error, None);
    assert!(store.list_dead_letter(10).await.unwrap().is_empty());
}

/// Replaying a schedule that is not dead-lettered is rejected.
pub(crate) async fn replay_rejects_non_dead_lettered<S: ScheduleAdmin>(store: &S) {
    let message = delay_message(future(100));
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");
    let error = store.replay(message.schedule_id).await.expect_err("reject");
    assert!(matches!(error, SchedulerError::NotReplayable { .. }));
}

/// Insert `count` due one-shot schedules and return their identifiers. Used by
/// the competing-consumer contention test, which needs raw access to the ids.
pub(crate) async fn insert_due_batch<S: ScheduleStore>(store: &S, count: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let message = delay_message(past(60));
        ids.push(message.schedule_id);
        store.insert(&message, MAX_ATTEMPTS).await.expect("insert");
    }
    ids
}

/// Round-trip fencing (#352): the lease token `claim_due` returns applies the
/// acknowledgement it is bound to. This exercises the real backend's
/// timestamp round-trip end to end (PostgreSQL `TIMESTAMPTZ` microsecond
/// precision, MySQL `DATETIME(6)`, SQLite millisecond text), not just the
/// SQL generated for the statement.
pub(crate) async fn ack_round_trips_the_claimed_lease<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let claimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);

    let applied = store
        .mark_delivered(schedule_id, claimed[0].leased_until)
        .await
        .expect("mark_delivered with the freshly claimed token");
    assert!(
        applied,
        "the token claim_due returned must round-trip back into the ack"
    );

    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::Delivered);
}

/// Round-trip fencing (#352): a lease token taken before another worker
/// reclaimed the same occurrence is rejected, and the reclaiming worker's
/// state is left untouched.
pub(crate) async fn ack_with_a_stale_lease_is_rejected_after_reclaim<S: ScheduleStore>(store: &S) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, MAX_ATTEMPTS).await.expect("insert");

    let short_lease = Duration::from_millis(500);
    let first = store
        .claim_due(SystemTime::now(), 10, short_lease)
        .await
        .expect("first claim");
    assert_eq!(first.len(), 1);
    let stale_token = first[0].leased_until;

    tokio::time::sleep(short_lease + Duration::from_millis(500)).await;

    let reclaimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("reclaim after expiry");
    assert_eq!(reclaimed.len(), 1);
    assert_ne!(
        reclaimed[0].leased_until, stale_token,
        "the reclaim must stamp a fresh, distinct token"
    );

    let applied = store
        .mark_delivered(schedule_id, stale_token)
        .await
        .expect("mark_delivered with a stale token");
    assert!(!applied, "a stale token must not apply");

    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(
        snapshot.status,
        ScheduleStatus::Pending,
        "the reclaimed occurrence must be untouched by the stale ack"
    );
}

/// A schedule left `Pending`, exhausted and unleased by a crashed worker is
/// swept to the dead-letter state by `dead_letter_exhausted`, and the sweep
/// is idempotent.
pub(crate) async fn dead_letter_exhausted_sweeps_crash_exhausted_schedules<S: ScheduleStore>(
    store: &S,
) {
    let message = delay_message(past(60));
    let schedule_id = message.schedule_id;
    store.insert(&message, 1).await.expect("insert");

    let short_lease = Duration::from_millis(500);
    let claimed = store
        .claim_due(SystemTime::now(), 10, short_lease)
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert!(claimed[0].is_exhausted());

    tokio::time::sleep(short_lease + Duration::from_millis(500)).await;

    let swept = store
        .dead_letter_exhausted()
        .await
        .expect("sweep crash-exhausted schedules");
    assert_eq!(swept, 1);

    let snapshot = store.inspect(schedule_id).await.unwrap().unwrap();
    assert_eq!(snapshot.status, ScheduleStatus::DeadLettered);
    assert_eq!(
        snapshot.last_error.as_deref(),
        Some(hexeract_scheduler::DEAD_LETTER_EXHAUSTED_MESSAGE)
    );

    let swept_again = store
        .dead_letter_exhausted()
        .await
        .expect("sweep again is idempotent");
    assert_eq!(swept_again, 0, "sweeping must be idempotent");
}
