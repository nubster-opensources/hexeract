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

    store
        .reschedule(schedule_id, past(30))
        .await
        .expect("reschedule");
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

    store
        .mark_dead_lettered(schedule_id, "boom")
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

    store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim");
    store.mark_delivered(schedule_id).await.expect("deliver");
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

/// A failed occurrence is deferred until `retry_at`: it is not reclaimable
/// before that instant, becomes reclaimable at or after it, and the snapshot
/// retains the error string. `attempts` is not incremented by `mark_failed`
/// itself (it was already counted at claim time).
pub(crate) async fn mark_failed_defers_reclaim_until_retry_at<S: ScheduleStore>(store: &S) {
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

    // Defer retry to 2 seconds in the future.
    let retry_at = SystemTime::now() + Duration::from_secs(2);
    store
        .mark_failed(schedule_id, retry_at, "connection refused")
        .await
        .expect("mark_failed");

    // Before retry_at: the occurrence must not be reclaimable.
    let too_early = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim before retry_at");
    assert!(
        too_early.is_empty(),
        "a failed schedule must not be reclaimable before retry_at"
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

    // Wait past retry_at.
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    // At or after retry_at: the occurrence is reclaimable and attempts advance.
    let reclaimed = store
        .claim_due(SystemTime::now(), 10, Duration::from_secs(30))
        .await
        .expect("claim after retry_at");
    assert_eq!(
        reclaimed.len(),
        1,
        "must be reclaimable once retry_at has passed"
    );
    assert_eq!(reclaimed[0].attempts, 2, "attempts must advance on reclaim");
    assert_eq!(reclaimed[0].message.schedule_id, schedule_id);
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
