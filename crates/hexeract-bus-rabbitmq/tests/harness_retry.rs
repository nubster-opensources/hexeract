//! Proof that the crate's container start policy recreates a subject that
//! never becomes ready, rather than waiting on it longer.
//!
//! No Docker, no container, no real waiting. The policy is generic over the
//! subject it starts, so this binary drives it with counters and a virtual
//! clock. That split matters: the Docker-backed suites prove a broker comes
//! up, and a broker comes up nearly every time, so a green run there is
//! equally consistent with a retry that works and with one that never fires.
//! Only this binary can tell those two apart.
//!
//! These tests deliberately live in their own binary rather than in
//! `harness.rs`. That module is recompiled into every test binary declaring
//! it, so tests placed there would be counted five times over and make the
//! crate's test totals unusable for comparing one commit against another.

mod harness;

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::FutureExt;
use harness::log_reports_broker_ready;
use harness::start_until_ready;

/// A subject handed back by a test double, carrying the serial number of the
/// attempt that built it.
///
/// The serial is what separates "recreated" from "waited on longer". A policy
/// that reuses the subject of the previous attempt hands back serial 1 no
/// matter how long its budget ran, so the serial is the only observation that
/// can tell the two behaviours apart from the outside.
#[derive(Clone, Copy, Debug)]
struct Subject {
    serial: usize,
}

/// The budget every test gives a single attempt.
///
/// Matches the production value so the tests exercise the same arithmetic,
/// and costs nothing: the clock is virtual and advances itself whenever every
/// task is parked on a timer.
const BUDGET: Duration = Duration::from_secs(120);

#[tokio::test(start_paused = true)]
async fn a_subject_ready_on_its_first_probe_is_returned_after_a_single_build() {
    let builds = Arc::new(AtomicUsize::new(0));
    let describes = Arc::new(AtomicUsize::new(0));

    let subject = start_until_ready(
        "an immediately ready subject",
        3,
        BUDGET,
        async || Subject {
            serial: builds.fetch_add(1, Ordering::SeqCst) + 1,
        },
        async |_subject: &Subject| true,
        async |_subject: &Subject| {
            describes.fetch_add(1, Ordering::SeqCst);
            String::from("a report nobody should have asked for")
        },
    )
    .await;

    assert_eq!(
        subject.serial, 1,
        "a subject ready on its first probe must be the one handed back"
    );
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "a subject ready on its first probe must cost exactly one build"
    );
    assert_eq!(
        describes.load(Ordering::SeqCst),
        0,
        "the failure report is for exhaustion only and must not be produced on success"
    );
}

#[tokio::test(start_paused = true)]
async fn a_subject_ready_on_its_first_probe_costs_no_waiting() {
    let started = tokio::time::Instant::now();

    let subject = start_until_ready(
        "an immediately ready subject",
        3,
        BUDGET,
        async || Subject { serial: 1 },
        async |_subject: &Subject| true,
        async |_subject: &Subject| String::new(),
    )
    .await;

    let elapsed = started.elapsed();
    assert!(
        elapsed.is_zero(),
        "a subject ready on its first probe must not be slept on, yet {elapsed:?} went by \
         before subject {} came back",
        subject.serial
    );
}

#[tokio::test(start_paused = true)]
async fn a_subject_that_overruns_its_budget_is_abandoned_for_a_freshly_built_one() {
    let builds = Arc::new(AtomicUsize::new(0));

    // The first subject built never reports ready; every later one does at
    // once. A policy that keeps probing the subject it already has can only
    // hang here until its attempts run out, and one that hands back what it
    // built first comes back with serial 1.
    let subject = start_until_ready(
        "a subject that needs a second attempt",
        3,
        BUDGET,
        async || Subject {
            serial: builds.fetch_add(1, Ordering::SeqCst) + 1,
        },
        async |subject: &Subject| subject.serial > 1,
        async |_subject: &Subject| String::from("a report nobody should have asked for"),
    )
    .await;

    assert!(
        subject.serial > 1,
        "the subject that overran its budget must be abandoned for one built afterwards, \
         but the policy handed back serial {}",
        subject.serial
    );
    assert_eq!(
        builds.load(Ordering::SeqCst),
        2,
        "recovering from one unready subject must cost exactly one extra build"
    );
}

#[tokio::test(start_paused = true)]
async fn running_out_of_attempts_panics_with_the_label_and_the_last_subject_report() {
    const LABEL: &str = "a subject that never comes up";
    const REPORT: &str = "the streams of the subject that was given up on";

    let builds = Arc::new(AtomicUsize::new(0));
    let describes = Arc::new(AtomicUsize::new(0));

    // The panic is caught in place rather than declared with
    // `#[should_panic]`, which ends the test at the panic and would leave the
    // build and report counts below unasserted. Catching it here, rather than
    // in a spawned task, keeps the policy's borrowed-argument futures out of
    // the `Send` bound a task would impose on them.
    //
    // The panic's own message is printed to the test output by the default
    // hook as it unwinds. It is the expected outcome of this test, not a
    // failure.
    let outcome = AssertUnwindSafe(start_until_ready(
        LABEL,
        3,
        BUDGET,
        async || Subject {
            serial: builds.fetch_add(1, Ordering::SeqCst) + 1,
        },
        async |_subject: &Subject| false,
        async |_subject: &Subject| {
            describes.fetch_add(1, Ordering::SeqCst);
            String::from(REPORT)
        },
    ))
    .catch_unwind()
    .await;

    let payload = match outcome {
        Ok(subject) => panic!(
            "a subject that never comes up must not be handed back, yet the policy returned \
             {subject:?}"
        ),
        Err(payload) => payload,
    };
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();

    assert!(
        message.contains(LABEL),
        "the panic must name which start path gave up, but it reads: {message}"
    );
    assert!(
        message.contains(REPORT),
        "the panic must carry the report of the subject it gave up on, so a boot failure that \
         repeats stays diagnosable, but it reads: {message}"
    );
    assert_eq!(
        builds.load(Ordering::SeqCst),
        3,
        "every attempt the caller paid for must build a subject of its own"
    );
    assert_eq!(
        describes.load(Ordering::SeqCst),
        1,
        "the report is read once, from the last subject only: producing it costs a round trip \
         to whatever the subject is backed by"
    );
}

#[test]
fn a_log_without_the_marker_does_not_report_the_broker_ready() {
    let booting = "Starting broker...\n node : rabbit@container\n home dir : /var/lib/rabbitmq";

    assert!(
        !log_reports_broker_ready(booting),
        "a broker that has printed nothing but its banner is not ready yet"
    );
    assert!(
        !log_reports_broker_ready(""),
        "a broker that has printed nothing at all is not ready yet"
    );
}

#[test]
fn the_marker_is_matched_as_a_prefix_of_the_full_startup_line() {
    // The real line ends with a plugin count. Matching the count would tie the
    // suite to the image: enabling one more plugin would leave every wait
    // hanging with no usable message.
    let with_four_plugins = "Server startup complete; 4 plugins started.";
    let with_none = "Server startup complete; 0 plugins started.";

    assert!(
        log_reports_broker_ready(with_four_plugins),
        "the startup line must be recognised whatever plugin count follows it"
    );
    assert!(
        log_reports_broker_ready(with_none),
        "the startup line must be recognised whatever plugin count follows it"
    );
}
