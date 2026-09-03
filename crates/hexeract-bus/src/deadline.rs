//! Absolute deadlines carried across the bus by the request-reply protocol.
//!
//! A deadline grants a responder the right to refuse work whose caller has
//! already given up. It is not remote cancellation: a handler that is
//! already running is never interrupted from the outside.
//!
//! Two clocks are involved and the distinction is load-bearing. The wall
//! clock is comparable between machines, which is what lets a deadline mean
//! the same thing on both sides of the wire, but it can jump backwards or be
//! corrected under a running process. The monotonic clock never jumps but
//! carries no meaning outside this process. A wire deadline is therefore
//! read against the wall clock exactly once, in [`Deadline::anchor`], and
//! every later decision reads the [`LocalDeadline`] that anchoring produced.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

/// Furthest into the future an inbound deadline is accepted.
///
/// The bound makes the arithmetic total, since adding an unbounded
/// millisecond count to a [`SystemTime`] can overflow, and refuses values
/// that carry no meaning for a request-reply call.
const MAX_DEADLINE_HORIZON: Duration = Duration::from_secs(3600);

/// How far past its deadline a request is still accepted, absorbing the
/// ordinary drift between two synchronized clocks.
///
/// Deliberately small. Sustained skew is an operational fault to fix, not a
/// condition to accommodate.
const CLOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(1);

/// Absolute instant, shared across processes, after which a request must no
/// longer be executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Deadline(SystemTime);

impl Deadline {
    /// Builds the deadline a caller reaches `timeout` from now.
    #[must_use]
    pub fn after(timeout: Duration) -> Self {
        Self::from_wall_clock(SystemTime::now(), timeout)
    }

    /// Builds the deadline reached `timeout` after `now`.
    ///
    /// A `timeout` that overflows the wall clock yields `now` itself, an
    /// already reached deadline. Refusing the work is the safe side of that
    /// arithmetic edge.
    #[must_use]
    pub fn from_wall_clock(now: SystemTime, timeout: Duration) -> Self {
        Self(now.checked_add(timeout).unwrap_or(now))
    }

    /// Rebuilds a deadline from the decimal Unix milliseconds carried on the
    /// wire.
    ///
    /// # Errors
    ///
    /// Returns [`DeadlineViolation::Unreadable`] when the offset is not
    /// representable as a [`SystemTime`] on this platform.
    pub fn from_unix_millis(millis: i64) -> Result<Self, DeadlineViolation> {
        let offset = Duration::from_millis(millis.unsigned_abs());
        let instant = if millis >= 0 {
            SystemTime::UNIX_EPOCH.checked_add(offset)
        } else {
            SystemTime::UNIX_EPOCH.checked_sub(offset)
        };
        instant.map(Self).ok_or(DeadlineViolation::Unreadable)
    }

    /// Renders the deadline as the decimal Unix milliseconds carried on the
    /// wire.
    ///
    /// Saturates at the `i64` bounds rather than wrapping. A deadline that
    /// far out is already refused by [`Deadline::anchor`].
    #[must_use]
    pub fn to_unix_millis(self) -> i64 {
        match self.0.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(since) => i64::try_from(since.as_millis()).unwrap_or(i64::MAX),
            Err(before) => {
                i64::try_from(before.duration().as_millis()).map_or(i64::MIN, |millis| -millis)
            }
        }
    }

    /// Judges this deadline against the wall-clock reading `now` and anchors
    /// it onto the monotonic clock.
    ///
    /// This is the single place where a wire deadline meets wall-clock time.
    /// The skew tolerance is applied here, once, and is therefore already
    /// carried by the returned [`LocalDeadline`]: a request is judged
    /// against one anchor for its whole lifetime, never against a tolerance
    /// that would compound at each later check.
    ///
    /// Never returns [`DeadlineReading::Absent`], which only
    /// [`read_deadline`](crate::read_deadline) can produce: reaching this
    /// method already means a deadline was present on the wire.
    #[must_use]
    pub fn anchor(self, now: SystemTime) -> DeadlineReading {
        let horizon = now.checked_add(MAX_DEADLINE_HORIZON);
        if horizon.is_none_or(|horizon| self.0 > horizon) {
            return DeadlineReading::Invalid(DeadlineViolation::BeyondHorizon);
        }
        let Some(tolerated) = self.0.checked_add(CLOCK_SKEW_TOLERANCE) else {
            return DeadlineReading::Invalid(DeadlineViolation::BeyondHorizon);
        };
        match tolerated.duration_since(now) {
            Ok(remaining) => DeadlineReading::Live(LocalDeadline::after(remaining)),
            Err(_elapsed) => DeadlineReading::Expired,
        }
    }
}

impl FromStr for Deadline {
    type Err = DeadlineViolation;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let millis: i64 = value.parse().map_err(|_| DeadlineViolation::Unreadable)?;
        Self::from_unix_millis(millis)
    }
}

impl fmt::Display for Deadline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.to_unix_millis())
    }
}

/// The same deadline seen from the local monotonic clock, immune to
/// wall-clock adjustments once established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalDeadline(tokio::time::Instant);

impl LocalDeadline {
    /// Builds a deadline reached `remaining` from now on the monotonic clock.
    ///
    /// Application code needs this only to unit-test a handler against a
    /// [`RequestContext`](crate::RequestContext) it builds itself.
    #[must_use]
    pub fn after(remaining: Duration) -> Self {
        Self(tokio::time::Instant::now() + remaining)
    }

    /// Time left before expiry, or `None` once the deadline has passed.
    #[must_use]
    pub fn remaining(self) -> Option<Duration> {
        self.0.checked_duration_since(tokio::time::Instant::now())
    }

    /// Whether the deadline has already passed.
    #[must_use]
    pub fn is_expired(self) -> bool {
        self.remaining().is_none()
    }

    /// The underlying instant, for callers driving
    /// [`tokio::time::timeout_at`].
    #[must_use]
    pub fn as_instant(self) -> tokio::time::Instant {
        self.0
    }
}

/// Why a deadline carried on the wire cannot be honored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DeadlineViolation {
    /// The value is not a decimal Unix millisecond count, or the instant it
    /// names is not representable on this platform.
    #[error("deadline header is not a decimal Unix millisecond count")]
    Unreadable,
    /// The deadline lies further ahead than the accepted horizon.
    #[error("deadline lies beyond the accepted horizon")]
    BeyondHorizon,
}

/// What a responder learned from an inbound deadline header.
///
/// The four cases call for three different dispositions, which is why they
/// are not collapsed: an absent deadline is nominal, an expired one is
/// dropped silently because its caller has already failed locally, and an
/// invalid one is answered with a protocol error because it signals a defect
/// in a peer that believes it speaks this protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineReading {
    /// The caller set no deadline.
    Absent,
    /// The deadline is valid and still ahead.
    Live(LocalDeadline),
    /// The deadline is valid but already elapsed, skew tolerance included.
    Expired,
    /// The deadline is present but unusable.
    Invalid(DeadlineViolation),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch_plus(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    #[test]
    fn a_deadline_round_trips_through_decimal_unix_milliseconds() {
        let deadline = Deadline::from_unix_millis(1_756_800_000_000).expect("representable");

        assert_eq!(deadline.to_unix_millis(), 1_756_800_000_000);
        assert_eq!(deadline.to_string(), "1756800000000");
    }

    #[test]
    fn a_non_numeric_header_value_is_unreadable() {
        assert_eq!(
            "not-a-number".parse::<Deadline>(),
            Err(DeadlineViolation::Unreadable)
        );
    }

    #[test]
    fn an_rfc_3339_header_value_is_unreadable() {
        assert_eq!(
            "2026-09-02T20:00:00Z".parse::<Deadline>(),
            Err(DeadlineViolation::Unreadable)
        );
    }

    #[test]
    fn a_deadline_beyond_the_horizon_is_refused_rather_than_honored() {
        let now = epoch_plus(1_000);
        let deadline = Deadline::from_wall_clock(now, Duration::from_secs(3_601));

        assert_eq!(
            deadline.anchor(now),
            DeadlineReading::Invalid(DeadlineViolation::BeyondHorizon)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_exactly_on_the_horizon_is_honored() {
        let now = epoch_plus(1_000);
        let deadline = Deadline::from_wall_clock(now, Duration::from_secs(3_600));

        assert!(matches!(deadline.anchor(now), DeadlineReading::Live(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_future_deadline_anchors_with_its_remaining_time() {
        let now = epoch_plus(1_000);
        let deadline = Deadline::from_wall_clock(now, Duration::from_secs(30));

        let DeadlineReading::Live(local) = deadline.anchor(now) else {
            panic!("a deadline thirty seconds away must anchor as live");
        };

        assert_eq!(
            local.remaining(),
            Some(Duration::from_secs(30) + CLOCK_SKEW_TOLERANCE)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_elapsed_within_the_skew_tolerance_is_still_honored() {
        let deadline = Deadline(epoch_plus(1_000));

        let reading = deadline.anchor(epoch_plus(1_000) + Duration::from_millis(500));

        assert!(matches!(reading, DeadlineReading::Live(_)));
    }

    #[test]
    fn a_deadline_elapsed_beyond_the_skew_tolerance_is_expired() {
        let deadline = Deadline(epoch_plus(1_000));

        let reading = deadline.anchor(epoch_plus(1_000) + Duration::from_secs(2));

        assert_eq!(reading, DeadlineReading::Expired);
    }

    #[tokio::test(start_paused = true)]
    async fn a_local_deadline_reports_less_time_as_the_monotonic_clock_advances() {
        let local = LocalDeadline::after(Duration::from_secs(10));

        tokio::time::advance(Duration::from_secs(4)).await;

        assert_eq!(local.remaining(), Some(Duration::from_secs(6)));
        assert!(!local.is_expired());
    }

    #[tokio::test(start_paused = true)]
    async fn a_local_deadline_reports_no_time_left_once_it_has_passed() {
        let local = LocalDeadline::after(Duration::from_secs(10));

        tokio::time::advance(Duration::from_secs(11)).await;

        assert_eq!(local.remaining(), None);
        assert!(local.is_expired());
    }

    #[test]
    fn a_timeout_that_overflows_the_wall_clock_yields_an_immediately_reached_deadline() {
        let now = epoch_plus(1_000);

        let deadline = Deadline::from_wall_clock(now, Duration::MAX);

        assert_eq!(deadline, Deadline(now));
    }
}
