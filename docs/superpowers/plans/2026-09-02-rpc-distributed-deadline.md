# RPC Distributed Deadline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry the caller's effective timeout across the bus as an absolute deadline so a responder can refuse work the caller has already abandoned, closing issue #440.

**Architecture:** A new `deadline` module owns two types: `Deadline` holds a wall-clock instant and is what travels on the wire as decimal Unix milliseconds; `LocalDeadline` holds a `tokio::time::Instant` and is what every local decision reads. The wall clock is read exactly once per inbound request, when `rpc_protocol::read_deadline` anchors the wire value onto the monotonic clock. The responder gains a fifth guard between the protocol-version check and payload decoding, plus a recheck immediately before publishing a reply.

**Tech Stack:** Rust 2024, `tokio` (time), `thiserror`, `tracing`, `uuid`. No new dependency is introduced.

**Spec:** `docs/superpowers/specs/2026-09-02-rpc-distributed-deadline-design.md`

## Global Constraints

- Before any `cargo` command in PowerShell, run `$env:PATH = "C:\msys64\mingw64\bin;" + $env:PATH`. Without it the MinGW linker fails on this machine.
- Clippy runs with `clippy::all` and `clippy::pedantic` at deny level. `unwrap` and `expect` are forbidden outside `#[cfg(test)]` code.
- Every task runs `cargo fmt --all -- --check` before committing, alongside clippy. The CI format gate refuses a branch rustfmt would rewrite, and clippy does not catch formatting.
- Every public item carries an English doc comment. No inline `//` commentary explaining what a line does; explanation belongs in doc comments.
- Never bump `PROTOCOL_VERSION`. Adding a header to protocol version 1 is compatible in both directions.
- `MAX_DEADLINE_HORIZON` is `Duration::from_secs(3600)`. `CLOCK_SKEW_TOLERANCE` is `Duration::from_secs(1)`. Both stay private to the `deadline` module.
- The wall clock is read in exactly one place in production code: the `SystemTime::now()` argument passed to `read_deadline` from `replied_handler`, plus `Deadline::after` on the caller side. Every function that judges a deadline takes `now` as a parameter so it can be tested without touching the system clock.
- Commit messages describe the change and its reasoning. They carry no attribution trailer of any kind.
- Branch is `feature/440-rpc-distributed-deadline`. Never commit to `main`.

---

### Task 1: The deadline module

**Files:**
- Create: `crates/hexeract-bus/src/deadline.rs`
- Modify: `crates/hexeract-bus/src/lib.rs:30` (module declaration, alphabetical among `pub mod` entries) and `crates/hexeract-bus/src/lib.rs:79` (re-exports, alphabetical)
- Test: inline `#[cfg(test)] mod tests` in `crates/hexeract-bus/src/deadline.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Deadline::after(Duration) -> Deadline`, `Deadline::from_wall_clock(SystemTime, Duration) -> Deadline`, `Deadline::from_unix_millis(i64) -> Result<Deadline, DeadlineViolation>`, `Deadline::to_unix_millis(self) -> i64`, `Deadline::anchor(self, SystemTime) -> DeadlineReading`, `impl FromStr for Deadline`, `impl Display for Deadline`, `LocalDeadline::after(Duration) -> LocalDeadline`, `LocalDeadline::remaining(self) -> Option<Duration>`, `LocalDeadline::is_expired(self) -> bool`, `LocalDeadline::as_instant(self) -> tokio::time::Instant`, `enum DeadlineViolation { Unreadable, BeyondHorizon }`, `enum DeadlineReading { Absent, Live(LocalDeadline), Expired, Invalid(DeadlineViolation) }`.

- [ ] **Step 1: Write the failing tests**

Create `crates/hexeract-bus/src/deadline.rs` containing only the test module for now:

```rust
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hexeract-bus --lib deadline`
Expected: FAIL to compile, with errors naming `Deadline`, `LocalDeadline`, `DeadlineReading`, `DeadlineViolation` and `CLOCK_SKEW_TOLERANCE` as not found.

- [ ] **Step 3: Write the implementation**

Prepend to `crates/hexeract-bus/src/deadline.rs`, above the test module:

```rust
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
            Err(before) => i64::try_from(before.duration().as_millis())
                .map_or(i64::MIN, |millis| -millis),
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
```

- [ ] **Step 4: Declare and re-export the module**

In `crates/hexeract-bus/src/lib.rs`, add the module declaration in alphabetical position (after `pub mod` entries beginning with a letter before `d`, so immediately before `pub mod envelope;` on line 30):

```rust
pub mod deadline;
```

Then add the re-exports in alphabetical position, immediately before `pub use envelope::BusEnvelope;` on line 79:

```rust
pub use deadline::Deadline;
pub use deadline::DeadlineReading;
pub use deadline::DeadlineViolation;
pub use deadline::LocalDeadline;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p hexeract-bus --lib deadline`
Expected: PASS, eleven tests.

- [ ] **Step 6: Check lints**

Run: `cargo clippy -p hexeract-bus --all-targets -- -D warnings`
Expected: no warning.

- [ ] **Step 7: Commit**

```bash
git add crates/hexeract-bus/src/deadline.rs crates/hexeract-bus/src/lib.rs
git commit -m "feat(bus): add the deadline types carried by the RPC protocol

Deadline holds the wall-clock instant that travels on the wire as decimal
Unix milliseconds. LocalDeadline holds the monotonic anchor every local
decision reads afterwards.

Deadline::anchor is the single point where the two clocks meet. It rejects
a deadline beyond a one hour horizon, which also makes the arithmetic
total, applies the one second skew tolerance exactly once, and reports
whether the request is still worth serving."
```

---

### Task 2: Reading the deadline header

**Files:**
- Modify: `crates/hexeract-bus/src/rpc_protocol.rs:33-36` (the `DEADLINE_HEADER` doc comment) and the end of the constant block, plus `crates/hexeract-bus/src/lib.rs:113` (re-export)
- Test: inline `#[cfg(test)] mod tests` in `crates/hexeract-bus/src/rpc_protocol.rs`

**Interfaces:**
- Consumes: `Deadline`, `DeadlineReading`, `DeadlineViolation` from Task 1.
- Produces: `rpc_protocol::read_deadline(&BusEnvelope, SystemTime) -> DeadlineReading` and the re-exported constant `DEADLINE_HEADER`.

- [ ] **Step 1: Write the failing tests**

Append inside the existing `mod tests` of `crates/hexeract-bus/src/rpc_protocol.rs`. The module already has a `request_envelope()` helper building a `BusEnvelope` through `BusEnvelope::restore`; reuse it, and set the header through `insert_protocol_header`.

```rust
    #[test]
    fn an_envelope_without_a_deadline_header_reads_as_absent() {
        let envelope = request_envelope();

        assert_eq!(
            read_deadline(&envelope, SystemTime::UNIX_EPOCH),
            DeadlineReading::Absent
        );
    }

    #[test]
    fn a_future_deadline_header_reads_as_live() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut envelope = request_envelope();
        envelope.insert_protocol_header(
            DEADLINE_HEADER,
            Deadline::from_wall_clock(now, Duration::from_secs(30)).to_string(),
        );

        assert!(matches!(
            read_deadline(&envelope, now),
            DeadlineReading::Live(_)
        ));
    }

    #[test]
    fn an_elapsed_deadline_header_reads_as_expired() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut envelope = request_envelope();
        envelope.insert_protocol_header(
            DEADLINE_HEADER,
            Deadline::from_wall_clock(now, Duration::ZERO).to_string(),
        );

        assert_eq!(
            read_deadline(&envelope, now + Duration::from_secs(5)),
            DeadlineReading::Expired
        );
    }

    #[test]
    fn an_unparsable_deadline_header_reads_as_invalid() {
        let mut envelope = request_envelope();
        envelope.insert_protocol_header(DEADLINE_HEADER, "yesterday".to_owned());

        assert_eq!(
            read_deadline(&envelope, SystemTime::UNIX_EPOCH),
            DeadlineReading::Invalid(DeadlineViolation::Unreadable)
        );
    }
```

Add the imports the tests need at the top of the test module:

```rust
    use std::time::{Duration, SystemTime};

    use crate::deadline::{Deadline, DeadlineReading, DeadlineViolation};
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hexeract-bus --lib rpc_protocol`
Expected: FAIL to compile, with `read_deadline` not found.

- [ ] **Step 3: Correct the header documentation and add the reader**

The existing doc comment on `DEADLINE_HEADER` announces an RFC 3339 value. It was written when the header was reserved but never honored, so no peer has ever produced that form and nothing is broken by changing it. Decimal Unix milliseconds have one single rendering per instant, which is what keeps the canonical form signed by #444 stable.

Replace lines 33 to 36 of `crates/hexeract-bus/src/rpc_protocol.rs`:

```rust
/// Header carrying the absolute request deadline, as a decimal count of Unix
/// milliseconds in UTC.
///
/// One instant has exactly one rendering in this form, which keeps the
/// canonical representation stable for envelope authentication. Optional: a
/// request without this header is served with no deadline.
pub const DEADLINE_HEADER: &str = "x-hexeract-deadline";
```

Then append the reader after `read_protocol_version`:

```rust
/// Read and judge the optional deadline announced by an envelope.
///
/// `now` is supplied by the caller rather than read here, so the judgement
/// is a pure function of its inputs and can be tested without touching the
/// system clock. The responder passes `SystemTime::now()`; that call is the
/// one wall-clock reading of an inbound request.
#[must_use]
pub fn read_deadline(
    envelope: &crate::BusEnvelope,
    now: std::time::SystemTime,
) -> crate::deadline::DeadlineReading {
    use crate::deadline::{Deadline, DeadlineReading};

    let Some(raw) = envelope.header(DEADLINE_HEADER) else {
        return DeadlineReading::Absent;
    };
    match raw.parse::<Deadline>() {
        Ok(deadline) => deadline.anchor(now),
        Err(violation) => DeadlineReading::Invalid(violation),
    }
}
```

- [ ] **Step 4: Re-export the constant**

`crates/hexeract-bus/src/lib.rs` groups the `rpc_protocol` re-exports as constants first, then free functions. Respect that grouping.

Add the constant immediately before `pub use rpc_protocol::PROTOCOL_VERSION;` on line 113:

```rust
pub use rpc_protocol::DEADLINE_HEADER;
```

Add the function immediately before `pub use rpc_protocol::is_reserved_header;` on line 121:

```rust
pub use rpc_protocol::read_deadline;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p hexeract-bus --lib rpc_protocol`
Expected: PASS, including the four new tests.

- [ ] **Step 6: Commit**

```bash
git add crates/hexeract-bus/src/rpc_protocol.rs crates/hexeract-bus/src/lib.rs
git commit -m "feat(bus): read the deadline header into a judged reading

read_deadline takes the wall-clock reading as a parameter instead of
calling the clock itself, so the judgement is a pure function of its
inputs and every case is testable without touching the system clock.

The header documentation announced an RFC 3339 value while the header was
reserved and never honored. No peer ever produced that form. Decimal Unix
milliseconds render one instant exactly one way, which is what keeps the
canonical representation stable for envelope authentication."
```

---

### Task 3: The caller writes the deadline

**Files:**
- Modify: `crates/hexeract-bus/src/request_client.rs:522-523` (immediately after the two existing `insert_protocol_header` calls in `request_inner`)
- Test: inline `#[cfg(test)] mod tests` in `crates/hexeract-bus/src/request_client.rs`

**Interfaces:**
- Consumes: `Deadline` from Task 1, `DEADLINE_HEADER` from Task 2.
- Produces: outbound request envelopes carrying `x-hexeract-deadline`. No public API changes.

- [ ] **Step 1: Write the failing tests**

The test module already provides `CapturingTransport` (with `last_published()`), the `client(transport, registry)` helper whose default timeout is hard-coded to `Duration::from_millis(200)`, and the `Ping` request type. Reuse all of them; add no second transport.

Both tests follow the shape of the existing `nominal_round_trip_returns_typed_reply`: drive the request future far enough to publish, then inspect what was published, without ever completing the call.

```rust
    #[tokio::test(start_paused = true)]
    async fn a_published_request_carries_a_deadline_derived_from_the_default_timeout() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let request_fut = client.request(Ping { seq: 1 });
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let published = transport.last_published().expect("a request was published");
        let deadline: Deadline = published
            .header(DEADLINE_HEADER)
            .expect("a request carries a deadline")
            .parse()
            .expect("a decimal millisecond count");
        assert!(matches!(
            deadline.anchor(SystemTime::now()),
            DeadlineReading::Live(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn an_explicit_per_call_timeout_replaces_the_client_default() {
        let transport = Arc::new(CapturingTransport::default());
        let registry = Arc::new(RequestRegistry::default());
        let client = client(Arc::clone(&transport), Arc::clone(&registry));

        let request_fut = client.request_with(
            Ping { seq: 1 },
            RequestOptions::new().with_timeout(Duration::from_secs(600)),
        );
        tokio::pin!(request_fut);
        tokio::select! {
            _ = &mut request_fut => panic!("should still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let published = transport.last_published().expect("a request was published");
        let deadline: Deadline = published
            .header(DEADLINE_HEADER)
            .expect("a request carries a deadline")
            .parse()
            .expect("a decimal millisecond count");
        assert!(
            deadline > Deadline::from_wall_clock(SystemTime::now(), Duration::from_millis(200)),
            "a ten minute call must publish a deadline far beyond the client default"
        );
    }
```

The second test compares deadlines rather than asserting an exact instant, which is why `Deadline` derives `PartialOrd` and `Ord` in Task 1. Note that `start_paused` freezes the Tokio clock but never `SystemTime`, so both assertions stay true regardless of how long the test takes in real time: the one second skew tolerance leaves ample margin.

Add to the test module imports:

```rust
    use std::time::SystemTime;

    use crate::deadline::{Deadline, DeadlineReading};
    use crate::rpc_protocol::DEADLINE_HEADER;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hexeract-bus --lib request_client::tests::a_published_request_carries`
Expected: FAIL, the header is absent so `expect("a request carries a deadline")` panics.

- [ ] **Step 3: Write the implementation**

In `request_inner`, immediately after the existing two calls:

```rust
        envelope.insert_protocol_header(REQUEST_ID_HEADER, request_id.to_string());
        envelope.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
        envelope.insert_protocol_header(DEADLINE_HEADER, Deadline::after(timeout).to_string());
```

Extend the existing `use crate::rpc_protocol::{...}` import in this file with `DEADLINE_HEADER`, and add `use crate::deadline::Deadline;`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p hexeract-bus --lib request_client`
Expected: PASS, the whole module including the pre-existing timeout tests.

- [ ] **Step 5: Commit**

```bash
git add crates/hexeract-bus/src/request_client.rs
git commit -m "feat(bus): publish the caller deadline with every request

The effective timeout, whether the client default or a per-call override,
now also travels as an absolute deadline so the responder can refuse work
this caller will no longer wait for.

The local monotonic deadline that bounds publication and waiting is
unchanged, and so is the RAII removal of the correlation slot on timeout.
No public API changes."
```

---

### Task 4: The handler can consult its deadline

**Files:**
- Modify: `crates/hexeract-bus/src/request_context.rs:15-27` (struct fields) and `crates/hexeract-bus/src/request_context.rs:47-55` (add methods after `new`)
- Test: inline `#[cfg(test)] mod tests` in `crates/hexeract-bus/src/request_context.rs`

**Interfaces:**
- Consumes: `LocalDeadline` from Task 1.
- Produces: `RequestContext::with_deadline(self, LocalDeadline) -> Self`, `RequestContext::remaining(&self) -> Option<Duration>`, and the public field `RequestContext::deadline: Option<LocalDeadline>`.

- [ ] **Step 1: Write the failing tests**

Append inside the existing `mod tests` of `crates/hexeract-bus/src/request_context.rs`:

```rust
    #[test]
    fn a_context_built_without_a_deadline_reports_no_remaining_time() {
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());

        let ctx = RequestContext::new(RequestId::new(), 1, &handler_ctx);

        assert_eq!(ctx.deadline, None);
        assert_eq!(ctx.remaining(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_time_shrinks_as_the_handler_works() {
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let ctx = RequestContext::new(RequestId::new(), 1, &handler_ctx)
            .with_deadline(LocalDeadline::after(Duration::from_secs(10)));

        tokio::time::advance(Duration::from_secs(7)).await;

        assert_eq!(ctx.remaining(), Some(Duration::from_secs(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_time_is_absent_once_the_deadline_has_passed() {
        let handler_ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
        let ctx = RequestContext::new(RequestId::new(), 1, &handler_ctx)
            .with_deadline(LocalDeadline::after(Duration::from_secs(10)));

        tokio::time::advance(Duration::from_secs(11)).await;

        assert_eq!(ctx.remaining(), None);
    }
```

Add to the test module imports:

```rust
    use std::time::Duration;

    use crate::deadline::LocalDeadline;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p hexeract-bus --lib request_context`
Expected: FAIL to compile, `with_deadline` and `remaining` not found, and no field `deadline`.

- [ ] **Step 3: Write the implementation**

Add the field to the struct, after `protocol_version` and before `handler`:

```rust
    /// Absolute deadline the caller attached to this call, anchored on the
    /// local monotonic clock, or `None` when the caller set none.
    ///
    /// Reaching the deadline does not interrupt a running handler. A handler
    /// doing long or segmented work is expected to consult
    /// [`RequestContext::remaining`] itself and stop early when it chooses
    /// to; the framework only refuses work before dispatch and suppresses a
    /// reply nobody awaits.
    pub deadline: Option<LocalDeadline>,
```

Set it to `None` in `new`, then add after `new`:

```rust
    /// Attaches the caller's deadline to a context.
    ///
    /// Separate from [`RequestContext::new`] so that adding it breaks no
    /// existing caller, in particular the application unit tests that build
    /// a context to exercise their own handler.
    #[must_use]
    pub fn with_deadline(mut self, deadline: LocalDeadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Time left before the caller's deadline, or `None` when no deadline
    /// was set or it has already passed.
    ///
    /// Recomputed on every call rather than frozen at dispatch, so a handler
    /// consulting it after thirty seconds of work sees thirty seconds less.
    #[must_use]
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline?.remaining()
    }
```

Add the imports at the top of the file:

```rust
use std::time::Duration;

use crate::deadline::LocalDeadline;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p hexeract-bus --lib request_context`
Expected: PASS, including the pre-existing `new_carries_the_three_arguments_it_was_given`.

- [ ] **Step 5: Commit**

```bash
git add crates/hexeract-bus/src/request_context.rs
git commit -m "feat(bus): expose the caller deadline to a request handler

RequestContext gains an optional deadline and a remaining() accessor that
recomputes on every call, so a handler consulting it mid-work sees the time
actually left rather than a value frozen at dispatch.

The field arrives through with_deadline rather than through new, as the
type documentation already anticipated, so no caller unit-testing its own
handler through new has to change."
```

---

### Task 5: The responder refuses expired work

**Files:**
- Modify: `crates/hexeract-bus/src/responder_counters.rs` (three counters, their snapshot fields, and the type doc), `crates/hexeract-bus/src/replied_handler.rs:135-172` (the guard-order doc), `crates/hexeract-bus/src/replied_handler.rs:206-219` (insert the new guard after the version check), `crates/hexeract-bus/src/replied_handler.rs:238` (build the context with the deadline) and `crates/hexeract-bus/src/replied_handler.rs:270-275` (recheck before publishing)
- Test: inline `#[cfg(test)] mod tests` in both files

**Interfaces:**
- Consumes: `DeadlineReading`, `DeadlineViolation`, `LocalDeadline` from Task 1; `read_deadline` and `DEADLINE_HEADER` from Task 2; `RequestContext::with_deadline` from Task 4.
- Produces: `ResponderCountersSnapshot::expired_deadline`, `ResponderCountersSnapshot::invalid_deadline`, `ResponderCountersSnapshot::reply_dropped_after_deadline`, all `u64`.

- [ ] **Step 1: Write the failing counter tests**

In `crates/hexeract-bus/src/responder_counters.rs`, update the two existing tests to include the three new fields set to `0`, then append:

```rust
    #[test]
    fn deadline_rejections_are_counted_apart_from_one_another() {
        let counters = ResponderCounters::default();

        counters.count_expired_deadline();
        counters.count_invalid_deadline();
        counters.count_reply_dropped_after_deadline();
        counters.count_expired_deadline();

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.expired_deadline, 2);
        assert_eq!(snapshot.invalid_deadline, 1);
        assert_eq!(snapshot.reply_dropped_after_deadline, 1);
    }
```

- [ ] **Step 2: Write the failing responder tests**

The test module in `crates/hexeract-bus/src/replied_handler.rs` already provides everything these tests need: `RecordingReplyPublisher` (its `published` field is a `StdMutex<Vec<(String, BusEnvelope)>>`, plus a `last_published()` accessor), `request_envelope(reply_to: Option<&str>)` which stamps a valid request id and protocol version, `ctx()` which builds a `HandlerContext`, the `Echo` handler for the nominal path, and `RecordingHandler { ran: Arc<AtomicBool> }` for proving a guard stopped dispatch. Reply destinations must start with `amq.gen-`, so pass `Some("amq.gen-inbox")`.

This task adds two helpers to that module, alongside the existing ones:

```rust
    fn a_deadline_that_passed() -> Deadline {
        Deadline::from_wall_clock(SystemTime::now() - Duration::from_secs(60), Duration::ZERO)
    }

    struct SlowHandler {
        work: Duration,
    }

    impl RequestHandler<Ping> for SlowHandler {
        type Error = BusError;

        async fn handle(&self, request: Ping, _ctx: &RequestContext<'_>) -> Result<Pong, BusError> {
            tokio::time::sleep(self.work).await;
            Ok(Pong { seq: request.seq })
        }
    }

    struct DeadlineCapturingHandler {
        remaining: Arc<StdMutex<Option<Duration>>>,
    }

    impl RequestHandler<Ping> for DeadlineCapturingHandler {
        type Error = BusError;

        async fn handle(&self, request: Ping, ctx: &RequestContext<'_>) -> Result<Pong, BusError> {
            *self.remaining.lock().unwrap() = ctx.remaining();
            Ok(Pong { seq: request.seq })
        }
    }
```

Under `start_paused`, `SlowHandler`'s sleep advances the Tokio clock without waiting in real time, which is what lets a deadline elapse mid-handler in a test that runs instantly.

Then append the tests:

```rust
    #[tokio::test(start_paused = true)]
    async fn a_request_expired_before_dispatch_never_reaches_the_handler() {
        let ran = Arc::new(AtomicBool::new(false));
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let counters = ResponderCounters::default();
        let erased = RepliedHandler::with_counters(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
            counters.clone(),
        );
        let mut request = request_envelope(Some("amq.gen-inbox"));
        request.insert_protocol_header(DEADLINE_HEADER, a_deadline_that_passed().to_string());

        erased.handle(&request, &ctx()).await.unwrap();

        assert!(!ran.load(Ordering::Relaxed), "the handler must not run");
        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "an expired request is dropped, never answered"
        );
        assert_eq!(counters.snapshot().expired_deadline, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreadable_deadline_is_answered_with_a_protocol_error() {
        let ran = Arc::new(AtomicBool::new(false));
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let counters = ResponderCounters::default();
        let erased = RepliedHandler::with_counters(
            RecordingHandler {
                ran: Arc::clone(&ran),
            },
            Arc::clone(&publisher),
            counters.clone(),
        );
        let mut request = request_envelope(Some("amq.gen-inbox"));
        request.insert_protocol_header(DEADLINE_HEADER, "soon".to_owned());

        erased.handle(&request, &ctx()).await.unwrap();

        assert!(!ran.load(Ordering::Relaxed), "the handler must not run");
        let published = publisher.last_published().expect("an error reply");
        assert_eq!(published.message_type, REPLY_ERROR_MESSAGE_TYPE);
        assert_eq!(counters.snapshot().invalid_deadline, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_without_a_deadline_is_served_as_before() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let counters = ResponderCounters::default();
        let erased = RepliedHandler::with_counters(Echo, Arc::clone(&publisher), counters.clone());

        erased
            .handle(&request_envelope(Some("amq.gen-inbox")), &ctx())
            .await
            .unwrap();

        let published = publisher.last_published().expect("a reply");
        assert_eq!(published.header(REPLY_STATUS_HEADER), Some(REPLY_STATUS_OK));
        assert_eq!(counters.snapshot().expired_deadline, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn the_handler_sees_the_time_left_before_the_caller_deadline() {
        let remaining = Arc::new(StdMutex::new(None));
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let erased = RepliedHandler::new(
            DeadlineCapturingHandler {
                remaining: Arc::clone(&remaining),
            },
            Arc::clone(&publisher),
        );
        let mut request = request_envelope(Some("amq.gen-inbox"));
        request.insert_protocol_header(
            DEADLINE_HEADER,
            Deadline::from_wall_clock(SystemTime::now(), Duration::from_secs(30)).to_string(),
        );

        erased.handle(&request, &ctx()).await.unwrap();

        let seen = remaining
            .lock()
            .unwrap()
            .expect("the handler must see a deadline");
        assert!(
            seen > Duration::from_secs(25),
            "roughly thirty seconds must still be left, saw {seen:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_is_not_published_when_the_deadline_passed_while_handling() {
        let publisher = Arc::new(RecordingReplyPublisher::default());
        let counters = ResponderCounters::default();
        let erased = RepliedHandler::with_counters(
            SlowHandler {
                work: Duration::from_secs(30),
            },
            Arc::clone(&publisher),
            counters.clone(),
        );
        let mut request = request_envelope(Some("amq.gen-inbox"));
        request.insert_protocol_header(
            DEADLINE_HEADER,
            Deadline::from_wall_clock(SystemTime::now(), Duration::from_secs(5)).to_string(),
        );

        erased.handle(&request, &ctx()).await.unwrap();

        assert!(
            publisher.published.lock().unwrap().is_empty(),
            "a reply nobody awaits must not be published"
        );
        assert_eq!(counters.snapshot().reply_dropped_after_deadline, 1);
    }
```

Add to the test module imports whichever of these are not already present:

```rust
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, SystemTime};

    use crate::deadline::Deadline;
    use crate::rpc_protocol::DEADLINE_HEADER;
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p hexeract-bus --lib replied_handler responder_counters`
Expected: FAIL to compile, `count_expired_deadline` and the three snapshot fields not found.

- [ ] **Step 4: Extend the counters**

In `crates/hexeract-bus/src/responder_counters.rs`, add to `ResponderCountersInner`:

```rust
    expired_deadline: AtomicU64,
    invalid_deadline: AtomicU64,
    reply_dropped_after_deadline: AtomicU64,
```

Add to `ResponderCountersSnapshot`:

```rust
    /// Requests dropped because their deadline had already elapsed.
    ///
    /// The caller has already failed locally on its own timeout, so this
    /// rejection is silent on the wire. A sustained non-zero rate points at
    /// a saturated responder or at clocks drifting beyond the tolerance.
    pub expired_deadline: u64,
    /// Requests answered with a protocol error because their deadline header
    /// was unreadable or beyond the accepted horizon.
    pub invalid_deadline: u64,
    /// Replies suppressed because the deadline passed while the handler ran.
    ///
    /// The handler did its work and the request was acknowledged; only the
    /// publication was skipped, because the reply could reach nothing but an
    /// orphaned inbox.
    pub reply_dropped_after_deadline: u64,
```

Read them in `snapshot`, and add the three `pub(crate)` increment methods following the shape of `count_invalid_reply_to`.

Widen the type-level doc comment: its current text says the scope is inbound requests refused before the domain handler ran, which `reply_dropped_after_deadline` no longer satisfies. Replace that opening sentence with a statement that the counters cover requests refused on the RPC envelope contract before dispatch, plus replies the responder chose not to publish.

- [ ] **Step 5: Add the responder guard**

In `crates/hexeract-bus/src/replied_handler.rs`, immediately after the protocol version match block and before `let request: R = match envelope.decode()`:

```rust
            let deadline = match read_deadline(envelope, SystemTime::now()) {
                DeadlineReading::Absent => None,
                DeadlineReading::Live(deadline) => Some(deadline),
                DeadlineReading::Expired => {
                    self.counters.count_expired_deadline();
                    tracing::warn!(
                        message_type = R::MESSAGE_TYPE,
                        %request_id,
                        "request deadline already elapsed, dropping without running the handler"
                    );
                    return Ok(());
                }
                DeadlineReading::Invalid(violation) => {
                    self.counters.count_invalid_deadline();
                    tracing::warn!(
                        message_type = R::MESSAGE_TYPE,
                        %request_id,
                        ?violation,
                        "request carries an unusable deadline, rejecting"
                    );
                    let reply =
                        error_reply(RemoteErrorType::Unsupported, correlation_id, request_id)?;
                    self.replies.publish_reply(&reply_to, &reply).await?;
                    return Ok(());
                }
            };
```

Then build the context with the deadline:

```rust
            let mut request_context = RequestContext::new(request_id, protocol_version, ctx);
            if let Some(deadline) = deadline {
                request_context = request_context.with_deadline(deadline);
            }
```

And guard the publication, replacing the final `self.replies.publish_reply(&reply_to, &reply_envelope).await?;`:

```rust
            if deadline.is_some_and(LocalDeadline::is_expired) {
                self.counters.count_reply_dropped_after_deadline();
                tracing::warn!(
                    message_type = R::MESSAGE_TYPE,
                    %request_id,
                    "deadline passed while handling, suppressing a reply nobody awaits"
                );
                return Ok(());
            }
            self.replies
                .publish_reply(&reply_to, &reply_envelope)
                .await?;
```

Add the imports: `use std::time::SystemTime;`, `use crate::deadline::{DeadlineReading, LocalDeadline};`, and extend the existing `use crate::rpc_protocol::{...}` with `read_deadline`.

- [ ] **Step 6: Update the guard-order documentation**

The doc comment on `handle` currently opens with "Four guards run in a fixed order". Rewrite it to five, and insert between the existing points 3 and 4:

```rust
    /// 4. The deadline is judged fourth, after the version and before the
    ///    payload. After the version, because interpreting a protocol
    ///    header presupposes knowing which protocol is being spoken. Before
    ///    the decode, because deserializing a payload whose work is already
    ///    known to be pointless spends the exact resource the deadline
    ///    exists to protect. An elapsed deadline is dropped silently, like
    ///    guards 1 and 2, since its caller has already failed locally; an
    ///    unreadable or out-of-range one is answered, like guard 3, since it
    ///    signals a defect in a peer that believes it speaks this protocol.
```

Renumber the payload decode to point 5, and add a closing paragraph stating that the deadline is rechecked once more immediately before publication, and that a reply is suppressed rather than published when it has passed.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p hexeract-bus --lib`
Expected: PASS across the whole crate.

- [ ] **Step 8: Check lints**

Run: `cargo clippy -p hexeract-bus --all-targets -- -D warnings`
Expected: no warning.

- [ ] **Step 9: Commit**

```bash
git add crates/hexeract-bus/src/replied_handler.rs crates/hexeract-bus/src/responder_counters.rs
git commit -m "feat(bus): refuse request work whose deadline has passed

A fifth guard sits between the protocol version check and payload decoding.
After the version, because reading a protocol header presupposes knowing
the protocol; before the decode, because deserializing work already known
to be pointless spends the resource the deadline exists to protect.

An elapsed deadline is dropped and acknowledged, matching the reply_to and
request-id guards, since the caller has already failed on its own timeout.
An unreadable or out-of-range one is answered with a sanitized protocol
error, matching the version guard.

The deadline is rechecked once more before publishing: a reply whose
deadline passed during handling could reach nothing but an orphaned inbox,
so it is suppressed and counted rather than published."
```

---

### Task 6: End-to-end coverage, documentation and release notes

**Files:**
- Modify: `crates/hexeract-bus-rabbitmq/tests/request_reply.rs` (add one broker-backed test)
- Modify: `docs/concepts/request-reply.md`, `docs/architecture/rpc-protocol.md`, `docs/CHANGELOG.md`
- Test: `crates/hexeract-bus-rabbitmq/tests/request_reply.rs`

**Interfaces:**
- Consumes: everything produced by Tasks 1 to 5.
- Produces: no new code interface.

- [ ] **Step 1: Write the failing end-to-end test**

`BusEnvelope::insert_protocol_header` is `pub(crate)` to `hexeract-bus`, so an integration test in `hexeract-bus-rabbitmq` cannot stamp the deadline header through it. The request must be forged in raw AMQP instead. The file already imports `AMQPValue`, `ShortString`, `FieldTable`, `BasicPublishOptions` and `BasicProperties` for exactly this purpose; before writing, find the existing test in this file that forges a request by hand and align the AMQP properties below with the ones it sets, so the worker decodes this envelope the same way.

Append to `crates/hexeract-bus-rabbitmq/tests/request_reply.rs`:

```rust
/// Echo handler that records whether its body ever ran, so a test can prove
/// a guard stopped dispatch rather than merely suppressing the reply.
struct RecordingEcho {
    ran: Arc<AtomicBool>,
}

impl RequestHandler<Ping> for RecordingEcho {
    type Error = BusError;

    async fn handle(&self, request: Ping, _ctx: &RequestContext<'_>) -> Result<Pong, BusError> {
        self.ran.store(true, Ordering::Relaxed);
        Ok(Pong { seq: request.seq })
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_request_whose_deadline_expired_in_the_queue_never_reaches_the_handler() {
    let broker = harness::start_rabbitmq().await;
    let cancel = CancellationToken::new();
    let queue = "tests.ping.expired";

    declare_ping_queue(broker.uri(), queue).await;

    let ran = Arc::new(AtomicBool::new(false));
    let responder_transport = Arc::new(RabbitMqTransport::new(broker.uri()).await.unwrap());
    let worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(broker.uri(), 5, Duration::from_millis(200))
            .await
            .unwrap(),
    )
    .queue(queue)
    .register_request_handler::<Ping, _>(
        RecordingEcho {
            ran: Arc::clone(&ran),
        },
        Arc::clone(&responder_transport),
    )
    .build()
    .unwrap();
    let worker_cancel = cancel.clone();
    let worker_handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    let connection = RabbitMqConnection::connect(broker.uri())
        .await
        .expect("publisher connection must open");
    let channel = connection
        .create_channel()
        .await
        .expect("publisher channel must open");

    let envelope = BusEnvelope::new(Uuid::now_v7(), &Ping { seq: 5 }).expect("request encodes");
    let expired_millis = i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is after the epoch")
            .as_millis(),
    )
    .expect("representable")
        - 60_000;

    let mut headers = FieldTable::default();
    headers.insert(
        ShortString::from(REQUEST_ID_HEADER),
        AMQPValue::LongString(Uuid::now_v7().to_string().into()),
    );
    headers.insert(
        ShortString::from(PROTOCOL_VERSION_HEADER),
        AMQPValue::LongString(PROTOCOL_VERSION.to_string().into()),
    );
    headers.insert(
        ShortString::from(DEADLINE_HEADER),
        AMQPValue::LongString(expired_millis.to_string().into()),
    );

    channel
        .basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            &envelope.payload,
            BasicProperties::default()
                .with_headers(headers)
                .with_message_id(envelope.message_id.to_string().into())
                .with_correlation_id(envelope.correlation_id.to_string().into())
                .with_kind(Ping::MESSAGE_TYPE.into())
                .with_reply_to("amq.gen-probe".into()),
        )
        .await
        .expect("publish must succeed");

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        !ran.load(Ordering::Relaxed),
        "a request whose deadline expired in the queue must never reach the handler"
    );

    cancel.cancel();
    let _ = worker_handle.await;
}
```

Add to the file's imports:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use hexeract_bus::DEADLINE_HEADER;
```

- [ ] **Step 2: Run the test to verify it fails**

Docker must be running.

Run: `cargo test -p hexeract-bus-rabbitmq --test request_reply -- --ignored a_request_whose_deadline_expired`
Expected: FAIL on the `ran` assertion, because without the guard the worker dispatches the request normally.

- [ ] **Step 3: Confirm it passes against the implementation**

Run: `cargo test -p hexeract-bus-rabbitmq --test request_reply -- --ignored a_request_whose_deadline_expired`
Expected: PASS. If it fails on decoding rather than on the `ran` assertion, the forged AMQP properties do not match what the worker expects: compare them against the existing hand-forged request test in this same file and correct them there, not by weakening the assertion.

- [ ] **Step 4: Document the contract**

In `docs/concepts/request-reply.md`, add a section titled "Deadlines" stating, in this order: the caller's effective timeout travels as an absolute deadline; a responder refuses work whose deadline has passed rather than running it; a deadline is **not** remote cancellation, so a handler already running is never interrupted and a handler doing long work is expected to consult its remaining time itself; an expired request is dropped silently because its caller has already failed locally; caller and responder clocks are expected to be synchronized within one second, and sustained drift shows up in the responder's `expired_deadline` counter.

In `docs/architecture/rpc-protocol.md`, document `x-hexeract-deadline` alongside the existing headers: optional, decimal Unix milliseconds in UTC, refused beyond a one hour horizon, tolerated up to one second past.

- [ ] **Step 5: Add the release note**

In `docs/CHANGELOG.md`, under the unreleased section, following the formatting the file already uses:

```markdown
- Request-reply now propagates the caller's effective timeout as an absolute
  deadline in the `x-hexeract-deadline` header. A responder refuses a request
  whose deadline has already passed instead of running its handler, and
  suppresses a reply whose deadline passed while the handler ran. A handler
  can read its remaining time through `RequestContext::remaining`. (#440)
```

- [ ] **Step 6: Full verification**

Run each, expecting no failure and no warning:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps --all-features
```

- [ ] **Step 7: Confirm no attribution trailer reached any commit**

```bash
git log origin/main..HEAD --format='%B' | grep -iE 'co-authored|claude|generated with|anthropic' && echo "TRAILER FOUND" || echo "clean"
```
Expected: `clean`.

- [ ] **Step 8: Commit and push**

```bash
git add crates/hexeract-bus-rabbitmq/tests/request_reply.rs docs/concepts/request-reply.md docs/architecture/rpc-protocol.md docs/CHANGELOG.md
git commit -m "docs(rpc): document the distributed deadline contract

State plainly what a deadline is not: it authorizes a responder to refuse
work, never to interrupt a handler already running. A handler doing long
work consults its remaining time itself.

Adds the broker-backed test covering a request that expires in the queue,
and records the clock synchronization expectation alongside the counter
that reveals drift."
git push
```

---

## Execution notes

Tasks 1 and 2 are pure additions and can be reviewed in isolation. Task 3 changes no public API. Task 4 changes a `#[non_exhaustive]` struct in the way its own documentation anticipated. Task 5 is the only task that changes existing behaviour on the inbound path, and is where review effort belongs.

The three `ResponderCountersSnapshot` fields added in Task 5 break the two existing tests in `responder_counters.rs` that build the snapshot as a struct literal. That breakage is expected and those tests are updated in the same task; `#[non_exhaustive]` does not apply within the defining crate.

Issue #494, on remote-error identity consistency, is already satisfied on the producer side: `error_reply` builds the payload from the same validated `request_id` that fills the header. Nothing in this plan changes that, and nothing in this plan closes #494 either, whose remaining work is on the consumer side.
