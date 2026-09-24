//! Per-call observation of one request-reply attempt: the span it carries,
//! the counters it contributes to, and the terminal state a [`Drop`] must
//! not double-count.

use tokio::time::Instant;
use tracing::{Level, Span};

use crate::remote_error::RemoteErrorType;
use crate::request_client_counters::RequestClientCounters;
use crate::request_outcome::{RequestOutcome, TransportCause};

/// Milliseconds elapsed since `started_at`, saturating rather than
/// panicking: a call's observed duration is never meaningfully above
/// `u64::MAX` milliseconds.
#[allow(
    clippy::cast_possible_truncation,
    reason = "a request-reply call running for u64::MAX milliseconds is not a realistic duration"
)]
fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
}

/// Outcomes whose terminal event is emitted at [`Level::WARN`] rather than
/// [`Level::DEBUG`]: see the outcome-level table this crate documents.
fn is_warn_outcome(outcome: RequestOutcome) -> bool {
    matches!(
        outcome,
        RequestOutcome::TimedOut | RequestOutcome::TransportFailed | RequestOutcome::InvalidReply
    )
}

/// Emit this call's terminal event on `span`, at the level its outcome
/// calls for.
///
/// The `tracing` macros require a compile-time level, so the runtime choice
/// between [`Level::WARN`] and [`Level::DEBUG`] is dispatched to one of two
/// monomorphic branches rather than passed as a value. `cause`'s absence
/// simply omits the field, through `tracing`'s own `Option<Value>` support,
/// rather than branching this function a second time.
fn emit_terminal_event(
    span: &Span,
    outcome: RequestOutcome,
    elapsed_ms: u64,
    cause: Option<TransportCause>,
) {
    let is_warn = is_warn_outcome(outcome);
    let outcome = outcome.as_str();
    let cause = cause.map(TransportCause::as_str);
    if is_warn {
        tracing::event!(parent: span, Level::WARN, outcome, elapsed_ms, cause, "rpc request finished");
    } else {
        tracing::event!(parent: span, Level::DEBUG, outcome, elapsed_ms, cause, "rpc request finished");
    }
}

/// One request-reply call's observation.
///
/// Two invariants hold it together. The `in_flight` gauge rises only after
/// [`Self::admit`], and every admitted call brings it back down exactly
/// once, on every exit path, cancellation included. And one call reports
/// exactly one outcome: [`Self::finish`] consumes the guard, so the [`Drop`]
/// below counts `cancelled` only when `finish` never ran.
pub(crate) struct CallObservation<'a> {
    counters: &'a RequestClientCounters,
    span: Span,
    started_at: Instant,
    is_admitted: bool,
    is_finished: bool,
    transport_cause: Option<TransportCause>,
}

impl<'a> CallObservation<'a> {
    /// Open an observation for a call about to attempt registration.
    ///
    /// Counts `started` immediately: every call this client attempts,
    /// admitted or refused alike, reaches this constructor exactly once,
    /// which makes it the one place that can count it unconditionally of
    /// whatever this call's eventual outcome turns out to be.
    pub(crate) fn start(counters: &'a RequestClientCounters, span: Span) -> Self {
        counters.count_started();
        Self {
            counters,
            span,
            started_at: Instant::now(),
            is_admitted: false,
            is_finished: false,
            transport_cause: None,
        }
    }

    /// Mark this call as admitted: its registration succeeded and it is
    /// about to be published.
    ///
    /// Raises the `in_flight` gauge, which only ever happens after a
    /// successful `register`, so the gauge never exceeds `max_in_flight`.
    pub(crate) fn admit(&mut self) {
        self.is_admitted = true;
        self.counters.raise_in_flight();
    }

    /// Record which of the three transport sites this call's
    /// [`crate::RequestError::Transport`] failed at.
    ///
    /// Stashed rather than written to the span immediately: [`Self::finish`]
    /// writes `cause` onto the span in the same pass as `outcome` and
    /// `elapsed_ms`, so the three always land together.
    pub(crate) fn note_transport_cause(&mut self, cause: TransportCause) {
        self.transport_cause = Some(cause);
    }

    /// Record the public category of a [`crate::RequestError::Remote`]
    /// failure.
    ///
    /// Written to the span immediately, unlike [`Self::note_transport_cause`]:
    /// this call is not stashed anywhere else, so there is nothing later to
    /// keep it in step with.
    pub(crate) fn note_remote_error_type(&self, error_type: RemoteErrorType) {
        self.span
            .record("remote_error_type", format!("{error_type:?}").as_str());
    }

    /// Consume this observation with its call's final outcome.
    ///
    /// Counts `outcome`, records the span's `outcome`, `elapsed_ms` and,
    /// when present, `cause` fields, and emits the terminal event at the
    /// level this outcome calls for. Bringing the `in_flight` gauge back
    /// down is left to the [`Drop`] below, which always runs immediately
    /// after this method returns since it consumes `self` by value: that
    /// keeps the gauge release to the single code path shared with the
    /// cancellation exit.
    pub(crate) fn finish(mut self, outcome: RequestOutcome) {
        self.is_finished = true;
        let elapsed_ms = elapsed_ms(self.started_at);
        self.span.record("outcome", outcome.as_str());
        self.span.record("elapsed_ms", elapsed_ms);
        if let Some(cause) = self.transport_cause {
            self.span.record("cause", cause.as_str());
        }
        self.counters.count_outcome(outcome);
        emit_terminal_event(&self.span, outcome, elapsed_ms, self.transport_cause);
    }
}

impl Drop for CallObservation<'_> {
    /// Bring the `in_flight` gauge back down for an admitted call, on every
    /// exit path. Count `cancelled` and record the span's terminal fields
    /// only when [`Self::finish`] never ran: a call that did finish already
    /// recorded its own outcome there, and this must not double-count it.
    fn drop(&mut self) {
        if self.is_admitted {
            self.counters.release_in_flight();
        }
        if !self.is_finished {
            self.counters.count_cancelled();
            let elapsed_ms = elapsed_ms(self.started_at);
            self.span
                .record("outcome", RequestOutcome::Cancelled.as_str());
            self.span.record("elapsed_ms", elapsed_ms);
            emit_terminal_event(
                &self.span,
                RequestOutcome::Cancelled,
                elapsed_ms,
                self.transport_cause,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_registry::ReplyCountersSnapshot;

    /// Every field at rest: no reply has been resolved, orphaned, refused
    /// or otherwise counted by the registry this observation's counters
    /// snapshot alongside.
    fn empty_replies() -> ReplyCountersSnapshot {
        ReplyCountersSnapshot {
            undecodable: 0,
            orphaned: 0,
            unauthenticated: 0,
            invalid: 0,
            duplicate: 0,
            late: 0,
        }
    }

    /// An admitted `CallObservation` dropped without
    /// `finish` must count `cancelled` and bring `in_flight` back to zero.
    #[test]
    fn an_admitted_observation_dropped_without_finish_counts_cancelled_and_frees_in_flight() {
        let counters = RequestClientCounters::default();
        {
            let mut observation = CallObservation::start(&counters, Span::none());
            observation.admit();
            // dropped here without ever calling `finish`
        }

        let snapshot = counters.snapshot(empty_replies());
        assert_eq!(
            snapshot.cancelled, 1,
            "an admitted call dropped without finish must count cancelled exactly once"
        );
        assert_eq!(
            snapshot.in_flight, 0,
            "the in_flight gauge must return to zero once the dropped call's slot is released"
        );
    }

    /// A `CallObservation` never admitted must never
    /// move `in_flight`, whatever its eventual outcome (checked here on
    /// both the drop-without-finish and the finish-as-refused exit paths).
    /// The second assertion, that a non-admitted finish still counts
    /// `started` and `refused`, follows from the same rule as the first: a
    /// registration refusal counts `started` and `refused`, never the
    /// gauge.
    #[test]
    fn a_never_admitted_observation_never_moves_in_flight_whatever_its_outcome() {
        let counters = RequestClientCounters::default();

        {
            let observation = CallObservation::start(&counters, Span::none());
            drop(observation);
        }
        assert_eq!(
            counters.snapshot(empty_replies()).in_flight,
            0,
            "a call dropped before admission must never move the in_flight gauge"
        );

        let observation = CallObservation::start(&counters, Span::none());
        observation.finish(RequestOutcome::Refused);
        let snapshot = counters.snapshot(empty_replies());
        assert_eq!(
            snapshot.in_flight, 0,
            "a call refused before admission must never move the in_flight gauge"
        );
        assert_eq!(
            snapshot.refused, 1,
            "a call finished as refused before admission must still count started and refused"
        );
    }

    /// `finish` must count exactly one outcome, and the
    /// `Drop` that runs immediately after (since `finish` consumes the
    /// guard by value) must not also count `cancelled`.
    #[test]
    fn finish_counts_exactly_one_outcome_and_the_subsequent_drop_does_not_also_count_cancelled() {
        let counters = RequestClientCounters::default();
        let mut observation = CallObservation::start(&counters, Span::none());
        observation.admit();
        observation.finish(RequestOutcome::Succeeded);

        let snapshot = counters.snapshot(empty_replies());
        assert_eq!(
            snapshot.succeeded, 1,
            "finish must count exactly the outcome it was given"
        );
        assert_eq!(
            snapshot.cancelled, 0,
            "the drop that follows finish must not also count cancelled"
        );
    }
}
