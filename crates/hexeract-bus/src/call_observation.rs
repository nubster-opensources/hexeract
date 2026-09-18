//! Per-call observation of one request-reply attempt: the span it carries,
//! the counters it contributes to, and the terminal state a [`Drop`] must
//! not double-count.

use tokio::time::Instant;
use tracing::Span;

use crate::remote_error::RemoteErrorType;
use crate::request_client_counters::RequestClientCounters;
use crate::request_outcome::{RequestOutcome, TransportCause};

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
    /// Counts nothing on its own: [`Self::admit`] and [`Self::finish`] are
    /// what move the counters, once the implementation wires them.
    pub(crate) fn start(counters: &'a RequestClientCounters, span: Span) -> Self {
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
    /// Inert in this revision: raising the `in_flight` gauge, which only
    /// ever happens after a successful `register`, lands with the
    /// implementation.
    pub(crate) fn admit(&mut self) {}

    /// Record which of the three transport sites this call's
    /// [`crate::RequestError::Transport`] failed at.
    ///
    /// Inert in this revision: recording `cause` on the span lands with the
    /// implementation.
    pub(crate) fn note_transport_cause(&mut self, cause: TransportCause) {
        let _ = cause;
    }

    /// Record the public category of a [`crate::RequestError::Remote`]
    /// failure.
    ///
    /// Inert in this revision: recording `remote_error_type` on the span
    /// lands with the implementation.
    pub(crate) fn note_remote_error_type(&self, error_type: RemoteErrorType) {
        let _ = error_type;
    }

    /// Consume this observation with its call's final outcome.
    ///
    /// Inert in this revision: counting `outcome`, recording the span's
    /// `elapsed_ms` and `outcome` fields, and emitting the terminal event
    /// at the level this outcome calls for all land with the
    /// implementation.
    pub(crate) fn finish(self, outcome: RequestOutcome) {
        let _ = outcome;
    }
}

impl Drop for CallObservation<'_> {
    /// Count `cancelled` for a call whose future was dropped before
    /// [`Self::finish`] ran, and bring the `in_flight` gauge back down.
    ///
    /// Inert in this revision: both effects land with the implementation.
    fn drop(&mut self) {}
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
