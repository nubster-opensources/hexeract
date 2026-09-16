//! Bounded memory of the identities that recently left the slot table.

use std::collections::HashMap;
use std::collections::VecDeque;

use hexeract_core::RequestId;

/// Why an identity left the slot table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotRetirement {
    /// A valid reply consumed the slot.
    Resolved,
    /// The caller gave up, or the registry drained, before any valid reply.
    Abandoned,
}

/// Bounded memory of the identities that recently left the slot table.
///
/// The window is measured in events, not in time: past `capacity` retirements
/// a straggler is counted `Orphaned` again. A time window would match the
/// word "short" more closely, but would put a clock inside a structure that
/// is otherwise pure, and make its tests depend on it.
///
/// Inert for the Red phase of #453: `record` and `lookup` are wired to the
/// bounded eviction their fields describe in the Building phase.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct RetiredSlots {
    entries: HashMap<RequestId, SlotRetirement>,
    order: VecDeque<RequestId>,
    capacity: usize,
}

impl RetiredSlots {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    pub(crate) fn record(&mut self, _request_id: RequestId, _retirement: SlotRetirement) {
        let _ = self;
    }

    pub(crate) fn lookup(&self, _request_id: RequestId) -> Option<SlotRetirement> {
        let _ = self;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_recorded_identity_is_found_with_its_retirement() {
        let mut retired = RetiredSlots::new(4);
        let request_id = RequestId::new();

        retired.record(request_id, SlotRetirement::Resolved);

        assert_eq!(retired.lookup(request_id), Some(SlotRetirement::Resolved));
    }

    #[test]
    fn an_unrecorded_identity_is_not_found() {
        let retired = RetiredSlots::new(4);
        let request_id = RequestId::new();

        assert_eq!(retired.lookup(request_id), None);
    }

    #[test]
    fn recording_beyond_capacity_evicts_the_oldest_identity() {
        let mut retired = RetiredSlots::new(2);
        let first = RequestId::new();
        let second = RequestId::new();
        let third = RequestId::new();

        retired.record(first, SlotRetirement::Resolved);
        retired.record(second, SlotRetirement::Resolved);
        retired.record(third, SlotRetirement::Resolved);

        assert_eq!(
            retired.lookup(first),
            None,
            "the first identity recorded must be evicted once capacity is exceeded"
        );
        assert_eq!(
            retired.lookup(third),
            Some(SlotRetirement::Resolved),
            "the most recently recorded identity must still be found"
        );
    }

    #[test]
    fn a_zero_capacity_memory_never_remembers() {
        let mut retired = RetiredSlots::new(0);
        let request_id = RequestId::new();

        retired.record(request_id, SlotRetirement::Abandoned);

        assert_eq!(
            retired.lookup(request_id),
            None,
            "a zero-capacity memory must refuse to remember rather than panic"
        );
    }

    #[test]
    fn recording_an_identity_twice_keeps_one_entry() {
        let mut retired = RetiredSlots::new(2);
        let request_id = RequestId::new();
        let other = RequestId::new();

        retired.record(request_id, SlotRetirement::Resolved);
        retired.record(request_id, SlotRetirement::Abandoned);
        retired.record(other, SlotRetirement::Resolved);

        assert_eq!(
            retired.lookup(request_id),
            Some(SlotRetirement::Abandoned),
            "re-recording the same identity must not leak a duplicate eviction slot"
        );
        assert_eq!(retired.lookup(other), Some(SlotRetirement::Resolved));
    }
}
