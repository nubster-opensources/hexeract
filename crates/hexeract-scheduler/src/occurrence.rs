use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use uuid::Uuid;

/// Namespace UUID for deriving occurrence identifiers.
///
/// A fixed, application-specific namespace keeps [`OccurrenceId::derive`]
/// stable across processes and releases.
const OCCURRENCE_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6b, 0x9d, 0x1c, 0x2e, 0x7a, 0x44, 0x4f, 0x8e, 0x9c, 0x10, 0x2d, 0x3b, 0x4c, 0x5d, 0x6e, 0x7f,
]);

/// Stable identity of a single firing of a schedule.
///
/// Derived deterministically from the schedule identifier and the instant
/// the firing is due, so the same occurrence always maps to the same id.
/// Downstream consumers use it as the deduplication key under the
/// at-least-once delivery contract: a redelivered occurrence carries the
/// same [`OccurrenceId`], letting sinks discard the duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OccurrenceId(Uuid);

impl OccurrenceId {
    /// Derive the stable identifier of the occurrence due at `scheduled_for`
    /// for the schedule `schedule_id`.
    ///
    /// The derivation is a `UUIDv5` over the schedule identifier and the
    /// signed offset of `scheduled_for` from the Unix epoch. It is stable
    /// across processes and never panics, including for instants before the
    /// epoch.
    #[must_use]
    pub fn derive(schedule_id: Uuid, scheduled_for: SystemTime) -> Self {
        let (sign, nanos) = match scheduled_for.duration_since(UNIX_EPOCH) {
            Ok(elapsed) => (1u8, elapsed.as_nanos()),
            Err(before_epoch) => (0u8, before_epoch.duration().as_nanos()),
        };
        let mut name = Vec::with_capacity(size_of::<Uuid>() + 1 + size_of::<u128>());
        name.extend_from_slice(schedule_id.as_bytes());
        name.push(sign);
        name.extend_from_slice(&nanos.to_be_bytes());
        Self(Uuid::new_v5(&OCCURRENCE_NAMESPACE, &name))
    }

    /// Return the underlying `UUID`.
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::OccurrenceId;
    use std::time::{Duration, UNIX_EPOCH};
    use uuid::Uuid;

    #[test]
    fn derive_is_deterministic_for_same_inputs() {
        let id = Uuid::from_u128(1);
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        assert_eq!(OccurrenceId::derive(id, at), OccurrenceId::derive(id, at));
    }

    #[test]
    fn derive_differs_when_scheduled_for_differs() {
        let id = Uuid::from_u128(1);
        let a = UNIX_EPOCH + Duration::from_secs(1_000);
        let b = UNIX_EPOCH + Duration::from_secs(2_000);
        assert_ne!(OccurrenceId::derive(id, a), OccurrenceId::derive(id, b));
    }

    #[test]
    fn derive_differs_when_schedule_id_differs() {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        assert_ne!(
            OccurrenceId::derive(Uuid::from_u128(1), at),
            OccurrenceId::derive(Uuid::from_u128(2), at),
        );
    }

    #[test]
    fn as_uuid_exposes_a_non_nil_identifier() {
        let occurrence =
            OccurrenceId::derive(Uuid::from_u128(7), UNIX_EPOCH + Duration::from_secs(42));
        assert_ne!(occurrence.as_uuid(), Uuid::nil());
    }
}
