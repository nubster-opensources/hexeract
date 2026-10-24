//! The single place where a settlement fence crosses into the database.
//!
//! The fence travels through [`hexeract_outbox::OutboxStore`] as a `u32`, like
//! `OutboxEnvelope::attempts`, while the `attempts` column is a 32-bit signed
//! `INTEGER`. The conversion is decided here once, so that no backend can
//! saturate a value the column cannot hold and let the fence match the wrong
//! row.

use hexeract_outbox::OutboxError;
use uuid::Uuid;

/// Convert a fence value to the type the `attempts` column binds as.
///
/// # Errors
///
/// Returns [`OutboxError::Internal`] when `value` exceeds [`i32::MAX`]. It is
/// never clamped: a saturated fence would compare equal to an unrelated
/// attempt count.
pub(crate) fn fence_value(value: u32) -> Result<i32, OutboxError> {
    i32::try_from(value).map_err(|_| {
        OutboxError::Internal(format!(
            "attempt number {value} does not fit the INTEGER attempts column"
        ))
    })
}

/// Pair every claim candidate with its bindable fence value.
///
/// # Errors
///
/// Returns [`OutboxError::Internal`] when any polled `attempts` cannot be bound.
pub(crate) fn bindable_claims(claims: &[(Uuid, u32)]) -> Result<Vec<(Uuid, i32)>, OutboxError> {
    claims
        .iter()
        .map(|(event_id, attempts)| Ok((*event_id, fence_value(*attempts)?)))
        .collect()
}

/// Return the obtained identifiers in the order the candidates were offered.
///
/// A database reports the rows a batch statement touched in no particular
/// order; the caller is entitled to a result that follows its own batch.
pub(crate) fn in_claim_order(claims: &[(Uuid, u32)], obtained: &[Uuid]) -> Vec<Uuid> {
    claims
        .iter()
        .map(|(event_id, _)| *event_id)
        .filter(|event_id| obtained.contains(event_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_value_accepts_the_largest_storable_attempt() {
        assert_eq!(fence_value(i32::MAX.unsigned_abs()).unwrap(), i32::MAX);
    }

    #[test]
    fn fence_value_rejects_instead_of_saturating() {
        for value in [i32::MAX.unsigned_abs() + 1, u32::MAX] {
            assert!(matches!(fence_value(value), Err(OutboxError::Internal(_))));
        }
    }

    #[test]
    fn obtained_identifiers_follow_the_order_of_the_batch() {
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let third = Uuid::from_u128(3);
        let claims = [(first, 0), (second, 0), (third, 0)];
        assert_eq!(in_claim_order(&claims, &[third, first]), vec![first, third]);
    }
}
