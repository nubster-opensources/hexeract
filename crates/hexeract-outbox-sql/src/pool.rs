//! The single mapping from a `sqlx` pool failure to an outbox error.
//!
//! Every backend acquires its connection from a pool and must distinguish a
//! pool timeout, which a caller can retry, from any other database failure,
//! which it cannot. The distinction is decided here once rather than per
//! backend, so that the three backends cannot disagree about which failures
//! are retryable.

use hexeract_outbox::OutboxError;

/// Map a `sqlx` error raised while acquiring a pooled connection onto the
/// outbox error that describes it.
///
/// A pool timeout becomes [`OutboxError::PoolTimeout`]; every other failure is
/// wrapped as a database error, because only the timeout tells the caller that
/// retrying later may succeed.
pub(crate) fn pool_error(error: sqlx::Error) -> OutboxError {
    match error {
        sqlx::Error::PoolTimedOut => OutboxError::PoolTimeout,
        other => OutboxError::Database(Box::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_timeout_maps_to_pool_timeout() {
        let error = pool_error(sqlx::Error::PoolTimedOut);
        assert!(matches!(error, OutboxError::PoolTimeout), "got {error:?}");
    }

    #[test]
    fn other_errors_map_to_database() {
        let error = pool_error(sqlx::Error::RowNotFound);
        assert!(matches!(error, OutboxError::Database(_)), "got {error:?}");
    }
}
