use hexeract_outbox::OutboxError;

/// Maximum byte length of an `event_type` string that the column definition
/// `VARCHAR(64)` / `TEXT` in the outbox DDL can store without truncation on
/// PostgreSQL and MySQL.
pub(crate) const MAX_EVENT_TYPE_LEN: usize = 64;

/// Reject anything that is not a safe SQL identifier and would overflow the
/// server's `NAMEDATALEN` limit.
///
/// The rule itself lives in [`crate::identifier::validate_table_name`], which
/// is shared with every other crate that generates SQL. This wrapper only
/// adapts the rejection to an [`OutboxError`] that names the rejected table.
pub(crate) fn validate_table_name(name: &str) -> Result<(), OutboxError> {
    crate::identifier::validate_table_name(name)
        .map_err(|rejection| OutboxError::Internal(format!("table name `{name}`: {rejection}")))
}

/// Validate that an `event_type` string fits within the `VARCHAR(64)` column
/// defined by the outbox DDL.
///
/// PostgreSQL and MySQL silently truncate or reject values that exceed the
/// column width; catching this at the call site produces a clearer error.
///
/// # Errors
///
/// Returns [`OutboxError::Internal`] when `event_type` is empty or longer
/// than [`MAX_EVENT_TYPE_LEN`] bytes.
pub(crate) fn validate_event_type(event_type: &str) -> Result<(), OutboxError> {
    if event_type.is_empty() {
        return Err(OutboxError::Internal(
            "event_type must not be empty".to_owned(),
        ));
    }
    if event_type.len() > MAX_EVENT_TYPE_LEN {
        return Err(OutboxError::Internal(format!(
            "event_type `{event_type}` exceeds the maximum length of {MAX_EVENT_TYPE_LEN} bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_event_type_accepts_valid_types() {
        assert!(validate_event_type("users.registered").is_ok());
        assert!(validate_event_type("orders.placed").is_ok());
        // Exactly 64 bytes must be accepted.
        let max = "a".repeat(MAX_EVENT_TYPE_LEN);
        assert!(validate_event_type(&max).is_ok());
    }

    #[test]
    fn validate_event_type_rejects_empty() {
        let err = validate_event_type("").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn validate_event_type_rejects_overlength() {
        let too_long = "a".repeat(MAX_EVENT_TYPE_LEN + 1);
        let err = validate_event_type(&too_long).unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    // Characterisation test. The rejected name is the one thing an operator
    // needs from the message, and a guard whose error does not say which name
    // it refused is a guard that cannot be diagnosed. This must stay true
    // whatever the identifier rule is factored into.
    #[test]
    fn rejection_message_names_the_rejected_table() {
        let err = validate_table_name("bad name; DROP").unwrap_err();
        let OutboxError::Internal(message) = err else {
            panic!("expected an internal error, got {err:?}");
        };
        assert!(
            message.contains("bad name; DROP"),
            "message must name the rejected table, got {message:?}"
        );
    }
}
