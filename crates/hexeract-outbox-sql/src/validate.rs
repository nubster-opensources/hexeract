use hexeract_outbox::OutboxError;

/// Maximum byte length of a PostgreSQL identifier before it is silently
/// truncated by the server (NAMEDATALEN - 1).
///
/// Identifiers longer than this are rejected so that index names derived
/// from the table name (e.g. `idx_{table}_pending`) cannot collide with
/// another index after server-side truncation.
pub(crate) const MAX_IDENTIFIER_LEN: usize = 63;

/// Maximum byte length of an `event_type` string that the column definition
/// `VARCHAR(64)` / `TEXT` in the outbox DDL can store without truncation on
/// PostgreSQL and MySQL.
pub(crate) const MAX_EVENT_TYPE_LEN: usize = 64;

/// Reject anything that is not a safe SQL identifier and would overflow the
/// server's `NAMEDATALEN` limit.
///
/// The validation enforces a strict subset matching
/// `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent SQL injection through the table
/// name, which is concatenated into the generated DDL and statements.
/// In addition the name must be at most [`MAX_IDENTIFIER_LEN`] bytes long so
/// that derived names such as `idx_{table}_pending` are not silently
/// truncated and do not collide after truncation.
pub(crate) fn validate_table_name(name: &str) -> Result<(), OutboxError> {
    if name.is_empty() {
        return Err(OutboxError::Internal(
            "table_name must not be empty".to_owned(),
        ));
    }
    if name.len() > MAX_IDENTIFIER_LEN {
        return Err(OutboxError::Internal(format!(
            "table_name `{name}` exceeds the maximum identifier length of {MAX_IDENTIFIER_LEN} bytes"
        )));
    }
    let Some(first) = name.chars().next() else {
        unreachable!("non-empty checked above");
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(OutboxError::Internal(format!(
            "table_name `{name}` must start with [a-zA-Z_]"
        )));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(OutboxError::Internal(format!(
            "table_name `{name}` must match [a-zA-Z_][a-zA-Z0-9_]*"
        )));
    }
    Ok(())
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
    fn accepts_safe_identifiers() {
        for ok in ["audit_outbox", "_internal", "outbox_v2", "A", "_"] {
            assert!(validate_table_name(ok).is_ok(), "should accept `{ok}`");
        }
    }

    #[test]
    fn rejects_injection_attempts() {
        for bad in [
            "",
            "1starts_with_digit",
            "has space",
            "has-dash",
            "has;semicolon",
            "drop_table\"; DROP",
            "a.b",
            "tbl\u{0000}",
        ] {
            assert!(validate_table_name(bad).is_err(), "should reject `{bad}`");
        }
    }

    #[test]
    fn empty_name_is_internal_error() {
        let err = validate_table_name("").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn rejects_name_exceeding_max_identifier_length() {
        // A name of exactly 64 bytes must be rejected; 63 bytes must be accepted.
        let name_63 = "a".repeat(MAX_IDENTIFIER_LEN);
        let name_64 = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert!(
            validate_table_name(&name_63).is_ok(),
            "63-byte name must be accepted"
        );
        let err = validate_table_name(&name_64).unwrap_err();
        assert!(
            matches!(err, OutboxError::Internal(_)),
            "64-byte name must be rejected with Internal error"
        );
    }

    #[test]
    fn rejects_reserved_word_identifiers_without_quoting() {
        // Reserved words are valid ASCII identifiers so the raw name passes
        // the character check. The identifier is accepted by validate_table_name
        // and must later be quoted before use in SQL. This test documents that
        // validate_table_name alone does NOT prevent reserved-word conflicts;
        // callers must also apply quote_identifier when embedding the name.
        for reserved in ["select", "user", "order", "table"] {
            assert!(
                validate_table_name(reserved).is_ok(),
                "reserved word `{reserved}` passes character validation"
            );
        }
    }

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
}
