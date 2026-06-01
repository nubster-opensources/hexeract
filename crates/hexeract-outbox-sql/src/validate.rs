use hexeract_outbox::OutboxError;

/// Reject anything that is not a safe SQL identifier.
///
/// The validation enforces a strict subset matching
/// `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent SQL injection through the table
/// name, which is concatenated into the generated DDL and statements.
pub(crate) fn validate_table_name(name: &str) -> Result<(), OutboxError> {
    if name.is_empty() {
        return Err(OutboxError::Internal(
            "table_name must not be empty".to_owned(),
        ));
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
}
