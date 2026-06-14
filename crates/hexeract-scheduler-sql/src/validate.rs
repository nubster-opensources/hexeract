use hexeract_scheduler::SchedulerError;

/// Maximum byte length of a PostgreSQL identifier before the server silently
/// truncates it (NAMEDATALEN - 1).
///
/// Identifiers longer than this are rejected so index names derived from the
/// table name (such as `idx_{table}_pending`) cannot collide after
/// server-side truncation.
pub(crate) const MAX_IDENTIFIER_LEN: usize = 63;

/// Reject anything that is not a safe SQL identifier or would overflow the
/// server's identifier length limit.
///
/// The table name is concatenated into the generated DDL, so it is
/// constrained to the strict subset `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent SQL
/// injection, and to at most [`MAX_IDENTIFIER_LEN`] bytes so derived index
/// names are not silently truncated.
///
/// This mirrors the identifier rule of `hexeract-outbox-sql`. The quoting of
/// identifiers in generated statements has a single source of truth in
/// [`hexeract_outbox_sql::Dialect`]; this function only validates the input.
pub(crate) fn validate_table_name(name: &str) -> Result<(), SchedulerError> {
    if name.is_empty() {
        return Err(SchedulerError::internal("table name must not be empty"));
    }
    if name.len() > MAX_IDENTIFIER_LEN {
        return Err(SchedulerError::internal(format!(
            "table name `{name}` exceeds the maximum identifier length of {MAX_IDENTIFIER_LEN} bytes"
        )));
    }
    let Some(first) = name.chars().next() else {
        unreachable!("non-empty checked above");
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(SchedulerError::internal(format!(
            "table name `{name}` must start with [a-zA-Z_]"
        )));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(SchedulerError::internal(format!(
            "table name `{name}` must match [a-zA-Z_][a-zA-Z0-9_]*"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_IDENTIFIER_LEN, validate_table_name};
    use hexeract_scheduler::SchedulerError;

    #[test]
    fn accepts_safe_identifiers() {
        for ok in ["scheduled_messages", "_internal", "schedules_v2", "A", "_"] {
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
        ] {
            assert!(validate_table_name(bad).is_err(), "should reject `{bad}`");
        }
    }

    #[test]
    fn rejects_name_exceeding_max_identifier_length() {
        let name_63 = "a".repeat(MAX_IDENTIFIER_LEN);
        let name_64 = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert!(validate_table_name(&name_63).is_ok());
        let error = validate_table_name(&name_64).unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[test]
    fn accepts_reserved_words_left_to_quoting() {
        for reserved in ["select", "user", "order", "table"] {
            assert!(validate_table_name(reserved).is_ok());
        }
    }
}
