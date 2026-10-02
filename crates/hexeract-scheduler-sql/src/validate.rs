use hexeract_scheduler::SchedulerError;

/// Reject anything that is not a safe SQL identifier or would overflow the
/// server's identifier length limit.
///
/// The rule itself lives in [`hexeract_outbox_sql::identifier`], the single
/// source shared with every other crate that generates SQL. This wrapper only
/// adapts the rejection to a [`SchedulerError`] that names the rejected table.
/// The quoting of identifiers in generated statements has a single source of
/// truth in [`hexeract_outbox_sql::Dialect`]; this function only validates the
/// input.
pub(crate) fn validate_table_name(name: &str) -> Result<(), SchedulerError> {
    hexeract_outbox_sql::identifier::validate_table_name(name)
        .map_err(|rejection| SchedulerError::internal(format!("table name `{name}`: {rejection}")))
}

#[cfg(test)]
mod tests {
    use super::validate_table_name;
    use hexeract_outbox_sql::identifier::MAX_IDENTIFIER_LEN;
    use hexeract_scheduler::SchedulerError;

    // Deliberately reduced to three cases: the full truth table is tested once
    // in `hexeract-outbox-sql`'s `identifier` module, and duplicating it here
    // would recreate exactly the divergence the single source removes. This
    // test only proves that this crate goes through the shared guard.
    #[test]
    fn delegates_to_the_shared_identifier_guard() {
        assert!(validate_table_name("scheduled_messages").is_ok());
        assert!(matches!(
            validate_table_name("has;semicolon"),
            Err(SchedulerError::Internal(_))
        ));
        let too_long = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert_eq!(too_long.len(), 64);
        assert!(matches!(
            validate_table_name(&too_long),
            Err(SchedulerError::Internal(_))
        ));
    }

    // Characterisation test. The rejected name is the one thing an operator
    // needs from the message, and a guard whose error does not say which name
    // it refused is a guard that cannot be diagnosed. This must stay true
    // whatever the identifier rule is factored into.
    #[test]
    fn rejection_message_names_the_rejected_table() {
        let error = validate_table_name("bad name; DROP").unwrap_err();
        let SchedulerError::Internal(message) = error else {
            panic!("expected an internal error, got {error:?}");
        };
        assert!(
            message.contains("bad name; DROP"),
            "message must name the rejected table, got {message:?}"
        );
    }
}
