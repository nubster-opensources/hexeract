use deadpool_postgres::Pool;
use hexeract_outbox::OutboxError;

/// Canonical PostgreSQL schema for an outbox table.
///
/// The literal `{{table}}` is substituted with the configured table name
/// by [`render_schema`] before being applied. Designed to be copy-pasted
/// into any migration tool (sqlx-cli, refinery, dbmate, Flyway, ...).
pub const POSTGRES_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}} (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      UUID         NOT NULL UNIQUE,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    subject_id    UUID         NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempts      INTEGER      NOT NULL DEFAULT 0,
    last_error    TEXT         NULL,
    next_retry_at TIMESTAMPTZ  NULL,
    delivered_at  TIMESTAMPTZ  NULL
);
CREATE INDEX IF NOT EXISTS idx_{{table}}_pending
    ON {{table}} (created_at)
    WHERE delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_{{table}}_subject
    ON {{table}} (subject_id, id)
    WHERE subject_id IS NOT NULL;
";

/// Render the canonical schema with the given table name substituted.
///
/// # Errors
///
/// Returns [`OutboxError::Internal`] if `table_name` is not a valid
/// PostgreSQL identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
pub fn render_schema(table_name: &str) -> Result<String, OutboxError> {
    validate_table_name(table_name)?;
    Ok(POSTGRES_SCHEMA_SQL.replace("{{table}}", table_name))
}

/// Apply the canonical schema to the target database.
///
/// **Intended for POCs, integration tests and local development.**
/// Production deployments should run their own migration tooling against
/// the SQL exposed by [`POSTGRES_SCHEMA_SQL`] or [`render_schema`].
/// Applying DDL from the running application typically requires elevated
/// privileges that the runtime database role should not own, and clashes
/// with versioned migration workflows.
///
/// # Errors
///
/// - [`OutboxError::Internal`] if `table_name` is invalid.
/// - [`OutboxError::Database`] if the pool, the connection or the DDL
///   statement fails.
pub async fn ensure_schema(pool: &Pool, table_name: &str) -> Result<(), OutboxError> {
    let sql = render_schema(table_name)?;
    let client = pool
        .get()
        .await
        .map_err(|e| OutboxError::Database(Box::new(e)))?;
    client
        .batch_execute(&sql)
        .await
        .map_err(|e| OutboxError::Database(Box::new(e)))?;
    Ok(())
}

/// Reject anything that is not a safe PostgreSQL identifier.
///
/// The validation deliberately enforces a strict subset to prevent SQL
/// injection through the table name, which is concatenated into the
/// generated DDL and the insert statements.
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
    fn render_schema_substitutes_table_name_in_every_occurrence() {
        let sql = render_schema("my_outbox").unwrap();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS my_outbox"));
        assert!(sql.contains("idx_my_outbox_pending"));
        assert!(sql.contains("idx_my_outbox_subject"));
        assert!(!sql.contains("{{table}}"));
    }

    #[test]
    fn render_schema_rejects_invalid_table_name() {
        let err = render_schema("bad name").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn validate_table_name_accepts_safe_identifiers() {
        for ok in ["audit_outbox", "_internal", "outbox_v2", "A", "_"] {
            assert!(validate_table_name(ok).is_ok(), "should accept `{ok}`");
        }
    }

    #[test]
    fn validate_table_name_rejects_injection_attempts() {
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
}
