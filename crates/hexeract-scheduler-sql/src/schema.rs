use hexeract_outbox_sql::Dialect;
use hexeract_scheduler::SchedulerError;

use crate::validate::validate_table_name;

/// Canonical PostgreSQL schema for a scheduled-messages table.
///
/// `{{table}}` is substituted by [`schema_ddl`]. The pending index is partial
/// so the worker's claim scan only walks rows that are still eligible.
const POSTGRES_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}} (
    schedule_id       UUID         PRIMARY KEY,
    event_type        VARCHAR(64)  NOT NULL,
    payload           JSONB        NOT NULL,
    trigger_kind      VARCHAR(16)  NOT NULL,
    cron_expr         TEXT         NULL,
    scheduled_for     TIMESTAMPTZ  NOT NULL,
    target_kind       VARCHAR(16)  NOT NULL,
    target_routing_key TEXT        NULL,
    attempts          INTEGER      NOT NULL DEFAULT 0,
    max_attempts      INTEGER      NOT NULL,
    leased_until      TIMESTAMPTZ  NULL,
    paused            BOOLEAN      NOT NULL DEFAULT FALSE,
    last_error        TEXT         NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    delivered_at      TIMESTAMPTZ  NULL,
    cancelled_at      TIMESTAMPTZ  NULL,
    dead_lettered_at  TIMESTAMPTZ  NULL
);
CREATE INDEX IF NOT EXISTS idx_{{table}}_pending
    ON {{table}} (scheduled_for)
    WHERE delivered_at IS NULL
      AND dead_lettered_at IS NULL
      AND cancelled_at IS NULL
      AND paused = FALSE;
CREATE INDEX IF NOT EXISTS idx_{{table}}_dead_letter
    ON {{table}} (dead_lettered_at)
    WHERE dead_lettered_at IS NOT NULL;
";

/// Canonical MySQL schema for a scheduled-messages table (requires MySQL
/// 8.0.13+).
///
/// MySQL supports neither partial indexes nor `CREATE INDEX IF NOT EXISTS`,
/// so the indexes are declared inline. UUIDs are stored as `BINARY(16)`, the
/// payload as native `JSON`, and timestamps as `DATETIME(6)` holding UTC with
/// an expression default `(UTC_TIMESTAMP(6))` that requires MySQL 8.0.13+.
const MYSQL_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}} (
    schedule_id       BINARY(16)   NOT NULL PRIMARY KEY,
    event_type        VARCHAR(64)  NOT NULL,
    payload           JSON         NOT NULL,
    trigger_kind      VARCHAR(16)  NOT NULL,
    cron_expr         TEXT         NULL,
    scheduled_for     DATETIME(6)  NOT NULL,
    target_kind       VARCHAR(16)  NOT NULL,
    target_routing_key TEXT        NULL,
    attempts          INT          NOT NULL DEFAULT 0,
    max_attempts      INT          NOT NULL,
    leased_until      DATETIME(6)  NULL,
    paused            BOOLEAN      NOT NULL DEFAULT FALSE,
    last_error        TEXT         NULL,
    created_at        DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    delivered_at      DATETIME(6)  NULL,
    cancelled_at      DATETIME(6)  NULL,
    dead_lettered_at  DATETIME(6)  NULL,
    INDEX idx_{{table}}_pending (scheduled_for),
    INDEX idx_{{table}}_dead_letter (dead_lettered_at)
);
";

/// Canonical SQLite schema for a scheduled-messages table.
///
/// SQLite has dynamic typing, so UUIDs are stored as `BLOB`, the payload and
/// timestamps as `TEXT`, and the paused flag as `INTEGER`. Timestamp defaults
/// render as RFC 3339 (`...T...Z`) so they sort lexicographically against the
/// bound timestamps.
const SQLITE_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}} (
    schedule_id       BLOB     NOT NULL PRIMARY KEY,
    event_type        TEXT     NOT NULL,
    payload           TEXT     NOT NULL,
    trigger_kind      TEXT     NOT NULL,
    cron_expr         TEXT     NULL,
    scheduled_for     TEXT     NOT NULL,
    target_kind       TEXT     NOT NULL,
    target_routing_key TEXT    NULL,
    attempts          INTEGER  NOT NULL DEFAULT 0,
    max_attempts      INTEGER  NOT NULL,
    leased_until      TEXT     NULL,
    paused            INTEGER  NOT NULL DEFAULT 0,
    last_error        TEXT     NULL,
    created_at        TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    delivered_at      TEXT     NULL,
    cancelled_at      TEXT     NULL,
    dead_lettered_at  TEXT     NULL
);
CREATE INDEX IF NOT EXISTS idx_{{table}}_pending
    ON {{table}} (scheduled_for)
    WHERE delivered_at IS NULL
      AND dead_lettered_at IS NULL
      AND cancelled_at IS NULL
      AND paused = 0;
CREATE INDEX IF NOT EXISTS idx_{{table}}_dead_letter
    ON {{table}} (dead_lettered_at)
    WHERE dead_lettered_at IS NOT NULL;
";

/// Canonical schema DDL (table plus indexes) for the scheduled-messages table,
/// rendered for `dialect`.
///
/// The DDL is exposed so it can drive migration tooling; the stores do not
/// require an `ensure_schema` step at runtime. The pending index is partial on
/// PostgreSQL and SQLite (excluding delivered, dead-lettered, cancelled and
/// paused rows) and a plain index on MySQL, which lacks partial indexes.
///
/// # Errors
///
/// Returns [`SchedulerError::Internal`] if `table` is not a valid identifier
/// matching `^[a-zA-Z_][a-zA-Z0-9_]*$` or exceeds the maximum identifier
/// length.
pub fn schema_ddl(dialect: Dialect, table: &str) -> Result<String, SchedulerError> {
    validate_table_name(table)?;
    let template = match dialect {
        Dialect::Postgres => POSTGRES_SCHEMA_SQL,
        Dialect::MySql => MYSQL_SCHEMA_SQL,
        Dialect::Sqlite => SQLITE_SCHEMA_SQL,
        _ => {
            return Err(SchedulerError::internal(format!(
                "unsupported SQL dialect: {dialect:?}"
            )));
        }
    };
    Ok(template.replace("{{table}}", table))
}

#[cfg(test)]
mod tests {
    use super::schema_ddl;
    use hexeract_outbox_sql::Dialect;
    use hexeract_scheduler::SchedulerError;

    #[test]
    fn postgres_schema_ddl_declares_the_scheduled_messages_table() {
        let ddl = schema_ddl(Dialect::Postgres, "scheduled_messages").unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS scheduled_messages"));
        assert!(ddl.contains("schedule_id       UUID"));
        assert!(ddl.contains("PRIMARY KEY"));
        assert!(ddl.contains("JSONB"));
        assert!(ddl.contains("scheduled_for"));
        assert!(ddl.contains("cron_expr"));
        assert!(ddl.contains("target_kind"));
        assert!(ddl.contains("leased_until"));
        assert!(ddl.contains("paused"));
        assert!(ddl.contains("dead_lettered_at"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn postgres_pending_index_excludes_terminal_and_paused_rows() {
        let ddl = schema_ddl(Dialect::Postgres, "scheduled_messages").unwrap();
        assert!(ddl.contains("idx_scheduled_messages_pending"));
        assert!(ddl.contains("delivered_at IS NULL"));
        assert!(ddl.contains("dead_lettered_at IS NULL"));
        assert!(ddl.contains("cancelled_at IS NULL"));
        assert!(ddl.contains("paused = FALSE"));
    }

    #[test]
    fn postgres_dead_letter_index_is_partial() {
        let ddl = schema_ddl(Dialect::Postgres, "scheduled_messages").unwrap();
        assert!(ddl.contains("idx_scheduled_messages_dead_letter"));
        assert!(ddl.contains("dead_lettered_at IS NOT NULL"));
    }

    #[test]
    fn mysql_schema_ddl_uses_native_types() {
        let ddl = schema_ddl(Dialect::MySql, "scheduled_messages").unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS scheduled_messages"));
        assert!(ddl.contains("BINARY(16)"));
        assert!(ddl.contains("JSON"));
        assert!(ddl.contains("DATETIME(6)"));
        assert!(ddl.contains("UTC_TIMESTAMP(6)"));
        assert!(ddl.contains("INDEX idx_scheduled_messages_pending"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn sqlite_schema_ddl_uses_portable_text_types() {
        let ddl = schema_ddl(Dialect::Sqlite, "scheduled_messages").unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS scheduled_messages"));
        assert!(ddl.contains("BLOB"));
        assert!(ddl.contains("strftime"));
        assert!(ddl.contains("paused = 0"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn schema_ddl_rejects_an_invalid_table_name() {
        let error = schema_ddl(Dialect::Postgres, "bad name; DROP").unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[test]
    fn schema_ddl_rejects_an_overlength_table_name() {
        let long_name = "a".repeat(64);
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            let error = schema_ddl(dialect, &long_name).unwrap_err();
            assert!(
                matches!(error, SchedulerError::Internal(_)),
                "{dialect:?} must reject a 64-byte table name"
            );
        }
    }

    #[test]
    fn schema_ddl_substitutes_a_custom_table_name() {
        let ddl = schema_ddl(Dialect::Postgres, "reminders").unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS reminders"));
        assert!(ddl.contains("idx_reminders_pending"));
    }
}
