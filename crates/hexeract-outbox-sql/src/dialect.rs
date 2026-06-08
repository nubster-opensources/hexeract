use hexeract_outbox::OutboxError;

use crate::validate::validate_table_name;

/// Canonical PostgreSQL schema for an outbox table.
///
/// `{{table}}` is substituted
/// by [`Dialect::schema_ddl`].
const POSTGRES_SCHEMA_SQL: &str = r"
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

/// Canonical MySQL schema for an outbox table (requires MySQL 8.0.13+).
///
/// MySQL supports neither partial indexes nor `CREATE INDEX IF NOT EXISTS`,
/// so the indexes are declared inline in the `CREATE TABLE` statement. UUIDs
/// are stored as `BINARY(16)` and the payload as native `JSON`. Timestamps use
/// `DATETIME(6)` holding UTC, with an expression default `(UTC_TIMESTAMP(6))`
/// that requires MySQL 8.0.13 or later.
const MYSQL_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}} (
    id            BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id      BINARY(16)   NOT NULL UNIQUE,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSON         NOT NULL,
    subject_id    BINARY(16)   NULL,
    created_at    DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    attempts      INT          NOT NULL DEFAULT 0,
    last_error    TEXT         NULL,
    next_retry_at DATETIME(6)  NULL,
    delivered_at  DATETIME(6)  NULL,
    INDEX idx_{{table}}_pending (delivered_at, created_at),
    INDEX idx_{{table}}_subject (subject_id, id)
);
";

/// Canonical PostgreSQL dead-letter schema.
///
/// Rows are moved here when `attempts >= max_attempts`. `exhausted_at`
/// defaults to `NOW()` and records when the envelope was declared poison.
/// `{{table}}` is substituted by [`Dialect::dead_letter_schema_ddl`].
const POSTGRES_DLQ_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}}_dead_letter (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      UUID         NOT NULL UNIQUE,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    subject_id    UUID         NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    attempts      INTEGER      NOT NULL,
    last_error    TEXT         NOT NULL,
    exhausted_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_{{table}}_dead_letter_event_id
    ON {{table}}_dead_letter (event_id);
CREATE INDEX IF NOT EXISTS idx_{{table}}_dead_letter_exhausted_at
    ON {{table}}_dead_letter (exhausted_at);
";

/// Canonical MySQL dead-letter schema (requires MySQL 8.0.13+).
///
/// Mirrors the MySQL outbox schema: UUIDs as `BINARY(16)`, payload as
/// `JSON`, timestamps as `DATETIME(6)` UTC. `{{table}}` is substituted
/// by [`Dialect::dead_letter_schema_ddl`].
const MYSQL_DLQ_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}}_dead_letter (
    id            BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id      BINARY(16)   NOT NULL UNIQUE,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSON         NOT NULL,
    subject_id    BINARY(16)   NULL,
    created_at    DATETIME(6)  NOT NULL,
    attempts      INT          NOT NULL,
    last_error    TEXT         NOT NULL,
    exhausted_at  DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    INDEX idx_{{table}}_dead_letter_event_id (event_id),
    INDEX idx_{{table}}_dead_letter_exhausted_at (exhausted_at)
);
";

/// Canonical SQLite dead-letter schema.
///
/// Mirrors the SQLite outbox schema: UUIDs as `BLOB`, timestamps as
/// `TEXT` in RFC 3339 form. `{{table}}` is substituted by
/// [`Dialect::dead_letter_schema_ddl`].
const SQLITE_DLQ_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}}_dead_letter (
    id            INTEGER  PRIMARY KEY AUTOINCREMENT,
    event_id      BLOB     NOT NULL UNIQUE,
    event_type    TEXT     NOT NULL,
    payload       TEXT     NOT NULL,
    subject_id    BLOB,
    created_at    TEXT     NOT NULL,
    attempts      INTEGER  NOT NULL,
    last_error    TEXT     NOT NULL,
    exhausted_at  TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_{{table}}_dead_letter_event_id
    ON {{table}}_dead_letter (event_id);
CREATE INDEX IF NOT EXISTS idx_{{table}}_dead_letter_exhausted_at
    ON {{table}}_dead_letter (exhausted_at);
";

/// Canonical SQLite schema for an outbox table.
///
/// SQLite has dynamic typing, so UUIDs are stored as `BLOB` and the payload
/// and timestamps as `TEXT`. The `created_at` default is rendered as RFC 3339
/// (`...T...Z`) so it sorts lexicographically against the bound timestamps.
const SQLITE_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS {{table}} (
    id            INTEGER  PRIMARY KEY AUTOINCREMENT,
    event_id      BLOB     NOT NULL UNIQUE,
    event_type    TEXT     NOT NULL,
    payload       TEXT     NOT NULL,
    subject_id    BLOB,
    created_at    TEXT     NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    attempts      INTEGER  NOT NULL DEFAULT 0,
    last_error    TEXT,
    next_retry_at TEXT,
    delivered_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_{{table}}_pending
    ON {{table}} (created_at)
    WHERE delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_{{table}}_subject
    ON {{table}} (subject_id, id)
    WHERE subject_id IS NOT NULL;
";

/// SQL dialect differences absorbed by the backend stores.
///
/// A [`Dialect`] knows how to render the four statements the outbox needs
/// (poll, mark-delivered, mark-failed, insert) and the canonical schema DDL
/// for its engine, accounting for placeholder style, row locking, the
/// "current instant" expression and per-engine column types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// PostgreSQL (`sqlx::Postgres`).
    Postgres,
    /// MySQL 8.0+ (`sqlx::MySql`).
    MySql,
    /// SQLite (`sqlx::Sqlite`).
    Sqlite,
}

impl Dialect {
    /// Whether competing-consumers row skip-locking is available.
    ///
    /// `true` for PostgreSQL and MySQL 8.0+, `false` for SQLite (which
    /// serializes writes through a single writer instead).
    #[must_use]
    pub fn supports_skip_locked(self) -> bool {
        matches!(self, Self::Postgres | Self::MySql)
    }

    /// Render the bind placeholder for the 1-based parameter `index`.
    ///
    /// PostgreSQL uses positional `$1`, `$2`; MySQL and SQLite use `?`.
    pub(crate) fn placeholder(self, index: usize) -> String {
        match self {
            Self::Postgres => format!("${index}"),
            Self::MySql | Self::Sqlite => "?".to_owned(),
        }
    }

    /// SQL expression evaluating to the current instant, in a form
    /// comparable to the stored timestamps.
    pub(crate) fn now_expr(self) -> &'static str {
        match self {
            Self::Postgres => "NOW()",
            // UTC_TIMESTAMP(6) is independent of the server session time zone
            // and matches the DATETIME(6) microsecond precision the MySQL store
            // binds, so the poll predicate never skips a sub-second retry.
            Self::MySql => "UTC_TIMESTAMP(6)",
            Self::Sqlite => "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        }
    }

    /// `SELECT ... WHERE delivered_at IS NULL ... [FOR UPDATE SKIP LOCKED]`.
    pub(crate) fn poll_sql(self, table: &str) -> String {
        let max_attempts = self.placeholder(1);
        let limit = self.placeholder(2);
        let now = self.now_expr();
        let lock = if self.supports_skip_locked() {
            " FOR UPDATE SKIP LOCKED"
        } else {
            ""
        };
        format!(
            "SELECT event_id, event_type, payload, subject_id, created_at, \
                    attempts, last_error, next_retry_at \
             FROM {table} \
             WHERE delivered_at IS NULL \
               AND attempts < {max_attempts} \
               AND (next_retry_at IS NULL OR next_retry_at <= {now}) \
             ORDER BY id \
             LIMIT {limit}{lock}"
        )
    }

    /// `UPDATE {table} SET delivered_at = {now} WHERE event_id = {ph}`.
    pub(crate) fn mark_delivered_sql(self, table: &str) -> String {
        let event_id = self.placeholder(1);
        let now = self.now_expr();
        format!("UPDATE {table} SET delivered_at = {now} WHERE event_id = {event_id}")
    }

    /// `UPDATE {table} SET attempts = attempts + 1, last_error, next_retry_at ...`.
    pub(crate) fn mark_failed_sql(self, table: &str) -> String {
        let last_error = self.placeholder(1);
        let next_retry_at = self.placeholder(2);
        let event_id = self.placeholder(3);
        format!(
            "UPDATE {table} \
             SET attempts = attempts + 1, last_error = {last_error}, next_retry_at = {next_retry_at} \
             WHERE event_id = {event_id}"
        )
    }

    /// `INSERT INTO {table} (event_id, event_type, payload, subject_id) VALUES (...)`.
    pub(crate) fn insert_sql(self, table: &str) -> String {
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);
        let p3 = self.placeholder(3);
        let p4 = self.placeholder(4);
        format!(
            "INSERT INTO {table} (event_id, event_type, payload, subject_id) \
             VALUES ({p1}, {p2}, {p3}, {p4})"
        )
    }

    /// Canonical schema DDL (table + indexes) rendered for this dialect.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn schema_ddl(self, table: &str) -> Result<String, OutboxError> {
        validate_table_name(table)?;
        let template = match self {
            Self::Postgres => POSTGRES_SCHEMA_SQL,
            Self::MySql => MYSQL_SCHEMA_SQL,
            Self::Sqlite => SQLITE_SCHEMA_SQL,
        };
        Ok(template.replace("{{table}}", table))
    }

    /// Dead-letter schema DDL (table + indexes) rendered for this dialect.
    ///
    /// Creates a table named `{table}_dead_letter`. Envelopes are moved here
    /// when they exhaust `max_attempts`.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    pub fn dead_letter_schema_ddl(self, table: &str) -> Result<String, OutboxError> {
        validate_table_name(table)?;
        let template = match self {
            Self::Postgres => POSTGRES_DLQ_SCHEMA_SQL,
            Self::MySql => MYSQL_DLQ_SCHEMA_SQL,
            Self::Sqlite => SQLITE_DLQ_SCHEMA_SQL,
        };
        Ok(template.replace("{{table}}", table))
    }

    /// `INSERT INTO {dlq} (...) SELECT ... FROM {main} WHERE event_id = {p1}`.
    ///
    /// Copies a row from the main outbox table into the dead-letter table.
    /// `exhausted_at` is not listed and gets its `DEFAULT` value (`NOW()` or
    /// equivalent). Called inside the same transaction as `mark_failed`.
    pub(crate) fn insert_dead_letter_sql(self, main: &str, dlq: &str) -> String {
        let event_id = self.placeholder(1);
        format!(
            "INSERT INTO {dlq} \
             (event_id, event_type, payload, subject_id, created_at, attempts, last_error) \
             SELECT event_id, event_type, payload, subject_id, created_at, attempts, last_error \
             FROM {main} \
             WHERE event_id = {event_id}"
        )
    }

    /// `DELETE FROM {table} WHERE event_id = {p1}`.
    ///
    /// Removes the row from the main outbox table after it has been copied to
    /// the dead-letter table. Called in the same transaction as
    /// [`Self::insert_dead_letter_sql`].
    pub(crate) fn delete_from_main_sql(self, table: &str) -> String {
        let event_id = self.placeholder(1);
        format!("DELETE FROM {table} WHERE event_id = {event_id}")
    }

    /// `UPDATE {table} SET next_retry_at = {p1} WHERE event_id IN ({p2..pN+1})`.
    ///
    /// Sets a soft lease on the given envelopes so competing workers skip
    /// them until the lease expires. Parameter 1 is the lease timestamp;
    /// parameters 2 through `n + 1` are the envelope `event_id` values.
    ///
    /// The query is generated dynamically because the `IN` clause length
    /// varies with the actual batch size. Call frequency is low (once per
    /// poll cycle) so the allocation is negligible.
    // Only the competing-consumers postgres and mysql stores issue claims;
    // a sqlite-only build never calls this, so the method is dead there.
    #[cfg_attr(not(any(feature = "postgres", feature = "mysql")), allow(dead_code))]
    pub(crate) fn claim_sql(self, table: &str, n: usize) -> String {
        let lease = self.placeholder(1);
        let placeholders = (2..=n + 1)
            .map(|i| self.placeholder(i))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "UPDATE {table} SET next_retry_at = {lease} \
             WHERE event_id IN ({placeholders})"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_locked_support_matches_engine_capabilities() {
        assert!(Dialect::Postgres.supports_skip_locked());
        assert!(Dialect::MySql.supports_skip_locked());
        assert!(!Dialect::Sqlite.supports_skip_locked());
    }

    #[test]
    fn postgres_uses_positional_placeholders() {
        assert_eq!(Dialect::Postgres.placeholder(1), "$1");
        assert_eq!(Dialect::Postgres.placeholder(4), "$4");
    }

    #[test]
    fn mysql_and_sqlite_use_question_mark_placeholders() {
        assert_eq!(Dialect::MySql.placeholder(1), "?");
        assert_eq!(Dialect::Sqlite.placeholder(3), "?");
    }

    #[test]
    fn postgres_poll_sql_locks_rows_and_binds_positionally() {
        let sql = Dialect::Postgres.poll_sql("audit_outbox");
        assert!(sql.contains("FROM audit_outbox"));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
        assert!(sql.contains("ORDER BY id"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("NOW()"));
    }

    #[test]
    fn mysql_poll_sql_locks_rows_with_question_marks() {
        let sql = Dialect::MySql.poll_sql("audit_outbox");
        assert!(sql.contains("FROM audit_outbox"));
        assert!(sql.contains('?'));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn sqlite_poll_sql_omits_skip_locked() {
        let sql = Dialect::Sqlite.poll_sql("audit_outbox");
        assert!(sql.contains("FROM audit_outbox"));
        assert!(sql.contains('?'));
        assert!(!sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("strftime"));
    }

    #[test]
    fn postgres_mark_delivered_sets_timestamp_by_event_id() {
        let sql = Dialect::Postgres.mark_delivered_sql("audit_outbox");
        assert!(sql.contains("UPDATE audit_outbox"));
        assert!(sql.contains("delivered_at"));
        assert!(sql.contains("$1"));
    }

    #[test]
    fn postgres_mark_failed_increments_attempts_with_three_binds() {
        let sql = Dialect::Postgres.mark_failed_sql("audit_outbox");
        assert!(sql.contains("attempts = attempts + 1"));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
        assert!(sql.contains("$3"));
    }

    #[test]
    fn postgres_insert_sql_binds_four_columns() {
        let sql = Dialect::Postgres.insert_sql("audit_outbox");
        assert!(sql.contains("INSERT INTO audit_outbox"));
        assert!(sql.contains("event_id, event_type, payload, subject_id"));
        assert!(sql.contains("$1, $2, $3, $4"));
    }

    #[test]
    fn sqlite_insert_sql_uses_question_marks() {
        let sql = Dialect::Sqlite.insert_sql("audit_outbox");
        assert!(sql.contains("INSERT INTO audit_outbox"));
        assert!(sql.contains("?, ?, ?, ?"));
    }

    #[test]
    fn postgres_schema_ddl_matches_current_canonical_schema() {
        let ddl = Dialect::Postgres.schema_ddl("audit_outbox").unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS audit_outbox"));
        assert!(ddl.contains("BIGSERIAL"));
        assert!(ddl.contains("UUID"));
        assert!(ddl.contains("JSONB"));
        assert!(ddl.contains("TIMESTAMPTZ"));
        assert!(ddl.contains("idx_audit_outbox_pending"));
        assert!(ddl.contains("idx_audit_outbox_subject"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn mysql_schema_ddl_uses_native_types() {
        let ddl = Dialect::MySql.schema_ddl("audit_outbox").unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS audit_outbox"));
        assert!(ddl.contains("AUTO_INCREMENT"));
        assert!(ddl.contains("BINARY(16)"));
        assert!(ddl.contains("JSON"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn sqlite_schema_ddl_uses_portable_text_types() {
        let ddl = Dialect::Sqlite.schema_ddl("audit_outbox").unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS audit_outbox"));
        assert!(ddl.contains("AUTOINCREMENT"));
        assert!(ddl.contains("BLOB"));
        assert!(ddl.contains("strftime"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn schema_ddl_rejects_invalid_table_name() {
        let err = Dialect::Postgres.schema_ddl("bad name; DROP").unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn mysql_poll_compares_against_microsecond_utc() {
        let sql = Dialect::MySql.poll_sql("audit_outbox");
        assert!(sql.contains("UTC_TIMESTAMP(6)"));
        assert!(!sql.contains("UTC_TIMESTAMP()"));
    }

    #[test]
    fn mysql_mark_delivered_uses_microsecond_utc() {
        let sql = Dialect::MySql.mark_delivered_sql("audit_outbox");
        assert!(sql.contains("delivered_at = UTC_TIMESTAMP(6)"));
    }

    #[test]
    fn mysql_schema_ddl_defaults_created_at_to_utc() {
        let ddl = Dialect::MySql.schema_ddl("audit_outbox").unwrap();
        assert!(ddl.contains("UTC_TIMESTAMP(6)"));
    }

    #[test]
    fn postgres_dead_letter_schema_ddl_substitutes_table_name() {
        let ddl = Dialect::Postgres
            .dead_letter_schema_ddl("audit_outbox")
            .unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS audit_outbox_dead_letter"));
        assert!(ddl.contains("exhausted_at"));
        assert!(ddl.contains("idx_audit_outbox_dead_letter_event_id"));
        assert!(ddl.contains("idx_audit_outbox_dead_letter_exhausted_at"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn mysql_dead_letter_schema_ddl_uses_native_types() {
        let ddl = Dialect::MySql
            .dead_letter_schema_ddl("audit_outbox")
            .unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS audit_outbox_dead_letter"));
        assert!(ddl.contains("BINARY(16)"));
        assert!(ddl.contains("UTC_TIMESTAMP(6)"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn sqlite_dead_letter_schema_ddl_uses_portable_text_types() {
        let ddl = Dialect::Sqlite
            .dead_letter_schema_ddl("audit_outbox")
            .unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS audit_outbox_dead_letter"));
        assert!(ddl.contains("strftime"));
        assert!(!ddl.contains("{{table}}"));
    }

    #[test]
    fn dead_letter_schema_ddl_rejects_invalid_table_name() {
        let err = Dialect::Postgres
            .dead_letter_schema_ddl("bad name; DROP")
            .unwrap_err();
        assert!(matches!(err, OutboxError::Internal(_)));
    }

    #[test]
    fn postgres_insert_dead_letter_sql_selects_from_main() {
        let sql =
            Dialect::Postgres.insert_dead_letter_sql("audit_outbox", "audit_outbox_dead_letter");
        assert!(sql.contains("INSERT INTO audit_outbox_dead_letter"));
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM audit_outbox"));
        assert!(sql.contains("$1"));
        assert!(!sql.contains("exhausted_at"));
    }

    #[test]
    fn sqlite_insert_dead_letter_sql_uses_question_mark() {
        let sql = Dialect::Sqlite.insert_dead_letter_sql("audit_outbox", "audit_outbox_dlq");
        assert!(sql.contains("INSERT INTO audit_outbox_dlq"));
        assert!(sql.contains("FROM audit_outbox"));
        assert!(sql.contains('?'));
    }

    #[test]
    fn postgres_delete_from_main_sql_binds_positionally() {
        let sql = Dialect::Postgres.delete_from_main_sql("audit_outbox");
        assert!(sql.contains("DELETE FROM audit_outbox"));
        assert!(sql.contains("$1"));
    }

    #[test]
    fn sqlite_delete_from_main_sql_uses_question_mark() {
        let sql = Dialect::Sqlite.delete_from_main_sql("audit_outbox");
        assert!(sql.contains("DELETE FROM audit_outbox"));
        assert!(sql.contains('?'));
    }

    #[test]
    fn postgres_claim_sql_uses_positional_placeholders() {
        let sql = Dialect::Postgres.claim_sql("audit_outbox", 3);
        assert!(sql.contains("UPDATE audit_outbox"));
        assert!(sql.contains("SET next_retry_at = $1"));
        assert!(sql.contains("$2, $3, $4"));
        assert!(sql.contains("WHERE event_id IN"));
    }

    #[test]
    fn mysql_claim_sql_uses_question_marks() {
        let sql = Dialect::MySql.claim_sql("audit_outbox", 2);
        assert!(sql.contains("UPDATE audit_outbox"));
        assert!(sql.contains("SET next_retry_at = ?"));
        assert!(sql.contains("WHERE event_id IN (?, ?)"));
    }

    #[test]
    fn sqlite_claim_sql_uses_question_marks() {
        let sql = Dialect::Sqlite.claim_sql("audit_outbox", 1);
        assert!(sql.contains("UPDATE audit_outbox"));
        assert!(sql.contains("SET next_retry_at = ?"));
        assert!(sql.contains("WHERE event_id IN (?)"));
    }
}
