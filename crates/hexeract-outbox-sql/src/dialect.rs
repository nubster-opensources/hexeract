use hexeract_outbox::OutboxError;

use crate::validate::quote_identifier;
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
///
/// Note: `event_id` is declared `UNIQUE`, which already creates an implicit
/// B-tree index. A separate `idx_{{table}}_dead_letter_event_id` index would
/// be a duplicate and is therefore omitted.
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
CREATE INDEX IF NOT EXISTS idx_{{table}}_dead_letter_exhausted_at
    ON {{table}}_dead_letter (exhausted_at);
";

/// Canonical MySQL dead-letter schema (requires MySQL 8.0.13+).
///
/// Mirrors the MySQL outbox schema: UUIDs as `BINARY(16)`, payload as
/// `JSON`, timestamps as `DATETIME(6)` UTC. `{{table}}` is substituted
/// by [`Dialect::dead_letter_schema_ddl`].
///
/// Note: `event_id` is declared `UNIQUE`, which already creates an implicit
/// index. A separate `idx_{{table}}_dead_letter_event_id` index is omitted.
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
    INDEX idx_{{table}}_dead_letter_exhausted_at (exhausted_at)
);
";

/// Canonical SQLite dead-letter schema.
///
/// Mirrors the SQLite outbox schema: UUIDs as `BLOB`, timestamps as
/// `TEXT` in RFC 3339 form. `{{table}}` is substituted by
/// [`Dialect::dead_letter_schema_ddl`].
///
/// Note: `event_id` is declared `UNIQUE`, which already creates an implicit
/// index. A separate `idx_{{table}}_dead_letter_event_id` index is omitted.
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
///
/// Marked `#[non_exhaustive]` so a future SQL backend can be added in a minor
/// version: downstream `match` arms must include a wildcard `_` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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

    /// SQL expression evaluating to the database current instant offset by a
    /// bound interval, in a form comparable to the stored timestamps.
    ///
    /// The offset is taken from the bind parameter at `index` and is always
    /// anchored to the **database** clock (`NOW()` / `UTC_TIMESTAMP(6)` /
    /// `strftime('now')`), never the application clock. This keeps lease and
    /// retry comparisons consistent even when the worker host and the database
    /// host disagree on wall-clock time (#230).
    ///
    /// The bound value's unit differs per engine, so each store binds the
    /// matching scalar: PostgreSQL binds seconds as `f64`, MySQL binds whole
    /// microseconds as `i64`, and SQLite binds a `strftime` modifier string
    /// such as `"+1.500 seconds"`.
    pub(crate) fn now_plus_interval_expr(self, index: usize) -> String {
        let ph = self.placeholder(index);
        match self {
            Self::Postgres => {
                format!("(NOW() + (CAST({ph} AS DOUBLE PRECISION) * INTERVAL '1 second'))")
            }
            Self::MySql => format!("(UTC_TIMESTAMP(6) + INTERVAL {ph} MICROSECOND)"),
            Self::Sqlite => format!("strftime('%Y-%m-%dT%H:%M:%fZ', 'now', {ph})"),
        }
    }

    /// `SELECT ... WHERE delivered_at IS NULL ... [FOR UPDATE SKIP LOCKED]`.
    pub(crate) fn poll_sql(self, table: &str) -> String {
        let qtable = quote_identifier(table);
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
             FROM {qtable} \
             WHERE delivered_at IS NULL \
               AND attempts < {max_attempts} \
               AND (next_retry_at IS NULL OR next_retry_at <= {now}) \
             ORDER BY id \
             LIMIT {limit}{lock}"
        )
    }

    /// `UPDATE {qtable} SET delivered_at = {now} WHERE event_id = {ph}`.
    pub(crate) fn mark_delivered_sql(self, table: &str) -> String {
        let qtable = quote_identifier(table);
        let event_id = self.placeholder(1);
        let now = self.now_expr();
        format!("UPDATE {qtable} SET delivered_at = {now} WHERE event_id = {event_id}")
    }

    /// `UPDATE {qtable} SET last_error, next_retry_at = {now + interval} ...`.
    ///
    /// `next_retry_at` is derived from the **database** clock plus the bound
    /// backoff interval (parameter 2), not from an application-supplied
    /// timestamp, so retry scheduling is immune to app/DB clock skew (#230).
    ///
    /// The attempt counter is **not** incremented here: it is consumed once
    /// per dispatch attempt by [`Self::claim_sql`] at claim time, so that a
    /// worker that crashes between claim and this call still burns one retry
    /// slot. Incrementing again here would double-count every clean failure.
    pub(crate) fn mark_failed_sql(self, table: &str) -> String {
        let qtable = quote_identifier(table);
        let last_error = self.placeholder(1);
        let next_retry_at = self.now_plus_interval_expr(2);
        let event_id = self.placeholder(3);
        format!(
            "UPDATE {qtable} \
             SET last_error = {last_error}, next_retry_at = {next_retry_at} \
             WHERE event_id = {event_id}"
        )
    }

    /// `INSERT INTO {qtable} (event_id, event_type, payload, subject_id) VALUES (...)`.
    pub(crate) fn insert_sql(self, table: &str) -> String {
        let qtable = quote_identifier(table);
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);
        let p3 = self.placeholder(3);
        let p4 = self.placeholder(4);
        format!(
            "INSERT INTO {qtable} (event_id, event_type, payload, subject_id) \
             VALUES ({p1}, {p2}, {p3}, {p4})"
        )
    }

    /// Canonical schema DDL (table + indexes) rendered for this dialect.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Internal`] if `table` is not a valid
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$` or exceeds
    /// [`crate::validate::MAX_IDENTIFIER_LEN`] bytes.
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
    /// identifier matching `^[a-zA-Z_][a-zA-Z0-9_]*$` or exceeds
    /// [`crate::validate::MAX_IDENTIFIER_LEN`] bytes.
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
        let qmain = quote_identifier(main);
        let qdlq = quote_identifier(dlq);
        let event_id = self.placeholder(1);
        format!(
            "INSERT INTO {qdlq} \
             (event_id, event_type, payload, subject_id, created_at, attempts, last_error) \
             SELECT event_id, event_type, payload, subject_id, created_at, attempts, last_error \
             FROM {qmain} \
             WHERE event_id = {event_id}"
        )
    }

    /// `DELETE FROM {qtable} WHERE event_id = {p1}`.
    ///
    /// Removes the row from the main outbox table after it has been copied to
    /// the dead-letter table. Called in the same transaction as
    /// [`Self::insert_dead_letter_sql`].
    pub(crate) fn delete_from_main_sql(self, table: &str) -> String {
        let qtable = quote_identifier(table);
        let event_id = self.placeholder(1);
        format!("DELETE FROM {qtable} WHERE event_id = {event_id}")
    }

    /// Claim SQL for Postgres: uses `= ANY($2)` with a single UUID-array bind
    /// so the number of bind parameters is fixed regardless of batch size,
    /// avoiding the 65,535 bind-parameter limit inherent to an `IN`-list.
    ///
    /// For MySQL and SQLite a per-row `IN`-list is still generated because
    /// neither supports the `= ANY($n)` array-bind syntax.
    ///
    /// Sets a soft lease on the given envelopes so competing workers skip
    /// them until the lease expires, and consumes one retry slot by
    /// incrementing `attempts`. The lease expiry is computed from the
    /// **database** clock plus the bound lease interval (parameter 1), not an
    /// application timestamp, so the lease window is immune to app/DB clock
    /// skew (#230).
    ///
    /// Incrementing `attempts` at claim time (rather than only on failure in
    /// [`Self::mark_failed_sql`]) is what makes a worker crash between claim
    /// and acknowledgement safe: the attempt is already counted, so the
    /// envelope cannot be redelivered forever without ever reaching the
    /// dead-letter threshold.
    // Every store issues a claim now (postgres/mysql for the competing-consumer
    // lease, sqlite to increment attempts); only a feature-less build leaves it
    // unused.
    #[cfg_attr(
        not(any(feature = "postgres", feature = "mysql", feature = "sqlite")),
        allow(dead_code)
    )]
    pub(crate) fn claim_sql(self, table: &str, n: usize) -> String {
        let qtable = quote_identifier(table);
        let lease = self.now_plus_interval_expr(1);
        match self {
            Self::Postgres => {
                // $2 is bound as a UUID array, so the bind count is always 2
                // regardless of batch size, sidestepping the 65,535-parameter limit.
                format!(
                    "UPDATE {qtable} SET next_retry_at = {lease}, attempts = attempts + 1 \
                     WHERE event_id = ANY($2)"
                )
            }
            Self::MySql | Self::Sqlite => {
                let placeholders = (2..=n + 1)
                    .map(|i| self.placeholder(i))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "UPDATE {qtable} SET next_retry_at = {lease}, attempts = attempts + 1 \
                     WHERE event_id IN ({placeholders})"
                )
            }
        }
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
        assert!(sql.contains("FROM \"audit_outbox\""));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
        assert!(sql.contains("ORDER BY id"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("NOW()"));
    }

    #[test]
    fn poll_sql_quotes_reserved_word_table_name() {
        // A table named "user" is a reserved word in SQL; quoting prevents
        // a runtime syntax error when the table name is embedded in statements.
        let sql = Dialect::Postgres.poll_sql("user");
        assert!(sql.contains("FROM \"user\""));
    }

    #[test]
    fn mysql_poll_sql_locks_rows_with_question_marks() {
        let sql = Dialect::MySql.poll_sql("audit_outbox");
        assert!(sql.contains("FROM \"audit_outbox\""));
        assert!(sql.contains('?'));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn sqlite_poll_sql_omits_skip_locked() {
        let sql = Dialect::Sqlite.poll_sql("audit_outbox");
        assert!(sql.contains("FROM \"audit_outbox\""));
        assert!(sql.contains('?'));
        assert!(!sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("strftime"));
    }

    #[test]
    fn postgres_mark_delivered_sets_timestamp_by_event_id() {
        let sql = Dialect::Postgres.mark_delivered_sql("audit_outbox");
        assert!(sql.contains("UPDATE \"audit_outbox\""));
        assert!(sql.contains("delivered_at"));
        assert!(sql.contains("$1"));
    }

    #[test]
    fn postgres_mark_failed_does_not_increment_attempts_with_three_binds() {
        let sql = Dialect::Postgres.mark_failed_sql("audit_outbox");
        // The increment moved to claim_sql so a crash between claim and
        // mark_failed still consumes a retry slot; mark_failed must not
        // double-count.
        assert!(!sql.contains("attempts = attempts + 1"));
        assert!(sql.contains("last_error = $1"));
        // next_retry_at is computed from the DB clock plus the bound interval
        // ($2), never an app timestamp (#230).
        assert!(sql.contains("next_retry_at = (NOW() +"));
        assert!(sql.contains("$2"));
        assert!(sql.contains("WHERE event_id = $3"));
    }

    #[test]
    fn postgres_insert_sql_binds_four_columns() {
        let sql = Dialect::Postgres.insert_sql("audit_outbox");
        assert!(sql.contains("INSERT INTO \"audit_outbox\""));
        assert!(sql.contains("event_id, event_type, payload, subject_id"));
        assert!(sql.contains("$1, $2, $3, $4"));
    }

    #[test]
    fn sqlite_insert_sql_uses_question_marks() {
        let sql = Dialect::Sqlite.insert_sql("audit_outbox");
        assert!(sql.contains("INSERT INTO \"audit_outbox\""));
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
        // The redundant event_id index is dropped: event_id is already UNIQUE,
        // which creates an implicit index. Only the exhausted_at index remains.
        assert!(!ddl.contains("idx_audit_outbox_dead_letter_event_id"));
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
        assert!(sql.contains("INSERT INTO \"audit_outbox_dead_letter\""));
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM \"audit_outbox\""));
        assert!(sql.contains("$1"));
        assert!(!sql.contains("exhausted_at"));
    }

    #[test]
    fn sqlite_insert_dead_letter_sql_uses_question_mark() {
        let sql = Dialect::Sqlite.insert_dead_letter_sql("audit_outbox", "audit_outbox_dlq");
        assert!(sql.contains("INSERT INTO \"audit_outbox_dlq\""));
        assert!(sql.contains("FROM \"audit_outbox\""));
        assert!(sql.contains('?'));
    }

    #[test]
    fn postgres_delete_from_main_sql_binds_positionally() {
        let sql = Dialect::Postgres.delete_from_main_sql("audit_outbox");
        assert!(sql.contains("DELETE FROM \"audit_outbox\""));
        assert!(sql.contains("$1"));
    }

    #[test]
    fn sqlite_delete_from_main_sql_uses_question_mark() {
        let sql = Dialect::Sqlite.delete_from_main_sql("audit_outbox");
        assert!(sql.contains("DELETE FROM \"audit_outbox\""));
        assert!(sql.contains('?'));
    }

    #[test]
    fn postgres_claim_sql_uses_any_array_instead_of_in_list() {
        // ANY($2) avoids the 65,535 bind-parameter limit that an IN-list of
        // UUIDs would hit at large batch sizes (#240).
        let sql = Dialect::Postgres.claim_sql("audit_outbox", 3);
        assert!(sql.contains("UPDATE \"audit_outbox\""));
        // Lease anchored to the DB clock plus the bound interval ($1), #230.
        assert!(sql.contains("SET next_retry_at = (NOW() +"));
        assert!(sql.contains("$1"));
        // Single array bind; no per-row placeholders ($2, $3, $4).
        assert!(sql.contains("WHERE event_id = ANY($2)"));
        assert!(!sql.contains("$3"));
        assert!(!sql.contains("$4"));
        assert!(!sql.contains("WHERE event_id IN"));
    }

    #[test]
    fn postgres_claim_sql_any_bind_count_is_independent_of_batch_size() {
        // Regardless of n the Postgres claim SQL has exactly two bind
        // parameters: $1 for the lease interval and $2 for the UUID array.
        for n in [1, 10, 1000] {
            let sql = Dialect::Postgres.claim_sql("audit_outbox", n);
            assert!(
                sql.contains("ANY($2)"),
                "n={n}: expected ANY($2), got: {sql}"
            );
            assert!(
                !sql.contains("$3"),
                "n={n}: unexpected $3 placeholder, got: {sql}"
            );
        }
    }

    #[test]
    fn mysql_claim_sql_uses_question_marks() {
        let sql = Dialect::MySql.claim_sql("audit_outbox", 2);
        assert!(sql.contains("UPDATE \"audit_outbox\""));
        assert!(sql.contains("SET next_retry_at = (UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND)"));
        assert!(sql.contains("WHERE event_id IN (?, ?)"));
    }

    #[test]
    fn sqlite_claim_sql_uses_question_marks() {
        let sql = Dialect::Sqlite.claim_sql("audit_outbox", 1);
        assert!(sql.contains("UPDATE \"audit_outbox\""));
        assert!(sql.contains("SET next_retry_at = strftime("));
        assert!(sql.contains("'now', ?)"));
        assert!(sql.contains("WHERE event_id IN (?)"));
    }

    #[test]
    fn now_plus_interval_uses_db_clock_per_dialect() {
        // Regression guard for #230: the lease/retry anchor is the database
        // clock, never an application timestamp bound by the worker.
        assert!(
            Dialect::Postgres
                .now_plus_interval_expr(1)
                .contains("NOW()")
        );
        assert!(
            Dialect::MySql
                .now_plus_interval_expr(1)
                .contains("UTC_TIMESTAMP(6)")
        );
        assert!(Dialect::Sqlite.now_plus_interval_expr(1).contains("'now'"));
    }

    #[test]
    fn claim_sql_increments_attempts_for_every_dialect() {
        // Regression guard for #213: claiming an envelope must consume a
        // retry slot so a crash between claim and mark_failed cannot
        // redeliver a poison row forever.
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            let sql = dialect.claim_sql("audit_outbox", 2);
            assert!(
                sql.contains("attempts = attempts + 1"),
                "{dialect:?} claim_sql must increment attempts, got: {sql}"
            );
        }
    }

    #[test]
    fn schema_ddl_rejects_overlength_table_name() {
        // A name of 64 bytes must exceed MAX_IDENTIFIER_LEN (63) and be rejected
        // so that derived index names cannot collide after server-side truncation.
        let long_name = "a".repeat(64);
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            let err = dialect.schema_ddl(&long_name).unwrap_err();
            assert!(
                matches!(err, OutboxError::Internal(_)),
                "{dialect:?} must reject a 64-byte table name"
            );
        }
    }

    #[test]
    fn dlq_schema_ddl_does_not_create_redundant_event_id_index() {
        // event_id is UNIQUE in every DLQ DDL, which creates an implicit index.
        // A separate named index would be write overhead for no read benefit.
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            let ddl = dialect.dead_letter_schema_ddl("audit_outbox").unwrap();
            assert!(
                !ddl.contains("dead_letter_event_id"),
                "{dialect:?} DLQ DDL must not create a redundant event_id index, got:\n{ddl}"
            );
        }
    }

    #[test]
    fn sql_generation_quotes_identifiers() {
        // All DML helpers must embed double-quoted identifiers so that reserved
        // words (e.g. "user", "order") are safe without runtime errors.
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            let table = "user";
            assert!(
                dialect.poll_sql(table).contains("\"user\""),
                "{dialect:?} poll_sql must quote the table name"
            );
            assert!(
                dialect.mark_delivered_sql(table).contains("\"user\""),
                "{dialect:?} mark_delivered_sql must quote the table name"
            );
            assert!(
                dialect.mark_failed_sql(table).contains("\"user\""),
                "{dialect:?} mark_failed_sql must quote the table name"
            );
            assert!(
                dialect.insert_sql(table).contains("\"user\""),
                "{dialect:?} insert_sql must quote the table name"
            );
            assert!(
                dialect.claim_sql(table, 1).contains("\"user\""),
                "{dialect:?} claim_sql must quote the table name"
            );
            assert!(
                dialect.delete_from_main_sql(table).contains("\"user\""),
                "{dialect:?} delete_from_main_sql must quote the table name"
            );
        }
    }
}
