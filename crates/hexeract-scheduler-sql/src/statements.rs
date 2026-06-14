//! Dialect-aware SQL statement generation for the scheduler store.
//!
//! The injection-safe identifier quoting and the database-clock instant
//! expressions are reused from [`hexeract_outbox_sql::Dialect`] so the
//! scheduler shares a single source of truth for those security- and
//! correctness-sensitive rules. The statements themselves are the
//! scheduler's own, because the `scheduled_messages` schema differs from the
//! outbox table.

use hexeract_outbox_sql::Dialect;

/// Columns returned for a claimed occurrence, in a fixed order.
///
/// The store maps a row in this column order onto a
/// [`hexeract_scheduler::ScheduledMessage`] plus its runtime lease state.
pub(crate) const CLAIM_COLUMNS: &str = "schedule_id, event_type, payload, trigger_kind, cron_expr, \
     scheduled_for, target_kind, target_routing_key, attempts, max_attempts, leased_until";

/// The SQL literal for boolean false in this dialect.
///
/// PostgreSQL and MySQL accept `FALSE`; SQLite stores booleans as integers.
fn false_literal(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Sqlite => "0",
        _ => "FALSE",
    }
}

/// The eligibility predicate shared by every claim variant (without a leading
/// `WHERE`).
///
/// An occurrence is eligible when it is due against the database clock, not
/// terminal (delivered, cancelled or dead-lettered), not paused, still within
/// its attempt budget, and free of an active lease.
fn eligible_predicate(dialect: Dialect) -> String {
    let now = dialect.now_expr();
    let not_paused = false_literal(dialect);
    format!(
        "scheduled_for <= {now} \
           AND delivered_at IS NULL \
           AND cancelled_at IS NULL \
           AND dead_lettered_at IS NULL \
           AND paused = {not_paused} \
           AND attempts < max_attempts \
           AND (leased_until IS NULL OR leased_until <= {now})"
    )
}

/// `INSERT` for a new schedule. Attempts, paused and timestamps use their
/// column defaults; nine columns are bound.
pub(crate) fn insert_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let placeholders = (1..=9)
        .map(|i| dialect.placeholder(i))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {qtable} \
         (schedule_id, event_type, payload, trigger_kind, cron_expr, scheduled_for, \
          target_kind, target_routing_key, max_attempts) \
         VALUES ({placeholders})"
    )
}

/// Combined claim for PostgreSQL and SQLite, which support
/// `UPDATE ... RETURNING`.
///
/// PostgreSQL locks the due rows with `FOR UPDATE SKIP LOCKED` inside a CTE so
/// competing workers do not contend; SQLite serializes writes through a single
/// writer and omits the lock. Both stamp the lease from the database clock
/// (parameter 1) and consume one attempt, then return the claimed rows.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn claim_returning_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let lease = dialect.now_plus_interval_expr(1);
    let limit = dialect.placeholder(2);
    let eligible = eligible_predicate(dialect);
    match dialect {
        Dialect::Postgres => format!(
            "WITH due AS ( \
                 SELECT schedule_id FROM {qtable} \
                 WHERE {eligible} \
                 ORDER BY scheduled_for \
                 LIMIT {limit} \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE {qtable} SET leased_until = {lease}, attempts = attempts + 1 \
             FROM due WHERE {qtable}.schedule_id = due.schedule_id \
             RETURNING {CLAIM_COLUMNS}"
        ),
        _ => format!(
            "UPDATE {qtable} SET leased_until = {lease}, attempts = attempts + 1 \
             WHERE schedule_id IN ( \
                 SELECT schedule_id FROM {qtable} \
                 WHERE {eligible} \
                 ORDER BY scheduled_for \
                 LIMIT {limit} \
             ) \
             RETURNING {CLAIM_COLUMNS}"
        ),
    }
}

/// MySQL step 1: select and lock the due rows.
///
/// MySQL has no `UPDATE ... RETURNING`, so the store claims in a transaction:
/// this statement selects and locks the eligible rows with
/// `FOR UPDATE SKIP LOCKED`, then [`mysql_claim_update_sql`] leases them and
/// [`mysql_claim_reselect_sql`] reads the leased rows back.
#[cfg(feature = "mysql")]
pub(crate) fn mysql_claim_select_sql(table: &str) -> String {
    let qtable = Dialect::MySql.quote_identifier(table);
    let limit = Dialect::MySql.placeholder(1);
    let eligible = eligible_predicate(Dialect::MySql);
    format!(
        "SELECT {CLAIM_COLUMNS} FROM {qtable} \
         WHERE {eligible} \
         ORDER BY scheduled_for \
         LIMIT {limit} \
         FOR UPDATE SKIP LOCKED"
    )
}

/// MySQL step 2: lease the selected rows and consume one attempt.
///
/// `n` is the number of schedule ids selected in step 1 and must be bounded by
/// the caller (the claim batch size) so the `IN` list stays well under the
/// bind-parameter limit. Parameter 1 is the lease interval; parameters 2..=n+1
/// are the schedule ids.
#[cfg(feature = "mysql")]
pub(crate) fn mysql_claim_update_sql(table: &str, n: usize) -> String {
    let qtable = Dialect::MySql.quote_identifier(table);
    let lease = Dialect::MySql.now_plus_interval_expr(1);
    let ids = (2..=n + 1)
        .map(|i| Dialect::MySql.placeholder(i))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "UPDATE {qtable} SET leased_until = {lease}, attempts = attempts + 1 \
         WHERE schedule_id IN ({ids})"
    )
}

/// MySQL step 3: read the leased rows back, with their incremented attempt
/// counter and stamped lease.
#[cfg(feature = "mysql")]
pub(crate) fn mysql_claim_reselect_sql(table: &str, n: usize) -> String {
    let qtable = Dialect::MySql.quote_identifier(table);
    let ids = (1..=n)
        .map(|i| Dialect::MySql.placeholder(i))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {CLAIM_COLUMNS} FROM {qtable} WHERE schedule_id IN ({ids})")
}

/// Mark a one-shot schedule delivered and release its lease, only if it is not
/// already terminal or cancelled.
pub(crate) fn mark_delivered_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let now = dialect.now_expr();
    let id = dialect.placeholder(1);
    format!(
        "UPDATE {qtable} SET delivered_at = {now}, leased_until = NULL \
         WHERE schedule_id = {id} \
           AND delivered_at IS NULL AND cancelled_at IS NULL AND dead_lettered_at IS NULL"
    )
}

/// Advance a recurring schedule to its next occurrence (parameter 1), reset
/// the attempt counter and release the lease, only if it is not terminal or
/// cancelled.
pub(crate) fn reschedule_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let next = dialect.placeholder(1);
    let id = dialect.placeholder(2);
    format!(
        "UPDATE {qtable} \
         SET scheduled_for = {next}, attempts = 0, leased_until = NULL, last_error = NULL \
         WHERE schedule_id = {id} \
           AND delivered_at IS NULL AND cancelled_at IS NULL AND dead_lettered_at IS NULL"
    )
}

/// Move a schedule to the dead-letter state with the last error (parameter 1),
/// only if it is not already terminal or cancelled.
pub(crate) fn mark_dead_lettered_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let now = dialect.now_expr();
    let error = dialect.placeholder(1);
    let id = dialect.placeholder(2);
    format!(
        "UPDATE {qtable} SET dead_lettered_at = {now}, last_error = {error}, leased_until = NULL \
         WHERE schedule_id = {id} \
           AND delivered_at IS NULL AND cancelled_at IS NULL AND dead_lettered_at IS NULL"
    )
}

/// Cancel a schedule. The store treats a zero row count as "not found".
pub(crate) fn cancel_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let now = dialect.now_expr();
    let id = dialect.placeholder(1);
    format!(
        "UPDATE {qtable} SET cancelled_at = {now}, leased_until = NULL WHERE schedule_id = {id}"
    )
}

/// Pause or resume a schedule (parameter 1 is the paused flag). The store
/// treats a zero row count as "not found".
pub(crate) fn set_paused_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let paused = dialect.placeholder(1);
    let id = dialect.placeholder(2);
    format!("UPDATE {qtable} SET paused = {paused} WHERE schedule_id = {id}")
}

/// Read the columns needed to build a schedule snapshot.
pub(crate) fn inspect_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let id = dialect.placeholder(1);
    format!(
        "SELECT schedule_id, trigger_kind, cron_expr, scheduled_for, attempts, max_attempts, \
                paused, last_error, delivered_at, cancelled_at, dead_lettered_at \
         FROM {qtable} WHERE schedule_id = {id}"
    )
}

/// Probe a schedule's existence, returning a single constant column when the
/// row is present.
///
/// Used to disambiguate a zero-row acknowledgement: MySQL reports affected
/// (changed) rows rather than matched rows, so a `set_paused` to an unchanged
/// value updates nothing even though the schedule exists. The store falls back
/// to this probe before deciding the schedule is absent.
pub(crate) fn exists_sql(dialect: Dialect, table: &str) -> String {
    let qtable = dialect.quote_identifier(table);
    let id = dialect.placeholder(1);
    format!("SELECT 1 FROM {qtable} WHERE schedule_id = {id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_claim_locks_in_a_cte_and_returns_rows() {
        let sql = claim_returning_sql(Dialect::Postgres, "scheduled_messages");
        assert!(sql.contains("WITH due AS"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("RETURNING"));
        assert!(sql.contains("attempts = attempts + 1"));
        // Lease anchored to the DB clock plus the bound interval ($1), #230.
        assert!(sql.contains("NOW() +"));
        assert!(sql.contains("$1"));
        assert!(sql.contains("LIMIT $2"));
        assert!(sql.contains("FROM \"scheduled_messages\""));
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_claim_uses_returning_without_skip_locked() {
        let sql = claim_returning_sql(Dialect::Sqlite, "scheduled_messages");
        assert!(sql.contains("RETURNING"));
        assert!(!sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("strftime"));
        assert!(sql.contains("paused = 0"));
        assert!(sql.contains('?'));
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_claim_is_a_three_step_sequence_without_returning() {
        let select = mysql_claim_select_sql("scheduled_messages");
        assert!(select.contains("FROM `scheduled_messages`"));
        assert!(select.contains("FOR UPDATE SKIP LOCKED"));
        assert!(!select.contains('"'));

        let update = mysql_claim_update_sql("scheduled_messages", 3);
        assert!(update.contains("UPDATE `scheduled_messages`"));
        assert!(update.contains("attempts = attempts + 1"));
        assert!(update.contains("UTC_TIMESTAMP(6)"));
        assert!(update.contains("WHERE schedule_id IN (?, ?, ?)"));

        let reselect = mysql_claim_reselect_sql("scheduled_messages", 2);
        assert!(reselect.contains("SELECT"));
        assert!(reselect.contains("WHERE schedule_id IN (?, ?)"));

        // No dialect emits RETURNING for MySQL.
        for sql in [&select, &update, &reselect] {
            assert!(!sql.contains("RETURNING"), "MySQL has no RETURNING: {sql}");
        }
    }

    #[test]
    fn eligible_predicate_excludes_terminal_paused_and_leased() {
        let predicate = eligible_predicate(Dialect::Postgres);
        assert!(predicate.contains("delivered_at IS NULL"));
        assert!(predicate.contains("cancelled_at IS NULL"));
        assert!(predicate.contains("dead_lettered_at IS NULL"));
        assert!(predicate.contains("paused = FALSE"));
        assert!(predicate.contains("attempts < max_attempts"));
        assert!(predicate.contains("leased_until IS NULL OR leased_until <="));
    }

    #[test]
    fn sqlite_eligible_predicate_uses_integer_false() {
        assert!(eligible_predicate(Dialect::Sqlite).contains("paused = 0"));
    }

    #[test]
    fn insert_binds_nine_columns_quoted_per_dialect() {
        let pg = insert_sql(Dialect::Postgres, "scheduled_messages");
        assert!(pg.contains("INSERT INTO \"scheduled_messages\""));
        assert!(pg.contains("$1, $2, $3, $4, $5, $6, $7, $8, $9"));

        let mysql = insert_sql(Dialect::MySql, "scheduled_messages");
        assert!(mysql.contains("INSERT INTO `scheduled_messages`"));
        assert!(!mysql.contains('"'));
        assert!(mysql.contains("?, ?, ?, ?, ?, ?, ?, ?, ?"));
    }

    #[test]
    fn acknowledgement_statements_guard_against_terminal_states() {
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            for sql in [
                mark_delivered_sql(dialect, "scheduled_messages"),
                reschedule_sql(dialect, "scheduled_messages"),
                mark_dead_lettered_sql(dialect, "scheduled_messages"),
            ] {
                assert!(
                    sql.contains("delivered_at IS NULL")
                        && sql.contains("cancelled_at IS NULL")
                        && sql.contains("dead_lettered_at IS NULL"),
                    "{dialect:?} ack must not revive a terminal or cancelled schedule: {sql}"
                );
            }
        }
    }

    #[test]
    fn reschedule_resets_attempts_and_clears_lease() {
        let sql = reschedule_sql(Dialect::Postgres, "scheduled_messages");
        assert!(sql.contains("attempts = 0"));
        assert!(sql.contains("leased_until = NULL"));
        assert!(sql.contains("scheduled_for = $1"));
        assert!(sql.contains("WHERE schedule_id = $2"));
    }

    #[test]
    fn cancel_and_set_paused_quote_identifiers() {
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            let quoted = dialect.quote_identifier("scheduled_messages");
            assert!(cancel_sql(dialect, "scheduled_messages").contains(&quoted));
            assert!(set_paused_sql(dialect, "scheduled_messages").contains(&quoted));
        }
    }

    #[test]
    fn statements_quote_reserved_word_table_names() {
        // A table named `order` is a reserved word; every statement must embed
        // it quoted so it does not raise a syntax error at runtime.
        for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            let quoted = dialect.quote_identifier("order");
            assert!(insert_sql(dialect, "order").contains(&quoted));
            assert!(mark_delivered_sql(dialect, "order").contains(&quoted));
            assert!(inspect_sql(dialect, "order").contains(&quoted));
        }
    }
}
