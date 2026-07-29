use clap::Args;

use super::connect::connect;
use crate::conn_string::ConnString;
use crate::error::CliError;

/// Validate that the target outbox table exists with the expected columns.
///
/// Returns exit code 0 on success, 1 when the table is missing or
/// incomplete (with a remediation message printed to stderr).
#[derive(Args, Debug)]
pub(crate) struct CheckArgs {
    /// PostgreSQL connection URL.
    ///
    /// Carries database credentials in its userinfo component. Prefer
    /// setting `DATABASE_URL` in the environment, or a `.pgpass` file,
    /// over passing this on the command line: argv is readable by every
    /// local user via `/proc/<pid>/cmdline` or `ps aux`, and shells
    /// persist it in history.
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    conn: ConnString,
    /// Outbox table name to validate.
    #[arg(long, default_value = "audit_outbox", env = "HEXERACT_OUTBOX_TABLE")]
    table: String,
}

const REQUIRED_COLUMNS: &[&str] = &[
    "id",
    "event_id",
    "event_type",
    "payload",
    "subject_id",
    "created_at",
    "attempts",
    "last_error",
    "next_retry_at",
    "delivered_at",
];

/// Read the target table's columns from the catalog.
///
/// Scoped to `table_schema = current_schema()` so a same-named table in
/// another schema cannot answer for the one the connection actually
/// resolves, which would report a missing or malformed table as valid.
const COLUMN_NAMES_QUERY: &str = "SELECT column_name FROM information_schema.columns \
                                  WHERE table_name = $1 AND table_schema = current_schema()";

impl CheckArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        let client = connect(&self.conn).await?;

        let rows = client
            .query(COLUMN_NAMES_QUERY, &[&self.table])
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;

        if rows.is_empty() {
            eprintln!("Table `{}` does not exist.", self.table);
            eprintln!(
                "Run `hexeract outbox patch --table {}` to get the canonical SQL.",
                self.table
            );
            return Err(CliError::Fatal(
                format!("table `{}` does not exist", self.table).into(),
            ));
        }

        let actual: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        let missing: Vec<&&str> = REQUIRED_COLUMNS
            .iter()
            .filter(|expected| !actual.iter().any(|a| a == **expected))
            .collect();

        if missing.is_empty() {
            println!(
                "Table `{}` is valid ({} required columns present).",
                self.table,
                REQUIRED_COLUMNS.len()
            );
            Ok(())
        } else {
            eprintln!("Table `{}` is missing columns: {missing:?}", self.table);
            eprintln!(
                "Run `hexeract outbox patch --table {}` to compare against the canonical schema.",
                self.table
            );
            Err(CliError::Fatal(
                format!(
                    "table `{}` is missing required columns: {missing:?}",
                    self.table
                )
                .into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::COLUMN_NAMES_QUERY;

    #[test]
    fn column_lookup_is_scoped_to_the_current_schema() {
        assert!(
            COLUMN_NAMES_QUERY.contains("table_schema = current_schema()"),
            "the catalog lookup must filter by schema, otherwise a same-named table in another \
             schema reports this one as valid, got {COLUMN_NAMES_QUERY}"
        );
    }
}
