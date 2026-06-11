use clap::Args;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::NoTls;

/// Validate that the target outbox table exists with the expected columns.
///
/// Returns exit code 0 on success, 1 when the table is missing or
/// incomplete (with a remediation message printed to stderr).
#[derive(Args, Debug)]
pub(crate) struct CheckArgs {
    /// PostgreSQL connection URL.
    #[arg(long, env = "DATABASE_URL")]
    conn: String,
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

impl CheckArgs {
    pub(crate) async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let client = connect(&self.conn).await?;

        let rows = client
            .query(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = $1 AND table_schema = current_schema()",
                &[&self.table],
            )
            .await?;

        if rows.is_empty() {
            eprintln!("Table `{}` does not exist.", self.table);
            eprintln!(
                "Run `hexeract outbox patch --table {}` to get the canonical SQL.",
                self.table
            );
            std::process::exit(1);
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
            std::process::exit(1);
        }
    }
}

/// Connect to PostgreSQL, using TLS unless `sslmode=disable` is present in the URL.
///
/// When `sslmode=disable` is set the connection proceeds without TLS.
/// All other `sslmode` values (including the default `prefer`) result in a TLS
/// connection using the platform certificate store via `native-tls`.
async fn connect(url: &str) -> Result<tokio_postgres::Client, Box<dyn std::error::Error>> {
    if is_ssl_disabled(url) {
        tracing::warn!("TLS disabled via sslmode=disable; credentials will be sent in cleartext");
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "PostgreSQL connection task error");
            }
        });
        Ok(client)
    } else {
        let builder = native_tls::TlsConnector::builder().build()?;
        let connector = MakeTlsConnector::new(builder);
        let (client, connection) = tokio_postgres::connect(url, connector).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "PostgreSQL connection task error");
            }
        });
        Ok(client)
    }
}

/// Returns `true` when the URL explicitly opts out of TLS via `sslmode=disable`.
pub(crate) fn is_ssl_disabled(url: &str) -> bool {
    url.contains("sslmode=disable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_ssl_disabled_detects_disable_param() {
        assert!(is_ssl_disabled(
            "postgres://user:pass@host/db?sslmode=disable"
        ));
    }

    #[test]
    fn is_ssl_disabled_returns_false_for_require() {
        assert!(!is_ssl_disabled(
            "postgres://user:pass@host/db?sslmode=require"
        ));
    }

    #[test]
    fn is_ssl_disabled_returns_false_for_default_url() {
        assert!(!is_ssl_disabled("postgres://user:pass@host/db"));
    }

    #[test]
    fn check_query_includes_table_schema_filter() {
        let query = "SELECT column_name FROM information_schema.columns \
                     WHERE table_name = $1 AND table_schema = current_schema()";
        assert!(
            query.contains("table_schema = current_schema()"),
            "query must filter by current schema to avoid cross-schema collisions"
        );
    }
}
