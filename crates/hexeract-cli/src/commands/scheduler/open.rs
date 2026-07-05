//! Dialect dispatch for the scheduler admin stores.
//!
//! `ScheduleAdmin` is built via `trait_variant::make(Send)`, which is not
//! `dyn`-compatible, so a `Box<dyn ScheduleAdmin>` cannot be used to erase the
//! backend. [`AnyScheduleAdmin`] sidesteps that limit with enum dispatch
//! instead: one variant per backend, each holding its concrete store, with
//! every operation forwarded through a `match`.

use clap::Args;
use hexeract_scheduler::{ScheduleAdmin, ScheduleSnapshot, ScheduleStore, SchedulerError};
use hexeract_scheduler_sql::{
    DEFAULT_TABLE_NAME, MySqlScheduleStore, PgScheduleStore, SqliteScheduleStore,
};
use sqlx::{MySqlPool, PgPool, SqlitePool};
use uuid::Uuid;

use crate::error::CliError;

/// Shared connection arguments for every scheduler admin command.
#[derive(Args, Debug)]
pub(crate) struct DatabaseArgs {
    /// Database connection URL. Its scheme selects the backend
    /// (`postgres://`, `mysql://` or `sqlite://`).
    #[arg(long, env = "DATABASE_URL")]
    pub(crate) conn: String,
    /// Name of the scheduler table.
    #[arg(long, default_value = DEFAULT_TABLE_NAME, env = "HEXERACT_SCHEDULER_TABLE")]
    pub(crate) table: String,
}

/// Backend deduced from a connection URL's scheme.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DialectKind {
    Postgres,
    MySql,
    Sqlite,
}

/// Deduce the SQL backend from the scheme of a connection URL.
///
/// # Errors
///
/// Returns [`CliError::Fatal`] if the scheme is not one of `postgres`,
/// `postgresql`, `mysql` or `sqlite`.
pub(crate) fn dialect_of(conn: &str) -> Result<DialectKind, CliError> {
    let scheme = conn.split(':').next().unwrap_or_default();
    match scheme {
        "postgres" | "postgresql" => Ok(DialectKind::Postgres),
        "mysql" => Ok(DialectKind::MySql),
        "sqlite" => Ok(DialectKind::Sqlite),
        other => Err(CliError::Fatal(
            format!("unsupported database url scheme: {other}").into(),
        )),
    }
}

/// A [`ScheduleAdmin`] whose concrete backend is chosen at runtime.
///
/// `scheduler list` (B4) wires `open`/`list_pending`; `scheduler inspect`
/// (B5) wires `inspect`; `scheduler dead-letter list`/`replay` (B6) wire
/// `list_dead_letter`/`replay`. Every associated item is now consumed by a
/// command.
pub(crate) enum AnyScheduleAdmin {
    Postgres(PgScheduleStore),
    MySql(MySqlScheduleStore),
    Sqlite(SqliteScheduleStore),
}

impl AnyScheduleAdmin {
    /// Open a pool for `conn`'s dialect and wrap it in the matching store.
    ///
    /// # Errors
    ///
    /// Returns [`CliError::Fatal`] if the scheme is unsupported, the pool
    /// fails to connect, or the store rejects `table` as an invalid
    /// identifier.
    pub(crate) async fn open(conn: &str, table: &str) -> Result<Self, CliError> {
        let fatal = |e: SchedulerError| CliError::Fatal(Box::new(e));
        let connect = |e: sqlx::Error| CliError::Fatal(Box::new(e));
        match dialect_of(conn)? {
            DialectKind::Postgres => {
                let pool = PgPool::connect(conn).await.map_err(connect)?;
                Ok(Self::Postgres(
                    PgScheduleStore::new(pool, table).map_err(fatal)?,
                ))
            }
            DialectKind::MySql => {
                let pool = MySqlPool::connect(conn).await.map_err(connect)?;
                Ok(Self::MySql(
                    MySqlScheduleStore::new(pool, table).map_err(fatal)?,
                ))
            }
            DialectKind::Sqlite => {
                let pool = SqlitePool::connect(conn).await.map_err(connect)?;
                Ok(Self::Sqlite(
                    SqliteScheduleStore::new(pool, table).map_err(fatal)?,
                ))
            }
        }
    }

    /// Forward to the active backend's [`ScheduleAdmin::list_pending`].
    pub(crate) async fn list_pending(
        &self,
        limit: usize,
    ) -> Result<Vec<ScheduleSnapshot>, SchedulerError> {
        match self {
            Self::Postgres(s) => s.list_pending(limit).await,
            Self::MySql(s) => s.list_pending(limit).await,
            Self::Sqlite(s) => s.list_pending(limit).await,
        }
    }

    /// Forward to the active backend's [`ScheduleAdmin::list_dead_letter`].
    pub(crate) async fn list_dead_letter(
        &self,
        limit: usize,
    ) -> Result<Vec<ScheduleSnapshot>, SchedulerError> {
        match self {
            Self::Postgres(s) => s.list_dead_letter(limit).await,
            Self::MySql(s) => s.list_dead_letter(limit).await,
            Self::Sqlite(s) => s.list_dead_letter(limit).await,
        }
    }

    /// Forward to the active backend's [`ScheduleStore::inspect`].
    pub(crate) async fn inspect(
        &self,
        id: Uuid,
    ) -> Result<Option<ScheduleSnapshot>, SchedulerError> {
        match self {
            Self::Postgres(s) => s.inspect(id).await,
            Self::MySql(s) => s.inspect(id).await,
            Self::Sqlite(s) => s.inspect(id).await,
        }
    }

    /// Forward to the active backend's [`ScheduleAdmin::replay`].
    pub(crate) async fn replay(&self, id: Uuid) -> Result<(), SchedulerError> {
        match self {
            Self::Postgres(s) => s.replay(id).await,
            Self::MySql(s) => s.replay(id).await,
            Self::Sqlite(s) => s.replay(id).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect_is_deduced_from_url_scheme() {
        assert!(matches!(
            dialect_of("postgres://x").unwrap(),
            DialectKind::Postgres
        ));
        assert!(matches!(
            dialect_of("postgresql://x").unwrap(),
            DialectKind::Postgres
        ));
        assert!(matches!(
            dialect_of("mysql://x").unwrap(),
            DialectKind::MySql
        ));
        assert!(matches!(
            dialect_of("sqlite://x.db").unwrap(),
            DialectKind::Sqlite
        ));
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        assert!(dialect_of("redis://x").is_err());
    }
}
