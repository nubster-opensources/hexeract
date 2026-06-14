//! SQL schema for the Hexeract scheduler, built on `sqlx`.
//!
//! This crate persists the [`hexeract_scheduler`] domain on SQL. It reuses
//! the SQL [`Dialect`] of [`hexeract_outbox_sql`] for injection-safe quoting
//! and database-clock lease anchoring, and adds the scheduler's own
//! `scheduled_messages` schema, which differs from the outbox table (it
//! carries `scheduled_for`, an optional cron expression, a dispatch target,
//! lease columns and a paused flag).
//!
//! A backend is selected at compile time per Cargo feature:
//!
//! - `postgres` (default)
//! - `mysql`
//! - `sqlite`
//!
//! This module ships the schema DDL ([`schema`]) and a `sqlx`-backed
//! [`hexeract_scheduler::ScheduleStore`] per backend: [`PgScheduleStore`],
//! [`MySqlScheduleStore`] and [`SqliteScheduleStore`], each gated by its
//! Cargo feature.
//!
//! [`Dialect`]: hexeract_outbox_sql::Dialect

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
compile_error!(
    "hexeract-scheduler-sql requires at least one backend feature: `postgres`, `mysql` or `sqlite`"
);

mod mapping;
/// Canonical schema DDL for the `scheduled_messages` table.
pub mod schema;
mod statements;
mod timestamp;
mod validate;

#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use hexeract_outbox_sql::Dialect;

#[cfg(feature = "mysql")]
pub use mysql::MySqlScheduleStore;
#[cfg(feature = "postgres")]
pub use postgres::PgScheduleStore;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteScheduleStore;

/// Default scheduler table name used when a table name is not set.
pub const DEFAULT_TABLE_NAME: &str = "scheduled_messages";
