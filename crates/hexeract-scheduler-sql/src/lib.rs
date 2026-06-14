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
//! This module ships the schema DDL ([`schema`]); the `sqlx`-backed
//! [`hexeract_scheduler::ScheduleStore`] implementations are added on top of
//! it.
//!
//! [`Dialect`]: hexeract_outbox_sql::Dialect

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
compile_error!(
    "hexeract-scheduler-sql requires at least one backend feature: `postgres`, `mysql` or `sqlite`"
);

/// Canonical schema DDL for the `scheduled_messages` table.
pub mod schema;
mod validate;

pub use hexeract_outbox_sql::Dialect;

/// Default scheduler table name used when a table name is not set.
pub const DEFAULT_TABLE_NAME: &str = "scheduled_messages";
