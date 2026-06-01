//! SQL backends for the Hexeract outbox, built on `sqlx`.
//!
//! This crate implements the backend-agnostic [`hexeract_outbox`] contracts
//! ([`OutboxStore`], [`OutboxPublisher`]) on top of `sqlx`, with one
//! compile-time backend per Cargo feature:
//!
//! - `postgres` (default): [`PgOutboxStore`] / [`PgOutboxPublisher`]
//! - `mysql`: `MySqlOutboxStore` / `MySqlOutboxPublisher`
//! - `sqlite`: `SqliteOutboxStore` / `SqliteOutboxPublisher`
//!
//! The SQL dialect differences (placeholder style, row locking, timestamp
//! handling and schema DDL) are centralized in [`Dialect`], so the per-backend
//! stores share the statement templating and the envelope assembly logic.
//!
//! [`OutboxStore`]: hexeract_outbox::OutboxStore
//! [`OutboxPublisher`]: hexeract_outbox::OutboxPublisher

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
compile_error!(
    "hexeract-outbox-sql requires at least one backend feature: `postgres`, `mysql` or `sqlite`"
);

/// SQL dialect differences absorbed by the backend stores.
pub mod dialect;
mod envelope;
mod validate;

#[cfg(feature = "postgres")]
/// PostgreSQL backend backed by `sqlx::PgPool`.
pub mod postgres;

#[cfg(feature = "mysql")]
/// MySQL backend backed by `sqlx::MySqlPool`.
pub mod mysql;

#[cfg(feature = "sqlite")]
/// SQLite backend backed by `sqlx::SqlitePool`.
pub mod sqlite;

pub use dialect::Dialect;

#[cfg(feature = "postgres")]
pub use postgres::{PgOutboxPublisher, PgOutboxStore, PgOutboxWorkerBuilder};

#[cfg(feature = "mysql")]
pub use mysql::{MySqlOutboxPublisher, MySqlOutboxStore, MySqlOutboxWorkerBuilder};

#[cfg(feature = "sqlite")]
pub use sqlite::{SqliteOutboxPublisher, SqliteOutboxStore, SqliteOutboxWorkerBuilder};

/// Default outbox table name used when a builder's `table_name` is not set.
pub const DEFAULT_TABLE_NAME: &str = "audit_outbox";
