//! PostgreSQL backend for the Hexeract outbox.
//!
//! This crate provides the canonical schema, the `PgOutboxPublisher`
//! implementation of [`hexeract_outbox::OutboxPublisher`] backed by
//! `deadpool_postgres`, and the helpers for managing the outbox table.
//!
//! # Deprecated
//!
//! This crate is deprecated since 0.4.0. Use the `hexeract-outbox-sql` crate
//! with the `postgres` feature instead; it supersedes this backend and also
//! adds MySQL and SQLite. This crate will be removed in 0.5.0.

// The crate keeps referencing its own deprecated items internally for one
// release cycle; external consumers still receive the deprecation warnings.
#![allow(deprecated)]

/// Fluent builder for an outbox worker backed by `PgOutboxStore`.
pub mod builder;
/// PostgreSQL implementation of the outbox publisher.
pub mod publisher;
/// Canonical schema definition and helpers.
pub mod schema;
/// PostgreSQL implementation of the outbox store driven by the worker.
pub mod store;

pub use builder::{DEFAULT_TABLE_NAME, PgOutboxWorkerBuilder};
pub use publisher::PgOutboxPublisher;
pub use schema::POSTGRES_SCHEMA_SQL;
pub use schema::ensure_schema;
pub use schema::render_schema;
pub use store::PgOutboxStore;
