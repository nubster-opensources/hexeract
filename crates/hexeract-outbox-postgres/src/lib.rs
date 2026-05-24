//! PostgreSQL backend for the Hexeract outbox.
//!
//! This crate provides the canonical schema, the `PgOutboxPublisher`
//! implementation of [`hexeract_outbox::OutboxPublisher`] backed by
//! `deadpool_postgres`, and the helpers for managing the outbox table.

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
