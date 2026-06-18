//! The Rust messaging framework for reliable event-driven services.
//!
//! Hexeract is an opinionated messaging framework for Rust. It unifies
//! in-process mediator dispatch, multi-broker bus transport and a
//! transactional outbox in a single ergonomic crate. Sagas, scheduler
//! and request/reply are planned for future releases (see
//! [ROADMAP.md](https://github.com/nubster-opensources/hexeract/blob/main/ROADMAP.md)).
//!
//! This facade re-exports every shipped sub-crate behind opt-in
//! feature flags so consumers only compile what they actually use.
//!
//! # Quick start
//!
//! Outbox over PostgreSQL:
//!
//! ```toml
//! [dependencies]
//! hexeract = { version = "0.5", features = ["outbox-sql-postgres"] }
//! ```
//!
//! Bus over RabbitMQ:
//!
//! ```toml
//! [dependencies]
//! hexeract = { version = "0.5", features = ["bus-rabbitmq"] }
//! ```
//!
//! Both together:
//!
//! ```toml
//! [dependencies]
//! hexeract = { version = "0.5", features = ["outbox-sql-postgres", "bus-rabbitmq"] }
//! ```
//!
//! # Feature matrix
//!
//! | Feature | Enables | Pulls |
//! | --- | --- | --- |
//! | `core` | Cross-cutting primitives (`MessageId`, `CorrelationId`, `HandlerContext`) | [`hexeract_core`] |
//! | `outbox` | Backend-agnostic outbox traits | [`hexeract_outbox`] |
//! | `outbox-sql-postgres` | PostgreSQL outbox backend via `sqlx` | [`hexeract_outbox`] + [`hexeract_outbox_sql`] |
//! | `outbox-sql-mysql` | MySQL outbox backend via `sqlx` | [`hexeract_outbox`] + [`hexeract_outbox_sql`] |
//! | `outbox-sql-sqlite` | SQLite outbox backend via `sqlx` | [`hexeract_outbox`] + [`hexeract_outbox_sql`] |
//! | `bus` | Backend-agnostic bus traits | [`hexeract_bus`] |
//! | `bus-rabbitmq` | RabbitMQ bus backend | [`hexeract_bus`] + [`hexeract_bus_rabbitmq`] |
//! | `mediator` | In-process CQRS mediator | [`hexeract_mediator`] |
//! | `middleware` | Built-in tracing and timeout middlewares | [`hexeract_middleware`] |
//! | `macros` | `#[handler]` attribute macro for handler registration | [`hexeract_macros`] + [`hexeract_core`] |
//! | `scheduler` | Backend-agnostic scheduler traits and `SchedulerBuilder` | [`hexeract_scheduler`] |
//! | `scheduler-mediator` | Scheduler sink that dispatches via the in-process mediator | [`hexeract_scheduler`] + mediator |
//! | `scheduler-bus` | Scheduler sink that publishes via the message bus | [`hexeract_scheduler`] + bus |
//! | `scheduler-outbox` | Scheduler sink that enqueues into the transactional outbox | [`hexeract_scheduler`] |
//! | `scheduler-sql-postgres` | PostgreSQL schedule store via `sqlx` | [`hexeract_scheduler`] + [`hexeract_scheduler_sql`] |
//! | `scheduler-sql-mysql` | MySQL schedule store via `sqlx` | [`hexeract_scheduler`] + [`hexeract_scheduler_sql`] |
//! | `scheduler-sql-sqlite` | SQLite schedule store via `sqlx` | [`hexeract_scheduler`] + [`hexeract_scheduler_sql`] |
//!
//! Every feature transitively enables `core`, so a downstream user
//! automatically has access to `hexeract::core::HandlerContext`.

#![cfg_attr(docsrs, feature(doc_cfg))]

/// Cross-cutting primitives shared by every feature module.
///
/// Re-export of [`hexeract_core`].
#[cfg(feature = "core")]
#[cfg_attr(docsrs, doc(cfg(feature = "core")))]
pub use hexeract_core as core;

/// Backend-agnostic outbox traits.
///
/// Re-export of [`hexeract_outbox`].
#[cfg(feature = "outbox")]
#[cfg_attr(docsrs, doc(cfg(feature = "outbox")))]
pub use hexeract_outbox as outbox;

/// SQL outbox backends for PostgreSQL, MySQL and SQLite via `sqlx`.
///
/// Re-export of [`hexeract_outbox_sql`]. Enabled by any of the
/// `outbox-sql-postgres`, `outbox-sql-mysql` or `outbox-sql-sqlite` features.
#[cfg(any(
    feature = "outbox-sql-postgres",
    feature = "outbox-sql-mysql",
    feature = "outbox-sql-sqlite"
))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        feature = "outbox-sql-postgres",
        feature = "outbox-sql-mysql",
        feature = "outbox-sql-sqlite"
    )))
)]
pub use hexeract_outbox_sql as outbox_sql;

/// Backend-agnostic bus traits.
///
/// Re-export of [`hexeract_bus`].
#[cfg(feature = "bus")]
#[cfg_attr(docsrs, doc(cfg(feature = "bus")))]
pub use hexeract_bus as bus;

/// RabbitMQ bus backend.
///
/// Re-export of [`hexeract_bus_rabbitmq`].
#[cfg(feature = "bus-rabbitmq")]
#[cfg_attr(docsrs, doc(cfg(feature = "bus-rabbitmq")))]
pub use hexeract_bus_rabbitmq as bus_rabbitmq;

/// In-process CQRS mediator: command, query and notification dispatch.
///
/// Re-export of [`hexeract_mediator`].
#[cfg(feature = "mediator")]
#[cfg_attr(docsrs, doc(cfg(feature = "mediator")))]
pub use hexeract_mediator as mediator;

/// Built-in middlewares: tracing spans and dispatch timeouts.
///
/// Re-export of [`hexeract_middleware`].
#[cfg(feature = "middleware")]
#[cfg_attr(docsrs, doc(cfg(feature = "middleware")))]
pub use hexeract_middleware as middleware;

/// Procedural macros: the `#[handler]` attribute for handler registration.
///
/// Re-export of [`hexeract_macros`].
#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
pub use hexeract_macros as macros;

/// Backend-agnostic scheduler traits and the validated `SchedulerBuilder`.
///
/// Re-export of [`hexeract_scheduler`].
#[cfg(feature = "scheduler")]
#[cfg_attr(docsrs, doc(cfg(feature = "scheduler")))]
pub use hexeract_scheduler as scheduler;

/// SQL schedule store backends for PostgreSQL, MySQL and SQLite via `sqlx`.
///
/// Re-export of [`hexeract_scheduler_sql`]. Enabled by any of the
/// `scheduler-sql-postgres`, `scheduler-sql-mysql` or `scheduler-sql-sqlite` features.
#[cfg(any(
    feature = "scheduler-sql-postgres",
    feature = "scheduler-sql-mysql",
    feature = "scheduler-sql-sqlite"
))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        feature = "scheduler-sql-postgres",
        feature = "scheduler-sql-mysql",
        feature = "scheduler-sql-sqlite"
    )))
)]
pub use hexeract_scheduler_sql as scheduler_sql;
