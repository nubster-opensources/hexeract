//! Backend-agnostic scheduling primitives for the Hexeract messaging framework.
//!
//! The scheduler is the outbox plus time: it persists a message together
//! with the instant it is due and an optional recurrence rule, then relays
//! it once that instant is reached. This crate ships the domain layer only:
//! the value types that backends and dispatch targets build upon. It pulls
//! in no database driver and runs entirely in isolation.
//!
//! # Building blocks
//!
//! - [`Trigger`] describes when a message fires: once at an instant
//!   ([`Trigger::delay`]) or repeatedly on a UTC cron expression
//!   ([`Trigger::cron`]).
//! - [`ScheduledMessage`] wraps the serialized event, its dispatch
//!   [`Target`] and its [`Trigger`] together with the instant of the
//!   current occurrence.
//! - [`OccurrenceId`] is the stable, content-derived identity of a single
//!   firing, used downstream as the deduplication key.
//! - [`ScheduleStore`] is the backend-agnostic persistence contract, with
//!   its crash-safe claim and lease protocol. [`InMemoryScheduleStore`] is
//!   a reference implementation for tests.
//! - [`ScheduleSink`] is the contract a due occurrence is dispatched
//!   through, with at-least-once delivery semantics.
//! - [`SchedulerWorker`] drives the loop: it claims due occurrences, dispatches
//!   them, then delivers, reschedules, retries with backoff or dead-letters.
//! - [`SchedulerError`] is the unified error type.
//!
//! # Time zone
//!
//! All instants are UTC. Per-schedule time zones are an explicit non-goal
//! of this version.

/// The unified scheduler error type.
pub mod error;
/// A due occurrence claimed under a lease.
pub mod lease;
/// In-process sink that republishes a due occurrence through the mediator.
#[cfg(feature = "mediator")]
pub mod mediator_sink;
/// An in-memory reference implementation of [`ScheduleStore`].
pub mod memory;
/// Stable identity of a single firing of a schedule.
pub mod occurrence;
/// Sink that enqueues a due occurrence into the transactional outbox.
#[cfg(feature = "outbox")]
pub mod outbox_sink;
/// A message persisted for future delivery.
pub mod schedule;
/// The contract a due occurrence is dispatched through.
pub mod sink;
/// A read-only view of a schedule's state.
pub mod snapshot;
/// The backend-agnostic persistence contract.
pub mod store;
/// The dispatch target of a scheduled message.
pub mod target;
/// When a scheduled message fires.
pub mod trigger;
/// The polling worker that drives schedules to their sink.
pub mod worker;

pub use error::SchedulerError;
pub use lease::LeasedOccurrence;
#[cfg(feature = "mediator")]
pub use mediator_sink::{MediatorSink, MediatorSinkBuilder};
pub use memory::InMemoryScheduleStore;
pub use occurrence::OccurrenceId;
#[cfg(feature = "outbox")]
pub use outbox_sink::OutboxSink;
pub use schedule::ScheduledMessage;
pub use sink::ScheduleSink;
pub use snapshot::{ScheduleSnapshot, ScheduleStatus};
pub use store::ScheduleStore;
pub use target::Target;
pub use trigger::{CronExpression, Trigger};
pub use worker::{SchedulerWorker, SchedulerWorkerConfig};
