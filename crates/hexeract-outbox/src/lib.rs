//! Transactional outbox primitives for the Hexeract messaging framework.
//!
//! The outbox pattern stores outgoing domain events in the same database
//! transaction as the business state mutation, then a worker polls the
//! table and dispatches each row to its registered handler. This crate
//! ships the backend-agnostic primitives: the [`Event`] marker trait, the
//! persisted [`OutboxEnvelope`] row representation, and the unified
//! [`OutboxError`] type.
//!
//! Backend implementations live in companion crates such as
//! `hexeract-outbox-postgres`.

/// Persisted representation of an event awaiting dispatch.
pub mod envelope;
/// Errors raised by the outbox primitives, publishers and workers.
pub mod error;
/// Marker trait for domain events that flow through the outbox.
pub mod event;
/// Asynchronous handler contract dispatched by the worker.
pub mod handler;
/// Backend-agnostic contract for inserting events into the outbox.
pub mod publisher;
/// Poll loop worker, type-erased dispatch and store abstraction.
pub mod worker;

pub use envelope::OutboxEnvelope;
pub use error::OutboxError;
pub use event::Event;
pub use handler::Handler;
pub use publisher::OutboxPublisher;
pub use worker::{
    BoxFuture, ErasedHandler, OutboxStore, OutboxWorker, OutboxWorkerConfig, TypedHandler,
};
