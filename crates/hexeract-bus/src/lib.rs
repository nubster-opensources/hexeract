//! Message bus primitives for the Hexeract messaging framework.
//!
//! This crate ships the backend-agnostic primitives the rest of the bus
//! ecosystem composes on top of: the [`Message`] marker trait, the
//! in-flight [`BusEnvelope`] carried across the wire, and the unified
//! [`BusError`] type.
//!
//! Backend implementations live in companion crates such as
//! `hexeract-bus-rabbitmq`.

/// In-flight representation of a message crossing the bus.
pub mod envelope;
/// Errors raised by the bus primitives, transports and workers.
pub mod error;
/// Marker trait for domain messages that flow through the bus.
pub mod message;

pub use envelope::BusEnvelope;
pub use error::BusError;
pub use message::Message;
