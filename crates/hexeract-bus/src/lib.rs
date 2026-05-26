//! Message bus primitives for the Hexeract messaging framework.
//!
//! This crate ships the backend-agnostic primitives the rest of the bus
//! ecosystem composes on top of: the [`Message`] marker trait, the
//! in-flight [`BusEnvelope`] carried across the wire, the unified
//! [`BusError`] type, the strongly-typed topology declarations
//! ([`Exchange`], [`Queue`], [`Binding`], [`RoutingKey`]), and the
//! backend-agnostic [`Transport`] publish contract.
//!
//! Backend implementations live in companion crates such as
//! `hexeract-bus-rabbitmq`.

/// In-flight representation of a message crossing the bus.
pub mod envelope;
/// Errors raised by the bus primitives, transports and workers.
pub mod error;
/// Marker trait for domain messages that flow through the bus.
pub mod message;
/// Strongly-typed topology declarations shared by transports.
pub mod topology;
/// Backend-agnostic publish contract implemented by bus backends.
pub mod transport;

pub use envelope::BusEnvelope;
pub use error::BusError;
pub use message::Message;
pub use topology::Binding;
pub use topology::Exchange;
pub use topology::ExchangeKind;
pub use topology::Queue;
pub use topology::RoutingKey;
pub use transport::Transport;
