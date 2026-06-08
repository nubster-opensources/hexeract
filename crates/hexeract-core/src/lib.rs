//! Core traits and types for the Hexeract messaging framework.
//!
//! This crate is the cross-cutting foundation that every other Hexeract
//! crate depends on. It defines the marker traits for messages
//! ([`Command`], [`Query`], [`Notification`]), the matching handler
//! traits ([`CommandHandler`], [`QueryHandler`], [`NotificationHandler`]),
//! the [`MessageEnvelope`] and [`HandlerContext`] passed alongside every
//! dispatch, the [`Middleware`] / [`Next`] / [`Terminal`] pipeline
//! primitives, the unified [`HexeractError`] type, and the
//! [`HandlerRegistration`] metadata collected by the `#[handler]` macro.

/// Marker trait for messages expressing the intent to mutate state.
pub mod command;
/// Contextual information propagated into every handler invocation.
pub mod context;
/// Type-erased metadata carried alongside every dispatch.
pub mod envelope;
/// Unified framework error type.
pub mod error;
/// Async handler traits dispatched by the mediator.
pub mod handler;
/// Unique identifier newtypes for messages and correlations.
pub mod ids;
/// Middleware pipeline primitives.
pub mod middleware;
/// Marker trait for broadcast messages with fan-out semantics.
pub mod notification;
/// Marker trait for read-only messages asking for information.
pub mod query;
/// Handler registration metadata collected at link time by the
/// `#[handler]` macro.
pub mod registration;

pub use command::Command;
pub use context::HandlerContext;
pub use envelope::MessageEnvelope;
pub use error::{HexeractError, NotificationFailure};
pub use handler::{CommandHandler, NotificationHandler, QueryHandler};
pub use ids::{CorrelationId, MessageId};
pub use middleware::{BoxOutput, DynMiddleware, Middleware, Next, Terminal};
pub use notification::Notification;
pub use query::Query;
pub use registration::{HandlerKind, HandlerRegistration};
