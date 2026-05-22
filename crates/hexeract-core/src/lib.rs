#![deny(clippy::all, clippy::pedantic)]
#![warn(missing_docs)]

//! Core traits and types for the Hexeract messaging framework.
//!
//! This crate is a placeholder. The full implementation ships in v0.1.0.

/// Contextual information propagated into every handler invocation.
pub mod context;
/// Unified framework error type.
pub mod error;
/// Unique identifier newtypes for messages and correlations.
pub mod ids;

pub use context::HandlerContext;
pub use error::HexeractError;
pub use ids::{CorrelationId, MessageId};
