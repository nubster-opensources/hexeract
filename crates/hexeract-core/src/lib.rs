#![deny(clippy::all, clippy::pedantic)]
#![warn(missing_docs)]

//! Core traits and types for the Hexeract messaging framework.
//!
//! This crate is a placeholder. The full implementation ships in v0.1.0.

pub mod context;
pub mod error;
pub mod ids;

pub use context::HandlerContext;
pub use error::HexeractError;
pub use ids::{CorrelationId, MessageId};
