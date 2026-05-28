//! Built-in middlewares for the Hexeract messaging framework.
//!
//! - [`TracingMiddleware`] opens a [`tracing::Span`] around every dispatch
//!   and emits a structured event on entry and on completion or failure.
//! - [`TimeoutMiddleware`] aborts the dispatch with
//!   [`hexeract_core::HexeractError::Timeout`] when the inner pipeline takes
//!   longer than the configured duration.
//!
//! # Recommended order
//!
//! Wire `TracingMiddleware` first so that the span observes the entry, the
//! timeout, and the resulting failure with the typed error in the exit
//! event. With the inverse order, the span never opens when the timeout
//! fires, which makes the failure harder to debug.
//!
//! ```ignore
//! use std::time::Duration;
//! use hexeract_mediator::MediatorBuilder;
//! use hexeract_middleware::{TimeoutMiddleware, TracingMiddleware};
//!
//! let mediator = MediatorBuilder::new()
//!     .with_middleware(TracingMiddleware::new())
//!     .with_middleware(TimeoutMiddleware::new(Duration::from_secs(5)))
//!     // .register_command_handler::<_, _>(...)
//!     .build()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod timeout;
mod tracing;

pub use timeout::TimeoutMiddleware;
pub use tracing::TracingMiddleware;
