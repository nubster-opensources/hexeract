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
//! Wire `TracingMiddleware` first (outermost) so that the span observes
//! the entry, the timeout, and the resulting failure. With the inverse
//! order, `TracingMiddleware` sits inside `TimeoutMiddleware`: the span
//! still opens and emits "entering", but when the timeout fires the inner
//! future is dropped at its next await point, so the "completed" or
//! "failed" exit events from `TracingMiddleware` are never emitted. The
//! recommended order keeps the full entry-to-exit record in the span.
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
