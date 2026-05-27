//! In-process mediator for the Hexeract messaging framework.
//!
//! The mediator dispatches a [`Command`], a [`Query`] or a [`Notification`]
//! to the handlers registered with a [`MediatorBuilder`] at startup. Dispatch
//! is type-safe and reflection-free: each call to
//! [`Mediator::send`], [`Mediator::query`] or [`Mediator::publish`] resolves
//! to the matching handler through a compile-time generic, while the internal
//! registry erases the handler types behind a `TypeId` lookup table.
//!
//! The three built-in middlewares (`TracingMiddleware`, `LoggingMiddleware`,
//! `TimeoutMiddleware`) ship in a follow-up release; users can still wire
//! their own [`Middleware`] implementations through
//! [`MediatorBuilder::with_middleware`] in the meantime.
#![cfg_attr(docsrs, feature(doc_cfg))]

mod erased;

use hexeract_core::{
    Command, CommandHandler, HexeractError, Middleware, Notification, NotificationHandler, Query,
    QueryHandler,
};

/// Errors raised by the mediator infrastructure itself, *before* a handler
/// runs. Handler failures keep flowing through [`HexeractError`].
#[derive(Debug, thiserror::Error)]
pub enum MediatorBuildError {
    /// A second handler was registered for a [`Command`] or [`Query`] that
    /// already had one. Commands and queries are single-handler by contract;
    /// notifications are not affected by this rule.
    #[error("duplicate handler registered for {type_name}")]
    DuplicateHandler {
        /// Fully-qualified type name of the offending message.
        type_name: &'static str,
    },
}

/// Errors surfaced when a dispatched message cannot reach a handler.
#[derive(Debug, thiserror::Error)]
pub enum MediatorDispatchError {
    /// No handler is registered for the dispatched [`Command`] or [`Query`].
    /// Notification dispatch never raises this error: a notification with
    /// zero handlers is a valid no-op.
    #[error("no handler registered for {type_name}")]
    HandlerNotFound {
        /// Fully-qualified type name of the unhandled message.
        type_name: &'static str,
    },
}

/// Fluent builder that wires handlers and middlewares into a [`Mediator`].
///
/// # Example
///
/// ```ignore
/// let mediator = MediatorBuilder::new()
///     .register_command_handler::<CreateUser, _>(UserRepository)
///     .register_query_handler::<GetUser, _>(UserReadModel)
///     .register_notification_handler::<UserCreated, _>(AuditWriter)
///     .register_notification_handler::<UserCreated, _>(EmailNotifier)
///     .with_middleware(TracingMiddleware)
///     .with_middleware(TimeoutMiddleware::new(Duration::from_secs(5)))
///     .build()?;
/// ```
pub struct MediatorBuilder {
    // private registry, opaque to public API
    _private: (),
}

impl MediatorBuilder {
    /// Creates a fresh builder with no handlers and no middlewares.
    #[must_use]
    pub fn new() -> Self {
        todo!()
    }

    /// Registers the single [`CommandHandler`] responsible for command `C`.
    ///
    /// Calling this twice for the same `C` produces a [`MediatorBuildError::DuplicateHandler`]
    /// at [`Self::build`] time.
    #[must_use]
    pub fn register_command_handler<C, H>(self, _handler: H) -> Self
    where
        C: Command,
        H: CommandHandler<C>,
    {
        todo!()
    }

    /// Registers the single [`QueryHandler`] responsible for query `Q`.
    ///
    /// Calling this twice for the same `Q` produces a [`MediatorBuildError::DuplicateHandler`]
    /// at [`Self::build`] time.
    #[must_use]
    pub fn register_query_handler<Q, H>(self, _handler: H) -> Self
    where
        Q: Query,
        H: QueryHandler<Q>,
    {
        todo!()
    }

    /// Registers one of possibly many [`NotificationHandler`]s for `N`.
    ///
    /// Notification dispatch fans out to every handler registered for `N` in
    /// registration order.
    #[must_use]
    pub fn register_notification_handler<N, H>(self, _handler: H) -> Self
    where
        N: Notification,
        H: NotificationHandler<N>,
    {
        todo!()
    }

    /// Appends a [`Middleware`] to the dispatch pipeline. Middlewares are
    /// invoked in the order they are added, around every handler invocation.
    #[must_use]
    pub fn with_middleware<M: Middleware>(self, _middleware: M) -> Self {
        todo!()
    }

    /// Consumes the builder and produces an immutable, ready-to-use
    /// [`Mediator`].
    pub fn build(self) -> Result<Mediator, MediatorBuildError> {
        todo!()
    }
}

impl Default for MediatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// In-process dispatcher for commands, queries and notifications.
///
/// Construct one with [`MediatorBuilder`], clone it freely (the registry is
/// shared behind an [`std::sync::Arc`]), and call [`Mediator::send`],
/// [`Mediator::query`] or [`Mediator::publish`] from anywhere in your async
/// runtime.
#[derive(Clone)]
pub struct Mediator {
    _private: (),
}

impl Mediator {
    /// Dispatches a [`Command`] to its registered handler and returns the
    /// handler's output.
    ///
    /// Returns [`HexeractError`] if no handler is registered for `C`, or if
    /// the handler itself fails.
    #[expect(
        clippy::unused_async,
        reason = "skeleton stub awaiting dispatch implementation"
    )]
    pub async fn send<C: Command>(&self, _command: C) -> Result<C::Output, HexeractError> {
        todo!()
    }

    /// Dispatches a [`Query`] to its registered handler and returns the
    /// handler's output.
    ///
    /// Returns [`HexeractError`] if no handler is registered for `Q`, or if
    /// the handler itself fails.
    #[expect(
        clippy::unused_async,
        reason = "skeleton stub awaiting dispatch implementation"
    )]
    pub async fn query<Q: Query>(&self, _query: Q) -> Result<Q::Output, HexeractError> {
        todo!()
    }

    /// Publishes a [`Notification`] to every handler registered for `N`, in
    /// registration order. A notification with zero handlers is a no-op.
    ///
    /// Every handler is invoked even if a previous one failed; the returned
    /// `Result` aggregates failures into a single [`HexeractError`].
    #[expect(
        clippy::unused_async,
        reason = "skeleton stub awaiting dispatch implementation"
    )]
    pub async fn publish<N: Notification>(&self, _notification: N) -> Result<(), HexeractError> {
        todo!()
    }
}
