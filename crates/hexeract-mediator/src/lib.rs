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

use std::any::{TypeId, type_name};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::Arc;

use hexeract_core::{
    Command, CommandHandler, DynMiddleware, HexeractError, Middleware, Notification,
    NotificationHandler, Query, QueryHandler,
};

use crate::erased::{
    ErasedCommandHandler, ErasedNotificationHandler, ErasedQueryHandler, TypedCommandHandler,
    TypedNotificationHandler, TypedQueryHandler,
};

/// Errors raised by [`MediatorBuilder::build`] when the requested
/// configuration is inconsistent. Handler failures at dispatch time keep
/// flowing through [`HexeractError`].
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
///     .build()?;
/// ```
pub struct MediatorBuilder {
    command_handlers: HashMap<TypeId, Arc<dyn ErasedCommandHandler>>,
    query_handlers: HashMap<TypeId, Arc<dyn ErasedQueryHandler>>,
    notification_handlers: HashMap<TypeId, Vec<Arc<dyn ErasedNotificationHandler>>>,
    middlewares: Vec<Arc<dyn DynMiddleware>>,
    errors: Vec<MediatorBuildError>,
}

impl MediatorBuilder {
    /// Creates a fresh builder with no handlers and no middlewares.
    #[must_use]
    pub fn new() -> Self {
        Self {
            command_handlers: HashMap::new(),
            query_handlers: HashMap::new(),
            notification_handlers: HashMap::new(),
            middlewares: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Registers the single [`CommandHandler`] responsible for command `C`.
    ///
    /// Calling this twice for the same `C` accumulates a
    /// [`MediatorBuildError::DuplicateHandler`] surfaced by [`Self::build`].
    #[must_use]
    pub fn register_command_handler<C, H>(mut self, handler: H) -> Self
    where
        C: Command,
        H: CommandHandler<C>,
    {
        let tid = TypeId::of::<C>();
        match self.command_handlers.entry(tid) {
            Entry::Vacant(slot) => {
                slot.insert(Arc::new(TypedCommandHandler::<C, H>::new(handler)));
            }
            Entry::Occupied(_) => {
                self.errors.push(MediatorBuildError::DuplicateHandler {
                    type_name: type_name::<C>(),
                });
            }
        }
        self
    }

    /// Registers the single [`QueryHandler`] responsible for query `Q`.
    ///
    /// Calling this twice for the same `Q` accumulates a
    /// [`MediatorBuildError::DuplicateHandler`] surfaced by [`Self::build`].
    #[must_use]
    pub fn register_query_handler<Q, H>(mut self, handler: H) -> Self
    where
        Q: Query,
        H: QueryHandler<Q>,
    {
        let tid = TypeId::of::<Q>();
        match self.query_handlers.entry(tid) {
            Entry::Vacant(slot) => {
                slot.insert(Arc::new(TypedQueryHandler::<Q, H>::new(handler)));
            }
            Entry::Occupied(_) => {
                self.errors.push(MediatorBuildError::DuplicateHandler {
                    type_name: type_name::<Q>(),
                });
            }
        }
        self
    }

    /// Registers one of possibly many [`NotificationHandler`]s for `N`.
    ///
    /// Notification dispatch fans out to every handler registered for `N`
    /// in registration order.
    #[must_use]
    pub fn register_notification_handler<N, H>(mut self, handler: H) -> Self
    where
        N: Notification,
        H: NotificationHandler<N>,
    {
        let tid = TypeId::of::<N>();
        self.notification_handlers
            .entry(tid)
            .or_default()
            .push(Arc::new(TypedNotificationHandler::<N, H>::new(handler)));
        self
    }

    /// Appends a [`Middleware`] to the dispatch pipeline. Middlewares are
    /// invoked in the order they are added, around every handler invocation.
    #[must_use]
    pub fn with_middleware<M: Middleware>(mut self, middleware: M) -> Self {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    /// Consumes the builder and produces an immutable, ready-to-use
    /// [`Mediator`].
    ///
    /// # Errors
    ///
    /// Returns the first accumulated [`MediatorBuildError`] when the
    /// configuration is inconsistent (for example a duplicate command or
    /// query handler registration).
    pub fn build(self) -> Result<Mediator, MediatorBuildError> {
        if let Some(err) = self.errors.into_iter().next() {
            return Err(err);
        }
        Ok(Mediator {
            inner: Arc::new(MediatorInner {
                command_handlers: self.command_handlers,
                query_handlers: self.query_handlers,
                notification_handlers: self.notification_handlers,
                middlewares: self.middlewares,
            }),
        })
    }
}

impl Default for MediatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MediatorBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediatorBuilder")
            .field("command_handlers", &self.command_handlers.len())
            .field("query_handlers", &self.query_handlers.len())
            .field(
                "notification_handlers",
                &self
                    .notification_handlers
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
            )
            .field("middlewares", &self.middlewares.len())
            .field("errors", &self.errors.len())
            .finish()
    }
}

/// In-process dispatcher for commands, queries and notifications.
///
/// Construct one with [`MediatorBuilder`], clone it freely (the registry is
/// shared behind an [`Arc`]), and call [`Mediator::send`], [`Mediator::query`]
/// or [`Mediator::publish`] from anywhere in your async runtime.
#[derive(Clone)]
pub struct Mediator {
    inner: Arc<MediatorInner>,
}

impl fmt::Debug for Mediator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mediator")
            .field("command_handlers", &self.inner.command_handlers.len())
            .field("query_handlers", &self.inner.query_handlers.len())
            .field(
                "notification_handlers",
                &self
                    .inner
                    .notification_handlers
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
            )
            .field("middlewares", &self.inner.middlewares.len())
            .finish()
    }
}

#[allow(
    dead_code,
    reason = "fields consumed by send, query and publish in subsequent commits"
)]
struct MediatorInner {
    command_handlers: HashMap<TypeId, Arc<dyn ErasedCommandHandler>>,
    query_handlers: HashMap<TypeId, Arc<dyn ErasedQueryHandler>>,
    notification_handlers: HashMap<TypeId, Vec<Arc<dyn ErasedNotificationHandler>>>,
    middlewares: Vec<Arc<dyn DynMiddleware>>,
}

impl Mediator {
    /// Dispatches a [`Command`] to its registered handler and returns the
    /// handler's output.
    ///
    /// # Errors
    ///
    /// Returns [`HexeractError::HandlerNotFound`] if no handler is
    /// registered for `C`, or the handler's own error converted into
    /// [`HexeractError`] when the handler itself fails.
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
    /// # Errors
    ///
    /// Returns [`HexeractError::HandlerNotFound`] if no handler is
    /// registered for `Q`, or the handler's own error converted into
    /// [`HexeractError`] when the handler itself fails.
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
    /// # Errors
    ///
    /// Every handler is invoked even if a previous one failed; failures
    /// are aggregated into a single [`HexeractError::Dispatch`] message.
    #[expect(
        clippy::unused_async,
        reason = "skeleton stub awaiting dispatch implementation"
    )]
    pub async fn publish<N: Notification>(&self, _notification: N) -> Result<(), HexeractError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::HandlerContext;

    struct Ping;

    impl Command for Ping {
        type Output = ();
    }

    struct PingHandler;

    impl CommandHandler<Ping> for PingHandler {
        type Error = HexeractError;

        async fn handle(&self, _cmd: Ping, _ctx: &HandlerContext) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct GetCount;

    impl Query for GetCount {
        type Output = u32;
    }

    struct CountHandler;

    impl QueryHandler<GetCount> for CountHandler {
        type Error = HexeractError;

        async fn handle(&self, _q: GetCount, _ctx: &HandlerContext) -> Result<u32, Self::Error> {
            Ok(0)
        }
    }

    #[derive(Clone)]
    struct UserCreated;

    impl Notification for UserCreated {}

    struct AuditHandler;

    impl NotificationHandler<UserCreated> for AuditHandler {
        type Error = HexeractError;

        async fn handle(&self, _n: UserCreated, _ctx: &HandlerContext) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn default_builder_is_empty() {
        let builder = MediatorBuilder::default();
        assert!(builder.command_handlers.is_empty());
        assert!(builder.query_handlers.is_empty());
        assert!(builder.notification_handlers.is_empty());
        assert!(builder.middlewares.is_empty());
        assert!(builder.errors.is_empty());
    }

    #[test]
    fn registers_one_command_handler_then_builds_ok() {
        let mediator = MediatorBuilder::new()
            .register_command_handler::<Ping, _>(PingHandler)
            .build()
            .expect("build must succeed");
        let _clone = mediator.clone();
    }

    #[test]
    fn detects_duplicate_command_handler() {
        let err = MediatorBuilder::new()
            .register_command_handler::<Ping, _>(PingHandler)
            .register_command_handler::<Ping, _>(PingHandler)
            .build()
            .expect_err("second registration must fail at build");
        let MediatorBuildError::DuplicateHandler { type_name } = err;
        assert!(type_name.ends_with("::Ping"));
    }

    #[test]
    fn detects_duplicate_query_handler() {
        let err = MediatorBuilder::new()
            .register_query_handler::<GetCount, _>(CountHandler)
            .register_query_handler::<GetCount, _>(CountHandler)
            .build()
            .expect_err("second registration must fail at build");
        let MediatorBuildError::DuplicateHandler { type_name } = err;
        assert!(type_name.ends_with("::GetCount"));
    }

    #[test]
    fn allows_multiple_notification_handlers_for_same_type() {
        let builder = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(AuditHandler)
            .register_notification_handler::<UserCreated, _>(AuditHandler)
            .register_notification_handler::<UserCreated, _>(AuditHandler);
        let tid = TypeId::of::<UserCreated>();
        assert_eq!(builder.notification_handlers[&tid].len(), 3);
        let mediator = builder.build().expect("notifications must not collide");
        assert_eq!(
            mediator.inner.notification_handlers[&TypeId::of::<UserCreated>()].len(),
            3
        );
    }

    #[test]
    fn mediator_is_clone_and_shares_registry() {
        let mediator = MediatorBuilder::new()
            .register_command_handler::<Ping, _>(PingHandler)
            .build()
            .expect("build must succeed");
        let clone = mediator.clone();
        assert!(Arc::ptr_eq(&mediator.inner, &clone.inner));
    }
}
