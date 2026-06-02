//! In-process mediator for the Hexeract messaging framework.
//!
//! The mediator dispatches a [`Command`], a [`Query`] or a [`Notification`]
//! to the handlers registered with a [`MediatorBuilder`] at startup. Dispatch
//! is type-safe and reflection-free: each call to
//! [`Mediator::send`], [`Mediator::query`] or [`Mediator::publish`] resolves
//! to the matching handler through a compile-time generic, while the internal
//! registry erases the handler types behind a `TypeId` lookup table.
//!
//! Commands and queries are single-handler: registering a second handler for
//! the same type is a build-time error. Notifications are multi-handler and
//! fan out in registration order; failures are aggregated so siblings keep
//! running even when one handler returns an error.
//!
//! The three built-in middlewares (`TracingMiddleware`, `LoggingMiddleware`,
//! `TimeoutMiddleware`) ship in a follow-up release; users can still wire
//! their own [`Middleware`] implementations through
//! [`MediatorBuilder::with_middleware`] in the meantime.
//!
//! # Example
//!
//! ```
//! use hexeract_core::{Command, CommandHandler, HandlerContext, HexeractError};
//! use hexeract_mediator::MediatorBuilder;
//!
//! struct Greet { name: String }
//!
//! impl Command for Greet {
//!     type Output = String;
//! }
//!
//! struct GreetHandler;
//!
//! impl CommandHandler<Greet> for GreetHandler {
//!     type Error = HexeractError;
//!     async fn handle(&self, cmd: Greet, _ctx: &HandlerContext)
//!         -> Result<String, Self::Error>
//!     {
//!         Ok(format!("hello {}", cmd.name))
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mediator = MediatorBuilder::new()
//!     .register_command_handler::<Greet, _>(GreetHandler)
//!     .build()?;
//!
//! let greeting = mediator.send(Greet { name: "world".into() }).await?;
//! assert_eq!(greeting, "hello world");
//! # Ok(()) }
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

mod erased;
mod terminal;

use std::any::{TypeId, type_name};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use hexeract_core::{
    Command, CommandHandler, CorrelationId, DynMiddleware, HandlerContext, HandlerKind,
    HandlerRegistration, HexeractError, MessageEnvelope, MessageId, Middleware, Next, Notification,
    NotificationHandler, Query, QueryHandler,
};

use crate::erased::{
    BoxAny, ErasedCommandHandler, ErasedNotificationHandler, ErasedQueryHandler,
    TypedCommandHandler, TypedNotificationHandler, TypedQueryHandler,
};
use crate::terminal::{CommandTerminal, NotificationTerminal, QueryTerminal};

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

/// One handler that was declared via the `#[handler]` attribute macro but
/// never registered through [`MediatorBuilder`].
#[derive(Debug, Clone)]
pub struct MissingHandler {
    /// Kind of handler that was expected.
    pub kind: HandlerKind,
    /// Fully-qualified type name of the message type.
    pub message_type_name: &'static str,
    /// Fully-qualified type name of the handler type.
    pub handler_type_name: &'static str,
}

/// Errors raised by [`MediatorBuilder::verify_handlers`].
#[derive(Debug, thiserror::Error)]
pub enum HandlersVerificationError {
    /// One or more handlers declared via the `#[handler]` macro were not
    /// registered through the fluent builder.
    #[error("{} handler(s) declared via #[handler] are missing from the registry", missing.len())]
    Missing {
        /// List of missing handlers, in inventory iteration order.
        missing: Vec<MissingHandler>,
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
    registered_command_types: HashSet<&'static str>,
    registered_query_types: HashSet<&'static str>,
    registered_notification_types: HashSet<&'static str>,
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
            registered_command_types: HashSet::new(),
            registered_query_types: HashSet::new(),
            registered_notification_types: HashSet::new(),
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
                self.registered_command_types.insert(type_name::<C>());
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
                self.registered_query_types.insert(type_name::<Q>());
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
        self.registered_notification_types.insert(type_name::<N>());
        self
    }

    /// Appends a [`Middleware`] to the dispatch pipeline. Middlewares are
    /// invoked in the order they are added, around every handler invocation.
    #[must_use]
    pub fn with_middleware<M: Middleware>(mut self, middleware: M) -> Self {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    /// Verifies that every handler declared with the `#[handler]` attribute
    /// macro from `hexeract-macros` was also registered through the fluent
    /// builder.
    ///
    /// The macro emits a [`HandlerRegistration`] for every annotated item
    /// via [`inventory`]; this method iterates the collected entries and
    /// returns the set of declared-but-not-registered handlers. The check
    /// is a sanity guard for typos and forgotten wirings; it does not
    /// auto-populate the registry, since stateful handlers cannot be
    /// constructed from metadata alone.
    ///
    /// # Ordering
    ///
    /// Call this method on the builder before [`Self::build`], which
    /// consumes the builder. It is safe to call multiple times: the
    /// method takes `&self` and does not mutate state.
    ///
    /// # Errors
    ///
    /// Returns [`HandlersVerificationError::Missing`] listing the handlers
    /// that are visible to `inventory` but not present in the registry.
    pub fn verify_handlers(&self) -> Result<(), HandlersVerificationError> {
        let mut missing = Vec::new();
        for reg in inventory::iter::<HandlerRegistration> {
            let message_type_name = (reg.message_type_name)();
            let present = match reg.kind {
                HandlerKind::Command => self.registered_command_types.contains(message_type_name),
                HandlerKind::Query => self.registered_query_types.contains(message_type_name),
                HandlerKind::Notification => self
                    .registered_notification_types
                    .contains(message_type_name),
            };
            if !present {
                missing.push(MissingHandler {
                    kind: reg.kind,
                    message_type_name,
                    handler_type_name: (reg.handler_type_name)(),
                });
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            Err(HandlersVerificationError::Missing { missing })
        }
    }

    /// Consumes the builder and produces an immutable, ready-to-use
    /// [`Mediator`].
    ///
    /// # Errors
    ///
    /// Returns the first accumulated [`MediatorBuildError`] when the
    /// configuration is inconsistent (for example a duplicate command or
    /// query handler registration). Only the first error is surfaced: if
    /// several invalid registrations were performed, fix the reported one
    /// and call [`Self::build`] again to see the next.
    pub fn build(self) -> Result<Mediator, MediatorBuildError> {
        if let Some(err) = self.errors.into_iter().next() {
            return Err(err);
        }
        Ok(Mediator {
            inner: Arc::new(MediatorInner {
                command_handlers: self.command_handlers,
                query_handlers: self.query_handlers,
                notification_handlers: self.notification_handlers,
                middlewares: self.middlewares.into(),
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
            .finish_non_exhaustive()
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

struct MediatorInner {
    command_handlers: HashMap<TypeId, Arc<dyn ErasedCommandHandler>>,
    query_handlers: HashMap<TypeId, Arc<dyn ErasedQueryHandler>>,
    notification_handlers: HashMap<TypeId, Vec<Arc<dyn ErasedNotificationHandler>>>,
    middlewares: Arc<[Arc<dyn DynMiddleware>]>,
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
    pub async fn send<C: Command>(&self, command: C) -> Result<C::Output, HexeractError> {
        let tid = TypeId::of::<C>();
        let handler = self.inner.command_handlers.get(&tid).ok_or_else(|| {
            HexeractError::HandlerNotFound {
                message_type: type_name::<C>(),
            }
        })?;

        let message_id = MessageId::new();
        let correlation_id = CorrelationId::new();
        let envelope = MessageEnvelope::for_command::<C>(message_id, correlation_id);
        let ctx = HandlerContext::new(message_id, correlation_id);

        let terminal = Arc::new(CommandTerminal {
            handler: Arc::clone(handler),
            payload: Mutex::new(Some(Box::new(command) as BoxAny)),
        });

        let next = Next::new(self.inner.middlewares.clone(), terminal);
        let output = next.run(&envelope, &ctx).await?;

        output
            .downcast::<C::Output>()
            .map(|boxed| *boxed)
            .map_err(|_| HexeractError::downcast_failed(type_name::<C::Output>()))
    }

    /// Dispatches a [`Query`] to its registered handler and returns the
    /// handler's output.
    ///
    /// # Errors
    ///
    /// Returns [`HexeractError::HandlerNotFound`] if no handler is
    /// registered for `Q`, or the handler's own error converted into
    /// [`HexeractError`] when the handler itself fails.
    pub async fn query<Q: Query>(&self, query: Q) -> Result<Q::Output, HexeractError> {
        let tid = TypeId::of::<Q>();
        let handler =
            self.inner
                .query_handlers
                .get(&tid)
                .ok_or_else(|| HexeractError::HandlerNotFound {
                    message_type: type_name::<Q>(),
                })?;

        let message_id = MessageId::new();
        let correlation_id = CorrelationId::new();
        let envelope = MessageEnvelope::for_query::<Q>(message_id, correlation_id);
        let ctx = HandlerContext::new(message_id, correlation_id);

        let terminal = Arc::new(QueryTerminal {
            handler: Arc::clone(handler),
            payload: Mutex::new(Some(Box::new(query) as BoxAny)),
        });

        let next = Next::new(self.inner.middlewares.clone(), terminal);
        let output = next.run(&envelope, &ctx).await?;

        output
            .downcast::<Q::Output>()
            .map(|boxed| *boxed)
            .map_err(|_| HexeractError::downcast_failed(type_name::<Q::Output>()))
    }

    /// Publishes a [`Notification`] to every handler registered for `N`, in
    /// registration order. A notification with zero handlers is a no-op.
    ///
    /// Every handler shares the same [`CorrelationId`] so traces can link
    /// the fan-out to its source publish call, but each handler receives a
    /// dedicated [`MessageId`].
    ///
    /// # Errors
    ///
    /// Every handler is invoked even if a previous one failed; failures are
    /// aggregated into a single [`HexeractError::Dispatch`] message of the
    /// form `"publish: N of M handlers failed: ..."`.
    pub async fn publish<N: Notification>(&self, notification: N) -> Result<(), HexeractError> {
        let tid = TypeId::of::<N>();
        let Some(handlers) = self.inner.notification_handlers.get(&tid) else {
            return Ok(());
        };
        if handlers.is_empty() {
            return Ok(());
        }

        let correlation_id = CorrelationId::new();
        let total = handlers.len();
        let mut failures: Vec<String> = Vec::new();

        // Shared once across the fan-out: each handler receives a cheap
        // `Arc` clone (refcount bump) rather than a deep clone of the payload.
        let shared = Arc::new(notification);

        for handler in handlers {
            let message_id = MessageId::new();
            let envelope = MessageEnvelope::for_notification::<N>(message_id, correlation_id);
            let ctx = HandlerContext::new(message_id, correlation_id);

            let payload = Box::new(Arc::clone(&shared)) as BoxAny;
            let terminal = Arc::new(NotificationTerminal {
                handler: Arc::clone(handler),
                payload: Mutex::new(Some(payload)),
            });

            let next = Next::new(self.inner.middlewares.clone(), terminal);
            if let Err(err) = next.run(&envelope, &ctx).await {
                failures.push(err.to_string());
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(HexeractError::Dispatch(format!(
                "publish: {} of {} handlers failed: {}",
                failures.len(),
                total,
                failures.join("; ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::HandlerContext;

    struct Ping {
        value: u32,
    }

    impl Command for Ping {
        type Output = u32;
    }

    struct PingHandler;

    impl CommandHandler<Ping> for PingHandler {
        type Error = HexeractError;

        async fn handle(&self, cmd: Ping, _ctx: &HandlerContext) -> Result<u32, Self::Error> {
            Ok(cmd.value * 2)
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
            Ok(99)
        }
    }

    #[derive(Clone)]
    struct UserCreated {
        id: u32,
    }

    impl Notification for UserCreated {}

    struct AuditHandler;

    impl NotificationHandler<UserCreated> for AuditHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            _n: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct RecordingNotifHandler {
        label: &'static str,
        seen: Arc<Mutex<Vec<(&'static str, u32)>>>,
    }

    impl NotificationHandler<UserCreated> for RecordingNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            notif: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.seen
                .lock()
                .expect("recorder mutex poisoned")
                .push((self.label, notif.id));
            Ok(())
        }
    }

    struct FailingNotifHandler;

    impl NotificationHandler<UserCreated> for FailingNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            _n: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Err(HexeractError::Dispatch("boom".into()))
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

    #[tokio::test]
    async fn send_routes_to_command_handler_and_returns_output() {
        let mediator = MediatorBuilder::new()
            .register_command_handler::<Ping, _>(PingHandler)
            .build()
            .expect("build must succeed");
        let out = mediator
            .send(Ping { value: 21 })
            .await
            .expect("dispatch must succeed");
        assert_eq!(out, 42);
    }

    #[tokio::test]
    async fn send_returns_handler_not_found_when_unregistered() {
        let mediator = MediatorBuilder::new().build().expect("empty build is ok");
        let err = mediator
            .send(Ping { value: 0 })
            .await
            .expect_err("missing handler must fail");
        assert!(matches!(
            err,
            HexeractError::HandlerNotFound { message_type } if message_type.ends_with("::Ping")
        ));
    }

    #[tokio::test]
    async fn query_routes_to_query_handler_and_returns_output() {
        let mediator = MediatorBuilder::new()
            .register_query_handler::<GetCount, _>(CountHandler)
            .build()
            .expect("build must succeed");
        let out = mediator.query(GetCount).await.expect("query must succeed");
        assert_eq!(out, 99);
    }

    #[tokio::test]
    async fn query_returns_handler_not_found_when_unregistered() {
        let mediator = MediatorBuilder::new().build().expect("empty build is ok");
        let err = mediator
            .query(GetCount)
            .await
            .expect_err("missing handler must fail");
        assert!(matches!(
            err,
            HexeractError::HandlerNotFound { message_type } if message_type.ends_with("::GetCount")
        ));
    }

    #[tokio::test]
    async fn publish_fans_out_to_all_notification_handlers() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "audit",
                seen: Arc::clone(&seen),
            })
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "email",
                seen: Arc::clone(&seen),
            })
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "search",
                seen: Arc::clone(&seen),
            })
            .build()
            .expect("build must succeed");

        mediator
            .publish(UserCreated { id: 7 })
            .await
            .expect("publish must succeed");

        let recorded = seen.lock().unwrap().clone();
        assert_eq!(
            recorded,
            vec![("audit", 7), ("email", 7), ("search", 7)],
            "every handler must observe the notification once, in registration order"
        );
    }

    #[tokio::test]
    async fn publish_with_no_handlers_is_ok() {
        let mediator = MediatorBuilder::new().build().expect("empty build is ok");
        mediator
            .publish(UserCreated { id: 1 })
            .await
            .expect("publish with zero handlers must succeed");
    }

    #[tokio::test]
    async fn publish_invokes_all_handlers_even_when_one_fails() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "first",
                seen: Arc::clone(&seen),
            })
            .register_notification_handler::<UserCreated, _>(FailingNotifHandler)
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "third",
                seen: Arc::clone(&seen),
            })
            .build()
            .expect("build must succeed");

        let err = mediator
            .publish(UserCreated { id: 42 })
            .await
            .expect_err("at least one handler failed");

        match err {
            HexeractError::Dispatch(msg) => {
                assert!(msg.starts_with("publish: 1 of 3 handlers failed"));
                assert!(msg.contains("boom"));
            }
            other => panic!("unexpected variant: {other:?}"),
        }

        let recorded = seen.lock().unwrap().clone();
        assert_eq!(
            recorded,
            vec![("first", 42), ("third", 42)],
            "siblings must run even after a failure"
        );
    }

    #[tokio::test]
    async fn audit_handler_stub_compiles() {
        // The `AuditHandler` fixture is kept for symmetry with prior tests.
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(AuditHandler)
            .build()
            .expect("build must succeed");
        mediator
            .publish(UserCreated { id: 0 })
            .await
            .expect("audit handler must succeed");
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

    fn verify_probe_cmd_name() -> &'static str {
        "hexeract_mediator::tests::VerifyProbeCmd"
    }

    fn verify_probe_handler_name() -> &'static str {
        "hexeract_mediator::tests::VerifyProbeHandler"
    }

    fn verify_probe_query_name() -> &'static str {
        "hexeract_mediator::tests::VerifyProbeQuery"
    }

    fn verify_probe_query_handler_name() -> &'static str {
        "hexeract_mediator::tests::VerifyProbeQueryHandler"
    }

    inventory::submit!(HandlerRegistration {
        kind: HandlerKind::Command,
        message_type_name: verify_probe_cmd_name,
        handler_type_name: verify_probe_handler_name,
    });

    inventory::submit!(HandlerRegistration {
        kind: HandlerKind::Query,
        message_type_name: verify_probe_query_name,
        handler_type_name: verify_probe_query_handler_name,
    });

    #[test]
    fn verify_handlers_reports_every_inventory_entry_when_builder_is_empty() {
        let err = MediatorBuilder::new()
            .verify_handlers()
            .expect_err("empty builder must report all inventory entries as missing");
        let HandlersVerificationError::Missing { missing } = err;
        assert!(missing.iter().any(|m| {
            m.kind == HandlerKind::Command
                && m.message_type_name == "hexeract_mediator::tests::VerifyProbeCmd"
        }));
        assert!(missing.iter().any(|m| {
            m.kind == HandlerKind::Query
                && m.message_type_name == "hexeract_mediator::tests::VerifyProbeQuery"
        }));
    }

    #[test]
    fn verify_handlers_uses_message_type_name_strings_to_match_registrations() {
        // The probe entries above name fictional types. We register handlers
        // for `Ping` and `GetCount`, whose `type_name`s do not match, so
        // verify_handlers should still report the probes as missing while
        // never complaining about Ping or GetCount themselves.
        let missing = MediatorBuilder::new()
            .register_command_handler::<Ping, _>(PingHandler)
            .register_query_handler::<GetCount, _>(CountHandler)
            .verify_handlers()
            .map_or_else(
                |HandlersVerificationError::Missing { missing }| missing,
                |()| Vec::new(),
            );
        assert!(
            missing.iter().all(|m| {
                m.message_type_name != type_name::<Ping>()
                    && m.message_type_name != type_name::<GetCount>()
            }),
            "registered handlers must not appear as missing"
        );
    }
}
