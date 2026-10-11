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
//! fan out concurrently; every handler runs regardless of its siblings, and
//! failures are aggregated so one handler's error never hides another.
//!
//! To propagate a [`CorrelationId`] across in-process dispatches (for example
//! forwarding `ctx.correlation_id` from a command handler to a follow-up
//! notification), use [`Mediator::send_with_correlation_id`],
//! [`Mediator::query_with_correlation_id`] or
//! [`Mediator::publish_with_correlation_id`]. The plain `send` / `query` /
//! `publish` methods mint a fresh id each time.
//!
//! The `hexeract-middleware` crate ships two built-in middlewares:
//! `TracingMiddleware` and `TimeoutMiddleware`. Wire them through
//! [`MediatorBuilder::with_middleware`], or supply your own [`Middleware`]
//! implementations for other cross-cutting concerns.
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

use std::any::{Any, TypeId, type_name};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use futures_util::future::join_all;
use hexeract_core::{
    Command, CommandHandler, CorrelationId, DynMiddleware, HandlerContext, HandlerKind,
    HandlerRegistration, HexeractError, MessageEnvelope, MessageId, Middleware, Next, Notification,
    NotificationFailure, NotificationHandler, Query, QueryHandler,
};

use crate::erased::{
    BoxAny, ErasedCommandHandler, ErasedNotificationHandler, ErasedQueryHandler,
    TypedCommandHandler, TypedNotificationHandler, TypedQueryHandler,
};
use crate::terminal::{CommandTerminal, NotificationTerminal, QueryTerminal};

/// Errors raised by [`MediatorBuilder::build`] when the requested
/// configuration is inconsistent. Handler failures at dispatch time keep
/// flowing through [`HexeractError`].
///
/// Marked `#[non_exhaustive]` so that new variants can be added in minor
/// versions without breaking downstream `match` arms.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
///
/// Marked `#[non_exhaustive]` so that new variants can be added in minor
/// versions without breaking downstream `match` arms.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
    /// # Known limitations
    ///
    /// **Type-name matching is not guaranteed unique.** Matching is performed
    /// by comparing `std::any::type_name()` strings, which are not guaranteed
    /// to be unique across different crates in the same build. Two distinct
    /// types with the same short name in different crates but identical paths
    /// would be treated as the same handler. This is unlikely in practice but
    /// callers should be aware that the check is a best-effort heuristic, not a
    /// formal proof. `type_name` output is also not stable across compiler
    /// versions, so the strings should never be persisted or compared across
    /// compilation units.
    ///
    /// **`inventory::iter` is process-global.** All `#[handler]`-annotated
    /// items in the current process are visible to `inventory`, regardless of
    /// which `MediatorBuilder` instance they were intended for. In a single
    /// process that constructs two or more mediators each covering a subset of
    /// handlers, both builders will see every registered declaration from every
    /// crate. Each builder will therefore report the handlers it does not cover
    /// as `Missing`. Use this method only when a single mediator is expected to
    /// cover all declared handlers in the process.
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
    /// A fresh [`CorrelationId`] is minted for this dispatch. To continue an
    /// existing causal chain (e.g. from inside another handler), use
    /// [`Mediator::send_with_correlation_id`] instead.
    ///
    /// # Errors
    ///
    /// Returns [`HexeractError::HandlerNotFound`] if no handler is
    /// registered for `C`, or the handler's own error converted into
    /// [`HexeractError`] when the handler itself fails.
    pub async fn send<C: Command>(&self, command: C) -> Result<C::Output, HexeractError> {
        self.send_with_correlation_id(command, CorrelationId::new())
            .await
    }

    /// Dispatches a [`Command`] to its registered handler using the supplied
    /// [`CorrelationId`], preserving the causal chain across in-process
    /// dispatches.
    ///
    /// Use this variant when the caller already holds a [`CorrelationId`]
    /// (for example `ctx.correlation_id` inside a handler) and wants all
    /// follow-up messages to share the same identifier. The plain
    /// [`Mediator::send`] method is equivalent to calling this with
    /// `CorrelationId::new()`.
    ///
    /// # Errors
    ///
    /// Returns [`HexeractError::HandlerNotFound`] if no handler is
    /// registered for `C`, or the handler's own error converted into
    /// [`HexeractError`] when the handler itself fails.
    pub async fn send_with_correlation_id<C: Command>(
        &self,
        command: C,
        correlation_id: CorrelationId,
    ) -> Result<C::Output, HexeractError> {
        let tid = TypeId::of::<C>();
        let handler = self
            .inner
            .command_handlers
            .get(&tid)
            .ok_or_else(|| HexeractError::handler_not_found(type_name::<C>()))?;

        let message_id = MessageId::new();
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
    /// A fresh [`CorrelationId`] is minted for this dispatch. To continue an
    /// existing causal chain, use [`Mediator::query_with_correlation_id`]
    /// instead.
    ///
    /// # Errors
    ///
    /// Returns [`HexeractError::HandlerNotFound`] if no handler is
    /// registered for `Q`, or the handler's own error converted into
    /// [`HexeractError`] when the handler itself fails.
    pub async fn query<Q: Query>(&self, query: Q) -> Result<Q::Output, HexeractError> {
        self.query_with_correlation_id(query, CorrelationId::new())
            .await
    }

    /// Dispatches a [`Query`] to its registered handler using the supplied
    /// [`CorrelationId`], preserving the causal chain across in-process
    /// dispatches.
    ///
    /// Use this variant when the caller already holds a [`CorrelationId`]
    /// (for example `ctx.correlation_id` inside a handler) and wants all
    /// follow-up messages to share the same identifier. The plain
    /// [`Mediator::query`] method is equivalent to calling this with
    /// `CorrelationId::new()`.
    ///
    /// # Errors
    ///
    /// Returns [`HexeractError::HandlerNotFound`] if no handler is
    /// registered for `Q`, or the handler's own error converted into
    /// [`HexeractError`] when the handler itself fails.
    pub async fn query_with_correlation_id<Q: Query>(
        &self,
        query: Q,
        correlation_id: CorrelationId,
    ) -> Result<Q::Output, HexeractError> {
        let tid = TypeId::of::<Q>();
        let handler = self
            .inner
            .query_handlers
            .get(&tid)
            .ok_or_else(|| HexeractError::handler_not_found(type_name::<Q>()))?;

        let message_id = MessageId::new();
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

    /// Publishes a [`Notification`] to every handler registered for `N`.
    /// A notification with zero handlers is a no-op.
    ///
    /// A fresh [`CorrelationId`] is minted for this dispatch. To continue an
    /// existing causal chain, use [`Mediator::publish_with_correlation_id`]
    /// instead.
    ///
    /// Handlers are dispatched **concurrently**: every handler future is built
    /// up front and driven together with [`join_all`], so a slow handler no
    /// longer blocks the handlers registered after it. The fan-out is
    /// cooperative and runtime-agnostic: it runs on the caller's task without
    /// spawning, so a CPU-bound handler that never `.await`s still blocks its
    /// siblings until it yields. Every handler shares the same
    /// [`CorrelationId`] so traces can link the fan-out to its source publish
    /// call, but each handler receives a dedicated [`MessageId`].
    ///
    /// # Ordering
    ///
    /// Execution is interleaved, so the order in which handlers observe the
    /// notification is unspecified. Only the *collection* order is
    /// deterministic: the [`NotificationFailure`]s aggregated on failure
    /// appear in handler registration order.
    ///
    /// # Errors
    ///
    /// Every handler runs regardless of its siblings. If one or more fail,
    /// their typed errors are aggregated into [`HexeractError::PublishFailed`],
    /// each [`NotificationFailure`] retaining the failing handler's name and
    /// the error `source` chain so the caller can recover an individual
    /// failure instead of parsing a flattened string.
    ///
    /// # Panics in handlers
    ///
    /// A panic is isolated like any other failure. Each dispatch is wrapped
    /// in [`catch_unwind`](futures_util::FutureExt::catch_unwind) around the
    /// whole chain, middlewares and handler alike, so a panicking handler
    /// does not unwind through the fan-out and its siblings still run. The
    /// panic is recorded as a [`NotificationFailure`] carrying
    /// [`HexeractError::HandlerPanicked`], in registration order with the
    /// other failures. Its payload is the panic message when the payload is a
    /// `&str` or a `String`, and the placeholder `unknown panic payload` for
    /// any other type.
    ///
    /// Two limits remain:
    ///
    /// - The future is wrapped in [`AssertUnwindSafe`], which is an assertion
    ///   and not a proof. A middleware holding mutable state shared across
    ///   the handlers of one fan-out is outside the guarantee: a panic in the
    ///   middle of a mutation of that state can leave it inconsistent for the
    ///   handlers that run after it.
    /// - Under `panic = "abort"`, a choice made by the downstream binary,
    ///   `catch_unwind` captures nothing and the process terminates.
    pub async fn publish<N: Notification>(&self, notification: N) -> Result<(), HexeractError> {
        self.publish_with_correlation_id(notification, CorrelationId::new())
            .await
    }

    /// Publishes a [`Notification`] to every handler registered for `N` using
    /// the supplied [`CorrelationId`], preserving the causal chain across
    /// in-process dispatches.
    ///
    /// Use this variant when the caller already holds a [`CorrelationId`]
    /// (for example `ctx.correlation_id` inside a handler) and wants all
    /// follow-up messages to share the same identifier. Every handler in the
    /// fan-out receives the same supplied id and a dedicated [`MessageId`].
    /// The plain [`Mediator::publish`] method is equivalent to calling this
    /// with `CorrelationId::new()`.
    ///
    /// # Ordering
    ///
    /// See [`Mediator::publish`] for ordering and concurrency guarantees.
    ///
    /// # Errors
    ///
    /// See [`Mediator::publish`] for error aggregation semantics.
    pub async fn publish_with_correlation_id<N: Notification>(
        &self,
        notification: N,
        correlation_id: CorrelationId,
    ) -> Result<(), HexeractError> {
        let tid = TypeId::of::<N>();
        let Some(handlers) = self.inner.notification_handlers.get(&tid) else {
            return Ok(());
        };
        if handlers.is_empty() {
            return Ok(());
        }

        let total = handlers.len();

        // Shared once across the fan-out: each handler receives a cheap
        // `Arc` clone (refcount bump) rather than a deep clone of the payload.
        let shared = Arc::new(notification);

        let dispatches = handlers.iter().map(|handler| {
            let handler_name = handler.handler_type_name();
            let message_id = MessageId::new();
            let envelope = MessageEnvelope::for_notification::<N>(message_id, correlation_id);
            let ctx = HandlerContext::new(message_id, correlation_id);

            let payload = Box::new(Arc::clone(&shared)) as BoxAny;
            let terminal = Arc::new(NotificationTerminal {
                handler: Arc::clone(handler),
                payload: Mutex::new(Some(payload)),
            });
            let next = Next::new(self.inner.middlewares.clone(), terminal);

            async move {
                match AssertUnwindSafe(next.run(&envelope, &ctx))
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(_output)) => Ok(()),
                    Ok(Err(error)) => Err(NotificationFailure {
                        handler: handler_name,
                        error,
                    }),
                    Err(payload) => Err(NotificationFailure {
                        handler: handler_name,
                        error: HexeractError::handler_panicked(panic_message(&payload)),
                    }),
                }
            }
        });

        let failures: Vec<NotificationFailure> = join_all(dispatches)
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect();

        if failures.is_empty() {
            Ok(())
        } else {
            Err(HexeractError::publish_failed(
                type_name::<N>(),
                total,
                failures,
            ))
        }
    }
}

/// Recovers a human-readable message from a caught panic payload.
///
/// A panic payload is `&str` for a literal `panic!("...")` and `String` for a
/// formatted one. Anything else, such as a `panic_any` of an arbitrary type,
/// has no printable form and yields a fixed placeholder rather than a debug
/// rendering of unknown data.
///
/// This duplicates the equivalent helper in the RabbitMQ worker on purpose.
/// That one is private to its crate, and promoting a diagnostic helper into
/// the public surface of `hexeract-core` would bind eight frozen lines to
/// semver for the lifetime of the crate.
fn panic_message(payload: &Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_core::{BoxOutput, HandlerContext};

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

    struct BarrierNotifHandler {
        barrier: Arc<tokio::sync::Barrier>,
    }

    impl NotificationHandler<UserCreated> for BarrierNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            _n: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            self.barrier.wait().await;
            Ok(())
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum PersistError {
        #[error("database unavailable")]
        Unavailable,
    }

    impl From<PersistError> for HexeractError {
        fn from(err: PersistError) -> Self {
            HexeractError::handler_failed(err)
        }
    }

    struct SourcedFailingHandler;

    impl NotificationHandler<UserCreated> for SourcedFailingHandler {
        type Error = PersistError;

        async fn handle(
            &self,
            _n: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            Err(PersistError::Unavailable)
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

    #[tokio::test]
    async fn a_cloned_mediator_dispatches_through_the_same_registry() {
        let mediator = MediatorBuilder::new()
            .register_command_handler::<Ping, _>(PingHandler)
            .build()
            .expect("build must succeed");
        let clone = mediator.clone();
        drop(mediator);

        let out = clone
            .send(Ping { value: 21 })
            .await
            .expect("a clone must dispatch to the handlers of the original");
        assert_eq!(out, 42);
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
            HexeractError::HandlerNotFound { message_type, .. } if message_type.ends_with("::Ping")
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
            HexeractError::HandlerNotFound { message_type, .. } if message_type.ends_with("::GetCount")
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

        let HexeractError::PublishFailed {
            total,
            ref failures,
            ..
        } = err
        else {
            panic!("unexpected variant: {err:?}");
        };
        assert_eq!(total, 3);
        assert_eq!(failures.len(), 1);
        assert!(matches!(failures[0].error, HexeractError::Dispatch(_)));
        assert!(failures[0].error.to_string().contains("boom"));
        assert!(
            err.to_string()
                .starts_with("publish: 1 of 3 handlers failed")
        );

        let recorded = seen.lock().unwrap().clone();
        assert_eq!(
            recorded,
            vec![("first", 42), ("third", 42)],
            "siblings must run even after a failure"
        );
    }

    use futures_util::FutureExt;
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::sync::Once;

    thread_local! {
        static PANIC_OUTPUT_SILENCED: Cell<bool> = const { Cell::new(false) };
    }

    static PANIC_HOOK_INSTALLED: Once = Once::new();

    /// Silences the default panic hook for the current thread only, for as
    /// long as the guard lives.
    ///
    /// The panic hook is process-global state and tests run in parallel, so a
    /// take/set/restore dance around each panic would race with itself. The
    /// hook is instead installed once (idempotent, guarded by `Once`) and
    /// delegates to the previous hook unless the panicking thread holds a
    /// guard. The mute is thread-local: a genuine failure on any other test
    /// thread still prints its trace. `#[tokio::test]` uses a current-thread
    /// runtime, so every handler panic happens on the thread holding the guard.
    struct PanicOutputSilencer;

    impl PanicOutputSilencer {
        fn new() -> Self {
            PANIC_HOOK_INSTALLED.call_once(|| {
                let previous = std::panic::take_hook();
                std::panic::set_hook(Box::new(move |info| {
                    if !PANIC_OUTPUT_SILENCED.with(Cell::get) {
                        previous(info);
                    }
                }));
            });
            PANIC_OUTPUT_SILENCED.with(|flag| flag.set(true));
            Self
        }
    }

    impl Drop for PanicOutputSilencer {
        fn drop(&mut self) {
            PANIC_OUTPUT_SILENCED.with(|flag| flag.set(false));
        }
    }

    struct PanickingNotifHandler {
        yields_first: bool,
    }

    impl NotificationHandler<UserCreated> for PanickingNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            _n: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            if self.yields_first {
                tokio::task::yield_now().await;
            }
            panic!("boom from handler")
        }
    }

    struct LateRecordingNotifHandler {
        label: &'static str,
        yields: usize,
        seen: Arc<Mutex<Vec<(&'static str, u32)>>>,
    }

    impl NotificationHandler<UserCreated> for LateRecordingNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            notif: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            for _ in 0..self.yields {
                tokio::task::yield_now().await;
            }
            self.seen
                .lock()
                .expect("recorder mutex poisoned")
                .push((self.label, notif.id));
            Ok(())
        }
    }

    #[tokio::test]
    async fn publish_isolates_a_panicking_notification_handler() {
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(PanickingNotifHandler {
                yields_first: false,
            })
            .build()
            .expect("build must succeed");

        let outcome = {
            let _silencer = PanicOutputSilencer::new();
            AssertUnwindSafe(mediator.publish(UserCreated { id: 1 }))
                .catch_unwind()
                .await
        };

        let result = outcome.expect("publish must not unwind when a handler panics");
        assert!(
            matches!(result, Err(HexeractError::PublishFailed { total: 1, .. })),
            "unexpected result: {result:?}"
        );
    }

    #[tokio::test]
    async fn publish_lets_siblings_complete_when_a_handler_panics_before_yielding() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "first",
                seen: Arc::clone(&seen),
            })
            .register_notification_handler::<UserCreated, _>(PanickingNotifHandler {
                yields_first: false,
            })
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "third",
                seen: Arc::clone(&seen),
            })
            .build()
            .expect("build must succeed");

        let outcome = {
            let _silencer = PanicOutputSilencer::new();
            AssertUnwindSafe(mediator.publish(UserCreated { id: 5 }))
                .catch_unwind()
                .await
        };

        let recorded = seen.lock().unwrap().clone();
        assert_eq!(
            recorded,
            vec![("first", 5), ("third", 5)],
            "every sibling must observe the notification despite the panic"
        );
        assert!(
            matches!(outcome, Ok(Err(HexeractError::PublishFailed { .. }))),
            "publish must return an aggregated error, not unwind"
        );
    }

    #[tokio::test]
    async fn publish_lets_siblings_complete_when_a_handler_panics_after_yielding() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        // The panicking handler is registered first and panics on its second
        // poll. The siblings need two yields to finish, so they are still in
        // flight, already polled once, when the panic lands.
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(PanickingNotifHandler {
                yields_first: true,
            })
            .register_notification_handler::<UserCreated, _>(LateRecordingNotifHandler {
                label: "second",
                yields: 2,
                seen: Arc::clone(&seen),
            })
            .register_notification_handler::<UserCreated, _>(LateRecordingNotifHandler {
                label: "third",
                yields: 3,
                seen: Arc::clone(&seen),
            })
            .build()
            .expect("build must succeed");

        let outcome = {
            let _silencer = PanicOutputSilencer::new();
            AssertUnwindSafe(mediator.publish(UserCreated { id: 6 }))
                .catch_unwind()
                .await
        };

        let recorded = seen.lock().unwrap().clone();
        assert_eq!(
            recorded,
            vec![("second", 6), ("third", 6)],
            "in-flight siblings must complete after a late panic"
        );
        assert!(
            matches!(outcome, Ok(Err(HexeractError::PublishFailed { .. }))),
            "publish must return an aggregated error, not unwind"
        );
    }

    #[tokio::test]
    async fn publish_reports_a_panic_as_a_handler_scoped_failure() {
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(AuditHandler)
            .register_notification_handler::<UserCreated, _>(PanickingNotifHandler {
                yields_first: false,
            })
            .build()
            .expect("build must succeed");

        let outcome = {
            let _silencer = PanicOutputSilencer::new();
            AssertUnwindSafe(mediator.publish(UserCreated { id: 2 }))
                .catch_unwind()
                .await
        };
        let Ok(Err(HexeractError::PublishFailed {
            total, failures, ..
        })) = outcome
        else {
            panic!("expected an aggregated PublishFailed, publish unwound or succeeded");
        };

        assert_eq!(total, 2);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].handler.ends_with("PanickingNotifHandler"));
        let HexeractError::HandlerPanicked { ref payload, .. } = failures[0].error else {
            panic!("expected HandlerPanicked, got {:?}", failures[0].error);
        };
        assert!(
            payload.contains("boom from handler"),
            "the panic message must be preserved, got {payload:?}"
        );
    }

    /// Panics with a formatted message, whose payload is a `String` and not a
    /// `&str`. Every other panicking handler in this module uses a literal,
    /// so without this one the `String` branch of `panic_message` is never
    /// exercised and deleting it would leave the suite green.
    struct FormattedPanicNotifHandler {
        attempt: u32,
    }

    impl NotificationHandler<UserCreated> for FormattedPanicNotifHandler {
        type Error = HexeractError;

        async fn handle(
            &self,
            _n: Arc<UserCreated>,
            _ctx: &HandlerContext,
        ) -> Result<(), Self::Error> {
            panic!("boom at attempt {}", self.attempt)
        }
    }

    #[tokio::test]
    async fn publish_recovers_a_formatted_panic_message_in_full() {
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(FormattedPanicNotifHandler {
                attempt: 7,
            })
            .build()
            .expect("build must succeed");

        let outcome = {
            let _silencer = PanicOutputSilencer::new();
            AssertUnwindSafe(mediator.publish(UserCreated { id: 3 }))
                .catch_unwind()
                .await
        };
        let Ok(Err(HexeractError::PublishFailed { ref failures, .. })) = outcome else {
            panic!("expected an aggregated PublishFailed, publish unwound or succeeded");
        };

        let HexeractError::HandlerPanicked { ref payload, .. } = failures[0].error else {
            panic!("expected HandlerPanicked, got {:?}", failures[0].error);
        };
        assert_eq!(
            payload, "boom at attempt 7",
            "a formatted panic carries a `String` payload and must be recovered in full"
        );
    }

    /// Panics before delegating to `next`, so the panic happens inside the
    /// middleware chain rather than inside a handler. The rustdoc of
    /// `publish` promises that the chain is covered "middlewares and handler
    /// alike", and this is what measures that promise.
    struct PanickingMiddleware;

    impl Middleware for PanickingMiddleware {
        async fn execute(
            &self,
            _envelope: &MessageEnvelope,
            _ctx: &HandlerContext,
            _next: Next,
        ) -> Result<BoxOutput, HexeractError> {
            panic!("boom from middleware")
        }
    }

    #[tokio::test]
    async fn publish_isolates_a_panicking_middleware() {
        let mediator = MediatorBuilder::new()
            .with_middleware(PanickingMiddleware)
            .register_notification_handler::<UserCreated, _>(AuditHandler)
            .build()
            .expect("build must succeed");

        let outcome = {
            let _silencer = PanicOutputSilencer::new();
            AssertUnwindSafe(mediator.publish(UserCreated { id: 4 }))
                .catch_unwind()
                .await
        };
        let Ok(Err(HexeractError::PublishFailed {
            total,
            ref failures,
            ..
        })) = outcome
        else {
            panic!("expected an aggregated PublishFailed, publish unwound or succeeded");
        };

        assert_eq!(total, 1);
        assert_eq!(failures.len(), 1);
        let HexeractError::HandlerPanicked { ref payload, .. } = failures[0].error else {
            panic!("expected HandlerPanicked, got {:?}", failures[0].error);
        };
        assert!(
            payload.contains("boom from middleware"),
            "a panic raised inside the middleware chain must be caught too, got {payload:?}"
        );
    }

    #[tokio::test]
    async fn publish_keeps_registration_order_when_a_panic_and_a_typed_error_mix() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(FailingNotifHandler)
            .register_notification_handler::<UserCreated, _>(PanickingNotifHandler {
                yields_first: true,
            })
            .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
                label: "third",
                seen: Arc::clone(&seen),
            })
            .build()
            .expect("build must succeed");

        let outcome = {
            let _silencer = PanicOutputSilencer::new();
            AssertUnwindSafe(mediator.publish(UserCreated { id: 3 }))
                .catch_unwind()
                .await
        };
        let Ok(Err(HexeractError::PublishFailed {
            total, failures, ..
        })) = outcome
        else {
            panic!("expected an aggregated PublishFailed, publish unwound or succeeded");
        };

        assert_eq!(total, 3);
        assert_eq!(failures.len(), 2);
        assert!(failures[0].handler.ends_with("FailingNotifHandler"));
        assert!(matches!(failures[0].error, HexeractError::Dispatch(_)));
        assert!(failures[1].handler.ends_with("PanickingNotifHandler"));
        assert!(matches!(
            failures[1].error,
            HexeractError::HandlerPanicked { .. }
        ));
        assert_eq!(seen.lock().unwrap().clone(), vec![("third", 3)]);
    }

    #[test]
    fn panic_message_falls_back_for_an_unknown_payload() {
        let payload = {
            let _silencer = PanicOutputSilencer::new();
            std::panic::catch_unwind(|| std::panic::panic_any(42u8))
                .expect_err("panic_any must unwind")
        };

        assert_eq!(panic_message(&payload), "unknown panic payload");
    }

    struct PanickingPingHandler;

    impl CommandHandler<Ping> for PanickingPingHandler {
        type Error = HexeractError;

        async fn handle(&self, _cmd: Ping, _ctx: &HandlerContext) -> Result<u32, Self::Error> {
            panic!("boom from command handler")
        }
    }

    struct PanickingCountHandler;

    impl QueryHandler<GetCount> for PanickingCountHandler {
        type Error = HexeractError;

        async fn handle(&self, _q: GetCount, _ctx: &HandlerContext) -> Result<u32, Self::Error> {
            panic!("boom from query handler")
        }
    }

    #[tokio::test]
    async fn send_and_query_panic_behaviour_is_measured_not_assumed() {
        let mediator = MediatorBuilder::new()
            .register_command_handler::<Ping, _>(PanickingPingHandler)
            .register_query_handler::<GetCount, _>(PanickingCountHandler)
            .build()
            .expect("build must succeed");

        let (send_outcome, query_outcome) = {
            let _silencer = PanicOutputSilencer::new();
            let send_outcome = AssertUnwindSafe(mediator.send(Ping { value: 1 }))
                .catch_unwind()
                .await;
            let query_outcome = AssertUnwindSafe(mediator.query(GetCount))
                .catch_unwind()
                .await;
            (send_outcome, query_outcome)
        };

        // Measured on the base tree: neither path isolates a handler panic, so
        // it unwinds straight through `await` into the caller. This freezes the
        // observation; it does not endorse it. Fixing it is a separate lot.
        let send_payload = send_outcome.expect_err("MEASURE: send was expected to unwind");
        let query_payload = query_outcome.expect_err("MEASURE: query was expected to unwind");
        assert_eq!(
            send_payload.downcast_ref::<&str>().copied(),
            Some("boom from command handler")
        );
        assert_eq!(
            query_payload.downcast_ref::<&str>().copied(),
            Some("boom from query handler")
        );
    }

    #[tokio::test]
    async fn publish_runs_handlers_concurrently() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(BarrierNotifHandler {
                barrier: Arc::clone(&barrier),
            })
            .register_notification_handler::<UserCreated, _>(BarrierNotifHandler {
                barrier: Arc::clone(&barrier),
            })
            .build()
            .expect("build must succeed");

        // Each handler parks on the shared barrier and only completes once the
        // other has arrived. Sequential dispatch would park the first handler
        // forever and trip the timeout; concurrent fan-out lets both arrive.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            mediator.publish(UserCreated { id: 1 }),
        )
        .await
        .expect("handlers must run concurrently, not sequentially")
        .expect("publish must succeed");
    }

    #[tokio::test]
    async fn publish_aggregates_typed_failures_with_handler_names() {
        let mediator = MediatorBuilder::new()
            .register_notification_handler::<UserCreated, _>(SourcedFailingHandler)
            .register_notification_handler::<UserCreated, _>(AuditHandler)
            .register_notification_handler::<UserCreated, _>(FailingNotifHandler)
            .build()
            .expect("build must succeed");

        let err = mediator
            .publish(UserCreated { id: 9 })
            .await
            .expect_err("two of three handlers failed");

        let HexeractError::PublishFailed {
            notification_type,
            total,
            failures,
            ..
        } = err
        else {
            panic!("expected PublishFailed, got {err:?}");
        };

        assert!(notification_type.ends_with("::UserCreated"));
        assert_eq!(total, 3);
        assert_eq!(failures.len(), 2);

        // Failures keep registration order: the sourced failure comes first.
        assert!(failures[0].handler.ends_with("SourcedFailingHandler"));
        assert!(matches!(
            failures[0].error,
            HexeractError::HandlerFailed { .. }
        ));
        assert!(
            std::error::Error::source(&failures[0].error).is_some(),
            "the handler's typed source must survive aggregation"
        );

        assert!(failures[1].handler.ends_with("FailingNotifHandler"));
        assert!(matches!(failures[1].error, HexeractError::Dispatch(_)));
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
