//! Handler registration metadata collected at link time by the `#[handler]`
//! procedural macro from `hexeract-macros`.
//!
//! Each expansion of `#[handler]` emits an
//! [`inventory::submit!`] producing a [`HandlerRegistration`] entry. The
//! Hexeract mediator iterates the collected entries through
//! `inventory::iter::<HandlerRegistration>` (see
//! `MediatorBuilder::verify_handlers`) to detect handlers that were
//! declared with the macro but never wired into the registry.
//!
//! The macro does not auto-populate the registry; handlers must still be
//! registered explicitly via the fluent builder so that stateful handlers
//! (database pools, configuration) can be constructed by the caller.

use inventory;

/// Kind of handler described by a [`HandlerRegistration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandlerKind {
    /// `CommandHandler<C>` registration.
    Command,
    /// `QueryHandler<Q>` registration.
    Query,
    /// `NotificationHandler<N>` registration.
    Notification,
}

/// One handler discovered at link time by the `#[handler]` macro.
///
/// `message_type_name` and `handler_type_name` are the
/// fully-qualified type names captured at macro expansion through
/// [`core::any::type_name`].
#[derive(Debug, Clone, Copy)]
pub struct HandlerRegistration {
    /// Kind of handler.
    pub kind: HandlerKind,
    /// Fully-qualified type name of the message type.
    pub message_type_name: &'static str,
    /// Fully-qualified type name of the handler type.
    pub handler_type_name: &'static str,
}

inventory::collect!(HandlerRegistration);

#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

#[cfg(test)]
mod tests {
    use super::*;

    inventory::submit!(HandlerRegistration {
        kind: HandlerKind::Command,
        message_type_name: "hexeract_core::registration::tests::TestCmd",
        handler_type_name: "hexeract_core::registration::tests::TestHandler",
    });

    #[test]
    fn inventory_collects_registrations_submitted_in_tests() {
        let found = inventory::iter::<HandlerRegistration>
            .into_iter()
            .any(|r| r.message_type_name.ends_with("::TestCmd"));
        assert!(
            found,
            "registration submitted from the tests module must be visible to inventory::iter",
        );
    }

    #[test]
    fn handler_kind_is_copyable_and_comparable() {
        let a = HandlerKind::Command;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(HandlerKind::Command, HandlerKind::Query);
    }
}
