/// Marker trait for messages announcing that something happened, with fan-out
/// semantics: zero or more handlers may react to the same notification.
///
/// Unlike [`crate::Command`] and [`crate::Query`], a notification has no
/// output and is not expected to be answered. It is broadcast to every
/// handler registered for its type.
///
/// The mediator shares a single `Arc<N>` across every handler, so the value is
/// never deep-cloned per handler and `Notification` does not require `Clone`.
/// A handler that needs an owned copy can clone out of the `Arc` itself.
///
/// # Example
///
/// ```
/// use hexeract_core::Notification;
///
/// struct OrderShipped {
///     pub order_id: uuid::Uuid,
/// }
///
/// impl Notification for OrderShipped {}
/// ```
pub trait Notification: Send + Sync + 'static {}
