/// Marker trait for messages announcing that something happened, with fan-out
/// semantics: zero or more handlers may react to the same notification.
///
/// Unlike [`crate::Command`] and [`crate::Query`], a notification has no
/// output and is not expected to be answered. It is broadcast to every
/// handler registered for its type.
///
/// `Clone` is required because the mediator delivers an independent copy of
/// the value to each registered handler.
///
/// # Example
///
/// ```
/// use hexeract_core::Notification;
///
/// #[derive(Clone)]
/// struct OrderShipped {
///     pub order_id: uuid::Uuid,
/// }
///
/// impl Notification for OrderShipped {}
/// ```
pub trait Notification: Send + Sync + Clone + 'static {}
