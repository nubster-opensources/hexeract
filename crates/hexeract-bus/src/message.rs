use serde::Serialize;
use serde::de::DeserializeOwned;

/// Marker trait for a domain message that flows through the bus.
///
/// Implementors are serialized to JSON before publication and
/// deserialized back when a consumer dispatches the message to its
/// handler.
///
/// # Convention
///
/// Choose a stable, kebab-case identifier scoped by bounded context for
/// [`Message::MESSAGE_TYPE`], for example `"orders.placed"` or
/// `"users.registered"`. Changing this value after consumers have been
/// deployed breaks dispatch on the consumer side.
///
/// # Example
///
/// ```
/// use hexeract_bus::Message;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct OrderPlaced {
///     order_id: uuid::Uuid,
///     amount_cents: u64,
/// }
///
/// impl Message for OrderPlaced {
///     const MESSAGE_TYPE: &'static str = "orders.placed";
/// }
///
/// assert_eq!(OrderPlaced::MESSAGE_TYPE, "orders.placed");
/// ```
pub trait Message: Send + Sync + 'static + Serialize + DeserializeOwned {
    /// Stable identifier of this message type used for routing.
    const MESSAGE_TYPE: &'static str;
}
