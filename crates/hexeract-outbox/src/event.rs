use serde::Serialize;
use serde::de::DeserializeOwned;

/// Marker trait for a domain event that can flow through the outbox.
///
/// Implementors are serialized to JSON when persisted and deserialized
/// back when dispatched to a handler.
///
/// # Convention
///
/// Pick a stable, kebab-case identifier scoped by bounded context for
/// [`Event::EVENT_TYPE`], for example `"users.registered"` or
/// `"orders.placed"`. Changing this value after rows have been persisted
/// breaks dispatch.
///
/// # Example
///
/// ```
/// use hexeract_outbox::Event;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct UserRegistered {
///     user_id: uuid::Uuid,
/// }
///
/// impl Event for UserRegistered {
///     const EVENT_TYPE: &'static str = "users.registered";
/// }
///
/// assert_eq!(UserRegistered::EVENT_TYPE, "users.registered");
/// ```
pub trait Event: Send + Sync + 'static + Serialize + DeserializeOwned {
    /// Stable identifier of this event type used for storage and routing.
    const EVENT_TYPE: &'static str;
}
