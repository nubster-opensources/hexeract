use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::BusError;

/// Maximum byte length of an exchange or queue name (AMQP 0.9.1 limit).
pub const MAX_NAME_LEN: usize = 127;

/// Maximum byte length of a routing key (AMQP 0.9.1 limit).
pub const MAX_ROUTING_KEY_LEN: usize = 255;

/// Routing rule applied by an exchange when dispatching messages.
///
/// Modelled after the AMQP 0.9.1 exchange types. Additional variants
/// such as `Stream` or `Delayed` may be added in a later milestone; the
/// enum is `#[non_exhaustive]` to keep that change non-breaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExchangeKind {
    /// Routes messages to queues bound with an exact routing key match.
    Direct,
    /// Routes messages to queues whose binding pattern matches the
    /// routing key (`orders.*`, `payments.#`).
    Topic,
    /// Broadcasts messages to every bound queue and ignores the routing key.
    Fanout,
    /// Routes messages based on header matches rather than the routing key.
    Headers,
}

/// Declaration of a logical exchange that a transport can ensure exists.
///
/// Built through [`Exchange::new`], which validates the name. Use the
/// fluent setters [`Exchange::durable`] and [`Exchange::auto_delete`]
/// to tune the defaults (`durable = true`, `auto_delete = false`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Exchange {
    /// Broker-side identifier of the exchange.
    pub name: String,
    /// Routing rule applied to outbound messages.
    pub kind: ExchangeKind,
    /// Whether the exchange survives a broker restart.
    pub durable: bool,
    /// Whether the broker deletes the exchange when its last binding goes away.
    pub auto_delete: bool,
}

impl Exchange {
    /// Build a new exchange declaration after validating `name`.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::InvalidTopology`] when `name` is empty,
    /// exceeds [`MAX_NAME_LEN`] bytes or contains ASCII control
    /// characters.
    pub fn new(name: impl Into<String>, kind: ExchangeKind) -> Result<Self, BusError> {
        let name = name.into();
        validate_name(&name, "exchange name")?;
        Ok(Self {
            name,
            kind,
            durable: true,
            auto_delete: false,
        })
    }

    /// Override the [`Self::durable`] flag and return the updated declaration.
    #[must_use]
    pub fn durable(mut self, value: bool) -> Self {
        self.durable = value;
        self
    }

    /// Override the [`Self::auto_delete`] flag and return the updated declaration.
    #[must_use]
    pub fn auto_delete(mut self, value: bool) -> Self {
        self.auto_delete = value;
        self
    }
}

/// Declaration of a logical queue that a transport can ensure exists.
///
/// Built through [`Queue::new`], which validates the name. Use the
/// fluent setters [`Queue::durable`], [`Queue::exclusive`] and
/// [`Queue::auto_delete`] to tune the defaults (`durable = true`,
/// `exclusive = false`, `auto_delete = false`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Queue {
    /// Broker-side identifier of the queue.
    pub name: String,
    /// Whether the queue survives a broker restart.
    pub durable: bool,
    /// Whether the queue is reserved to a single connection.
    pub exclusive: bool,
    /// Whether the broker deletes the queue when its last consumer disconnects.
    pub auto_delete: bool,
}

impl Queue {
    /// Build a new queue declaration after validating `name`.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::InvalidTopology`] when `name` is empty,
    /// exceeds [`MAX_NAME_LEN`] bytes or contains ASCII control
    /// characters.
    pub fn new(name: impl Into<String>) -> Result<Self, BusError> {
        let name = name.into();
        validate_name(&name, "queue name")?;
        Ok(Self {
            name,
            durable: true,
            exclusive: false,
            auto_delete: false,
        })
    }

    /// Override the [`Self::durable`] flag and return the updated declaration.
    #[must_use]
    pub fn durable(mut self, value: bool) -> Self {
        self.durable = value;
        self
    }

    /// Override the [`Self::exclusive`] flag and return the updated declaration.
    #[must_use]
    pub fn exclusive(mut self, value: bool) -> Self {
        self.exclusive = value;
        self
    }

    /// Override the [`Self::auto_delete`] flag and return the updated declaration.
    #[must_use]
    pub fn auto_delete(mut self, value: bool) -> Self {
        self.auto_delete = value;
        self
    }
}

/// Routing key used to bind a queue to an exchange or to publish a message.
///
/// Validated on construction: rejects values longer than
/// [`MAX_ROUTING_KEY_LEN`] bytes and values containing ASCII control
/// characters. Empty values are allowed because fanout exchanges
/// ignore the routing key by design.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RoutingKey(String);

impl RoutingKey {
    /// Build a new routing key after validation.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::InvalidTopology`] when the value exceeds
    /// [`MAX_ROUTING_KEY_LEN`] bytes or contains ASCII control
    /// characters.
    pub fn new(value: impl Into<String>) -> Result<Self, BusError> {
        let value = value.into();
        validate_routing_key(&value)?;
        Ok(Self(value))
    }

    /// Borrow the routing key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RoutingKey {
    type Error = BusError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<RoutingKey> for String {
    fn from(value: RoutingKey) -> Self {
        value.0
    }
}

impl AsRef<str> for RoutingKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Binding from a queue to an exchange under a [`RoutingKey`].
///
/// A broker uses this declaration to dispatch messages published to
/// `exchange` and matching `routing_key` to `queue`. Built through
/// [`Binding::new`], which validates both names.
///
/// # Example
///
/// ```
/// use hexeract_bus::{Binding, Exchange, ExchangeKind, Queue, RoutingKey};
///
/// # fn main() -> Result<(), hexeract_bus::BusError> {
/// let exchange = Exchange::new("orders", ExchangeKind::Topic)?;
/// let queue = Queue::new("orders.received")?;
/// let binding = Binding::new(&queue.name, &exchange.name, RoutingKey::new("orders.*")?)?;
///
/// assert_eq!(binding.queue, "orders.received");
/// assert_eq!(binding.exchange, "orders");
/// assert_eq!(binding.routing_key.as_str(), "orders.*");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Binding {
    /// Name of the queue receiving matching messages.
    pub queue: String,
    /// Name of the exchange feeding the binding.
    pub exchange: String,
    /// Routing key the broker matches against incoming messages.
    pub routing_key: RoutingKey,
}

impl Binding {
    /// Build a new binding declaration after validating both names.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::InvalidTopology`] when either name is
    /// empty, exceeds [`MAX_NAME_LEN`] bytes or contains ASCII
    /// control characters.
    pub fn new(
        queue: impl Into<String>,
        exchange: impl Into<String>,
        routing_key: RoutingKey,
    ) -> Result<Self, BusError> {
        let queue = queue.into();
        let exchange = exchange.into();
        validate_name(&queue, "queue name")?;
        validate_name(&exchange, "exchange name")?;
        Ok(Self {
            queue,
            exchange,
            routing_key,
        })
    }
}

fn validate_name(value: &str, kind: &'static str) -> Result<(), BusError> {
    if value.is_empty() {
        return Err(BusError::InvalidTopology {
            reason: format!("{kind} cannot be empty"),
        });
    }
    if value.len() > MAX_NAME_LEN {
        return Err(BusError::InvalidTopology {
            reason: format!("{kind} `{value}` exceeds {MAX_NAME_LEN} bytes"),
        });
    }
    if value.bytes().any(|b| b.is_ascii_control()) {
        return Err(BusError::InvalidTopology {
            reason: format!("{kind} `{value}` contains control characters"),
        });
    }
    Ok(())
}

fn validate_routing_key(value: &str) -> Result<(), BusError> {
    if value.len() > MAX_ROUTING_KEY_LEN {
        return Err(BusError::InvalidTopology {
            reason: format!("routing key exceeds {MAX_ROUTING_KEY_LEN} bytes"),
        });
    }
    if value.bytes().any(|b| b.is_ascii_control()) {
        return Err(BusError::InvalidTopology {
            reason: "routing key contains control characters".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_new_sets_defaults() {
        let exchange = Exchange::new("orders", ExchangeKind::Topic).unwrap();
        assert_eq!(exchange.name, "orders");
        assert_eq!(exchange.kind, ExchangeKind::Topic);
        assert!(exchange.durable);
        assert!(!exchange.auto_delete);
    }

    #[test]
    fn exchange_setters_override_defaults() {
        let exchange = Exchange::new("orders", ExchangeKind::Direct)
            .unwrap()
            .durable(false)
            .auto_delete(true);
        assert!(!exchange.durable);
        assert!(exchange.auto_delete);
    }

    #[test]
    fn exchange_new_rejects_empty_name() {
        let error = Exchange::new("", ExchangeKind::Direct).unwrap_err();
        assert!(
            matches!(error, BusError::InvalidTopology { ref reason } if reason.contains("empty"))
        );
    }

    #[test]
    fn exchange_new_rejects_name_over_limit() {
        let too_long = "a".repeat(MAX_NAME_LEN + 1);
        let error = Exchange::new(too_long, ExchangeKind::Direct).unwrap_err();
        assert!(
            matches!(error, BusError::InvalidTopology { ref reason } if reason.contains("exceeds"))
        );
    }

    #[test]
    fn exchange_new_rejects_control_characters() {
        let error = Exchange::new("orders\x01", ExchangeKind::Direct).unwrap_err();
        assert!(
            matches!(error, BusError::InvalidTopology { ref reason } if reason.contains("control"))
        );
    }

    #[test]
    fn queue_new_sets_defaults() {
        let queue = Queue::new("orders.received").unwrap();
        assert!(queue.durable);
        assert!(!queue.exclusive);
        assert!(!queue.auto_delete);
    }

    #[test]
    fn queue_setters_override_defaults() {
        let queue = Queue::new("orders.received")
            .unwrap()
            .durable(false)
            .exclusive(true)
            .auto_delete(true);
        assert!(!queue.durable);
        assert!(queue.exclusive);
        assert!(queue.auto_delete);
    }

    #[test]
    fn queue_new_rejects_empty_name() {
        let error = Queue::new("").unwrap_err();
        assert!(matches!(error, BusError::InvalidTopology { .. }));
    }

    #[test]
    fn routing_key_accepts_valid_value() {
        let key = RoutingKey::new("orders.*").unwrap();
        assert_eq!(key.as_str(), "orders.*");
        assert_eq!(format!("{key}"), "orders.*");
    }

    #[test]
    fn routing_key_accepts_empty_for_fanout() {
        let key = RoutingKey::new("").unwrap();
        assert_eq!(key.as_str(), "");
    }

    #[test]
    fn routing_key_rejects_over_limit() {
        let too_long = "a".repeat(MAX_ROUTING_KEY_LEN + 1);
        let error = RoutingKey::new(too_long).unwrap_err();
        assert!(matches!(error, BusError::InvalidTopology { .. }));
    }

    #[test]
    fn routing_key_rejects_control_characters() {
        let error = RoutingKey::new("orders\n").unwrap_err();
        assert!(matches!(error, BusError::InvalidTopology { .. }));
    }

    #[test]
    fn binding_new_validates_both_names() {
        let key = RoutingKey::new("orders.created").unwrap();
        let binding = Binding::new("orders.received", "orders", key.clone()).unwrap();
        assert_eq!(binding.queue, "orders.received");
        assert_eq!(binding.exchange, "orders");
        assert_eq!(binding.routing_key, key);
    }

    #[test]
    fn binding_new_rejects_empty_queue_name() {
        let key = RoutingKey::new("orders.*").unwrap();
        let error = Binding::new("", "orders", key).unwrap_err();
        assert!(
            matches!(error, BusError::InvalidTopology { ref reason } if reason.contains("queue"))
        );
    }

    #[test]
    fn binding_new_rejects_empty_exchange_name() {
        let key = RoutingKey::new("orders.*").unwrap();
        let error = Binding::new("orders.received", "", key).unwrap_err();
        assert!(
            matches!(error, BusError::InvalidTopology { ref reason } if reason.contains("exchange"))
        );
    }

    #[test]
    fn exchange_round_trips_through_json() {
        let exchange = Exchange::new("orders", ExchangeKind::Topic)
            .unwrap()
            .auto_delete(true);
        let json = serde_json::to_string(&exchange).unwrap();
        let decoded: Exchange = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, exchange);
    }

    #[test]
    fn queue_round_trips_through_json() {
        let queue = Queue::new("orders.received").unwrap().exclusive(true);
        let json = serde_json::to_string(&queue).unwrap();
        let decoded: Queue = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, queue);
    }

    #[test]
    fn routing_key_round_trips_through_json() {
        let key = RoutingKey::new("orders.created").unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, "\"orders.created\"");
        let decoded: RoutingKey = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn routing_key_deserialization_runs_validation() {
        let invalid = format!("\"{}\"", "a".repeat(MAX_ROUTING_KEY_LEN + 1));
        let error = serde_json::from_str::<RoutingKey>(&invalid).unwrap_err();
        assert!(error.to_string().contains("routing key"));
    }

    #[test]
    fn binding_round_trips_through_json() {
        let key = RoutingKey::new("orders.created").unwrap();
        let binding = Binding::new("orders.received", "orders", key).unwrap();
        let json = serde_json::to_string(&binding).unwrap();
        let decoded: Binding = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, binding);
    }

    #[test]
    fn exchange_kind_serializes_as_snake_case() {
        let direct = serde_json::to_string(&ExchangeKind::Direct).unwrap();
        let topic = serde_json::to_string(&ExchangeKind::Topic).unwrap();
        assert_eq!(direct, "\"direct\"");
        assert_eq!(topic, "\"topic\"");
    }
}
