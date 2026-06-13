/// The dispatch destination of a scheduled message.
///
/// The worker routes a due occurrence to the sink matching its target.
/// Marked `#[non_exhaustive]`: build instances through the constructors so
/// new targets can be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Target {
    /// Dispatch in-process through the mediator.
    Mediator,
    /// Enqueue transactionally into the outbox.
    Outbox,
    /// Publish onto the message bus under a routing key.
    Bus {
        /// Routing key the bus sink publishes the occurrence under.
        routing_key: String,
    },
}

impl Target {
    /// Target the in-process mediator.
    #[must_use]
    pub fn mediator() -> Self {
        Self::Mediator
    }

    /// Target the transactional outbox.
    #[must_use]
    pub fn outbox() -> Self {
        Self::Outbox
    }

    /// Target the message bus under `routing_key`.
    #[must_use]
    pub fn bus(routing_key: impl Into<String>) -> Self {
        Self::Bus {
            routing_key: routing_key.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Target;

    #[test]
    fn mediator_constructor_builds_mediator_variant() {
        assert_eq!(Target::mediator(), Target::Mediator);
    }

    #[test]
    fn outbox_constructor_builds_outbox_variant() {
        assert_eq!(Target::outbox(), Target::Outbox);
    }

    #[test]
    fn bus_constructor_keeps_routing_key() {
        match Target::bus("orders.placed") {
            Target::Bus { routing_key } => assert_eq!(routing_key, "orders.placed"),
            other => panic!("expected Target::Bus, got {other:?}"),
        }
    }
}
