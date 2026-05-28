use crate::command::Command;
use crate::ids::{CorrelationId, MessageId};
use crate::notification::Notification;
use crate::query::Query;

/// Metadata carried alongside every dispatch.
///
/// The envelope exposes the message type name and identifiers without
/// requiring the middleware to know the concrete `Command` or `Query` type.
#[derive(Debug, Clone)]
pub struct MessageEnvelope {
    type_name: &'static str,
    message_id: MessageId,
    correlation_id: CorrelationId,
}

impl MessageEnvelope {
    /// Builds an envelope for a [`Command`] dispatch.
    #[must_use]
    pub fn for_command<C: Command>(message_id: MessageId, correlation_id: CorrelationId) -> Self {
        Self {
            type_name: std::any::type_name::<C>(),
            message_id,
            correlation_id,
        }
    }

    /// Builds an envelope for a [`Query`] dispatch.
    #[must_use]
    pub fn for_query<Q: Query>(message_id: MessageId, correlation_id: CorrelationId) -> Self {
        Self {
            type_name: std::any::type_name::<Q>(),
            message_id,
            correlation_id,
        }
    }

    /// Builds an envelope for a [`Notification`] dispatch. Each handler in a
    /// fan-out receives its own envelope; callers typically share the
    /// [`CorrelationId`] across the whole fan-out to preserve the causal link
    /// in traces.
    #[must_use]
    pub fn for_notification<N: Notification>(
        message_id: MessageId,
        correlation_id: CorrelationId,
    ) -> Self {
        Self {
            type_name: std::any::type_name::<N>(),
            message_id,
            correlation_id,
        }
    }

    /// The fully-qualified type name of the dispatched message.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// The unique identifier of this dispatch.
    #[must_use]
    pub fn message_id(&self) -> MessageId {
        self.message_id
    }

    /// The correlation identifier linking this dispatch to its causal chain.
    #[must_use]
    pub fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Ping;
    impl Command for Ping {
        type Output = ();
    }

    struct Pong;
    impl Query for Pong {
        type Output = ();
    }

    #[derive(Clone)]
    struct Bell;
    impl Notification for Bell {}

    #[test]
    fn for_notification_records_notification_type_name() {
        let env = MessageEnvelope::for_notification::<Bell>(MessageId::new(), CorrelationId::new());
        assert!(env.type_name().ends_with("::Bell"));
    }

    #[test]
    fn for_command_records_command_type_name() {
        let env = MessageEnvelope::for_command::<Ping>(MessageId::new(), CorrelationId::new());
        assert!(env.type_name().ends_with("::Ping"));
    }

    #[test]
    fn for_query_records_query_type_name() {
        let env = MessageEnvelope::for_query::<Pong>(MessageId::new(), CorrelationId::new());
        assert!(env.type_name().ends_with("::Pong"));
    }

    #[test]
    fn envelope_preserves_identifiers() {
        let msg = MessageId::new();
        let corr = CorrelationId::new();
        let env = MessageEnvelope::for_command::<Ping>(msg, corr);
        assert_eq!(env.message_id(), msg);
        assert_eq!(env.correlation_id(), corr);
    }

    #[test]
    fn envelope_is_clone() {
        let env = MessageEnvelope::for_command::<Ping>(MessageId::new(), CorrelationId::new());
        let cloned = env.clone();
        assert_eq!(env.message_id(), cloned.message_id());
        assert_eq!(env.type_name(), cloned.type_name());
    }
}
