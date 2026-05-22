use uuid::Uuid;

/// Unique identifier for a single message instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MessageId(Uuid);

impl MessageId {
    /// Creates a new random [`MessageId`].
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the inner [`Uuid`].
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for MessageId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Identifier that links a chain of causally related messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CorrelationId(Uuid);

impl CorrelationId {
    /// Creates a new random [`CorrelationId`].
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the inner [`Uuid`].
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for CorrelationId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_id_new_is_unique() {
        assert_ne!(MessageId::new(), MessageId::new());
    }

    #[test]
    fn correlation_id_new_is_unique() {
        assert_ne!(CorrelationId::new(), CorrelationId::new());
    }

    #[test]
    fn message_id_display_is_uuid_format() {
        let id = MessageId::new();
        let s = id.to_string();
        assert_eq!(s.len(), 36);
        assert!(s.contains('-'));
    }

    #[test]
    fn message_id_and_correlation_id_are_distinct_types() {
        let msg = MessageId::new();
        let corr = CorrelationId::new();
        assert_ne!(msg.to_string(), corr.to_string());
    }

    #[test]
    fn from_uuid_roundtrip() {
        let uuid = Uuid::new_v4();
        let msg = MessageId::from(uuid);
        assert_eq!(msg.as_uuid(), &uuid);
    }
}
