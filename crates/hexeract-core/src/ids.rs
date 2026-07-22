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

/// Unique identity of a single request-reply call.
///
/// Distinct from [`CorrelationId`]: a correlation identifier labels a whole
/// causal chain and is shared by every message in it, whereas a `RequestId`
/// identifies exactly one in-flight call and keys the caller's pending-reply
/// slot. Two concurrent calls issued from the same handler share their
/// correlation and never their request identity.
///
/// Minted as a `UUIDv7` so identifiers sort chronologically in logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestId(Uuid);

impl RequestId {
    /// Creates a new time-ordered [`RequestId`].
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Returns the inner [`Uuid`].
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for RequestId {
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

#[cfg(test)]
mod request_id_tests {
    use super::RequestId;
    use uuid::Uuid;

    #[test]
    fn two_request_ids_are_distinct() {
        assert_ne!(RequestId::new(), RequestId::new());
    }

    #[test]
    fn request_id_is_a_uuid_v7() {
        let request_id = RequestId::new();
        assert_eq!(request_id.as_uuid().get_version_num(), 7);
    }

    #[test]
    fn request_id_round_trips_through_uuid() {
        let uuid = Uuid::now_v7();
        let request_id = RequestId::from(uuid);
        assert_eq!(request_id.as_uuid(), &uuid);
        assert_eq!(request_id.to_string(), uuid.to_string());
    }
}
