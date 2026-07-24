//! The destination a reply is published to, validated at construction.
//!
//! `reply_to` is caller-supplied protocol input and is therefore untrusted.
//! Parsing it into this type is the only way to obtain a destination, so no
//! code path can publish to a raw, unchecked string.

/// Longest AMQP 0-9-1 short string, which bounds a queue name.
///
/// Kept here as a transport-neutral upper bound rather than as an AMQP rule:
/// no known broker accepts a longer destination name.
const MAX_LENGTH: usize = 255;

/// A validated reply destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyDestination(String);

/// Why a `reply_to` value is not a usable reply destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyDestinationError {
    /// The value was empty.
    Empty,
    /// The value exceeded an AMQP short string.
    TooLong,
    /// The value contained a control character.
    IllegalCharacter,
    /// The value lies outside the namespace the backend reserves for reply
    /// inboxes. Only a backend can raise this.
    OutsideReplyNamespace,
}

impl ReplyDestination {
    /// Validate `raw` against the transport-neutral rules.
    ///
    /// A backend whose reply inboxes live in a reserved namespace adds its
    /// own rule by overriding its reply publisher's destination check; this
    /// function deliberately knows nothing about any such convention.
    ///
    /// # Errors
    ///
    /// Returns the [`ReplyDestinationError`] describing the first rule
    /// violated.
    pub fn parse(raw: &str) -> Result<Self, ReplyDestinationError> {
        if raw.is_empty() {
            return Err(ReplyDestinationError::Empty);
        }
        if raw.len() > MAX_LENGTH {
            return Err(ReplyDestinationError::TooLong);
        }
        if raw.chars().any(char::is_control) {
            return Err(ReplyDestinationError::IllegalCharacter);
        }
        Ok(Self(raw.to_owned()))
    }

    /// The validated destination.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_server_named_inbox() {
        let destination = ReplyDestination::parse("amq.gen-Xa8sK2p_QwertY").expect("valid");
        assert_eq!(destination.as_str(), "amq.gen-Xa8sK2p_QwertY");
    }

    #[test]
    fn rejects_an_empty_destination() {
        assert_eq!(
            ReplyDestination::parse(""),
            Err(ReplyDestinationError::Empty)
        );
    }

    #[test]
    fn rejects_a_destination_longer_than_an_amqp_short_string() {
        let raw = format!("amq.gen-{}", "x".repeat(255));
        assert_eq!(
            ReplyDestination::parse(&raw),
            Err(ReplyDestinationError::TooLong)
        );
    }

    #[test]
    fn rejects_control_characters() {
        assert_eq!(
            ReplyDestination::parse("amq.gen-a\nb"),
            Err(ReplyDestinationError::IllegalCharacter)
        );
    }

    #[test]
    fn accepts_any_well_formed_name_since_the_namespace_policy_lives_in_the_backend() {
        let destination = ReplyDestination::parse("orders.inbox").expect("neutral rules only");
        assert_eq!(destination.as_str(), "orders.inbox");
    }
}
