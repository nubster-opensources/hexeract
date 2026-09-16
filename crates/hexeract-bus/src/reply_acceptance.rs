//! Whether an inbound delivery is an acceptable reply for a pending slot.
//!
//! Kept free of the registry, the transport and the async runtime so the
//! protocol rules can be tested on their own. The registry calls this before
//! consuming a slot, so an invalid delivery never terminates a legitimate
//! call. Payload decoding stays on the client side: it depends on the
//! caller's generic reply type, which the registry does not know.

use crate::BusEnvelope;
use crate::reply_rejection_kind::ReplyRejectionKind;
use crate::rpc_protocol::{
    PROTOCOL_VERSION, REPLY_ERROR_MESSAGE_TYPE, REPLY_STATUS_ERROR, REPLY_STATUS_HEADER,
    REPLY_STATUS_OK, read_protocol_version,
};

/// What a pending slot accepts as its reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplyExpectation {
    /// Message type of the nominal reply this call awaits.
    pub reply_message_type: &'static str,
}

impl ReplyExpectation {
    /// Build the expectation for a call awaiting `reply_message_type`.
    #[must_use]
    pub fn new(reply_message_type: &'static str) -> Self {
        Self { reply_message_type }
    }
}

/// Why a delivery is not an acceptable reply.
///
/// Deliberately carries no free-form text: it is built from untrusted input
/// and feeds diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplyRejection {
    /// No protocol version header.
    MissingVersion,
    /// A protocol version this crate does not implement.
    UnsupportedVersion {
        /// The announced version.
        version: u32,
    },
    /// No reply status header.
    MissingStatus,
    /// A reply status outside the closed set.
    UnknownStatus,
    /// The message type does not match the status.
    UnexpectedType,
    /// The signature could not be established against a known key.
    ///
    /// Carries no cause on purpose. Telling an unknown key apart from an
    /// invalid signature or a wrong audience would build an oracle into a
    /// value that is derived from untrusted input and travels all the way to
    /// the caller, which may log it or forward it. The detailed cause stays
    /// in the transport's own `debug!`, on the side that observed it.
    Unauthenticated,
}

impl ReplyRejection {
    /// The operational category this reason feeds.
    #[must_use]
    pub fn kind(self) -> ReplyRejectionKind {
        match self {
            ReplyRejection::Unauthenticated => ReplyRejectionKind::Unauthenticated,
            ReplyRejection::MissingVersion
            | ReplyRejection::UnsupportedVersion { .. }
            | ReplyRejection::MissingStatus
            | ReplyRejection::UnknownStatus
            | ReplyRejection::UnexpectedType => ReplyRejectionKind::Invalid,
        }
    }
}

/// Whether `envelope` is an acceptable reply for `expectation`.
///
/// Checks run from the most structural to the most specific: an unsupported
/// version makes every later check meaningless, so it comes first.
///
/// # Errors
///
/// Returns the [`ReplyRejection`] describing the first rule violated.
pub fn accepts(
    expectation: &ReplyExpectation,
    envelope: &BusEnvelope,
) -> Result<(), ReplyRejection> {
    match read_protocol_version(envelope) {
        Some(PROTOCOL_VERSION) => {}
        Some(version) => return Err(ReplyRejection::UnsupportedVersion { version }),
        None => return Err(ReplyRejection::MissingVersion),
    }

    match envelope.header(REPLY_STATUS_HEADER) {
        Some(REPLY_STATUS_OK) => {
            if envelope.message_type == expectation.reply_message_type {
                Ok(())
            } else {
                Err(ReplyRejection::UnexpectedType)
            }
        }
        Some(REPLY_STATUS_ERROR) => {
            if envelope.message_type == REPLY_ERROR_MESSAGE_TYPE {
                Ok(())
            } else {
                Err(ReplyRejection::UnexpectedType)
            }
        }
        Some(_) => Err(ReplyRejection::UnknownStatus),
        None => Err(ReplyRejection::MissingStatus),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::rpc_protocol::{
        PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, REPLY_ERROR_MESSAGE_TYPE, REPLY_STATUS_ERROR,
        REPLY_STATUS_HEADER, REPLY_STATUS_OK,
    };

    const EXPECTED_REPLY: &str = "test.reply";

    fn expectation() -> ReplyExpectation {
        ReplyExpectation::new(EXPECTED_REPLY)
    }

    fn envelope(message_type: &str, version: Option<u32>, status: Option<&str>) -> BusEnvelope {
        let mut envelope = BusEnvelope::restore(
            uuid::Uuid::now_v7(),
            message_type.to_owned(),
            Vec::new(),
            uuid::Uuid::now_v7(),
            None,
            HashMap::default(),
            std::time::SystemTime::now(),
        );
        if let Some(version) = version {
            envelope.insert_protocol_header(PROTOCOL_VERSION_HEADER, version.to_string());
        }
        if let Some(status) = status {
            envelope.insert_protocol_header(REPLY_STATUS_HEADER, status.to_owned());
        }
        envelope
    }

    #[test]
    fn accepts_a_well_formed_ok_reply() {
        let envelope = envelope(
            EXPECTED_REPLY,
            Some(PROTOCOL_VERSION),
            Some(REPLY_STATUS_OK),
        );
        assert_eq!(accepts(&expectation(), &envelope), Ok(()));
    }

    #[test]
    fn accepts_a_well_formed_error_reply() {
        let envelope = envelope(
            REPLY_ERROR_MESSAGE_TYPE,
            Some(PROTOCOL_VERSION),
            Some(REPLY_STATUS_ERROR),
        );
        assert_eq!(accepts(&expectation(), &envelope), Ok(()));
    }

    #[test]
    fn rejects_a_missing_protocol_version() {
        let envelope = envelope(EXPECTED_REPLY, None, Some(REPLY_STATUS_OK));
        assert_eq!(
            accepts(&expectation(), &envelope),
            Err(ReplyRejection::MissingVersion)
        );
    }

    #[test]
    fn rejects_an_unsupported_protocol_version() {
        let envelope = envelope(EXPECTED_REPLY, Some(99), Some(REPLY_STATUS_OK));
        assert_eq!(
            accepts(&expectation(), &envelope),
            Err(ReplyRejection::UnsupportedVersion { version: 99 })
        );
    }

    #[test]
    fn rejects_a_missing_status() {
        let envelope = envelope(EXPECTED_REPLY, Some(PROTOCOL_VERSION), None);
        assert_eq!(
            accepts(&expectation(), &envelope),
            Err(ReplyRejection::MissingStatus)
        );
    }

    #[test]
    fn rejects_an_unknown_status() {
        let envelope = envelope(EXPECTED_REPLY, Some(PROTOCOL_VERSION), Some("maybe"));
        assert_eq!(
            accepts(&expectation(), &envelope),
            Err(ReplyRejection::UnknownStatus)
        );
    }

    #[test]
    fn rejects_an_ok_status_carrying_the_wrong_reply_type() {
        let envelope = envelope(
            "some.other.type",
            Some(PROTOCOL_VERSION),
            Some(REPLY_STATUS_OK),
        );
        assert_eq!(
            accepts(&expectation(), &envelope),
            Err(ReplyRejection::UnexpectedType)
        );
    }

    #[test]
    fn rejects_an_error_status_without_the_error_sentinel() {
        let envelope = envelope(
            EXPECTED_REPLY,
            Some(PROTOCOL_VERSION),
            Some(REPLY_STATUS_ERROR),
        );
        assert_eq!(
            accepts(&expectation(), &envelope),
            Err(ReplyRejection::UnexpectedType)
        );
    }

    #[test]
    fn an_unauthenticated_rejection_maps_to_its_own_kind() {
        assert_eq!(
            ReplyRejection::Unauthenticated.kind(),
            ReplyRejectionKind::Unauthenticated
        );
    }

    #[test]
    fn every_shape_rejection_maps_to_invalid() {
        let shape_rejections = [
            ReplyRejection::MissingVersion,
            ReplyRejection::UnsupportedVersion { version: 99 },
            ReplyRejection::MissingStatus,
            ReplyRejection::UnknownStatus,
            ReplyRejection::UnexpectedType,
        ];

        for rejection in shape_rejections {
            assert_eq!(
                rejection.kind(),
                ReplyRejectionKind::Invalid,
                "expected {rejection:?} to map to Invalid"
            );
        }
    }
}
