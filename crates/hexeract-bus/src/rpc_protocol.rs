//! Wire constants of the request-reply protocol.
//!
//! Every header prefixed `x-hexeract-` is reserved by the framework: an
//! application must not write one of its own. Nothing filters or rejects
//! an application-supplied header in this namespace today; a domain header
//! sharing the prefix silently collides with the framework's own value
//! instead of being ignored or rejected. The protocol version travels in
//! the message rather than in the channel, so two versions can coexist on
//! the same topology during a progressive rollout.

use std::collections::HashMap;

/// Header carrying the protocol version of a request or a reply.
pub const PROTOCOL_VERSION_HEADER: &str = "x-hexeract-protocol-version";
/// Protocol version implemented by this crate.
pub const PROTOCOL_VERSION: u32 = 1;
/// Header carrying the unique identity of one request-reply call.
pub const REQUEST_ID_HEADER: &str = "x-hexeract-request-id";
/// Header carrying the reply status on a response envelope.
pub const REPLY_STATUS_HEADER: &str = "x-hexeract-reply-status";
/// Value of [`REPLY_STATUS_HEADER`] for a successful reply.
pub const REPLY_STATUS_OK: &str = "ok";
/// Value of [`REPLY_STATUS_HEADER`] for a failed reply.
pub const REPLY_STATUS_ERROR: &str = "error";
/// Sentinel `message_type` stamped on an error reply envelope.
pub const REPLY_ERROR_MESSAGE_TYPE: &str = "hexeract.rpc.error";
/// Header reserved for the absolute request deadline, as an RFC 3339 UTC
/// timestamp. Reserved by this version, honored by a later one.
pub const DEADLINE_HEADER: &str = "x-hexeract-deadline";

/// Read the protocol version announced by `headers`.
///
/// Returns `None` when the header is absent or cannot be parsed. Both cases
/// are treated as unsupported by callers: a peer that does not announce a
/// version does not speak this protocol.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn read_protocol_version(headers: &HashMap<String, String>) -> Option<u32> {
    headers.get(PROTOCOL_VERSION_HEADER)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn headers_live_in_the_reserved_namespace() {
        for header in [
            PROTOCOL_VERSION_HEADER,
            REQUEST_ID_HEADER,
            REPLY_STATUS_HEADER,
            DEADLINE_HEADER,
        ] {
            assert!(
                header.starts_with("x-hexeract-"),
                "{header} escapes the reserved namespace"
            );
        }
    }

    #[test]
    fn reads_a_well_formed_version() {
        let headers = HashMap::from([(PROTOCOL_VERSION_HEADER.to_owned(), "1".to_owned())]);
        assert_eq!(read_protocol_version(&headers), Some(1));
    }

    #[test]
    fn missing_version_reads_as_none() {
        assert_eq!(read_protocol_version(&HashMap::new()), None);
    }

    #[test]
    fn unparsable_version_reads_as_none() {
        let headers = HashMap::from([(PROTOCOL_VERSION_HEADER.to_owned(), "v1".to_owned())]);
        assert_eq!(read_protocol_version(&headers), None);
    }
}
