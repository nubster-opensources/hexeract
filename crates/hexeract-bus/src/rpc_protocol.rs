//! Wire constants of the request-reply protocol.
//!
//! Every header prefixed `x-hexeract-` is reserved by the framework: an
//! application must not write one of its own. `BusEnvelope::with_headers`
//! rejects application headers in this namespace, and outbound adapters can
//! revalidate the public application-header map before publishing. Protocol
//! headers remain separate from that map. The protocol version travels in the
//! message rather than in the channel, so two versions can coexist on the
//! same topology during a progressive rollout.

use std::collections::HashMap;

/// Prefix reserved for framework protocol headers.
pub const RESERVED_HEADER_PREFIX: &str = "x-hexeract-";

/// Whether `key` belongs to the framework-reserved protocol namespace.
#[must_use]
pub fn is_reserved_header(key: &str) -> bool {
    key.get(..RESERVED_HEADER_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(RESERVED_HEADER_PREFIX))
}

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
    fn reserved_namespace_is_ascii_case_insensitive() {
        for key in [
            "x-hexeract-request-id",
            "X-Hexeract-Request-Id",
            "X-HEXERACT-future",
        ] {
            assert!(is_reserved_header(key), "{key} must be reserved");
        }
        assert!(!is_reserved_header("x-hexeract"));
        assert!(!is_reserved_header("x-hexeractx-request-id"));
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
