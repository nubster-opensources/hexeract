//! Wire constants of the envelope security protocol.
//!
//! Every constant below lives in the `x-hexeract-` reserved namespace, which
//! application headers may not use. The namespace is enforced at envelope
//! construction and again when AMQP metadata is decoded.

/// Header carrying the base64 signature over the canonical representation.
pub const SIGNATURE_HEADER: &str = "x-hexeract-signature";

/// Header naming the key that produced the signature.
pub const KEY_ID_HEADER: &str = "x-hexeract-key-id";

/// Header naming the authenticated publisher.
pub const ISSUER_HEADER: &str = "x-hexeract-issuer";

/// Header naming the intended recipient.
pub const AUDIENCE_HEADER: &str = "x-hexeract-audience";

/// Header naming the signature algorithm.
pub const ALGORITHM_HEADER: &str = "x-hexeract-algorithm";

/// Header naming the destination the publisher signed the envelope for.
///
/// Compared against the observed routing key before any cryptographic work,
/// so a rerouted envelope is reported as a routing failure rather than as a
/// forged one.
pub const DESTINATION_HEADER: &str = "x-hexeract-destination";

/// Domain separation prefix opening every canonical representation.
///
/// It makes a signature produced for a Hexeract envelope unusable in any
/// other context signed with the same key, and versions the canonical format
/// independently of the RPC protocol version.
pub const CANONICAL_DOMAIN: &str = "hexeract-envelope-v1";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_protocol::is_reserved_header;

    #[test]
    fn every_security_header_lives_in_the_reserved_namespace() {
        for header in [
            SIGNATURE_HEADER,
            KEY_ID_HEADER,
            ISSUER_HEADER,
            AUDIENCE_HEADER,
            ALGORITHM_HEADER,
            DESTINATION_HEADER,
        ] {
            assert!(
                is_reserved_header(header),
                "{header} escapes the reserved namespace"
            );
        }
    }

    #[test]
    fn the_security_headers_are_all_distinct() {
        use std::collections::HashSet;

        let headers = [
            SIGNATURE_HEADER,
            KEY_ID_HEADER,
            ISSUER_HEADER,
            AUDIENCE_HEADER,
            ALGORITHM_HEADER,
            DESTINATION_HEADER,
        ];
        let unique: HashSet<&str> = headers.iter().copied().collect();
        assert_eq!(unique.len(), headers.len());
    }
}
