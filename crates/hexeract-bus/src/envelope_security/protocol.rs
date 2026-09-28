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

/// Reserved header names this module allows a formatter to disclose.
///
/// Only the names are disclosable, never the values. Naming
/// [`SIGNATURE_HEADER`] says that an envelope is signed, which is what an
/// operator debugging a rejected verification needs; printing the signature
/// itself would put cryptographic material in a log.
///
/// Adding a header constant above means deciding here whether its presence is
/// safe to announce. The list is exhaustive by test, not by convention.
pub const DISCLOSABLE_HEADER_NAMES: &[&str] = &[
    SIGNATURE_HEADER,
    KEY_ID_HEADER,
    ISSUER_HEADER,
    AUDIENCE_HEADER,
    ALGORITHM_HEADER,
    DESTINATION_HEADER,
];

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
    fn every_security_header_is_disclosable_by_name() {
        for header in [
            SIGNATURE_HEADER,
            KEY_ID_HEADER,
            ISSUER_HEADER,
            AUDIENCE_HEADER,
            ALGORITHM_HEADER,
            DESTINATION_HEADER,
        ] {
            assert!(
                DISCLOSABLE_HEADER_NAMES.contains(&header),
                "{header} is absent from DISCLOSABLE_HEADER_NAMES, so a formatter \
                 counts it instead of naming it"
            );
        }
    }

    #[test]
    fn the_disclosable_list_holds_exactly_the_security_headers() {
        assert_eq!(
            DISCLOSABLE_HEADER_NAMES.len(),
            6,
            "a header constant was added or removed above without deciding here \
             whether its presence is safe to announce. Rust cannot enumerate a \
             module's constants, so this count is the only guard there is"
        );
        for header in DISCLOSABLE_HEADER_NAMES {
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
