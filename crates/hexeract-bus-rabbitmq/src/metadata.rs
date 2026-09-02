//! Bounded AMQP metadata codec shared by the transport, the worker and the
//! reply inbox.
//!
//! AMQP field tables are untrusted once a delivery reaches the client: a small
//! payload can still carry a large header table, and `lapin` has already
//! decoded it by the time Hexeract sees it. This module bounds the work that
//! happens *after* that decode, so an oversized table never reaches a header
//! map allocation, a [`hexeract_bus::BusEnvelope`], an RPC correlation slot or
//! a typed handler.
//!
//! Both directions share one [`AmqpMetadataLimits`] value so the normal worker
//! and the reply inbox cannot drift apart: a path that enforced weaker limits
//! would be a complete bypass of the stronger one.

use std::collections::HashMap;

use hexeract_bus::BusEnvelope;
use hexeract_bus::BusError;
use hexeract_bus::InvalidMetadataReason;
use hexeract_bus::MetadataLimit;
use hexeract_bus::is_reserved_header;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;

use crate::transport::to_short_string;

/// Default maximum number of top-level AMQP header entries.
pub const DEFAULT_MAX_HEADERS: usize = 64;
/// Default maximum UTF-8 byte length of one field-table key.
pub const DEFAULT_MAX_HEADER_KEY_BYTES: usize = 128;
/// Default maximum measured byte size of one top-level header value.
pub const DEFAULT_MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;
/// Default maximum measured byte size of all keys and values combined.
pub const DEFAULT_MAX_METADATA_BYTES: usize = 32 * 1024;

/// Bounds applied to AMQP metadata in both directions.
///
/// Every dimension is a byte length, never a count of Unicode scalars: a
/// multi-byte character costs what it costs on the wire. Input sitting exactly
/// on a limit is accepted and a one-byte overflow is rejected. Zero is a valid
/// deny-all value for the corresponding dimension.
///
/// The framework's own `x-hexeract-*` protocol headers count toward the same
/// budget as application headers. An application that fills its header budget
/// therefore fails its own publish rather than silently dropping protocol
/// metadata; the defaults leave ample room for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmqpMetadataLimits {
    /// Maximum number of top-level AMQP header entries.
    pub max_headers: usize,
    /// Maximum UTF-8 byte length of one field-table key.
    ///
    /// AMQP itself caps a field-table key at 255 bytes, so a value above that
    /// relaxes nothing: an outbound key past the protocol bound still fails,
    /// as [`BusError::InvalidTopology`] rather than as a limit violation.
    pub max_key_bytes: usize,
    /// Maximum measured byte size of one top-level header value.
    pub max_value_bytes: usize,
    /// Maximum measured byte size of all keys and values combined.
    pub max_total_bytes: usize,
}

impl Default for AmqpMetadataLimits {
    fn default() -> Self {
        Self {
            max_headers: DEFAULT_MAX_HEADERS,
            max_key_bytes: DEFAULT_MAX_HEADER_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_HEADER_VALUE_BYTES,
            max_total_bytes: DEFAULT_MAX_METADATA_BYTES,
        }
    }
}

/// Build the limit error for one dimension, carrying sizes but never content.
fn limit_error(limit: MetadataLimit, actual: usize, max: usize) -> BusError {
    BusError::MetadataLimitExceeded { limit, actual, max }
}

/// Build the invalid-metadata error for one reason, carrying no content.
fn invalid_metadata(reason: InvalidMetadataReason) -> BusError {
    BusError::InvalidMetadata { reason }
}

/// Application headers and framework protocol headers read from one delivery.
type DecodedMetadata = (HashMap<String, String>, HashMap<String, String>);

/// Whether `error` reports rejected metadata rather than another failure.
///
/// The worker uses this to route a metadata violation through the sanitized
/// quarantine path: a dead-letter copy that clones the rejected field table
/// would carry the very metadata the worker just refused.
pub(crate) fn is_metadata_error(error: &BusError) -> bool {
    matches!(
        error,
        BusError::ReservedHeaderNamespace
            | BusError::MetadataLimitExceeded { .. }
            | BusError::InvalidMetadata { .. }
    )
}

/// Validate and encode an outbound envelope's metadata as an AMQP field table.
///
/// Every dimension is checked before a single key is converted, so a rejected
/// publish never allocates a field table and never reaches a pooled channel.
///
/// # Errors
///
/// Returns [`BusError::ReservedHeaderNamespace`] when an application header
/// occupies any case variant of the reserved namespace, and
/// [`BusError::MetadataLimitExceeded`] when the combined application and
/// protocol metadata exceeds `limits` in any dimension.
pub(crate) fn encode_headers(
    envelope: &BusEnvelope,
    limits: AmqpMetadataLimits,
) -> Result<FieldTable, BusError> {
    envelope.validate_application_headers()?;

    // Dimensions are checked in their own passes so the reported dimension
    // never depends on the iteration order of the underlying hash map.
    let count = envelope.wire_headers().count();
    if count > limits.max_headers {
        return Err(limit_error(
            MetadataLimit::HeaderCount,
            count,
            limits.max_headers,
        ));
    }

    for (key, _) in envelope.wire_headers() {
        if key.len() > limits.max_key_bytes {
            return Err(limit_error(
                MetadataLimit::KeyBytes,
                key.len(),
                limits.max_key_bytes,
            ));
        }
    }

    for (_, value) in envelope.wire_headers() {
        if value.len() > limits.max_value_bytes {
            return Err(limit_error(
                MetadataLimit::ValueBytes,
                value.len(),
                limits.max_value_bytes,
            ));
        }
    }

    // Saturating rather than wrapping: an implausible overflow must land on
    // `usize::MAX` and be rejected, never wrap around into an accepted total.
    let mut total: usize = 0;
    for (key, value) in envelope.wire_headers() {
        total = total.saturating_add(key.len()).saturating_add(value.len());
    }
    if total > limits.max_total_bytes {
        return Err(limit_error(
            MetadataLimit::TotalBytes,
            total,
            limits.max_total_bytes,
        ));
    }

    let mut fields = FieldTable::default();
    for (key, value) in envelope.wire_headers() {
        fields.insert(
            to_short_string(key, "header key")?,
            AMQPValue::LongString(value.into()),
        );
    }
    Ok(fields)
}

/// Measure and decode an inbound AMQP field table into application and
/// protocol metadata.
///
/// Measurement happens before any value is cloned, so an oversized table costs
/// a walk over already-decoded memory rather than a second copy of it. Only
/// valid UTF-8 long strings are copied out; other AMQP values are measured and
/// left behind, since [`hexeract_bus::BusEnvelope`] carries string metadata
/// only.
///
/// # Errors
///
/// Returns [`BusError::MetadataLimitExceeded`] when the table exceeds `limits`
/// in any dimension, and [`BusError::InvalidMetadata`] when a long string is
/// not valid UTF-8 or a reserved key is not in its canonical lowercase form.
pub(crate) fn decode_headers(
    table: Option<&FieldTable>,
    limits: AmqpMetadataLimits,
) -> Result<DecodedMetadata, BusError> {
    let Some(table) = table else {
        return Ok((HashMap::new(), HashMap::new()));
    };
    let entries = table.inner();

    if entries.len() > limits.max_headers {
        return Err(limit_error(
            MetadataLimit::HeaderCount,
            entries.len(),
            limits.max_headers,
        ));
    }

    for key in entries.keys() {
        let key_bytes = key.as_str().len();
        if key_bytes > limits.max_key_bytes {
            return Err(limit_error(
                MetadataLimit::KeyBytes,
                key_bytes,
                limits.max_key_bytes,
            ));
        }
    }

    let mut total: usize = 0;
    for (key, value) in entries {
        let value_bytes = measure_value(value, limits.max_value_bytes)?;
        total = total
            .saturating_add(key.as_str().len())
            .saturating_add(value_bytes);
        if total > limits.max_total_bytes {
            return Err(limit_error(
                MetadataLimit::TotalBytes,
                total,
                limits.max_total_bytes,
            ));
        }
    }

    let mut application = HashMap::new();
    let mut protocol = HashMap::new();
    for (key, value) in entries {
        let key = key.as_str();
        let reserved = is_reserved_header(key);
        // A reserved key is rejected on any non-canonical spelling before its
        // value is even looked at: accepting `X-Hexeract-Request-Id` as an
        // alias would hand a remote producer a way to spell a protocol field.
        if reserved && key.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(invalid_metadata(
                InvalidMetadataReason::NonCanonicalReservedHeader,
            ));
        }
        let AMQPValue::LongString(text) = value else {
            continue;
        };
        let text = std::str::from_utf8(text.as_bytes())
            .map_err(|_| invalid_metadata(InvalidMetadataReason::NonUtf8LongString))?;
        if reserved {
            protocol.insert(key.to_owned(), text.to_owned());
        } else {
            application.insert(key.to_owned(), text.to_owned());
        }
    }

    Ok((application, protocol))
}

/// Measure one top-level AMQP value, including everything nested inside it.
///
/// The walk uses an explicit stack rather than recursion: nesting depth is
/// attacker-controlled, and a recursive walk would trade a bounded metadata
/// limit for an unbounded call stack. Measurement stops as soon as the running
/// total exceeds `max_value_bytes`, so an oversized value is not walked in
/// full.
fn measure_value(value: &AMQPValue, max_value_bytes: usize) -> Result<usize, BusError> {
    let mut measured: usize = 0;
    let mut pending: Vec<&AMQPValue> = vec![value];

    while let Some(current) = pending.pop() {
        // Fixed-width scalars contribute their encoded wire width; containers
        // contribute their own keys and push their members. The match is
        // exhaustive on purpose: a new `lapin` value type must fail to compile
        // here rather than silently measure as zero bytes.
        let contribution = match current {
            AMQPValue::Boolean(_) | AMQPValue::ShortShortInt(_) | AMQPValue::ShortShortUInt(_) => 1,
            AMQPValue::ShortInt(_) | AMQPValue::ShortUInt(_) => 2,
            AMQPValue::LongInt(_) | AMQPValue::LongUInt(_) | AMQPValue::Float(_) => 4,
            AMQPValue::LongLongInt(_) | AMQPValue::Double(_) | AMQPValue::Timestamp(_) => 8,
            AMQPValue::DecimalValue(_) => 5,
            AMQPValue::ShortString(text) => text.as_str().len(),
            AMQPValue::LongString(text) => text.as_bytes().len(),
            AMQPValue::ByteArray(bytes) => bytes.as_slice().len(),
            AMQPValue::Void => 0,
            AMQPValue::FieldArray(items) => {
                pending.extend(items.as_slice().iter());
                0
            }
            AMQPValue::FieldTable(fields) => {
                let mut keys: usize = 0;
                for (key, nested) in fields.inner() {
                    keys = keys.saturating_add(key.as_str().len());
                    pending.push(nested);
                }
                keys
            }
        };

        measured = measured.saturating_add(contribution);
        if measured > max_value_bytes {
            return Err(limit_error(
                MetadataLimit::ValueBytes,
                measured,
                max_value_bytes,
            ));
        }
    }

    Ok(measured)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use hexeract_bus::BusEnvelope;
    use hexeract_bus::BusError;
    use hexeract_bus::InvalidMetadataReason;
    use hexeract_bus::MetadataLimit;
    use lapin::types::AMQPValue;
    use lapin::types::FieldArray;
    use lapin::types::FieldTable;
    use uuid::Uuid;

    use super::*;

    /// Build an envelope carrying only application headers.
    fn envelope_with_headers<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> BusEnvelope {
        envelope_with_split_headers(entries, [])
    }

    /// Build an envelope carrying both application and protocol headers.
    fn envelope_with_split_headers<'a, 'b>(
        application: impl IntoIterator<Item = (&'a str, &'a str)>,
        protocol: impl IntoIterator<Item = (&'b str, &'b str)>,
    ) -> BusEnvelope {
        BusEnvelope::restore_from_transport(
            Uuid::now_v7(),
            "orders.placed".to_owned(),
            b"{}".to_vec(),
            Uuid::now_v7(),
            None,
            owned(application),
            owned(protocol),
            SystemTime::now(),
        )
    }

    fn owned<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> HashMap<String, String> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }

    /// Build an inbound AMQP field table.
    fn table<'a>(entries: impl IntoIterator<Item = (&'a str, AMQPValue)>) -> FieldTable {
        let mut fields = FieldTable::default();
        for (key, value) in entries {
            fields.insert(key.into(), value);
        }
        fields
    }

    fn long_string(bytes: impl Into<Vec<u8>>) -> AMQPValue {
        AMQPValue::LongString(bytes.into().into())
    }

    // ---------------------------------------------------------------- limits

    #[test]
    fn defaults_match_the_public_contract() {
        assert_eq!(
            AmqpMetadataLimits::default(),
            AmqpMetadataLimits {
                max_headers: 64,
                max_key_bytes: 128,
                max_value_bytes: 8 * 1024,
                max_total_bytes: 32 * 1024,
            }
        );
    }

    #[test]
    fn default_constants_are_the_documented_values() {
        assert_eq!(DEFAULT_MAX_HEADERS, 64);
        assert_eq!(DEFAULT_MAX_HEADER_KEY_BYTES, 128);
        assert_eq!(DEFAULT_MAX_HEADER_VALUE_BYTES, 8 * 1024);
        assert_eq!(DEFAULT_MAX_METADATA_BYTES, 32 * 1024);
    }

    // -------------------------------------------------------------- outbound

    #[test]
    fn unicode_value_is_measured_in_utf8_bytes() {
        let limits = AmqpMetadataLimits {
            max_headers: 1,
            max_key_bytes: 1,
            max_value_bytes: 3,
            max_total_bytes: 4,
        };
        let mut envelope = envelope_with_headers([("k", "é")]);
        assert!(encode_headers(&envelope, limits).is_ok());

        envelope.headers.insert("k".into(), "éé".into());
        assert!(matches!(
            encode_headers(&envelope, limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::ValueBytes,
                actual: 4,
                max: 3,
            })
        ));
    }

    #[test]
    fn direct_public_map_mutation_cannot_publish_reserved_namespace() {
        let mut envelope = envelope_with_headers([]);
        envelope
            .headers
            .insert("X-Hexeract-Future".into(), "x".into());
        assert!(matches!(
            encode_headers(&envelope, AmqpMetadataLimits::default()),
            Err(BusError::ReservedHeaderNamespace)
        ));
    }

    #[test]
    fn outbound_header_count_accepts_exact_and_rejects_one_more() {
        let limits = AmqpMetadataLimits {
            max_headers: 2,
            ..AmqpMetadataLimits::default()
        };
        let exact = envelope_with_headers([("a", "1"), ("b", "2")]);
        assert!(encode_headers(&exact, limits).is_ok());

        let over = envelope_with_headers([("a", "1"), ("b", "2"), ("c", "3")]);
        assert!(matches!(
            encode_headers(&over, limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::HeaderCount,
                actual: 3,
                max: 2,
            })
        ));
    }

    #[test]
    fn outbound_key_bytes_accept_exact_and_reject_one_more() {
        let limits = AmqpMetadataLimits {
            max_key_bytes: 4,
            ..AmqpMetadataLimits::default()
        };
        let exact = envelope_with_headers([("abcd", "1")]);
        assert!(encode_headers(&exact, limits).is_ok());

        let over = envelope_with_headers([("abcde", "1")]);
        assert!(matches!(
            encode_headers(&over, limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::KeyBytes,
                actual: 5,
                max: 4,
            })
        ));
    }

    #[test]
    fn outbound_total_bytes_accept_exact_and_reject_one_more() {
        let limits = AmqpMetadataLimits {
            max_total_bytes: 8,
            ..AmqpMetadataLimits::default()
        };
        let exact = envelope_with_headers([("aaa", "1"), ("bbb", "2")]);
        assert!(encode_headers(&exact, limits).is_ok());

        let over = envelope_with_headers([("aaa", "1"), ("bbb", "22")]);
        assert!(matches!(
            encode_headers(&over, limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::TotalBytes,
                actual: 9,
                max: 8,
            })
        ));
    }

    #[test]
    fn many_small_headers_exceed_the_count_limit() {
        let owned_keys: Vec<String> = (0..65).map(|index| format!("h{index}")).collect();
        let envelope = envelope_with_headers(owned_keys.iter().map(|key| (key.as_str(), "v")));
        assert!(matches!(
            encode_headers(&envelope, AmqpMetadataLimits::default()),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::HeaderCount,
                actual: 65,
                max: 64,
            })
        ));
    }

    #[test]
    fn protocol_headers_reach_the_wire() {
        let envelope = envelope_with_split_headers([], [("x-hexeract-request-id", "request-1")]);
        let fields = encode_headers(&envelope, AmqpMetadataLimits::default())
            .expect("canonical protocol metadata must encode");
        assert_eq!(fields.inner().len(), 1);
        assert!(matches!(
            fields.inner().get("x-hexeract-request-id"),
            Some(AMQPValue::LongString(value)) if value.as_bytes() == b"request-1"
        ));
    }

    #[test]
    fn protocol_headers_count_toward_the_same_budget() {
        let limits = AmqpMetadataLimits {
            max_headers: 1,
            ..AmqpMetadataLimits::default()
        };
        let envelope = envelope_with_split_headers(
            [("tenant", "acme")],
            [("x-hexeract-request-id", "request-1")],
        );
        assert!(matches!(
            encode_headers(&envelope, limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::HeaderCount,
                actual: 2,
                max: 1,
            })
        ));
    }

    #[test]
    fn zero_limits_deny_every_header_yet_accept_none() {
        let limits = AmqpMetadataLimits {
            max_headers: 0,
            max_key_bytes: 0,
            max_value_bytes: 0,
            max_total_bytes: 0,
        };
        assert!(encode_headers(&envelope_with_headers([]), limits).is_ok());
        assert!(matches!(
            encode_headers(&envelope_with_headers([("k", "")]), limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::HeaderCount,
                actual: 1,
                max: 0,
            })
        ));
    }

    // --------------------------------------------------------------- inbound

    #[test]
    fn absent_field_table_decodes_to_empty_maps() {
        let (application, protocol) = decode_headers(None, AmqpMetadataLimits::default())
            .expect("an absent table must decode");
        assert!(application.is_empty());
        assert!(protocol.is_empty());
    }

    #[test]
    fn inbound_exact_value_limit_passes_and_one_byte_over_fails() {
        let limits = AmqpMetadataLimits {
            max_headers: 1,
            max_key_bytes: 1,
            max_value_bytes: 3,
            max_total_bytes: 4,
        };
        let exact = table([("k", long_string(vec![b'x'; 3]))]);
        assert!(decode_headers(Some(&exact), limits).is_ok());

        let over = table([("k", long_string(vec![b'x'; 4]))]);
        assert!(matches!(
            decode_headers(Some(&over), limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::ValueBytes,
                actual: 4,
                max: 3,
            })
        ));
    }

    #[test]
    fn inbound_header_count_accepts_exact_and_rejects_one_more() {
        let limits = AmqpMetadataLimits {
            max_headers: 2,
            ..AmqpMetadataLimits::default()
        };
        let exact = table([("a", long_string("1")), ("b", long_string("2"))]);
        assert!(decode_headers(Some(&exact), limits).is_ok());

        let over = table([
            ("a", long_string("1")),
            ("b", long_string("2")),
            ("c", long_string("3")),
        ]);
        assert!(matches!(
            decode_headers(Some(&over), limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::HeaderCount,
                actual: 3,
                max: 2,
            })
        ));
    }

    #[test]
    fn inbound_key_bytes_accept_exact_and_reject_one_more() {
        let limits = AmqpMetadataLimits {
            max_key_bytes: 4,
            ..AmqpMetadataLimits::default()
        };
        let exact = table([("abcd", long_string("1"))]);
        assert!(decode_headers(Some(&exact), limits).is_ok());

        let over = table([("abcde", long_string("1"))]);
        assert!(matches!(
            decode_headers(Some(&over), limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::KeyBytes,
                actual: 5,
                max: 4,
            })
        ));
    }

    #[test]
    fn inbound_total_bytes_accept_exact_and_reject_one_more() {
        let limits = AmqpMetadataLimits {
            max_total_bytes: 8,
            ..AmqpMetadataLimits::default()
        };
        let exact = table([("aaa", long_string("1")), ("bbb", long_string("2"))]);
        assert!(decode_headers(Some(&exact), limits).is_ok());

        let over = table([("aaa", long_string("1")), ("bbb", long_string("22"))]);
        assert!(matches!(
            decode_headers(Some(&over), limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::TotalBytes,
                actual: 9,
                max: 8,
            })
        ));
    }

    #[test]
    fn noncanonical_reserved_wire_key_is_rejected() {
        let headers = table([("X-Hexeract-Request-Id", long_string("request-1"))]);
        assert!(matches!(
            decode_headers(Some(&headers), AmqpMetadataLimits::default()),
            Err(BusError::InvalidMetadata {
                reason: InvalidMetadataReason::NonCanonicalReservedHeader,
            })
        ));
    }

    #[test]
    fn canonical_unknown_reserved_key_enters_the_protocol_map() {
        let headers = table([("x-hexeract-future", long_string("1"))]);
        let (application, protocol) = decode_headers(Some(&headers), AmqpMetadataLimits::default())
            .expect("a canonical reserved key must decode");
        assert!(application.is_empty());
        assert_eq!(
            protocol.get("x-hexeract-future").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn application_keys_preserve_their_case() {
        let headers = table([("Tenant-Id", long_string("acme"))]);
        let (application, protocol) = decode_headers(Some(&headers), AmqpMetadataLimits::default())
            .expect("an application key must decode");
        assert_eq!(
            application.get("Tenant-Id").map(String::as_str),
            Some("acme")
        );
        assert!(protocol.is_empty());
    }

    #[test]
    fn invalid_utf8_long_string_is_rejected() {
        let headers = table([("tenant", long_string(vec![0xff, 0xfe]))]);
        assert!(matches!(
            decode_headers(Some(&headers), AmqpMetadataLimits::default()),
            Err(BusError::InvalidMetadata {
                reason: InvalidMetadataReason::NonUtf8LongString,
            })
        ));
    }

    #[test]
    fn nested_non_string_values_count_but_are_not_copied() {
        let nested = FieldArray::from(vec![
            AMQPValue::ByteArray(vec![1, 2, 3].into()),
            AMQPValue::LongLongInt(7),
        ]);
        let headers = table([("x-death", AMQPValue::FieldArray(nested))]);
        let (application, protocol) = decode_headers(Some(&headers), AmqpMetadataLimits::default())
            .expect("a normal x-death history must decode");
        assert!(application.is_empty());
        assert!(protocol.is_empty());
    }

    #[test]
    fn nested_keys_and_values_count_toward_the_value_limit() {
        // "queue" (5) + "orders" (6) = 11 bytes inside one top-level value.
        let limits = AmqpMetadataLimits {
            max_value_bytes: 11,
            ..AmqpMetadataLimits::default()
        };
        let inner = table([("queue", long_string("orders"))]);
        let headers = table([("x-death", AMQPValue::FieldTable(inner))]);
        assert!(decode_headers(Some(&headers), limits).is_ok());

        let inner = table([("queue", long_string("orders7"))]);
        let headers = table([("x-death", AMQPValue::FieldTable(inner))]);
        assert!(matches!(
            decode_headers(Some(&headers), limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::ValueBytes,
                actual: 12,
                max: 11,
            })
        ));
    }

    #[test]
    fn fixed_width_scalars_contribute_their_encoded_width() {
        // One 8-byte scalar plus a 1-byte key exceeds a 8-byte aggregate.
        let limits = AmqpMetadataLimits {
            max_total_bytes: 8,
            ..AmqpMetadataLimits::default()
        };
        let headers = table([("k", AMQPValue::LongLongInt(7))]);
        assert!(matches!(
            decode_headers(Some(&headers), limits),
            Err(BusError::MetadataLimitExceeded {
                limit: MetadataLimit::TotalBytes,
                actual: 9,
                max: 8,
            })
        ));
    }

    #[test]
    fn void_contributes_nothing() {
        let limits = AmqpMetadataLimits {
            max_total_bytes: 1,
            ..AmqpMetadataLimits::default()
        };
        let headers = table([("k", AMQPValue::Void)]);
        assert!(decode_headers(Some(&headers), limits).is_ok());
    }

    #[test]
    fn deeply_nested_values_are_measured_without_recursion() {
        let mut value = AMQPValue::LongLongInt(1);
        for _ in 0..256 {
            value = AMQPValue::FieldArray(FieldArray::from(vec![value]));
        }
        let headers = table([("nested", value)]);
        assert!(decode_headers(Some(&headers), AmqpMetadataLimits::default()).is_ok());
    }

    #[test]
    fn a_representative_x_death_history_fits_the_defaults() {
        let entry = table([
            ("count", AMQPValue::LongLongInt(3)),
            ("exchange", long_string("orders")),
            ("queue", long_string("orders.work")),
            ("reason", long_string("rejected")),
            ("time", AMQPValue::Timestamp(1_756_000_000)),
        ]);
        let headers = table([(
            "x-death",
            AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::FieldTable(entry)])),
        )]);
        assert!(decode_headers(Some(&headers), AmqpMetadataLimits::default()).is_ok());
    }
}
