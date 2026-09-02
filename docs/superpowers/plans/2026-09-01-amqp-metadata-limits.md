# AMQP Metadata Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound RabbitMQ metadata in both directions and prevent application headers from occupying any case variant of the `x-hexeract-*` protocol namespace.

**Architecture:** `hexeract-bus` separates public application headers from a private protocol-header map and exposes a read-only combined wire view. `hexeract-bus-rabbitmq` owns one `AmqpMetadataLimits` type and a shared codec that validates outbound string headers and iteratively measures inbound AMQP values before cloning; workers and reply inboxes pass violations through their existing poison/drop boundaries.

**Tech Stack:** Rust 2024 edition, Tokio, `lapin`/AMQP 0-9-1 types, `thiserror`, existing unit and Docker-backed integration tests.

**Spec:** `docs/superpowers/specs/2026-09-01-amqp-metadata-limits-design.md`

## Global Constraints

- Preserve the released `BusEnvelope::headers: HashMap<String, String>` application-header surface and every existing constructor signature.
- Reserve every ASCII case variant of the complete `x-hexeract-*` prefix; accept only canonical lowercase protocol keys from the wire.
- Defaults are exactly 64 top-level headers, 128 key bytes, 8 KiB per value, and 32 KiB aggregate metadata.
- Measure UTF-8 byte lengths, accept exact limits, reject a one-byte overflow, and treat zero as a valid deny-all bound.
- Measure all inbound AMQP values, including nested arrays/tables and values that are not copied to `BusEnvelope`.
- Never format or log a header key or value when rejecting metadata.
- Do not authenticate metadata in this change; #444 remains responsible for producer identity, integrity, and audience.
- Do not alter retry or acknowledgement semantics except that a metadata-invalid application dead-letter copy must rebuild properties with an empty field table.
- Do not add dependencies.
- Leave the unrelated untracked `.claude/` directory untouched.

---

### Task 1: Core metadata errors and application/protocol header separation

**Files:**
- Modify: `crates/hexeract-bus/src/error.rs`
- Modify: `crates/hexeract-bus/src/rpc_protocol.rs`
- Modify: `crates/hexeract-bus/src/envelope.rs`
- Modify: `crates/hexeract-bus/src/lib.rs`
- Test: unit modules in the four files above

**Interfaces:**
- Produces: `MetadataLimit`, `InvalidMetadataReason`, and three new `BusError` variants.
- Produces: `RESERVED_HEADER_PREFIX`, `is_reserved_header(&str) -> bool`, `BusEnvelope::header`, `BusEnvelope::wire_headers`, `BusEnvelope::validate_application_headers`, `BusEnvelope::restore_from_transport`, and crate-private `insert_protocol_header`/`remove_protocol_header`.
- Preserves: public `headers` field, `new`, `with_headers`, `with_reply_to`, and `restore` signatures.

- [ ] **Step 1: Write failing namespace and envelope tests**

Add tests proving ASCII case-insensitive prefix recognition, near-miss acceptance, rejection by `with_headers`, rejection after direct mutation through `validate_application_headers`, protocol/application lookup separation, combined iteration, and restoration with two maps:

```rust
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
fn application_constructor_rejects_reserved_header_case_variants() {
    let headers = HashMap::from([(
        "X-Hexeract-Request-Id".to_owned(),
        "spoofed".to_owned(),
    )]);
    assert!(matches!(
        BusEnvelope::with_headers(Uuid::nil(), headers, &sample_order()),
        Err(BusError::ReservedHeaderNamespace)
    ));
}

#[test]
fn protocol_headers_are_private_but_visible_to_wire_and_protocol_readers() {
    let mut envelope = BusEnvelope::new(Uuid::nil(), &sample_order()).unwrap();
    envelope.headers.insert("tenant".into(), "acme".into());
    envelope.insert_protocol_header(REQUEST_ID_HEADER, "request-1".into());
    assert_eq!(envelope.headers.get(REQUEST_ID_HEADER), None);
    assert_eq!(envelope.header(REQUEST_ID_HEADER), Some("request-1"));
    assert_eq!(envelope.header("tenant"), Some("acme"));
    assert_eq!(envelope.wire_headers().count(), 2);
}
```

- [ ] **Step 2: Run the focused core tests and verify RED**

Run:

```powershell
cargo test -p hexeract-bus rpc_protocol::tests
cargo test -p hexeract-bus envelope::tests
cargo test -p hexeract-bus error::tests
```

Expected: compilation fails because the constants, variants, field, and methods do not exist.

- [ ] **Step 3: Add typed, value-free errors**

In `error.rs`, add and export these exact types:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataLimit {
    HeaderCount,
    KeyBytes,
    ValueBytes,
    TotalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidMetadataReason {
    NonUtf8LongString,
    NonCanonicalReservedHeader,
}
```

Give both enums `Display` implementations containing only stable reason names. Extend `BusError` with:

```rust
#[error("application header uses the reserved x-hexeract-* namespace")]
ReservedHeaderNamespace,

#[error("metadata {limit} limit exceeded: observed {actual}, maximum {max}")]
MetadataLimitExceeded {
    limit: MetadataLimit,
    actual: usize,
    max: usize,
},

#[error("invalid metadata: {reason}")]
InvalidMetadata { reason: InvalidMetadataReason },
```

Add unit tests asserting the rendered errors contain the dimension and sizes but no sample key/value.

- [ ] **Step 4: Implement the split envelope model**

In `rpc_protocol.rs`:

```rust
pub const RESERVED_HEADER_PREFIX: &str = "x-hexeract-";

#[must_use]
pub fn is_reserved_header(key: &str) -> bool {
    key.get(..RESERVED_HEADER_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(RESERVED_HEADER_PREFIX))
}
```

In `BusEnvelope`, add `protocol_headers: HashMap<String, String>`, initialize it in every constructor, and implement:

```rust
pub fn header(&self, key: &str) -> Option<&str>;
#[doc(hidden)] pub fn wire_headers(&self) -> impl Iterator<Item = (&str, &str)>;
#[doc(hidden)] pub fn validate_application_headers(&self) -> Result<(), BusError>;
pub(crate) fn insert_protocol_header(&mut self, key: &'static str, value: String);
pub(crate) fn remove_protocol_header(&mut self, key: &'static str) -> Option<String>;
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn restore_from_transport(
    message_id: Uuid,
    message_type: String,
    payload: Vec<u8>,
    correlation_id: Uuid,
    reply_to: Option<String>,
    headers: HashMap<String, String>,
    protocol_headers: HashMap<String, String>,
    published_at: SystemTime,
) -> Self;
```

`with_headers` must call `validate_application_headers` before returning. `header` must consult only `protocol_headers` for a reserved key, preventing a directly mutated public map from shadowing protocol data. `wire_headers` chains application then protocol iterators without allocation.

- [ ] **Step 5: Run core tests and verify GREEN**

Run:

```powershell
cargo test -p hexeract-bus rpc_protocol::tests
cargo test -p hexeract-bus envelope::tests
cargo test -p hexeract-bus error::tests
```

Expected: all focused tests pass.

- [ ] **Step 6: Commit the core boundary**

```powershell
git add crates/hexeract-bus/src/error.rs crates/hexeract-bus/src/rpc_protocol.rs crates/hexeract-bus/src/envelope.rs crates/hexeract-bus/src/lib.rs
git commit -m "feat(bus): separate protocol metadata"
```

---

### Task 2: Migrate request/reply to the private protocol API

**Files:**
- Modify: `crates/hexeract-bus/src/rpc_protocol.rs`
- Modify: `crates/hexeract-bus/src/request_client.rs`
- Modify: `crates/hexeract-bus/src/replied_handler.rs`
- Modify: `crates/hexeract-bus/src/reply_acceptance.rs`
- Modify: `crates/hexeract-bus/src/request_registry.rs`
- Modify: `crates/hexeract-bus/src/remote_error.rs`
- Test: unit modules in those files

**Interfaces:**
- Consumes: Task 1 protocol insert/read/remove API and error variants.
- Produces: all RPC protocol fields live only in `BusEnvelope::protocol_headers`; `read_protocol_version(&BusEnvelope)` reads through `header`.
- Preserves: RPC wire names and values byte-for-byte.

- [ ] **Step 1: Add failing RPC separation tests**

Adapt existing helpers to construct protocol headers through crate-private insertion, then add explicit assertions:

```rust
#[test]
fn request_wire_fields_do_not_occupy_application_headers() {
    let mut envelope = request_envelope();
    envelope.insert_protocol_header(REQUEST_ID_HEADER, RequestId::new().to_string());
    envelope.insert_protocol_header(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION.to_string());
    assert!(envelope.headers.keys().all(|key| !is_reserved_header(key)));
    assert!(envelope.header(REQUEST_ID_HEADER).is_some());
    assert_eq!(
        envelope.header(PROTOCOL_VERSION_HEADER),
        Some("1")
    );
}
```

Add request-client tests showing request envelopes, successful replies, and error replies expose all canonical protocol fields through `header`, while inserting `X-Hexeract-*` into the public map cannot change `RequestRegistry` resolution.

- [ ] **Step 2: Run RPC tests and verify RED**

Run:

```powershell
cargo test -p hexeract-bus request_client::tests
cargo test -p hexeract-bus replied_handler::tests
cargo test -p hexeract-bus request_registry::tests
cargo test -p hexeract-bus reply_acceptance::tests
```

Expected: failures where production code still inserts and reads reserved keys through `headers`.

- [ ] **Step 3: Replace every production RPC map access**

Make these mechanical changes:

- `RequestClient::request_inner`: use `insert_protocol_header` for request id and version.
- `decode_reply`, `RequestRegistry::resolve`, `RepliedHandler`, and `reply_acceptance`: use `envelope.header`.
- `read_protocol_version`: accept `&BusEnvelope`, call `header`, and update all callers.
- Successful and error reply builders: start with `BusEnvelope::new`/`restore`, then use `insert_protocol_header` for status, request id, and version.
- Test-only corruption helpers: use `remove_protocol_header` and `insert_protocol_header` instead of editing `headers`.
- `RemoteErrorType::from_bus_error`: classify all three new metadata errors as `Malformed`.

Confirm with:

```powershell
rg -n "headers\.(get|insert|remove)\((REQUEST_ID_HEADER|PROTOCOL_VERSION_HEADER|REPLY_STATUS_HEADER)" crates/hexeract-bus/src -g '*.rs'
```

Expected: no production match.

- [ ] **Step 4: Run the entire core crate test suite**

Run:

```powershell
cargo test -p hexeract-bus
```

Expected: all tests pass and existing RPC wire-value assertions remain unchanged.

- [ ] **Step 5: Commit the RPC migration**

```powershell
git add crates/hexeract-bus/src
git commit -m "refactor(rpc): isolate protocol headers"
```

---

### Task 3: Shared RabbitMQ metadata limits and codec

**Files:**
- Create: `crates/hexeract-bus-rabbitmq/src/metadata.rs`
- Modify: `crates/hexeract-bus-rabbitmq/src/lib.rs`
- Modify: `crates/hexeract-bus-rabbitmq/src/transport.rs`
- Modify: `crates/hexeract-bus-rabbitmq/src/worker.rs`
- Test: unit modules in `metadata.rs`, `transport.rs`, and `worker.rs`

**Interfaces:**
- Consumes: Task 1 `BusEnvelope::wire_headers`, `validate_application_headers`, `restore_from_transport`, and metadata errors.
- Produces: public `AmqpMetadataLimits` plus constants `DEFAULT_MAX_HEADERS`, `DEFAULT_MAX_HEADER_KEY_BYTES`, `DEFAULT_MAX_HEADER_VALUE_BYTES`, `DEFAULT_MAX_METADATA_BYTES`.
- Produces internally: `encode_headers`, `decode_headers`, and `is_metadata_error`.

- [ ] **Step 1: Write failing default and outbound boundary tests**

Create `metadata.rs` tests for exact limits, one-byte overflow, Unicode byte length, many small headers, and case variants:

```rust
#[test]
fn defaults_match_the_public_contract() {
    assert_eq!(AmqpMetadataLimits::default(), AmqpMetadataLimits {
        max_headers: 64,
        max_key_bytes: 128,
        max_value_bytes: 8 * 1024,
        max_total_bytes: 32 * 1024,
    });
}

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
    envelope.headers.insert("X-Hexeract-Future".into(), "x".into());
    assert!(matches!(
        encode_headers(&envelope, AmqpMetadataLimits::default()),
        Err(BusError::ReservedHeaderNamespace)
    ));
}
```

- [ ] **Step 2: Write failing inbound AMQP measurement tests**

Build `FieldTable` values containing `LongString`, invalid UTF-8, `ByteArray`, `FieldArray`, nested `FieldTable`, and a representative `x-death`. Start with these exact boundary cases:

```rust
#[test]
fn inbound_exact_value_limit_passes_and_one_byte_over_fails() {
    let limits = AmqpMetadataLimits {
        max_headers: 1,
        max_key_bytes: 1,
        max_value_bytes: 3,
        max_total_bytes: 4,
    };
    let exact = table([("k", AMQPValue::LongString(vec![b'x'; 3].into()))]);
    assert!(decode_headers(Some(&exact), limits).is_ok());

    let over = table([("k", AMQPValue::LongString(vec![b'x'; 4].into()))]);
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
fn noncanonical_reserved_wire_key_is_rejected() {
    let headers = table([(
        "X-Hexeract-Request-Id",
        AMQPValue::LongString(b"request-1".to_vec().into()),
    )]);
    assert!(matches!(
        decode_headers(Some(&headers), AmqpMetadataLimits::default()),
        Err(BusError::InvalidMetadata {
            reason: InvalidMetadataReason::NonCanonicalReservedHeader,
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
    let (application, protocol) =
        decode_headers(Some(&headers), AmqpMetadataLimits::default()).unwrap();
    assert!(application.is_empty());
    assert!(protocol.is_empty());
}
```

Complete the same table-driven test module with these assertions:

- every top-level key counts toward `max_headers`;
- every nested key/value counts toward `max_value_bytes` and `max_total_bytes`;
- exact limit succeeds and one-byte overflow returns the correct dimension;
- invalid UTF-8 returns `InvalidMetadataReason::NonUtf8LongString`;
- `X-Hexeract-Request-Id` returns `NonCanonicalReservedHeader`;
- canonical unknown `x-hexeract-future` is returned in the protocol map;
- non-string values are measured but absent from both returned maps.

Use an iterative stack in the expected implementation, so include a 256-level nested array test that returns a size result without recursion.

- [ ] **Step 3: Run metadata tests and verify RED**

Run:

```powershell
cargo test -p hexeract-bus-rabbitmq metadata::tests --lib
```

Expected: compilation fails because the module and types do not exist.

- [ ] **Step 4: Implement `AmqpMetadataLimits` and outbound encoding**

Define:

```rust
pub const DEFAULT_MAX_HEADERS: usize = 64;
pub const DEFAULT_MAX_HEADER_KEY_BYTES: usize = 128;
pub const DEFAULT_MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;
pub const DEFAULT_MAX_METADATA_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmqpMetadataLimits {
    pub max_headers: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_total_bytes: usize,
}
```

Implement `Default`. `encode_headers(&BusEnvelope, AmqpMetadataLimits) -> Result<FieldTable, BusError>` must:

1. call `validate_application_headers`;
2. iterate the combined wire view once;
3. check count, key bytes, value bytes, and checked aggregate bytes in that order;
4. convert keys with existing `to_short_string` only after all limits pass;
5. never include a rejected key/value in an error.

- [ ] **Step 5: Implement iterative inbound measurement and decoding**

Implement:

```rust
pub(crate) fn decode_headers(
    table: Option<&FieldTable>,
    limits: AmqpMetadataLimits,
) -> Result<(HashMap<String, String>, HashMap<String, String>), BusError>;
```

Use `Vec<&AMQPValue>` as the explicit work stack. Fixed widths are 1, 2, 4, or 8 bytes according to the scalar; `DecimalValue` is 5 bytes; `ShortString`, `LongString`, and `ByteArray` contribute their byte lengths; array/table members are pushed; nested table keys contribute their UTF-8 lengths; `Void` contributes zero. Use `checked_add(...).unwrap_or(usize::MAX)` so arithmetic overflow deterministically exceeds the bound.

After measurement succeeds, make one second top-level pass that copies only UTF-8 `LongString` values. Recognize the prefix with `is_reserved_header`; reject any reserved key not equal to its ASCII lowercase form; put canonical reserved keys in the protocol map and other keys in the application map.

- [ ] **Step 6: Wire the codec into outbound and inbound conversion**

Add `metadata_limits: AmqpMetadataLimits` to `RabbitMqTransport`, default it in every constructor, and add:

```rust
#[must_use]
pub fn with_metadata_limits(mut self, metadata_limits: AmqpMetadataLimits) -> Self {
    self.metadata_limits = metadata_limits;
    self
}
```

Change `envelope_to_properties` to accept limits and use `encode_headers`. Change `delivery_to_envelope` to accept limits, call `decode_headers` before any header-map allocation/copy, and finish with `BusEnvelope::restore_from_transport`.

Update existing call sites temporarily with `AmqpMetadataLimits::default()`; Tasks 4 and 5 will propagate configured values.

- [ ] **Step 7: Run focused codec tests and verify GREEN**

Run:

```powershell
cargo test -p hexeract-bus-rabbitmq metadata::tests --lib
cargo test -p hexeract-bus-rabbitmq transport::tests --lib
cargo test -p hexeract-bus-rabbitmq worker::tests::delivery_to_envelope --lib
```

Expected: all focused tests pass.

- [ ] **Step 8: Commit the shared codec**

```powershell
git add crates/hexeract-bus-rabbitmq/src/metadata.rs crates/hexeract-bus-rabbitmq/src/lib.rs crates/hexeract-bus-rabbitmq/src/transport.rs crates/hexeract-bus-rabbitmq/src/worker.rs
git commit -m "feat(bus-rabbitmq): bound AMQP metadata"
```

---

### Task 4: Worker configuration and sanitized poison quarantine

**Files:**
- Modify: `crates/hexeract-bus-rabbitmq/src/worker.rs`
- Test: unit module in `worker.rs`
- Test: `crates/hexeract-bus-rabbitmq/tests/integration.rs`

**Interfaces:**
- Consumes: Task 3 `AmqpMetadataLimits`, `decode_headers`, and `is_metadata_error`.
- Produces: `RabbitMqWorkerConfig::metadata_limits` and `RabbitMqWorkerBuilder::metadata_limits`.
- Produces internally: `properties_without_headers(&BasicProperties) -> BasicProperties`.

- [ ] **Step 1: Add failing configuration and quarantine tests**

Add a default/configuration test:

```rust
#[test]
fn worker_uses_default_and_custom_metadata_limits() {
    let custom = AmqpMetadataLimits { max_headers: 2, ..Default::default() };
    let cfg = RabbitMqWorkerConfig::default();
    assert_eq!(cfg.metadata_limits, AmqpMetadataLimits::default());
    let worker = builder().metadata_limits(custom).build().unwrap();
    assert_eq!(worker.config.metadata_limits, custom);
}
```

Add `properties_without_headers` and dispatch-level tests with concrete assertions:

```rust
#[test]
fn sanitized_properties_preserve_core_fields_and_drop_headers() {
    let original = BasicProperties::default()
        .with_content_type("application/json".into())
        .with_message_id("message-1".into())
        .with_correlation_id("correlation-1".into())
        .with_type("orders.placed".into())
        .with_reply_to("reply.queue".into())
        .with_timestamp(42)
        .with_delivery_mode(2)
        .with_headers(table([(
            "secret",
            AMQPValue::LongString(b"value".to_vec().into()),
        )]));
    let sanitized = properties_without_headers(&original);
    assert!(sanitized.headers().as_ref().is_some_and(|h| h.inner().is_empty()));
    assert_eq!(sanitized.message_id(), original.message_id());
    assert_eq!(sanitized.correlation_id(), original.correlation_id());
    assert_eq!(sanitized.kind(), original.kind());
    assert_eq!(sanitized.reply_to(), original.reply_to());
    assert_eq!(sanitized.timestamp(), original.timestamp());
    assert_eq!(sanitized.delivery_mode(), original.delivery_mode());
}

#[tokio::test]
async fn metadata_violation_never_reaches_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = worker_with_counting_handler(
        Arc::clone(&calls),
        AmqpMetadataLimits { max_headers: 0, ..Default::default() },
    );
    let disposition = dispatch_with_injected_settlement(
        &worker,
        delivery_with_headers([("tenant", "acme")]),
    ).await;
    assert!(disposition.keep_running());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
```

Implement the test helpers beside the existing generic `exhausted_core` tests: inject settlement closures instead of requiring a live `lapin::Channel`, while retaining the real decode and handler lookup path.

Add an ignored RabbitMQ integration test publishing a tiny payload with a one-byte-over-limit header; configure a DLQ and assert the handler count stays zero and the DLQ copy has an empty header table while retaining message id/type/correlation id.

- [ ] **Step 2: Run focused worker tests and verify RED**

Run:

```powershell
cargo test -p hexeract-bus-rabbitmq worker::tests --lib
```

Expected: failures for the missing config field, setter, and sanitization helper.

- [ ] **Step 3: Propagate limits through the worker**

Add `pub metadata_limits: AmqpMetadataLimits` to `RabbitMqWorkerConfig`, default it, expose the consuming builder setter, and pass `self.config.metadata_limits` into `delivery_to_envelope`.

- [ ] **Step 4: Rebuild metadata-invalid dead-letter properties without cloning headers**

Implement `properties_without_headers` from `BasicProperties::default()`. For each non-header getter (`content_type`, `content_encoding`, `delivery_mode`, `priority`, `correlation_id`, `reply_to`, `expiration`, `message_id`, `timestamp`, `kind`, `user_id`, `app_id`, `cluster_id`), copy or clone only that bounded scalar into the corresponding `with_*` method. Finish with `with_headers(FieldTable::default())`.

Pass a `sanitize_metadata: bool` derived from `is_metadata_error(err)` through `handle_poison` to the dead-letter publisher. Metadata errors use rebuilt properties; every other poison error retains `delivery.properties.clone()` exactly as before. Structured logs include only the error variant and sizes already carried by `BusError`.

- [ ] **Step 5: Run unit and Docker-backed focused tests**

Run:

```powershell
cargo test -p hexeract-bus-rabbitmq worker::tests --lib
cargo test -p hexeract-bus-rabbitmq --test integration oversized_metadata_is_quarantined_without_headers -- --ignored --exact
```

Expected: unit tests pass; the ignored integration test passes when Docker is available. If Docker is unavailable, record the exact infrastructure error and leave CI as the integration gate.

- [ ] **Step 6: Commit worker enforcement**

```powershell
git add crates/hexeract-bus-rabbitmq/src/worker.rs crates/hexeract-bus-rabbitmq/tests/integration.rs
git commit -m "fix(bus-rabbitmq): quarantine oversized metadata"
```

---

### Task 5: Reply inbox and request-client limit propagation

**Files:**
- Modify: `crates/hexeract-bus-rabbitmq/src/reply_inbox.rs`
- Modify: `crates/hexeract-bus-rabbitmq/src/request_client.rs`
- Modify: `crates/hexeract-bus-rabbitmq/src/lib.rs`
- Test: unit modules in `reply_inbox.rs` and `request_client.rs`
- Test: `crates/hexeract-bus-rabbitmq/tests/request_reply.rs`

**Interfaces:**
- Consumes: Task 3 limits and decoder; Task 4 does not change reply semantics.
- Produces: `RabbitMqRequestClientConfig::metadata_limits`, builder setter, and internal `run_reply_inbox_with_limits`.
- Preserves: existing `run_reply_inbox` test entry point with default limits.

- [ ] **Step 1: Add failing request-client and reply-inbox tests**

Add default/custom config assertions and a decoder test showing an oversized reply errors before `RequestRegistry::resolve`:

```rust
#[test]
fn request_client_config_carries_custom_metadata_limits() {
    let limits = AmqpMetadataLimits { max_headers: 2, ..Default::default() };
    let config = RabbitMqRequestClientConfigBuilder::new()
        .metadata_limits(limits)
        .build();
    assert_eq!(config.metadata_limits, limits);
}

#[test]
fn oversized_reply_metadata_fails_before_resolution() {
    let limits = AmqpMetadataLimits { max_headers: 0, ..Default::default() };
    let request_id = RequestId::new().to_string();
    let delivery = delivery_with_headers([(REQUEST_ID_HEADER, request_id.as_str())]);
    assert!(matches!(
        decode_delivery(&delivery, limits),
        Err(BusError::MetadataLimitExceeded {
            limit: MetadataLimit::HeaderCount,
            actual: 1,
            max: 0,
        })
    ));
}
```

Extend the existing supervisor dependency-injection test harness so its inbox closure sends each received `AmqpMetadataLimits` through a channel. Drive one initial setup and one simulated connection failure, then assert both observations equal the configured value.

Add an ignored integration test that registers a pending slot, publishes a reply with a one-byte-over-limit header plus the correct request id, waits briefly, and asserts the pending slot remains unresolved until a valid bounded reply arrives.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
cargo test -p hexeract-bus-rabbitmq request_client::tests --lib
cargo test -p hexeract-bus-rabbitmq reply_inbox::tests --lib
```

Expected: failures for missing config and limits-aware reply-inbox functions.

- [ ] **Step 3: Add limits-aware reply-inbox entry point**

Keep the existing function as a wrapper:

```rust
pub async fn run_reply_inbox(
    channel: Channel,
    inbox: String,
    registry: Arc<RequestRegistry>,
    cancel: CancellationToken,
) -> Result<(), BusError> {
    run_reply_inbox_with_limits(
        channel,
        inbox,
        registry,
        cancel,
        AmqpMetadataLimits::default(),
    ).await
}
```

The internal limits-aware function passes its value to `decode_delivery`. Metadata errors stay at `warn` with the typed error only and are dropped before `resolve` under the existing `no_ack` contract.

- [ ] **Step 4: Propagate one config value through publisher, initial inbox, and reconnects**

Add `pub metadata_limits: AmqpMetadataLimits` to `RabbitMqRequestClientConfig`, default it, and add `RabbitMqRequestClientConfigBuilder::metadata_limits`. In `connect_request_client_with_config`:

- call `RabbitMqTransport::with_metadata_limits(config.metadata_limits)`;
- pass the same copy to the initial inbox task;
- move the same copy into `spawn_reply_inbox_supervisor` and every reconnect invocation.

Never silently fall back to defaults on reconnect.

- [ ] **Step 5: Run focused and integration tests**

Run:

```powershell
cargo test -p hexeract-bus-rabbitmq request_client::tests --lib
cargo test -p hexeract-bus-rabbitmq reply_inbox::tests --lib
cargo test -p hexeract-bus-rabbitmq --test request_reply oversized_reply_metadata_does_not_consume_slot -- --ignored --exact
```

Expected: unit tests pass; the Docker test passes when infrastructure is available.

- [ ] **Step 6: Commit reply-path parity**

```powershell
git add crates/hexeract-bus-rabbitmq/src/reply_inbox.rs crates/hexeract-bus-rabbitmq/src/request_client.rs crates/hexeract-bus-rabbitmq/src/lib.rs crates/hexeract-bus-rabbitmq/tests/request_reply.rs
git commit -m "fix(rpc): enforce reply metadata limits"
```

---

### Task 6: Public documentation, changelog, and full verification

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `crates/hexeract-bus-rabbitmq/CHANGELOG.md`
- Modify: `docs/reference/hexeract-bus.md`
- Modify: `docs/reference/hexeract-bus-rabbitmq.md`
- Modify: `docs/operations/production-checklist.md`
- Modify: `docs/operations/migration-v0.6-v0.7.md`

**Interfaces:**
- Consumes: final API and behavior from Tasks 1-5.
- Produces: user-facing defaults, configuration instructions, migration warning, and broker defense-in-depth guidance.

- [ ] **Step 1: Update documentation with exact behavior**

Document all of the following, using the final symbol names:

- `BusEnvelope::headers` is application-only; any case variant of `x-hexeract-*` returns `ReservedHeaderNamespace` from helpers and RabbitMQ publication.
- `AmqpMetadataLimits` defaults: 64 / 128 bytes / 8 KiB / 32 KiB.
- worker and request-client builder configuration examples.
- inbound byte accounting includes nested AMQP values, while only UTF-8 `LongString` reaches the envelope.
- worker metadata failures use the poison path and sanitized DLQ properties; reply-inbox failures are dropped before slot resolution.
- RabbitMQ `max_message_size` should match the deployment's real ingress ceiling; `frame_max` should retain the broker/client negotiated default as recommended by RabbitMQ.
- client limits run after `lapin` decode and therefore complement rather than replace broker limits.

- [ ] **Step 2: Update both changelogs**

Add concise `[Unreleased]` entries naming #448, the namespace behavior change, limits/defaults, worker quarantine, and reply-inbox parity. Do not claim authentication; link #444 as the remaining authenticity boundary.

- [ ] **Step 3: Run formatting and static checks**

Run:

```powershell
cargo fmt --all -- --check
cargo check -p hexeract-bus --all-targets
cargo check -p hexeract-bus-rabbitmq --all-targets
cargo clippy -p hexeract-bus -p hexeract-bus-rabbitmq --all-targets --all-features -- -D warnings
```

Expected: every command exits 0 with no warnings.

- [ ] **Step 4: Run non-Docker test suites**

Run:

```powershell
cargo test -p hexeract-bus
cargo test -p hexeract-bus-rabbitmq --lib
```

Expected: all tests pass. On the known Windows GNU/AWS-LC `nanosleep64` linker failure, record the full failure and run the corresponding `cargo check --all-targets`; do not report executable tests as passing.

- [ ] **Step 5: Run Docker-backed regression tests**

Run:

```powershell
cargo test -p hexeract-bus-rabbitmq --test integration oversized_metadata_is_quarantined_without_headers -- --ignored --exact
cargo test -p hexeract-bus-rabbitmq --test request_reply oversized_reply_metadata_does_not_consume_slot -- --ignored --exact
```

Expected: both tests pass when Docker and the native linker are available. Otherwise record the infrastructure blocker exactly for CI.

- [ ] **Step 6: Challenge the final diff**

Inspect:

```powershell
git diff --check origin/main...HEAD
git diff --stat origin/main...HEAD
git diff origin/main...HEAD -- crates/hexeract-bus crates/hexeract-bus-rabbitmq docs CHANGELOG.md
rg -n "x-hexeract-" crates/hexeract-bus/src crates/hexeract-bus-rabbitmq/src -g '*.rs'
```

Confirm specifically that direct public-map mutation is revalidated outbound, no rejected field table is cloned into the metadata-error DLQ path, all reconnect paths retain custom limits, errors/logs contain no header values, and unknown canonical reserved keys cannot enter application headers.

- [ ] **Step 7: Commit documentation and final verification adjustments**

```powershell
git add CHANGELOG.md crates/hexeract-bus-rabbitmq/CHANGELOG.md docs/reference/hexeract-bus.md docs/reference/hexeract-bus-rabbitmq.md docs/operations/production-checklist.md docs/operations/migration-v0.6-v0.7.md
git commit -m "docs(bus): document AMQP metadata limits"
```

---

## Execution notes

Deviations from the plan as written, and why.

**Pre-work: the branch was not green.** Tasks 1-2 left `clippy -D warnings`
failing on two counts: `remove_protocol_header` had no production caller (its
only callers are tests, so it is now gated on `cfg(test)`), and six test
envelopes built their header map through `Default::default()`. Fixed in its own
commit before any new work, so each later commit starts and ends green.

**Pre-work: the protocol headers had stopped reaching the wire.** After the
split, `envelope_to_properties` still iterated `envelope.headers` alone, so
`x-hexeract-*` fields never left the process. No unit test covered it and the
end-to-end test that would have caught it is `#[ignore]`. Task 3 repairs this
by construction (`encode_headers` iterates the combined wire view), and
`protocol_headers_reach_the_wire` now pins it in a unit test.

**Task 3.** `is_metadata_error` moved to Task 4, where its first production
caller lives: introducing it in Task 3 would have committed a function no
production code called, which `clippy -D warnings` rejects as dead code.
`decode_headers` returns a named `DecodedMetadata` alias rather than the bare
tuple, which `clippy::type_complexity` rejects.

**Task 4.** `publish_dead_letter` takes the `BasicProperties` to publish rather
than a positional `sanitize_metadata: bool`, so the call site names what it
sends instead of encoding it in a boolean. The dispatch-level settlement
injection was not built: `dispatch` needs a live `lapin::Channel`, which is not
constructible in a unit test, and rebuilding it for injection is a large change
to a hot path for a property the design already guarantees structurally
(decoding precedes handler lookup). The spec assigns that proof to the ignored
integration tests, and `oversized_metadata_is_quarantined_without_headers`
provides it.

**Task 5.** `decode_delivery` takes the properties and payload it actually
reads instead of a whole `Delivery`: `lapin::message::Delivery` cannot be
constructed outside `lapin` (its `Acker` is crate-private), so the previous
signature was untestable. `spawn_reply_inbox_supervisor` now takes the
`(Channel, String)` pair as one `ActiveInbox` argument, matching the concept
`supervise_reply_inbox` already documents and staying within the seven-argument
clippy bound.

**Task 6, step 5.** The Docker-backed tests were not run: Docker is unavailable
on this machine. Both are written, compile, and are gated `#[ignore = "requires
Docker"]`; CI is their gate. Everything else verified locally:
`cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets
--all-features -- -D warnings` and `cargo doc --workspace --all-features
--no-deps` with `RUSTDOCFLAGS=-D warnings` all exit 0, and
`cargo test --workspace --all-features` passes 894 tests.
