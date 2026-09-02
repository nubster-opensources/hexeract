# AMQP metadata limits and reserved namespace design

## Goal

Close issue #448 by bounding the metadata that Hexeract accepts from and sends
to RabbitMQ, and by separating application headers from the framework-owned
`x-hexeract-*` protocol namespace. A small payload must not bypass the worker's
resource bounds through a large AMQP field table, and application publishing
helpers must not be able to collide accidentally with current or future
protocol fields.

## Security boundary

AMQP properties and field tables are untrusted once a broker delivery reaches
the client. `lapin` has already decoded those values before Hexeract can inspect
them, so the application-level limits cannot replace broker ingress limits.
They do prevent unbounded validation, cloning into `BusEnvelope`, storage in an
RPC correlation slot, and downstream handler processing.

On the publishing side, application code is trusted to run in the process but
must not be able to create an ambiguous envelope accidentally through the
ordinary header API. Deliberate code in the process can always bypass policy by
implementing a transport or speaking AMQP directly; that is outside this
boundary. Authentication of remote producers and integrity of protocol fields
remain the responsibility of #444.

The invariant is:

> No RabbitMQ transport operation clones, publishes, dispatches, or resolves an
> envelope whose metadata exceeds the configured limits, and no application
> header can occupy any ASCII case variant of the `x-hexeract-*` namespace.

## Metadata model

`BusEnvelope` keeps its existing public `headers: HashMap<String, String>` as
the application metadata collection. A second private collection stores
framework protocol headers. Existing non-reserved application headers retain
their current representation and visibility.

The core bus crate adds the following operations:

- A shared `RESERVED_HEADER_PREFIX` constant and a non-allocating,
  ASCII-case-insensitive predicate for recognizing the whole namespace.
- Validation in `BusEnvelope::with_headers` and `Transport::publish_with_headers`
  that rejects a reserved application key with a typed `BusError`.
- A crate-private protocol-header insertion API used by request/reply code.
- A read accessor that returns a protocol value for reserved keys and an
  application value otherwise. RPC readers stop indexing the public map
  directly.
- A transport-facing iterator over both collections. The iterator exposes the
  values read-only and never lets an adapter manufacture protocol metadata.
- A transport restoration constructor that receives already-separated
  application and protocol maps. The existing `restore` constructor remains
  available and treats its map as application metadata.

The RabbitMQ decoder recognizes the reserved prefix case-insensitively but only
accepts its canonical lowercase spelling on the wire. A key such as
`X-Hexeract-Request-Id` is invalid metadata rather than an alias for the wire
protocol. Canonical but unknown future `x-hexeract-*` keys enter the private
protocol collection. Application keys remain case-preserving.

This is additive for the released application-header surface. The request/reply
protocol headers are unreleased v0.7 functionality, so moving them behind an
accessor does not break a published contract.

## Configurable limits

`hexeract-bus-rabbitmq` exposes a copyable `AmqpMetadataLimits` value with four
independently configurable fields:

| Limit | Default | Meaning |
| --- | ---: | --- |
| `max_headers` | 64 | Maximum top-level AMQP header entries |
| `max_key_bytes` | 128 | Maximum UTF-8 byte length of any field-table key |
| `max_value_bytes` | 8 KiB | Maximum measured size of one top-level value |
| `max_total_bytes` | 32 KiB | Maximum sum of measured keys and values |

Lengths are byte lengths, never Unicode scalar counts. Exact-limit input is
accepted and a one-byte overflow is rejected. Zero is a valid deny-all value
for the corresponding dimension.

The defaults leave ample room for trace context, tenancy metadata, the RPC
wire fields, and RabbitMQ's normal `x-death` history while remaining well below
the broker's normal frame and message limits.

Existing constructors retain these defaults. Configuration is available at
every relevant owner:

- `RabbitMqTransport::with_metadata_limits` controls outbound validation.
- `RabbitMqWorkerBuilder::metadata_limits` controls normal inbound delivery.
- `RabbitMqRequestClientConfigBuilder::metadata_limits` applies the same value
  to its publisher transport and supervised reply inbox, including reconnects.
- The existing public `run_reply_inbox` keeps its signature and uses defaults;
  a limits-aware entry point is used by the configured request client.

## Deterministic measurement

Outbound envelopes contain string keys and values. Measurement is the checked
sum of the UTF-8 byte length of every key and value. Both application and
protocol collections count toward the same limits.

Inbound field tables can contain any AMQP value, including arrays and nested
tables such as RabbitMQ's `x-death`. Validation therefore measures the whole
decoded value iteratively rather than cloning it or formatting it:

- byte and string values contribute their raw byte length;
- fixed-width scalar values contribute their encoded scalar width;
- arrays contribute every element;
- nested tables contribute every nested key and value;
- checked arithmetic turns overflow into a limit violation.

Only valid UTF-8 `LongString` values are copied into an envelope, preserving the
existing string-only application API. Non-string values remain available to
transport machinery such as retry counting but are not copied downstream.
An invalid UTF-8 `LongString` is rejected instead of being silently dropped.
The iterative walk avoids making attacker-controlled nesting consume the Rust
call stack.

The new backend-agnostic error surface consists of typed, value-free errors:

- reserved application namespace;
- header-count limit;
- key-byte limit;
- value-byte limit;
- aggregate-byte limit;
- invalid UTF-8 header value;
- non-canonical reserved protocol key.

Errors record only a reason/dimension and the observed and configured sizes
where applicable. They never contain header keys or values.

## Enforcement flow

### Outbound

Every RabbitMQ `publish_envelope` call validates application namespace usage
and measures the combined application and protocol collections before building
a `FieldTable` or acquiring a pooled channel. Failure therefore guarantees
that no publish attempt reached the broker. Framework RPC code inserts its
reserved fields only through the private protocol API.

### Normal worker

`delivery_to_envelope` receives `AmqpMetadataLimits` alongside the payload cap.
It validates and measures `BasicProperties::headers` before allocating either
header map or copying a value. A failure follows the existing poison-delivery
path:

- manual acknowledgement without an application dead-letter target uses
  `basic_nack(requeue = false)` so a broker DLX can receive it;
- manual or ack-on-receive with an application dead-letter target republishes
  a sanitized quarantine copy and settles it using the existing confirmed
  path;
- unacknowledged consumption can only perform the existing best-effort
  dead-letter copy because the broker has already settled the delivery.

No typed handler runs on failure. Existing settlement behavior is reused rather
than adding a metadata-specific retry policy.

The sanitized quarantine properties preserve the payload and bounded core AMQP
properties needed to diagnose and route the message (`message_id`,
`correlation_id`, `type`, `content_type`, `reply_to`, `timestamp`, delivery
mode), but rebuild the properties object with an empty field table instead of
cloning the rejected one. The reason and observed sizes remain in value-free
structured logs. This exception applies only to a metadata-validation failure;
other poison deliveries keep the existing raw dead-letter behavior.

### Reply inbox

The reply inbox calls the same decoder with the same limits type. Invalid or
oversized metadata is logged by reason and dropped under its existing `no_ack`
contract before `RequestRegistry::resolve`, so it cannot consume a correlation
slot. Configured limits survive supervisor reconnects.

## Authentication compatibility

Issue #444 will canonicalize the already validated combined metadata view.
This issue does not add signatures or claim authenticity. The split prevents
application helpers from colliding with protocol fields, while the shared
transport iterator gives later authentication code one unambiguous set of
fields to cover. Unknown future `x-hexeract-*` fields remain private protocol
metadata and count toward every limit.

## Documentation

The RabbitMQ reference and production checklist document:

- the four defaults and configuration points;
- byte-based accounting and poison/reply-inbox disposition;
- that `max_message_size` is a broker-side ingress defense and should be set to
  the application's actual maximum rather than relied on as the client limit;
- that RabbitMQ recommends retaining the negotiated `frame_max` default rather
  than tuning it as an application metadata policy;
- that broker limits act before the client, while Hexeract limits bound work
  after `lapin` has decoded a delivery.

## Test strategy

Tests are written before implementation and cover:

- case variants of `x-hexeract-*` rejected by public application helpers and
  direct RabbitMQ envelope publication;
- internal RPC request, success reply, and error reply still carrying their
  canonical protocol fields;
- exact count/key/value/aggregate limits and one-unit overflow;
- Unicode values measured by UTF-8 bytes;
- many small headers exceeding count or aggregate limits;
- invalid UTF-8 and duplicate case-variant protocol headers;
- sanitized dead-letter publication that does not clone rejected metadata;
- nested AMQP values measured without cloning, including a normal `x-death`;
- the normal worker returning the existing poison disposition before a handler;
- the reply inbox dropping the delivery before correlation-slot resolution;
- custom request-client limits surviving the initial inbox and reconnect path;
- default configuration compatibility for ordinary application headers.

Focused unit tests exercise validation without a broker. Existing ignored
RabbitMQ integration tests remain the end-to-end proof for publish, worker,
reply-inbox, and settlement behavior.

## Out of scope

- Authenticating producers, audiences, or metadata; #444 owns that work.
- Changing the payload limit or attempting to prevent `lapin` from decoding a
  broker-accepted frame.
- Exposing arbitrary AMQP value types through `BusEnvelope`.
- Redacting existing `BusEnvelope::Debug` output; #431 owns that work.
- Changing retry, dead-letter, or acknowledgement semantics beyond routing a
  metadata violation through the existing poison path.
