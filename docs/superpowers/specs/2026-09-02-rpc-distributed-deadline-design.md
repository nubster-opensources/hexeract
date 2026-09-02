# RPC distributed deadline design

## Goal

Close issue #440 by carrying the caller's effective timeout across the bus as an
absolute deadline, so that a responder can refuse work the caller has already
abandoned. A local timeout alone releases the caller's correlation slot but
leaves the published request queued, and that request later triggers work whose
result nobody is waiting for.

The deadline grants a responder the right to refuse expired work. It is not
remote cancellation: a handler already running is never interrupted from the
outside, and the documentation must state this without ambiguity.

## Clock model

A distributed deadline needs two clocks with different properties, and
conflating them is the main hazard of this feature.

The wall clock (`SystemTime`) is comparable between machines, which is what
makes an absolute deadline meaningful across a process boundary. It can also
jump backwards, be corrected by NTP, or drift.

The monotonic clock (`tokio::time::Instant`) never jumps and measures elapsed
time reliably, but carries no meaning outside the local process.

The design reads the wall clock exactly once per inbound request, at the moment
the deadline header is parsed, and converts the deadline into a monotonic
anchor. Every later decision, including the pre-publication recheck and the
remaining time reported to the handler, reads only the monotonic clock.

Two consequences follow. A wall-clock adjustment occurring while a handler runs
cannot corrupt the recheck. And because `tokio::time::pause` advances
`tokio::time::Instant` but never `SystemTime`, the conversion boundary is what
makes the whole flow testable under `#[tokio::test(start_paused = true)]`, as
issue #440 requires.

## Wire representation

The reserved header `x-hexeract-deadline` already exists in
`crates/hexeract-bus/src/rpc_protocol.rs` with no reader and no writer. This
work gives it both.

The value is the deadline expressed as decimal Unix milliseconds in UTC. A
single instant therefore has a single valid rendering, which keeps
canonicalisation trivial for the envelope signature of #444, and parsing needs
no additional dependency.

The header is written through `BusEnvelope::insert_protocol_header`, so it lives
in the framework-owned protocol map rather than in the application headers, and
falls under the reserved namespace bounded by #448.

## Bounds and clock skew

Two internal constants govern acceptance. Neither is exposed publicly, so
neither widens the semver surface that #537 is already tracking.

`MAX_DEADLINE_HORIZON` is one hour. A deadline further in the future than the
horizon is rejected as a protocol violation. The bound serves two purposes: it
makes the arithmetic total, since adding an unbounded millisecond count to a
`SystemTime` can overflow, and it refuses values that carry no meaning for a
request-reply call. Issue #440 requires that an out-of-range deadline produce a
protocol error and never a panic.

`CLOCK_SKEW_TOLERANCE` is one second. A deadline that elapsed less than the
tolerance ago is still honoured. Without it, a responder whose clock runs one
second fast rejects every request from a correctly configured caller, and
because expiry is dropped silently that failure would carry no signal to the
caller at all. The tolerance is deliberately small: it absorbs ordinary NTP
drift and nothing more. Sustained skew is an operational fault to fix, not a
condition to accommodate.

The tolerance is applied once, when the wire deadline is anchored onto the
monotonic clock, and is therefore already carried by the resulting
`LocalDeadline`. The pre-publication recheck adds nothing further: a single
request is judged against one anchor throughout its lifetime, never against a
tolerance that would compound at each check.

## Public surface

Three types live in a new `crates/hexeract-bus/src/deadline.rs`.

`Deadline` wraps a `SystemTime` and represents the absolute instant shared
across processes. It converts to and from decimal Unix milliseconds, and anchors
itself onto the monotonic clock through `to_local`, which applies the skew
tolerance and returns `None` when the deadline has already elapsed.

`LocalDeadline` wraps a `tokio::time::Instant` and represents the same deadline
seen from the local monotonic clock. It answers how much time remains, whether
it has expired, and exposes the underlying instant for callers driving
`tokio::time::timeout_at`.

`DeadlineViolation` names the two ways a wire deadline cannot be honoured:
`Unreadable` when the value is not a decimal millisecond count, and
`BeyondHorizon` when it exceeds `MAX_DEADLINE_HORIZON`.

`rpc_protocol.rs` gains `read_deadline`, mirroring the existing
`read_protocol_version`. It returns `Ok(None)` when the caller set no deadline,
which is a valid request and not a violation.

`RequestContext` gains a `deadline: Option<LocalDeadline>` field and a
`with_deadline` builder. The type is already `#[non_exhaustive]` and its
documentation already anticipates this extension, so `new` keeps its signature
and no existing caller breaks. `RequestContext::remaining` recomputes the
remaining duration on every call rather than freezing it at reception, so a
handler that consults it after thirty seconds of work sees thirty seconds less.

`RequestClient` gains no public API at all. It already resolves an effective
timeout from `RequestOptions::timeout` or its configured default, and it already
builds a single monotonic deadline covering both publication and waiting. The
change writes the corresponding wall-clock deadline into the outbound envelope.

## Enforcement flow

### Caller

The effective timeout resolved in `request_inner` produces a `Deadline`, written
to the outbound envelope as a protocol header. Publication and waiting keep
using the existing monotonic deadline, and the RAII removal of the correlation
slot on timeout is unchanged.

### Responder guard

`replied_handler.rs` runs four guards before executing a handler: reply
destination validation, request identity parsing, protocol version check, then
payload decoding. The deadline guard is inserted between the protocol version
check and payload decoding.

The position is deliberate. It runs after the version check because
interpreting a protocol header presupposes knowing which protocol is being
spoken. It runs before decoding because deserialising a payload whose work is
already known to be pointless wastes the exact resources the deadline exists to
protect.

The guard produces one of four outcomes. An absent deadline leaves current
behaviour untouched. A live deadline is anchored and attached to the request
context. An elapsed deadline drops the request, acknowledges it so that it does
not loop, and increments a rejection counter. An unreadable or out-of-range
deadline publishes a sanitized remote protocol error, matching how the existing
protocol version guard already treats an unsupported version.

The split between dropping and answering follows the existing guards. Silent
drop is what reply-destination and request-identity failures already do; a
published remote error is what a version mismatch already does. Expiry is a
nominal event and the caller has already failed locally, so it is dropped. A
malformed deadline is a protocol defect worth reporting.

### Pre-publication recheck

Before `publish_reply`, the responder rechecks the anchored deadline. If it has
expired, the reply is not published, the request is still acknowledged, and a
distinct counter records the drop. Publishing a reply nobody awaits costs a
broker round trip and can only land in an orphaned inbox.

## Protocol compatibility

Adding a header to the existing protocol version 1 is backward and forward
compatible in both directions. A responder built before this change ignores an
unknown protocol header. A responder built after it treats a request without the
header exactly as it behaves today. `PROTOCOL_VERSION` is therefore not bumped.

## Authentication compatibility

The deadline is a protocol header, so it is written through the private protocol
API and sits inside the reserved namespace that #448 bounded. Issue #444 must
cover it in the canonical representation it signs, alongside payload, message
type, identities, destination, `reply_to`, audience and reserved headers.

Delivering #440 before #444 is what allows that canonical form to be defined
once over a complete field set. Signing an envelope whose deadline field does
not yet exist would fix an incomplete format that a later protocol version would
have to replace.

Until #444 lands, a deadline can be forged in transit. Forging it later only
shortens or lengthens the responder's willingness to work on a request the
attacker already controls, so it does not create a new capability. This is a
known and bounded gap, not an accepted permanent state.

## Test strategy

Unit tests live inline in each source file, matching the existing convention in
`crates/hexeract-bus/src/`.

`deadline.rs` covers the millisecond round trip, an unreadable value, a value
beyond the horizon, a deadline elapsed within the skew tolerance and therefore
honoured, and one elapsed beyond it and therefore refused.

`request_client.rs` verifies that the outbound envelope carries a deadline
derived from the effective timeout, for both the client default and an explicit
`RequestOptions::with_timeout`.

`request_context.rs` verifies that the remaining duration decreases as the
monotonic clock advances under `start_paused`.

`replied_handler.rs` covers the four guard outcomes: a request expired before
dispatch never reaches the handler and publishes nothing; a malformed deadline
produces a remote protocol error; a deadline that elapses while the handler runs
suppresses the reply publication; an absent deadline preserves current
behaviour.

`crates/hexeract-bus-rabbitmq/tests/request_reply.rs` covers the end to end path
against a real broker.

Every test that depends on elapsed time uses `#[tokio::test(start_paused = true)]`,
which is possible only because the wall clock is read once at the conversion
boundary.

## Documentation

The request-reply documentation must state that a deadline authorises refusal
and never interruption, that a running handler completes even once its deadline
has passed unless it consults `remaining` itself, and that responder and caller
clocks are expected to be synchronised within the skew tolerance.

## Out of scope

The metrics backend belongs to #441; this work only places the instrumentation
points and their rejection reasons. Covering the deadline field with an envelope
signature belongs to #444. Remote cancellation is not part of the contract and
is not planned. Deadline propagation across a chain of nested calls is left to
the saga work in v0.9.
