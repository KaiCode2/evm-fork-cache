# Optional canonical transport receipt timing

With the default-off `ingress-timing` Cargo feature enabled,
`AlloySubscriber::with_transport_receipt_lookup` installs an application-owned
lookup from the exact raw result consumed by an Alloy subscription to a
`TransportReceipt`. The timestamp must be captured when the client receives the
complete message, before JSON-RPC decoding. It is not a provider-produced time,
block timestamp, or timestamp taken when this callback runs.

The callback must be bounded and nonblocking, and return `None` for ambiguous
duplicates, expired/evicted receipt identity, or unavailable clocks. An application
must not simply choose the earliest identical payload from several connections.
The callback never decides whether a record is accepted. Failure to associate a
receipt leaves the canonical input's timing absent and preserves state delivery.

`SubscriberInputRecord::canonical_timing` carries the optional receipt, raw
subscription dequeue time and typed decode completion through the existing
bounded pending queue. Backfilled/polled inputs have no fabricated receipt clock.
The existing preconfirmation timing and canonical control/state semantics are
unchanged. These process-local `Instant`s are not durable restart provenance.

Validation includes a real Alloy broadcast subscription consumed through the
normal gap-observing stream: a 30ms earlier transport timestamp survives the
queue and typed decode, while the payload remains unchanged. Existing gap tests
continue to require explicit loss notification. No extra provider operation is
introduced. The complete release matrix remains required before publication.

CI compatibility also replaces the checkpoint generation allocator's deprecated
`fetch_update` call with an equivalent acquire/release compare-exchange loop.
This keeps Rust 1.90 compatibility without adopting the newer replacement API;
checked increment still rejects generation exhaustion without wrapping.

## Alloy integration and public boundary

The inspected consumer locks Alloy 1.6.3. `alloy-rpc-client::ClientBuilder::layer`
uses Tower middleware around RPC request/response transport calls. Unsolicited
subscription messages take the `PubSubConnect` backend path and reach
`RawSubscription::recv` as `Box<RawValue>` with no receive clock. A request layer
cannot recover a consumed notification's transport origin. Applications may use a
custom `PubSubConnect` backend to observe complete-message receipt, then supply
this optional generic lookup; this crate does not install or own that backend.
Alloy's HTTP `TraceParentLayer` (the default-enabled `traceparent` feature in
1.6.3) propagates application tracing context via W3C headers; it does not install
an exporter. This extension follows application-owned instrumentation boundaries,
not every Alloy feature default. It carries local clocks rather than trace
headers, so it adds no OpenTelemetry dependency. Its default-off Cargo feature
is an explicit consumer requirement.

Public additions are the immutable `TransportReceipt` and
`CanonicalIngressTiming` types (checked constructors and read-only accessors),
the `TransportReceiptLookup` callback alias, the subscriber builder method and
`SubscriberInputRecord::canonical_timing`. Existing constructors remain valid;
new fields are private on existing types. The callback rustdoc includes a
backend-neutral one-shot in-memory/tracing example. No metrics/exporter dependency,
bot labels, runtime task or provider request is added. With the Cargo feature
enabled but lookup disabled, no additional receipt clock reads or callback work
run; optional queue metadata still adds fixed record size and an `Option` branch.
With the Cargo feature disabled, that metadata and new code are absent. Enabled callbacks must honor the
bounded/nonblocking contract. Existing execution clocks are unchanged.

The PR base gap includes `ab08a5702982eba87d8d590120ef1076de8a3342`, the prior
v0.4.0 release commit. The actual registry v0.4.0 `.cargo_vcs_info.json` identifies
that same commit. Its version/dependency/CI release changes predate this telemetry
work; review telemetry relative to that published source. The proposed additive
next release is 0.4.1 after full review and explicit publication authorization.

## Optional ingress timing feature

The `ingress-timing` Cargo feature is disabled by default. Without it, new
receipt types, callback configuration, accessors, inline metadata fields and
receipt clockwork are not compiled. Existing canonical and Flashblock timing
APIs keep their prior behavior. Enabling the feature adds only generic data and
hooks; consumers must still explicitly install a bounded receipt lookup. No
exporter, tracing subscriber, global setup or runtime task is installed.

Use `features = ["ingress-timing"]` alongside the existing transport features.
Default/default-off and enabled builds, compile-fail API absence, record-layout
regression and a real consumed-subscription timing fixture are covered in CI.
This is a compatible extension at Alloy's pubsub boundary, not an Alloy-provided
telemetry subsystem. Request Tower layers do not expose unsolicited receipt time.
