# Optional canonical transport receipt timing

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
