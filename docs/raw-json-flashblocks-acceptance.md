# Raw JSON Flashblocks acceptance

This document records point-in-time evidence for the optional,
transport-independent receipt-enriched JSON adapter. It is not a provider
compatibility promise. Applications must requalify the exact endpoint and wire
profile they intend to use.

## Deterministic boundary

The default-off `raw-flashblocks-json` feature is exercised with one-index,
two-index, replacement, invalidation, malformed, oversized, discontinuous,
duplicate-transaction, receipt-incomplete, stale-generation, bounded-channel,
duplicate-receipt-key, channel-closure, subscriber-rejection recovery,
owner-replacement invalidation, and canonical-reconciliation cases. Direct and
bounded-channel regressions
cover index-zero starts, exact progression, same-index conflict, stable base
identity, cumulative transaction prefixes, and delta-log membership. Runtime
tests prove exact canonical-successor lineage, stale-replay rejection, immediate
canonical restoration, preferred-mode fallback, and required-mode failure for
both channel closure and queued malformed updates. The live probe performs the
zero-Flashblocks-RPC preflight and rejects a canonical chain-id mismatch before
starting its observation window.

The optional buffered wrapper additionally covers exactly one future frame
across one missing index, ordered two-frame drain within a 300–500 millisecond
caller-timed window, deadline invalidation, late-remainder quarantine,
same-index conflict, a second future index, source reset, payload replacement,
malformed input, resource exhaustion, and a property check that no more distant
index enters the buffer. The immediate adapter remains unchanged. The wrapper
performs no provider request, starts no timer, mutates no canonical state, and
does not authorize execution from a provisional update.

Timed coverage additionally proves that each member of an ordered two-frame
gap drain retains the monotonic arrival supplied on its original adapter call.
The standardized subscriber handoff preserves that timing through queued
normalization, record construction, scoped batching, and the public reactive
batch. Native Base coverage retains the earlier pending-log arrival when the
later cumulative preview releases that buffered log. Timing is optional
metadata only; absence cannot be interpreted as a later inferred ingress.

The public package check separately verifies that enabling only
`raw-flashblocks-json` does not add a socket transport to the library dependency
surface. The live example's WebSocket client is a development dependency and
the example is excluded unless both raw conversion and reactive WebSocket
features are selected.

## Live source-to-subscriber control window

On 2026-08-06, the optimized
`raw_json_flashblocks_subscriber_acceptance` example connected a passive raw
source to `RawJsonFlashblocksAdapter` and `AlloySubscriber` while an independent
canonical WebSocket subscribed to matching swap logs. It sent no application
message, issued no HTTP request, and intentionally implemented no retry or
backoff policy.

The run stopped after 100 exact raw/canonical pairs in 162.833 seconds:

| Result | Value |
| --- | ---: |
| Raw swap logs | 102 |
| Canonical swap logs | 100 |
| Exact pairs | 100 |
| Mature raw records unmatched | 0 |
| Closing-edge raw records unmatched | 2 |
| Interior canonical records unmatched | 0 |
| Content mismatches | 0 |
| Raw duplicate identities | 0 |
| Canonical duplicate identities | 0 |
| Raw-first pairs | 100 |
| Canonical-first pairs | 0 |

Raw notification lead was 1,548.368 ms p50, 2,042.045 ms p95, 2,044.087 ms
p99, and 2,044.089 ms maximum. The subscriber processed 81 canonical heads and
81 normal canonical-overlay invalidations. Two unmatched raw records were at
the closing observation boundary and were not counted as mature misses.

This proves that the observed receipt-enriched indexed profile can be converted
into the existing standardized subscriber path and correlated exactly with an
independent canonical stream. It does not prove availability, schema stability,
authentication behavior, retry policy, an application AMM integration, or
permission to execute against speculative state. Those remain consumer-owned
acceptance boundaries.

## Resource-limit policy

The library defaults are deliberately broad compatibility ceilings: 16 MiB per
frame, 64 indexed deltas, 50,000 cumulative transactions, and 200,000 cumulative
logs. They bound untrusted allocation and parsing, but they are not latency
targets and should not be copied blindly into an execution application.

For the observed indexed OP profile, the downstream BIFI dry-run uses an
explicit 4 MiB frame, 16-index, 10,000-transaction, and 40,000-log profile. That
is application policy rather than a new wire-format promise. A consumer should
measure its own source distributions, retain reviewed headroom, and reconnect
or rotate the speculative source when a limit is exceeded. Canonical processing
must remain independent in preferred mode.

A 4,108,968-byte stress frame just below that application byte limit, containing
4,500 transactions and 9,000 logs, measured 10.522 milliseconds (9.621–11.616
milliseconds Criterion interval) and 372.42 MiB/s on the Apple M1 Pro release
build on 2026-08-10. Of 20 samples, one was classified as a high mild outlier and
three as high severe outliers.
The parsing stage precedes downstream decision timing and remains part of
end-to-end signal latency; the count limits independently reject more
allocation-heavy shapes that fit under the byte ceiling.

## Local conversion benchmark

On an Apple M1 Pro release build, the checked-in Criterion workload measured:

| Workload | Mean |
| --- | ---: |
| Initial indexed snapshot | 4.814 microseconds |
| Next cumulative index | 3.440 microseconds |
| 250 transactions / 500 logs | 459.46 microseconds |
| Bounded standardized-update queue admission | 9.496 microseconds |

The scaled conversion processed approximately 217.36 MiB/s. The handoff number
measures non-blocking queue admission only; subscriber validation is separately
acknowledged by the API and intentionally excluded. These local measurements
do not include network delivery, downstream cache application, AMM quoting, or
canonical inclusion.

The checked-in suite also builds a 15,521,468-byte frame containing 17,000
transactions and 34,000 logs, below every default count ceiling and close to the
16 MiB frame ceiling. On the same Apple M1 Pro in a release build on 2026-08-10,
its Criterion point estimate was 43.051 milliseconds (40.628–46.041
milliseconds Criterion interval), or 343.83 MiB/s, with one high mild and one
high severe outlier among 20 samples. Its purpose is to expose the bounded worst-case
parsing cost before release. Production consumers should select smaller limits
unless their measured source requires the broader compatibility envelope.
