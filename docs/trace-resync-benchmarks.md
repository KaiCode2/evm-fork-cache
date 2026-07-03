# Trace-backed resync benchmarks

Captured on July 2, 2026 while evaluating Phase 8 Tier-3 state-diff resync.
This note records the measured behavior of:

- `debug_traceBlockByNumber` with Geth `prestateTracer` `diffMode: true`
- batched `eth_getStorageAt` point reads for known slots
- gzip-compressed HTTPS responses for large trace payloads

The core takeaway is deliberately split into three axes:

| Axis | Result |
| --- | --- |
| Alchemy compute units | Trace wins once it replaces 3 or more point storage reads in the same block. |
| Latency without compression | Storage batches stayed faster through the clean measurable range on the public endpoint tested. |
| Latency with gzip on Alchemy | Gzip substantially reduced one large trace response's transfer time. |

## RPC economics

Alchemy's published CU table lists:

- `eth_getStorageAt`: 20 CU
- `eth_getProof`: 20 CU
- `debug_traceBlockByNumber`: 40 CU

That means the CU break-even is two point reads:

| Storage slots repaired from one block | Point-read CU | Trace CU | Trace CU result |
| ---: | ---: | ---: | --- |
| 1 | 20 | 40 | 2x more expensive |
| 2 | 40 | 40 | break-even |
| 3 | 60 | 40 | 33% cheaper |
| 10 | 200 | 40 | 5x cheaper |
| 100 | 2000 | 40 | 50x cheaper |

This is a billing/throughput result, not a latency result. Trace responses are
large JSON payloads and can still arrive later than a small storage batch.

Sources:

- Alchemy CU table: https://www.alchemy.com/docs/reference/compute-unit-costs
- Alchemy debug endpoint: https://www.alchemy.com/docs/node/debug-api/debug-api-endpoints/debug-trace-block-by-number
- Geth tracer behavior: https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers

## Latency benchmark: trace vs batched storage

### Setup

- Pool: Uniswap V3 USDC/WETH 5 bps
  `0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640`
- Trigger: live `Swap(address,address,int256,int256,uint160,uint128,int24)`
  subscription plus historical blocks containing Swap logs.
- Trace call:
  `debug_traceBlockByNumber(block, {"tracer":"prestateTracer","tracerConfig":{"diffMode":true}})`
- Storage calls:
  one JSON-RPC batch of `eth_getStorageAt(pool, slot, block)` for slots
  `0..N-1`.
- Endpoint for the scaling run: MEW public Ethereum RPC, which supports both
  `debug_traceBlockByNumber` and JSON-RPC storage batches.

This benchmark intentionally measures end-to-end RPC response latency from one
client process. It does not isolate node execution time from network transfer
time.

### Live Swap-triggered samples

| Slots | Trace latency | Storage batch latency | Winner |
| ---: | ---: | ---: | --- |
| 1 | 455 ms | 111 ms | storage |
| 8 | 418 ms | 73 ms | storage |
| 32 | 282 ms | 35 ms | storage |
| 128 | 610 ms | invalid | storage batch had 29 rate-limit errors |

### Historical Swap-block scaling

Clean successful median latencies:

| Slots | Trace median | Storage median | Trace wins | Storage wins |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 480 ms | 34 ms | 0 | 4 |
| 2 | 352 ms | 55 ms | 1 | 3 |
| 4 | 476 ms | 34 ms | 0 | 4 |
| 8 | 305 ms | 48 ms | 0 | 4 |
| 16 | 645 ms | 31 ms | 0 | 4 |
| 32 | 277 ms | 47 ms | 0 | 4 |
| 64 | 332 ms | 92 ms | 1 | 3 |
| 80 | 182 ms | 42 ms | 0 | 4 |
| 96 | 486 ms | 233 ms | 0 | 2 |

No reliable latency crossover was observed before the endpoint's clean batch
limit. The same public endpoint started returning per-item rate-limit errors near
100 `eth_getStorageAt` requests per second, even when sent as one JSON-RPC
batch. A noisy linear interpolation suggested a possible crossover around 260
slots, but that is outside the clean measurable region and should not be treated
as a production threshold.

Interpretation:

- For small and medium known slot sets, batched storage reads are latency-faster.
- Trace can still be preferable when CU budget, completeness, or avoiding many
  point reads matters more than wall-clock latency.
- Provider behavior matters. Public RPC endpoints differ materially in debug
  namespace support, batch limits, response caching, and rate limits.

## Alchemy gzip test

### Setup

- Endpoint: Alchemy Ethereum mainnet HTTPS.
- Block: `25444665` (`0x1844139`).
- Payload: same `debug_traceBlockByNumber` prestate diff call.
- Client: raw HTTPS with automatic decompression disabled, so wire bytes,
  decompression time, and JSON parse time were measured separately.
- Comparison:
  - Standard: `Accept-Encoding: identity`
  - Gzip: `Accept-Encoding: gzip`

### Result

| Mode | Wire bytes | Full response received | Parsed and ready |
| --- | ---: | ---: | ---: |
| Standard HTTPS (`identity`) | 4.48 MB | 664 ms | 671 ms |
| Gzip HTTPS (`gzip`) | 1.03 MB | 268 ms | 286 ms |

Observed improvement:

- Wire payload reduction: 76.9%
- Receive-time improvement: 397 ms
- Parsed-and-ready improvement: 385 ms
- End-to-end parsed latency improvement: about 57%
- Local gzip decompression cost: 11 ms

A separate concurrent sanity check on another block also reduced wire bytes by
78.7%, but gzip arrived slower in that single paired sample. Treat compression as
a strong payload-size win and a likely latency win for large traces, but keep
latency thresholds empirical per provider.

## Runtime policy guidance

1. Always request gzip for debug/trace RPC over HTTPS when the provider supports
   it. The payload is large, repetitive JSON and compresses well.
2. Do not replace small known-slot repairs with trace solely for latency. Storage
   batches were faster up to the clean measured range.
3. Keep the trace source as an accelerator with fallback, not a hard dependency.
   `debug`/`trace` namespaces are commonly gated or disabled.
4. Make the trace decision adaptive:
   - Use point reads for low slot counts on latency-sensitive hot paths.
   - Use trace when expected unresolved targets per block are high enough, when
     CU budget dominates, or when whole-block completeness is valuable.
   - Continue falling back to point reads for cold targets absent from the trace
     diff.
5. Separate thresholds by objective:
   - CU threshold on Alchemy: trace breaks even at two point reads and wins at
     three or more.
   - Latency threshold: not established by the public-endpoint run; after gzip,
     it should be measured directly on the target Alchemy plan and region.

## Follow-up benchmark to run before hard-coding defaults

Run the same scaling harness directly against Alchemy with:

- gzip enabled for trace
- the same Alchemy endpoint for storage batches
- slot counts past 100 if the plan's rate limits allow it
- at least 20 samples per slot count bucket
- separate reporting for transfer time, decompression time, JSON parse time, and
  provider errors

Until that exists, the implementation should expose configuration rather than
hard-code a latency threshold.
