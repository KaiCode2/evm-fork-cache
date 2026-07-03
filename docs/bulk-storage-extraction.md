# Bulk storage extraction (`eth_call` state overrides)

Captured on July 2, 2026 against an Alchemy Ethereum-mainnet HTTPS endpoint
(pinned block `25445270`, 3 samples per measurement, gzip-enabled transport).
This note records the design, the measured before/after behavior, and every
limitation hit while building the `bulk_storage` module — **the crate's
default storage loader since 0.2.0**.

> **Re-verified live twice more** (July 4 block `25459219`, July 5 block
> `25466494`, same harness: `RPC_URL=… cargo run --release --example
> bulk_storage_bench`). The **CU costs and ratios below reproduced exactly all
> three times** — they are deterministic (a fixed price per call), so they are
> the durable claim. The wall-clock latencies here are the fastest of the three
> captures and are a **conservative floor**: the re-verify sessions ran on
> constrained networks and measured the same loads *slower* (up to several×),
> never with different CU, so well-provisioned connectivity should meet or beat
> these millisecond figures. Read the CU column and the ratios, not the absolute
> latencies. One chain-state-dependent figure moved with pool activity: both
> re-verify blocks held **7,654** initialized-tick slots (**153,080 CU as point
> reads → 2,944× cheaper**) versus 7,674 / 2,952× here — the count drifts as the
> pool trades; see that section. The verified-code-seeding table below uses the
> July-4 capture.

The core technique is Dedaub's "bulk storage extraction", used here with full
credit:

- blog: <https://dedaub.com/blog/bulk-storage-extraction/>
- reference implementation: <https://github.com/Dedaub/storage-extractor>

## Mechanism in one paragraph

`eth_call` accepts a *state-override set* that can replace the **code** at any
address while leaving its **storage** intact. The fetcher overrides each target
contract with Dedaub's 23-byte extractor
(`0x5f5b80361460135780355481526020016001565b365ff3`), which treats calldata as
a raw array of 32-byte slot keys, `SLOAD`s each one, and returns the packed
values — no selector, no ABI, ~2,664 gas per slot. For multi-contract batches
the canonical Multicall3 runtime is *also* injected (at
`0xcA11bde05977b3631167028862bE2a173976CA11`) and one `aggregate3` call fans
out to every overridden target, so the dispatcher exists on every chain and at
every historical block. See the `bulk_storage` module docs for the annotated
disassembly and the full API.

## RPC economics (Alchemy CU table)

| Method | CU | Covers |
| --- | ---: | --- |
| `eth_getStorageAt` | 20 | one slot |
| `eth_call` | 26 | up to `max_slots_per_call` slots (default 10,000) |
| `eth_callMany` | 20 | **all** chunks of a batch, in one request |
| `debug_traceBlockByNumber` | 40 | one block's *changed* slots |
| `eth_simulateV1` | 40 | works with overrides too, but twice `eth_callMany`'s price — not used |

Break-even against point reads is **two slots**. The bulk call is also cheaper
than the Tier-3 trace path (see
[`trace-resync-benchmarks.md`](trace-resync-benchmarks.md)) whenever the slots
are *known* rather than "whatever changed in this block" — the two are
complementary: traces repair event-driven drift, bulk calls load known working
sets.

Alchemy prices `eth_call`/`eth_callMany` flat, regardless of gas used.
Providers that meter by gas or execution time will price this differently —
re-check per provider.

## Default-on integration

Since 0.2.0 every provider-backed `EvmCache` installs the bulk fetcher as its
default `StorageBatchFetchFn`, wrapping the classic point-read fetcher as
fallback. Every batch consumer — freshness verification, cold-start
verify/probe, reactive resync point reads, `prefetch_registry`, and the new
`EvmCache::prewarm_slots` — flows through it automatically. Degradation rules:

- requests below `BulkCallConfig::point_read_threshold` (default 2) go
  straight to point reads (20 CU < 26 CU for a single slot);
- any slot the bulk path fails is repaired through the point-read fallback;
- two *consecutive* fully-failed batches with provider-level errors (the
  signature of an endpoint without state-override support) **latch** the
  fetcher to the fallback for its lifetime, so such providers pay at most two
  wasted calls total;
- EVM-lazy single-slot misses during simulation (the `SharedBackend` path)
  are unchanged — they are inherently serial single reads.

Opt-out and tuning via the builder:

```rust
// Tune the bulk path (chunk size, concurrency, dispatch mode):
EvmCache::builder(provider.clone())
    .bulk_call_config(BulkCallConfig { max_slots_per_call: 15_000, ..Default::default() })
    .build().await;

// Or restore the classic pre-0.2.0 point-read behavior:
EvmCache::builder(provider)
    .storage_fetch_strategy(StorageFetchStrategy::PointRead)
    .build().await;
```

Async callers (e.g. cold-starting an AMM pool) can use the core directly:
`bulk_storage::fetch_slots_bulk(&provider, requests, block, config).await`,
then inject via `EvmCache::inject_storage_batch` — or in one step,
`EvmCache::prewarm_slots(&requests)`.

**Gzip:** enable the `gzip` feature on `reqwest` and build the provider over a
`reqwest::Client::builder().gzip(true)` client (see
`examples/bulk_storage_bench.rs::make_provider`).

## Measured results

All numbers from `examples/bulk_storage_bench.rs` (medians of 3, correctness
spot-checked against `eth_getStorageAt` ground truth at the same pinned block —
16/16 slots identical, plus per-scenario spot checks). The point-read baseline
is the classic fetcher at its default `Slow` preset (75 slots per JSON-RPC
batch, 4 in flight). Latencies vary meaningfully between runs on a hosted
endpoint; CU counts do not.

### Single-target scaling (WETH, pseudo-random slots)

| Slots | Bulk calls | Bulk median | Bulk CU | Point median | Point CU | CU ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 1 | 25 ms | 26 | 27 ms | 200 | 8x |
| 100 | 1 | 27 ms | 26 | 28 ms | 2,000 | 77x |
| 1,000 | 1 | 45 ms | 26 | 39 ms | 20,000 | 769x |
| 5,000 | 1 | 89 ms | 26 | — (skipped) | 100,000 | 3,846x |
| 10,000 | 1 | 148 ms | 26 | — (skipped) | 200,000 | 7,692x |
| 15,000 | 2 | 282 ms | 52 | — (skipped) | 300,000 | 5,769x |

Latency is comparable-to-better at small sizes and the only game in town at
large ones (a 10k-slot point-read run would need 134 HTTP batches and 200k CU).
The CU column is the headline: cost is *flat* per call.

### Multi-contract dispatch

| Workload | Calls | Median | CU | Point-read CU |
| --- | ---: | ---: | ---: | ---: |
| 20 real tokens × 25 slots (500) | 1 | 35 ms | 26 | 10,000 |
| 100 contracts × 30 slots (3,000) | 1 | 77 ms | 26 | 60,000 |

The 100-contract fleet uses synthetic (empty) addresses — dispatch cost is
identical whatever the slots hold; the 20-token row carries real nonzero data.

### Uniswap V3 USDC/WETH 0.05% — full tick-range load

The `evm-amm-state` cold-start shape, at block 25445270:

| Phase | Slots | Calls | Median |
| --- | ---: | ---: | ---: |
| 1. statics (slot0..8) + all 694 tickBitmap words | 703 | 1 | 33 ms |
| 2. 1,562 initialized ticks × 4 slots + 723 observations | 6,971 | 1 | 183 ms |
| **Total** | **7,674** | **2** | **~220 ms** |

**52 CU versus 153,480 CU as point reads — 2,952× cheaper**, and the whole
pool (every initialized tick over the full range plus the entire observation
ring) is resident after two round trips. Sanity: all 1,562 ticks decoded from
the bitmap had nonzero `liquidityGross`; spot samples matched
`eth_getStorageAt` exactly.

The exact slot count tracks live pool activity: at the July-4 re-verify block
(`25459219`) the same load was **7,654** slots (1,557 initialized ticks) in 2
calls — **52 CU versus 153,080 CU, 2,944× cheaper**. The mechanism (whole pool
in two flat-priced calls) is invariant; only the tick population drifts.

### `eth_callMany` dispatch (`CallDispatch::CallMany`)

Verified live on Alchemy (Erigon-lineage method; geth proper does not serve
it — the standardized `eth_simulateV1` also works but costs 40 CU):

| Payload | `eth_call` (per-call) | `eth_callMany` |
| --- | --- | --- |
| 6,971 tick slots (1 chunk) | 129 ms / 26 CU | **107 ms / 20 CU** |
| 25,000 slots (3 chunks) | **292 ms** / 78 CU | 497 ms / **20 CU** |

The tradeoff is clean: for payloads that fit one chunk, `CallMany` is both
faster and cheaper. For multi-chunk jobs it is ~3.9× cheaper but slower — the
bundled transactions execute *sequentially* server-side, while per-call chunks
run in parallel. Defaults stay `PerCall` (universal support, parallel);
CU-sensitive Alchemy/Erigon deployments should opt into `CallMany`. A provider
without the method transparently re-dispatches that request per-call inside
the same fetch. Hash-pinned blocks always dispatch per-call (`eth_callMany`
takes a number/tag block context).

### Custom storage programs

`StorageProgram` / `run_storage_program[s]` inject *caller-supplied* bytecode
through the same override transport — removing the "client must know every
slot key" constraint, because the program derives what to read in-EVM. The
worked example is a 40-byte one-shot Uniswap V3 observation-ring loader: it
reads `observationCardinality` out of `slot0` inside the EVM, then returns the
whole ring — **723 slots in one 48 ms call with zero calldata**, values
verified against slot-list extraction. The same idea extends to a full
tick-walker (bitmap scan + tick loads in one call), the natural next step
inside `evm-amm-state` where the pool layout is owned.

`run_storage_programs` batches programs with distinct targets into one
Multicall3 dispatch (programs sharing a target run individually — one code
override per address per call).

### Companion extractors

| Extractor | One call returns | Measured |
| --- | --- | --- |
| `fetch_account_fields_bulk` | `BALANCE` + `EXTCODEHASH` per address | 20 contracts in 25 ms / 26 CU (vs 800 CU via `eth_getBalance` + `eth_getCode`) |
| `fetch_block_context` | `NUMBER`, `TIMESTAMP`, `BASEFEE`, `COINBASE`, `PREVRANDAO`, `GASLIMIT`, `CHAINID` | one call, 32 ms; `number` matched the pin |

Caveats measured live: **nonces and storage roots are not EVM-visible** — the
`eth_getProof` path (`AccountProofFetchFn`) remains the tool for those; and
**`BASEFEE` reads 0 through `eth_call`** when no gas price is attached (geth
zeroes it so unfunded calls succeed), so treat the basefee word as unreliable
unless the call carries explicit gas pricing. `EXTCODEHASH` follows EIP-1052:
zero for non-existent accounts, `keccak256("")` for code-less ones.

### Verified code seeding (cold-start account materialization)

Since 0.2.0 the account-fields extractor above also powers
`EvmCache::verify_code_seeds`: an adapter that already embeds a contract's
deployed bytecode writes it locally (`seed_account_code`) and one bulk call
settles the **entire pending set** against on-chain `EXTCODEHASH`,
materializing each account's real native balance from the same response.
Nothing is fetched per account and no code bytes travel. Captured July 4,
2026 at block 25459219 (scenario 11, 20 mainnet tokens, medians of 3):

| Path | Latency | RPCs | CU | Code on the wire |
| --- | ---: | ---: | ---: | ---: |
| `ensure_account` × 20 (balance + nonce + code each) | 1,218 ms | 60 | 1,200 | ~211 KB |
| `seed_account_code` × 20 + one `verify_code_seeds` | **48 ms** | **1** | **26** | **0** |

**46× cheaper, 25.6× faster** for a 20-contract working set (108,495 bytes of
runtime code that never travels), and the CU gap widens linearly with fleet
size — verification costs ~5.3k gas per address inside one flat-priced call.
The same run exercised the fail-closed path live: a deliberately wrong
template came back `mismatched` (both hashes reported: expected
`0xd0a06b12…`, actual `0xd80d4b7c…`) and was purged for honest refetch, and
an undeployed address was classified `not_deployed` — one call classified
both alongside the valid claims. Caveat shared with the extractor: nonces are
not EVM-visible, so seeded contracts default to nonce 1
(`seed_account_code_with` overrides) — the right value for any contract that
has never `CREATE`d.

### Gzip vs identity (tick-range payload, 6,971 slots)

| Mode | Wire bytes | Wire time |
| --- | ---: | ---: |
| identity | 446,182 | 364 ms |
| gzip | 138,821 | 121 ms |

A consistent **68.9% payload reduction**. End-to-end medians flip between runs
(99 ms gzip vs 250 ms identity in one run; 151 vs 132 in another) — network
variance dominates at this payload size, matching the trace-benchmarks
finding: treat compression as a guaranteed payload win and an empirical
latency win. Keep it on.

### Chunk-ceiling probe (single `eth_call`)

| Slots/call | Est. gas | Result |
| ---: | ---: | --- |
| 15,000 | ~40M | ok, 279 ms |
| 20,000 | ~53M | ok, 336 ms |
| 25,000 | ~67M | ok, 377 ms |
| 30,000 | ~80M | ok, 543 ms |
| 40,000 | ~107M | **HTTP 413 Payload Too Large** |

Two findings, stable across runs:

1. **Alchemy's `eth_call` gas allowance exceeds Geth's 50M default** — 30k
   slots (~80M estimated gas) executed fine.
2. **The binding constraint on Alchemy is the request body, not gas.**
   40,000 slot keys ≈ 2.56 MB of hex calldata → HTTP 413. Practical per-call
   ceiling ≈ 30k slots (~1.9 MB). The defaults
   (`max_slots_per_call = 10_000`, `max_slots_per_request = 25_000` for
   `CallMany`) stay well inside both this and Geth-default gas caps.

## Limitations and issues encountered

1. **Request-payload cap (HTTP 413).** Slot keys travel as incompressible hex
   calldata (~64 bytes each in JSON); Alchemy rejects bodies somewhere between
   1.9 MB and 2.6 MB. Request bodies are not compressed (`Accept-Encoding`
   negotiation is response-only), so chunking is the only mitigation — handled
   by the planner.
2. **Provider support required.** State overrides are Geth-lineage/Reth/Erigon
   standard and verified on Alchemy, but not guaranteed everywhere. The
   default fetcher repairs failures through point reads and **latches** to
   them after two consecutive fully-failed batches.
3. **Precompiles cannot be overridden.** Geth dispatches precompiles
   (`0x01..=0x11` post-Pectra) by address before consulting code; the
   exact-length response check turns that into a per-slot error (repaired by
   the fallback) rather than silent garbage.
4. **`PUSH0` needs Shanghai.** All three shipped extractors use `PUSH0`; the
   slot extractor has a `PUSH1 0x00` variant
   (`STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI`, `BulkCallConfig::pre_shanghai_extractor`)
   for older chains. Every shipped bytecode is executed against revm in the
   offline test suite.
5. **Zero vs absent is indistinguishable** — exactly like `eth_getStorageAt`.
   No semantic change for the cache.
6. **Dispatcher-address collision.** A request targeting `MULTICALL3_ADDRESS`
   itself gets a dedicated call (its extractor override cannot share an
   override map with the dispatcher override) — in both dispatch modes.
7. **Flat CU pricing is provider-specific.** Alchemy bills `eth_call` at 26 CU
   and `eth_callMany` at 20 CU regardless of gas; gas-metered providers erode
   (not erase) the win. The round-trip and latency win is provider-independent.
8. **`eth_callMany` bundles execute sequentially server-side** — cheaper but
   slower than parallel per-call chunks for multi-chunk jobs (measured above).
9. **Tiny requests are cheaper as point reads** — handled by
   `point_read_threshold` (default 2).
10. **In-EVM account sampling is partial.** No nonce/storage-root opcodes
    (use `eth_getProof`); `BASEFEE` reads 0 without explicit gas pricing;
    querying the extractor-host address itself reports the overridden code
    hash.
11. **CU accounting for planners:** `bulk_storage::planned_call_count` exposes
    exactly how many calls a request set will produce.

## Follow-ups

- Wire `evm-amm-state` cold start through `fetch_slots_bulk` /
  `prewarm_slots` using the two-phase bitmap → ticks pattern (benchmark
  scenario 4 is the reference), then collapse it to one call with a custom
  tick-walker `StorageProgram` (the observation-ring program is the template).
- Consider `CallDispatch::CallMany` as the configured default in
  Alchemy-pinned deployments once per-tx gas behavior has soaked in
  production.

## Reproduction

```sh
RPC_URL=https://eth-mainnet.g.alchemy.com/v2/<key> \
    cargo run --release --example bulk_storage_bench
```

Knobs: `BULK_BENCH_SAMPLES` (default 3), `BULK_BENCH_BASELINE_MAX` (default
1000 — caps the point-read baseline, which costs 20 CU *per slot*),
`BULK_BENCH_PROBE=0` to skip the ceiling probe, and
`BULK_BENCH_SCENARIOS=4,11` to re-run selected scenarios without paying for
the rest. A full default run costs ~130k CU, dominated by the point-read
baselines.
