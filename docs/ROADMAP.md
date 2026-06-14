# evm-fork-cache — engineering roadmap

> Status: living document. Last updated alongside the Phase 1 work on branch
> `phase-1-engine-seam`.

## Vision

A **high-performance forked-EVM simulation engine** for DeFi search / MEV /
backtesting. The moat is three capabilities working together:

1. **Cheap parallel fan-out** — freeze state once, clone it near-free, run many
   isolated simulations in parallel.
2. **Event-driven state sync** — keep hot state correct from the event stream
   (WebSocket logs), avoiding RPC round-trips.
3. **Freshness as a first-class concept** — the engine knows what it can trust,
   for how long, and purges the rest.

Today the crate implements a first draft of #1 and the lazy-fetch plumbing for a
fork DB. #2 and #3 do not yet exist. This document captures the target shape and
the phased path to it.

## Target architecture

Four layers, bottom to top:

```
Parallel overlays ×N        (isolated Send simulations)
        ▲ clone ×N (cheap)
Snapshot · Arc · COW         (rapidly clonable, point-in-time)
        ▲ create_snapshot
Fork DB (foundry-fork-db)    (lazy RPC fetch + local state cache)
        ▲ lazy fetch              ▲ targeted writes / purge (no RPC)
RPC node                     Event-driven sync  ← WS logs · new block
```

- **State stack (left):** RPC → fork DB → snapshot → overlays. Reads flow up;
  the fork DB lazily fetches misses from RPC.
- **Control plane (right):** a WS log + block stream drives an event-driven sync
  that applies *targeted* writes (e.g. a V3 `Swap` → `slot0`) and purges stale
  state directly into the fork DB, without RPC.

### The three pillars

- **Pillar A — COW snapshots.** Replace the deep-clone `create_snapshot` with
  structurally-shared, copy-on-write state so cloning is O(changed), not
  O(total). This is the performance payoff for parallel fan-out.
- **Pillar B — Event → state pipeline.** Decode protocol events into targeted
  `StateUpdate`s and apply them to the fork DB. Key insight: events already
  carry the post-state (a V3 `Swap` emits `sqrtPriceX96`/`tick`/`liquidity`;
  `Mint`/`Burn` emit the affected tick range), so we decode-and-write rather
  than re-derive.
- **Pillar C — Freshness & invalidation.** A per-address / per-slot validity
  policy (`Pinned`, `EventDriven`, `ValidThrough(block)`, `VolatilePerBlock`)
  enforced on each new block: purge what we can no longer trust; the next read
  lazily re-fetches.

## Design principles

1. **Generic core, pluggable protocols.** The simulation engine knows nothing
   about Uniswap. DeFi knowledge (slot layouts, event ABIs) lives behind the
   `protocols` feature and will eventually move to the `evm-amm-state` crate.
2. **Honest freshness.** Reuse aggressively where safe; purge loudly where not.
   Never silently serve stale state.
3. **Correctness is verifiable.** Event-derived state must be reconcilable
   against RPC (sampled re-reads that alarm on mismatch).
4. **Pre-1.0: break now, not later.** Fix API shape before the surface freezes.

## Phased roadmap

| Phase | Scope | Status |
| --- | --- | --- |
| **0** | API hygiene + correctness: drop `amms`, fix `set_block` divergence + `block_in_place` panic, commit the tree. | **Done** (`p0-oss-prep`) |
| **1** | Engine seam: typed errors, configurable tx/block env, hot-path benches, builder, `protocols` feature. | **Done** (`phase-1-engine-seam`) |
| **2** | Freshness core (Pillar C): `Validity` + `FreshnessRegistry`; `on_new_block` purge; pin immutables. | Planned |
| **3** | State-update primitives (Pillar B.1): `StateUpdate` + targeted writers; refold `inject_*`; surface state-diff output. | Planned |
| **4** | Event pipeline + adapters (Pillar B.2): `EventDecoder` trait, V3 adapter, WS ingestion loop, reorg handling. | Planned |
| **5** | COW snapshots (Pillar A): structural sharing; overlay buffer reuse. | Planned |

Cross-cutting (land opportunistically): call tracer Inspector, full offline
(`default-features = false`, no provider) build split, CHANGELOG/CONTRIBUTING.

---

## Phase 1 — engine seam (detailed)

Goal: lift the crate out of read-only-swap simulation into value-bearing
simulation, give it a typed error contract and a real constructor, isolate
protocol knowledge behind a feature, and add the benchmarks that will quantify
the Pillar A rewrite. These are the breaking changes that must precede a 1.0.

### 1a — Typed error model

- **Change:** derive the simulation error with `thiserror`; add a first-class
  `Halt { reason, gas_used }` variant instead of folding halts into the generic
  `Other(anyhow::Error)`. Keep `SimulationError` (the decoded revert) as the
  `Revert` payload.
- **Files:** `src/errors.rs` (+ `thiserror` dep), call sites in
  `src/cache/mod.rs` / `src/cache/overlay.rs`.
- **API:** `enum SimError { Revert(Box<SimulationError>), Halt { .. }, Host(anyhow::Error) }`;
  `type SimulationResult<T> = Result<T, SimError>`. `SimulationErrorKind`
  retained as a deprecated alias.
- **Done when:** halts surface typed; `cargo test` + clippy green.

### 1b — Configurable transaction & block environment

- **Change:** introduce a `TxConfig { value, gas_limit, gas_price, nonce,
  access_list }` threaded through a new `build_tx_env_with`; add `*_with`
  call variants that take it. Enable revm's `optional_balance_check` and set
  `disable_balance_check = true` so value-bearing sims run without funding.
  Complete `BlockEnv`: populate `coinbase`/`prevrandao`/`gas_limit` from the
  fetched header at construction, store on `EvmCache` + `EvmSnapshot`, set them
  in both `build_evm` paths, and add `set_coinbase` / `set_prevrandao` setters.
- **Files:** `src/cache/mod.rs`, `src/cache/overlay.rs`, `src/cache/snapshot.rs`,
  `Cargo.toml` (revm feature).
- **API:** `call_raw_with(from, to, calldata, commit, &TxConfig)` and friends;
  `TxConfig` (Default = current behavior). Existing `call_raw(..)` becomes a thin
  wrapper, so callers keep working.
- **Done when:** a value-bearing call succeeds in a test; `coinbase`/`prevrandao`
  read correctly in a sim.

### 1c — Hot-path benchmarks

- **Change:** add offline criterion benches for the real hot paths — `create_snapshot`
  across cache sizes (N accounts × M slots), an M-way overlay fan-out
  (clone + simulate), and `inject_storage_batch`. Build the cache once inside a
  runtime; benchmark the sync hot paths.
- **Files:** `benches/simulation.rs`, `Cargo.toml` (`[[bench]]`).
- **Done when:** benches run and give a baseline for the Pillar A rewrite.

### 1d — Builder

- **Change:** add `EvmCacheBuilder` (fluent: block, chain id, spec, cache config,
  speed mode) subsuming the positional `with_cache`/`from_backend`. Move the
  process-global `CACHE_SPEED_MODE` into per-instance config so multiple caches
  (multi-chain search) tune independently.
- **Files:** `src/cache/mod.rs` (+ a `builder` submodule).
- **API:** `EvmCache::builder(provider).block(..).spec(..).build().await`.
  Existing constructors retained (possibly deprecated).
- **Done when:** builder constructs an equivalent cache; speed mode is per-instance.

### 1e — `protocols` feature

- **Change:** add a `[features]` table with `default = ["protocols"]`. Gate the
  DeFi-specific surface behind `protocols`: the V3 tick-snapshot module
  (`tick_snapshot`), the `inject_v3_ticks*` / `inject_v2_pool_metadata` methods,
  and the protocol slot constants in `storage_keys` (V2/V3/Pancake/Slipstream).
  Generic machinery (errors, create3, access sets, multicall, ERC20 helpers,
  the cache core, `CacheConfig`, token-decimals cache) stays always-on.
- **Files:** `Cargo.toml`, `src/lib.rs`, `src/cache/mod.rs`, `src/cache/storage_keys.rs`.
- **Done:** `mod storage_keys` / `mod tick_snapshot`, their re-exports, the
  `tick_snapshot_cache` field + its construction/save, the `inject_v2_pool_metadata`
  / `inject_v3_*` methods, and `CacheConfig::tick_snapshot_cache_path` are all
  gated behind `protocols` (default on). The library builds and lints cleanly
  with `--no-default-features` (CI enforces `cargo clippy --lib
  --no-default-features -- -D warnings`).
- **Deferred (next, with the `evm-amm-state` move):** the in-crate *unit tests*
  that exercise the tick math still assume the default feature, so
  `cargo test --no-default-features` is not yet supported (gate or relocate those
  tests). Pool *metadata* structs (`V2/V3/BalancerPoolMetadata`, entangled with
  `ImmutableDataCache`) stay always-on for now, as does the full no-provider build
  (making revm/foundry-fork-db/alloy-provider optional behind an `rpc` feature).

### Phase 1 acceptance — met

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` (default),
`cargo clippy --lib --no-default-features -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc`, and all examples/benches build.

---

## Key abstractions for later phases (sketches)

```rust
// Pillar B — event → state
enum StateUpdate {
    Slot   { address: Address, slot: U256, value: U256 },
    Account{ address: Address, info: AccountInfo },
    Purge  { address: Address, scope: PurgeScope },
}
trait EventDecoder: Send + Sync {
    fn decode(&self, log: &Log) -> Vec<StateUpdate>;
}

// Pillar C — freshness
enum Validity { Pinned, EventDriven, ValidThrough(u64), VolatilePerBlock }
struct FreshnessRegistry { /* per-address & per-(address,slot) Validity */ }

// The composed engine
impl SimulationEngine {
    fn snapshot(&self) -> Arc<EvmSnapshot>;        // Pillar A
    fn ingest(&mut self, logs: LogStream, blocks: BlockStream); // Pillar B
    fn on_new_block(&mut self, n: u64);            // Pillar C: apply + purge
}
```

## Hard problems to resolve (tracked)

1. **Event-derived correctness** — add a reconciliation mode (sampled RPC re-read
   vs event-derived value; alarm on mismatch). Build into Phase 4 from day one.
2. **Reorgs** — purge-and-resync affected addresses on a reorg signal; `ValidThrough`
   is the lever.
3. **Snapshot consistency point** — snapshot at block boundaries (between
   `on_new_block` applies) or behind a generation guard, so the ingestion loop
   can't produce a torn read.
4. **Protocol/metadata extraction** — `ImmutableDataCache` couples generic
   token-decimals with V2/V3/Balancer pool metadata; fully separating them is the
   precondition for the `evm-amm-state` move.
