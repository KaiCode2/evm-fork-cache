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
| **2** | Freshness core (Pillar C): `Validity` + `FreshnessRegistry`; observation tracker; policies; optimistic verify-and-rerun loop. | **Done** (`phase-2-freshness`) |
| **3** | State-update primitives (Pillar B.1): `StateUpdate` + targeted writers; refold `inject_*`; surface state-diff output. | **Done** (`phase-3-state-updates`) |
| **4** | Event pipeline + adapters (Pillar B.2): `EventDecoder` trait, ERC-20 + V3 adapters, ingest/reorg/reconcile pipeline. | **Done** (`phase-4-event-pipeline`) |
| **5** | COW snapshots (Pillar A): structural sharing; overlay buffer reuse. | **Done** (`phase-5-cow-snapshots`) |

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

## Phase 2 — freshness core (detailed, decisions locked)

Builds the freshness/invalidation control plane **and** the optimistic
verify-and-rerun execution loop on top of it. Out of scope: event-derived
*writes* (Phase 3), the WS ingestion loop and reorg handling (Phase 4).

### Locked decisions

1. **`Validity` has three variants** (`EventDriven` dropped — folded into
   `Pinned`): `Pinned` (caller-owned: immutable or kept fresh via event writes;
   the freshness system never touches it), `Volatile` (governed by the active
   policy), `ValidThrough(block)` (pinned until block N, then volatile). Default
   is `Volatile`, configurable.
2. **Optimistic verify-and-rerun is in scope.** Don't block on a purge: snapshot,
   run sims, and concurrently re-fetch the volatile slots they read (scoped by the
   `TxConfig.access_list`); on a value mismatch, refresh and re-run only the
   affected sims. Correctness is independent of access-list completeness (the
   post-sim actual read-set is re-verified before results are trusted).
3. **Adaptive freshness via the (revived) `SlotObservationTracker`.** Per-slot
   `last_value`/`observation_count`/`change_count`/`last_checked`/`last_changed`
   drive `should_refetch`. Frequently-changing slots are verified often; stable
   ones rarely.
4. **Configurable clock, block-based by default.** `SlotObservationTracker` is made
   clock-agnostic (takes `now: u64`); a `FreshnessClock` supplies it —
   `BlockClock` (default) or `WallClock` (today's behavior).
5. **Account-level purge.** A fully-volatile address drops account
   (balance/nonce/code) + storage via a new `purge_account` primitive; an address
   with any pinned slot keeps its account and only its volatile slots are purged.

### Four-layer model

| Layer | What | Type |
| --- | --- | --- |
| Classification | `Pinned` / `Volatile` / `ValidThrough` per address/slot | `FreshnessRegistry` |
| Observation | per-slot change-frequency stats (clock-agnostic) | `SlotObservationTracker` (revived) |
| Policy | which volatile slots to verify this cycle, and how | `FreshnessPolicy` trait |
| Mechanism | re-fetch+compare, purge, re-run | `EvmCache` + `FreshnessController` |

```rust
pub enum Validity { Pinned, Volatile, ValidThrough(u64) }   // resolution: slot ▸ account ▸ default

pub trait FreshnessClock { fn now(&self) -> u64; }          // BlockClock (default) | WallClock

pub trait FreshnessPolicy {
    fn select(&mut self, candidates: &[(Address, U256)],
              obs: &SlotObservationTracker, now: u64) -> Vec<(Address, U256)>;
    fn on_new_block(&mut self, block: u64) {}
}
// built-ins: AlwaysVerify, ObservationDriven (wraps should_refetch), NeverVerify.
// tunable heuristics (min-observations, max-reuse, staleness threshold, …) move
// into a `FreshnessParams` config so users can tune the adaptive model.

pub struct FreshnessController<P: FreshnessPolicy, C: FreshnessClock> { /* registry, tracker, policy, clock, fetcher */ }
```

### Primitives (on `EvmCache`)

- `verify_slots(&mut self, slots) -> Vec<SlotChange>` — re-fetch current values via
  the existing batched `StorageBatchFetchFn`, compare to cached values, and inject the
  changed ones. Returns the changed set. (It does **not** update the observation
  tracker — only the background validator observes checked slots.)
- `purge_account(&mut self, addr)` — remove `addr` from the CacheDB overlay, the
  BlockchainDb accounts map, and its storage, so the next access re-fetches a clean
  `AccountInfo`. Distinct from storage-only `purge_pool_storage`.

### Optimistic execution loop with deferred validation (`FreshnessController::run`)

`run` returns a `SpeculativeSim { optimistic, validation }` **as soon as the
optimistic sims finish** — it does *not* await RPC. The caller computes against
`optimistic()` immediately and `validate().await`s the verdict when ready.

```rust
pub struct SpeculativeSim { /* optimistic results + JoinHandle<Validation> */ }
impl SpeculativeSim {
    pub fn optimistic(&self) -> &[SimOutcome];
    pub async fn validate(self) -> Validation;
}
pub enum Validation {
    Confirmed,
    Corrected { results: Vec<SimOutcome>, changed: Vec<SlotChange> },
    Unverified { reason: String },
}
```

Main thread (`run`): drain pending corrections into the cache → `create_snapshot()`
→ run optimistic sims (capturing read-sets) → **spawn** the validator with `Send`
data only (`Arc<EvmSnapshot>`, the `Arc` `StorageBatchFetchFn`, requests, read-sets)
→ return `SpeculativeSim`.

Background validator (spawned task — never touches the `!Send` cache): `verify_slots`
the predicted volatile set; reconcile by verifying any volatile slot in the actual
read-set not yet checked; if nothing changed → `Confirmed`; else build *corrected*
overlays from the snapshot with the fresh values in their dirty layers, re-run only
the affected sims → `Corrected { results, changed }`. RPC failure → `Unverified`.

Freshness flow-back: the validator can't mutate the live cache, so `changed` is
returned **and** queued; the next `run` drains the queue and applies it before
snapshotting (eventually-fresh, no cross-thread cache mutation). Dropping a
`SpeculativeSim` aborts the background task.

Correctness rests on the reconcile step (verify the actual read-set); the access
list only buys the overlap. This `FreshnessController` is the seed of the eventual
`SimulationEngine`.

### Placement

`src/cache/freshness.rs` (child of `cache` → reads private layers for enumeration);
`slot_observations.rs` revived + made clock-agnostic; `verify_slots`/`purge_account`
on `EvmCache`. The whole freshness surface lives under the always-on (non-`protocols`)
core.

### Tests (offline)

Classification resolution (slot ▸ account ▸ default); observation tracker with an
injected clock (block-based); each built-in policy's `select`; `verify_slots`
against a **stubbed** `StorageBatchFetchFn` returning chosen "current" values
(changed vs unchanged); the full loop — match path (no re-run) and mismatch path
(refresh + selective re-run of only affected sims); `purge_account` drops account +
storage on both layers; `ValidThrough` boundary; `WallClock` vs `BlockClock`.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings` (default +
`--lib --no-default-features`), `cargo test`, `RUSTDOCFLAGS=-D warnings cargo doc`.

Landed on `phase-2-freshness`: `src/freshness.rs` (the generic core — `Validity`
/ `FreshnessRegistry`, `FreshnessClock` + `BlockClock`/`WallClock`,
`FreshnessParams`, `FreshnessPolicy` + `AlwaysVerify`/`NeverVerify`/
`ObservationDriven`, `SlotChange`/`Validation`/`SpeculativeSim`/`SimRequest`,
`FreshnessController`); a clock-agnostic `SlotObservationTracker`;
`EvmCache::verify_slots`/`purge_account`/`set_storage_batch_fetcher`;
`EvmSnapshot::storage_value` + `EvmOverlay::override_slot` validator seams; the
offline `examples/freshness_optimistic.rs`; and `tests/freshness.rs`.

---

## Phase 3 — state-update primitives (detailed, decisions locked)

Builds **Pillar B.1 — the writer half** of the event → state pipeline: the
generic state-mutation vocabulary and the single apply primitive that writes it
consistently across both cache layers, returning a structured diff. Out of
scope (Phase 4): event decoding (`EventDecoder`, `Log` → `StateUpdate`), the WS
ingestion loop, reorg handling, and overlay-side apply.

### Locked decisions

1. **`Account` variant is a partial `AccountPatch`** (`balance`/`nonce`/`code`,
   each `Option`), not a full `AccountInfo`: best fit for event-derived writes
   (one field at a time) and keeps revm's type out of the public vocabulary.
2. **`inject_v2/v3_*` (`protocols`) normalized to write-through.** Refolded onto
   the write-through `StateUpdate::Slot` primitive (backend + overlay-if-present)
   instead of the old overlay-only write — a deliberate behavior change recorded
   in `CHANGELOG.md` (`### Changed`) and `KNOWN_ISSUES.md`, with a test pinning
   the new placement. The cold-backfill `inject_storage_batch` stays layer-2-only.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings` (default +
`--lib --no-default-features`), `cargo test`, `RUSTDOCFLAGS=-D warnings cargo doc`.

Landed on `phase-3-state-updates`: `src/state_update.rs` (the generic vocabulary
— `StateUpdate` / `AccountPatch` / `PurgeScope`, the `StateDiff` / `AccountChange`
/ `PurgeRecord` output, reusing `freshness::SlotChange`); `EvmCache::apply_update`
/ `apply_updates` with the dual-layer write-through `Slot`/`Account` and dispatch
`Purge` semantics; the refold of `inject_storage_batch_fresh` / `purge_account` /
`purge_pool_storage` / `purge_pool_slots` / `inject_v2_pool_metadata` /
`inject_v3_*` onto the primitive and the freshness correction-drain routed
through `apply_updates`; the offline `examples/state_update_apply.rs`;
`benches/state_update.rs`; and `tests/state_update.rs`. The §15 addendum adds the
relative / read-modify-write surface — a saturating `SlotDelta`, the
`StateUpdate::SlotDelta` variant, `EvmCache::modify_slot`, and the cold-aware
skip-and-surface contract via the new `StateDiff.skipped` field — to keep
event-derived balances (e.g. ERC-20 `Transfer` deltas) hot without knowing the
resulting absolute value. The §16 post-audit remediation then fixed a
HIGH-severity silent-corruption bug — `cached_storage_value` now mirrors the EVM
`SLOAD` for `StorageCleared`/`NotExisting` overlay accounts instead of returning a
shadowed backend value — and hardened the surface: a no-op `Account` patch no
longer materializes a backend account; the vocabulary and diff gained `serde`;
`StateDiff`/`AccountPatch` became `#[non_exhaustive]`; relative native-balance
tracking landed (`StateUpdate::BalanceDelta`, `EvmCache::modify_account_balance`,
`StateDiff.skipped_balances`, `SkippedBalanceDelta`) with discoverable skip
accessors (`has_skipped`/`skipped_len`/`is_fully_applied`) and the
`StateUpdate::nonce`/`code`/`account` constructors; and `apply_updates` gained a
batched single-lock fast-path (byte-identical to the sequential fold, pinned by an
equivalence test).

---

## Phase 4 — event pipeline + adapters (detailed, decisions locked)

Builds **Pillar B.2 — the reader half** of the event → state pipeline: decode an
on-chain `Log` into the Phase 3 `StateUpdate` vocabulary, apply it, and run the
reactive maintenance (reconcile, reorg) that keeps event-derived state honest.
Decoders are pure functions of `(log, pre-state)`; the `!Send` cache discipline is
preserved by keeping the tested core synchronous (the async ingestion driver is a
thin convenience). The full build contract is in
[`phase-4-spec.md`](phase-4-spec.md).

### Locked decisions

1. **Packed-slot updates → `StateUpdate::SlotMasked`** (a cold-aware RMW masked
   write), so a pure decoder can express a partial update to a packed word (V3
   `slot0`) without clobbering the bits it does not own (notably `unlocked`).
2. **V3 adapter coverage → `Swap` **and** `Mint`/`Burn` (full ticks).** `slot0` +
   `liquidity` from `Swap`; per-tick `liquidityGross`/`liquidityNet` +
   `initialized` + `tickBitmap` + in-range global `liquidity` from `Mint`/`Burn`,
   computed against the `StateView`. Fee-growth/oracle state is out of scope (a
   documented limitation; reconcile/purge are the backstop).
3. **Reorg → purge-and-resync.** A depth-bounded ring tracks addresses touched per
   block; `reorg_to(n)` purges everything touched after `n` so reads re-fetch.
   `ValidThrough` is the freshness lever.
4. **Reconciliation → sampled re-read, correct **and** alarm.** `reconcile` samples
   event-derived slots and re-reads via `EvmCache::reconcile_slots` (a honest
   wrapper over `verify_slots` that errors on a total fetch failure rather than
   reporting a false all-clear); the fresh chain value wins and the drift is
   surfaced.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings` (default +
`--lib --no-default-features`), `cargo test` (both feature configs),
`RUSTDOCFLAGS=-D warnings cargo doc`, `cargo bench --no-run`.

Landed on `phase-4-event-pipeline`: `src/events/` (the generic core —
`EventDecoder`/`StateView`, `DecoderRegistry`, `EventPipeline` with
`ingest_logs`/`reorg_to`/`reconcile` + `BlockDigest`/`ReconcileReport`/
`ReorgConfig`, and the async `drive`/`LogSource`), the generic
`Erc20TransferDecoder` (`events::erc20`), and the `protocols`-gated
`UniswapV3Decoder`/`UniswapV3Layout` (`events::uniswap_v3`); the cold-aware
`StateUpdate::SlotMasked` vocabulary + `StateDiff.skipped_masks`/`SkippedMask`
(`state_update`) and its dual-layer apply arm; `EvmCache::reconcile_slots` and the
`StateView` impl; the offline `examples/reactive_cache.rs`;
`benches/event_pipeline.rs`; and `tests/event_pipeline.rs` (+ the `SlotMasked`
tests in `tests/state_update.rs`). The §6.4 V3 fee-growth/oracle maintenance gap
is recorded in `KNOWN_ISSUES.md`.

---

## Phase 5 — copy-on-write snapshots (detailed, decisions locked)

Builds **Pillar A**: replace the O(total state) deep-clone `create_snapshot` with
a two-tier copy-on-write view whose cost tracks *changed* state, not *total*
state. The cold `BlockchainDb` index (layer 2) is flattened once into an
internal, immutable, `Arc`-shared base — both the base as a whole and each
account's storage map are shared by `Arc`, so structural sharing needs no new
dependency (Decision D1) — memoized across snapshots and rebuilt copy-on-write
only for the addresses that changed; each snapshot then folds just the hot
CacheDB delta (layer 1). Reads stay O(1), lock-free, and bit-for-bit identical to
the deep clone. The full build contract is in
[`phase-5-spec.md`](phase-5-spec.md).

### Locked decisions

1. **`Arc`-shared maps, not a persistent HAMT** (D1). Reads stay O(1) with no
   per-`SLOAD` regression and no external dependency.
2. **Base memoized as immutable; over-invalidation is acceptable, silent
   staleness is not** (D2). The write-through funnel marks an address dirty
   unconditionally; the differential-equivalence test is the hard backstop.
3. **Keep the deep clone** as `create_snapshot_deep_clone` (D3) — the A/B
   benchmark baseline and the read-equivalence reference.
4. **Overlay reuse: buffer reuse *and* `reset()` recycle** (D4) — both in scope.
5. **`create_snapshot` becomes `&mut self`** (D5) — the memoization cost; the
   freshness controller and all callers are updated.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings` (default +
`--lib --no-default-features`), `cargo test` (both feature configs),
`RUSTDOCFLAGS=-D warnings cargo doc`, `cargo bench --no-run`; the
`tests/cow_snapshot.rs` differential-equivalence gate and the existing
snapshot/overlay/freshness tests pass unchanged.

Landed on `phase-5-cow-snapshots`: the memoized two-tier base (`BaseState` +
the rewritten two-tier `EvmSnapshot` with `account_info`/`storage_value`/`code`
accessors, `src/cache/snapshot.rs`); `EvmCache::refresh_base`/`build_base_full`,
the COW `create_snapshot` (now `&mut self`), the retained
`create_snapshot_deep_clone`, and the `mark_base_dirty`/`invalidate_base`
invalidation wired into `write_slot_through`/`apply_slot_run`/
`write_account_info_through`/`inject_storage_batch`/the `purge_*` paths and
`set_block` (`src/cache/mod.rs`); `EvmOverlay::reset` plus the reusable
shared-memory buffer recycled across the call methods (`src/cache/overlay.rs`);
the layer-2-seeded A/B + hot-loop + `reset()`-fanout benches
(`benches/simulation.rs`); and the differential-equivalence gate
(`tests/cow_snapshot.rs`). The residual O(accounts) length-scan / O(layer-1)
fold cost model is recorded in `KNOWN_ISSUES.md`.

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
