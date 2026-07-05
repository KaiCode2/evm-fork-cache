# evm-fork-cache — engineering roadmap

> Status: living document. Phases 0-8 have landed: through bundle simulation +
> call tracing, plus Phase 8 — storageHash liveness & state invalidation —
> which shipped in **0.2.0** (all six steps of
> [`phase-8-liveness-spec.md`](phase-8-liveness-spec.md), including the
> cold-start root baseline and the Tier-3 trace-backed resync source).

## Vision

A **high-performance forked-EVM simulation engine** for DeFi search / MEV /
backtesting. The moat is three capabilities working together:

1. **Cheap parallel fan-out** — freeze state once, clone it near-free, run many
   isolated simulations in parallel.
2. **Event-driven state sync** — keep hot state correct from the event stream
   (WebSocket logs), avoiding RPC round-trips.
3. **Freshness as a first-class concept** — the engine knows what it can trust,
   for how long, and purges the rest.

Today the crate implements all three pillars end-to-end: copy-on-write snapshots
and overlays, the freshness control plane, targeted state-update writers, the
event-to-state reader pipeline, and — as of Phase 6 — a provider-neutral reactive
runtime with journaled reorg recovery, a live WebSocket `AlloySubscriber`
transport, declarative cold-start warming; as of Phase 7 — multi-transaction
bundle simulation with coinbase accounting and a call-frame tracer; and — as of
Phase 8 — liveness as a first-class engine responsibility via `storageHash`-based
state invalidation (the root gate, tracking policies, and the trace-backed resync
source), plus mid-lifecycle adapter register/unregister. The remaining work
toward 1.0 is breadth on those primitives (`Create`-kind bundle txs, opcode
tracing) and transport depth (full block bodies, full pending-tx hydration,
non-log historical backfill; log interests can request owner-scoped `get_logs`
backfill from a block anchor).

## Target architecture

Four layers, bottom to top:

```
Parallel overlays ×N        (isolated Send simulations)
        ▲ clone ×N (cheap)
Snapshot · Arc · COW         (rapidly clonable, point-in-time)
        ▲ snapshot
Fork DB (foundry-fork-db)    (lazy RPC fetch + local state cache)
        ▲ lazy fetch              ▲ targeted writes / purge (no RPC)
RPC node                     Event-driven sync  ← WS logs · new block
```

- **State stack (left):** RPC → fork DB → snapshot → overlays. Reads flow up;
  the fork DB lazily fetches misses from RPC.
- **Control plane (right):** decoded logs drive event-derived targeted writes
  (e.g. a V3 `Swap` → `slot0`) and purges stale state directly into the fork DB,
  without RPC on the hot path. The reader/writer pipeline, the reactive runtime
  that drives it (journaled parent-hash reorg detection and recovery), and a live
  WebSocket subscription transport (`AlloySubscriber`) all ship in-crate.

### The three pillars

- **Pillar A — COW snapshots.** Replace the deep-clone `snapshot` with
  structurally-shared, copy-on-write state so cloning is O(changed), not
  O(total). This is the performance payoff for parallel fan-out.
- **Pillar B — Event → state pipeline.** Decode protocol events into targeted
  `StateUpdate`s and apply them to the fork DB. Key insight: events already
  carry the post-state (a V3 `Swap` emits `sqrtPriceX96`/`tick`/`liquidity`;
  `Mint`/`Burn` emit the affected tick range), so we decode-and-write rather
  than re-derive.
- **Pillar C — Freshness & invalidation.** A per-address / per-slot validity
  policy (`Pinned`, `Volatile`, `ValidThrough(block)`) enforced by freshness
  policy and validation: purge or verify what we can no longer trust; the next
  read lazily re-fetches.

## Design principles

1. **Generic core, external protocol adapters.** The simulation engine knows
   nothing about AMM layouts. DeFi knowledge (slot layouts, event ABIs, and AMM
   state tracking) lives in `evm-amm-state` or downstream crates.
2. **Honest freshness.** Reuse aggressively where safe; purge loudly where not.
   Never silently serve stale state.
3. **Correctness is verifiable.** Event-derived state must be reconcilable
   against RPC (sampled re-reads that alarm on mismatch).
4. **Pre-1.0: break now, not later.** Fix API shape before the surface freezes.

## Phased roadmap

| Phase | Scope | Status |
| --- | --- | --- |
| **0** | API hygiene + correctness: drop `amms`, fix `set_block` divergence + `block_in_place` panic, commit the tree. | **Done** (`p0-oss-prep`) |
| **1** | Engine seam: typed errors, configurable tx/block env, hot-path benches, builder, protocol-adapter extraction path. | **Done** (`phase-1-engine-seam`) |
| **2** | Freshness core (Pillar C): `Validity` + `FreshnessRegistry`; observation tracker; policies; optimistic verify-and-rerun loop. | **Done** (`phase-2-freshness`) |
| **3** | State-update primitives (Pillar B.1): `StateUpdate` + targeted writers; refold `inject_*`; surface state-diff output. | **Done** (`phase-3-state-updates`) |
| **4** | Event pipeline + adapters (Pillar B.2): `EventDecoder` trait, ERC-20 decoder, ingest/reorg/reconcile pipeline. (The in-crate V3 adapter built here was later extracted to `evm-amm-state`; the core stays protocol-neutral.) | **Done** (`phase-4-event-pipeline`) |
| **5** | COW snapshots (Pillar A): structural sharing; overlay buffer reuse. | **Done** (`phase-5-cow-snapshots`) |
| **6** | Reactive runtime + live transport: provider-neutral `ReactiveRuntime` / `ReactiveHandler`, journaled depth-bounded reorg recovery, the WebSocket `AlloySubscriber`, and declarative `cold_start` warming. | **Done** (`cold-start-sync`) |
| **7** | Bundle simulation + call tracing: `EvmOverlay::simulate_bundle` (ordered cumulative-state txs, `RevertPolicy`, coinbase-payment accounting) and a `CallTracer` (call-frame tree) + composable `InspectorStack`. | **Done** (`phase-6-bundle-sim`) |
| **8** | storageHash liveness & state invalidation (Pillar C): account/root fetcher seam (`eth_getProof`); per-block root gate + complement resync (`ResyncReason::RootMoved`) + coverage alarm; per-contract `TrackingPolicy` (`Slots`/`WholeAccount`/`Scalars`); event-write `Validity` stamping; `advance_block` block-env refresh; cold-start root baseline (`roots.bin`); Tier-3 trace-backed resync. | **Done in 0.2.0** ([spec](phase-8-liveness-spec.md)) |

Cross-cutting remaining work: `Create`-kind / state-override bundles, opcode-level
tracing, and a full no-provider build split.

---

## Phase 1 — engine seam (detailed)

Goal: lift the crate out of read-only-swap simulation into value-bearing
simulation, give it a typed error contract and a real constructor, isolate
protocol knowledge from the generic engine, and add the benchmarks that will
quantify the Pillar A rewrite. These are the breaking changes that must precede
a 1.0.

### 1a — Typed error model

- **Change:** derive the simulation error with `thiserror`; add a first-class
  `Halt { reason, gas_used }` variant instead of folding halts into the generic
  host failure arm. Keep `SimulationError` (the decoded revert) as the
  `Revert` payload, and route host failures through `SimHostError`.
- **Files:** `src/errors.rs` (+ `thiserror` dep), call sites in
  `src/cache/mod.rs` / `src/cache/overlay.rs`.
- **API:** `enum SimError { Revert(Box<SimulationError>), Halt { .. }, Other(SimHostError) }`;
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

- **Change:** add offline criterion benches for the real hot paths — `snapshot`
  across cache sizes (N accounts × M slots), an M-way overlay fan-out
  (clone + simulate), and `inject_storage_batch`. Build the cache once inside a
  runtime; benchmark the sync hot paths.
- **Files:** `benches/simulation.rs`, `Cargo.toml` (`[[bench]]`).
- **Done when:** benches run and give a baseline for the Pillar A rewrite.

### 1d — Builder

- **Change:** add `EvmCacheBuilder` (fluent: block, spec, cache config,
  shared-memory capacity, storage batch config) as the preferred constructor over positional
  `with_cache` / `from_backend`.
- **Files:** `src/cache/mod.rs` (+ a `builder` submodule).
- **API:** `EvmCache::builder(provider).block(..).spec(..).build().await`.
  Existing constructors retained (possibly deprecated).
- **Done:** builder constructs an equivalent cache. Storage batch tuning is now
  per cache via `EvmCacheBuilder::storage_batch_config`, with
  `EvmCacheBuilder::speed_mode` as preset shorthand.

### 1e — Protocol adapter extraction

- **Change:** keep the generic engine focused on cache mechanics, snapshots,
  freshness, state updates, access lists, ERC-20 helpers, multicall, deploy, and
  CREATE3. Protocol-specific storage layout helpers, AMM metadata, and
  concentrated-liquidity adapter state move out to `evm-amm-state`.
- **Files:** `Cargo.toml`, `src/lib.rs`, `src/cache/mod.rs`, `src/events/mod.rs`,
  tests, benches, examples, and release docs.
- **Done:** removed the old in-crate AMM adapter surface, made
  `ImmutableDataCache` token-decimals-only, bumped its on-disk version, and
  updated CI/docs/examples/benches to present this crate as the generic engine.

### Phase 1 acceptance — met

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc`, and all examples/benches build.

---

## Phase 2 — freshness core (detailed, decisions locked)

Builds the freshness/invalidation control plane **and** the optimistic
verify-and-rerun execution loop on top of it. Out of scope for Phase 2:
event-derived *writes* (Phase 3), event decoding, generic ingestion, and reorg
handling (Phase 4).

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
  `AccountInfo`. Distinct from storage-only `purge_contract_storage`.

### Optimistic execution loop with deferred validation (`FreshnessController::run`)

`run` returns a `SpeculativeSim { optimistic, validation }` **as soon as the
optimistic sims finish** — it does *not* await RPC. The caller computes against
`optimistic()` immediately and `validate().await?`s the verdict when ready.

```rust
pub struct SpeculativeSim { /* optimistic results + JoinHandle<Validation> */ }
impl SpeculativeSim {
    pub fn optimistic(&self) -> &[SimOutcome];
    pub async fn validate(self) -> Result<Validation>;
}
pub enum Validation {
    ConfirmedStorage,
    ConfirmedFull,
    Corrected {
        results: Vec<SimOutcome>,
        changed_slots: Vec<SlotChange>,
        changed_accounts: Vec<AccountChange>,
    },
    Unverified { reason: String },
}
```

Main thread (`run`): drain pending corrections into the cache → `snapshot()`
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
on `EvmCache`. The whole freshness surface lives under the always-on generic core.

### Tests (offline)

Classification resolution (slot ▸ account ▸ default); observation tracker with an
injected clock (block-based); each built-in policy's `select`; `verify_slots`
against a **stubbed** `StorageBatchFetchFn` returning chosen "current" values
(changed vs unchanged); the full loop — match path (no re-run) and mismatch path
(refresh + selective re-run of only affected sims); `purge_account` drops account +
storage on both layers; `ValidThrough` boundary; `WallClock` vs `BlockClock`.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc`.

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
2. **Legacy protocol writers normalized before extraction.** The old protocol
   writers were refolded onto the write-through `StateUpdate::Slot` primitive
   before being moved out, keeping the generic write path as the single contract.
   The cold-backfill `inject_storage_batch` stays layer-2-only.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc`.

Landed on `phase-3-state-updates`: `src/state_update.rs` (the generic vocabulary
— `StateUpdate` / `AccountPatch` / `PurgeScope`, the `StateDiff` / `AccountChange`
/ `PurgeRecord` output, reusing `freshness::SlotChange`); `EvmCache::apply_update`
/ `apply_updates` with the dual-layer write-through `Slot`/`Account` and dispatch
`Purge` semantics; the refold of `inject_storage_batch_fresh` / `purge_account` /
`purge_contract_storage` / `purge_contract_slots` onto the primitive and the freshness
correction-drain routed through `apply_updates`; the offline
`examples/state_update_apply.rs`;
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
thin convenience). The public build contract and acceptance evidence are
summarized below.

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

`cargo fmt --check`, `clippy --all-targets -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc`, `cargo bench --no-run`.

Landed on `phase-4-event-pipeline`: `src/events/` (the generic core —
`EventDecoder`/`StateView`, `DecoderRegistry`, `EventPipeline` with
`ingest_logs`/`reorg_to`/`reconcile` + `BlockDigest`/`ReconcileReport`/
`ReorgConfig`, and the async `drive`/`LogSource`), the generic
`Erc20TransferDecoder` (`events::erc20`); the cold-aware `StateUpdate::SlotMasked`
vocabulary + `StateDiff.skipped_masks`/`SkippedMask` (`state_update`) and its
dual-layer apply arm; `EvmCache::reconcile_slots` and the `StateView` impl; the
offline `examples/reactive_cache.rs`;
`benches/event_pipeline.rs`; and `tests/event_pipeline.rs` (+ the `SlotMasked`
tests in `tests/state_update.rs`). The §6.4 V3 fee-growth/oracle maintenance gap
is recorded in `KNOWN_ISSUES.md`.

---

## Phase 5 — copy-on-write snapshots (detailed, decisions locked)

Builds **Pillar A**: replace the O(total state) deep-clone `snapshot` with
a two-tier copy-on-write view whose cost tracks *changed* state, not *total*
state. The cold `BlockchainDb` index (layer 2) is flattened once into an
internal, immutable, `Arc`-shared base — both the base as a whole and each
account's storage map are shared by `Arc`, so structural sharing needs no new
dependency (Decision D1) — memoized across snapshots and rebuilt copy-on-write
only for the addresses that changed; each snapshot then folds just the hot
CacheDB delta (layer 1). Reads stay O(1), lock-free, and bit-for-bit identical to
the deep clone. The public build contract and acceptance evidence are summarized
below.

### Locked decisions

1. **`Arc`-shared maps, not a persistent HAMT** (D1). Reads stay O(1) with no
   per-`SLOAD` regression and no external dependency.
2. **Base memoized as immutable; over-invalidation is acceptable, silent
   staleness is not** (D2). The write-through funnel marks an address dirty
   unconditionally; the differential-equivalence test is the hard backstop.
3. **Keep the deep clone** as `snapshot_deep_clone` (D3) — the A/B
   benchmark baseline and the read-equivalence reference.
4. **Overlay reuse: buffer reuse *and* `reset()` recycle** (D4) — both in scope.
5. **`snapshot` becomes `&mut self`** (D5) — the memoization cost; the
   freshness controller and all callers are updated.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc`, `cargo bench --no-run`; the
`tests/cow_snapshot.rs` differential-equivalence gate and the existing
snapshot/overlay/freshness tests pass unchanged.

Landed on `phase-5-cow-snapshots`: the memoized two-tier base (`BaseState` +
the rewritten two-tier `EvmSnapshot` with `account_info`/`storage_value`/`code`
accessors, `src/cache/snapshot.rs`); `EvmCache::refresh_base`/`build_base_full`,
the COW `snapshot` (now `&mut self`), the retained
`snapshot_deep_clone`, and the `mark_base_dirty`/`invalidate_base`
invalidation wired into `write_slot_through`/`apply_slot_run`/
`write_account_info_through`/`inject_storage_batch`/the `purge_*` paths and
`set_block` (`src/cache/mod.rs`); `EvmOverlay::reset` plus the reusable
shared-memory buffer recycled across the call methods (`src/cache/overlay.rs`);
the layer-2-seeded A/B + hot-loop + `reset()`-fanout benches
(`benches/simulation.rs`); and the differential-equivalence gate
(`tests/cow_snapshot.rs`). The residual O(accounts) length-scan / O(layer-1)
fold cost model is recorded in `KNOWN_ISSUES.md`.

---

## Phase 6 — reactive runtime + live transport (detailed)

Builds the operational layer that drives Pillar B and closes the "consumer wires
the transport themselves" gap from the original roadmap: a provider-neutral
runtime that turns subscription inputs into validated cache mutations, a live
WebSocket transport, and a declarative cold-start warmer. Landed on
`cold-start-sync`.

### Locked decisions

1. **Pure handlers, runtime-owned mutation.** `ReactiveHandler::handle` is a pure
   function of `(context, input, &dyn StateView)` returning a `HandlerOutcome` of
   declarative `ReactiveEffect`s (`StateUpdate`, `Invalidate`, `Resync`,
   `Speculative`, `Hook`). The runtime — never the handler — validates the effect
   set (rejecting conflicting writes and canonical mutations from pending inputs)
   and applies canonical cache mutations. This keeps the `!Send` cache discipline
   intact: handlers carry no cache handle and dispatch is synchronous.
2. **Journaled, depth-bounded reorg recovery.** The runtime journals each
   canonical block's applied effects in a `VecDeque` capped at
   `ReactiveConfig::journal_depth` (default 64). A removed log or a parent-hash
   discontinuity drains the dropped blocks and recovers them: reversible
   slot writes are rolled back to their exact prior values (LIFO), while accounts
   whose balance/nonce/code moved are promoted to a `PurgeScope::Account` so the
   next read re-fetches clean state. Hash-pinned resyncs from dropped blocks are
   canceled. A `ReactiveReport::Reorg` describes precisely what was rolled back
   and purged. See the runnable [`reactive_runtime`](../examples/reactive_runtime.rs)
   example.
3. **Live transport behind a trait.** `EventSubscriber` is the transport seam;
   `AlloySubscriber` is the in-crate implementation over Alloy pubsub —
   `subscribe_logs` / `subscribe_blocks` / `subscribe_pending_transactions`,
   exponential-backoff reconnect with a bounded dedupe window, and `get_logs`
   backfill from the last-seen block on reconnect. WebSocket TLS uses rustls' ring
   provider (`reactive-ws`, default); an HTTP polling transport is opt-in
   (`reactive-polling`).
4. **Declarative cold-start.** `EvmCache::run_cold_start` drives a
   `ColdStartPlanner` (a bounded discover-then-verify loop) to warm a working set
   of accounts/slots into the cache in batched passes before going reactive,
   returning a structured `ColdStartRunReport`.

### Known limitations (tracked in `docs/KNOWN_ISSUES.md`)

- Reorgs deeper than `journal_depth` (or any depth when `journal_depth = 0`)
  recover only the blocks still resident in the journal; effects from aged-out
  blocks are neither rolled back nor purged, and the freshness/validation loop is
  the backstop. `journal_depth` must exceed the deepest reorg you intend to
  recover from.
- The subscriber surfaces pending-transaction **hashes** only (no full pending-tx
  hydration), no full block bodies, and non-log historical backfill is not
  implemented. Log interests can request owner-scoped `get_logs` backfill from a
  block anchor; reconnect backfill is still anchored at the last-seen block. Hook
  dispatch is synchronous on the ingest thread; `hook_backpressure` is reserved
  for a future async dispatcher.

### Acceptance — met

`cargo fmt --check`, `clippy --all-targets -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc`, `cargo bench --no-run`. Landed: `src/reactive/`
(runtime, registry/router, journaling + reorg recovery, `EventSubscriber` +
`AlloySubscriber`), `src/cold_start/` (planner/driver/plan/results), the
`reactive_cache` / `reactive_runtime` / `reactive_alloy_amm_live_probe` examples,
`benches/event_pipeline.rs`, and `tests/reactive_*` + `tests/cold_start.rs`.

---

## Phase 8 — storageHash liveness & state invalidation (shipped in 0.2.0)

Deepens **Pillar C** by making liveness a first-class engine responsibility
instead of a consumer chore. Today freshness can only re-check slots a sim
actually read or the caller explicitly sampled, and the event pipeline only keeps
state fresh for protocols with a decoder — so state changed by any uncovered path
(proxy `sstore`, admin writes, an undecoded token, a `SELFDESTRUCT`/`CREATE2`
redeploy) goes silently stale. An account's storage-trie root (`storageHash`, from
`eth_getProof`) is a commitment over *all* its storage, so `root_unchanged ⟹
provably nothing under the account changed` — a sound, per-account change oracle
with zero false negatives, obtainable from any standard RPC in one call, with **no
local trie and no proof verification**. This phase uses it to detect and repair
the staleness the current footprint-bounded model cannot see.

Full design, type sketches, tests, and the cold-start correctness argument live in
[`phase-8-liveness-spec.md`](phase-8-liveness-spec.md). The build set (in order):

1. **Account/root fetcher seam** on `EvmCache` (`AccountProofFetchFn` over
   `eth_getProof`, mirroring `StorageBatchFetchFn`). The linchpin — it also
   resolves the tracked `ResyncTarget::Account` `Unsupported` gap
   (`reactive/mod.rs`) and the account-field freshness gap.
2. **`advance_block(header)`** — engine-driven block-env refresh from the
   canonical header stream (the runtime does not refresh scalars per block today).
3. **`Validity` stamping** of reactive/event-derived writes — the first (minimal,
   intentional) coupling of the reactive runtime to `FreshnessRegistry`
   (`valid_through_slot(N)` on touched slots, aged by `on_new_block`).
4. **The centerpiece:** per-contract `TrackingPolicy` (`Slots` / `WholeAccount` /
   `Scalars`) + a per-block **root gate** that, on a root move no decoder
   explained, emits a `ResyncRequest` (new `ResyncReason::RootMoved`) through the
   runtime's existing resync channel and raises a `CoverageGap` report.
5. **Cold-start root baseline** (`roots.bin`) + restart drift diff in the
   `ColdStartPlanner` — "if the observed root matches the persisted baseline,
   we're already synced; skip re-reading."
6. A Tier-3 state-diff trace source (`debug_traceBlock` /
   `trace_replayBlockTransactions`) that resolves matching resync targets with
   one block-level diff where available, then falls back to portable point reads
   for unresolved targets.
   [`trace-resync-benchmarks.md`](trace-resync-benchmarks.md) records the live
   RPC measurements behind the policy: trace wins CU economics quickly, gzip
   materially helps large trace payloads, and latency-sensitive small slot sets
   should continue to use batched point reads.

### Locked decisions

1. **No local storage trie; no cryptographic proof verification.** The root is
   *observed* via `eth_getProof`, never reconstructed locally, and never verified
   against a `stateRoot` fetched from the same (already-trusted) RPC — that would
   be circular. `alloy-trie` stays out. No new production dependency.
2. **`storageHash` gates `WholeAccount` only**, never `Slots` (a sparse-interest
   contract's root churns every block → noisy, wasteful). The policy is a pure
   cost knob: a false-positive resync is never *incorrect*, only redundant.
3. **Compose, don't merge** — a `TrackingRegistry` sits beside
   `FreshnessRegistry`; the freshness API stays verbatim.
4. **Reuse the existing resync channel** — the root gate emits `ResyncRequest`s;
   it does not build a parallel repair loop.
5. **Cold-start gate is currency, not completeness** — the across-time root diff
   proves tracked slots are current, not that the cache is complete; a
   `FullMirror` completeness mode is explicitly deferred.

### Closes (on landing, convert in `KNOWN_ISSUES.md`)

Account-field resync `Unsupported` (`reactive/mod.rs`), "freshness reconciles
storage slots only" (the balance/nonce/code gap), and the account-field-resync
transport-depth item below.

### Acceptance (target)

Branch `phase-8-liveness`. Green bar at every commit: `cargo fmt --all --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`,
`RUSTDOCFLAGS=-D warnings cargo doc --no-deps`, `cargo bench --no-run`. Offline
acceptance contract in the spec (`tests/liveness_*`).

---

## Remaining work toward 1.0

1. **Bundle-simulation breadth (Phase 7 — core shipped).** `EvmOverlay::simulate_bundle`
   now evaluates an ordered tx sequence over cumulative state with a revert policy
   and coinbase-payment accounting, and a `CallTracer` reconstructs the call-frame
   tree (see Phase 7 below). The remaining breadth: `Create`-kind bundle txs, a
   builder-style state-override bundle, opcode/step-level tracing, and tightening
   reverted-tx gas accounting under `AllowReverts` (a reverted tx's gas is rolled
   back with its checkpoint, so it is not counted toward the searcher's cost —
   tracked in `docs/KNOWN_ISSUES.md`).
2. **Transport depth.** The live `AlloySubscriber` ships log/block/pending-hash
   subscriptions, exponential-backoff reconnect, `get_logs` backfill, and
   journaled parent-hash reorg recovery. The remaining transport gaps are full
   block bodies, full pending-transaction hydration (today only pending-tx
   hashes), and non-log historical backfill. Log interests can request
   owner-scoped `get_logs` backfill from a block anchor. Remaining gaps are
   tracked in `docs/KNOWN_ISSUES.md`.
3. **Snapshot consistency point in continuous ingestion.** Closed in 0.2.0:
   `EvmCache::snapshot_generation()` is the crate-provided generation guard —
   read it around `snapshot()` and re-snapshot when it moved, so simulations
   never observe a partially applied block (G6).
4. **Full no-provider build split.** The dependency graph still includes
   provider/RPC crates. A later `rpc` feature can make those optional for pure
   offline users.
