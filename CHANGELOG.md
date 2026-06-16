# Changelog

All notable changes to `evm-fork-cache` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Pre-1.0 policy:** until `1.0.0`, breaking changes may land in **minor**
versions (`0.x.0`); patch versions (`0.x.y`) are non-breaking. The roadmap in
[`docs/ROADMAP.md`](docs/ROADMAP.md) deliberately reshapes the API before the
surface freezes at 1.0.

## [Unreleased]

This is the first release line. It captures the work done across the
pre-release development phases (see [`docs/ROADMAP.md`](docs/ROADMAP.md)).

### Added

- **Forked EVM cache** (`cache::EvmCache`) backed by `foundry-fork-db` with lazy
  RPC loading and on-disk persistence for accounts, storage, bytecode, immutable
  metadata, and Uniswap V3-style tick snapshots.
- **`EvmCacheBuilder`** — a fluent constructor (`EvmCache::builder(provider)`)
  subsuming the positional `with_cache` / `from_backend` constructors, with
  per-instance cache-speed configuration.
- **Snapshots and overlays** — `create_snapshot()` produces an immutable,
  `Send + Sync` `EvmSnapshot`; `EvmOverlay` is a cheap per-simulation clone for
  isolated parallel evaluation.
- **Freshness control plane** (`freshness` module, Phase 2) — the four-layer
  model (`Validity`/`FreshnessRegistry`, `SlotObservationTracker`,
  `FreshnessPolicy`, `FreshnessController`), a configurable `FreshnessClock`
  (`BlockClock`/`WallClock`), and the optimistic verify-and-rerun execution loop
  with deferred validation (`SpeculativeSim`/`Validation`).
- **Freshness primitives on `EvmCache`** — `verify_slots`, `purge_account`,
  `set_storage_batch_fetcher`; `EvmOverlay::call_raw_with_access_list` and
  `override_slot` for read-set capture and corrected re-runs.
- **State-update vocabulary & apply primitive** (`state_update` module, Phase 3,
  Pillar B.1) — a generic `StateUpdate` enum (`Slot` / partial-`AccountPatch`
  `Account` / `Purge` by `PurgeScope`) plus `EvmCache::apply_update` /
  `apply_updates`, the single dual-layer write-through primitive (backend always,
  overlay-if-present, no new overlay account materialized), returning a structured
  `StateDiff` (`SlotChange`s, `AccountChange`s, `PurgeRecord`s) that records only
  actual changes. The existing `inject_storage_batch_fresh` / `purge_account` /
  `purge_pool_storage` / `purge_pool_slots` writers and the freshness
  correction-drain are refolded onto it (signatures unchanged); generic, builds
  with `--no-default-features`.
- **Relative / read-modify-write state updates** (`state_update`, Phase 3 §15) —
  a saturating `SlotDelta` (`Add`/`Sub`, clamping at `U256::MAX`/`U256::ZERO`), a
  `StateUpdate::SlotDelta { address, slot, delta }` variant (with the
  `StateUpdate::slot_delta` constructor) so deltas flow through `apply_updates`,
  and `EvmCache::modify_slot(address, slot, |Option<U256>| -> Option<U256>)` as
  the general closure escape hatch. Relative application is **cold-aware**: a
  delta against a slot absent from both layers is not applied (it would corrupt an
  unknown value) but surfaced in the new `StateDiff.skipped: Vec<SkippedDelta>`
  field for the caller to fetch+seed and retry. `skipped` is informational
  metadata and does not affect `StateDiff::is_empty` / `len` (changes-only).
  Adding the `StateDiff.skipped` field is a struct change permitted under the
  pre-1.0 break policy. Generic core (builds `--no-default-features`).
- **Post-audit state-update remediation** (`state_update`, Phase 3 §16):
  - **`serde`** — `Serialize`/`Deserialize` derived (unconditionally) on the whole
    vocabulary (`SlotDelta`, `StateUpdate`, `AccountPatch`, `PurgeScope`) and the
    diff (`StateDiff`, `AccountChange`, `PurgeRecord`, `SkippedDelta`,
    `SkippedBalanceDelta`) plus `freshness::SlotChange`, so updates can be shipped
    over the wire and diffs persisted.
  - **`#[non_exhaustive]`** on `StateDiff` and `AccountPatch` (both
    `Default`/builder-constructed), so future field additions are non-breaking. The
    leaf record types (`SlotChange`/`AccountChange`/`PurgeRecord`/`SkippedDelta`/
    `SkippedBalanceDelta`) are deliberately left exhaustive — they are routinely
    built as struct literals in equality assertions.
  - **Relative native-balance updates** — a `StateUpdate::BalanceDelta { address,
    delta: SlotDelta }` variant (with `StateUpdate::balance_delta`), the
    `EvmCache::modify_account_balance(addr, |Option<U256>| -> Option<U256>)`
    closure escape hatch, a new `StateDiff.skipped_balances:
    Vec<SkippedBalanceDelta>` field, and `SkippedBalanceDelta`. Cold-aware: a delta
    on an account absent from both layers is skipped and surfaced (never
    materialized). Adding `skipped_balances` is a struct change permitted under the
    pre-1.0 break policy.
  - **Discoverable skip accessors** — `StateDiff::has_skipped()` / `skipped_len()`
    / `is_fully_applied()`, counting **both** `skipped` and `skipped_balances`, so a
    silently-dropped cold relative update is easy to detect (the changes-only
    `is_empty()`/`len()` do not reflect skips).
  - **Constructor symmetry** — `StateUpdate::nonce(addr, u64)`,
    `StateUpdate::code(addr, Bytes)`, `StateUpdate::account(addr, AccountPatch)`.
- **Batched single-lock fast-path for `apply_updates`** (Phase 3 §16.9): a run of
  consecutive `Slot`/`SlotDelta` writes now holds the backend storage write-guard
  once for the run (the guard is dropped before any `Account`/`BalanceDelta`/
  `Purge` update to avoid deadlocking the non-reentrant `RwLock`, then re-acquired),
  and the `SlotDelta` double-read of the old value is eliminated. The result is
  byte-identical to folding `apply_update` over the batch (pinned by the
  batched==sequential equivalence test). Generic core.
- **Configurable transaction & block environment** — `TxConfig` (value, gas
  limit, gas price, nonce, access list) threaded through `call_raw_with`; block
  context setters (`set_coinbase`, `set_prevrandao`, `set_block_gas_limit`).
- **Transfer-inspector simulation** (`inspector`) reporting per-token balance
  deltas from the `Transfer` event stream.
- **Access-list tooling** (`access_list`, `access_set`) — `StorageAccessList`
  touch-set capture, EIP-2930 list construction, and L2 profitability estimation.
- **Multicall3 batching** (`multicall`).
- **Deployment & etching** (`deploy`) — deploy from creation code, etch Foundry
  artifacts over forked contracts; **CREATE3** address derivation (`create3`).
- **Extensible revert decoder** (`errors`) — native `Error(string)` / `Panic(uint256)`
  decoding plus one-line custom-error registration; typed `SimError`
  (`Revert` / `Halt` / `Host`).
- **Two-stage prefetch registry** (`prefetch_registry`) for cross-cycle
  storage-slot pre-warming.
- **`protocols` feature** (default-on) gating the Uniswap V2/V3 storage layouts,
  V3 tick snapshots, and `inject_v3_*` / `inject_v2_pool_metadata` helpers, so
  the generic engine builds with `--no-default-features`.

### Changed

- Simulation entry points that distinguish failure modes return
  `SimulationResult<T>` (`Result<T, SimError>`), separating decoded reverts,
  EVM halts, and host errors. `SimulationErrorKind` remains as a deprecated alias.
- **`inject_v2_pool_metadata` / `inject_v3_tick_bitmap*` / `inject_v3_ticks*`
  (`protocols`) now write through both cache layers** (Phase 3, Decision 2).
  Previously these wrote only the CacheDB overlay (layer 1); they are now folded
  onto the write-through `StateUpdate::Slot` primitive, so the injected slots also
  land in the BlockchainDb backend (layer 2). Signatures and return values are
  unchanged and the visible `token0()`/`tickBitmap()`/`ticks()` reads are the
  same; only the slot *placement* across layers changed. See
  [`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md). (The cold-backfill
  `inject_storage_batch` keeps its layer-2-only intent and is unchanged.)

### Fixed

- **`cached_storage_value` silent-corruption bug** (Phase 3 §16.0, audit HIGH +
  MED). For a storage slot absent from an overlay account whose revm
  `account_state` is `StorageCleared` or `NotExisting`, the accessor now returns
  `Some(U256::ZERO)` — mirroring what the live EVM `SLOAD`s
  (`CacheDB::storage_ref`) — instead of falling through to the BlockchainDb
  backend and returning a *shadowed* backend value the EVM never sees. The old
  behavior let a `SlotDelta` / `modify_slot` compute a relative update against a
  base the EVM never reads (silent state corruption) and mis-recorded
  `apply_slot`'s `SlotChange.old` / change predicate. This also closes the
  same-root mismatch shared by `verify_slots` / `inject_storage_batch_fresh`.
- **No-op `Account` patch no longer materializes a backend account** (Phase 3
  §16.1, audit LOW). `apply_account_patch` now computes the field change first and
  **skips both layer writes** (returning an empty diff) when no field actually
  changes, instead of unconditionally inserting `AccountInfo::default()` into the
  shared backend for an all-`None` (or value-unchanged) patch on an absent address.
  A real field change still materializes the backend account (unchanged intent).

### Notes

- MSRV is Rust 1.88; edition 2024. Both are enforced in CI.
- `EvmCache` requires a multi-thread tokio runtime for any RPC-touching path.
- See [`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md) for current limitations.

[Unreleased]: https://github.com/KaiCode2/evm-fork-cache/commits/main
