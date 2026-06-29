# Changelog

All notable changes to `evm-fork-cache` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Pre-1.0 policy:** until `1.0.0`, breaking changes may land in **minor**
versions (`0.x.0`); patch versions (`0.x.y`) are non-breaking. The roadmap in
[`docs/ROADMAP.md`](docs/ROADMAP.md) deliberately reshapes the API before the
surface freezes at 1.0.

## [Unreleased]

_Nothing yet._

## [0.1.0] - 2026-06-25

This is the first release line. It captures the work done across the
pre-release development phases (see [`docs/ROADMAP.md`](docs/ROADMAP.md)).

### Changed

- **Breaking:** the in-memory chain ID is no longer hard-defaulted to Arbitrum
  (`42161`). When no chain ID is set explicitly and no disk `CacheConfig` is
  supplied, the cache now infers it from the provider (`eth_chainId`) and falls
  back to `1` (Ethereum mainnet) only if that query fails. Set it explicitly with
  the new `EvmCacheBuilder::chain_id` / `EvmCache::set_chain_id`.
- **Breaking:** extracted the old in-crate AMM adapter surface before public
  release. Protocol-specific storage layouts, protocol metadata, injector
  helpers, tick snapshots, and protocol log decoders belong in `evm-amm-state`;
  this crate now exposes only the generic fork cache, simulation, freshness,
  state-update, ERC-20 decoding, and event-pipeline primitives.
- **Breaking:** `ImmutableDataCache` is now token-decimals-only and its on-disk
  format version is bumped to `2`, so older metadata files with protocol payloads
  are treated as stale cache misses.
- **Breaking:** renamed the remaining generic storage-purge helpers from
  pool-oriented names to contract-oriented names:
  `has_contract_storage`, `contract_storage_slot_count`,
  `purge_contract_storage`, and `purge_contract_slots`. The old pool-named
  aliases were removed rather than deprecated before the first public release.

### Added

- **Honest performance section & benchmarks.** The README "Performance" section
  is framed against a *competent* baseline (a shared `foundry-fork-db`
  `SharedBackend` + `checkpoint`/`revert` isolation), not a naive
  fork-per-candidate strawman. It states plainly that within-block fetch count,
  single-threaded CPU, and time-to-result are ~1× against that baseline, and
  leads with the genuinely-unique wins: cross-block freshness (0 RPC fetches/block,
  pinned in `tests/event_pipeline.rs` — `foundry-fork-db`'s cache is not
  block-keyed), parallel `Send` fan-out (`benches/fanout.rs`, a modest measured
  ~1.2× on micro-sims), point-in-time consistency, and the act-then-validate
  control plane (`benches/freshness.rs`). Adds `examples/fetch_minimization_counted.rs`
  and `tests/fetch_minimization.rs` (the fetch-once mechanic, with the
  shared-backend caveat stated). The internal copy-on-write snapshot cost model
  moved to [`docs/INTERNALS.md`](docs/INTERNALS.md).
- **Forked EVM cache** (`cache::EvmCache`) backed by `foundry-fork-db` with lazy
  RPC loading and on-disk persistence for accounts, storage, bytecode, and
  immutable metadata.
- **`EvmCacheBuilder`** — a fluent constructor (`EvmCache::builder(provider)`)
  subsuming the positional `with_cache` / `from_backend` constructors, with
  block pin, EVM spec, cache-config, chain-ID, and shared-memory-capacity
  configuration. `EvmCacheBuilder::chain_id` sets the `CHAINID` opcode value
  explicitly (recommended); `EvmCache::set_chain_id` sets it post-construction.
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
  `purge_contract_storage` / `purge_contract_slots` writers and the freshness
  correction-drain are refolded onto it (signatures unchanged).
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
  pre-1.0 break policy. Generic core.
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
- **Event → state pipeline** (`events` module, Phase 4, Pillar B.2 — the *reader
  half* of the event pipeline) — turn on-chain logs into the Phase 3 `StateUpdate`
  vocabulary and drive them through the cache for reactive freshness:
  - **`EventDecoder` / `StateView`** — a decoder is a pure function of
    `(log, pre-state)` returning `Vec<StateUpdate>`; the narrow read-only
    `StateView` (implemented by `EvmCache` via `cached_storage_value`) lets
    stateful adapters read current cached state without RPC. Generic core.
  - **`DecoderRegistry`** — dispatches a log to the decoders registered for its
    emitting address (plus globals) and concatenates their output. Generic core.
  - **`Erc20TransferDecoder`** — decodes ERC-20 `Transfer` logs into relative
    balance `SlotDelta`s (skipping the zero-address mint/burn leg), with per-token
    balance-slot config. The reactive-balance case from Phase 3 §15, now
    log-driven. Generic core.
  - **`EventPipeline`** — `ingest_logs` decodes + applies a block's logs
    **log-by-log in order** (so a later log sees earlier applies) and returns a
    `BlockDigest`; `reorg_to` purges (purge-and-resync) the addresses touched
    after a new head; `reconcile` re-reads sampled event-derived slots against
    chain truth (correct **and** alarm) via the new `EvmCache::reconcile_slots`. A
    thin async `drive`/`LogSource` convenience layers the synchronous core over a
    stream. Generic core.
- **Reactive runtime** (`reactive` module, default-enabled) — a provider-neutral
  handler pipeline for logs, block notifications, and pending transaction
  signals. `ReactiveHandler`s are pure synchronous functions over
  `ReactiveInput` + `ReactiveContext` + `StateView`; they emit `StateUpdate`s,
  invalidations, resync requests, speculative requests, and hook signals. The
  runtime deduplicates inputs by `InputRef`, orders canonical logs by
  `(block_number, transaction_index, log_index)`, routes by `ReactiveInterest`
  with Alloy `Filter`s and local matchers, validates pending inputs so they
  cannot mutate canonical cache state, detects conflicting absolute writes for a
  single input, applies canonical mutations through `EvmCache::apply_updates`,
  and dispatches `ReactiveReport`s to hooks after mutation phases.
  `ReactiveRegistry` exposes consolidated Alloy log filters for provider
  subscription setup and exact local log routing with optional route keys.
  Includes a provider-agnostic `EventSubscriber` trait, an `AlloySubscriber`
  that drives default WebSocket/pubsub Alloy `subscribe_logs`,
  `subscribe_blocks`, and `subscribe_pending_transactions` streams for live log,
  block-header, and pending transaction hash inputs, with automatic
  source-specific reconnect that retries immediately first, attempts three
  times by default, and performs log gap backfill from the last seen block when
  a pubsub stream terminates, plus an adapter from legacy `EventDecoder`s to
  reactive handlers. HTTP polling `watch_logs` / `watch_pending_transactions`
  support is still exported behind the opt-in `reactive-polling` feature. Full
  block bodies, full pending transaction hydration, and arbitrary historical
  backfill remain follow-up transport work. Generic core.
- **Reactive storage resync execution** — `ReactiveRuntime::ingest_batch_with_resync`
  preserves the direct-effect behavior of `ingest_batch`, then executes surfaced
  storage resync requests through `EvmCache`'s provider-neutral
  `StorageBatchFetchFn`, applies successful values as `StateUpdate::slot`
  updates, and reports requested targets, applied updates, the resulting
  `StateDiff`, and per-target failures in `ResyncReport`. `ResyncFailureKind`
  gives downstream retry policy and metrics a stable failure classification.
  Account-field resyncs remain explicitly unsupported until a provider-neutral
  account fetch callback exists. Generic core.
- **Reactive block journaling and reorg recovery** — canonical block inputs and
  applied handler reports are retained in a depth-bounded runtime journal.
  Removed logs, explicit `ChainStatus::Reorged` inputs, and parent-hash
  discontinuities now emit `ReactiveReport::Reorg` with dropped blocks, dropped
  inputs, rollback updates/diffs, purge updates/diffs, and canceled hash-pinned
  resync requests. Reversible storage-slot changes are rolled back in reverse
  apply order; account/code changes and prior purge effects conservatively fall
  back to targeted purge updates because `StateDiff` does not carry enough data
  to reconstruct those cache entries exactly. Recovery is bounded to the
  configured `ReactiveConfig::journal_depth` (default 64): a reorg deeper than
  that — or any reorg when `journal_depth = 0` — recovers only the blocks still
  resident in the journal and emits a `tracing::warn!` for the under-recovered
  span (the freshness loop is the backstop; see `docs/KNOWN_ISSUES.md`).
  Demonstrated offline in `examples/reactive_runtime.rs`. Generic core.
- **Cold-start** (`cold_start` module, default-enabled / reactive-gated) —
  declarative warming of a working set of accounts and storage slots into the
  cache in one batched pass via `EvmCache::run_cold_start` /
  `execute_cold_start_round` and a `ColdStartPlanner` (discover slots through a
  view-call, then verify them through the `StorageBatchFetchFn`), returning a
  structured `ColdStartRunReport` (`ColdStartConfig`/`Plan`/`Results`/
  `RoundSummary`/`Step`/`RoundOutcome`/`Error`, with per-slot `SlotOutcome`s). The
  account-warming phase is serial (one fetch per declared account) and aborts the
  round on the first account failure; storage verify/probe are batched. Generic core.
- **`StateUpdate::SlotMasked`** (`state_update`, Phase 4) — a cold-aware
  read-modify-write *masked* slot write (`new = (old & !mask) | (value & mask)`)
  with the `StateUpdate::slot_masked` constructor, so a pure decoder can update
  selected bits of a **packed** storage word without clobbering the rest. A masked
  write to a cold slot is skipped and surfaced in the new
  `StateDiff.skipped_masks: Vec<SkippedMask>` (counted by `has_skipped` /
  `skipped_len`, not by the changes-only `is_empty`/`len`); `serde` on
  `SkippedMask`. Adding the variant and the field is permitted under the pre-1.0
  break policy (`StateUpdate`/`StateDiff` are `#[non_exhaustive]`). Generic core.
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
- **Fallible custom-error registration** (`errors`) —
  `RevertDecoder::try_register`, `try_register_raw`, and
  `DuplicateSelectorError` let callers reject duplicate custom-error selectors
  during decoder setup instead of relying on the warning-only ergonomic path.
- **Two-stage prefetch registry** (`prefetch_registry`) for cross-cycle
  storage-slot pre-warming.
- **Copy-on-write snapshots** (Phase 5, Pillar A) — `create_snapshot` is now a
  two-tier copy-on-write view instead of an O(total state) deep clone. The cold
  `BlockchainDb` index (layer 2) is flattened once into an internal, immutable,
  `Arc`-shared base (`Arc` per account storage map, structural sharing — no new
  dependency, Decision D1), memoized across snapshots and rebuilt copy-on-write
  only for the addresses that changed; each `create_snapshot` then folds just the
  hot CacheDB delta (layer 1) over a cheap `Arc::clone` of that base. Reads stay
  O(1) and lock-free and are bit-for-bit identical to the deep clone (pinned by
  the `tests/cow_snapshot.rs` differential-equivalence gate). The retained
  `EvmCache::create_snapshot_deep_clone()` (`#[doc(hidden)] pub`, Decision D3) is
  the equivalence reference and the A/B benchmark baseline. `EvmSnapshot` stays
  `Send + Sync` and `EvmOverlay` stays `Send`.
- **`EvmOverlay::reset()`** (Phase 5, Pillar A.2) — recycle one overlay across
  many simulations against the same snapshot without reallocating: it clears the
  per-simulation dirty layer (keeping the snapshot `Arc`, `ext_db`, and the
  reusable shared-memory buffer), reading the pristine snapshot again and behaving
  exactly like a freshly-built overlay. The 64 KiB shared-memory buffer is also
  recycled across the build→transact→revert call methods (stored as a plain
  `Vec<u8>`, so the overlay stays `Send`).
- **Configurable EVM shared-memory pre-allocation** — `SharedMemoryCapacity`
  (`Fixed(usize)` / `Auto`, default `Fixed(64 * 1024)` / 65,536 bytes) set via
  `EvmCacheBuilder::shared_memory_capacity`. `Fixed` pins the per-context working-
  memory buffer (general users running wide fan-outs of small simulations can lower
  it to cut per-overlay memory; the previous behavior is the default); `Auto` sizes
  it from the chain state loaded at build time (e.g. a bincode state file), clamped
  to a 64 KiB floor / 4 MiB ceiling. The resolved size is readable via
  `EvmCache::shared_memory_capacity()` and is propagated to every snapshot so
  snapshot-backed overlays pre-allocate the same amount. `with_cache_capacity` is
  the lower-level constructor behind the builder setter.
- **Explicit cold-account materialization** — `StateUpdate::AccountUpsert` and
  `StateUpdate::account_upsert(...)` intentionally materialize an account absent
  from both layers. Normal `StateUpdate::Account` patches are now cold-aware and
  surface skipped cold patches through `StateDiff.skipped_accounts:
  Vec<SkippedAccountPatch>`.
- **Invalidating layer-2 mutation wrapper** — `EvmCache::with_blockchain_db_mut`
  runs a synchronous direct `BlockchainDb` mutation and invalidates the Phase 5
  memoized COW base automatically after the closure returns.
- **Exact access-list RLP data-gas helper** —
  `access_list::access_list_rlp_data_gas(&AccessList)` returns the EIP-2930 RLP
  calldata gas for an access list and backs the L2 profitability calculation.
- **Versioned on-disk cache envelope** — binary EVM state, bytecode, and
  `ImmutableDataCache` files now start with crate-specific magic bytes plus a
  `u32` version before the bincode payload.
- **Public-release CI gates** — the GitHub Actions workflow now enforces format,
  clippy on all targets, all-target tests, doctests, warning-free docs, bench
  compilation, package verification, and the MSRV library check.

### Changed

- **Duplicate `RevertDecoder` registrations keep the first selector owner.**
  `register`, `register_raw`, and builder-style `with_error` no longer replace an
  existing custom-error decoder for the same 4-byte selector. They retain the
  original registration and emit a `tracing::warn!`; callers that want hard
  failure can use `try_register` / `try_register_raw`.
- **`EvmCache::create_snapshot` is now `&mut self`** (Phase 5, Decision D5) —
  taking a snapshot memoizes/refreshes the cold copy-on-write base, which requires
  a mutable borrow. All callers (the freshness controller, tests, examples,
  benches) are updated; the return type (`Arc<EvmSnapshot>`) is unchanged.
  Permitted under the pre-1.0 break policy.
- **`EvmCache::inject_storage_batch` is now `&mut self`** (Phase 5) — the
  layer-2 bulk write now marks the touched addresses dirty for the memoized
  copy-on-write base. The write itself is still a direct backend (layer-2) write
  with the same semantics; only the receiver mutability changed.
- **Raw layer-2 handles were renamed to unchecked accessors** (Phase 5) —
  `EvmCache::blockchain_db()` is now `unchecked_blockchain_db()` and
  `EvmCache::backend()` is now `unchecked_backend()`. The rename makes the
  bypass explicit; use `with_blockchain_db_mut` for synchronous direct writes that
  should automatically invalidate the snapshot base.
- **Persistence APIs now return `Result<()>`** — `cache::save_binary_state`,
  `PrefetchRegistry::save`, and `EvmCache::flush` report serialization,
  directory-creation, and write failures to explicit callers. `Drop` remains
  best-effort and logs `flush()` errors.
- **Block re-pins clear stale context** — `set_block` sets `block_number` only
  for concrete numeric pins, clears it for tag/hash/`None` pins, and clears stale
  `basefee` on block changes and on non-concrete pin calls that can drift under
  the same tag. `repin_to_block` follows the same no-stale-basefee rule; callers
  refresh `NUMBER`/`BASEFEE` via `set_block_context` after fetching the new
  header.
- **Legacy raw-bincode cache files are treated as misses** — the versioned cache
  envelope intentionally rejects unversioned `evm_state.bin`, `bytecodes.bin`,
  and `immutable_data.bin` payloads rather than trying to deserialize ambiguous
  layouts.
- Simulation entry points that distinguish failure modes return
  `SimulationResult<T>` (`Result<T, SimError>`), separating decoded reverts,
  EVM halts, and host errors.

### Fixed

- **Duplicate custom-error selectors no longer shadow silently.** A second
  registration for the same selector is now observable through
  `DuplicateSelectorError` on the fallible APIs, or through a warning on the
  ergonomic `register` / `register_raw` / `with_error` path. Decoding keeps using
  the first registered selector owner.
- **Cold absolute account patches no longer mask on-chain accounts.**
  `StateUpdate::Account` on an account absent from both layers now skips instead
  of writing `AccountInfo::default()` fields through the shared backend. The
  skipped patch is visible in `StateDiff.skipped_accounts`; intentional cold
  creation uses `StateUpdate::AccountUpsert`.
- **Access-list profitability no longer conflates provider failures with
  unprofitable lists.** `SmartAccessList::into_access_list_if_profitable` and
  `access_list_if_profitable` now propagate provider/pricing failures as `Err`
  and reserve `Ok(None)` for empty, zero-priced, or genuinely unprofitable lists.
- **`simulate_call_with_balance_deltas` now isolates balance reads and reports a
  real access list.** Pre/post `balanceOf` calls run outside the target-call
  checkpoint, so they cannot warm the target call or commit side effects. The
  method commits only the target call when requested and returns the deduplicated
  EIP-2930 accounts/slots from balance reads plus the target call.
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
  Phase 5 later tightened this further: real field changes on cold accounts now
  skip through `StateDiff.skipped_accounts` unless the caller uses
  `StateUpdate::AccountUpsert`.
- **`account_state`-awareness extended to the snapshot + account-info paths**
  (Phase 3 fix-review, HIGH + MED). A follow-up adversarial review found the §16.0
  `cached_storage_value` fix had not been propagated to two sibling read paths:
  - `create_snapshot` now mirrors the live read: a `StorageCleared`/`NotExisting`
    account's storage is captured as **only** its overlay slots (shadowed backend
    slots dropped) and recorded in a new `EvmSnapshot.storage_cleared` set, so
    `EvmSnapshot::storage_value` and snapshot-backed `EvmOverlay`s read such a
    slot as ZERO instead of the shadowed backend value (which also kept the
    background freshness validator's `old` consistent with `verify_slots`). The
    `EvmOverlay` storage read honors the set and does **not** fall through to its
    `ext_db` for a cleared account.
  - `loaded_account_info` now mirrors revm `DbAccount::info()`: a `NotExisting`
    overlay account is treated as absent (returns `None`), so a `BalanceDelta` /
    partial `Account` patch skips rather than computing against a stale `info`.
  - `write_account_info_through` normalizes a `ZERO` `code_hash` to `KECCAK_EMPTY`
    so both cache layers store an identical hash (matching revm's `insert_contract`).
- **`account_state`-awareness completed on the account (`basic`) axis** (round-2
  review, HIGH). The snapshot path still leaked a `NotExisting` account's stale
  info: `create_snapshot` inserted it into `accounts`, so `EvmOverlay::basic`
  returned a phantom existing account where live revm / `loaded_account_info`
  return `None`. Now `create_snapshot` excludes `NotExisting` accounts from
  `accounts`/`code_by_hash` and records them in a new
  `EvmSnapshot.accounts_not_existing` set; `EvmOverlay::basic` returns `None` for
  them (no `ext_db` fall-through). `target_account_info` (deploy path) and
  `loaded_account_info` (code_hash normalized at load) were brought into line too.
- **Freshness validator trust contract hardened** (Phase 2 review). The
  background validator no longer returns a *trusted* verdict on incomplete or
  ambiguous verification:
  - **Fixed-point round cap → `Unverified`.** Exceeding `MAX_VALIDATION_ROUNDS`
    (corrections kept opening new volatile slots) now returns
    `Validation::Unverified` and queues no corrections, instead of a best-effort
    `Corrected` resting on un-verified state.
  - **Corrected re-run host error → `Unverified`.** A failed corrected re-run
    (a `transact` error, not a revert/halt) returns `Unverified` rather than
    silently keeping the stale optimistic result.
  - **Missing fetcher results → `Unverified`.** A new `collect_fetch_results`
    helper requires the batch fetcher to return *every* requested slot; an omitted
    slot yields `Unverified` instead of defaulting to zero (which could produce a
    false confirmation/correction with a custom fetcher).
- **`call_raw_with_access_list*` reverts its checkpoint on transact errors**
  (Phase 2 review; `EvmCache` + `EvmOverlay`). Previously the host-error path
  `?`-returned before `checkpoint_revert`, leaving the journal checkpoint
  un-reverted; both methods now revert on every path. This is recorded in the
  fixed-issues section of `docs/KNOWN_ISSUES.md`.

### Notes

- MSRV is Rust 1.88; edition 2024. Both are enforced in CI.
- `EvmCache` requires a multi-thread tokio runtime for any RPC-touching path.
- See [`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md) for current limitations.

[Unreleased]: https://github.com/KaiCode2/evm-fork-cache/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/KaiCode2/evm-fork-cache/releases/tag/v0.1.0
