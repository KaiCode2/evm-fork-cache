# Changelog

All notable changes to `evm-fork-cache` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Pre-1.0 policy:** until `1.0.0`, breaking changes may land in **minor**
versions (`0.x.0`); patch versions (`0.x.y`) are non-breaking. The roadmap in
[`docs/ROADMAP.md`](docs/ROADMAP.md) deliberately reshapes the API before the
surface freezes at 1.0.

## [Unreleased]

Remaining from the 0.2.0 plan, tracked as fast-follow: the docs/examples
polish remainder (a toy constant-product AMM example and the end-to-end
reactive integration tests enumerated in
[`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md)).

## [0.2.0] - 2026-07-05

Closes the top publication-review gaps (account-level freshness, strict
block-context, bounded `derived_slots`, conservative deep-reorg behavior) and
lands the full Phase-8 liveness plan (spec steps 1–6). Breaking changes are
grouped under **Changed**; they are permitted pre-1.0 (see the policy above).

### Performance

The round's RPC-economics wins (each documented with numbers and a reproduction
path; see the README "Performance & honest trade-offs" section for the full,
hedged picture):

- **Bulk `eth_call` storage extraction is now the default loader.** One
  state-override call reads thousands of slots at a flat ~26 CU instead of one
  point read each — e.g. 10,000 slots in 1 call vs 200,000 CU, and a full
  Uniswap V3 tick range (7,674 slots) in 2 calls / 52 CU (**~2,952× cheaper**),
  with automatic point-read fallback. Methodology + tables:
  [`docs/bulk-storage-extraction.md`](docs/bulk-storage-extraction.md);
  reproduce with `RPC_URL=… cargo run --release --example bulk_storage_bench`.
- **Verified code-seed cold starts.** Materializing a known contract set's code
  and balances is one bulk call settled against on-chain `EXTCODEHASH` rather
  than three reads per account: **~46× cheaper, ~25.6× faster** for 20 contracts
  (48 ms vs ~1.2 s, 26 CU vs 1,200). Same doc + bench (scenario 11).
- **Proof economics.** `eth_getProof` (no bulk substitute) is kept off the
  per-block path: root-gate probes fire on a cadence (default every 16 blocks →
  **16× fewer probes** by construction) and batch every gated account into one
  bounded fan-out (**≈4.7–7.3× over serial** across runs, live-measured for a
  50-account sweep — bounded by the concurrency cap, larger when per-proof
  latency is higher; reproduce with `E2E_RPC_URL=… cargo test --test
  liveness_root_gate -- --ignored`).

### Added

- **Mid-lifecycle adapter register/unregister (`ReactiveEngine`).** A new
  `ReactiveEngine` binds a `ReactiveRuntime` to an `EventSubscriber` and drives
  handler lifecycle as one operation, for consumers that add and drop adapters
  while the cache runs (e.g. register an AMM on a `PoolCreated` event, drop it
  later). `register_handler` updates runtime routing and subscriber interests
  together and, once ingestion has journaled a canonical block, backfills the
  new handler from that block automatically — closing the discovery→subscription
  gap with no caller bookkeeping (`register_handler_with_backfill` for deeper
  history, `register_handler_live_only` to opt out; `sync_handler_interests`
  bootstraps a pre-populated runtime). `unregister_handler` removes routing and
  transport for that handler only; the runtime adds `last_canonical_block()`,
  `pending_resyncs()`, `cancel_pending_resyncs(address)`, `handler_ids()`,
  `contains_handler`, `handler_interests`, and `unregister_handler` for the full
  teardown recipe (unregister + `untrack_account` + `cancel_pending_resyncs`;
  cache eviction stays explicit). Under the hood the new
  `InterestOwnerSubscriber` trait lets a subscriber add/remove interests keyed by
  a stable per-adapter `HandlerId` without disturbing unrelated owners; growing
  an owner's filter set carries its delivery anchor across the change and
  self-heals the gap, identical filters across owners share one subscription, and
  retired filters release their bookkeeping. `SubscriberBackfill` describes the
  anchor for owner-scoped `get_logs` backfill (`ReactiveEngineError` /
  `ReactiveEngineRegisterError` are the engine's typed errors). All existing
  runtime/subscriber APIs are unchanged; `register_interests` remains the
  full-replacement setup path.
- **Queryable cache health + metrics.** `ReactiveRuntime::health() -> CacheHealth`
  (`Healthy` / `Degraded` / `Unhealthy`) and `metrics() -> CacheMetricsSnapshot`
  (atomic counters: `deep_reorgs`, `reorgs_recovered`, `resync_requests`/`_failures`,
  `missed_ranges`, `coverage_gaps`, `pending_contamination`). `ReactiveReport::Health`
  transition reports and a caller-invoked `reset_health()`.
- **Conservative deep-reorg self-heal + missed-range detection.** A forward gap in
  the canonical block sequence (block *N* → *N+k*) is no longer silently accepted:
  it emits `ReactiveReport::MissedBlockRange` (+ `ResyncReason::MissedBlockRange`)
  and escalates health, while still applying the arriving block. Repeated trust-loss
  events (deep reorg beyond the journal, or a missed range) escalate
  `Healthy → Degraded → Unhealthy` (a "stop until rebuilt" signal).
- **Account/root fetcher seam.** `EvmCache::account_proof_fetcher` /
  `set_account_proof_fetcher` (`AccountProofFetchFn` over `eth_getProof`, returning
  `AccountProof`); `ResyncTarget::Account` resyncs now resolve through it
  (materializing so cold accounts are not silently skipped).
- **Bulk storage extraction (`bulk_storage` module) — now the default storage
  loader.** Every provider-backed cache ships a `StorageBatchFetchFn` that
  loads thousands of slots per `eth_call` by overriding target code with
  [Dedaub's 23-byte extractor](https://dedaub.com/blog/bulk-storage-extraction/)
  (storage survives a code override), plus a Multicall3-dispatch path that
  spans many contracts in one call — with the classic point-read fetcher as
  automatic fallback (tiny requests, providers without override support —
  which also latch the fetcher to point reads after consecutive failures —
  and precompile targets). Public surface:
  `bulk_call_storage_fetcher[_with_fallback]`, the async core
  `fetch_slots_bulk`, `BulkCallConfig` (chunking, concurrency,
  `CallDispatch::CallMany` for Erigon-lineage endpoints where one 20-CU
  `eth_callMany` request carries the whole batch), `planned_call_count`,
  `StorageFetchStrategy` + `EvmCacheBuilder::{storage_fetch_strategy,
  bulk_call_config}` (opt-out / tuning), `point_read_storage_fetcher` (the
  extracted classic path), and both Shanghai (`PUSH0`) and pre-Shanghai
  extractor variants — all executed against revm in the offline test suite.
  Measured on Alchemy: 10,000 slots in one 26-CU call (vs 200,000 CU as point
  reads); a full Uniswap V3 pool tick range — 7,674 slots — in 2 calls /
  52 CU; 100 contracts × 30 slots in one call
  ([benchmarks + limitations](docs/bulk-storage-extraction.md)).
  `StorageFetchError` and `RuntimeError` now derive `Clone` so one
  chunk-level failure can be reported per affected slot.
- **Custom storage programs + companion extractors + prewarm.**
  `StorageProgram` / `run_storage_program[s]` inject caller-supplied bytecode
  via the same override transport, enabling data-*dependent* extraction
  derived in-EVM (a worked one-shot Uniswap V3 observation-ring loader — zero
  calldata — ships in the benchmark example and revm tests);
  `fetch_account_fields_bulk` (`BALANCE` + `EXTCODEHASH` for many accounts in
  one call) and `fetch_block_context` (seven env words in one call) ride the
  same mechanism; `EvmCache::prewarm_slots` bulk-loads a declared working set
  into layer 2 through the installed fetcher, returning a `PrewarmReport`.
- **Verified code seeding & local etch.** Adapters can push runtime bytecode
  into the cache instead of paying an `eth_getCode` (plus the lazy backend's
  balance/nonce round trips) per address, with per-address trust marks
  (`CodeSeedState`; absence of a mark = RPC-origin) persisted to
  `code_seeds.bin` — saved *before* `bytecodes.bin` and pruned on load, so an
  unverified claim can never masquerade as chain-fetched across restarts.
  `seed_account_code[_with]` records a *canonical claim* (`Pending`);
  `verify_code_seeds()` settles the whole pending set against on-chain
  `EXTCODEHASH` in **one** `eth_call` through the new `AccountFieldsFetchFn`
  seam (default-wired to `fetch_account_fields_bulk`), marking matches
  `Verified` durably (real balance patched from the same response) and
  purging contradicted claims for refetch — `CodeVerifyReport` buckets
  `mismatched` / `not_deployed` / `codeless` / `unverifiable` (the last is
  fail-safe: transport failures keep seeds `Pending`). Chain-fetched code
  beats templates: an equal-hash seed over RPC-origin code verifies
  instantly with zero RPC; a conflicting one errors
  (`CacheError::CodeSeedConflict`) without touching the cache.
  `etch_account_code` is the raw-bytes *deliberate divergence* sibling
  (`Etched`, never verified); `override_account_code*` and `deploy_contract`
  targets now also record `Etched`, making `etched_accounts()` the single
  local-divergence health surface. The cold-start driver gained a
  `verify_code` phase that runs **first** — no discover sim executes over an
  unverified claim — recording `ColdStartResults.code_verifications` and
  guarded by `ColdStartError::NoAccountFieldsFetcher` for pending-bearing
  rounds. Spec: `docs/verified-code-seeding-spec.md`.
- **Strict block context + engine-driven env refresh.** `BlockContextRequirements`
  (`strict`/`lenient`/per-field) + `BlockContextError`;
  `EvmCacheBuilder::strict_block_context` / `block_context_requirements` and a
  fallible `try_build` (`EvmCache::new` stays infallible + lenient);
  `EvmCache::advance_block(header)` refreshes the full block env
  (number/basefee/coinbase/prevrandao/gas-limit/timestamp) with the same strict
  validation, driven from the reactive canonical-header path; new
  `coinbase()` / `prevrandao()` / `block_gas_limit()` getters.
- **`Validity` stamping (opt-in).** `ReactiveRuntime::enable_freshness_stamping()`
  stamps canonical event-derived writes `Validity::ValidThrough(N)` so
  event-maintained slots stop being needlessly re-verified.
- **storageHash root gate + `TrackingPolicy` + `RootGateCadence`.**
  `TrackingPolicy` (`Slots` / `WholeAccount` / `Scalars`) + `track_account`;
  the root gate emits `ReactiveReport::CoverageGap` (+
  `ResyncReason::RootMoved`) when a tracked account's storage root moves with
  no covering decoder, and `Scalars` tracks native balance/nonce/code (which
  do not move the storage root). The gate fires per `RootGateCadence`
  (`set_root_gate_cadence`), **default every 16 canonical blocks, never
  per-block**: `eth_getProof` is the slowest read the crate issues, and the
  gate diffs against its persisted baseline (never block-over-block), so
  skipping blocks trades bounded detection lag for a 16× probe-cost cut
  without losing detection. The decoder-touched set accumulates across
  skipped blocks (drained per firing) so covered writes never false-positive
  as gaps; `every_n_blocks(1)` restores per-block probing and `Disabled`
  turns the gate off. All root-gate and account-resync probes now go through
  **one seam invocation per firing** (grouped by resync block), and the
  default proof fetcher fans them out with bounded, order-preserving
  concurrency — `EvmCacheBuilder::max_concurrent_proofs` (default 8) — so a
  fleet probe costs ~`ceil(N / cap)` round trips instead of `N`.
- **Cold-start root baseline (`roots.bin`).** `RootBaseline` persists each
  tracked account's `storageHash` alongside the state file (versioned binary,
  magic `EFCROOT`); on restart, `RootBaselinePlanner` root-probes the baseline
  via `eth_getProof` and converts moved roots into targeted resyncs instead of
  trusting stale disk state (restart-drift detection, Phase-8 step 5).
- **Tier-3 trace-backed resync.** The reactive runtime resolves matching
  resync targets from one `debug_traceBlockByNumber` (`prestateTracer`,
  `diffMode`) block diff before falling back to storage/account point reads —
  `BlockStateDiffFetchFn` (+ `set_block_state_diff_fetcher`),
  `BlockStateDiff`/`BlockStateAccountDiff`/`BlockStateStorageDiff`, and
  `ReactiveRuntime::ingest_batch_with_resync` (Phase-8 step 6). Measured
  economics/latency in
  [`docs/trace-resync-benchmarks.md`](docs/trace-resync-benchmarks.md).
- **Bundle cost accounting.** `BundleResult::successful_tx_gas` and
  `reverted_tx_gas` — net searcher cost (`coinbase_payment + reverted_tx_gas`) is now
  direct under `AllowReverts`.
- **Snapshot-consistency generation guard.** `EvmCache::snapshot_generation()`
  — an opaque monotonic counter bumped by targeted state writes
  (`apply_update`/`apply_updates`/`modify_slot` and everything built on them)
  and block re-pins (`set_block`/`advance_block`), but not by cold prefetch.
  Read it around `snapshot()` to detect (and retry) a snapshot taken
  mid-block during continuous ingestion, closing the ROADMAP-listed
  consistency gap (G6).

### Changed (breaking)

- **Freshness verdict taxonomy is honest about scope.** `Validation::Confirmed` is
  renamed `ConfirmedStorage` (it only ever meant "no volatile storage slot changed",
  not account-level state); a new `ConfirmedFull` covers storage **and** verified
  account fields; `Validation::Corrected`'s `changed` field is renamed
  `changed_slots` and gains `changed_accounts: Vec<AccountChange>`.
- **`ResyncFailureKind::UnsupportedAccountTarget` removed**, replaced by
  `MissingAccountFetcher` / `AccountFetchFailed` / `AccountFetchOmitted` now that
  account resync is supported.
- **Growth-bound enums are now `#[non_exhaustive]`.** `ReactiveReport`,
  `ResyncReason`, `ResyncFailureKind`, `TrackingPolicy`, `CacheHealth`, and the
  `CacheMetricsSnapshot` struct are marked `#[non_exhaustive]` (matching the
  `StateUpdate`/`PurgeScope` precedent): downstream `match`es need a wildcard arm,
  and future variants/counters will no longer be breaking changes.
- **Snapshot API names are now role-based.** The low-level in-place rollback copy
  is `EvmCache::checkpoint()` and the immutable fan-out view is
  `EvmCache::snapshot() -> Arc<EvmSnapshot>`. The pre-0.2.0 `snapshot()` /
  `create_snapshot()` public names were removed rather than aliased; the hidden
  benchmark/reference helper is now `snapshot_deep_clone()`.
- **Storage batch tuning is per instance.** `StorageBatchConfig` exposes
  `slots_per_batch` and `max_concurrent_batches` for the provider-backed
  storage fetcher. `EvmCacheBuilder::storage_batch_config(...)` accepts exact
  values, `CacheSpeedMode` remains as presets via `From<CacheSpeedMode>`, and
  `EvmCacheBuilder::speed_mode(...)` is shorthand. The process-global
  `set_cache_speed_mode` / `cache_speed_mode` functions and static are removed.
- **`SpeculativeSim::validate()` returns `Result<Validation>`.** Validator-owned
  uncertainty still returns `Validation::Unverified`, while a failed background
  task (for example a panic) now surfaces as an error instead of being folded into
  a verdict or panicking on the handle.
- **Crate-owned fallible APIs now return typed errors instead of `anyhow`.**
  Public helpers expose domain errors such as `CacheError`, `StorageFetchError`,
  `RpcError`, `FreshnessError`, `DeployError`, `MulticallError`, and
  `AccessListError`; `SimError::Other` now carries `SimHostError`. `anyhow` is no
  longer a normal library dependency, though examples/tests may still use it as
  harness glue.

### Fixed

- **`EventPipeline::derived_slots` no longer grows unbounded.** It is now a
  block-horizon ring bounded to `ReorgConfig::depth` (mirroring the `touched` ring),
  so steady-state ingestion does not leak memory.
- **`BLOCKHASH`-reading sims fail closed in freshness validation.** Validator
  overlays carry no block hashes, so an in-lookback-range `BLOCKHASH` read
  resolves to ZERO; such sims were previously eligible for a silent
  `ConfirmedStorage`. The read is now recorded
  (`EvmOverlay::blockhash_zero_fallback`) and the verdict is
  `Validation::Unverified` — on the optimistic pass and on corrected re-runs.
  Out-of-lookback reads return the spec-mandated ZERO and are not flagged.
- **Block-diff traces now surface full account deletions.** A SELFDESTRUCTed
  account (present in the trace's `pre`, absent from `post`) gets explicit
  zeroed balance/nonce/code in `BlockStateDiff`, so account-target resyncs
  resolve from the trace instead of falling back to point reads.

## [0.1.0] - 2026-06-30

This is the first release line. It captures the work done across the
pre-release development phases (see [`docs/ROADMAP.md`](docs/ROADMAP.md)).

### Added

- **Bundle simulation + coinbase accounting (Phase 6 Track A+B).** New
  `EvmOverlay::simulate_bundle` (and the cache-side convenience
  `EvmCache::simulate_bundle`, which snapshots internally and never mutates the
  cache) apply an ordered sequence of `Call`-kind transactions over **cumulative**
  block state on a single overlay — transaction `i` observes the committed writes
  of `0..i`. A `RevertPolicy` chooses between `Atomic` (any revert rolls the whole
  bundle back) and `AllowReverts(indices)` (whitelisted reverts roll back only
  their own transaction and execution continues). `BundleResult.coinbase_payment`
  reports the miner payment as the block beneficiary's balance delta — the honest
  priority fee plus any direct coinbase tips; revm already burns the base-fee
  portion in-EVM (EIP-1559), so no base-fee correction is applied. New public types
  live in the `bundle` module and are re-exported at the crate root: `BundleTx`,
  `BundleOptions`, `RevertPolicy`, `TxOutcome`, `BundleResult`.
- **`EvmCache::set_basefee(U256)`** installs a block base fee (the `BASEFEE`
  opcode) that propagates into the next `create_snapshot()`, so offline caches
  with no fetched header can exercise base-fee-aware (`Mainnet`) bundle accounting.
- Call-frame tracing (`tracing` module): `CallTracer`, a `revm::Inspector` that
  reconstructs the call-frame tree (`CallTrace`) of a simulation — top-level call
  plus nested `CALL`/`STATICCALL`/`DELEGATECALL`/`CALLCODE` and `CREATE`/`CREATE2`
  frames, each with from/to/value/input/gas/output/`CallStatus`/depth/subcalls.
  Implemented via the `call`/`call_end`/`create`/`create_end` hooks (no
  opcode/step or `SLOAD`/`SSTORE` tracing). Re-exported at the crate root as
  `CallTracer`, `CallTrace`, `CallKind`, `CallStatus`.
- `InspectorStack<A, B>`, a composing `revm::Inspector` that fans out every hook
  to two inner inspectors so, e.g., a `CallTracer` and a `TransferInspector`
  capture independently in one pass. Re-exported at the crate root.
- `EvmOverlay::call_raw_with_inspector`: a public, inspector-generic single-call
  seam that attaches any `revm::Inspector` (honoring `TxConfig` and `commit`) and
  returns the raw `ExecutionResult` (a revert/halt is `Ok`, not `Err`) alongside
  the inspector for the caller to read.

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
  block bodies and full pending transaction hydration remain follow-up transport
  work; owner-scoped log backfill can fetch from an explicit block anchor when
  adding new interests. Generic core.
- **Reactive storage resync execution** — `ReactiveRuntime::ingest_batch_with_resync`
  preserves the direct-effect behavior of `ingest_batch`, then executes surfaced
  storage resync requests through `EvmCache`'s provider-neutral
  `StorageBatchFetchFn`, applies successful values as `StateUpdate::slot`
  updates, and reports requested targets, applied updates, the resulting
  `StateDiff`, and per-target failures in `ResyncReport`. `ResyncFailureKind`
  gives downstream retry policy and metrics a stable failure classification.
  Account targets resolve through the provider-neutral account proof fetcher;
  missing, failed, or omitted account fetches surface as typed failures. Generic
  core.
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

[Unreleased]: https://github.com/KaiCode2/evm-fork-cache/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/KaiCode2/evm-fork-cache/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/KaiCode2/evm-fork-cache/releases/tag/v0.1.0
