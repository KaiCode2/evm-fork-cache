# Known issues & limitations

A living triage list of bugs, smells, and limitations surfaced during the
publication-readiness review. Items here are either **remaining limitations** or
**recently-fixed issues kept for auditability**; behavior-changing fixes should
carry red/green tests and a `CHANGELOG.md` entry.

Confidence legend: **[V]** verified against the source during review;
**[R]** reported by the review and worth confirming before acting.

## Recently fixed before public release

1. **[FIXED] Cold absolute `Account` patches no longer materialize unknown
   accounts.** `StateUpdate::Account` is cold-aware: a partial patch against an
   address absent from both layers is skipped, does not write a default backend
   account, and is surfaced through `StateDiff.skipped_accounts`. Intentional
   cold materialization is now explicit via `StateUpdate::AccountUpsert` /
   `StateUpdate::account_upsert(...)`.

2. **[FIXED] Block-context drift and default/latest block-pin ambiguity.**
   `EvmCache::new(provider)` now pins to `BlockId::latest()` instead of a
   "no block pin" state; explicit construction uses
   `EvmCache::at_block(provider, block)`. `set_block` takes a concrete
   `BlockId`, sets `block_number` only for numeric pins, and clears it for
   tag/hash pins. Every block change clears stale `basefee`; callers refresh
   `NUMBER`/`BASEFEE` together with `set_block_context` after fetching the new
   header. Freshness validation captures the cache's concrete snapshot pin and
   passes it through to storage fetchers.

3. **[FIXED] Synchronous layer-2 escape hatches have an invalidating wrapper.**
   Raw handles are now visibly named `unchecked_blockchain_db()` /
   `unchecked_backend()`, and `EvmCache::with_blockchain_db_mut(...)` runs a
   synchronous `BlockchainDb` mutation and invalidates the COW snapshot base
   automatically.

4. **[FIXED] Explicit persistence failures are observable.**
   `cache::save_binary_state`, `PrefetchRegistry::save`, and
   `EvmCache::flush` now return `anyhow::Result<()>`. `Drop` remains best-effort
   and logs flush errors.

5. **[FIXED] Access-list profitability uses exact EIP-2930 RLP bytes.**
   Arbitrum profitability now centralizes data-gas accounting in
   `access_list_rlp_data_gas(...)` and provider/pricing failures propagate as
   `Err`, leaving `Ok(None)` for empty/zero-priced/unprofitable lists.

6. **[FIXED] `simulate_call_with_balance_deltas` isolates balance reads and
   returns the touched access list.** Pre/post `balanceOf` reads run in isolated
   checkpoints so malicious/non-view token reads cannot affect target-call gas or
   committed state. The method commits only the target call when `commit=true`
   and returns the deduplicated EIP-2930 access list from the pre-reads, target
   call, and post-reads.

7. **[FIXED] On-disk cache files carry magic bytes and a version number.**
   `binary_state`, `bytecode`, `ImmutableDataCache`, `PrefetchRegistry`, and
   `SlotObservationTracker` now write a crate-specific magic header plus an
   explicit version before the bincode payload. Unknown magic/version values and
   legacy raw-bincode files are treated as cache misses.

8. **[FIXED] `call_raw_with_access_list` did not revert its checkpoint on a
   transact error.** Both `EvmCache::call_raw_with_access_list` and
   `EvmOverlay::call_raw_with_access_list_with` now match on the `transact_one`
   result and `checkpoint_revert` on **every** path (success and host error),
   matching `call_raw` / `simulate_with_transfer_tracking`. A host-level transact
   error no longer leaves the overlay checkpoint un-reverted.

9. **[FIXED] Duplicate custom-error selectors no longer shadow silently.**
   `RevertDecoder::try_register` and `try_register_raw` return a
   `DuplicateSelectorError` when a selector is already registered. The ergonomic
   `register` / `register_raw` / `with_error` path keeps the first registration
   and emits a warning instead of replacing it.

10. **[FIXED] EVM timestamp construction no longer panics on pre-epoch clocks.**
    EVM builders use a shared saturating helper for implicit wall-clock
    timestamps, returning `0` when the system clock is before the Unix epoch
    instead of panicking. Explicit timestamp overrides are unchanged.

## Remaining open issues ranked by unexpected-result risk

No release-blocking unexpected-result issues remain open from this audit. The
remaining items below are accepted limitations or code-quality/API nits.

## Code-quality nits

No current code-quality nits are tracked here after the protocol-specific cache
surface was moved out of this crate.

## API ergonomics

1. **[R] `snapshot()` vs `create_snapshot()`.** `snapshot()` returns a low-level
    `revm::database::Cache` for in-place `restore()`; `create_snapshot()` returns
    an `Arc<EvmSnapshot>` for cross-thread fan-out. The names don't convey the
    difference. Docs now cross-reference them (see the rustdoc), but a rename
    could be considered pre-1.0.

2. **[R] Process-global cache speed mode.** `set_cache_speed_mode` /
    `cache_speed_mode` are a process-wide `static`, so two caches in one process
    cannot tune concurrency independently. Phase 1 moved configuration toward
    per-instance (`EvmCacheBuilder::cache_config`); the global setter remains.

3. **[V] `SpeculativeSim` consumption contract.** Both `validate()` and
    `into_optimistic()` take `self` by value, so double-consumption is unreachable
    under normal ownership. Internally `validate()` uses `.expect("validation
    handle taken twice")` (defensive) while `into_optimistic()` no-ops if the
    handle was already taken; this is now documented with a `# Panics` note on
    `validate`. A `Result`-returning variant could remove the residual foot-gun.

## Limitations by design / roadmap

- **Solidity `Panic(uint256)` codes above `u64::MAX` decode as `Unknown`.**
  `decode_solidity_panic` drops out-of-range codes rather than exposing a lossy
  `u64`. Real compiler-emitted panic codes are single-byte constants, so this is
  an accepted limitation and is documented in the error module.
- **ERC20 `Transfer` decoding assumes the standard event layout.** `inspector.rs`
  reads `from`/`to` from indexed topics and `value` from the first 32 data bytes.
  Non-standard or packed `Transfer` encodings may parse incorrectly or be skipped.
  A self-transfer where `from == to` nets to zero for that owner; this is
  documented at the call site.
- **Copy-on-write snapshots (Phase 5, Pillar A) — done.** `create_snapshot()` is
  no longer an O(total state) deep clone. The cold `BlockchainDb` index (layer 2)
  is flattened once into an internal, immutable, `Arc`-shared base (per-account
  storage shared by `Arc`), memoized across snapshots and rebuilt copy-on-write
  only for the addresses that changed; each snapshot folds just the hot CacheDB
  delta (layer 1) over a cheap `Arc::clone`. **Residual cost model (honest):** a
  snapshot is no longer free. When layer 2 is unchanged since the last snapshot
  it still pays an **O(accounts) length-scan** of the layer-2 storage/account
  maps (to catch uncontrolled lazy-fetch growth that bypasses the write funnel,
  since `foundry-fork-db` cannot be hooked) plus an **O(layer-1) fold** of the hot
  delta — so the cost tracks `accounts + changed state`, not total slots. A
  full rebuild (first snapshot, or after `set_block`/re-pin) is still O(total
  state). `create_snapshot` is now `&mut self` (it memoizes the base, Decision
  D5). The retained `create_snapshot_deep_clone()` (the legacy full flatten) is
  kept as the A/B benchmark baseline and the read-equivalence reference; the
  `create_snapshot` group in `benches/simulation.rs` measures both. Decisions and
  the cost model are in [`phase-5-spec.md`](phase-5-spec.md) / `ROADMAP.md`.
- **Layer-2 unchecked accessors remain an explicit contract boundary (Phase 5).** The
  snapshot base's growth scan is count/absence-based, which is sufficient for the
  supported writers: the crate's own mutators (`apply_update`, `inject_storage_batch`,
  purges, code overrides) explicitly mark the base dirty,
  and the `foundry-fork-db` `SharedBackend` lazy fetch is append-only at a fixed
  block (it only inserts on a cache miss, never overwrites in place — a load-bearing
  invariant noted in `refresh_base`). Direct out-of-band writes through the
  `unchecked_blockchain_db()` / `unchecked_backend()` handles still bypass the
  normal write funnel by design. For synchronous `BlockchainDb` map writes, prefer
  [`EvmCache::with_blockchain_db_mut`], which invalidates the base automatically
  after the closure returns. If using the unchecked handle directly, call
  [`EvmCache::invalidate_snapshot_base`] after the write lands and before the next
  snapshot (or re-pin via `set_block`). For
  `SharedBackend::insert_or_update_storage` / `insert_or_update_address`, the call
  only enqueues work on the backend handler; `invalidate_snapshot_base()` does not
  wait for that queued update. First synchronize or read back until the expected
  value is visible in `BlockchainDb` / through the backend, then invalidate before
  creating the snapshot. The rustdoc on both accessors and the hook carries this
  warning, and `tests/cow_snapshot.rs`
  (`invalidate_snapshot_base_rehonest_after_escape_hatch_write`,
  `invalidate_snapshot_base_rehonest_after_existing_account_write`,
  `with_blockchain_db_mut_rehonest_after_storage_overwrite`,
  `with_blockchain_db_mut_rehonest_after_account_overwrite`)
  pins it.
- **Protocol adapters are intentionally out of scope.** AMM state tracking,
  protocol-specific storage layouts, and DeFi event adapters now belong in
  `evm-amm-state` or downstream crates. This crate provides the generic
  `StateUpdate` writer vocabulary, `EventDecoder`/`DecoderRegistry`, the ERC-20
  decoder, and `EventPipeline` orchestration.
- **Event-driven sync (roadmap Pillar B) — reader/writer halves done; live WS
  transport is not.** The Phase 3 **writer half** (`StateUpdate` +
  `apply_update`/`apply_updates`) and the Phase 4 **reader half** (the `events`
  module: `EventDecoder`/`DecoderRegistry`, the ERC-20 adapter, and the
  `EventPipeline` with `ingest_logs`/`reorg_to`/`reconcile`) are implemented.
  What is **not** shipped is a concrete production WS transport: the async
  `events::drive`/`LogSource` convenience is generic over a log source and is
  exercised only by the offline example feeding an in-memory source; wiring it to
  a live `subscribe_logs`/WS provider (and detecting reorgs from block-hash
  mismatches) is left to the consumer.
- **Recent toolchain.** MSRV 1.88 and edition 2024 are intentional and
  CI-enforced; consumers on older toolchains are not supported.
