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
   `EvmCache::flush` now return typed errors (`PersistenceError` /
   `CacheError`). `Drop` remains best-effort and logs flush errors.

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

1. **[R][Closed in 0.2.0] Snapshot API names.** The low-level
    `revm::database::Cache` restore point is now `checkpoint()`; the
    cross-thread `Arc<EvmSnapshot>` fan-out view is now `snapshot()`. The old
    `snapshot()` / `create_snapshot()` public names were removed rather than
    kept as aliases.

2. **[R][Closed in 0.2.0] Per-instance storage batch tuning.**
    `StorageBatchConfig` exposes exact `slots_per_batch` and
    `max_concurrent_batches` knobs for one cache's provider-backed storage
    fetcher. `CacheSpeedMode` remains as preset shorthand via
    `From<CacheSpeedMode>` / `EvmCacheBuilder::speed_mode`. The old
    process-global setter/getter/static are removed.

3. **[V][Closed in 0.2.0] `SpeculativeSim` consumption contract.**
    `SpeculativeSim::validate()` now returns `Result<Validation>`: validator
    uncertainty remains `Validation::Unverified`, while a failed background task
    surfaces as an error instead of panicking on a defensive `.expect(...)`.

## Limitations by design / roadmap

- **Storage-only freshness verification; `ConfirmedFull` is defined but not yet
  emitted.** The optimistic verify-and-rerun loop builds its verify set from the
  volatile storage *slots* in each sim's read set, and its success verdict says
  exactly that: `Validation::ConfirmedStorage` guarantees only that no volatile
  storage slot the sims read had changed — a sim whose result depends on a
  `BALANCE`/`SELFBALANCE` (or nonce/code) that moved on-chain without a
  co-changing storage slot can still be `ConfirmedStorage`. The
  `Validation::ConfirmedFull` verdict (storage **and** verified account fields)
  and the `changed_accounts` field of `Validation::Corrected` are **defined but
  not yet emitted** by the validator; wiring validator-side account verification
  that populates them is a tracked follow-up. Account-level freshness *does*
  exist today, but in a **separate subsystem** — the reactive runtime's root
  gate, not the speculative validator. Opting an account into
  `TrackingPolicy::WholeAccount`/`Scalars` via `ReactiveRuntime::track_account`
  root-probes it with `eth_getProof` and repairs balance/nonce/code drift through
  resync, keeping the **cache** fresh. That reactive tracking does **not** feed
  the speculative freshness verdict, so it does not turn a `ConfirmedStorage`
  result into a `ConfirmedFull` one. Deriving a validator verify set from each
  sim's `BALANCE`/`CODE` read set (rather than per-tracked-account opt-in) is the
  further step that same follow-up covers.
- **`Verified` code seeds are never re-verified (EIP-6780 assumption).** A
  canonical code seed confirmed once against on-chain `EXTCODEHASH` is marked
  `CodeSeedState::Verified` durably, including across restarts. Post-Cancun
  (EIP-6780) deployed code is immutable — `SELFDESTRUCT` only works in the
  creating transaction — so one confirmation is sound on mainnet and current
  major L2s. On a chain without 6780, a `SELFDESTRUCT`-then-redeploy can
  change code under a `Verified` mark; the escape hatch is
  `EvmCache::purge_account` (clears the mark; the next touch refetches
  authoritative chain state) before re-seeding.
- **Solidity `Panic(uint256)` codes above `u64::MAX` decode as `Unknown`.**
  `decode_solidity_panic` drops out-of-range codes rather than exposing a lossy
  `u64`. Real compiler-emitted panic codes are single-byte constants, so this is
  an accepted limitation and is documented in the error module.
- **ERC20 `Transfer` decoding assumes the standard event layout.** `inspector.rs`
  reads `from`/`to` from indexed topics and `value` from the first 32 data bytes.
  Non-standard or packed `Transfer` encodings may parse incorrectly or be skipped.
  A self-transfer where `from == to` nets to zero for that owner; this is
  documented at the call site. Transfer **values ≥ 2^255** are reinterpreted as
  negative when accumulated as a signed `I256` delta (`I256::from_raw`), so a
  token emitting such a value would corrupt the reconstructed delta. Real ERC-20
  supplies are far below 2^255, so this is unreachable for honest tokens; a
  malicious token can misreport balances by other means regardless.
- **[Hardened in 0.2.0] `BLOCKHASH` resolves to ZERO in ext-db-less overlays —
  and the freshness validator now fails closed on it.** Snapshots do not track
  block hashes (the live cache does not track them either), so an `EvmOverlay`
  built without an `ext_db` returns `B256::ZERO` for in-lookback-range
  `BLOCKHASH` reads. Since 0.2.0 the freshness pipeline records such reads
  (`EvmOverlay::blockhash_zero_fallback`) and reports the batch
  `Validation::Unverified` — on the optimistic pass **and** on corrected
  re-runs — instead of silently confirming a result whose control flow may
  depend on the real hash. Out-of-range reads return the spec-mandated ZERO
  without a database call and are deliberately not flagged (they are correct
  on-chain too). Direct, non-validator simulations over ext-db-less overlays
  still observe ZERO; supply an `ext_db` or snapshot-provided hashes when
  `BLOCKHASH` accuracy matters to such a sim.
- **[Closed in 0.2.0] `EventPipeline::derived_slots` is bounded.** The
  event-derived `(address, slot)` set is now a block-horizon ring bounded to
  `ReorgConfig::depth` (mirroring the `touched` ring), so steady-state
  ingestion no longer grows it monotonically. Slots aged past the horizon
  cannot be reorg-invalidated anyway, so eviction loses nothing actionable;
  sampled reconciliation now draws from the resident window.
- **Reorgs deeper than `ReorgConfig::depth` cannot fully purge.** The touched-
  address ring is bounded to `depth` blocks; `reorg_to(n)` only purges addresses
  still in the ring, so state touched solely in blocks that have already aged out
  of the horizon is silently left un-purged (no error). Size `depth` above the
  deepest reorg you expect to handle.
- **Reactive runtime reorgs deeper than `ReactiveConfig::journal_depth` recover
  only the resident span.** The runtime journals each canonical block's effects in
  a ring capped at `journal_depth` (default 64). A reorg deeper than that — or any
  reorg when `journal_depth = 0` — rolls back / purges only the blocks still in the
  journal; effects from aged-out blocks are **neither rolled back nor purged**, and
  the freshness/validation loop is the backstop for that span. This is not silent:
  the runtime emits a `tracing::warn!` when a reorg references a block no longer in
  the journal. Set `journal_depth` above the deepest reorg you intend to recover
  precisely. (The full conservative-purge fallback for aged-out blocks is a tracked
  follow-up, not a known defect.)
- **Bundle `coinbase_payment` excludes the gas of `AllowReverts` transactions that
  actually revert.** `simulate_bundle` rolls a reverting whitelisted tx back to its
  inner checkpoint, which also undoes the gas that tx charged to the beneficiary. So
  `coinbase_payment` reflects only the kept (successful) txs' priority fees + direct
  tips — the honest miner *receipt* for those txs. On real mainnet a reverted bundle
  tx still consumes gas and pays the miner, so for precise searcher *cost* accounting
  (miner payment minus the gas you spend on failed attempts) you must add the
  reverted txs' gas yourself (it is in `per_tx[i].gas_used`). Only relevant under
  `AllowReverts` with a tx that actually reverts; the `Atomic` path is unaffected.
  A future revision may surface reverted-tx gas cost directly.
- **Copy-on-write snapshots (Phase 5, Pillar A) — done.** `snapshot()` is
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
  state). `snapshot` is `&mut self` (it memoizes the base, Decision D5). The
  retained `snapshot_deep_clone()` (the legacy full flatten) is
  kept as the A/B benchmark baseline and the read-equivalence reference; the
  `snapshot` group in `benches/simulation.rs` measures both. Decisions and
  the cost model are summarized in `ROADMAP.md` and `docs/INTERNALS.md`.
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
- **Event-driven sync (roadmap Pillar B) — shipped, with bounded live transport.**
  The Phase 3 **writer half** (`StateUpdate` + `apply_update`/`apply_updates`), the
  Phase 4 **reader half** (the `events` module), the **reactive runtime**
  (`reactive`: handler routing, canonical dedup/ordering, resync-through-fetcher,
  depth-bounded journaled reorg recovery), and a **live `AlloySubscriber`**
  (WebSocket `subscribe_logs`/`subscribe_blocks`/`subscribe_pending_transactions`
  with exponential-backoff reconnect and `get_logs` backfill) all ship. Honest
  remaining transport limits: **full block bodies**, **full pending-transaction
  hydration**, and non-log historical backfill are not implemented (the
  subscriber returns a typed `SubscriberError::Unsupported` for non-hash pending
  interests). Mid-lifecycle handler registration through `ReactiveEngine`
  backfills a new handler's logs from the runtime's last canonical block
  automatically and catches the newly connected stream up from that anchor after
  it subscribes, so the discovery→subscription window is closed without caller
  bookkeeping; the bounded `dedupe_window` suppresses the overlap. The residual
  limit is a genuinely live-only registration (no anchor and no backfill
  requested): logs between the registration call and the live subscription start
  are not fetched. A reconnect after more than `dedupe_window` matching logs can
  re-emit already-processed logs as `Backfill` records (the runtime's canonical
  dedup catches most). Account-field resync requires an account proof fetcher;
  missing, failed, or omitted fetches surface as typed failures.
- **Reactive integration-test coverage is partial.** The reactive runtime, the
  subscriber, and reorg recovery are well covered individually (reorg rollback is
  pinned by state-equivalence assertions; reconnect/backfill/dedup by inline unit
  tests). Composing `AlloySubscriber` output into `ReactiveRuntime::ingest_batch`
  end-to-end is now covered offline in `tests/reactive_subscriber_ingest.rs` (a
  real subscriber batch, produced via the mockable `get_logs` backfill path,
  drives a real runtime ingest and asserts the cache write). The remaining paths
  without dedicated integration coverage are the block-header ingest path, the
  `ReactiveReport::Decoded` shape, the `EventDecoderHandler` adapter, and custom
  pending-tx matcher/route-key routing; the live WebSocket transport plumbing is
  covered by reconnect/termination unit tests but not by a networked end-to-end
  test. These are tracked follow-ups, not known defects.
- **Recent toolchain.** MSRV 1.88 and edition 2024 are intentional and
  CI-enforced; consumers on older toolchains are not supported.
