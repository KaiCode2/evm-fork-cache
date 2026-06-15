# Known issues & limitations

A living triage list of bugs, smells, and limitations surfaced during the
publication-readiness review. Items here are **flagged, not fixed** — the test
suite deliberately pins *current* behavior, so changing any of these is a
conscious, reviewable decision (and a `CHANGELOG.md` entry).

Confidence legend: **[V]** verified against the source during review;
**[R]** reported by the review and worth confirming before acting.

## Correctness / behavior to review

1. **[V] Silent persistence failures.** `cache::save_binary_state`,
   `PrefetchRegistry::save`, and `ImmutableDataCache::save` log a warning on I/O
   error but return `()`, so callers cannot detect a failed write (full disk,
   permissions, partial flush). Consider returning `Result<()>` (a breaking
   change worth taking pre-1.0). Tested today only insofar as the happy-path
   round-trip succeeds.

2. **[V] Access-list L2 profitability uses an approximate gas model.** In
   `access_list.rs`, `into_access_list_if_profitable` / `access_list_if_profitable`
   estimate L1 calldata cost with hand-rolled RLP-overhead constants
   (`4 * 16` per address, `16` per key, `3 * 16` for the list header). This is an
   intentional heuristic, not a precise EIP-2930 serialization cost — verify it
   against real serialized sizes before relying on the profitability verdict for
   anything other than a rough gate. The two functions also duplicate this logic
   (a maintenance hazard: a fix to one must be mirrored).

3. **[V] Profitability swallows provider errors.** The same functions catch all
   provider errors and return `Ok(None)`, which is indistinguishable from
   "computed: not profitable." A caller cannot tell a skipped check (RPC down)
   from a real negative. Consider a result type that distinguishes the two.

4. **[R] `set_block` with a tag leaves `block.number` stale.** Only
   `BlockId::Number(n)` syncs the `NUMBER` opcode value; pinning to a tag (e.g.
   `BlockId::latest()`) leaves the previously-set number in the block env. Either
   resolve tags to a concrete number at pin time or document the constraint
   loudly.

5. **[R] Duplicate custom-error selectors shadow silently.** `RevertDecoder`
   registration replaces an existing entry for the same 4-byte selector with no
   warning, so an accidental double-registration silently wins. Consider a
   debug-level log or a `try_register` that reports collisions.

6. **[R] ERC20 `Transfer` decoding assumes the standard layout.** `inspector.rs`
   reads `from`/`to` from indexed topics and `value` from the first 32 data
   bytes. Non-standard or packed `Transfer` encodings parse incorrectly. Also, an
   address that appears as both `from` and `to` in one transfer is both
   subtracted and added (a semantically-invalid self-transfer is not rejected).

7. **[R] Panic codes above `u64::MAX` are dropped.** `decode_solidity_panic`
   converts out-of-range panic codes to `None`. Real compiler-emitted panic codes
   are single-byte constants, so this is benign in practice; now documented at the
   call site.

8. **[V] `simulate_call_with_balance_deltas` returns an empty access list.** It
   sets `CallSimulationResult.access_list = AccessList::default()`, unlike
   `simulate_with_transfer_tracking` which populates it via `extract_access_list`.
   Either the field is meaningless on this path or the population was missed —
   the docs now state the field is empty here; reconcile before relying on it.

9. **[V] `call_raw_with_access_list` does not revert its checkpoint on a transact
   error.** It propagates the EVM `transact` error with `?` *before* reverting the
   journaled checkpoint, whereas `call_raw` / `simulate_with_transfer_tracking`
   revert on every path. A host-level transact error therefore leaves the overlay
   checkpoint un-reverted. (Reverts normally on success and on revert/halt.)

10. **[V] `SystemTime::now().unwrap()` panic risk in EVM construction.**
    `build_evm` / `make_local_context` (and the overlay equivalents) call
    `SystemTime::now().duration_since(UNIX_EPOCH).unwrap()` when no timestamp
    override is set, which panics if the system clock is before the Unix epoch.
    Setting an explicit timestamp avoids it; consider a saturating fallback.

## Code-quality nits

11. **[V] Dead branch in `i128_to_u256`** (`cache/storage_keys.rs`): both the
    `value >= 0` and `else` arms evaluate the identical `U256::from(value as u128)`.
    The two's-complement cast is correct for both signs, so the `if`/`else` can
    collapse to one line (keep the explanatory comment).

12. **[R] V3 tick-snapshot keys serialize as strings.** `V3PoolTickSnapshot`
    stringifies `i16`/`i32` tick/word keys for bincode, then `parse()`s them back
    in `to_tick_bitmap`/`to_ticks`, silently dropping any key that fails to parse.
    A native integer-keyed encoding would be faster and would not fail silently.

13. **[V] On-disk caches have no version header.** `binary_state`, `bytecode`,
    `metadata` (`ImmutableDataCache`), and `tick_snapshot` all persist raw bincode
    with no magic bytes or version field, so a struct-layout change silently
    invalidates every existing cache file (decoded as a miss). A version header
    would enable detection/migration.

14. **[R] Balancer pool id keyed by `Debug` formatting.** `ImmutableDataCache`
    keys `balancer_pools` by `format!("{:?}", pool_id)`. `Debug` output is not a
    stable encoding contract; a hex encoding would be safer for a persisted key.

## API ergonomics

15. **[R] `snapshot()` vs `create_snapshot()`.** `snapshot()` returns a low-level
    `revm::database::Cache` for in-place `restore()`; `create_snapshot()` returns
    an `Arc<EvmSnapshot>` for cross-thread fan-out. The names don't convey the
    difference. Docs now cross-reference them (see the rustdoc), but a rename
    could be considered pre-1.0.

16. **[R] Process-global cache speed mode.** `set_cache_speed_mode` /
    `cache_speed_mode` are a process-wide `static`, so two caches in one process
    cannot tune concurrency independently. Phase 1 moved configuration toward
    per-instance (`EvmCacheBuilder::cache_config`); the global setter remains.

17. **[V] `SpeculativeSim` consumption contract.** Both `validate()` and
    `into_optimistic()` take `self` by value, so double-consumption is unreachable
    under normal ownership. Internally `validate()` uses `.expect("validation
    handle taken twice")` (defensive) while `into_optimistic()` no-ops if the
    handle was already taken; this is now documented with a `# Panics` note on
    `validate`. A `Result`-returning variant could remove the residual foot-gun.

## Limitations by design / roadmap

- **No copy-on-write snapshots yet.** `create_snapshot()` deep-clones state
  (`O(accounts + slots)`); the COW rewrite is roadmap Pillar A. The `simulation`
  benchmarks exist to measure the baseline this will improve on.
- **`protocols` not yet extracted.** The DeFi surface is feature-gated but still
  in-crate; `cargo test --no-default-features` is not yet supported because some
  unit tests assume the default feature. Extraction into `evm-amm-state` is
  planned (roadmap), blocked partly by `ImmutableDataCache` coupling generic
  token-decimals with V2/V3/Balancer pool metadata.
- **Event-driven sync (roadmap Pillar B) is not implemented.** Targeted
  inject/purge exist; decoding logs into state updates and the WS ingestion loop
  with reorg handling are future phases.
- **Recent toolchain.** MSRV 1.88 and edition 2024 are intentional and
  CI-enforced; consumers on older toolchains are not supported.
