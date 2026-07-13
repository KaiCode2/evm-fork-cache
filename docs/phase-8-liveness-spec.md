# Phase 8 — storageHash liveness & state invalidation (spec)

> Status: **planned / design distillation.** This document captures a design
> thread on making liveness a first-class engine responsibility rather than a
> consumer chore. It is written against the post-Phase-7 tree (reactive runtime,
> cold-start, bundle sim already landed) and supersedes the informal "P0–P4"
> numbering used while sketching. Read it **with** [`ROADMAP.md`](ROADMAP.md)
> (Pillar B/C) and [`KNOWN_ISSUES.md`](KNOWN_ISSUES.md) (the liveness/staleness
> limitations this closes).

## Motivation

The engine is excellent at *simulation* but offloads most *liveness* complexity
to the consumer. Freshness (`src/freshness.rs`) can only re-check slots a sim
actually read or the caller explicitly sampled; the event pipeline only keeps
state fresh for protocols someone wrote a decoder for; and nothing detects state
that changed via a path no decoder covers. The result: **state can go silently
stale outside the active read/decoder footprint, with no signal.**

An account's storage-trie root (`storageHash`, from `eth_getProof`) is a
collision-resistant commitment over *all* of that account's storage. So
`root_unchanged ⟹ provably nothing under the account changed` — a **sound,
per-account change oracle with zero false negatives**, obtainable from any
standard RPC in one call, with no local trie and no proof verification. That is
the centerpiece of this phase: use it to detect and repair the staleness the
current pull-based, footprint-bounded model cannot see.

## 0. Ground rules (non-negotiable)

- Branch: `phase-8-liveness`. Green bar at every commit:
  `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test`, `RUSTDOCFLAGS=-D warnings cargo doc --no-deps`, `cargo bench --no-run`.
- MSRV 1.88, edition 2024. **No new production dependencies** — `eth_getProof`
  is already reachable via the existing `alloy-provider` dep
  (`Provider::get_proof(address, keys) -> RpcWithBlock<_, EIP1186AccountProofResponse>`,
  `keys: Vec<StorageKey>` accepts `vec![]`, block-pinnable via `.number(n)`).
- Feature-gate to match the tree: the runtime/cold-start live behind the
  default-on `reactive` feature; `src/freshness.rs` is the generic (ungated)
  core. New liveness wiring that touches the runtime is `reactive`-gated; the
  probe seam on `EvmCache` and any `Validity` coupling stay in the generic core.
- Follow the repository's normal commit-signing and attribution policy; never
  force-push `main`.

## 1. Objective & scope

**In scope**

1. A provider-neutral **account/root fetcher seam** on `EvmCache` (mirrors the
   existing `StorageBatchFetchFn`). This single seam unblocks *three* things: the
   storageHash probe, the account-field (`Scalars`) resync, and the tracked-but-
   `Unsupported` `ResyncTarget::Account` path (`reactive/mod.rs:2000`).
2. **`StorageHashProbe`** + a per-block **root gate**: probe tracked accounts'
   `storageHash` each block, and on a move that no decoder explained, emit a
   `ResyncRequest` through the runtime's *existing* resync channel + raise a
   coverage alarm.
3. A per-contract **`TrackingPolicy`** (`Slots` / `WholeAccount` / `Scalars`)
   that decides *how* an account is kept live.
4. **`Validity` stamping** of event/reactive-derived writes (couple the
   reactive apply path to `FreshnessRegistry`), closing the "event writes carry
   no freshness stamp" gap.
5. Engine-driven **block-env refresh** (`advance_block`) from the canonical
   header stream, so a long-running reactive cache stops simulating against a
   stale block env.
6. A **cold-start root baseline** (`roots.bin`) so a process restarting after
   downtime can cheaply detect which tracked accounts changed while it was down.

**Out of scope (documented as follow-ups, not built here)**

- A local storage MPT / trie maintenance. **Decision locked** (§Decisions): the
  root is *observed*, never *reconstructed*.
- Cryptographic proof verification of storage/account proofs against a
  `stateRoot`. **Decision locked**: circular trust against the same RPC; no
  independent root source exists. `alloy-trie` stays out.
- A trace-only runtime that depends on `debug`/`trace` RPC availability. The core
  path can use block state diffs when available, but must degrade to existing
  storage/account point reads without changing handler semantics.
- A `FullMirror` completeness-verified policy (would require full-storage
  enumeration via `debug_storageRangeAt` + local root reconstruction). Explicitly
  deferred; §Decisions explains why.

## 2. What already shipped — build on, do not rebuild

The earlier sketch predates Phases 6–7. Re-grounded against the current tree:

| Sketch item | Status in tree today |
| --- | --- |
| Engine-driven per-block pipeline ("drive") | **Shipped** as `ReactiveRuntime::ingest_batch{,_with_resync}` (`reactive/mod.rs:1341/1362`). |
| Engine-detected parent-hash reorg + recovery | **Shipped, stronger than sketched.** Journaled parent-hash detection + LIFO rollback + conservative purge (`recover_for_canonical_input` 1518, `recover_dropped_journals` 1692), bounded by `ReactiveConfig::journal_depth` (default 64). |
| Slot resync mechanism | **Shipped** — `execute_resync_requests` (1973) batches `ResyncTarget::StorageSlot{,s}` through `cache.storage_batch_fetcher()`. `ResyncTarget::Account` returns `UnsupportedAccountTarget` (2000) — the seam this phase adds. |
| Resync/invalidation *vocabulary* | **Shipped** — `ReactiveEffect::{Resync,Invalidate}`, `ResyncRequest`/`ResyncTarget`/`ResyncReason`/`ResyncPriority` (662–770). This phase *emits into* it, adding a `RootMoved` reason. |
| Declarative cold-start warming | **Shipped** — `EvmCache::run_cold_start` + `ColdStartPlanner` (`cold_start/`). Pure warming; no root baseline, no restart diff. |
| Validity classifier | **Shipped but unwired** — `FreshnessRegistry`/`Validity{Pinned,Volatile,ValidThrough}` (`freshness.rs:163,184`). Nothing auto-stamps it; the runtime never touches it. |
| storageHash / `get_proof` / account root | **Entirely greenfield.** Zero occurrences in `src/`. |

The consequence: this phase is *smaller* than the original sketch. It adds one
RPC seam, one probe, one policy type, and wires two already-built subsystems
(reactive runtime ↔ freshness) together. It does **not** build a new controller,
a reorg engine, or a drive loop — those exist.

## 3. The liveness signal hierarchy

Three tiers, increasing completeness / decreasing availability. They compose;
they do not compete.

1. **Event decoders (Tier 1 — shipped).** Free (logs already ingested),
   value-carrying, but only covers protocols with a decoder. The baseline via
   reactive handlers / `EventPipeline`.
2. **`storageHash` probe (Tier 2 — this phase).** `get_proof(addr, []).number(N)`
   → the account's storage root. A sound per-account change oracle: *no values,
   no slot detail*, but zero false negatives, universal on any RPC at `latest`.
   Catches everything Tier 1 misses.
3. **State-diff trace (Tier 3 — trace-first when available, §7).** `debug`/`trace`
   per-block diff: decoder-free, exact changed slots + values, one call per
   block. Subsumes point storage reads for already-tracked accounts *where the
   namespace is enabled*; unresolved cold targets still fall back to the existing
   storage/account seams.

The soundness of Tier 2 is the whole point: today a change on a path no decoder
covers (proxy `sstore`, admin writes, a token without a decoder,
`SELFDESTRUCT`/`CREATE2` redeploy) is invisible until the optimistic validator or
a sampled `reconcile` happens to re-read the slot. A per-block root gate turns
that into an explicit, cheap, complete signal.

## 4. `TrackingPolicy` — the WETH vs. Uniswap-V2 nuance

The root gate behaves *oppositely* for two contract shapes, so liveness strategy
must be per-contract:

```rust
pub enum TrackingPolicy {
    /// Sparse interest (e.g. WETH: a few balance slots). The root churns on
    /// nearly every block, so it is a noisy gate — do NOT root-gate. Keep the
    /// enumerated slots fresh via decoders + cadence reconcile.
    Slots { slots: Vec<U256> },
    /// Whole economic state (e.g. a V2 pool). root_moved ≈ my_state_changed,
    /// so the root is a tight, cheap gate: probe each block; on a move re-read
    /// the tracked slot set.
    WholeAccount,
    /// balance / nonce / code_hash only — resolved from the same get_proof
    /// response's account fields; no storage interest.
    Scalars,
}
```

Per-block engine behavior:

| State after decode | `WholeAccount` | `Slots` | `Scalars` |
| --- | --- | --- | --- |
| root `!moved` | stamp tracked set `ValidThrough(N)` — **0 reads** | (not root-gated) cadence reconcile | stamp `ValidThrough(N)` |
| root `moved`, addr ∈ touched | **skip** — decoder already applied authoritative values | decoder already applied | diff account fields |
| root `moved`, addr ∉ touched | **resync** tracked slots + **coverage alarm** | n/a | resync scalars + alarm |

A false-positive resync is never *incorrect* — it costs one batched read — so the
policy is a **pure cost knob**, not a correctness lever. `Slots` opts out of the
noisy gate; `WholeAccount` opts in because for it the gate is tight.

## 5. Core types & behavior (proposed)

### 5.1 Account/root fetcher seam (`src/cache/mod.rs`)

Mirror `StorageBatchFetchFn`. One new callback the builder can install; drives
`eth_getProof` through the same `block_in_place_handle` bridge used by
`ensure_account_blocking` / the storage batch fetcher.

```rust
/// Fetches account headers + (optionally) storage proofs, one entry per address.
/// keys == &[] ⇒ root-only probe (no storage-proof payload).
pub type AccountProofFetchFn = Arc<
    dyn Fn(Vec<(Address, Vec<U256>)>, BlockId)
        -> Vec<(Address, Result<AccountProof>)> + Send + Sync,
>;

pub struct AccountProof {
    pub storage_hash: B256,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    pub slots: Vec<(U256, U256)>, // populated only for requested keys
}
```

This seam is the linchpin: it also directly resolves
`ResyncTarget::Account` (`UnsupportedAccountTarget`, `reactive/mod.rs:2000`) and
the "freshness reconciles storage slots only" account-field gap tracked in
`KNOWN_ISSUES.md`.

### 5.2 `StorageHashProbe` + per-block root gate (`src/liveness/`, `reactive`-gated)

- A small `TrackingRegistry` mapping `Address -> TrackingPolicy` (see §6 on
  composition with `FreshnessRegistry`), plus per-account baseline
  `HashMap<Address, TrackedRoot { last_root, last_block, balance, nonce, code_hash }>`.
- Hook the per-block boundary in `ReactiveRuntime::ingest_batch_direct`
  (`reactive/mod.rs:1418`, right after the canonical block input is journaled) —
  or the `SubscriberEvent::BlockHeader` path (`mod.rs:3105`). At that point the
  runtime holds `&mut EvmCache`, the canonical `BlockRef`, and the batch's
  touched-address set.
- For each `WholeAccount`/`Scalars` target: probe the root; compare to baseline;
  apply the §4 table. A move with `addr ∉ touched_addrs` synthesizes a
  `ResyncRequest{ targets: tracked slots, block: ResyncBlock::Hash{..},
  reason: ResyncReason::RootMoved }` fed through the existing
  `ingest_batch_with_resync` → `execute_resync_requests` path.

### 5.3 Coverage alarm

`ResyncReason::RootMoved` (new variant) + a new `ReactiveReport` variant (e.g.
`CoverageGap { address, block }`) dispatched via the existing `dispatch_reports`
(`mod.rs:1510`) so `ReactiveHook::on_report` observers can see "a tracked
account's root moved with no covering event." This is the high-value new signal
— the decoder-coverage blind spot made observable.

### 5.4 `Validity` stamping of event/reactive writes

Hand the runtime a `&mut FreshnessRegistry` (or a thin trait it can call). After
`cache.apply_updates(&execution.state_updates)` (`mod.rs:1442`), stamp each
touched `(address, slot)` from the returned `StateDiff` as `ValidThrough(N)` via
`FreshnessRegistry::valid_through_slot` (`freshness.rs:246`). On a new canonical
block, `FreshnessController::on_new_block(N)` (`freshness.rs:837`) already ages
`ValidThrough(m)` into `Volatile` once `N > m`. This makes event-maintained slots
stop being needlessly re-verified while staying honest.

### 5.5 Cold-start root baseline (`roots.bin`)

- Extend `ColdStartPlan` with `probe_roots: Vec<Address>` (mirrors the existing
  slot `probe` phase) and `ColdStartResults` with observed roots. Persist a
  versioned `roots.bin` next to `evm_state.bin` (same magic+`u32`+bincode
  envelope as `binary_state.rs`; unknown magic ⇒ cache miss).
- On restart, feed loaded baselines into `ColdStartPlanner::initial_plan`: probe
  each tracked account's root now; where it equals the persisted baseline, the
  cached tracked slots are still valid — **skip re-reading** (this is the "if no
  divergence, we're already synced" case). Where it diverges (or no baseline),
  re-read the tracked slots and adopt the new root. The multi-round
  `Continue/Done` protocol already supports "root changed → re-read next round."

## 6. Cold-start gate — what it proves, and the trap to avoid

The tempting version — *reconstruct* the storage root from our local slots and
compare to chain — is a **trap** and is explicitly out of scope:

- `storageHash` commits over the account's **entire** storage. Reproducing it
  requires holding **every** slot; for the contracts worth tracking (a V2 pool
  carries its full LP-token `balanceOf` map; WETH carries every balance) the full
  storage is large/unbounded, and there is **no portable enumeration** (alloy
  exposes no `debug_storageRangeAt`; it is archive-gated where it exists).
- Reconstruction also needs the trie machinery (`alloy-trie` `HashBuilder`) — the
  exact infra Decision D2 rules out.

So we compare the on-chain root **across time**, never local-vs-chain: persist
the *observed* on-chain root as a baseline and diff `root_now` vs baseline. This
is a **currency** gate, not a **completeness** gate:

> `root_unchanged ⟹ nothing under the account changed ⟹ the tracked subset is
> unchanged ⟹ if it was correct at the baseline block, it is correct now`
> (and it was, because the baseline values came from RPC at that block).

It cannot tell you that you were *missing* a slot you should have tracked — a
missing slot is not a "change." Completeness is a separate property only full
enumeration/reconstruction gives, which is why `FullMirror` is deferred.

## 7. Tier 3: state-diff trace acceleration

Where an archive/trace node is available, a single per-block
`debug_traceBlockByNumber` (prestate `diffMode`) or
`trace_replayBlockTransactions().state_diff()` returns every changed
`(account, slot, value)` — a decoder-free, value-carrying superset of Tier 1/2
for tracked accounts. Model it as a block-scoped state-diff source that runs
before per-slot/per-account resync fetches and **degrades honestly** when the
namespace is unavailable (`-32601`) or historical trie data is missing. One trace
call/block (all accounts) replaces N point reads when the requested targets are
present in the diff; unmatched cold targets still go through the portable fetch
seams so the core never depends on trace availability.

Live RPC benchmarking and gzip transport measurements are documented in
[`trace-resync-benchmarks.md`](trace-resync-benchmarks.md). The important design
constraint from those measurements is that trace is an excellent CU/completeness
accelerator, but small known-slot repairs can still be faster through batched
point reads; production policy should stay adaptive.

## 8. Tests (offline, no network) — the acceptance contract

Mock the `AccountProofFetchFn` (and `StorageBatchFetchFn`) with in-memory tables;
no live RPC. Grouped by file:

- `tests/liveness_root_gate.rs`: unchanged root ⇒ 0 slot reads + tracked set
  stamped `ValidThrough(N)`; `WholeAccount` root moved + addr ∉ touched ⇒ resync
  emitted + `CoverageGap` report; root moved + addr ∈ touched ⇒ skip; `Slots`
  policy is never root-gated; native balance/nonce move detected via account
  fields.
- `tests/liveness_validity_stamp.rs`: a reactive/event write stamps
  `ValidThrough(N)`; `on_new_block(N+1)` ages it to `Volatile`.
- `tests/liveness_cold_start.rs`: baseline round-trip through `roots.bin`
  (versioned envelope; legacy/unknown magic ⇒ miss); equal baseline ⇒ no
  re-read; diverged baseline ⇒ re-read + adopt.
- `tests/liveness_account_resync.rs`: `ResyncTarget::Account` now succeeds via
  the new seam (previously `UnsupportedAccountTarget`).

## 9. Decisions (LOCKED by the design thread)

1. **No local storage trie.** The root is observed via `eth_getProof`, never
   reconstructed locally.
2. **No cryptographic proof verification.** Verifying a storage proof against a
   `stateRoot` fetched from the same RPC is circular; the crate has no
   independent root source. `alloy-trie` stays out. (Revisit only if the engine
   ever ingests state from an untrusted peer or must prove correctness to a third
   party.)
3. **`storageHash` gates `WholeAccount` only**, never `Slots` (root churns every
   block for sparse-interest contracts → noisy, wasteful).
4. **Compose, don't merge.** A `TrackingRegistry` wraps / sits beside
   `FreshnessRegistry`; the entire freshness API stays verbatim. (This introduces
   the *first* coupling between the reactive runtime and `freshness.rs` — an
   intentional, minimal one, for §5.4.)
5. **Reuse the existing resync channel.** The root gate emits `ResyncRequest`s;
   it does not build a parallel repair loop.
6. **Cold-start gate is currency, not completeness** (§6). `FullMirror` deferred.
7. **Tier 3 (trace) is a trace-first accelerator when configured/available**; the
   existing point-fetch seams remain the portable floor.

## 10. Open decisions (confirm before building)

- **Default `TrackingPolicy`** for a touched-but-unregistered address —
  recommend *untracked / best-effort* (lazy semantics) so liveness cost is
  strictly opt-in.
- **Coverage-alarm action** — surface-only in the report (recommended; caller
  sets policy) vs. auto-`mark_volatile` the emitter so it self-heals next cycle.
- **`Scalars` gating** — cheap empty-key probe every block (recommended; account
  fields are free in the response) vs. treat as `Pinned`.
- **Where `advance_block` is driven** — inside the runtime's canonical-block
  path (automatic, opinionated) vs. a `FreshnessController::on_new_block`
  extension the caller wires (explicit).

## 11. Build order (commit per step, green each time)

1. `AccountProofFetchFn` seam on `EvmCache` + builder installer (unblocks
   `ResyncTarget::Account`; land with a test flipping the `Unsupported` path).
2. `advance_block(header)` block-env refresh + tracked-set dirty/reverify; drive
   from the reactive canonical-block path.
3. `Validity` stamping of reactive/event writes (§5.4) + aging test.
4. `TrackingPolicy` / `TrackingRegistry` + `StorageHashProbe` + per-block root
   gate + `ResyncReason::RootMoved` + `CoverageGap` report (the centerpiece).
5. Cold-start root baseline (`roots.bin`) + restart diff in the planner.
6. Tier-3 block-state-diff fetcher that resolves matching resync targets before
   falling back to storage/account point reads.

## 12. ROADMAP integration

Add to `ROADMAP.md`:

- **Phase-table row** (once landed): `| **8** | storageHash liveness &
  invalidation: account/root fetcher seam; per-block root gate + complement
  resync (`RootMoved`); `TrackingPolicy`; event-write `Validity` stamping;
  `advance_block` env refresh; cold-start root baseline | **Done** (`phase-8-liveness`) |`.
- A `## Phase 8 — storageHash liveness (detailed, decisions locked)` section
  after the Phase 7 detailed section, and a "Remaining work toward 1.0" bullet
  until it lands.
- On landing, convert the `KNOWN_ISSUES.md` items this closes: account-field
  resync `Unsupported` (`reactive/mod.rs:2000`), "freshness reconciles storage
  slots only," and the account-field-resync transport-depth item.
