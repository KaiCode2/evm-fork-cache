# Phase 5 — copy-on-write snapshots (Pillar A)

> Status: **build contract**. Authored by the overseer before implementation; the
> red acceptance tests in [`../tests/cow_snapshot.rs`](../tests/cow_snapshot.rs)
> and the extended overlay tests pin this contract and gate the deliverable.
> Decisions below are **locked** (resolved with the user) unless marked OPEN.

## 0. Ground rules

1. **No behavior change on reads.** Every read against a snapshot or an overlay
   built from it must return *exactly* what today's deep-clone snapshot returns —
   bit-for-bit, including the audited `StorageCleared` / `NotExisting` /
   two-layer-precedence semantics. This is enforced by a **differential-equivalence
   test** (§8.1): the new `create_snapshot()` must be read-indistinguishable from
   the retained reference `create_snapshot_deep_clone()` after *every* mutation kind.
2. **`Send + Sync` snapshot, `Send` overlay, lock-free reads.** `EvmSnapshot`
   stays `Send + Sync`; `EvmOverlay` stays `Send`. Snapshot/overlay reads must not
   take a lock and must not regress to a non-`O(1)` lookup (no persistent/HAMT map
   on the read path — see Decision D1). The existing `test_snapshot_is_send_sync`
   and `test_overlay_is_send` must keep passing unchanged.
3. **No new external dependency.** Structural sharing is achieved with `Arc` over
   the per-account storage maps, not a third-party persistent-map crate (D1).
4. **Keep the deep-clone reachable** for A/B benchmarking and as the equivalence
   reference (D3). It is retained as `create_snapshot_deep_clone()`.
5. **Standard bars.** `cargo fmt --check`; `cargo clippy --all-targets -- -D
   warnings` (default) **and** `cargo clippy --lib --no-default-features -- -D
   warnings`; `cargo test` (both feature configs); `RUSTDOCFLAGS=-D warnings cargo
   doc`; `cargo bench --no-run`.

## 1. Goal

`EvmCache::create_snapshot()` is today an **O(total state) deep clone**
([`mod.rs`](../src/cache/mod.rs) `create_snapshot`): it copies every account and,
dominantly, **every storage slot** of both cache layers into fresh `HashMap`s on
every call. Pillar A replaces this with a **copy-on-write** scheme whose cost
tracks *changed* state, not *total* state, and whose clones are `Arc` handle
copies rather than deep copies.

Two pillars (both in scope this phase):

- **A.1 — structural sharing for `create_snapshot`** (§2–§4).
- **A.2 — overlay buffer / instance reuse** (§5).

## 2. Model: memoized immutable base + fresh hot-layer fold

### 2.1 The two layers and the cost asymmetry

- **Layer 2 — `BlockchainDb` (the cold base).** The lazily-fetched / bulk-seeded
  fork index. At a fixed block it is **append-mostly**: a fetched `(addr, slot)`
  value is canonical and is not rewritten; only `set_block`/re-pin replaces it,
  and the controlled bulk writers (`inject_storage_batch*`) and the write-through
  funnel mutate it. This is the *large* state.
- **Layer 1 — `CacheDB` overlay (the hot delta).** revm sim commits, write-through
  applies, direct inserts, freshness corrections. This is the *small, changing*
  set, and it always **shadows** layer 2 on a read (overlay wins).

The deep clone re-copies all of layer 2 every call even though it barely changes
between successive snapshots. COW memoizes layer 2 and folds only layer 1 fresh.

### 2.2 The frozen base

Add an internal, immutable, `Arc`-shared flatten of **layer 2 only**:

```rust
// src/cache/snapshot.rs (or a new src/cache/cow.rs, implementer's choice)
pub(crate) struct BaseState {
    /// Layer-2 account info, by address. (Layer-2 has no NotExisting concept;
    /// that classification is purely a layer-1 property — see §4.)
    pub(crate) accounts: HashMap<Address, AccountInfo>,
    /// Layer-2 storage, per account, **shared by `Arc`** so cloning a base is a
    /// handle copy, never a per-slot copy.
    pub(crate) storage: HashMap<Address, Arc<HashMap<U256, U256>>>,
    /// Bytecode by hash, derived from `accounts` at build time.
    pub(crate) code_by_hash: HashMap<B256, Bytecode>,
}
```

`EvmCache` memoizes the current base and the bookkeeping needed to keep it honest:

```rust
// fields on EvmCache
base: Option<Arc<BaseState>>,         // None until first snapshot / after a reset
base_dirty: HashSet<Address>,         // layer-2 addrs changed since `base` was built
base_full_rebuild: bool,              // set by set_block / re-pin: rebuild from scratch
base_storage_lens: HashMap<Address, usize>, // per-acct layer-2 slot counts at last build
```

These fields are **not** part of any public API and **not** serialized.

### 2.3 `refresh_base(&mut self)` — called at the top of `create_snapshot`

Produces an up-to-date `Arc<BaseState>` reusing the previous one wherever layer 2
is unchanged. It must **never mutate an `Arc<BaseState>` that may be shared** with a
live snapshot — on any change it builds a *new* `BaseState` that shares the `Arc`s
of unchanged accounts and rebuilds only changed ones (copy-on-write).

Algorithm:

1. **Full rebuild** if `base.is_none() || base_full_rebuild`:
   flatten all of layer 2 into a fresh `BaseState` (one `Arc<HashMap>` per account);
   record `base_storage_lens`; clear `base_dirty`; clear `base_full_rebuild`.
2. **Else, detect uncontrolled growth** (lazy RPC fetch / prefetch writes layer 2
   from inside `foundry-fork-db`, which we cannot hook): scan
   `blockchain_db.storage().read()` and `accounts().read()`; for any address whose
   slot count differs from `base_storage_lens`, or any account absent from the base,
   add it to `base_dirty`. This is an `O(accounts)` length comparison — **not** an
   `O(slots)` value scan.
3. **Else, if `base_dirty` is empty** → reuse the existing `Arc<BaseState>`
   unchanged (the common hot-loop case; `create_snapshot` is then `O(1)` for the
   base).
4. **Otherwise (some addresses dirty)** → build a new `BaseState`:
   - clone the outer maps (an `O(accounts)` clone of `Arc` handles + plain
     `AccountInfo`, **no per-slot copy**);
   - for each dirty address, rebuild its `Arc<HashMap<U256,U256>>` from the current
     layer-2 storage and refresh its `AccountInfo` / `code_by_hash`;
   - update `base_storage_lens`; clear `base_dirty`; store as the new `Arc`.

> Correctness rests on `base_dirty` ∪ the growth scan covering every way layer 2
> can change such that the change is **not shadowed by layer 1**. §3 enumerates the
> sites. The equivalence test (§8.1) exercises all of them and fails loudly on any
> miss — a missed invalidation is a red test, never a silent stale read.

### 2.4 `create_snapshot()` — the two-tier snapshot

```rust
pub fn create_snapshot(&mut self) -> Arc<EvmSnapshot> { … }
```

Note the signature change to `&mut self` (it now refreshes/memoizes the base).
Steps:

1. `self.refresh_base()` → `let base = Arc::clone(self.base.as_ref().unwrap());`
   (`O(1)` when layer 2 is unchanged).
2. Fold **layer 1** (`self.db.cache.accounts`) into the snapshot's overlay maps and
   the cleared/not-existing sets, applying the same classification as today
   (§4) — `O(layer-1)`. Per-account overlay storage may be a plain
   `HashMap<U256,U256>` (the hot set is small); `Arc`-interning it is optional.
3. Construct the two-tier `EvmSnapshot { base, …overlay…, …block ctx… }`.

Block context (`block_number`, `basefee`, `coinbase`, `prevrandao`, `gas_limit`,
`chain_id`, `timestamp`, `spec_id`) is copied as today.

### 2.5 New `EvmSnapshot` shape

```rust
pub struct EvmSnapshot {
    pub(crate) base: Arc<BaseState>,
    /// Layer-1 accounts that are present to the EVM (NotExisting excluded).
    pub(crate) overlay_accounts: HashMap<Address, AccountInfo>,
    /// Layer-1 storage delta. A cleared account ALWAYS has an entry here (possibly
    /// empty) so the cleared rule is decided without consulting the base.
    pub(crate) overlay_storage: HashMap<Address, HashMap<U256, U256>>,
    /// Bytecode introduced by layer 1 (checked before `base.code_by_hash`).
    pub(crate) overlay_code_by_hash: HashMap<B256, Bytecode>,
    pub(crate) storage_cleared: HashSet<Address>,
    pub(crate) accounts_not_existing: HashSet<Address>,
    pub(crate) block_hashes: HashMap<u64, B256>,
    // …block context fields unchanged…
}
```

All fields stay `pub(crate)` (no public field break). In-crate `#[cfg(test)]`
constructors of `EvmSnapshot` (in `overlay.rs`) must be updated to the new shape.

## 3. Base-invalidation sites (the correctness checklist)

Every site below must keep the memoized base honest. Implement as a private
helper (e.g. `self.mark_base_dirty(addr)` / `self.invalidate_base()`).

| Site | Layer touched | Action |
| --- | --- | --- |
| `write_slot_through(addr, …)` | layer 2 always; layer 1 if present | `mark_base_dirty(addr)` (over-invalidation when also in layer 1 is **safe** — it just re-folds that one account; D2 keeps it simple over clever) |
| `inject_storage_batch` / `inject_storage_batch_fresh` | layer 2 only | `mark_base_dirty(addr)` for each touched addr |
| account info / storage seeded into layer 2 (construction, `inject_v2/v3_*` paths that hit layer 2) | layer 2 | `mark_base_dirty(addr)` |
| `purge_*` removing layer-2 entries | layer 2 | `mark_base_dirty(addr)` (or `invalidate_base()` if simpler for account-level purge) |
| `set_block` / `repin_to_block` | replaces layer 2 | `base_full_rebuild = true` |
| revm commit (`call_raw(commit=true)`, session commit) | **layer 1 only** | **nothing** — folded fresh; never makes the base stale |
| direct `db_mut()` inserts (`insert_account_info`/`insert_account_storage`) | **layer 1** | **nothing** — folded fresh |
| uncontrolled lazy RPC fetch / prefetch | layer 2 | caught by the `O(accounts)` growth scan in `refresh_base` step 2 |

> The litmus test for "needs invalidation": *can this change a layer-2 value that a
> snapshot read would surface (i.e. that layer 1 does not shadow)?* If yes → dirty.
> Layer-1-only writes are always shadowed → never dirty the base.

## 4. Read semantics (must equal today's flatten, bit-for-bit)

`EvmSnapshot` exposes the lookups the overlay needs; `EvmOverlay` calls these
instead of indexing fields directly.

```rust
impl EvmSnapshot {
    /// Account info as the EVM sees it. None for NotExisting (do NOT consult base).
    pub(crate) fn account_info(&self, a: Address) -> Option<&AccountInfo> {
        if self.accounts_not_existing.contains(&a) { return None; }
        self.overlay_accounts.get(&a).or_else(|| self.base.accounts.get(&a))
    }

    /// Storage value, mirroring cached_storage_value / today's flatten.
    pub fn storage_value(&self, a: Address, s: U256) -> Option<U256> {
        if let Some(m) = self.overlay_storage.get(&a) {
            if let Some(v) = m.get(&s) { return Some(*v); }
            if self.storage_cleared.contains(&a) { return Some(U256::ZERO); } // cleared: base dropped
            // not cleared: fall through to base
        }
        if let Some(v) = self.base.storage.get(&a).and_then(|m| m.get(&s)) {
            return Some(v);
        }
        None
    }

    pub(crate) fn code(&self, h: B256) -> Option<&Bytecode> {
        self.overlay_code_by_hash.get(&h).or_else(|| self.base.code_by_hash.get(&h))
    }
}
```

Invariants the equivalence test pins:
- A **cleared** (`StorageCleared`/`NotExisting`) layer-1 account: snapshot holds
  only its overlay slots; an absent slot reads `Some(ZERO)`; base slots are never
  surfaced (this is why cleared accounts always get an `overlay_storage` entry).
- A **NotExisting** account: `account_info` → `None`, `storage_value` → `Some(ZERO)`
  for any slot; excluded from `overlay_accounts`/`overlay_code_by_hash`.
- A **non-cleared** layer-1 account: overlay slot wins, else base slot, else `None`.
- An address only in layer 2: base slot, else `None`.

`EvmOverlay::{basic, storage, code_by_hash}` are rewritten to: dirty layer →
`snapshot.account_info/storage_value/code` → (the `NotExisting`/`cleared`
short-circuits already live inside those) → `ext_db` fallback (unchanged) →
default. Behavior must match the current overlay exactly (the existing
`overlay.rs` unit tests must keep passing).

## 5. Overlay buffer / instance reuse (Pillar A.2)

### 5.1 `EvmOverlay::reset(&mut self)` — recycle one overlay across many sims

```rust
/// Clear the per-simulation dirty layer so this overlay can be reused for the
/// next simulation against the same snapshot, without reallocating.
pub fn reset(&mut self) {
    self.dirty_accounts.clear();
    self.dirty_storage.clear();
    // keep: snapshot Arc, ext_db, the reusable buffer (§5.2)
}
```

A worker doing K sims calls `EvmOverlay::new` once and `reset()` between sims
instead of allocating a fresh overlay (+ dirty maps + `Arc` clone) each time. Must
be exactly equivalent to a fresh overlay: a reset overlay reads the pristine
snapshot base again (regression test §8.2).

### 5.2 Reusable shared-memory buffer (keep `EvmOverlay: Send`)

Today each `build_evm` / `build_evm_with_inspector` allocates a fresh
`Rc<RefCell<Vec<u8>>>` of 64 KB. Reuse it across calls **without** making the
overlay `!Send`:

- Store the buffer on the overlay as a **plain `Vec<u8>`** (`Send`):
  `reusable_buffer: Vec<u8>` (pre-allocated to `OVERLAY_SHARED_MEMORY_CAPACITY` in
  `new`/`reset` keeps it).
- In the **call methods** (`call_raw`, `simulate_with_transfer_tracking`,
  `call_raw_with_access_list_with`) that own the full build→transact→revert cycle:
  `let buf = std::mem::take(&mut self.reusable_buffer);` **before** the
  `with_db(&mut *self)` borrow, move it into a method-local
  `Rc::new(RefCell::new(buf))`, build the EVM with that local context, run, then
  after the EVM is dropped reclaim `self.reusable_buffer = Rc::try_unwrap(rc).into_inner(); self.reusable_buffer.clear();`
  The `Rc` never lives on the overlay → the overlay stays `Send`.
- Refactor the shared body into e.g. `build_evm_with_local(&mut self, local: LocalContext)`;
  the **public `build_evm`** keeps allocating a fresh buffer (it hands out the EVM
  and cannot reclaim) — documented.
- A panic between take and reclaim only loses the buffer (re-allocated next call);
  no correctness impact.

`test_overlay_is_send` must still compile/pass.

## 6. Public API surface

- `EvmCache::create_snapshot(&mut self) -> Arc<EvmSnapshot>` — **signature change**
  `&self` → `&mut self` (memoizes the base). Update all call sites (freshness
  controller, tests, examples, benches). Record in CHANGELOG `### Changed`.
- `EvmCache::create_snapshot_deep_clone(&self) -> Arc<EvmSnapshot>` — **new**,
  `#[doc(hidden)] pub`. The retained reference: today's flatten producing a
  two-tier snapshot with `base` = the fully-merged flatten and empty overlay maps
  (plus the cleared/not-existing sets in place). Used by the equivalence test and
  the A/B bench. Stays `&self`.
- `EvmOverlay::reset(&mut self)` — **new** public method.
- `EvmSnapshot::storage_value` — retained (reimplemented over two tiers); used by
  the freshness validator. `account_info`/`code` are `pub(crate)`.
- No other public signatures change.

## 7. Benchmarks (`benches/simulation.rs`)

The current `populated_cache` seeds everything via `db_mut()` into **layer 1**,
which is *not* how a fork cache holds its cold index. Update/extend:

1. **Realistic cold index in layer 2.** Add a `populated_cache_layer2` that bulk-
   seeds the index via `inject_storage_batch` (the cold-load path) so the cold
   state lives in the base. The `create_snapshot` group runs the COW path on it.
2. **A/B group.** For each size, bench both `create_snapshot` (COW) and
   `create_snapshot_deep_clone` (legacy) so the win is explicit in one report.
3. **Hot-loop re-snapshot.** New bench: build the cold base, take one snapshot
   (warms the base), apply a *small* layer-1 mutation (a handful of slots via
   `apply_updates`), then measure `create_snapshot` — this is the memoization win
   (should be ≈ flat across cold-index size, vs. the deep clone's slope).
4. **`overlay_fanout`.** Add a `reset()`-recycled variant alongside the
   `EvmOverlay::new`-per-iter variant to show the A.2 win.

Keep all benches offline (mocked provider). Document expected shapes in the module
header (COW `create_snapshot` flat vs. deep-clone sloped; re-snapshot ≈ O(changed)).

## 8. Tests (red contract — written before implementation)

### 8.1 `tests/cow_snapshot.rs` — differential equivalence (the gate)

A helper `assert_equivalent(cache)` builds `cow = cache.create_snapshot()` and
`deep = cache.create_snapshot_deep_clone()` and asserts they are
**read-indistinguishable**, comparing via reads (internal reprs differ by design):
- identical account set (union of probed addresses); `basic`/`account_info` equal
  for each (including `None` for NotExisting);
- `storage_value(a, s)` equal for every probed `(a, s)` — including **absent**
  slots (expect equal `None`/`Some(ZERO)`), cleared accounts, and not-existing
  accounts;
- identical `code` for each probed code hash; identical block context;
- overlays built from each (`EvmOverlay::new(snap, None)`) return identical
  `balanceOf` / `call_raw` outputs for a `MockERC20`.

Drive a single cache through a sequence and assert equivalence **after each step**:
1. empty cache; 2. after `insert_account_info`; 3. after layer-1 storage insert;
4. after `apply_updates` `Slot` write-through (addr in layer 1 → shadowed);
5. after `apply_updates` `Slot` write-through to an addr **absent** from layer 1
   (layer-2-only — the §3 footgun); 6. after `apply_updates` `BalanceDelta`;
7. after a committing `call_raw` (revm commit → layer 1); 8. after
   `inject_storage_batch` (layer-2-only, incl. **overwriting** an existing slot at
   unchanged length); 9. after a simulated lazy fetch (insert directly into
   `blockchain_db` to mimic backend growth — both a new account and a **new slot on
   an existing account**); 10. after a `purge_*`; 11. after `set_block`.
Also: take a snapshot, then mutate the cache, and assert the **earlier** snapshot is
unchanged (memoized base is COW, not aliased).

### 8.2 Overlay reuse (extend `tests/snapshot_overlay.rs` or new module)

- `reset()` clears dirty state: commit a transfer into an overlay, `reset()`, then
  reads observe the pristine snapshot again.
- A reset-recycled overlay across two sims yields identical results to two fresh
  overlays.
- `EvmOverlay` stays `Send`; `EvmSnapshot` stays `Send + Sync` (compile asserts).
- Buffer reuse does not change call results (a second `call_raw` on the same
  overlay returns the same value as the first).

The existing `tests/snapshot_overlay.rs` cases (immutability, isolation, cleared,
not-existing) must keep passing **unchanged** — they are part of the contract.

## 9. Locked decisions

- **D1 — `Arc`-shared maps, not persistent HAMT.** Reads stay `O(1)` with no
  per-`SLOAD` regression; no external dependency. (Rejected: `imbl`/`rpds`.)
- **D2 — base memoized as immutable; over-invalidation is acceptable, silent
  staleness is not.** `write_slot_through` marks the address dirty unconditionally
  (simpler than reasoning per-call about layer-1 shadowing); the equivalence test
  is the hard backstop.
- **D3 — keep the deep clone** as `create_snapshot_deep_clone` for A/B + as the
  equivalence reference.
- **D4 — overlay reuse: buffer reuse *and* `reset()` recycle** (both in scope).
- **D5 — `create_snapshot` becomes `&mut self`** (the memoization cost). The
  freshness controller and all callers are updated.

## 10. Acceptance

All §0.5 bars green in both feature configs; the §8 tests pass; the existing
snapshot/overlay/freshness tests pass unchanged; `benches/simulation.rs` shows the
COW `create_snapshot` and the `reset()` fan-out beating their legacy/`new`
baselines, with the numbers reported. CHANGELOG / ROADMAP (Phase 5 → Done) /
KNOWN_ISSUES updated. Lands on `phase-5-cow-snapshots`, stacked on
`phase-4-event-pipeline`.
