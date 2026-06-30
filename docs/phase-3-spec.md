# Phase 3 implementation spec — state-update primitives (Pillar B.1)

> **Archival pre-release implementation note:** this file records an internal
> build contract from before the public crate boundary was finalized. It is not
> current release documentation or a current acceptance checklist. The old
> protocol adapter surface, feature-gated protocol APIs, and related
> no-default-feature validation flow were removed/extracted before public release;
> protocol-specific state tracking now belongs in `evm-amm-state`.

Implementation contract for the **targeted state-mutation vocabulary** and the
single apply primitive that writes it correctly across both cache layers,
returning a structured state diff. Read this **with**
[`ROADMAP.md`](ROADMAP.md) (the "Phase 3" row and the "Pillar B — event → state
pipeline" / "Key abstractions" sections are the design of record). This document
is the precise build contract; where they overlap, prefer this.

This is **Pillar B.1 — the writer half** of the event → state pipeline. It does
**not** decode events (no `EventDecoder`, no `Log` parsing, no WS loop): that is
Phase 4. Phase 3 builds the vocabulary an event decoder will *emit into* and the
mechanism that *applies* it, with no protocol or event knowledge in the core.

## 0. Ground rules (non-negotiable)

- **Branch:** create `phase-3-state-updates` off the current `phase-2-freshness`
  HEAD. Commit there in logical steps. Do **not** push, do **not** tag. Commits
  must be unsigned: `git -c commit.gpgsign=false commit …` (the 1Password signing
  agent is unavailable here). End every commit message with exactly:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- **The whole state-update surface is generic core** — it must compile and lint
  with `--no-default-features`. `StateUpdate` / `PurgeScope` / `AccountPatch` /
  `StateDiff` / `apply_update` / `apply_updates` must NOT depend on the
  `protocols` feature. (The *refold* of the `protocols`-gated `inject_v2/v3_*`
  helpers stays behind `protocols`, but it consumes the generic primitive.)
- **Historical green bar at every commit:**
  - `cargo fmt --all --check`
  - `cargo clippy --all-targets --no-deps -- -D warnings`
  - `cargo test`
  - `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
- MSRV is 1.88 — no newer-than-1.88 std APIs. Edition 2024.
- **Do not break existing behavior or any existing test.** Existing `inject_*` /
  `purge_*` public methods keep their signatures and return values; they become
  thin wrappers over the new primitive (the Phase 1 `call_raw` → `call_raw_with`
  pattern). The one place a *deliberate* behavior change is on the table is
  Decision 2 (§12) — and only with sign-off + a CHANGELOG/KNOWN_ISSUES entry.
- No new dependencies. (`alloy-primitives`, `revm`, `foundry-fork-db` are present.)

## 1. Objective & scope

Today the crate writes cached state through a scatter of ad-hoc methods with
**inconsistent layering**:

| Method | Layer 1 (CacheDB overlay) | Layer 2 (BlockchainDb) | Creates overlay acct? |
| --- | --- | --- | --- |
| `inject_storage_batch` | — | write | no |
| `inject_storage_batch_fresh` | write-through *if present* | write | no |
| `inject_v2_pool_metadata` / `inject_v3_*` | write (via `insert_account_storage`) | — | **yes** |
| `purge_account` | remove acct | remove acct + storage | n/a |
| `purge_contract_storage` | clear storage | remove storage | n/a |
| `purge_contract_slots` | remove slots | remove slots | n/a |
| `override_account_code*` | insert info | insert info | n/a |

Three different slot-write semantics, no machine-readable record of *what
changed*, and no single vocabulary an event decoder can target. Phase 3 fixes
all three:

1. **`StateUpdate`** — a small, generic enum: the vocabulary of targeted
   mutations (`Slot`, `Account`, `Purge`). This is what a Phase 4 `EventDecoder`
   will produce.
2. **`EvmCache::apply_update` / `apply_updates`** — the *single* primitive that
   applies a `StateUpdate` (or batch) with **one, documented, consistent**
   dual-layer policy, returning a `StateDiff`.
3. **`StateDiff`** — the structured "what actually changed" output (slot/account
   diffs + purge records), so callers (and Phase 4's reconciliation) can observe
   the effect of an apply.
4. **Refold** the existing `inject_*` / `purge_*` writers onto the primitive so
   there is exactly one place the dual-layer write logic lives.

**In scope:** the generic vocabulary; the apply primitive with write-through
semantics; the state-diff output; refolding the storage-slot and purge writers;
routing the freshness controller's correction drain through the primitive;
offline tests, an example, a benchmark, and docs.

**Out of scope (document as Phase 4/5 follow-ups, do not build):**
- **Event decoding** — `EventDecoder` trait, V3/V2 adapters, `Log` → `StateUpdate`
  (Phase 4). Phase 3 ends at the vocabulary; nothing parses a `Log`.
- **WS ingestion / `on_new_block` apply-and-purge / reorgs / RPC reconciliation**
  (Phase 4).
- **Overlay-side apply** — `EvmOverlay::apply` so a live overlay receives updates
  mid-fan-out (Phase 4/5). Phase 3 applies to the **`EvmCache`** only.
- **COW snapshots** (Phase 5) — `apply_*` operates on the existing layers.

## 2. Reuse these existing pieces (do not reinvent)

- `cache::EvmCache` (`src/cache/mod.rs`): the dual-layer fields
  `self.db.cache.accounts` (CacheDB overlay, layer 1) and `self.blockchain_db`
  (`accounts()` / `storage()` `RwLock`s, layer 2); the established write-through
  pattern in `inject_storage_batch_fresh` (the F1 fix — **the** reference for
  correct slot-write layering); `cached_storage_value`; `purge_account` /
  `purge_contract_storage` / `purge_contract_slots` (the purge layer logic to fold in);
  `self.db.insert_account_info` / `insert_account_storage` (CacheDB writers).
- `freshness::SlotChange { address, slot, old, new }` (`src/freshness.rs`,
  re-exported at crate root) — **reuse it** as the slot-diff type; do not define a
  parallel one. `StateDiff.slots: Vec<SlotChange>`.
- `revm::state::{AccountInfo, Bytecode}` — the account representation in both
  layers; `Bytecode::hash_slow()` recomputes a code hash.
- `alloy_primitives::{Address, U256, B256, Bytes}`.
- The offline test/example harness: `tests/common`, `examples/support/mock.rs`
  (mocked provider; `from_backend` cache construction with no network).

## 3. Module layout

- **`src/state_update.rs`** (new, top-level, generic, **non-`protocols`**): the
  pure data types — `StateUpdate`, `PurgeScope`, `AccountPatch`, `StateDiff`,
  `AccountChange`, `PurgeRecord` — plus their constructors / small helpers and
  in-module unit tests. No `EvmCache` dependency (pure data + logic on itself).
- **`src/cache/mod.rs`**: `EvmCache::apply_update`, `EvmCache::apply_updates`,
  and the internal per-variant helpers. Refold `inject_storage_batch_fresh`,
  `purge_account`, `purge_contract_storage`, `purge_contract_slots`,
  `override_account_code*`, and (Decision 2) `inject_v2/v3_*` onto them.
- **`src/freshness.rs`**: route the `FreshnessController::run` `pending` drain
  through `apply_updates` (§9) — behavior-preserving.
- **`src/lib.rs`**: `pub mod state_update;` + re-export
  `StateUpdate, PurgeScope, AccountPatch, StateDiff, AccountChange, PurgeRecord`.

## 4. Types & behavior

### 4.1 `StateUpdate` — the vocabulary

```rust
/// A single targeted mutation to cached EVM state.
///
/// The vocabulary an event decoder (Phase 4) emits and [`EvmCache::apply_update`]
/// consumes. Generic: carries no protocol or event knowledge.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StateUpdate {
    /// Set one storage slot to `value`, authoritative across both cache layers.
    Slot { address: Address, slot: U256, value: U256 },
    /// Patch an account's balance/nonce/code (partial — see [`AccountPatch`]).
    Account { address: Address, patch: AccountPatch },
    /// Purge cached state for `address` at `scope`; the next read re-fetches.
    Purge { address: Address, scope: PurgeScope },
}
```

Constructors for ergonomics: `StateUpdate::slot(addr, slot, value)`,
`StateUpdate::balance(addr, value)`, `StateUpdate::purge(addr, scope)`. The enum
is `#[non_exhaustive]` (new variants — e.g. a code-only convenience — may be
added pre-1.0 without a breaking change).

### 4.2 `AccountPatch` — partial account mutation

```rust
/// A partial account mutation: each `Some` field overwrites the cached value,
/// each `None` leaves it unchanged. Setting `code` recomputes the code hash;
/// `Some(empty bytes)` clears code to the empty-code hash.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountPatch {
    pub balance: Option<U256>,
    pub nonce: Option<u64>,
    pub code: Option<Bytes>,
}
```
Builders: `AccountPatch::default()`, `.balance(U256)`, `.nonce(u64)`, `.code(Bytes)`
(each returns `Self`). Rationale for **partial** (vs. a full `AccountInfo`): the
Pillar B driver is events, which usually carry *one* field (a `Transfer` changes
a balance, not nonce/code). Partial application avoids forcing a caller to
reconstruct a full `AccountInfo` (and avoids leaking revm's type into the public
vocabulary). **See Decision 1 (§12).**

### 4.3 `PurgeScope`

```rust
/// What part of an address's cached state a purge removes.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PurgeScope {
    /// Full account: `AccountInfo` (balance/nonce/code) **and** all storage.
    /// Equivalent to today's `purge_account`.
    Account,
    /// All storage slots; account info preserved. Equivalent to `purge_contract_storage`.
    AllStorage,
    /// Only the listed storage slots. Equivalent to `purge_contract_slots`.
    Slots(Vec<U256>),
}
```

### 4.4 `StateDiff` — the output

```rust
/// What an `apply_*` call actually changed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StateDiff {
    /// Storage slots whose value changed (old != new).
    pub slots: Vec<SlotChange>,           // reused from `freshness`
    /// Accounts whose balance/nonce/code-hash changed.
    pub accounts: Vec<AccountChange>,
    /// Purges performed, with what they removed.
    pub purged: Vec<PurgeRecord>,
}

/// An account field delta. Each field is `Some((old, new))` only when it changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountChange {
    pub address: Address,
    pub balance: Option<(U256, U256)>,
    pub nonce: Option<(u64, u64)>,
    pub code_hash: Option<(B256, B256)>,
}

/// Record of a purge: how much of each layer it removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PurgeRecord {
    pub address: Address,
    pub scope: PurgeScope,
    /// Storage slots removed from the BlockchainDb backend (layer 2).
    pub slots_removed: usize,
    /// Whether an `AccountInfo` was removed (only the `Account` scope).
    pub account_removed: bool,
}
```
`StateDiff` helpers: `is_empty()`, `len()` (total changed entries),
`merge(&mut self, other: StateDiff)` (used by `apply_updates` to fold per-update
diffs). **Only actual changes are recorded** — applying a `Slot` whose value
already matches the cache yields an empty diff (idempotence is observable).

## 5. `EvmCache::apply_update` / `apply_updates`

```rust
pub fn apply_update(&mut self, update: &StateUpdate) -> StateDiff;
pub fn apply_updates(&mut self, updates: &[StateUpdate]) -> StateDiff;
```
`apply_updates` folds left, merging each per-update `StateDiff`; later updates
observe the effect of earlier ones (e.g. two `Slot` writes to the same key: the
first records old→a, the second a→b). Both are **synchronous, infallible**
(no RPC — a write primitive, not a fetch). They return the diff; they do not
error. (Account/Slot writes can always succeed against the in-memory layers.)

### 5.1 `Slot` — write-through (authoritative)

Identical semantics to `inject_storage_batch_fresh` (the F1-fix reference):
1. `old = self.cached_storage_value(address, slot)` (overlay ▸ backend ▸ `None`).
2. Write `value` into the BlockchainDb backend (layer 2).
3. Write `value` into the CacheDB overlay **iff an overlay account already
   exists** for `address` (`self.db.cache.accounts.get_mut`). Do **not**
   materialize a new overlay account (preserves the cold-prefetch / layer-2-only
   invariant; materializing one could shadow later RPC reads, and a
   `StorageCleared` overlay account reads missing slots as ZERO).
4. Record `SlotChange { address, slot, old: old.unwrap_or(ZERO), new: value }`
   **only if** `old.unwrap_or(ZERO) != value`.

> A slot the cache never saw is treated as `old = ZERO` (the value a sim would
> have read), consistent with `verify_slots`.

### 5.2 `Account` — partial patch, write-through

1. Load the current `AccountInfo` from the cached layers only (overlay ▸ backend
   ▸ `AccountInfo::default()`); remember the `old` field values for the change
   record. **No RPC** (apply is a write, not a fetch).
2. Apply each `Some` patch field: `balance`, `nonce`, and for `code` set
   `info.code = Some(Bytecode::new_raw(bytes))` (the empty bytecode for empty
   input) and `info.code_hash = <that bytecode>.hash_slow()`.
3. Write-through, mirroring §5.1: write the patched `AccountInfo` into the
   BlockchainDb backend (layer 2) **always**, and into the CacheDB overlay
   (`insert_account_info`) **iff an overlay account already exists** (do not
   materialize a new overlay account — the read path falls through to the backend
   for an absent overlay entry, so a backend-only write is authoritative and we
   avoid polluting layer 1). This keeps the winning layer correct without the
   cold-backfill hazard.
4. Record an `AccountChange` with `Some((old,new))` only for fields that changed
   (compare balance, nonce, code_hash).

### 5.3 `Purge` — dispatch to existing layer logic

Dispatch on `scope` to the **existing** purge implementations (now sharing one
home), returning a `PurgeRecord`:
- `Account` → `purge_account` logic: remove from overlay accounts, backend
  accounts, backend storage. `account_removed` = removed from any account layer;
  `slots_removed` = backend storage slots removed.
- `AllStorage` → `purge_contract_storage` logic (clear overlay storage, remove
  backend storage); `slots_removed` = backend slots removed.
- `Slots(slots)` → `purge_contract_slots` logic; `slots_removed` = backend slots
  removed.

## 6. Refold map (existing → primitive)

Every existing public method **keeps its signature and return value**; it
becomes a wrapper. Existing tests must pass unchanged.

| Existing | Refold | Public API |
| --- | --- | --- |
| `inject_storage_batch_fresh(&[(a,s,v)])` | `apply_updates` of `Slot`s (discard diff) | unchanged (`-> ()`) |
| `purge_account(a)` | `apply_update(Purge{a, Account})` | unchanged (`-> ()`) |
| `purge_contract_storage(a) -> usize` | `apply_update(Purge{a, AllStorage})`; return `rec.slots_removed` | unchanged |
| `purge_contract_slots(a, slots) -> usize` | `apply_update(Purge{a, Slots(..)})`; return `rec.slots_removed` | unchanged |
| `override_account_code*` | **best-effort**: route its final write through `apply_update(Account{ patch: code })` **only if** behavior-equivalent; it has bespoke target-creation (`MissingTargetBehavior`) + source→target code-copy semantics, so if the refold is not cleanly equivalent, leave the method as-is and only cross-reference the primitive in its doc | unchanged |
| `inject_v2_pool_metadata`, `inject_v3_*` (`protocols`) | build `Vec<StateUpdate::Slot>`, `apply_updates` | **Decision 2 (§12)** |

**Not refolded (kept distinct, documented):**
- `inject_storage_batch(&[(a,s,v)])` — the **layer-2-only cold-backfill** path
  (deliberately no write-through, no overlay touch). This is a *different intent*
  from `StateUpdate::Slot` (authoritative write-through). Keep it as the
  low-level backfill primitive; add a doc line cross-referencing `apply_update`
  for authoritative writes.
- `purge_contracts_storage`, `purge_all_storage` — multi-address / whole-cache
  sweeps. Leave as-is (they already share the layer logic); optionally note they
  are batch forms of `Purge{AllStorage}`. Not required to refold.

## 7. Public re-exports

`src/lib.rs`: `pub mod state_update;` and
```rust
pub use state_update::{
    AccountChange, AccountPatch, PurgeRecord, PurgeScope, StateDiff, StateUpdate,
};
```
(`SlotChange` is already re-exported from `freshness`.)

## 8. `cargo doc` / rustdoc requirements

- A module-level `//!` doc on `state_update.rs`: the vocabulary, the apply
  primitive, the dual-layer write-through policy (one paragraph: backend always,
  overlay-if-present, no new overlay account for slots), the `StateDiff` output,
  and the **Pillar B.1** framing with an explicit "events are Phase 4" boundary.
- Rustdoc on **every** public item (no `missing_docs` gate, but `-D warnings`
  must pass and the surface must be documented thoroughly).
- A short **runnable doctest** on `apply_update` (or the module): build nothing
  network-bound — construct `StateUpdate`s and an `AccountPatch`, show the
  vocabulary and a `StateDiff` shape. (If a doctest needs an `EvmCache`, gate it
  `no_run` and use the example harness pattern; prefer a pure-data doctest.)

## 9. Freshness integration (behavior-preserving)

Route `FreshnessController::run`'s `pending` drain (currently
`cache.inject_storage_batch_fresh(&injects)`) through the new primitive:
`cache.apply_updates(&pending.iter().map(|c| StateUpdate::slot(c.address, c.slot, c.new)).collect::<Vec<_>>())`.
This is **behavior-identical** (both are write-through), and demonstrates the one
unified write path. Do not change any freshness test expectation. (The validator
itself still flows corrections back as `SlotChange`s; only the main-thread apply
changes its call.)

## 10. Tests (offline, no network) — authored as the acceptance contract

These are written **before** implementation and define correctness. Unit tests
in-module (`#[cfg(test)]` in `state_update.rs`); apply/refold integration tests
in a new `tests/state_update.rs` (reuse `tests/common`).

**`state_update.rs` unit (pure data):**
- `AccountPatch` builders compose; `Default` is all-`None`.
- `StateDiff::merge` concatenates and `is_empty`/`len` count correctly.
- `StateUpdate` constructors produce the expected variants.

**`tests/state_update.rs` integration (mocked-provider / `from_backend` cache):**
1. **Slot write-through, overlay present:** seed an overlay account + slot;
   `apply_update(Slot)`; assert both layers hold the new value and the synchronous
   SLOAD path reads it; `StateDiff.slots == [SlotChange{old,new}]`.
2. **Slot write-through, no overlay account:** apply to an address with no overlay
   entry; assert the backend holds it, **no overlay account was materialized**,
   and a subsequent read sees the value.
3. **Slot no-op:** apply the same value already cached → empty `StateDiff`.
4. **Slot idempotence:** apply twice → first diff non-empty, second empty.
5. **Account balance patch:** patch balance only; assert balance changed,
   nonce/code preserved; `AccountChange.balance == Some((old,new))`,
   `nonce/code_hash == None`.
6. **Account code patch:** patch code; assert `code_hash` recomputed
   (`Bytecode::hash_slow`), `code_hash` delta recorded; balance/nonce preserved.
7. **Cold account patch:** patch an absent account → skipped and surfaced in
   `StateDiff.skipped_accounts`; explicit `AccountUpsert` materializes with
   patched fields.
8. **Purge Account / AllStorage / Slots:** correct layers cleared; `PurgeRecord`
   counts (`slots_removed`, `account_removed`) correct on both layers.
9. **`apply_updates` fold + merge:** a mixed batch (Slot, Account, Purge) →
   merged `StateDiff`; later-overrides-earlier ordering for same-key slots.
10. **Refold equivalence:** `purge_contract_storage` wrapper returns the same `usize`
    as the pre-refold behavior on a seeded cache; `inject_storage_batch_fresh`
    wrapper leaves the cache in the same state as the equivalent `apply_updates`.
11. **(Decision 2, if "normalize"):** `inject_v3_*` now writes through to the
    backend (layer 2) — pin the new behavior.

**Existing suites must stay green** — `tests/freshness.rs`,
`tests/cache_state.rs`, `tests/snapshot_overlay.rs`, the `protocols` cache tests.

## 11. Docs, example & benchmark

- **Example** `examples/state_update_apply.rs` (offline, `examples/support`):
  build a `from_backend` cache, apply a batch — a `Slot`, an `Account` balance
  patch, and a `Purge { Slots }` — then print the returned `StateDiff` (slots
  changed, account deltas, purge records). Add a row to the README "Examples"
  table (Advanced).
- **Benchmark** `benches/state_update.rs` (offline): `apply_updates` throughput
  across batch sizes (1 → 1000), and per-variant cost (Slot vs Account vs Purge),
  building the cache once. Register `[[bench]]` in `Cargo.toml` and add a row to
  the README "Benchmarks" table. Mirror `benches/freshness.rs` structure.
- **CHANGELOG**: an `### Added` entry for the state-update vocabulary + apply +
  diff; if Decision 2 = normalize, a `### Changed` entry for the `inject_v3_*`
  layer behavior.
- **ROADMAP**: flip the Phase 3 row to **Done** with the landing branch, mirroring
  the Phase 2 "Landed on …" paragraph.
- **KNOWN_ISSUES**: if Decision 2 = normalize, add an entry recording the
  `inject_v2/v3_*` layer-behavior change (and that tests now pin it).

## 12. Decisions (LOCKED)

> Mirrors the Phase 2 "locked decisions" gate. Both were confirmed with the user
> on 2026-06-15 before the acceptance tests were authored.

**Decision 1 — `Account` variant shape. → LOCKED: partial `AccountPatch`.**
`Account { address, patch: AccountPatch { balance: Option<U256>, nonce:
Option<u64>, code: Option<Bytes> } }` (§4.2). Each `Some` overwrites, `None`
leaves as-is. Best fit for event-derived writes (one field at a time); no revm
type leaked into the public vocabulary. (The full-`AccountInfo` alternative from
the ROADMAP sketch is **not** taken.)

**Decision 2 — `inject_v2/v3_*` refold behavior. → LOCKED: normalize to
write-through.** Refold the `protocols`-gated `inject_v2_pool_metadata` /
`inject_v3_*` helpers onto the write-through `StateUpdate::Slot` primitive
(backend + overlay-if-present) instead of today's layer-1-only write. This is a
deliberate behavior change and **requires**: a CHANGELOG `### Changed` entry, a
KNOWN_ISSUES entry, and test #11 (§10) pinning the new write-through behavior.
The `protocols` pool tests do not pin layer placement, so they stay green.

## 13. Build order (commit per step, green each time)

1. `src/state_update.rs`: `StateUpdate`, `PurgeScope`, `AccountPatch`,
   `StateDiff`, `AccountChange`, `PurgeRecord` + constructors/helpers + unit
   tests; `lib.rs` re-exports.
2. `EvmCache::apply_update` / `apply_updates` (Slot, Account, Purge) + the
   `tests/state_update.rs` integration tests.
3. Refold `inject_storage_batch_fresh`, `purge_account`, `purge_contract_storage`,
   `purge_contract_slots`, `override_account_code*`, and (per Decision 2)
   `inject_v2/v3_*`; route the freshness drain through `apply_updates`.
4. Example + benchmark + README rows.
5. Docs (module `//!`, item rustdoc, doctest), CHANGELOG, ROADMAP → Done,
   KNOWN_ISSUES (if normalize).

## 14. Final acceptance

Both feature configs green (§0). All new + existing tests pass (`tests/state_update.rs`
+ the in-module unit tests + the untouched existing suites). The example runs
offline and prints a non-trivial `StateDiff`. The benchmark builds and runs.
Report: what landed per file, the public API added, the refold map (with any
behavior change called out), test coverage, and the verification output.

---

## 15. Addendum — relative / read-modify-write updates (decisions LOCKED)

> Added 2026-06-15 after the §1–§14 surface landed (green, uncommitted). Motivated
> by the event-driven balance-tracking case: a caller indexing ERC-20 `Transfer`
> logs to keep a tracked account's balance hot only learns the **delta**
> (`amount`), not the resulting absolute balance, so the engine must support
> *relative* updates — read the current value, apply a mutation, write back. The
> §4 vocabulary today is **absolute-only**; this addendum adds the relative
> capability. It remains generic core (no protocol knowledge; the slot derivation
> and the ± decision belong to the caller / the Phase-4 decoder).

### 15.1 The correctness constraint (non-negotiable)

A relative update is only valid against a value the cache **actually holds**. An
un-fetched ("cold") slot has *no* value — and `cached_storage_value` / `apply_slot`
treat absent as `ZERO`. Applying `delta` to a cold slot would compute
`0 ± amount`, write a wrong value, and (write-through) make it authoritative —
silently corrupting state. Therefore relative application must be **cold-aware**:
apply only when the current value is known; otherwise **skip and surface** it.

### 15.2 Locked decisions

**Decision 3 — shape. → LOCKED: vocabulary variant + method.** Add *both*:
(a) a data-level [`StateUpdate::SlotDelta`] variant (so it flows through
`apply_updates` and a Phase-4 `EventDecoder` can emit it as data); (b) a general
`EvmCache::modify_slot` closure escape hatch for arbitrary transforms.

**Decision 4 — cold-slot handling. → LOCKED: skip & surface.** A `SlotDelta`
targeting a slot absent from **both** layers is **not applied**; it is recorded
in `StateDiff.skipped` so the caller can fetch+seed the true value (the next read
otherwise lazily fetches it). For `modify_slot`, the closure receives
`Option<U256>` (`None` when cold) and decides. Overflow is **saturating**
(`Add` clamps at `U256::MAX`, `Sub` at `U256::ZERO`).

### 15.3 Types (in `src/state_update.rs`)

```rust
/// A relative storage-slot mutation: read the current value, transform it, write
/// back. Both directions saturate (`Add` at `U256::MAX`, `Sub` at `U256::ZERO`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotDelta { Add(U256), Sub(U256) }
impl SlotDelta {
    /// Apply the (saturating) delta to a current value.
    pub fn apply(self, current: U256) -> U256;
}

// New variant on the existing enum:
pub enum StateUpdate {
    Slot { address, slot, value },
    SlotDelta { address: Address, slot: U256, delta: SlotDelta },   // NEW
    Account { address, patch },
    Purge { address, scope },
}
impl StateUpdate {
    /// Construct a relative slot update.
    pub fn slot_delta(address: Address, slot: U256, delta: SlotDelta) -> Self;
}

/// A relative update that could not be applied because the slot's current value
/// is unknown (not cached in either layer). Fetch+seed the slot, then retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkippedDelta { pub address: Address, pub slot: U256, pub delta: SlotDelta }

// New field on the existing StateDiff (Default = empty):
pub struct StateDiff {
    pub slots: Vec<SlotChange>,
    pub accounts: Vec<AccountChange>,
    pub purged: Vec<PurgeRecord>,
    pub skipped: Vec<SkippedDelta>,     // NEW
}
```
`StateDiff::merge` also extends `skipped`. `is_empty()` / `len()` remain
**changes-only** (slots + accounts + purged) — a skip is *not* a change; document
that `skipped` is separate informational metadata (it does not affect
`is_empty`/`len`, so the §10 no-op/idempotence expectations are unchanged).

### 15.4 `EvmCache` behavior

- `apply_update(StateUpdate::SlotDelta { address, slot, delta })`: if
  `cached_storage_value(address, slot)` is `Some(current)`, write
  `delta.apply(current)` through both layers (reuse the §5.1 write path) and push
  a `SlotChange` iff it changed; if `None` (cold), push a `SkippedDelta` to
  `diff.skipped` and write nothing.
- `modify_slot(&mut self, address: Address, slot: U256, f: impl FnOnce(Option<U256>) -> Option<U256>) -> Option<SlotChange>`:
  call `f` with the current cached value (`None` if cold); if it returns
  `Some(new)`, write-through (same path) and return the `SlotChange` iff
  `old.unwrap_or(ZERO) != new`; if it returns `None`, write nothing and return
  `None`. (The caller owns the cold/overflow policy here; e.g.
  `|cur| cur.map(|v| v.saturating_add(amount))` implements skip-on-cold.)
- Refactor the dual-layer slot write out of `apply_slot` into a private
  `write_slot_through(address, slot, value)` helper shared by `apply_slot`,
  the `SlotDelta` handler, and `modify_slot` (one write path).

Scope note: account-native-ETH-balance relative updates (an `AccountDelta` /
`modify_account_balance`) are **out of scope** here — the asked case is ERC-20,
whose balances are storage slots. Document that they can be added symmetrically
later if native-ETH tracking is needed.

### 15.5 Tests (append to `tests/state_update.rs`)

- `slot_delta_add_applies_to_hot_slot` — seed (backend) 100, `Add(50)` → 150;
  `diff.slots == [SlotChange{100,150}]`, `diff.skipped` empty.
- `slot_delta_sub_saturates_at_zero` — seed 30, `Sub(50)` → 0.
- `slot_delta_add_saturates_at_max` — seed `MAX-1`, `Add(10)` → `MAX`.
- `slot_delta_cold_slot_is_skipped_and_surfaced` — fresh (uncached) slot, `Add(50)`
  → not applied; `diff.slots` empty; `diff.skipped == [SkippedDelta{..}]`;
  `cached_storage_value` still `None`.
- `slot_delta_writes_through_both_layers` — overlay-resident slot (install account
  + seed), `Add` updates both overlay and backend.
- `modify_slot_applies_transform` — seed 10, `|c| c.map(|v| v*2)` → 20.
- `modify_slot_closure_skips_cold` — fresh slot, `|c| c.map(|v| v+1)` → returns
  `None`, nothing written, slot still cold.
- `modify_slot_can_write_absolute_on_cold` — fresh slot, `|_| Some(7)` → writes 7
  (caller's explicit choice), `SlotChange{0,7}`.
- `state_diff_merge_includes_skipped` — merge concatenates `skipped`.
- `balance_tracking_scenario` — **the motivating end-to-end case**: seed two
  holders' balance slots, then apply a `Transfer` as
  `[SlotDelta::Sub(amount) on from, SlotDelta::Add(amount) on to]` via
  `apply_updates`; assert both balances are correct and `from + to` is conserved.

### 15.6 Docs / example / changelog

- Rustdoc on every new item; the module `//!` doc gains a short "relative updates"
  paragraph (the cold-aware read-modify-write rule).
- Extend `examples/state_update_apply.rs` (or a focused addition) to show a
  `SlotDelta` balance bump **and** a cold-slot skip surfaced via `diff.skipped`.
- Optionally extend `benches/state_update.rs` with a `SlotDelta` apply case
  (not required).
- CHANGELOG `### Added`: the relative-update vocabulary (`SlotDelta`,
  `StateUpdate::SlotDelta`, `modify_slot`, `StateDiff.skipped`). Note the
  `StateDiff` field addition under the pre-1.0 break policy.
- ROADMAP: fold a one-line mention into the Phase 3 "Landed on …" paragraph.

### 15.7 Acceptance (addendum)

All of §14 plus: the new tests pass; `diff.skipped` is exercised; the
`balance_tracking_scenario` demonstrates the motivating use case end-to-end; both
feature configs stay green.

---

## 16. Addendum — post-audit remediation (COMPREHENSIVE, decisions LOCKED)

> Added 2026-06-15 after a 5-lens adversarial audit of the §1–§15 surface (bugs,
> API design, coverage, benchmarks). The user selected the **Comprehensive**
> remediation scope. This section is the precise build contract for that scope.
> Every item below is LOCKED. Where this section conflicts with earlier sections,
> prefer this. Hard rules of §0 still apply (offline tests, both feature configs
> green, MSRV 1.88, edition 2024, no new deps, unsigned commits).

### 16.0 The correctness bug (P0 — must fix first)

**Defect (audit HIGH + MED, verified with a reproducer):** the cold-aware safety
guarantee rests on `EvmCache::cached_storage_value` returning what the EVM would
`SLOAD`. That invariant is **false** for an overlay account whose revm
`account_state` is `StorageCleared` or `NotExisting`: for a slot absent from the
overlay storage map, the live `CacheDB::storage`/`storage_ref` returns **ZERO and
never consults the backend**, but `cached_storage_value` (src/cache/mod.rs
~1404-1412) falls through to the BlockchainDb backend and returns
`Some(backend_value)`. Consequences: a `SlotDelta`/`modify_slot` computes
`delta.apply(backend_value)` against a base the EVM never sees (silent
corruption), and `apply_slot` records a wrong `SlotChange.old` and mis-gates the
change predicate. `install_mock_erc20` produces exactly this state
(`replace_account_storage` ⇒ `StorageCleared`), and a backend-only seed via
`inject_storage_batch` is invisible to the EVM — which is why
`balance_tracking_scenario` currently passes while asserting against the buggy
accessor instead of a real `SLOAD`.

**Fix (LOCKED): make `cached_storage_value` `account_state`-aware**, mirroring
`CacheDB::storage_ref`:
```rust
pub fn cached_storage_value(&self, address: Address, slot: U256) -> Option<U256> {
    if let Some(db_account) = self.db.cache.accounts.get(&address) {
        if let Some(value) = db_account.storage.get(&slot) {
            return Some(*value);
        }
        // Match the EVM SLOAD: a StorageCleared / NotExisting overlay account
        // reads a missing slot as ZERO and never consults the backend.
        if matches!(
            db_account.account_state,
            AccountState::StorageCleared | AccountState::NotExisting
        ) {
            return Some(U256::ZERO);
        }
    }
    let storage = self.blockchain_db.storage().read();
    storage.get(&address).and_then(|s| s.get(&slot).copied())
}
```
`AccountState` is revm's enum on `DbAccount` (resolve the exact import path; it is
re-exported from the revm database crate already in use). This single fix repairs
the `SlotDelta`/`modify_slot` base read (HIGH) and `apply_slot`'s `old`/predicate
(MED) at once, and also closes the pre-existing same-root mismatch shared by
`verify_slots` / `inject_storage_batch_fresh`.

**Tests must validate the EVM SLOAD, not the accessor:**
- **New invariant test** (the red reproducer): with `install_mock_erc20` +
  backend-only `inject_storage_batch` seed of slot=100, assert
  `cached_storage_value(token, slot) == Some(ZERO)` **and** that it equals what a
  real `balance_of`/SLOAD reads (both ZERO). Pre-fix this returns `Some(100)`.
- **Re-point `balance_tracking_scenario`**: seed the holder balance slots in an
  **EVM-visible** way (overlay-resident via `db_mut().insert_account_storage`, so
  the slots are real to the EVM), apply the `SlotDelta` transfer, and assert the
  results via `balance_of` (a real `SLOAD`) in addition to `cached_storage_value`.
- **Present-as-ZERO vs cold**: a slot known to be ZERO (overlay-resident `0`, or a
  `StorageCleared` account's absent slot) is **hot** — `SlotDelta::Add(50)` ⇒ 50,
  recorded in `diff.slots`, **not** in `diff.skipped`. Cold (no overlay account
  **and** no backend value) stays skip-and-surface. Add a test for the hot-zero
  case (it is currently the untested seam between Decision-4 skip and apply).

### 16.1 No-op / cold `Account` patch must not materialize a backend account (audit LOW; tightened in Phase 5)

`apply_account_patch` (src/cache/mod.rs ~1331-1340) writes the patched
`AccountInfo` into the backend **unconditionally**, so an all-`None` (or
otherwise no-change) patch on an address absent from both layers inserts
`AccountInfo::default()` into the shared backend map while returning an **empty**
diff — breaking no-op parity with the Slot path and (per the cold-account hazard)
masking a future RPC fetch. **Fix (LOCKED):** compute the change first; **only
write-through when at least one field actually changes** (i.e. skip both layer
writes and return `None` when the patched `info` equals the loaded base).
Phase 5 tightened the cold-account contract further: a real field change on an
address absent from both layers now skips and records `SkippedAccountPatch` in
`StateDiff.skipped_accounts`; explicit materialization uses
`StateUpdate::AccountUpsert`. Add no-op and cold-skip idempotence tests (patching
balance to its current value ⇒ empty diff; cold balance patch ⇒ no backend account
materialized).

### 16.2 Cold absolute-`Account`-patch hazard — fixed in Phase 5 (audit LOW)

A *partial* absolute `Account` patch on a cold (un-fetched) address used to write
default nonce/code through the shared backend, masking the real on-chain account.
Phase 5 changed this contract: `StateUpdate::Account` is cold-aware and records a
`SkippedAccountPatch` instead; callers that intentionally want a synthetic/default
account use `StateUpdate::AccountUpsert`.

### 16.3 `serde` on the vocabulary (audit HIGH gap)

`serde` is a non-optional crate dependency and other public types
(`StorageAccessList`, `PrefetchRegistry`) already derive/serialize. The event
pipeline (the stated motivation) needs to serialize `StateUpdate`s and ship
`StateDiff`s. **Fix (LOCKED):** derive `serde::Serialize, serde::Deserialize`
**unconditionally** on `SlotDelta`, `StateUpdate`, `AccountPatch`, `PurgeScope`,
`StateDiff`, `AccountChange`, `PurgeRecord`, `SkippedDelta`, the new `BalanceDelta`
payload / `SkippedBalanceDelta` (§16.5), **and** `SlotChange` (src/freshness.rs).
Add a JSON round-trip test for a representative `StateUpdate` set and a `StateDiff`.
(All fields are `Address`/`U256`/`B256`/`Bytes`/`u64`/`usize`/`bool` — derives
compile today with the alloy serde features already enabled.)

### 16.4 `#[non_exhaustive]` on output/record types (audit HIGH)

`StateDiff` just grew a field as a documented pre-1.0 break, and §16.5 adds
another (`skipped_balances`). **Fix (LOCKED, scoped):** add `#[non_exhaustive]`
to **`StateDiff`** (the aggregate that demonstrably grows) and **`AccountPatch`**
(builder-constructed via `.balance()/.nonce()/.code()` + `Default`). Both are
still constructed by external callers/tests through `Default` + field-assignment
(`StateDiff`) or the builders (`AccountPatch`), so future field additions are
non-breaking at zero ergonomic cost. (`StateUpdate` and `PurgeScope` already are
`#[non_exhaustive]`.)

**Deliberately NOT `#[non_exhaustive]`** — the leaf record types `SlotChange`,
`AccountChange`, `PurgeRecord`, `SkippedDelta`, and `SkippedBalanceDelta`. These
are routinely **constructed as struct literals in equality assertions** by both
the test suite (`diff.skipped == vec![SkippedDelta { .. }]`,
`diff.slots == vec![SlotChange { .. }]`) and downstream users testing against a
returned diff. `#[non_exhaustive]` would forbid that external construction — a
real, non-zero cost that outweighs the speculative benefit of these stable,
fully-determined shapes gaining a field. (This is a deliberate, documented
departure from the audit finding, which assumed these were read-only; assertion
construction is the counter-case.)

### 16.5 New capability — account-native-balance delta (audit MED gap)

Relative-update symmetry: `SlotDelta` covers ERC-20 (storage) balances, but
native-ETH tracking (value transfers, coinbase, selfdestruct) is learned as a
delta too. **Add (LOCKED):**
```rust
// reuse SlotDelta (Add/Sub, saturating) for the relative amount
pub enum StateUpdate { /* … */ BalanceDelta { address: Address, delta: SlotDelta } }   // NEW variant
impl StateUpdate { pub fn balance_delta(address: Address, delta: SlotDelta) -> Self; }  // NEW ctor

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct SkippedBalanceDelta { pub address: Address, pub delta: SlotDelta }            // NEW

pub struct StateDiff { /* … */ pub skipped_balances: Vec<SkippedBalanceDelta> }          // NEW field

impl EvmCache {
    /// Read-modify-write the native balance. `f` gets the current cached balance
    /// (`None` if the account is absent from both layers); `Some(new)` writes it
    /// through (preserving nonce/code), `None` writes nothing.
    pub fn modify_account_balance(
        &mut self, address: Address, f: impl FnOnce(Option<U256>) -> Option<U256>,
    ) -> Option<AccountChange>;
}
```
- **Cold-aware:** "cold" for a balance = the account is absent from **both** layers
  (balance unknown). `account_state` does **not** matter here (it governs storage,
  not the basic `AccountInfo`). A `BalanceDelta` on a cold account is **not
  applied**; it is surfaced in `StateDiff.skipped_balances` (avoids the §16.2
  masking — we never write a default account). On a present account, load the full
  `AccountInfo` (overlay ▸ backend), apply the saturating delta to `info.balance`,
  preserve nonce/code, write-through (backend always, overlay-if-present), record
  an `AccountChange` (balance only) iff it changed.
- `modify_account_balance` is the closure analog (same load/cold rules; `f` decides).
- `StateDiff::merge` extends `skipped_balances`. `is_empty`/`len` stay
  **changes-only**. `has_skipped`/`skipped_len`/`is_fully_applied` (§16.6) count
  **both** `skipped` and `skipped_balances`.
- Tests: hot apply (Add/Sub/saturation, AccountChange recorded, nonce/code
  preserved), cold skip-and-surface (`skipped_balances` populated, no backend
  account materialized), `modify_account_balance` hot/cold/`None`.

### 16.6 Discoverable skip accessors + loud docs (audit MED footgun)

A cold-skipped relative update is invisible to the natural `is_empty()`/`len()`
success check, so a dropped balance update can break conservation silently.
**Add (LOCKED)** on `StateDiff`:
- `has_skipped(&self) -> bool` — `!skipped.is_empty() || !skipped_balances.is_empty()`.
- `skipped_len(&self) -> usize` — `skipped.len() + skipped_balances.len()`.
- `is_fully_applied(&self) -> bool` — `!self.has_skipped()`.

Document prominently on `apply_update`/`apply_updates` that after relative
updates the caller **must** check `has_skipped()`/`skipped`/`skipped_balances` — a
cold target is dropped, not applied. Mirror this in the example.

### 16.7 Constructor symmetry (audit LOW)

Add convenience constructors for parity with `slot`/`balance`/`purge`/`slot_delta`:
`StateUpdate::nonce(address, u64)`, `StateUpdate::code(address, Bytes)`,
`StateUpdate::account(address, AccountPatch)`.

### 16.8 Coverage gaps (audit — all listed)

Add tests (in `tests/state_update.rs` unless noted). Each must assert the
**layer-correct** outcome, not just the accessor:
- **Account-patch backend-write-always:** on an overlay-present account, assert
  `backend_balance(...)` updates (not only overlay).
- **Account-patch no-overlay-materialization:** after patching a backend-only /
  absent account that *does* change, assert no *new* overlay account is
  materialized where the spec says none should be (mirror
  `apply_slot_no_overlay_account_is_not_materialized`).
- **Backend-only account patch:** seed an account only in the backend
  (`unchecked_blockchain_db().accounts().write().insert`), patch balance, assert
  `AccountChange.balance == Some((old,new))`, backend updated, overlay still absent.
- **Nonce-only** and **multi-field (balance+nonce+code)** patches: assert the
  respective `AccountChange` fields are `Some`/`None` correctly.
- **Empty-code clear:** patch `code(Bytes::new())` over non-empty code ⇒
  `code_hash` → `KECCAK_EMPTY`; patching empty over already-empty ⇒ `None`.
- **Account-patch idempotence/no-op** (also pins §16.1): balance→current ⇒ empty
  diff, no backend materialization.
- **Decision-2 pins:** `inject_v2_pool_metadata_writes_through_to_backend` and
  `inject_v3_ticks_writes_through_to_backend` (protocols-gated), mirroring the
  existing bitmap test.
- **Purge edges:** purge of an absent address ⇒ `PurgeRecord{account_removed:false,
  slots_removed:0}`; `PurgeScope::Slots` with some slots absent ⇒ `slots_removed`
  counts only present backend slots; overlay-vs-backend accounting (overlay-only
  slots not counted in `slots_removed`).
- **`modify_slot` write-through layers:** overlay-present ⇒ both layers updated;
  absent ⇒ backend only, no overlay materialized.
- **Batched == sequential equivalence** (the perf-fast-path safety net, §16.9):
  a mixed `apply_updates([...])` batch (distinct addresses, a same-address repeat,
  and a `Purge` mid-batch) leaves **byte-identical** layer state **and** an
  equivalent merged `StateDiff` to applying each update via `apply_update` in
  sequence. This test must pass both before and after the perf work.

### 16.9 Performance (audit — benchmarks)

Benchmarks showed `apply_updates` ≈ 4.4× the per-element cost of raw
`inject_storage_batch`, dominated by **per-update `RwLock` churn** (a read lock for
the old value + a separate write lock per slot) and a redundant `SlotDelta` read.
This matters because `inject_storage_batch_fresh` and the `inject_v3_*` writers now
route through `apply_updates` for **bulk** seeding. **Fix (LOCKED):**
1. **Eliminate the `SlotDelta` double read:** the `SlotDelta` arm already reads
   `cached_storage_value` for `current`; build the `SlotChange` from that value and
   call the shared write path directly instead of routing through `apply_slot`
   (which re-reads the same slot).
2. **Batched single-lock fast-path** for `apply_updates`: process consecutive
   `Slot`/`SlotDelta` writes holding the backend storage write-guard **once** for
   the run (overlay access is lock-free on `self.db.cache.accounts`). Preserve
   apply order: when an `Account`/`Purge` update is reached, **drop the guard
   first** (those take `accounts()` / `storage()` locks themselves — holding the
   storage write-guard across `apply_purge` would deadlock on the non-reentrant
   `RwLock`), process it, then lazily re-acquire on the next slot run. Correctness
   is pinned by the §16.8 batched==sequential equivalence test and the existing
   refold-equivalence tests; **do not** weaken any of them. The old-value read must
   stay `account_state`-aware (§16.0) even inside the held guard.
3. Single-update `apply_update`/`apply_slot` may keep the read-then-write split
   (correctness first); the batch path is where the lock win is realized.

### 16.10 Missing benchmarks (audit)

Extend `benches/state_update.rs` (keep existing cases): `SlotDelta` hot-apply and
cold-skip; `modify_slot`; a **heterogeneous** `apply_updates` batch (Slot +
Account + Purge); `Account` **code** patch (the `Bytecode::new_raw` + `hash_slow`
keccak — likely the most expensive single apply); `PurgeScope::Account` and
`PurgeScope::Slots`; a **distinct-address** `apply_updates` batch (the only fair
apples-to-apples vs the `inject_storage_batch` baseline). All benches stay offline
and must build under `cargo bench --no-run`.

### 16.11 Docs / CHANGELOG / ROADMAP

- Rustdoc on every new item; update the `state_update` module `//!` doc to cover
  `BalanceDelta`, the skip accessors, and the cold-account warning.
- `examples/state_update_apply.rs`: add a `BalanceDelta` bump + a cold
  `BalanceDelta` surfaced via `diff.skipped_balances`, and use `has_skipped()`.
- CHANGELOG: `### Fixed` (the `cached_storage_value` corruption bug; the no-op
  Account materialization) and `### Added` (`serde`; `#[non_exhaustive]`;
  `BalanceDelta`/`modify_account_balance`/`SkippedBalanceDelta`/
  `StateDiff.skipped_balances`; `has_skipped`/`skipped_len`/`is_fully_applied`;
  `StateUpdate::nonce`/`code`/`account`). Note the additive `StateDiff` field and
  the `#[non_exhaustive]` additions under the pre-1.0 break policy.
- ROADMAP: extend the Phase 3 "Landed on …" paragraph with the §16 remediation.
- KNOWN_ISSUES: the §16.2 cold-account-patch entry.

### 16.12 Acceptance (remediation)

All of §14 plus: every §16 test passes; the corruption reproducer is **red before /
green after** the §16.0 fix; `balance_tracking_scenario` validates via a real
`SLOAD`; the batched==sequential equivalence test passes; `serde` round-trips;
both feature configs green (`cargo test`, `clippy` default + `--no-default-features`,
`fmt`, `RUSTDOCFLAGS=-D warnings doc`); `cargo bench --no-run` builds all benches;
the example runs offline and shows a skipped relative update via `has_skipped()`.
