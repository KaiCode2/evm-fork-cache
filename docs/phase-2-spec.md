# Phase 2 implementation spec — freshness core + optimistic execution

Implementation contract for the freshness control plane and the optimistic
verify-and-rerun loop with deferred validation. Read this **with**
[`ROADMAP.md`](ROADMAP.md) (the "Phase 2 — freshness core" section is the design
of record). This document is the precise build contract; where they overlap,
prefer this.

## 0. Ground rules (non-negotiable)

- **Branch:** create `phase-2-freshness` off the current `phase-1-engine-seam`
  HEAD. Commit there in logical steps. Do **not** push, do **not** tag. Commits
  must be unsigned: `git -c commit.gpgsign=false commit …` (the 1Password signing
  agent is unavailable here). End every commit message with exactly:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- **The whole freshness surface is generic core** — it must compile and lint with
  `--no-default-features` (it must NOT depend on the `protocols` feature).
- **Green bar at every commit, both feature configs:**
  - `cargo fmt --all --check`
  - `cargo clippy --all-targets --no-deps -- -D warnings`
  - `cargo clippy --lib --no-default-features --no-deps -- -D warnings`
  - `cargo test`
  - `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
- MSRV is 1.88 — no newer-than-1.88 std APIs. Edition 2024.
- Do **not** change Phase 1 behavior or break any existing test (118 + doctests).
- No new dependencies without strong justification (tokio is already present with
  `rt-multi-thread` + `macros` in dev). The async loop uses tokio (already a dep).

## 1. Objective & scope

Deliver the four-layer freshness model and the optimistic execution loop:

- **Classification** — `Validity` (`Pinned`/`Volatile`/`ValidThrough`) + `FreshnessRegistry`.
- **Observation** — revive `SlotObservationTracker`, make it clock-agnostic.
- **Policy** — `FreshnessPolicy` trait + `AlwaysVerify`/`ObservationDriven`/`NeverVerify`.
- **Mechanism** — `EvmCache::verify_slots` + `purge_account`; `FreshnessController`
  running the optimistic loop returning `SpeculativeSim` (deferred validation).

**In scope:** optimistic verification of the **storage-slot** read-set, deferred
validation (`SpeculativeSim`/`Validation`), background re-run on mismatch,
configurable block/wall clock, the `purge_account` primitive, overlay read-set
capture.

**Out of scope (document as follow-ups, do not build):** account-*balance*
optimistic verification (needs a batched balance fetcher — the current
`StorageBatchFetchFn` is storage-only); event-derived writes (Phase 3); WS
ingestion / reorgs / RPC reconciliation (Phase 4). Committing simulations
speculatively is out of scope — the optimistic loop handles **non-committing**
evaluation sims only.

## 2. Reuse these existing pieces (do not reinvent)

- `cache::EvmCache` (`src/cache/mod.rs`): `create_snapshot() -> Arc<EvmSnapshot>`,
  `storage_batch_fetcher() -> Option<&StorageBatchFetchFn>`,
  `inject_storage_batch(&[(Address,U256,U256)])`, `purge_pool_storage`,
  `purge_pool_slots`, `call_raw_with`/`TxConfig`, `CallSimulationResult`,
  `blockchain_db()`, `db_mut()`.
- `cache::EvmOverlay` / `cache::EvmSnapshot` (`overlay.rs`/`snapshot.rs`):
  `EvmOverlay::new(Arc<EvmSnapshot>, Option<SharedBackend>)`, `call_raw`,
  `simulate_with_transfer_tracking`. `EvmOverlay` is `Send`.
- `cache::SlotObservationTracker` (`src/cache/slot_observations.rs`): **dormant** —
  this is its intended use. `SlotObservation { last_value, observation_count,
  change_count, last_checked, last_changed }`, `observe`, `should_refetch`,
  `take_skipped`, persistence.
- `StorageBatchFetchFn = Arc<dyn Fn(Vec<(Address,U256)>) -> Vec<(Address,U256,Result<U256>)> + Send + Sync>`
  — the batched RPC fetcher. **Synchronous** (it block_on's internally), `Send + Sync`.
- `access_set::StorageAccessList { accounts: HashSet<Address>, slots: HashSet<(Address,U256)> }`.

## 3. Module layout

- **`src/freshness.rs`** (new, top-level, generic): `Validity`, `FreshnessRegistry`,
  `FreshnessClock` + `BlockClock` + `WallClock`, `FreshnessParams`,
  `FreshnessPolicy` + built-ins, `SlotChange`, `Validation`, `SpeculativeSim`,
  `SimRequest`, `FreshnessController`. Operates on `EvmCache` via its public API.
- **`src/cache/mod.rs`**: add `verify_slots`, `purge_account`,
  `set_storage_batch_fetcher` (test seam).
- **`src/cache/overlay.rs`**: add `call_raw_with_access_list` (read-set capture).
- **`src/cache/slot_observations.rs`**: make clock-agnostic (take `now: u64`).
- **`src/lib.rs`**: `pub mod freshness;` + re-export the key types.

## 4. Types & behavior

### 4.1 Classification

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Validity { Pinned, Volatile, ValidThrough(u64) }

#[derive(Clone, Debug)]
pub struct FreshnessRegistry {
    default: Validity,                           // Volatile by default
    accounts: HashMap<Address, Validity>,
    slots: HashMap<(Address, U256), Validity>,
}
```
- `new()` → default `Volatile`; `with_default(Validity)`.
- Builder-style setters returning `&mut Self`: `pin`, `pin_slot`, `mark_volatile`,
  `mark_volatile_slot`, `valid_through`, `valid_through_slot`, `set_account`, `set_slot`.
- `validity(addr, slot) -> Validity` — resolution **slot ▸ account ▸ default**.
- `is_volatile(addr, slot, now: u64) -> bool` — `true` for `Volatile`, and for
  `ValidThrough(m)` when `now > m`; `false` for `Pinned` / still-valid `ValidThrough`.
- Must be `Clone` (background task needs a snapshot of it).

### 4.2 Clock

```rust
pub trait FreshnessClock: Send + Sync { fn now(&self) -> u64; }
pub struct BlockClock(Arc<AtomicU64>);   // settable via set_block(u64); Clone shares the Arc
pub struct WallClock;                     // now() = unix seconds
```
`BlockClock` is the default. The controller calls `clock.now()` and threads it as
`now: u64` everywhere (tracker, policy, `is_volatile`).

### 4.3 Observation tracker (revive + clock-agnostic)

Change `SlotObservationTracker` so it does **not** call `unix_now()` internally:
- `observe(&mut self, addr, slot, value, now: u64) -> bool`
- `should_refetch(&self, addr, slot, now: u64, params: &FreshnessParams) -> bool`

Move the hardcoded thresholds into `FreshnessParams`:
```rust
#[derive(Clone, Debug)]
pub struct FreshnessParams {
    pub min_observations: u32,     // default 10
    pub max_reuse: u64,            // clock units; block default e.g. 300; wall = 7*86400
    pub staleness_threshold: f64,  // default 0.05
    pub always_refetch_rate: f64,  // default 0.9
    pub cycle_interval: u64,       // clock units per "cycle"; block default 1
}
```
`should_refetch` keeps the existing probabilistic logic but in clock units. Update
the existing `slot_observations.rs` unit tests to pass `now`/`params`.

### 4.4 Policy

```rust
pub trait FreshnessPolicy: Send {
    /// Of these volatile candidate slots, which must be verified this cycle?
    fn select(&mut self, candidates: &[(Address, U256)],
              obs: &SlotObservationTracker, now: u64) -> Vec<(Address, U256)>;
    fn on_new_block(&mut self, _block: u64) {}
}
```
Built-ins:
- `AlwaysVerify` — returns all candidates (safe/eager).
- `NeverVerify` — returns empty (trust-all; results always `Confirmed`).
- `ObservationDriven { params: FreshnessParams }` — returns candidates where
  `obs.should_refetch(addr, slot, now, &params)`.

### 4.5 Results & deferred validation

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotChange { pub address: Address, pub slot: U256, pub old: U256, pub new: U256 }

pub enum Validation {
    Confirmed,
    Corrected { results: Vec<CallSimulationResult>, changed: Vec<SlotChange> },
    Unverified { reason: String },
}

pub struct SpeculativeSim {
    optimistic: Vec<CallSimulationResult>,
    validation: tokio::task::JoinHandle<Validation>,
}
impl SpeculativeSim {
    pub fn optimistic(&self) -> &[CallSimulationResult];
    pub fn into_optimistic(self) -> Vec<CallSimulationResult>;  // aborts validation
    pub async fn validate(self) -> Validation;                  // awaits the verdict
}
impl Drop for SpeculativeSim { /* abort the background task */ }
```
`CallSimulationResult` must be `Clone` (verify it already is; add derive if needed)
so optimistic + corrected copies can coexist and cross the task boundary. It also
carries a `pub output: Bytes` field (the call's raw return data: the `Success`
payload, the `Revert` payload, or empty on `Halt`), so a corrected **view-call**
re-run that returns a new value is observable even when both runs succeed —
`Corrected.results[i].output` differs from `optimistic[i].output`.

### 4.6 Request

```rust
pub struct SimRequest {
    pub from: Address,
    pub to: Address,
    pub calldata: Bytes,
    pub tx: TxConfig,   // access_list here is the predicted read set (perf hint)
}
```

## 5. `EvmCache` primitives

- `verify_slots(&mut self, slots: &[(Address, U256)]) -> anyhow::Result<Vec<SlotChange>>`:
  fetch fresh values via the batch fetcher; compare to currently-cached values; for
  each that differs, `inject_storage_batch` the fresh value and record a `SlotChange`.
  Returns the changed set. (Synchronous main-thread helper + the test target.)
- `purge_account(&mut self, addr: Address)`: remove `addr` from the CacheDB overlay
  accounts (`self.db.cache.accounts`), the BlockchainDb accounts map, and the
  BlockchainDb storage map — so the next access re-fetches a clean `AccountInfo`.
  Distinct from storage-only `purge_pool_storage`. Add a doc comment + a test.
- `set_storage_batch_fetcher(&mut self, f: StorageBatchFetchFn)`: test/extensibility
  seam so a stub fetcher can be injected without a provider.

## 6. `EvmOverlay` read-set capture

Add `call_raw_with_access_list(&mut self, from, to, calldata) -> Result<(ExecutionResult, StorageAccessList)>`
mirroring `EvmCache::call_raw_with_access_list`: run non-committing, extract touched
accounts/slots from the journaled state before reverting. This is the per-sim read
set the reconcile step needs.

## 7. `FreshnessController` + the optimistic loop

```rust
pub struct FreshnessController<P: FreshnessPolicy, C: FreshnessClock> {
    registry: FreshnessRegistry,
    tracker: Arc<Mutex<SlotObservationTracker>>,
    policy: P,
    clock: C,
    pending: Arc<Mutex<Vec<SlotChange>>>,   // corrections flowing back from bg tasks
}
```

Adaptive thresholds (`FreshnessParams`) are **not** a controller field — they
live on the policy that consumes them (`ObservationDriven { params }`), so the
controller never carries an unused copy.

`run(&mut self, cache: &mut EvmCache, requests: Vec<SimRequest>) -> Result<SpeculativeSim>`
(main thread):
1. **Drain `pending`** into `cache.inject_storage_batch(...)` (apply corrections from
   prior cycles before snapshotting).
2. `let snapshot = cache.create_snapshot();` and grab
   `let fetcher = cache.storage_batch_fetcher().cloned();` (the Arc fetcher).
3. **Optimistic sims:** for each request, build an `EvmOverlay::new(snapshot.clone(), None)`
   and run `call_raw_with_access_list` → collect `optimistic: Vec<CallSimulationResult>`
   and per-sim actual volatile read-sets (touched slots filtered by
   `registry.is_volatile(addr, slot, now)`).
4. **Predicted candidates:** union of each request's `tx.access_list` slots filtered
   to volatile; `policy.select(candidates, &tracker.lock(), now)` → the verify set.
5. **Spawn the validator** (`tokio::spawn`) with `Send` data only: `snapshot` (Arc),
   `fetcher` (Arc), the requests, the per-sim read-sets, a `registry.clone()`, the
   `tracker` (Arc<Mutex>), the `pending` (Arc<Mutex>), `now`. Return `SpeculativeSim`
   immediately.

**Background validator** (must touch **no** `!Send` state — only the Arc/Send data):
1. `verify` = the policy-selected set ∪ (each sim's actual volatile read-set). Call
   the `fetcher` for those slots; compare each to the snapshot's value
   (`snapshot` exposes its slot values — add a crate-internal accessor if needed).
2. `observe` every checked slot into the `tracker` (lock); collect `changed: Vec<SlotChange>`.
3. If `changed` empty → `Validation::Confirmed`.
4. Else: push `changed` into `pending` (flow-back); build corrected overlays
   (`EvmOverlay::new(snapshot.clone(), None)` then write the fresh values into the
   overlay via a dirty-layer override — add an `EvmOverlay::override_slot(addr,slot,value)`
   if needed); re-run **only** the requests whose read-set intersects `changed`;
   return `Validation::Corrected { results, changed }` (results = optimistic with the
   re-run ones replaced).
5. On fetcher error → `Validation::Unverified { reason }` (do not trust silently).

`on_new_block(&mut self, block: u64)`: advance the clock via
`FreshnessClock::advance(block)` (a no-op for `WallClock`, a `set_block` for
`BlockClock`), then `policy.on_new_block(block)`. Advancing the clock ages
`ValidThrough` slots into `Volatile` and progresses the reuse window through the
natural API — callers do not bump a `BlockClock` separately.

**Concurrency notes:** `tracker` and `pending` are `Arc<Mutex<…>>` so the background
task updates them safely; the live `EvmCache` is never shared across threads.
`run` requires a multi-thread tokio runtime (document it; mirror the Phase-1
constructor note). The `fetcher` is synchronous (block_in_place internally) and is
fine to call from the spawned task.

## 8. Tests (offline, no network)

All via a **stubbed** `StorageBatchFetchFn` (`set_storage_batch_fetcher`) returning
chosen "current" values; build the cache over the mocked provider (see
`tests/common`/`examples/support` patterns). Cover:

- `FreshnessRegistry`: resolution order (slot ▸ account ▸ default); `is_volatile`
  for each variant incl. `ValidThrough` boundary at `now == m` vs `now > m`;
  `with_default` non-default.
- `SlotObservationTracker` (clock-agnostic): `observe` change detection with explicit
  `now`; `should_refetch` for unknown / insufficient / never-changed / always-changed
  with a `FreshnessParams`; existing tests updated to the new signatures.
- Each policy's `select`: `AlwaysVerify` (all), `NeverVerify` (none),
  `ObservationDriven` (only `should_refetch` slots).
- `EvmCache::verify_slots` against a stub fetcher: changed vs unchanged; assert it
  injects fresh values and returns the right `SlotChange`s.
- `EvmCache::purge_account`: account + storage gone from both layers.
- `EvmOverlay::call_raw_with_access_list`: returns the touched slots/accounts.
- **The full loop** (`FreshnessController::run` on a multi-thread test runtime,
  stub fetcher): (a) **match path** — fetcher returns unchanged values →
  `Validation::Confirmed`, optimistic == nothing re-run; (b) **mismatch path** —
  fetcher returns a changed value for a slot a sim read → `Validation::Corrected`
  with corrected results differing from optimistic, and only the affected sim re-run;
  (c) `optimistic()` is readable before `validate()`; (d) `pending` drained on the
  next `run`; (e) `Unverified` when the stub returns an error.
- `BlockClock` vs `WallClock` selection behavior.

Put unit tests in-module (`#[cfg(test)]`) and the loop/integration tests in
`tests/freshness.rs` (shared `tests/common` helpers; add a stub-fetcher helper).

## 9. Docs & example

- Rustdoc on every public item (CI runs `-D warnings`; there is no `missing_docs`
  gate, but document thoroughly anyway).
- A module-level `//!` doc on `freshness.rs` explaining the four layers + the
  optimistic/deferred-validation model, with a short runnable doctest for the
  registry + policy (no network).
- An offline example `examples/freshness_optimistic.rs` (using `examples/support`)
  that: builds a cache, registers a pinned + a volatile slot, runs a `SimRequest`
  through a `FreshnessController` with a **stub fetcher** that reports one slot
  changed, and prints the `optimistic()` result then the `Validation` (showing a
  `Corrected`). Add it to the README example table.

## 10. Build order (commit per step, green each time)

1. Clock-agnostic `SlotObservationTracker` + `FreshnessParams` (update its tests).
2. `Validity` + `FreshnessRegistry` + `FreshnessClock`/`BlockClock`/`WallClock` +
   `FreshnessPolicy` + built-ins (with unit tests).
3. `EvmCache::verify_slots` + `purge_account` + `set_storage_batch_fetcher`;
   `EvmOverlay::call_raw_with_access_list` (with tests).
4. `FreshnessController` + `SpeculativeSim`/`Validation` optimistic loop (with the
   full-loop tests).
5. Docs + example + README + lib re-exports; update `docs/ROADMAP.md` Phase 2 status
   to "Done".

## 11. Final acceptance

Both feature configs green (§0). All new + existing tests pass. The example runs
offline and demonstrates a `Corrected` validation. Report: what landed per file,
the public API added, test coverage, and the verification output.
