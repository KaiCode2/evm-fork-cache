# Phase 6 — bundle simulation, coinbase accounting, call tracing (spec)

> Status: implementation spec. Two parallel implementation tracks (A+B and C),
> each developed in an isolated worktree, integrated and reviewed by the manager.
> 0.2 feature work; separate from the release-readiness line.

## Goal

Lift the engine from an isolated single-call evaluator to an MEV-bundle simulator:
apply an **ordered sequence of transactions over cumulative block state**, account
for **miner (coinbase) payment**, and expose a **call-frame tracer**. All three are
additive — they build on machinery that already exists:

- Cumulative commit: `EvmOverlay` `commit=true` + per-call `journaled_state.checkpoint()`
  (`src/cache/overlay.rs`). Chaining committing calls on one overlay yields cumulative state.
- Inspector seam: `build_evm_with_inspector_local<INSP>(…)` → `evm.inspect_one_tx(tx)`
  is generic over any revm `Inspector` (`src/cache/overlay.rs:320`). `TransferInspector`
  (`src/inspector.rs`) is the working model.
- Coinbase: `evm.block.beneficiary = coinbase` is already set from the snapshot
  (`src/cache/overlay.rs:355`); the beneficiary accrues payment during execution.

Reuse: `TxConfig` (value/gas_limit/gas_price/nonce/access_list), `SimError` /
`SimulationResult`, the checkpoint/commit path, and the `TransferInspector` pattern.

## Non-goals (out of scope for Phase 6)

- Full block building (mempool, tx ordering/auction). A "bundle" is an ordered tx
  sequence on cumulative state — nothing more.
- `Create`-kind bundle txs (Call only for now; note as a follow-up).
- Opcode/step-level tracing, SLOAD/SSTORE capture, per-frame gas attribution beyond
  total gas. The tracer ships call frames only.
- Builder-style state-override bundles.

The crate must remain **protocol-neutral**: no AMM/protocol-specific logic in `src/`.

---

## Track A + B — bundle execution + coinbase accounting

### Public API (new module `src/bundle.rs`, re-exported at crate root)

```rust
/// One transaction in a bundle. `Call`-kind only for Phase 6.
#[derive(Clone, Debug)]
pub struct BundleTx {
    pub from: Address,
    pub to: Address,
    pub calldata: Bytes,
    pub tx: TxConfig,            // value/gas_limit/gas_price/nonce/access_list (reused)
}
impl BundleTx {
    pub fn new(from: Address, to: Address, calldata: Bytes) -> Self;          // tx = default
    pub fn with_config(from: Address, to: Address, calldata: Bytes, tx: TxConfig) -> Self;
}

/// How miner payment is computed from the beneficiary balance delta.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum GasAccounting {
    /// Base-fee-aware (honest profit): payment = beneficiary_delta − Σ(gas_usedᵢ × basefee).
    /// Mirrors what a builder actually nets (base fee is burned on mainnet).
    #[default]
    Mainnet,
    /// Raw beneficiary balance delta (base fee NOT subtracted). For diagnostics.
    Raw,
}

/// What happens when a bundle tx reverts.
#[derive(Clone, Debug)]
pub enum RevertPolicy {
    /// Any tx revert/halt reverts the WHOLE bundle and sets `succeeded = false`.
    Atomic,
    /// The listed tx indices may revert without aborting the bundle; their state
    /// effects are rolled back individually, later txs still execute.
    AllowReverts(Vec<usize>),
}
impl Default for RevertPolicy { fn default() -> Self { RevertPolicy::Atomic } }

#[derive(Clone, Debug, Default)]
pub struct BundleOptions {
    pub revert_policy: RevertPolicy,   // default Atomic
    pub gas_accounting: GasAccounting, // default Mainnet
    pub commit: bool,                  // default false (evaluate, don't persist)
}

#[derive(Clone, Debug)]
pub struct TxOutcome {
    pub result: ExecutionResult,   // revm enum (Success/Revert/Halt)
    pub gas_used: u64,
    pub reverted: bool,            // true if Revert or Halt
    pub logs: Vec<Log>,
}

#[derive(Clone, Debug)]
pub struct BundleResult {
    pub per_tx: Vec<TxOutcome>,    // one per executed tx (length == txs.len() unless Atomic aborted early)
    pub coinbase_payment: U256,    // per gas_accounting; saturating
    pub gas_used: u64,             // total across executed txs
    pub succeeded: bool,           // false iff Atomic aborted on a revert
}

impl EvmOverlay {
    /// Apply `txs` in order against this overlay with cumulative state.
    pub fn simulate_bundle(&mut self, txs: &[BundleTx], opts: &BundleOptions)
        -> SimulationResult<BundleResult>;
}
impl EvmCache {
    /// Convenience: snapshot self, run the bundle on a fresh overlay.
    /// (Does not mutate the cache even when `opts.commit` is true — commit applies
    /// to the transient overlay; document this clearly.)
    pub fn simulate_bundle(&mut self, txs: &[BundleTx], opts: &BundleOptions)
        -> SimulationResult<BundleResult>;
}
```

### Behavioral requirements

1. **Ordered cumulative state.** Tx `i` observes the committed writes of txs `0..i`.
   Implement with one outer bundle checkpoint and one inner checkpoint per tx, on a
   single overlay/EVM (do NOT rebuild a fresh overlay per tx).
2. **Revert policy.**
   - `Atomic`: on the first tx that reverts/halts, revert the whole bundle to the
     outer checkpoint, set `succeeded = false`, and stop (per_tx ends at the failing
     tx, whose `reverted = true`). `coinbase_payment = 0` and state is unchanged.
   - `AllowReverts(idxs)`: a revert at an index in `idxs` rolls back only that tx
     (inner checkpoint revert), records `reverted = true`, and continues; a revert at
     an index NOT in `idxs` behaves like `Atomic`. `succeeded = true` if the bundle
     ran to completion.
3. **Coinbase / payment accounting.** Capture `beneficiary` balance before the bundle
   and after the last committed tx. `Raw` returns the delta. `Mainnet` returns
   `delta.saturating_sub(Σ gas_usedᵢ × basefee)`, where `basefee` is the snapshot/overlay
   block base fee (0 if unset). This subtracts the burned base fee so the figure is the
   honest priority-fee + direct-coinbase-transfer payment. Use saturating arithmetic.
4. **Base-fee control for honest accounting + testability.** Add a way to set the block
   base fee on the cache/overlay (e.g. `EvmCacheBuilder::basefee(U256)` /
   `EvmCache::set_basefee(U256)` propagated into `EvmSnapshot`), since offline caches
   have no fetched header. Required so Mainnet accounting is exercisable.
5. **Commit semantics.** `commit = true` folds the bundle's cumulative state into the
   overlay's dirty layer (and is observable by subsequent overlay calls). `commit = false`
   reverts the outer checkpoint so the overlay is unchanged. `EvmCache::simulate_bundle`
   always runs on a transient overlay (cache is not mutated).
6. **Isolation / safety.** A failed bundle (Atomic abort) never leaves partial state.
   Errors (tx-env build failure, transact DB error) return `SimError`, not panic.

### Acceptance criteria (Track A+B)

- AB1 cumulative state: a 2-transfer bundle nets correctly; tx 2 sees tx 1's write.
- AB2 atomic revert: a bundle whose 2nd tx reverts → `succeeded == false`, owner state
  unchanged from pre-bundle, `per_tx[1].reverted == true`.
- AB3 allow-reverts: same bundle with `AllowReverts([1])` → `succeeded == true`,
  tx 0's effect persists, tx 1 rolled back.
- AB4 direct coinbase payment: a tx sending `value` to the beneficiary with `gas_price = 0`
  yields `coinbase_payment == value` (both Raw and Mainnet, since gas credit is 0).
- AB5 mainnet vs raw gas: with a set base fee and a tx priced at `gas_price ≥ basefee`,
  `Mainnet == Raw − gas_used×basefee` and `Mainnet < Raw`.
- AB6 commit: `commit=true` on an overlay persists cumulative state to the next call;
  `commit=false` leaves it isolated.

---

## Track C — call tracer + generalized inspector seam

### Public API (new module `src/tracing.rs`, re-exported at crate root)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind { Call, StaticCall, DelegateCall, CallCode, Create, Create2 }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallStatus { Success, Revert, Halt }

#[derive(Clone, Debug)]
pub struct CallTrace {
    pub kind: CallKind,
    pub from: Address,
    pub to: Address,            // created address for Create*
    pub value: U256,
    pub input: Bytes,
    pub gas_used: u64,
    pub output: Bytes,
    pub status: CallStatus,
    pub depth: usize,
    pub subcalls: Vec<CallTrace>,
}

/// revm Inspector that builds a call-frame tree.
#[derive(Clone, Debug, Default)]
pub struct CallTracer { /* frame stack + completed root */ }
impl CallTracer {
    pub fn new() -> Self;
    /// The root frame after a transact (None if nothing executed).
    pub fn into_trace(self) -> Option<CallTrace>;
    pub fn root(&self) -> Option<&CallTrace>;
}
impl<CTX, INTR> revm::Inspector<CTX, INTR> for CallTracer where INTR: InterpreterTypes { … }
```

### Generalized inspector seam

`build_evm_with_inspector_local<INSP>` is already generic. Expose a **public**,
inspector-generic simulate entry on `EvmOverlay` so callers can attach a `CallTracer`
(or any `Inspector`) instead of being limited to the hardcoded `TransferInspector`:

```rust
impl EvmOverlay {
    /// Run a single call with a caller-supplied inspector; returns the raw
    /// ExecutionResult and hands the inspector back for the caller to read.
    pub fn call_raw_with_inspector<I>(&mut self, from: Address, to: Address,
        calldata: Bytes, tx: &TxConfig, inspector: I, commit: bool)
        -> SimulationResult<(ExecutionResult, I)>
    where I: Inspector<…>;
}
```

Plus a small composing inspector so a tracer AND transfer tracking can run together:

```rust
/// Runs two inspectors over the same execution.
pub struct InspectorStack<A, B>(pub A, pub B);
impl<A,B,CTX,INTR> Inspector<CTX,INTR> for InspectorStack<A,B> where … { /* fan out every hook */ }
```

### Behavioral requirements

1. The tracer captures the full call-frame tree: top-level call + nested CALL/STATICCALL/
   DELEGATECALL/CREATE frames, each with from/to/value/input/output/gas_used/status/depth/subcalls.
2. Revert attribution: a frame that reverts has `status == Revert` (Halt → `Halt`); its
   parent records it as a subcall regardless.
3. `InspectorStack` fans out every `Inspector` hook to both inner inspectors so a
   `CallTracer` + `TransferInspector` produce their independent results in one pass.
4. `call_raw_with_inspector` honors `commit` exactly like `simulate_with_transfer_tracking`.

### Acceptance criteria (Track C)

- C1 single frame: a top-level call to a contract yields a root `CallTrace` with the
  right from/to/input and `status == Success`.
- C2 nested calls: a contract that calls another contract yields a root with a `subcalls`
  entry for the inner call (correct depth + to-address).
- C3 revert attribution: a call into a reverting contract yields a frame with
  `status == Revert`.
- C4 composition: `InspectorStack<CallTracer, TransferInspector>` over a token transfer
  yields BOTH a non-empty trace AND the expected `TokenTransfer`.

---

## Integration (manager-owned)

- A+B owns `src/bundle.rs` + `EvmOverlay::simulate_bundle` (one new method in `overlay.rs`)
  + `EvmCache::simulate_bundle` + base-fee setter + crate-root re-exports.
- C owns `src/tracing.rs` + `EvmOverlay::call_raw_with_inspector` (one new method in
  `overlay.rs`) + `InspectorStack` + crate-root re-exports.
- Shared edit surfaces: `overlay.rs` (each adds ONE localized method — keep them adjacent
  to the existing `simulate_with_transfer_tracking` to ease the merge), `lib.rs` (each adds
  its re-export block), `CHANGELOG.md`. The manager resolves these additive conflicts.

## Verification

`cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
`cargo test --all-features`, doctests, `RUSTDOCFLAGS=-D warnings cargo doc --no-deps`.
Manager-authored acceptance tests: `tests/bundle_simulation.rs` (A+B),
`tests/call_tracer.rs` (C). No new production dependencies. Offline only (mocked provider).
