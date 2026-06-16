# Phase 4 implementation spec — event pipeline + adapters (Pillar B.2)

Implementation contract for the **reader half** of Pillar B: turn an on-chain
`Log` into the Phase 3 [`StateUpdate`] vocabulary, apply it through
`apply_updates`, and keep the cache **reactively fresh** from the event stream —
with reconciliation, reorg handling, and freshness wiring. Read this **with**
[`ROADMAP.md`](ROADMAP.md) (the "Phase 4" row, the "Pillar B — event → state
pipeline" section, and the "Hard problems to resolve" list) and
[`phase-3-spec.md`](phase-3-spec.md) (the writer half this builds on). This
document is the precise build contract; where they overlap, prefer this.

Phase 3 built the writer half (`StateUpdate` + `apply_updates` with cold-aware
`SlotDelta`/`BalanceDelta` RMW and `account_state`-correct reads). Phase 4 builds
the decoder, the protocol adapters, and the orchestration that drives them.

## 0. Ground rules (non-negotiable)

- **Branch:** create `phase-4-event-pipeline` off the current
  `phase-3-state-updates` HEAD. Commit there in logical steps. Do **not** push,
  do **not** tag, do **not** open a PR (the overseer does that). Commits must be
  **unsigned**: `git -c commit.gpgsign=false commit …` (the 1Password signing
  agent is unavailable here). End every commit message with exactly:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- **Generic core vs `protocols`.** The pipeline, the `EventDecoder`/`StateView`
  traits, the `DecoderRegistry`, the ERC-20 decoder, and the `SlotMasked`
  vocabulary addition are **generic core** — they must compile and lint with
  `--no-default-features`. Only the UniswapV3 adapter (`uniswap_v3`) is gated
  behind the `protocols` feature.
- **Green bar at every commit, both feature configs:**
  - `cargo fmt --all --check`
  - `cargo clippy --all-targets --no-deps -- -D warnings`
  - `cargo clippy --lib --no-default-features --no-deps -- -D warnings`
  - `cargo test`
  - `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
  - `cargo bench --no-run` (all benches build offline)
- MSRV is 1.88 — no newer-than-1.88 std APIs. Edition 2024.
- **Do not break existing behavior or any existing test.** The Phase 3 surface
  (`StateUpdate`, `apply_updates`, `modify_slot`, `StateDiff`) keeps its shape;
  Phase 4 *adds* the `SlotMasked` variant and the `skipped_masks` diff field
  under the pre-1.0 break policy (both already-`#[non_exhaustive]` types).
- **No new dependencies.** `alloy-primitives` (with `Log`), `alloy-sol-types`
  (`sol!` / `SolEvent`), `alloy-provider`, `futures`, and `tokio` are already
  present. Decode logs with `sol!`-generated event types + `SolEvent`, not
  hand-rolled byte slicing.

## 1. Objective & scope

Today the crate can *write* targeted state but has no way to *derive* those
writes from chain activity: a caller must hand it concrete `StateUpdate`s. Phase
4 closes the loop — decode a `Log` into `StateUpdate`s, apply them, and run the
reactive maintenance (reconcile, reorg) that keeps event-derived state honest.

**In scope:**

1. **`StateUpdate::SlotMasked`** — a cold-aware read-modify-write *masked* slot
   write (`(old & !mask) | (value & mask)`), so a pure decoder can express a
   partial update to a **packed** storage word (e.g. V3 `slot0`) without knowing
   or clobbering the bits it does not own. Generic core (§4.1).
2. **`EventDecoder` + `StateView`** — the decoder trait (`Log` + read-only
   pre-state view → `Vec<StateUpdate>`) and the narrow read-only cache view it is
   handed. `EvmCache` implements `StateView`. Generic core (§4.2).
3. **`DecoderRegistry`** — dispatches a log to the decoder(s) registered for its
   emitting address (and/or topic0) and concatenates their output. Generic core
   (§4.3).
4. **`Erc20TransferDecoder`** — generic ERC-20 `Transfer` → relative balance
   `SlotDelta`s (the §15 reactive-balance case, now log-driven). Generic core
   (§5).
5. **`UniswapV3Decoder`** — `protocols`-gated adapter: `Swap` → `slot0`
   (masked sqrtPriceX96 + tick) + `liquidity`; `Mint`/`Burn` → per-tick
   `liquidityGross`/`liquidityNet`, the `initialized` flag, `tickBitmap` word
   flips, and the global `liquidity` (conditional on the current tick). Computed
   against the `StateView` (tick maintenance is inherently RMW). (§6).
6. **`EventPipeline`** — the orchestration: `ingest_logs` (decode+apply
   **log-by-log**, in order, recording touched state for reorg tracking),
   `reorg_to` (purge-and-resync addresses touched after the new head), and
   `reconcile` (sampled RPC re-read via `verify_slots`: correct **and** alarm).
   Generic core (§7).
7. **Freshness wiring** — `BlockDigest` surfaces the touched `(address, slot)`
   set so a caller can classify event-derived slots (`valid_through` / `pin`) and
   call `FreshnessController::on_new_block`. No controller internals change (§8).
8. Offline example, benchmark, docs, CHANGELOG, ROADMAP → Done (§11).

**Out of scope (document as follow-ups; do not build):**
- **A concrete WS transport / live subscription loop.** The async `drive`
  convenience (§7.5) is generic over a log source and is exercised only by the
  offline example feeding a vec-backed source; a production WS/`subscribe_logs`
  adapter is a follow-up. The *tested* surface is the synchronous core.
- **V3 fee-growth / oracle observation maintenance.** Event-derived tick init
  does **not** reconstruct `feeGrowthOutside0/1X128` (slots +1/+2) or oracle
  observations — those are not derivable from `Mint`/`Burn`/`Swap`. Swap
  *price/liquidity quoting* is unaffected; fee-accounting reads are not
  maintained. Document as a KNOWN_ISSUE with reconcile/purge as the backstop
  (§6.4).
- **Non-Uniswap-layout V3 (Slipstream slot0).** The adapter assumes the
  Uniswap/Pancake `slot0` bit layout (only base slots differ). Slipstream's
  different `slot0` packing is a follow-up.
- **COW snapshots** (Phase 5). The pipeline mutates the existing `EvmCache`
  layers via `apply_updates`.

## 2. Reuse these existing pieces (do not reinvent)

- **`StateUpdate` / `apply_update` / `apply_updates` / `modify_slot`**
  (`state_update.rs`, `cache/mod.rs`) — the write half. The pipeline applies
  decoded updates through `apply_updates`; the `SlotMasked` handler reuses the
  private `write_slot_through` and the `account_state`-aware
  `cached_storage_value` (§16.0).
- **`EvmCache::cached_storage_value`** — the `StateView::storage`
  implementation (overlay ▸ backend ▸ `None`, `account_state`-correct).
- **`EvmCache::verify_slots`** (`cache/mod.rs`) — the synchronous
  fetch-compare-inject reconciliation primitive. `reconcile` is a thin wrapper:
  it samples event-derived slots and calls `verify_slots`; the returned
  `Vec<SlotChange>` is the drift report (verify_slots already injected the fresh
  chain values — correct + alarm).
- **`EvmCache::purge_account` / `apply_update(Purge { … })`** — the reorg
  purge mechanism. `reorg_to` purges touched addresses through `apply_updates`
  of `Purge` updates so the next read re-fetches.
- **`inspector::TransferInspector::parse_transfer`** + the
  `TRANSFER_EVENT_SIGNATURE` constant (`src/inspector.rs`) — reuse for the
  ERC-20 decoder's signature match and topic decoding (or reuse the same
  `sol!` event). Do not redefine the signature constant.
- **`cache::storage_keys`** (`protocols`) — `V3_*`/`PANCAKE_V3_*` slot
  constants, `v3_tick_info_storage_keys_with_base`,
  `v3_tick_bitmap_storage_key_with_base`, `i256_from_i24`, `i128_to_u256`. The
  V3 adapter reuses these for slot derivation and packing.
- **`freshness::{FreshnessController, FreshnessRegistry, Validity, SlotChange}`**
  — the freshness wiring target. `SlotChange` is the reconcile-report element.
- **`alloy_sol_types::{sol, SolEvent}`** — generate `Swap`/`Mint`/`Burn` and
  ERC-20 `Transfer` event types and decode with `SolEvent::decode_log_data`.
- **The offline harness** — `examples/support/mock.rs` (`offline_cache`,
  `install_mock_erc20`, `MockERC20`, `MOCK_ERC20_BALANCE_SLOT`),
  `tests/common`. The new tests/example build the cache over the mocked provider
  and never touch the network.

## 3. Module layout

A new `src/events/` directory module (the crate's only other dir module is
`cache/`):

- **`src/events/mod.rs`** (generic core): `EventDecoder`, `StateView`,
  `DecoderRegistry`, `EventPipeline`, `BlockDigest`, `ReconcileReport`,
  `ReorgConfig`, and the async `drive` convenience + its `LogSource` trait. The
  module `//!` doc frames Pillar B.2 and the `!Send`-cache discipline.
- **`src/events/erc20.rs`** (generic core): `Erc20TransferDecoder` + config.
- **`src/events/uniswap_v3.rs`** (`#[cfg(feature = "protocols")]`):
  `UniswapV3Decoder` + `UniswapV3Layout` config (base slots + tick spacing).
- **`src/state_update.rs`**: add the `SlotMasked` variant, the `slot_masked`
  constructor, `SkippedMask`, and the `StateDiff.skipped_masks` field +
  `merge`/`has_skipped`/`skipped_len` updates.
- **`src/cache/mod.rs`**: the `SlotMasked` apply arm (reusing `write_slot_through`
  + the cold-aware read); `impl events::StateView for EvmCache`.
- **`src/lib.rs`**: `pub mod events;` + re-exports (§9).

## 4. Core types & behavior

### 4.1 `StateUpdate::SlotMasked` — cold-aware masked write (generic core)

```rust
pub enum StateUpdate {
    Slot { address, slot, value },
    SlotDelta { address, slot, delta },
    /// Set only the `mask` bits of a storage slot to the corresponding bits of
    /// `value`, preserving the rest: `new = (old & !mask) | (value & mask)`.
    /// Read-modify-write, **cold-aware** — a masked write to a slot absent from
    /// both layers is not applied (the un-masked bits are unknown); it is
    /// surfaced in [`StateDiff::skipped_masks`].
    SlotMasked { address: Address, slot: U256, mask: U256, value: U256 },   // NEW
    BalanceDelta { address, delta },
    Account { address, patch },
    Purge { address, scope },
}
impl StateUpdate {
    pub fn slot_masked(address: Address, slot: U256, mask: U256, value: U256) -> Self;
}

/// A masked write ([`StateUpdate::SlotMasked`]) skipped because the target slot
/// was cold (un-masked bits unknown). Fetch+seed the slot, then retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SkippedMask { pub address: Address, pub slot: U256, pub mask: U256, pub value: U256 }

pub struct StateDiff {
    pub slots: Vec<SlotChange>,
    pub accounts: Vec<AccountChange>,
    pub purged: Vec<PurgeRecord>,
    pub skipped: Vec<SkippedDelta>,
    pub skipped_balances: Vec<SkippedBalanceDelta>,
    pub skipped_masks: Vec<SkippedMask>,   // NEW
}
```

`SkippedMask` is a leaf record constructed as a struct literal in equality
assertions, so — like `SkippedDelta`/`SkippedBalanceDelta` (§16.4) — it is
**not** `#[non_exhaustive]`. `StateUpdate` and `StateDiff` already are.

Apply behavior (`cache/mod.rs`, mirrors the `SlotDelta` arm):
- Read `old = cached_storage_value(address, slot)` (cold-aware, §16.0).
- If `Some(old)`: `new = (old & !mask) | (value & mask)`; write through both
  layers via `write_slot_through`; push a `SlotChange { old, new }` iff
  `old != new`.
- If `None` (cold): push `SkippedMask` to `diff.skipped_masks`, write nothing.

`StateDiff::merge` extends `skipped_masks`. `has_skipped` /`skipped_len`
include `skipped_masks`. `is_empty`/`len` stay **changes-only** (a skip is not a
change). serde derives on `SkippedMask`. Module `//!` doc gains a `SlotMasked`
paragraph (packed-word updates, cold-aware).

> A masked write with `mask == U256::MAX` equals an absolute `Slot` write but
> **stays cold-skip** (a cold full-mask write is still skipped, unlike `Slot`
> which writes unconditionally). Decoders that want an unconditional absolute
> write use `Slot`; those that must preserve neighbouring bits use `SlotMasked`.

### 4.2 `EventDecoder` + `StateView`

```rust
/// Read-only view of current cached state handed to a decoder.
///
/// Decoders that compute post-state from pre-state (e.g. V3 tick maintenance)
/// read through this; stateless decoders (ERC-20 `Transfer`, V3 `Swap`) ignore
/// it. The view never touches RPC — a slot absent from the cache reads `None`.
pub trait StateView {
    /// Current cached value of `(address, slot)` (overlay ▸ backend ▸ `None`),
    /// matching what the EVM would `SLOAD` (`account_state`-aware).
    fn storage(&self, address: Address, slot: U256) -> Option<U256>;
}

/// Decode one log into zero or more targeted [`StateUpdate`]s.
///
/// `decode` is a pure function of `(log, pre-state)`: it performs no I/O and
/// emits data (the updates are serializable and replayable against matching
/// pre-state). The pipeline applies the result through `apply_updates`.
pub trait EventDecoder: Send + Sync {
    fn decode(&self, log: &Log, view: &dyn StateView) -> Vec<StateUpdate>;
}
```

`EvmCache` implements `StateView` via `cached_storage_value`. Rationale for the
`StateView` parameter (a refinement of the ROADMAP's `fn decode(&self, log) ->
Vec<StateUpdate>` sketch): event-driven sync is fundamentally *events +
pre-state → post-state*. Most updates are expressible without pre-state
(`SlotDelta` and `SlotMasked` are RMW **at apply time**), but V3 tick
maintenance must read current `liquidityGross`/`liquidityNet`/`tick`/bitmap to
compute the next packed value, and a pure decoder cannot. Handing decoders a
narrow read-only view keeps the output as serializable `StateUpdate` data while
making the stateful adapters expressible and offline-testable (feed a stub
view).

### 4.3 `DecoderRegistry`

```rust
#[derive(Default)]
pub struct DecoderRegistry { /* global decoders + per-address decoders */ }

impl DecoderRegistry {
    pub fn new() -> Self;
    /// Register a decoder consulted for **every** log.
    pub fn register(&mut self, decoder: Arc<dyn EventDecoder>) -> &mut Self;
    /// Register a decoder consulted only for logs emitted by `address`.
    pub fn register_for_address(&mut self, address: Address, decoder: Arc<dyn EventDecoder>) -> &mut Self;
    /// Decode `log` through every applicable decoder, concatenating the results
    /// (address-scoped decoders first, then global), preserving order.
    pub fn decode(&self, log: &Log, view: &dyn StateView) -> Vec<StateUpdate>;
}
```

Dispatch is by emitting address (`log.address`); topic0 filtering is the
decoder's own concern (each decoder returns `vec![]` for a log it does not
recognise). Keep it simple: address-scoped entries + a global list, both
consulted, output concatenated.

## 5. `Erc20TransferDecoder` (generic core, `events/erc20.rs`)

```rust
pub struct Erc20TransferDecoder {
    /// Balance mapping slot per token (the `balanceOf` mapping's base slot).
    balance_slots: HashMap<Address, U256>,
    /// Fallback balance slot for tokens not in the map.
    default_balance_slot: U256,
}
impl Erc20TransferDecoder {
    pub fn new(default_balance_slot: U256) -> Self;
    pub fn with_token(mut self, token: Address, balance_slot: U256) -> Self;
}
impl EventDecoder for Erc20TransferDecoder { /* … */ }
```

Decode rule for a `Transfer(from, to, value)` log (signature match via
`TRANSFER_EVENT_SIGNATURE`; topics/data decoded like `parse_transfer`):
- `slot = balance_slots.get(token).copied().unwrap_or(default_balance_slot)`.
- `balance_key(owner) = U256::from(keccak256(abi_encode((owner, slot))))`.
- Emit, **skipping the zero-address leg** (mint = `from == 0`, burn = `to == 0`):
  - if `from != Address::ZERO`: `SlotDelta::Sub(value)` on `balance_key(from)`.
  - if `to != Address::ZERO`: `SlotDelta::Add(value)` on `balance_key(to)`.
- A non-`Transfer` log (wrong topic0, < 3 topics, < 32 data bytes) → `vec![]`.

Cold balances follow the Phase 3 contract: the `SlotDelta` is skipped and
surfaced in `StateDiff.skipped` (the caller seeds the balance, or the next read
lazily fetches it). The decoder ignores the `StateView`. `value == 0` transfers
emit deltas of zero (a no-op at apply — empty diff); that is acceptable.

## 6. `UniswapV3Decoder` (`protocols`, `events/uniswap_v3.rs`)

```rust
#[derive(Clone, Debug)]
pub struct UniswapV3Layout {
    pub slot0_slot: U256,          // V3_SLOT0_SLOT (0) — Uniswap/Pancake
    pub liquidity_slot: U256,      // V3_LIQUIDITY_SLOT (4) / PANCAKE (5)
    pub ticks_base_slot: U256,     // V3_TICKS_BASE_SLOT (5) / PANCAKE (6)
    pub tick_bitmap_base_slot: U256, // V3_TICK_BITMAP_BASE_SLOT (6) / PANCAKE (7)
    pub tick_spacing: i32,         // pool tickSpacing (for bitmap word/bit)
}
impl UniswapV3Layout {
    pub fn uniswap(tick_spacing: i32) -> Self;   // canonical Uniswap V3 slots
    pub fn pancake(tick_spacing: i32) -> Self;   // PancakeSwap V3 slots
}

pub struct UniswapV3Decoder {
    /// Per-pool layout (slot bases + tick spacing). A log from an unregistered
    /// pool decodes to nothing.
    pools: HashMap<Address, UniswapV3Layout>,
}
impl UniswapV3Decoder {
    pub fn new() -> Self;
    pub fn with_pool(mut self, pool: Address, layout: UniswapV3Layout) -> Self;
}
impl EventDecoder for UniswapV3Decoder { /* … */ }
```

`tick_spacing` is required for `Mint`/`Burn` bitmap maintenance: the tickBitmap
is keyed by the **compressed** tick `tick / tick_spacing`. A log from a pool not
in `pools` → `vec![]`. Match events by topic0 (`Swap`/`Mint`/`Burn` signature
hashes from `sol!`); decode with `SolEvent`.

### 6.1 `Swap` → price + liquidity (stateless)

`Swap(sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick)`:
- **slot0** (`SlotMasked`, preserves observation/feeProtocol/`unlocked` bits):
  - `mask = (U256::from(1) << 184) - 1` (low 184 bits = sqrtPriceX96 [0,160) +
    tick [160,184)).
  - `value = U256::from(sqrtPriceX96) | (tick_24bit << 160)` where `tick_24bit`
    is the int24 two's-complement low-24-bits of `tick`
    (`U256::from(tick as i32 as u32 & 0x00FF_FFFF)`).
  - Emit `StateUpdate::slot_masked(pool, slot0_slot, mask, value)`.
- **liquidity** (absolute — the event carries the post-swap pool liquidity):
  - `StateUpdate::slot(pool, liquidity_slot, U256::from(liquidity))`.

The `unlocked` bit (bit 240) and observation/fee bits are **preserved** by the
mask — clobbering `unlocked` to 0 would make a subsequent quote/swap revert
`LOK`. This is the headline correctness reason for `SlotMasked`. Stateless
(ignores the view); a cold slot0 → `skipped_masks` (the pool must be seeded
first).

### 6.2 `Mint` → tick + liquidity maintenance (stateful, reads `StateView`)

`Mint(sender, owner, tickLower, tickUpper, amount, amount0, amount1)` adds
`amount` (uint128 liquidity) over `[tickLower, tickUpper)`. For **each** of
`tickLower` and `tickUpper`, and for the global liquidity, compute the post-state
from the current cached value (read via `view.storage`); emit an **absolute**
`Slot` write of the recomputed word (a packed word recomputed from known
pre-state is an absolute write, not a delta). If a needed word is **cold**
(`view.storage` → `None`), **skip that update and surface it** as a
`SkippedMask`/`SkippedDelta` (choose `SkippedDelta` with a zero-amount marker is
wrong — use a dedicated skip; see §6.5) so the caller knows the pool tick state
is incomplete (re-seed via `inject_v3_ticks`).

Per tick (`tick` ∈ {`tickLower`, `tickUpper`}):
- **Tick slot +0** (`liquidityGross` [0,128) ‖ `liquidityNet` [128,256) signed):
  - base = `v3_tick_info_storage_keys_with_base(tick, ticks_base_slot)[0]`.
  - read current word; `gross = low128`, `net = high128 as i128`.
  - `gross' = gross + amount` (uint128).
  - `net' = net + amount` for `tickLower`, `net' = net - amount` for `tickUpper`
    (int128).
  - repacked = `U256::from(gross') | (i128_to_u256(net') << 128)`; emit
    `Slot(pool, base, repacked)`.
- **Tick slot +3** (`initialized` flag, byte 31 / bit 248 — matching the
  existing `inject_v3_ticks` placement): if `gross == 0 && gross' > 0`
  (tick newly initialized), set `initialized` by emitting
  `SlotMasked(pool, base+3, mask = U256::from(1) << 248, value = U256::from(1) << 248)`.
  (No change if it was already initialized.)
- **tickBitmap**: when a tick is newly initialized, flip its bit:
  - `compressed = tick / tick_spacing` (floor toward negative infinity — match
    Solidity: `tick / tickSpacing` truncates toward zero, and V3 requires
    `tick % tickSpacing == 0`, so plain integer division is exact).
  - `word_pos = (compressed >> 8) as i16`, `bit_pos = (compressed & 0xFF) as u8`.
  - key = `v3_tick_bitmap_storage_key_with_base(word_pos, tick_bitmap_base_slot)`.
  - emit `SlotMasked(pool, key, mask = U256::from(1) << bit_pos, value = U256::from(1) << bit_pos)`
    (set the bit). On Burn that uninitialises the tick, clear it (value = 0).

Global **liquidity** (slot `liquidity_slot`): the `Mint` event does **not**
carry the resulting pool liquidity, so it must be derived: read current `slot0`
→ extract `tick` (bits [160,184), sign-extended int24); if
`tickLower <= currentTick < tickUpper`, read current `liquidity` and emit
`Slot(pool, liquidity_slot, current + amount)`. If `slot0` or `liquidity` is
cold, skip+surface. (Safety net: the next `Swap` sets `liquidity` absolutely.)

### 6.3 `Burn` → the inverse

`Burn(owner, tickLower, tickUpper, amount, amount0, amount1)`: identical to
`Mint` with the signs inverted:
- `gross' = gross - amount` (uint128, saturating at 0 defensively).
- `net' = net - amount` for `tickLower`, `net' = net + amount` for `tickUpper`.
- If `gross > 0 && gross' == 0` (tick now uninitialised): clear the
  `initialized` flag (`SlotMasked` slot+3 value 0) **and** clear the bitmap bit
  (`SlotMasked` value 0).
- Global liquidity: `Slot(pool, liquidity_slot, current - amount)` if current
  tick in `[tickLower, tickUpper)`.

> A `Burn` removing all of a tick's liquidity but a same-block re-`Mint` is
> handled by the **log-by-log** apply order (§7.1): the second decode reads the
> first's applied effect through the view.

### 6.4 Known limitation (document, do not fix)

Event-derived tick maintenance does **not** set `feeGrowthOutside0/1X128`
(slots +1/+2), `secondsOutside`, or oracle observations — these are not
derivable from `Mint`/`Burn`/`Swap`. **Swap price/liquidity quoting is
unaffected** (the swap-amount math does not depend on `feeGrowthOutside`); fee
accounting and `collect` are not maintained. Record this as a `KNOWN_ISSUES.md`
entry with sampled `reconcile` + reorg `purge` as the backstop, in the project's
honest-freshness spirit.

### 6.5 Cold-skip surfacing for stateful V3 updates

When a V3 tick/liquidity update cannot be computed because a needed word is cold,
surface it so the gap is visible (never silently drop it). Reuse
`SkippedMask` for masked sub-word updates (bitmap/initialized) and, for the
absolute tick-word / liquidity writes that were skipped, push a `SkippedMask`
with `mask == U256::MAX` and `value == U256::ZERO` as the "could-not-compute"
marker, **or** (cleaner) add the skipped target to a dedicated field. **Locked
choice:** reuse `SkippedMask` with `mask == U256::MAX, value == 0` as the
cold-tick marker to avoid a fourth skip vector; document this convention on
`SkippedMask`. The pipeline's `BlockDigest.skipped` count (via
`StateDiff::skipped_len`) then includes them, and the caller re-seeds the pool.

## 7. `EventPipeline` (generic core, `events/mod.rs`)

```rust
pub struct EventPipeline {
    registry: DecoderRegistry,
    reorg: ReorgConfig,
    touched: VecDeque<(u64, Vec<Address>)>,   // ring of per-block touched addrs
    derived_slots: HashSet<(Address, U256)>,  // event-derived slots (for reconcile sampling)
}

#[derive(Clone, Debug)]
pub struct ReorgConfig {
    /// How many recent blocks of touched-address history to retain for reorg
    /// purge (the reorg horizon). Older entries are dropped.
    pub depth: usize,
    /// Purge scope used on reorg (default `AllStorage` — storage re-fetches but
    /// the account header survives; `Account` for a full drop).
    pub scope: PurgeScope,
}

pub struct BlockDigest {
    pub block: u64,
    /// Merged diff of everything applied for the block (changes-only + skips).
    pub applied: StateDiff,
    /// Number of logs that decoded to at least one update.
    pub decoded_logs: usize,
    /// The (address, slot) set written this block (for freshness classification).
    pub touched_slots: Vec<(Address, U256)>,
}

pub struct ReconcileReport {
    pub checked: usize,
    /// Slots whose event-derived value disagreed with chain truth. Non-empty =
    /// drift alarm. `verify_slots` has already injected the fresh values.
    pub mismatched: Vec<SlotChange>,
}

impl EventPipeline {
    pub fn new(registry: DecoderRegistry) -> Self;             // default ReorgConfig
    pub fn with_reorg_config(mut self, cfg: ReorgConfig) -> Self;

    /// Decode + apply a block's logs, **log-by-log in order**, recording touched
    /// state for reorg tracking. Returns the per-block digest.
    pub fn ingest_logs(&mut self, cache: &mut EvmCache, block: u64, logs: &[Log]) -> BlockDigest;

    /// Reorg to `new_head`: purge (per `ReorgConfig.scope`) every address
    /// touched in a block **>** `new_head`, drop those ring entries, and return
    /// the merged purge diff. The next read re-fetches from RPC.
    pub fn reorg_to(&mut self, cache: &mut EvmCache, new_head: u64) -> StateDiff;

    /// Sampled reconciliation: re-read `slots` via `EvmCache::verify_slots`
    /// (correct + alarm). Returns the mismatches; an empty `slots` or no fetcher
    /// surfaces as appropriate (errors if no fetcher, mirroring `verify_slots`).
    pub fn reconcile(&mut self, cache: &mut EvmCache, slots: &[(Address, U256)]) -> Result<ReconcileReport>;

    /// All event-derived slots seen so far (sampling source for `reconcile`).
    pub fn derived_slots(&self) -> impl Iterator<Item = (Address, U256)> + '_;
}
```

### 7.1 `ingest_logs` — decode + apply **log-by-log**

For each log, in order: `let updates = registry.decode(log, &*cache); let diff =
cache.apply_updates(&updates); merge into the block diff`. Apply **immediately
per log** (not decode-all-then-apply-all) so a later log's decode sees the
effects of earlier logs in the same block through the `StateView` (e.g. two
overlapping `Mint`s, or a `Burn`+`Mint` pair). Record the touched addresses
(`diff.slots` + `diff.accounts` addresses + `diff.skipped*` targets' addresses)
into the ring under `block`, and the touched `(address, slot)` into
`derived_slots`. Trim the ring to `ReorgConfig.depth`.

> `&*cache` is used as the `&dyn StateView` while `cache.apply_updates(&mut …)`
> needs `&mut` — sequence them (decode borrow ends before the apply borrow), do
> not hold both. Decode returns owned `Vec<StateUpdate>`, so there is no
> borrow overlap.

### 7.2 `reorg_to` — purge-and-resync

Collect every address in ring entries with `block > new_head`; dedupe; for each,
`apply_update(Purge { address, scope: cfg.scope })`; remove those ring entries
and their `derived_slots`. Merge the purge `StateDiff`s and return. (The caller
then re-ingests the canonical chain's logs for the reorged range, and/or the
next read lazily re-fetches.)

### 7.3 `reconcile` — correct + alarm

`let changed = cache.verify_slots(slots)?;` → `ReconcileReport { checked:
slots.len(), mismatched: changed }`. `verify_slots` already injected the fresh
chain values (correct); the returned set is the alarm. Document that a non-empty
`mismatched` means event-derived state had drifted and has now been corrected.

### 7.4 `!Send` discipline

All three methods take `&mut EvmCache` and are **synchronous** — they never
`.await`, so the `!Send` cache is never held across a yield. This is what makes
the core deterministically testable offline.

### 7.5 `drive` — async convenience (thin, example-only)

A generic `LogSource` (`async fn next_block(&mut self) -> Option<(u64, Vec<Log>, ReorgSignal)>`)
and an `async fn drive(pipeline, cache, source, hooks)` that loops: pull a block,
`reorg_to` if signalled, `ingest_logs`, invoke an optional per-block hook (where
the caller wires `FreshnessController::on_new_block` + classification). Runs on
the current task (holds the `!Send` cache across the *source* await only — the
source future is `Send`; the cache is untouched during the await). **Not**
unit-tested beyond a vec-backed `LogSource` smoke test in the example; the
synchronous core (§7.1–7.3) is the contract.

## 8. Freshness wiring (behavior-preserving)

No change to `FreshnessController` internals. The integration is demonstrated,
not hard-wired: `BlockDigest.touched_slots` lets a caller mark event-derived
slots `Pinned` or `ValidThrough(block + horizon)` in a `FreshnessRegistry` (so
the optimistic validator does not waste RPC re-verifying state the pipeline keeps
fresh), then call `controller.on_new_block(block)`. The example shows this
end-to-end. Document the recommended pattern (event-driven slots → `Pinned`,
reconciled periodically) on the `EventPipeline` type.

## 9. Public re-exports (`src/lib.rs`)

```rust
pub mod events;
pub use events::{
    BlockDigest, DecoderRegistry, EventDecoder, EventPipeline, ReconcileReport,
    ReorgConfig, StateView,
};
pub use events::erc20::Erc20TransferDecoder;
#[cfg(feature = "protocols")]
pub use events::uniswap_v3::{UniswapV3Decoder, UniswapV3Layout};
// state_update additions:
pub use state_update::{SkippedMask /* + existing */};
```

## 10. Tests (offline, no network) — the acceptance contract

Authored **before** implementation. In-module unit tests where pure; integration
tests in new `tests/event_pipeline.rs` (reuse `tests/common` + the `mock`
harness pattern). All offline.

**`state_update.rs` unit (pure):**
- `slot_masked_constructor_produces_variant`.
- `state_diff_merge_extends_skipped_masks_without_counting_it`.
- `slot_masked` serde JSON round-trip; `SkippedMask` round-trip.
- `has_skipped`/`skipped_len`/`is_fully_applied` include `skipped_masks`.

**`tests/state_update.rs` (masked apply, mocked cache):**
- `slot_masked_sets_only_masked_bits` — seed slot = `0xFFFF…FF00` (overlay),
  `SlotMasked{ mask: 0xFF, value: 0x42 }` → `0xFFFF…FF42`; other bits preserved;
  `SlotChange{old,new}` recorded.
- `slot_masked_noop_when_masked_bits_already_equal` → empty diff.
- `slot_masked_cold_slot_is_skipped_and_surfaced` → `diff.skipped_masks ==
  [SkippedMask{..}]`, slot still cold, `has_skipped()`.
- `slot_masked_writes_through_both_layers` — overlay-resident slot → both layers.
- `slot_masked_full_mask_equals_absolute_on_hot_but_skips_cold`.

**`events` unit / `tests/event_pipeline.rs` (decoders + pipeline):**

*Decoder purity & registry:*
- `decoder_registry_dispatches_by_address` — a decoder registered for token A
  fires only for A's logs; a global decoder fires for all; output concatenated
  in order.
- `unknown_log_decodes_to_empty` — non-matching topic0 → `vec![]`.

*ERC-20 (`Erc20TransferDecoder`):*
- `erc20_transfer_decodes_to_sub_and_add_deltas` — `Transfer(A,B,100)` →
  `[SlotDelta::Sub(100) @ balanceSlot(A), SlotDelta::Add(100) @ balanceSlot(B)]`
  at the configured mapping slot.
- `erc20_mint_skips_zero_from` / `erc20_burn_skips_zero_to` — only the non-zero
  leg emitted.
- `erc20_uses_per_token_slot_override_else_default`.
- `erc20_ingest_updates_balance_and_conserves` — **end-to-end**: build the mock
  cache, seed two holders' balance slots (overlay-resident, EVM-visible),
  `ingest_logs` a `Transfer` log, assert both balances via `balance_of`
  (real `SLOAD`) and that `from + to` is conserved; `digest.applied.slots` has 2
  entries.
- `erc20_cold_balance_transfer_is_skipped_and_surfaced` — unseeded `to` →
  `digest.applied.skipped` non-empty; `has_skipped()`.

*UniswapV3 (`protocols`, gated tests):*
- `v3_swap_sets_price_and_tick_preserving_unlocked` — seed slot0 with a known
  packed word incl. `unlocked=1` (bit 240) and a nonzero observation index;
  ingest a `Swap` with new sqrtPriceX96/tick; assert slot0's low-184 bits are the
  new price/tick **and** bits 184+ (incl. `unlocked`) are unchanged.
- `v3_swap_sets_liquidity_absolute` — `liquidity` slot == event liquidity.
- `v3_swap_cold_slot0_is_skipped` — unseeded slot0 → `skipped_masks`.
- `v3_mint_increments_gross_and_net_signs` — seed tick slot+0 = 0; `Mint(amount)`
  at `[lo,hi]` → lo word `gross=amount, net=+amount`; hi word
  `gross=amount, net=-amount` (decode the packed words).
- `v3_mint_initializes_tick_and_flips_bitmap` — newly-init tick sets slot+3
  initialized bit and flips the correct bitmap word/bit (using `tick_spacing`).
- `v3_burn_decrements_and_uninitializes` — `Burn` returning gross to 0 clears the
  initialized bit and the bitmap bit.
- `v3_mint_updates_global_liquidity_when_in_range` — seed slot0 tick within
  `[lo,hi)` and a known `liquidity`; `Mint` → liquidity += amount; out-of-range →
  liquidity unchanged.
- `v3_mint_cold_tick_word_is_skipped` — unseeded tick word → surfaced skip, no
  write.
- `v3_same_block_burn_then_mint_sees_prior_apply` — two logs in one
  `ingest_logs`; the `Mint` decode reads the `Burn`'s applied gross/net.

*Pipeline (reorg + reconcile):*
- `ingest_records_touched_and_trims_ring_to_depth`.
- `reorg_to_purges_addresses_touched_after_head` — ingest blocks N, N+1, N+2
  touching distinct pools; `reorg_to(N)` purges only N+1/N+2 pools (assert their
  storage re-reads cold / re-fetches; N's survives).
- `reorg_to_returns_merged_purge_diff` — `PurgeRecord`s for the purged set.
- `reconcile_reports_mismatch_and_corrects` — stub the batch fetcher so an
  event-derived slot disagrees; `reconcile` returns it in `mismatched` and the
  cache now holds the fresh value (assert via `cached_storage_value`).
- `reconcile_empty_when_event_state_matches_chain`.
- `reconcile_errs_without_fetcher`.

**Existing suites stay green** — `tests/state_update.rs`, `tests/freshness.rs`,
`tests/snapshot_overlay.rs`, the `protocols` cache tests.

## 11. Docs, example & benchmark

- **Example** `examples/reactive_cache.rs` (offline, `examples/support`):
  build a `from_backend`/mock cache; register an `Erc20TransferDecoder` and a
  `UniswapV3Decoder` in a `DecoderRegistry`; `ingest_logs` a small vec of logs
  (an ERC-20 `Transfer` + a V3 `Swap`) for a block; print the `BlockDigest`;
  then demonstrate (a) a `reorg_to` purge, and (b) a `reconcile` drift alarm
  against a stub fetcher; wire `FreshnessController::on_new_block` +
  `registry.valid_through` on the touched slots. Add a README "Examples" row.
- **Benchmark** `benches/event_pipeline.rs` (offline): decode throughput
  (ERC-20 `Transfer`, V3 `Swap`, V3 `Mint`); `ingest_logs` per-block apply across
  log-batch sizes (1 → 1000); `reorg_to` purge cost across touched-set sizes.
  Register `[[bench]]` in `Cargo.toml`; mirror `benches/state_update.rs`; add a
  README "Benchmarks" row.
- **CHANGELOG** `### Added`: the event pipeline (`EventDecoder`/`StateView`/
  `DecoderRegistry`/`EventPipeline`), the ERC-20 + V3 adapters, and the
  `SlotMasked` vocabulary + `StateDiff.skipped_masks` (note the additive
  `StateDiff` field + new `StateUpdate` variant under the pre-1.0 break policy).
- **ROADMAP**: flip the Phase 4 row to **Done** with the landing branch, a
  "Landed on …" paragraph mirroring Phases 2/3.
- **KNOWN_ISSUES**: the §6.4 V3 fee-growth/oracle limitation.
- Rustdoc on **every** public item; module `//!` docs on `events` (Pillar B.2
  framing, `!Send` discipline, the events→Phase-3-vocabulary flow), `events/erc20`,
  `events/uniswap_v3`. At least one runnable doctest (a pure decoder on a
  hand-built `Log`, or the `SlotMasked` masked-write shape).

## 12. Decisions (LOCKED)

Confirmed with the user on 2026-06-16 before the acceptance tests were authored.

**Decision 1 — packed-slot updates → `StateUpdate::SlotMasked`.** Add the
cold-aware RMW masked-write variant (§4.1) so a pure decoder can express a
partial update to a packed word (V3 `slot0`) without clobbering the bits it does
not own (notably `unlocked`). The "absolute clobber" and "impure decoder"
alternatives were rejected.

**Decision 2 — V3 adapter coverage → `Swap` **and** `Mint`/`Burn` (full
ticks).** The adapter maintains `slot0`/`liquidity` from `Swap` and per-tick
`liquidityGross`/`liquidityNet`/`initialized` + `tickBitmap` + global
`liquidity` from `Mint`/`Burn` (§6). Fee-growth/oracle state is out of scope
(§6.4). Mint/Burn tick maintenance is computed against the `StateView`
(Decision 1's pure-data model needs the pre-state read).

**Decision 3 — reorg → purge-and-resync touched addresses.** Track touched
addresses per block in a depth-bounded ring; `reorg_to(n)` purges everything
touched after `n` so reads re-fetch (§7.2). `ValidThrough` is the freshness
lever. Per-slot value rollback rejected.

**Decision 4 — reconciliation → sampled re-read, correct **and** alarm.**
Opt-in `reconcile` samples event-derived slots and re-reads via `verify_slots`:
the fresh chain value wins (auto-correct) **and** the drift is surfaced (§7.3).
Honest freshness, built in from day one. Alarm-only and defer rejected.

## 13. Build order (commit per step, green each time)

1. `state_update.rs` + `cache/mod.rs`: `SlotMasked` variant, `slot_masked`,
   `SkippedMask`, `StateDiff.skipped_masks` (+ merge/has_skipped/skipped_len),
   serde, the apply arm (reuse `write_slot_through` + cold-aware read), and the
   §10 masked-apply tests. Re-exports.
2. `events/mod.rs`: `StateView` (+ `impl … for EvmCache`), `EventDecoder`,
   `DecoderRegistry` + dispatch tests.
3. `events/erc20.rs`: `Erc20TransferDecoder` + decoder/ingest tests.
4. `events/uniswap_v3.rs` (`protocols`): `UniswapV3Decoder`/`UniswapV3Layout`,
   `Swap`/`Mint`/`Burn` + the §10 V3 tests.
5. `EventPipeline` (`ingest_logs`/`reorg_to`/`reconcile`/`derived_slots`) +
   `BlockDigest`/`ReconcileReport`/`ReorgConfig` + the pipeline tests; the async
   `drive`/`LogSource` convenience.
6. Example + benchmark + README rows.
7. Docs (module `//!`, item rustdoc, doctest), CHANGELOG, ROADMAP → Done,
   KNOWN_ISSUES.

## 14. Final acceptance

Both feature configs green (§0). All new + existing tests pass. The example runs
offline and prints a non-trivial `BlockDigest`, a reorg purge, and a reconcile
alarm. The benchmark builds and runs (`cargo bench --no-run`). The V3 adapter
preserves the `slot0` `unlocked`/observation bits under `Swap` and maintains
tick gross/net/initialized/bitmap/global-liquidity under `Mint`/`Burn`, all
cold-aware. Report: what landed per file, the public API added, the decoder/
adapter behavior (with the §6.4 limitation called out), test coverage, and the
verification output.
