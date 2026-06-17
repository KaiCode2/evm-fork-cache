//! UniswapV3 / PancakeSwap V3 event adapter (`protocols` feature).
//!
//! [`UniswapV3Decoder`] turns a pool's `Swap` / `Mint` / `Burn` logs into the
//! Phase 3 [`StateUpdate`] vocabulary, maintaining the slots a
//! swap simulation reads:
//!
//! - **`Swap`** (stateless) → a masked `slot0` write (new `sqrtPriceX96` + `tick`,
//!   **preserving** the observation index and the `unlocked` flag) plus an
//!   absolute `liquidity` write (the event carries post-swap liquidity).
//! - **`Mint`/`Burn`** (stateful, reads the [`StateView`]) → per-tick
//!   `liquidityGross` / `liquidityNet`, the `initialized` flag, the `tickBitmap`
//!   word bit, and the global `liquidity` (conditional on the current tick).
//!
//! The decoder dispatches by emitting address: a log from a pool not registered
//! via [`with_pool`](UniswapV3Decoder::with_pool) decodes to nothing. It matches
//! events by topic0 (`Swap`/`Mint`/`Burn` signature hashes) and decodes with
//! [`SolEvent`].
//!
//! # `slot0` bit layout (Uniswap / Pancake)
//!
//! `sqrtPriceX96` = bits [0,160), `tick` (int24) = bits [160,184), and the
//! observation index / cardinality / fee-protocol / **`unlocked`** flag occupy
//! bits [184,256). The `Swap` handler masks the low 184 bits, so the high bits —
//! crucially `unlocked` — survive. Clobbering `unlocked` to 0 would make a
//! subsequent quote/swap revert `LOK`; that is the headline reason `Swap` uses a
//! [`SlotMasked`](crate::StateUpdate::SlotMasked) rather than an absolute write.
//!
//! # Tick word packing
//!
//! Tick slot **+0** packs `liquidityGross` (uint128) = bits [0,128) and
//! `liquidityNet` (int128, two's-complement) = bits [128,256). `Mint` adds
//! `amount` to gross at both ticks and to net at the lower / from net at the
//! upper; `Burn` is the inverse. These are recomputed against the **current**
//! cached word read through the [`StateView`] and emitted as absolute `Slot`
//! writes. The `initialized` flag lives at tick slot **+3**, bit 248 (matching
//! `inject_v3_ticks`); the `tickBitmap` is keyed by the compressed tick
//! `tick / tick_spacing`.
//!
//! # Cold-aware
//!
//! When a needed word is cold ([`StateView::storage`] → `None`), the update is
//! **not** computed against an assumed value — it is skipped and surfaced. Masked
//! sub-word updates (bitmap / initialized) surface as their natural
//! [`SkippedMask`](crate::SkippedMask); the absolute tick-word / global-liquidity
//! writes that cannot be computed surface as a `SkippedMask` with
//! `mask == U256::MAX, value == U256::ZERO` (the "could-not-compute" cold marker —
//! see [`SkippedMask`](crate::SkippedMask)). A pool installed with
//! `StorageCleared` storage reads an unseeded slot as `Some(ZERO)` (hot zero), so
//! tick maintenance proceeds from zero; only a pool with no local account reads
//! cold.
//!
//! # Known limitation (§6.4)
//!
//! Event-derived tick maintenance does **not** reconstruct `feeGrowthOutside0/1X128`
//! (tick slots +1/+2), `secondsOutside`, or oracle observations — these are not
//! derivable from `Mint`/`Burn`/`Swap`. **Swap price/liquidity quoting is
//! unaffected** (the swap-amount math does not depend on `feeGrowthOutside`); fee
//! accounting and `collect` are not maintained. Sampled
//! [`reconcile`](crate::events::EventPipeline::reconcile) and reorg
//! [`reorg_to`](crate::events::EventPipeline::reorg_to) are the backstop. See
//! `KNOWN_ISSUES.md`.

use std::collections::HashMap;

use alloy_primitives::{Address, Log, U256};
use alloy_sol_types::{SolEvent, sol};

use crate::cache::{
    PANCAKE_V3_LIQUIDITY_SLOT, PANCAKE_V3_TICK_BITMAP_BASE_SLOT, PANCAKE_V3_TICKS_BASE_SLOT,
    V3_LIQUIDITY_SLOT, V3_SLOT0_SLOT, V3_TICK_BITMAP_BASE_SLOT, V3_TICKS_BASE_SLOT,
    v3_tick_bitmap_storage_key_with_base, v3_tick_info_storage_keys_with_base,
};
use crate::events::{EventDecoder, StateView};
use crate::state_update::StateUpdate;

sol! {
    event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick);
    event Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1);
    event Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1);
}

/// Bit position where the `tick` field starts in a packed `slot0` word.
const SLOT0_TICK_SHIFT: usize = 160;
/// Number of low bits of `slot0` owned by `sqrtPriceX96` ‖ `tick`
/// (`[0,160)` + `[160,184)`); the bits above are preserved by the swap mask.
const SLOT0_PRICE_TICK_BITS: usize = 184;
/// Bit position of the `initialized` flag in tick slot +3.
const TICK_INITIALIZED_BIT: usize = 248;

/// Per-pool V3 storage layout (slot bases + tick spacing).
///
/// Uniswap V3 and PancakeSwap V3 share the `slot0` bit layout (only the base slot
/// numbers differ — Pancake's `uint32 feeProtocol` shifts subsequent slots by +1).
/// `tick_spacing` is required for `tickBitmap` word/bit math (the bitmap is keyed
/// by the compressed tick `tick / tick_spacing`).
#[derive(Clone, Debug)]
pub struct UniswapV3Layout {
    /// Storage slot of `slot0` (packed price / tick / observation / unlocked).
    pub slot0_slot: U256,
    /// Storage slot of the global `liquidity`.
    pub liquidity_slot: U256,
    /// Base slot of the `ticks` mapping (`mapping(int24 => Tick.Info)`).
    pub ticks_base_slot: U256,
    /// Base slot of the `tickBitmap` mapping (`mapping(int16 => uint256)`).
    pub tick_bitmap_base_slot: U256,
    /// The pool's `tickSpacing` (for compressed-tick bitmap word/bit math).
    pub tick_spacing: i32,
}

impl UniswapV3Layout {
    /// The canonical Uniswap V3 layout for a pool with the given `tick_spacing`.
    pub fn uniswap(tick_spacing: i32) -> Self {
        Self {
            slot0_slot: V3_SLOT0_SLOT,
            liquidity_slot: V3_LIQUIDITY_SLOT,
            ticks_base_slot: V3_TICKS_BASE_SLOT,
            tick_bitmap_base_slot: V3_TICK_BITMAP_BASE_SLOT,
            tick_spacing,
        }
    }

    /// The PancakeSwap V3 layout for a pool with the given `tick_spacing` (slots
    /// shifted +1 relative to Uniswap; `slot0` stays at slot 0).
    pub fn pancake(tick_spacing: i32) -> Self {
        Self {
            slot0_slot: V3_SLOT0_SLOT,
            liquidity_slot: PANCAKE_V3_LIQUIDITY_SLOT,
            ticks_base_slot: PANCAKE_V3_TICKS_BASE_SLOT,
            tick_bitmap_base_slot: PANCAKE_V3_TICK_BITMAP_BASE_SLOT,
            tick_spacing,
        }
    }
}

/// Decodes UniswapV3 / PancakeSwap V3 `Swap` / `Mint` / `Burn` logs into targeted
/// [`StateUpdate`]s.
///
/// Register pools with [`with_pool`](Self::with_pool); a log from an unregistered
/// pool decodes to nothing.
#[derive(Default)]
pub struct UniswapV3Decoder {
    /// Per-pool layout. A log from a pool not in this map decodes to nothing.
    pools: HashMap<Address, UniswapV3Layout>,
}

impl UniswapV3Decoder {
    /// Create an empty decoder with no pools registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `pool` with its storage `layout` (builder style).
    pub fn with_pool(mut self, pool: Address, layout: UniswapV3Layout) -> Self {
        self.pools.insert(pool, layout);
        self
    }
}

/// The cold-tick "could-not-compute" marker: a [`StateUpdate::SlotMasked`] with
/// `mask == U256::MAX, value == U256::ZERO`, which `apply_updates` skip-surfaces
/// for a cold slot (see [`SkippedMask`](crate::SkippedMask)).
fn cold_marker(pool: Address, slot: U256) -> StateUpdate {
    StateUpdate::slot_masked(pool, slot, U256::MAX, U256::ZERO)
}

/// Unpack a tick slot +0 word: `(liquidityGross, liquidityNet)`.
fn unpack_tick_word(word: U256) -> (u128, i128) {
    let gross = u128::try_from(word & U256::from(u128::MAX)).unwrap_or(0);
    let net = u128::try_from((word >> 128) & U256::from(u128::MAX)).unwrap_or(0) as i128;
    (gross, net)
}

/// Pack `(liquidityGross, liquidityNet)` into a tick slot +0 word.
fn pack_tick_word(gross: u128, net: i128) -> U256 {
    U256::from(gross) | (U256::from(net as u128) << 128)
}

/// Convert a `sol!`-decoded int24 tick to `i32` (a tick always fits in i24 ⊂ i32).
fn tick_to_i32(tick: alloy_primitives::aliases::I24) -> i32 {
    i128::try_from(tick).unwrap_or(0) as i32
}

/// The pre-state context a `Mint`/`Burn` maintenance pass reads against: the pool
/// address, its layout, and the read-only [`StateView`].
struct LiquidityCtx<'a> {
    pool: Address,
    layout: &'a UniswapV3Layout,
    view: &'a dyn StateView,
}

impl LiquidityCtx<'_> {
    /// Maintenance for one tick endpoint of a `Mint`/`Burn`. `is_burn` selects the
    /// sign (mint adds, burn subtracts); `is_lower` selects the `liquidityNet`
    /// sign convention (lower += / upper -= on a mint). Appends the recomputed
    /// tick-word write plus any `initialized`/bitmap flips (or a cold marker) to
    /// `out`.
    fn maintain_tick(
        &self,
        tick: i32,
        amount: u128,
        is_burn: bool,
        is_lower: bool,
        out: &mut Vec<StateUpdate>,
    ) {
        let keys = v3_tick_info_storage_keys_with_base(tick, self.layout.ticks_base_slot);
        let base = keys[0];
        let slot3 = keys[3];

        // The current packed tick word. Cold → cannot recompute: surface a marker.
        let Some(word) = self.view.storage(self.pool, base) else {
            out.push(cold_marker(self.pool, base));
            return;
        };
        let (gross, net) = unpack_tick_word(word);

        // gross is always +amount on mint, -amount on burn (saturating defensively).
        let new_gross = if is_burn {
            gross.saturating_sub(amount)
        } else {
            gross.saturating_add(amount)
        };
        // net: lower += amount, upper -= amount on mint; inverse on burn.
        let net_delta = amount as i128;
        let signed_delta = match (is_burn, is_lower) {
            (false, true) => net_delta,   // mint lower: +
            (false, false) => -net_delta, // mint upper: -
            (true, true) => -net_delta,   // burn lower: -
            (true, false) => net_delta,   // burn upper: +
        };
        let new_net = net.wrapping_add(signed_delta);

        out.push(StateUpdate::slot(
            self.pool,
            base,
            pack_tick_word(new_gross, new_net),
        ));

        // initialized flag (+3, bit 248) + bitmap bit flip on a 0↔positive cross.
        let init_mask = U256::from(1) << TICK_INITIALIZED_BIT;
        let newly_initialized = gross == 0 && new_gross > 0;
        let now_uninitialized = gross > 0 && new_gross == 0;

        if newly_initialized {
            out.push(StateUpdate::slot_masked(
                self.pool, slot3, init_mask, init_mask,
            ));
            if let Some(flip) = self.bitmap_flip(tick, true) {
                out.push(flip);
            }
        } else if now_uninitialized {
            out.push(StateUpdate::slot_masked(
                self.pool,
                slot3,
                init_mask,
                U256::ZERO,
            ));
            if let Some(flip) = self.bitmap_flip(tick, false) {
                out.push(flip);
            }
        }
    }

    /// Build the `tickBitmap` word/bit flip for `tick` (`set` = newly initialized,
    /// clear = newly uninitialized). The bitmap is keyed by the compressed tick
    /// `tick / tick_spacing` (V3 guarantees `tick % tick_spacing == 0`, so the
    /// division is exact). Returns `None` if `tick_spacing` is non-positive
    /// (degenerate layout).
    fn bitmap_flip(&self, tick: i32, set: bool) -> Option<StateUpdate> {
        if self.layout.tick_spacing <= 0 {
            return None;
        }
        let compressed = tick / self.layout.tick_spacing;
        let word_pos = (compressed >> 8) as i16;
        let bit_pos = (compressed & 0xFF) as u8;
        let key = v3_tick_bitmap_storage_key_with_base(word_pos, self.layout.tick_bitmap_base_slot);
        let mask = U256::from(1) << bit_pos;
        let value = if set { mask } else { U256::ZERO };
        Some(StateUpdate::slot_masked(self.pool, key, mask, value))
    }

    /// Global-liquidity maintenance for a `Mint`/`Burn`: if the current `slot0`
    /// tick is within `[tickLower, tickUpper)`, emit an absolute `liquidity` write
    /// of `current ± amount`. Reads `slot0` and `liquidity` through the view; if
    /// either is cold, surface a cold marker on the liquidity slot.
    fn maintain_global_liquidity(
        &self,
        tick_lower: i32,
        tick_upper: i32,
        amount: u128,
        is_burn: bool,
        out: &mut Vec<StateUpdate>,
    ) {
        let liquidity_slot = self.layout.liquidity_slot;
        let Some(slot0) = self.view.storage(self.pool, self.layout.slot0_slot) else {
            out.push(cold_marker(self.pool, liquidity_slot));
            return;
        };
        let current_tick = extract_tick(slot0);
        if !(tick_lower <= current_tick && current_tick < tick_upper) {
            return; // out of range: global liquidity unchanged.
        }
        let Some(current_word) = self.view.storage(self.pool, liquidity_slot) else {
            out.push(cold_marker(self.pool, liquidity_slot));
            return;
        };
        let current = u128::try_from(current_word & U256::from(u128::MAX)).unwrap_or(0);
        let new = if is_burn {
            current.saturating_sub(amount)
        } else {
            current.saturating_add(amount)
        };
        out.push(StateUpdate::slot(
            self.pool,
            liquidity_slot,
            U256::from(new),
        ));
    }

    /// Decode a `Mint` or `Burn` (shared tick + liquidity maintenance).
    fn decode_liquidity_event(
        &self,
        tick_lower: i32,
        tick_upper: i32,
        amount: u128,
        is_burn: bool,
    ) -> Vec<StateUpdate> {
        let mut out = Vec::new();
        self.maintain_tick(tick_lower, amount, is_burn, true, &mut out);
        self.maintain_tick(tick_upper, amount, is_burn, false, &mut out);
        self.maintain_global_liquidity(tick_lower, tick_upper, amount, is_burn, &mut out);
        out
    }
}

/// Sign-extend the int24 `tick` field (bits [160,184)) out of a packed `slot0`.
fn extract_tick(slot0: U256) -> i32 {
    let raw = ((slot0 >> SLOT0_TICK_SHIFT) & U256::from(0x00FF_FFFFu32)).to::<u32>();
    // Sign-extend from 24 bits.
    if raw & 0x0080_0000 != 0 {
        (raw | 0xFF00_0000) as i32
    } else {
        raw as i32
    }
}

impl EventDecoder for UniswapV3Decoder {
    fn decode(&self, log: &Log, view: &dyn StateView) -> Vec<StateUpdate> {
        let Some(layout) = self.pools.get(&log.address) else {
            return Vec::new();
        };
        let pool = log.address;
        let topic0 = match log.topics().first() {
            Some(t) => *t,
            None => return Vec::new(),
        };

        if topic0 == Swap::SIGNATURE_HASH {
            let Ok(swap) = Swap::decode_log_data(&log.data) else {
                return Vec::new();
            };
            // slot0: masked write of sqrtPriceX96 [0,160) + tick [160,184),
            // preserving observation / feeProtocol / unlocked bits [184,256).
            let mask = (U256::from(1) << SLOT0_PRICE_TICK_BITS) - U256::from(1);
            let sqrt_price = U256::from_be_slice(swap.sqrtPriceX96.to_be_bytes::<20>().as_slice());
            let tick = tick_to_i32(swap.tick);
            let tick24 = U256::from((tick as u32) & 0x00FF_FFFF);
            let value = sqrt_price | (tick24 << SLOT0_TICK_SHIFT);
            vec![
                StateUpdate::slot_masked(pool, layout.slot0_slot, mask, value),
                // liquidity: absolute (the event carries post-swap liquidity).
                StateUpdate::slot(pool, layout.liquidity_slot, U256::from(swap.liquidity)),
            ]
        } else if topic0 == Mint::SIGNATURE_HASH {
            let Ok(mint) = Mint::decode_log_data(&log.data) else {
                return Vec::new();
            };
            let ctx = LiquidityCtx { pool, layout, view };
            ctx.decode_liquidity_event(
                tick_to_i32(mint.tickLower),
                tick_to_i32(mint.tickUpper),
                mint.amount,
                false,
            )
        } else if topic0 == Burn::SIGNATURE_HASH {
            let Ok(burn) = Burn::decode_log_data(&log.data) else {
                return Vec::new();
            };
            let ctx = LiquidityCtx { pool, layout, view };
            ctx.decode_liquidity_event(
                tick_to_i32(burn.tickLower),
                tick_to_i32(burn.tickUpper),
                burn.amount,
                true,
            )
        } else {
            Vec::new()
        }
    }
}
