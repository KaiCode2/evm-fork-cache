//! AMM storage-slot key math.
//!
//! DEX pool contracts (UniswapV3 and similar) store their state at well-known
//! slot numbers and in Solidity mappings keyed by tick or bitmap word. This
//! module pins those base slot constants and computes the concrete storage keys
//! (`keccak256`-derived mapping slots) for individual ticks and bitmap words,
//! so the cache can selectively purge or refresh just the slots that matter
//! instead of an entire account's storage.

use alloy_primitives::{U256, keccak256};

// ============================================================================
// UniswapV3 Storage Layout Constants
// ============================================================================
//
// These constants map to the storage slot numbers in the UniswapV3Pool contract.
// They are used for selective cache purging (purging only specific slots instead
// of all storage) and for computing mapping storage keys.

/// Storage slot for UniswapV3Pool.slot0 (packed: sqrtPriceX96, tick, etc.)
pub const V3_SLOT0_SLOT: U256 = U256::ZERO;

/// Storage slot for UniswapV3Pool.liquidity
pub const V3_LIQUIDITY_SLOT: U256 = U256::from_limbs([4, 0, 0, 0]);

/// Base storage slot for UniswapV3Pool.ticks mapping (used by Uniswap V3)
pub const V3_TICKS_BASE_SLOT: U256 = U256::from_limbs([5, 0, 0, 0]);

/// Base storage slot for UniswapV3Pool.tickBitmap mapping (int16 => uint256)
pub const V3_TICK_BITMAP_BASE_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);

/// PancakeSwap V3 has a storage layout shift: slot0 uses uint32 feeProtocol
/// instead of uint8, pushing subsequent slots by +1.
///
/// Storage slot for PancakeSwapV3Pool.liquidity (slot 5 vs Uniswap's slot 4)
pub const PANCAKE_V3_LIQUIDITY_SLOT: U256 = U256::from_limbs([5, 0, 0, 0]);

/// Base storage slot for PancakeSwapV3Pool.ticks mapping (slot 6 vs Uniswap's slot 5)
pub const PANCAKE_V3_TICKS_BASE_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);

/// Base storage slot for PancakeSwapV3Pool.tickBitmap mapping (slot 7 vs Uniswap's slot 6)
pub const PANCAKE_V3_TICK_BITMAP_BASE_SLOT: U256 = U256::from_limbs([7, 0, 0, 0]);

/// Aerodrome/Velodrome Slipstream CL pools have extra reward-related state variables
/// (gauge, nft, factoryRegistry, rewardGrowthGlobalX128, etc.) that shift storage slots.
/// slot0 is at slot 6, liquidity at 17, tickBitmap at 18, ticks at 19.
///
/// Storage slot for Slipstream CLPool.slot0
pub const SLIPSTREAM_SLOT0_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);

/// Storage slot for Slipstream CLPool.liquidity (slot 17)
pub const SLIPSTREAM_LIQUIDITY_SLOT: U256 = U256::from_limbs([17, 0, 0, 0]);

/// Base storage slot for Slipstream CLPool.tickBitmap mapping (slot 18)
pub const SLIPSTREAM_TICK_BITMAP_BASE_SLOT: U256 = U256::from_limbs([18, 0, 0, 0]);

/// Base storage slot for Slipstream CLPool.ticks mapping (slot 19)
pub const SLIPSTREAM_TICKS_BASE_SLOT: U256 = U256::from_limbs([19, 0, 0, 0]);

/// Storage slot for UniswapV2Pair packed reserves (reserve0 | reserve1 | blockTimestampLast)
pub const V2_RESERVES_SLOT: U256 = U256::from_limbs([8, 0, 0, 0]);

/// Compute the storage key for a UniswapV3 tickBitmap entry.
///
/// tickBitmap is a `mapping(int16 => uint256)` at base slot 6.
/// For a mapping at slot `p`, the value for key `k` is at `keccak256(abi.encode(k, p))`.
///
/// This is the convenience wrapper over
/// [`v3_tick_bitmap_storage_key_with_base`] pinned to
/// [`V3_TICK_BITMAP_BASE_SLOT`].
///
/// # Examples
///
/// ```
/// use evm_fork_cache::cache::{
///     v3_tick_bitmap_storage_key, v3_tick_bitmap_storage_key_with_base,
///     V3_TICK_BITMAP_BASE_SLOT,
/// };
///
/// // Equivalent to calling the `_with_base` form with the default base slot.
/// assert_eq!(
///     v3_tick_bitmap_storage_key(3),
///     v3_tick_bitmap_storage_key_with_base(3, V3_TICK_BITMAP_BASE_SLOT),
/// );
/// // The key is deterministic and distinct per word position.
/// assert_ne!(v3_tick_bitmap_storage_key(3), v3_tick_bitmap_storage_key(-3));
/// ```
pub fn v3_tick_bitmap_storage_key(word_position: i16) -> U256 {
    v3_tick_bitmap_storage_key_with_base(word_position, V3_TICK_BITMAP_BASE_SLOT)
}

/// Compute the storage key for a V3-style tickBitmap entry with a custom base slot.
///
/// PancakeSwap V3 uses base slot 7 instead of Uniswap V3's slot 6.
///
/// The key is `keccak256(abi.encode(int256(word_position), base_slot))`, so a
/// different `base_slot` yields a different key for the same word position.
///
/// # Examples
///
/// ```
/// use evm_fork_cache::cache::{
///     v3_tick_bitmap_storage_key_with_base, V3_TICK_BITMAP_BASE_SLOT,
///     PANCAKE_V3_TICK_BITMAP_BASE_SLOT,
/// };
///
/// let uniswap = v3_tick_bitmap_storage_key_with_base(10, V3_TICK_BITMAP_BASE_SLOT);
/// let pancake = v3_tick_bitmap_storage_key_with_base(10, PANCAKE_V3_TICK_BITMAP_BASE_SLOT);
/// assert_ne!(uniswap, pancake);
/// ```
pub fn v3_tick_bitmap_storage_key_with_base(word_position: i16, base_slot: U256) -> U256 {
    let word_i256 = i256_from_i16(word_position);
    let mut preimage = [0u8; 64];
    preimage[..32].copy_from_slice(&word_i256);
    preimage[32..64].copy_from_slice(&base_slot.to_be_bytes::<32>());
    keccak256(preimage).into()
}

/// Compute the storage slot keys for a UniswapV3 tick's Info struct.
///
/// The ticks mapping is at slot 5: `mapping(int24 => Tick.Info)`
/// Storage key: `keccak256(abi.encode(int256(tick), uint256(5)))`
/// The Tick.Info struct occupies 4 consecutive slots starting from the base.
///
/// This is the convenience wrapper over [`v3_tick_info_storage_keys_with_base`]
/// pinned to [`V3_TICKS_BASE_SLOT`].
///
/// # Examples
///
/// ```
/// use evm_fork_cache::cache::v3_tick_info_storage_keys;
/// use alloy_primitives::U256;
///
/// let keys = v3_tick_info_storage_keys(0);
/// // The four slots are consecutive, starting from the hashed base.
/// assert_eq!(keys[1], keys[0] + U256::from(1));
/// assert_eq!(keys[2], keys[0] + U256::from(2));
/// assert_eq!(keys[3], keys[0] + U256::from(3));
/// ```
pub fn v3_tick_info_storage_keys(tick: i32) -> [U256; 4] {
    v3_tick_info_storage_keys_with_base(tick, V3_TICKS_BASE_SLOT)
}

/// Compute the storage slot keys for a V3-style tick's Info struct with a custom ticks mapping slot.
///
/// PancakeSwap V3 uses ticks at slot 6 instead of Uniswap V3's slot 5. The four
/// returned keys are consecutive, starting from
/// `keccak256(abi.encode(int256(tick), ticks_slot))`.
///
/// # Examples
///
/// ```
/// use evm_fork_cache::cache::{v3_tick_info_storage_keys_with_base, V3_TICKS_BASE_SLOT};
/// use alloy_primitives::U256;
///
/// let keys = v3_tick_info_storage_keys_with_base(-100, V3_TICKS_BASE_SLOT);
/// assert_eq!(keys[3], keys[0] + U256::from(3));
/// ```
pub fn v3_tick_info_storage_keys_with_base(tick: i32, ticks_slot: U256) -> [U256; 4] {
    let tick_i256 = i256_from_i24(tick);
    let mut preimage = [0u8; 64];
    preimage[..32].copy_from_slice(&tick_i256);
    preimage[32..64].copy_from_slice(&ticks_slot.to_be_bytes::<32>());
    let base: U256 = keccak256(preimage).into();
    [
        base,
        base + U256::from(1),
        base + U256::from(2),
        base + U256::from(3),
    ]
}

/// Sign-extend an i16 to a 32-byte big-endian representation (i256).
///
/// This is needed for Solidity ABI encoding of signed integers in mapping keys.
/// Positive values are zero-extended, negative values are sign-extended with 0xFF bytes.
pub(crate) fn i256_from_i16(value: i16) -> [u8; 32] {
    let mut result = if value < 0 {
        [0xFF; 32] // Sign-extend with 1s for negative
    } else {
        [0x00; 32] // Zero-extend for positive
    };
    // Place the i16 value in the last 2 bytes (big-endian)
    let bytes = value.to_be_bytes();
    result[30] = bytes[0];
    result[31] = bytes[1];
    result
}

/// Sign-extend an i24 (stored as i32) to a 32-byte big-endian representation (i256).
///
/// UniswapV3 uses int24 for tick indices. We store them as i32 but only the lower
/// 24 bits are meaningful. This function sign-extends based on the 24-bit value.
pub(crate) fn i256_from_i24(value: i32) -> [u8; 32] {
    // Mask to 24 bits and check sign bit (bit 23)
    let masked = value & 0x00FF_FFFF;
    let is_negative = (masked & 0x0080_0000) != 0;

    let mut result = if is_negative {
        [0xFF; 32] // Sign-extend with 1s for negative
    } else {
        [0x00; 32] // Zero-extend for positive
    };
    // Place the i24 value in the last 3 bytes (big-endian)
    result[29] = ((masked >> 16) & 0xFF) as u8;
    result[30] = ((masked >> 8) & 0xFF) as u8;
    result[31] = (masked & 0xFF) as u8;
    result
}

/// Convert an i128 to U256, handling negative values via two's complement.
///
/// This is needed for packing signed integers into storage slots.
pub(crate) fn i128_to_u256(value: i128) -> U256 {
    if value >= 0 {
        U256::from(value as u128)
    } else {
        // Two's complement: for negative values, we need the bit pattern
        // as an unsigned value. In Rust, casting i128 to u128 gives us this.
        U256::from(value as u128)
    }
}
