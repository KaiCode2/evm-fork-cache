//! Tests for the Uniswap V3-style storage-key derivation helpers, exercised
//! through their public re-export path so the coverage travels with the crate.

use alloy_primitives::U256;
use evm_fork_cache::cache::{v3_tick_bitmap_storage_key, v3_tick_info_storage_keys};

#[test]
fn tick_bitmap_storage_key_is_consistent_and_distinct() {
    // Same word -> same key.
    assert_eq!(
        v3_tick_bitmap_storage_key(0),
        v3_tick_bitmap_storage_key(0),
        "same word should produce the same key"
    );

    // Distinct words -> distinct keys.
    let key0 = v3_tick_bitmap_storage_key(0);
    let key_neg1 = v3_tick_bitmap_storage_key(-1);
    let key_pos1 = v3_tick_bitmap_storage_key(1);
    assert_ne!(key0, key_neg1);
    assert_ne!(key0, key_pos1);
    assert_ne!(key_neg1, key_pos1);

    // Keys are keccak outputs, never zero.
    assert_ne!(key0, U256::ZERO);
}

#[test]
fn tick_info_storage_keys_are_four_consecutive_slots() {
    // Same tick -> same keys.
    let keys = v3_tick_info_storage_keys(0);
    assert_eq!(keys, v3_tick_info_storage_keys(0));

    // Tick.Info occupies four consecutive slots.
    assert_eq!(keys[1], keys[0] + U256::from(1));
    assert_eq!(keys[2], keys[0] + U256::from(2));
    assert_eq!(keys[3], keys[0] + U256::from(3));

    // Distinct ticks -> distinct base slots.
    let pos = v3_tick_info_storage_keys(60);
    let neg = v3_tick_info_storage_keys(-60);
    assert_ne!(keys[0], pos[0]);
    assert_ne!(keys[0], neg[0]);
    assert_ne!(pos[0], neg[0]);

    assert_ne!(keys[0], U256::ZERO);
}
