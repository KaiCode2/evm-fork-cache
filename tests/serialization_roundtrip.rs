//! Round-trip persistence tests for the on-disk side caches.
//!
//! `ImmutableDataCache` (token decimals + pool metadata) and, under the
//! `protocols` feature, `V3TickSnapshotCache` are serialized with bincode and
//! reloaded across runs. These modules had no test coverage; the tests here pin
//! that a save/load cycle preserves the data, that a missing file is reported as
//! "no cache", and the current (silent-drop) behavior of the string-keyed V3 tick
//! snapshot — see `docs/KNOWN_ISSUES.md`.
//!
//! Files are written under the system temp directory and cleaned up, following
//! the dependency-free pattern used by the `binary_state` unit tests.

use std::path::PathBuf;

use alloy_primitives::{Address, B256, U256};
use evm_fork_cache::cache::{
    BalancerPoolMetadata, ImmutableDataCache, V2PoolMetadata, V3PoolMetadata,
};

/// A unique temp directory for one test, removed on drop so a failing assertion
/// still cleans up.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("evm_fork_cache_roundtrip_{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        TempDir(dir)
    }

    fn path(&self, file: &str) -> PathBuf {
        self.0.join(file)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn immutable_data_cache_round_trips() {
    let dir = TempDir::new("immutable");
    let path = dir.path("immutable_data.bin");

    let token_a = Address::repeat_byte(0xA1);
    let token_b = Address::repeat_byte(0xB2);
    let v2_pool = Address::repeat_byte(0x22);
    let v3_pool = Address::repeat_byte(0x33);
    let balancer_id = B256::repeat_byte(0x44);

    let mut cache = ImmutableDataCache::default();
    assert!(cache.is_empty());

    cache.set_token_decimals(token_a, 6);
    cache.set_token_decimals(token_b, 18);
    cache.set_v2_pool(
        v2_pool,
        V2PoolMetadata {
            token0: token_a,
            token1: token_b,
            last_block_timestamp: 1_700_000_000,
        },
    );
    cache.set_v3_pool(
        v3_pool,
        V3PoolMetadata {
            token0: token_a,
            token1: token_b,
            fee: 3000,
            tick_spacing: 60,
        },
    );
    cache.set_balancer_pool(
        balancer_id,
        BalancerPoolMetadata {
            tokens: vec![token_a, token_b],
            weights: vec![U256::from(80u64), U256::from(20u64)],
            swap_fee: U256::from(1_000u64),
            last_change_block: U256::from(18_000_000u64),
        },
    );

    assert!(!cache.is_empty());
    let len_before = cache.len();

    cache.save(&path).expect("save immutable cache");
    let bytes = std::fs::read(&path).expect("read immutable cache file");
    assert!(
        bytes.starts_with(b"EFCMETA\0"),
        "immutable cache must carry a magic header"
    );
    assert_eq!(
        &bytes[8..12],
        &1u32.to_le_bytes(),
        "immutable cache must carry an explicit version"
    );
    let loaded = ImmutableDataCache::load(&path).expect("load immutable cache");

    // Counts and scalar values survive the round trip.
    assert_eq!(loaded.len(), len_before);
    assert_eq!(loaded.get_token_decimals(token_a), Some(6));
    assert_eq!(loaded.get_token_decimals(token_b), Some(18));
    assert_eq!(loaded.get_token_decimals(Address::ZERO), None);

    // Metadata structs do not derive PartialEq, so compare field-by-field.
    let v2 = loaded.get_v2_pool(v2_pool).expect("v2 pool present");
    assert_eq!(v2.token0, token_a);
    assert_eq!(v2.token1, token_b);
    assert_eq!(v2.last_block_timestamp, 1_700_000_000);

    let v3 = loaded.get_v3_pool(v3_pool).expect("v3 pool present");
    assert_eq!(v3.token0, token_a);
    assert_eq!(v3.token1, token_b);
    assert_eq!(v3.fee, 3000);
    assert_eq!(v3.tick_spacing, 60);

    // The Balancer pool is keyed by the id's Debug formatting; a lookup with the
    // same B256 after reload must still resolve.
    let bal = loaded
        .get_balancer_pool(balancer_id)
        .expect("balancer pool present after reload (Debug-key round trip)");
    assert_eq!(bal.tokens, vec![token_a, token_b]);
    assert_eq!(bal.weights, vec![U256::from(80u64), U256::from(20u64)]);
    assert_eq!(bal.swap_fee, U256::from(1_000u64));
    assert_eq!(bal.last_change_block, U256::from(18_000_000u64));
}

#[test]
fn immutable_data_cache_load_legacy_raw_bincode_is_none() {
    let dir = TempDir::new("immutable_legacy");
    let path = dir.path("legacy_immutable_data.bin");
    let mut cache = ImmutableDataCache::default();
    cache.set_token_decimals(Address::repeat_byte(0xA1), 6);
    std::fs::write(&path, bincode::serialize(&cache).unwrap()).expect("write legacy cache");

    assert!(
        ImmutableDataCache::load(&path).is_none(),
        "unversioned legacy bincode must be treated as a cache miss"
    );
}

#[test]
fn immutable_data_cache_load_missing_file_is_none() {
    let dir = TempDir::new("immutable_missing");
    let missing = dir.path("does_not_exist.bin");
    assert!(ImmutableDataCache::load(&missing).is_none());
}

#[test]
fn immutable_data_cache_load_corrupt_file_is_none() {
    let dir = TempDir::new("immutable_corrupt");
    let path = dir.path("corrupt.bin");
    std::fs::write(&path, b"not valid bincode at all").expect("write corrupt file");
    // A decode failure is swallowed and reported as "no cache" (see KNOWN_ISSUES).
    assert!(ImmutableDataCache::load(&path).is_none());
}

#[cfg(feature = "protocols")]
mod tick_snapshots {
    use super::*;
    use std::collections::HashMap;

    use evm_fork_cache::cache::{TickInfo, V3PoolTickSnapshot, V3TickSnapshotCache};

    #[test]
    fn v3_tick_snapshot_round_trips_including_negative_keys() {
        let dir = TempDir::new("v3_ticks");
        let path = dir.path("v3_tick_snapshots.bin");
        let pool = Address::repeat_byte(0x77);

        // Word positions and tick indices are signed; include negatives, which
        // are exactly where the string-key encoding could go wrong.
        let mut bitmap: HashMap<i16, U256> = HashMap::new();
        bitmap.insert(-3, U256::from(0b1010u64));
        bitmap.insert(0, U256::from(1u64));
        bitmap.insert(5, U256::from(u128::MAX));

        let mut ticks: HashMap<i32, TickInfo> = HashMap::new();
        ticks.insert(
            -887_272,
            TickInfo {
                liquidity_gross: 1_000,
                liquidity_net: -500,
                initialized: true,
            },
        );
        ticks.insert(
            60,
            TickInfo {
                liquidity_gross: 42,
                liquidity_net: 7,
                initialized: false,
            },
        );

        let snapshot = V3PoolTickSnapshot::from_pool_data(&bitmap, &ticks, 12_345u128, -120);

        let mut cache = V3TickSnapshotCache::default();
        assert!(cache.is_empty());
        cache.set(pool, snapshot);
        assert_eq!(cache.len(), 1);

        cache.save(&path).expect("save tick cache");
        let bytes = std::fs::read(&path).expect("read tick cache file");
        assert!(
            bytes.starts_with(b"EFCTICK\0"),
            "tick snapshot cache must carry a magic header"
        );
        assert_eq!(
            &bytes[8..12],
            &1u32.to_le_bytes(),
            "tick snapshot cache must carry an explicit version"
        );
        let loaded = V3TickSnapshotCache::load(&path).expect("load tick cache");

        let snap = loaded.get(pool).expect("snapshot present");
        assert_eq!(snap.last_liquidity, 12_345u128);
        assert_eq!(snap.last_tick, -120);
        // TickInfo derives PartialEq/Eq, so the recovered maps compare directly.
        assert_eq!(snap.to_tick_bitmap(), bitmap, "bitmap survives round trip");
        assert_eq!(snap.to_ticks(), ticks, "ticks survive round trip");
    }

    #[test]
    fn v3_tick_snapshot_cache_load_legacy_raw_bincode_is_none() {
        let dir = TempDir::new("v3_ticks_legacy");
        let path = dir.path("legacy_v3_tick_snapshots.bin");
        let pool = Address::repeat_byte(0x77);
        let mut cache = V3TickSnapshotCache::default();
        cache.set(
            pool,
            V3PoolTickSnapshot::from_pool_data(&HashMap::new(), &HashMap::new(), 0, 0),
        );
        std::fs::write(&path, bincode::serialize(&cache).unwrap()).expect("write legacy cache");

        assert!(
            V3TickSnapshotCache::load(&path).is_none(),
            "unversioned legacy bincode must be treated as a cache miss"
        );
    }

    #[test]
    fn v3_tick_snapshot_silently_drops_unparseable_keys() {
        // Pin the documented behavior (KNOWN_ISSUES): a string key that does not
        // parse as the expected integer type is dropped without error.
        let mut snapshot = V3PoolTickSnapshot::from_pool_data(
            &HashMap::from([(1i16, U256::from(9u64))]),
            &HashMap::new(),
            0,
            0,
        );
        snapshot
            .tick_bitmap
            .insert("not-a-number".to_string(), U256::from(123u64));

        let recovered = snapshot.to_tick_bitmap();
        assert_eq!(recovered.len(), 1, "the unparseable key is dropped");
        assert_eq!(recovered.get(&1i16), Some(&U256::from(9u64)));
    }

    #[test]
    fn v3_tick_snapshot_cache_remove() {
        let pool = Address::repeat_byte(0x01);
        let mut cache = V3TickSnapshotCache::default();
        cache.set(
            pool,
            V3PoolTickSnapshot::from_pool_data(&HashMap::new(), &HashMap::new(), 0, 0),
        );
        assert_eq!(cache.len(), 1);
        cache.remove(pool);
        assert!(cache.is_empty());
        assert!(cache.get(pool).is_none());
    }
}
