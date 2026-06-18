//! Round-trip persistence tests for the on-disk side caches.
//!
//! `ImmutableDataCache` persists generic immutable side data that belongs in the
//! core engine. Protocol-specific metadata lives in higher-level adapter crates.

use std::path::PathBuf;

use alloy_primitives::Address;
use evm_fork_cache::cache::ImmutableDataCache;

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
fn immutable_data_cache_round_trips_token_decimals() {
    let dir = TempDir::new("immutable");
    let path = dir.path("immutable_data.bin");

    let token_a = Address::repeat_byte(0xA1);
    let token_b = Address::repeat_byte(0xB2);

    let mut cache = ImmutableDataCache::default();
    assert!(cache.is_empty());

    cache.set_token_decimals(token_a, 6);
    cache.set_token_decimals(token_b, 18);

    assert!(!cache.is_empty());
    assert_eq!(cache.len(), 2);

    cache.save(&path).expect("save immutable cache");
    let bytes = std::fs::read(&path).expect("read immutable cache file");
    assert!(
        bytes.starts_with(b"EFCMETA\0"),
        "immutable cache must carry a magic header"
    );
    assert_eq!(
        &bytes[8..12],
        &2u32.to_le_bytes(),
        "immutable cache must carry an explicit version"
    );
    let loaded = ImmutableDataCache::load(&path).expect("load immutable cache");

    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded.get_token_decimals(token_a), Some(6));
    assert_eq!(loaded.get_token_decimals(token_b), Some(18));
    assert_eq!(loaded.get_token_decimals(Address::ZERO), None);
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
    assert!(ImmutableDataCache::load(&path).is_none());
}
