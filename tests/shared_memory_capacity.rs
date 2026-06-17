//! Offline tests for the configurable EVM shared-memory pre-allocation
//! ([`SharedMemoryCapacity`]) wired through [`EvmCacheBuilder`].
//!
//! Covers the three user-facing behaviors: the default, an explicit `Fixed` size,
//! and `Auto` sizing from the chain state loaded at build time (the
//! "intelligently allocate from a bincode state file" path). All offline.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::{Address, U256};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use anyhow::Result;
use evm_fork_cache::cache::{CacheConfig, EvmCacheBuilder, SharedMemoryCapacity};

fn mock_provider() -> Arc<RootProvider<AnyNetwork>> {
    Arc::new(RootProvider::<AnyNetwork>::new(RpcClient::mocked(
        Asserter::new(),
    )))
}

/// A unique temp dir for a disk-backed cache (no two tests collide).
fn unique_cache_dir(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("evm_fork_cache_smc_{tag}_{nanos}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn default_capacity_is_fixed_64k() -> Result<()> {
    let cache = EvmCacheBuilder::new(mock_provider()).build().await;
    assert_eq!(
        cache.shared_memory_capacity(),
        65_536,
        "the default must be Fixed(64 * 1024)"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn fixed_capacity_is_honored() -> Result<()> {
    let cache = EvmCacheBuilder::new(mock_provider())
        .shared_memory_capacity(SharedMemoryCapacity::Fixed(8_192))
        .build()
        .await;
    assert_eq!(cache.shared_memory_capacity(), 8_192);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_capacity_with_no_loaded_state_falls_back_to_floor() -> Result<()> {
    // No cache_config → nothing loaded → Auto resolves to the 64 KiB floor.
    let cache = EvmCacheBuilder::new(mock_provider())
        .shared_memory_capacity(SharedMemoryCapacity::Auto)
        .build()
        .await;
    assert_eq!(
        cache.shared_memory_capacity(),
        SharedMemoryCapacity::MIN_AUTO
    );
    Ok(())
}

/// The headline: `Auto` sizes the buffer from the chain state in a loaded bincode
/// state file. A first cache persists 10 000 storage slots; a second cache built
/// with `Auto` over the same `CacheConfig` loads them and pre-allocates
/// `10_000 * 16 = 160_000` bytes (vs. the 64 KiB default).
#[tokio::test(flavor = "multi_thread")]
async fn auto_capacity_scales_with_loaded_binary_state() -> Result<()> {
    let dir = unique_cache_dir("auto");
    let cfg = CacheConfig::new(&dir, 1, Default::default(), Default::default());

    // First cache: seed 10k slots into layer 2 and persist to the bincode state file.
    {
        let mut cache = EvmCacheBuilder::new(mock_provider())
            .cache_config(cfg.clone())
            .build()
            .await;
        let token = Address::repeat_byte(0x11);
        let batch: Vec<(Address, U256, U256)> = (0..10_000u64)
            .map(|i| (token, U256::from(i), U256::from(i + 1)))
            .collect();
        cache.inject_storage_batch(&batch);
        cache.flush()?; // writes evm_state.bin
    }

    // Second cache: Auto over the same config loads the 10k slots and sizes from them.
    let reloaded = EvmCacheBuilder::new(mock_provider())
        .cache_config(cfg.clone())
        .shared_memory_capacity(SharedMemoryCapacity::Auto)
        .build()
        .await;
    assert_eq!(
        reloaded.shared_memory_capacity(),
        160_000,
        "Auto must size from the 10k loaded slots (10_000 * 16 bytes)"
    );

    // A Fixed override ignores the loaded state.
    let fixed = EvmCacheBuilder::new(mock_provider())
        .cache_config(cfg.clone())
        .shared_memory_capacity(SharedMemoryCapacity::Fixed(64 * 1024))
        .build()
        .await;
    assert_eq!(fixed.shared_memory_capacity(), 65_536);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn flush_reports_unwritable_cache_paths() -> Result<()> {
    let path_conflict = unique_cache_dir("flush_error");
    std::fs::write(&path_conflict, b"not a directory")?;
    let cfg = CacheConfig::new(&path_conflict, 1, Default::default(), Default::default());
    let cache = EvmCacheBuilder::new(mock_provider())
        .cache_config(cfg)
        .build()
        .await;

    let err = cache
        .flush()
        .expect_err("flush must report persistence failures");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("directory") || rendered.contains("Not a directory"),
        "unexpected error: {rendered}"
    );

    let _ = std::fs::remove_file(&path_conflict);
    Ok(())
}
