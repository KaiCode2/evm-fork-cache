//! Offline tests for chain-ID resolution on [`EvmCacheBuilder`] / [`EvmCache`].
//!
//! Resolution order (highest priority first):
//!   1. an explicit [`EvmCacheBuilder::chain_id`] value;
//!   2. a disk [`CacheConfig`]'s `chain_id` (also the on-disk namespace);
//!   3. the value inferred from the provider via `eth_chainId`;
//!   4. `1` (Ethereum mainnet) as a last-resort fallback when inference fails.
//!
//! All tests run fully offline over a mocked provider whose `eth_chainId` query
//! errors (empty `Asserter`), so the inference branch deterministically takes the
//! mainnet fallback — letting us assert the explicit / config / fallback rungs
//! without a network.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use anyhow::Result;
use evm_fork_cache::cache::{CacheConfig, EvmCache, EvmCacheBuilder};

fn mock_provider() -> Arc<RootProvider<AnyNetwork>> {
    Arc::new(RootProvider::<AnyNetwork>::new(RpcClient::mocked(
        Asserter::new(),
    )))
}

fn unique_cache_dir(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("evm_fork_cache_chainid_{tag}_{nanos}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_builder_chain_id_wins() -> Result<()> {
    // 8453 = Base. The explicit builder value is authoritative.
    let cache = EvmCacheBuilder::new(mock_provider())
        .chain_id(8453)
        .build()
        .await;
    assert_eq!(cache.chain_id(), 8453);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unset_chain_id_falls_back_to_mainnet_when_inference_fails() -> Result<()> {
    // No explicit value, no cache_config, and the mock provider's eth_chainId
    // errors — so resolution lands on the mainnet (1) fallback, not Arbitrum.
    let cache = EvmCacheBuilder::new(mock_provider()).build().await;
    assert_eq!(cache.chain_id(), 1);

    // The bare `new` constructor takes the same inference path.
    let via_new = EvmCache::new(mock_provider()).await;
    assert_eq!(via_new.chain_id(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_config_chain_id_is_used_when_no_explicit_value() -> Result<()> {
    // 10 = OP Mainnet. With a disk cache and no explicit builder value, the
    // CacheConfig chain id is authoritative.
    let dir = unique_cache_dir("config");
    let cfg = CacheConfig::new(&dir, 10, Default::default(), Default::default());
    let cache = EvmCacheBuilder::new(mock_provider())
        .cache_config(cfg)
        .build()
        .await;
    assert_eq!(cache.chain_id(), 10);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_builder_chain_id_overrides_cache_config() -> Result<()> {
    let dir = unique_cache_dir("override");
    let cfg = CacheConfig::new(&dir, 10, Default::default(), Default::default());
    let cache = EvmCacheBuilder::new(mock_provider())
        .cache_config(cfg)
        .chain_id(8453)
        .build()
        .await;
    // Explicit builder value wins for the CHAINID opcode even when a config is set.
    assert_eq!(cache.chain_id(), 8453);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn set_chain_id_updates_after_construction() -> Result<()> {
    let mut cache = EvmCacheBuilder::new(mock_provider()).build().await;
    assert_eq!(cache.chain_id(), 1);
    cache.set_chain_id(137); // Polygon
    assert_eq!(cache.chain_id(), 137);
    Ok(())
}
