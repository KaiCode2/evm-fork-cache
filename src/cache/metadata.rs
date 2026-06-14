//! Disk-cache configuration and immutable side-data persistence.
//!
//! Alongside the raw EVM state, the cache tracks values that rarely or never
//! change for a given fork — token decimals, pool metadata, and similar
//! immutable data. This module defines the on-disk cache layout
//! ([`CacheConfig`]) and the serializable containers used to persist and reload
//! that side data so subsequent runs avoid re-fetching it over RPC.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, U256};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::warn;

use std::collections::HashSet;

/// Configuration for disk-based caching of EVM state.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Base directory for cache files.
    pub cache_dir: PathBuf,
    /// Chain ID for namespace isolation.
    pub chain_id: u64,
    /// Addresses whose entire storage is preserved on cache load.
    pub maintain_addresses: HashSet<Address>,
    /// Addresses with specific slots to preserve (all other slots purged).
    pub maintain_slots: HashMap<Address, HashSet<U256>>,
}

impl CacheConfig {
    /// Create a new cache configuration.
    pub fn new(
        cache_dir: impl Into<PathBuf>,
        chain_id: u64,
        maintain_addresses: HashSet<Address>,
        maintain_slots: HashMap<Address, HashSet<U256>>,
    ) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            chain_id,
            maintain_addresses,
            maintain_slots,
        }
    }

    /// Get the directory for this chain's cache files.
    pub(crate) fn chain_dir(&self) -> PathBuf {
        self.cache_dir.join(format!("chain_{}", self.chain_id))
    }

    /// Get the path for the bytecode cache file (binary format, persists across blocks).
    pub(crate) fn bytecode_cache_path(&self) -> PathBuf {
        self.chain_dir().join("bytecodes.bin")
    }

    /// Get the path for the immutable data cache file (binary format).
    pub(crate) fn immutable_cache_path(&self) -> PathBuf {
        self.chain_dir().join("immutable_data.bin")
    }

    /// Get the path for the V3 tick snapshot cache file (binary format).
    #[cfg(feature = "protocols")]
    pub(crate) fn tick_snapshot_cache_path(&self) -> PathBuf {
        self.chain_dir().join("v3_tick_snapshots.bin")
    }

    /// Get the path for the EVM state cache file (bincode format).
    ///
    /// This cache stores the complete EVM state (accounts + storage) in
    /// bincode format for fast serialization/deserialization.
    pub fn binary_state_cache_path(&self) -> PathBuf {
        self.chain_dir().join("evm_state.bin")
    }
}

/// Cached metadata for a UniswapV2 pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2PoolMetadata {
    pub token0: Address,
    pub token1: Address,
    /// The blockTimestampLast from getReserves() at cache time.
    /// Used to detect stale cached storage - if the on-chain value differs,
    /// the reserves have changed and cached storage should be purged.
    #[serde(default)]
    pub last_block_timestamp: u32,
}

/// Cached metadata for a UniswapV3 pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3PoolMetadata {
    pub token0: Address,
    pub token1: Address,
    pub fee: u32,
    pub tick_spacing: i32,
}

/// Cached metadata for a Balancer pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancerPoolMetadata {
    pub tokens: Vec<Address>,
    pub weights: Vec<U256>,
    pub swap_fee: U256,
    /// The lastChangeBlock from getPoolTokens() at cache time.
    /// Used to detect stale cached storage - if the on-chain value differs,
    /// the balances have changed and cached storage should be purged.
    #[serde(default)]
    pub last_change_block: U256,
}

/// Cache for immutable on-chain data that doesn't change between blocks.
///
/// This includes:
/// - Token decimals (ERC20 decimals are immutable)
/// - Pool metadata (token addresses, fees, tick spacing)
///
/// By caching this data, we avoid redundant RPC calls across block changes
/// and process restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImmutableDataCache {
    /// Token address -> decimals
    pub token_decimals: HashMap<Address, u8>,
    /// UniswapV2 pool address -> metadata
    pub v2_pools: HashMap<Address, V2PoolMetadata>,
    /// UniswapV3 pool address -> metadata
    pub v3_pools: HashMap<Address, V3PoolMetadata>,
    /// Balancer pool ID (as hex string) -> metadata
    pub balancer_pools: HashMap<String, BalancerPoolMetadata>,
}

impl ImmutableDataCache {
    /// Load immutable data cache from disk (binary format).
    pub fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read(path).ok()?;
        bincode::deserialize(&data)
            .inspect_err(|e| warn!("Failed to parse immutable data cache (bincode): {}", e))
            .ok()
    }

    /// Save immutable data cache to disk (binary format).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = bincode::serialize(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Get cached token decimals.
    pub fn get_token_decimals(&self, token: Address) -> Option<u8> {
        self.token_decimals.get(&token).copied()
    }

    /// Cache token decimals.
    pub fn set_token_decimals(&mut self, token: Address, decimals: u8) {
        self.token_decimals.insert(token, decimals);
    }

    /// Get cached V2 pool metadata.
    pub fn get_v2_pool(&self, address: Address) -> Option<&V2PoolMetadata> {
        self.v2_pools.get(&address)
    }

    /// Cache V2 pool metadata.
    pub fn set_v2_pool(&mut self, address: Address, metadata: V2PoolMetadata) {
        self.v2_pools.insert(address, metadata);
    }

    /// Get cached V3 pool metadata.
    pub fn get_v3_pool(&self, address: Address) -> Option<&V3PoolMetadata> {
        self.v3_pools.get(&address)
    }

    /// Cache V3 pool metadata.
    pub fn set_v3_pool(&mut self, address: Address, metadata: V3PoolMetadata) {
        self.v3_pools.insert(address, metadata);
    }

    /// Get cached Balancer pool metadata.
    pub fn get_balancer_pool(&self, pool_id: B256) -> Option<&BalancerPoolMetadata> {
        self.balancer_pools.get(&format!("{:?}", pool_id))
    }

    /// Cache Balancer pool metadata.
    pub fn set_balancer_pool(&mut self, pool_id: B256, metadata: BalancerPoolMetadata) {
        self.balancer_pools
            .insert(format!("{:?}", pool_id), metadata);
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.token_decimals.is_empty()
            && self.v2_pools.is_empty()
            && self.v3_pools.is_empty()
            && self.balancer_pools.is_empty()
    }

    /// Get the total number of cached entries.
    pub fn len(&self) -> usize {
        self.token_decimals.len()
            + self.v2_pools.len()
            + self.v3_pools.len()
            + self.balancer_pools.len()
    }
}
