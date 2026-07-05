//! Disk-cache configuration and immutable side-data persistence.
//!
//! Alongside the raw EVM state, the cache tracks values that rarely or never
//! change for a given fork, currently ERC-20 token decimals. This module defines
//! the on-disk cache layout
//! ([`CacheConfig`]) and the serializable containers used to persist and reload
//! that side data so subsequent runs avoid re-fetching it over RPC.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, U256};
use serde::{Deserialize, Serialize};

use std::collections::HashSet;

use super::versioned;
use crate::errors::PersistenceError;

const IMMUTABLE_CACHE_MAGIC: &[u8; 8] = b"EFCMETA\0";
const IMMUTABLE_CACHE_VERSION: u32 = 2;

/// Configuration for disk-based caching of EVM state.
///
/// Enables on-disk persistence of fetched fork state. Cache files are laid out
/// per chain under `cache_dir` (see [`CacheConfig::binary_state_cache_path`] and
/// the other path helpers), so multiple chains can share one base directory
/// without colliding.
///
/// The `maintain_*` fields drive selective retention when state is reloaded:
/// `maintain_addresses` whitelists accounts whose storage is kept in full, while
/// `maintain_slots` whitelists individual slots for accounts whose remaining
/// storage should be purged. Together they let a cache load keep only the
/// long-lived state worth reusing and drop the rest.
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
    ///
    /// `cache_dir` is the base directory for all per-chain cache files,
    /// `chain_id` namespaces them, and `maintain_addresses` / `maintain_slots`
    /// select which state survives a reload (see the type-level docs).
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

    /// Get the path for the code-seed mark cache file (binary format).
    ///
    /// Saved by `flush()` strictly before `bytecodes.bin` so persisted code
    /// can never outrun the trust marks describing it.
    pub(crate) fn code_seeds_cache_path(&self) -> PathBuf {
        self.chain_dir().join("code_seeds.bin")
    }

    /// Get the path for the EVM state cache file (bincode format).
    ///
    /// This cache stores the complete EVM state (accounts + storage) in
    /// bincode format for fast serialization/deserialization.
    pub fn binary_state_cache_path(&self) -> PathBuf {
        self.chain_dir().join("evm_state.bin")
    }
}

/// Cache for immutable on-chain data that doesn't change between blocks.
///
/// This includes:
/// - Token decimals (ERC20 decimals are immutable)
///
/// By caching this data, we avoid redundant RPC calls across block changes
/// and process restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImmutableDataCache {
    /// Token address -> decimals
    pub token_decimals: HashMap<Address, u8>,
}

impl ImmutableDataCache {
    /// Load immutable data cache from disk (binary format).
    ///
    /// Returns `None` if `path` cannot be read, fails the magic/version check, or
    /// the payload is not valid bincode for this type. Callers should treat
    /// `None` as "no cache yet" and start fresh.
    pub fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read(path).ok()?;
        versioned::decode(
            &data,
            IMMUTABLE_CACHE_MAGIC,
            IMMUTABLE_CACHE_VERSION,
            "immutable data cache",
        )
    }

    /// Save immutable data cache to disk (binary format).
    ///
    /// Creates the parent directory if it does not exist, then writes the
    /// bincode-serialized cache to `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created, if bincode
    /// serialization fails, or if writing the file fails.
    pub fn save(&self, path: &Path) -> Result<(), PersistenceError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| PersistenceError::create_dir(parent, err))?;
        }
        let data = versioned::encode(
            IMMUTABLE_CACHE_MAGIC,
            IMMUTABLE_CACHE_VERSION,
            self,
            "immutable data cache",
        )?;
        std::fs::write(path, data).map_err(|err| PersistenceError::write(path, err))?;
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

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.token_decimals.is_empty()
    }

    /// Get the total number of cached entries.
    pub fn len(&self) -> usize {
        self.token_decimals.len()
    }
}
