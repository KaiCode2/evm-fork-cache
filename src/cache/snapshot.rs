//! Immutable, shareable EVM state snapshots.
//!
//! # Flattening model
//!
//! A snapshot flattens the live cache (CacheDB overlay plus the BlockchainDb
//! backend) into a single immutable, `Send + Sync` view of accounts and
//! storage. The layered lookups of the live cache are collapsed into flat
//! `HashMap`s at creation time, so every read against the snapshot is an O(1)
//! lookup with no locks and no fallback chain.
//!
//! # `Arc` sharing
//!
//! Because the snapshot is read-only it can be wrapped in an `Arc` and shared
//! across threads, letting many parallel simulations read from one consistent
//! state. Handing a new simulation task its state is a cheap `Arc::clone`
//! rather than a deep copy of the accounts/storage maps.
//!
//! # Per-simulation dirty layer
//!
//! Each simulation does not mutate the shared snapshot. Instead it wraps the
//! `Arc<EvmSnapshot>` in an [`EvmOverlay`], which adds a per-simulation
//! *dirty layer* on top: writes (committed account/storage changes, RPC
//! fallbacks, freshness overrides) land in the overlay's own maps and take
//! precedence over the snapshot on subsequent reads. Two overlays built from
//! the same `Arc<EvmSnapshot>` are fully isolated from one another, so
//! simulations can run in parallel without contending for or corrupting the
//! shared base state.
//!
//! [`EvmOverlay`]: super::EvmOverlay

use std::collections::{HashMap, HashSet};

use alloy_primitives::{Address, B256, U256};
use revm::primitives::hardfork::SpecId;
use revm::state::{AccountInfo, Bytecode};

/// Immutable EVM state snapshot — `Send + Sync`, shared via `Arc` across threads.
///
/// Contains merged account info + storage from both CacheDB overlay and
/// BlockchainDb backend, providing a single flat `HashMap` view for O(1) lookups.
///
/// Created via [`super::EvmCache::create_snapshot()`]. Each parallel simulation
/// task gets its own [`super::EvmOverlay`] backed by a cheap `Arc::clone` of
/// the snapshot.
pub struct EvmSnapshot {
    pub(crate) accounts: HashMap<Address, AccountInfo>,
    pub(crate) storage: HashMap<Address, HashMap<U256, U256>>,
    /// Accounts whose storage is locally complete (revm `StorageCleared` /
    /// `NotExisting`): a slot absent from `storage` for such an account reads as
    /// ZERO and must NOT fall through to an `ext_db`, mirroring the live EVM SLOAD
    /// and [`EvmCache::cached_storage_value`](super::EvmCache::cached_storage_value).
    pub(crate) storage_cleared: HashSet<Address>,
    /// Accounts that are absent to the EVM (revm `NotExisting`): `basic` returns
    /// `None` for them and must NOT fall through to an `ext_db`, mirroring revm
    /// `DbAccount::info()` and [`EvmCache`](super::EvmCache)'s live account read.
    /// These addresses are excluded from `accounts` / `code_by_hash`.
    pub(crate) accounts_not_existing: HashSet<Address>,
    pub(crate) block_hashes: HashMap<u64, B256>,
    /// Bytecode lookup by code_hash (derived from accounts at creation time).
    pub(crate) code_by_hash: HashMap<B256, Bytecode>,
    // Block context
    pub(crate) block_number: Option<u64>,
    pub(crate) basefee: Option<u64>,
    pub(crate) coinbase: Option<Address>,
    pub(crate) prevrandao: Option<B256>,
    pub(crate) gas_limit: Option<u64>,
    pub(crate) chain_id: u64,
    pub(crate) timestamp: Option<u64>,
    pub(crate) spec_id: SpecId,
}

impl EvmSnapshot {
    /// Return the snapshot's value for a storage slot, mirroring the live read.
    ///
    /// Used by the freshness validator to compare a freshly-fetched value against
    /// the value the snapshot was built from. Resolution matches
    /// [`EvmCache::cached_storage_value`](super::EvmCache::cached_storage_value):
    /// a captured slot returns its value; a slot absent from a cleared account
    /// (revm `StorageCleared`/`NotExisting`) returns `Some(ZERO)` (its storage is
    /// locally complete); any other absent slot returns `None`.
    pub fn storage_value(&self, address: Address, slot: U256) -> Option<U256> {
        if let Some(value) = self
            .storage
            .get(&address)
            .and_then(|s| s.get(&slot).copied())
        {
            return Some(value);
        }
        if self.storage_cleared.contains(&address) {
            return Some(U256::ZERO);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_snapshot_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EvmSnapshot>();
        assert_send_sync::<Arc<EvmSnapshot>>();
    }

    #[test]
    fn test_empty_snapshot() {
        let snap = EvmSnapshot {
            accounts: HashMap::new(),
            storage: HashMap::new(),
            storage_cleared: HashSet::new(),
            accounts_not_existing: HashSet::new(),
            block_hashes: HashMap::new(),
            code_by_hash: HashMap::new(),
            block_number: Some(100),
            basefee: Some(1000),
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        };
        assert_eq!(snap.chain_id, 42161);
        assert_eq!(snap.block_number, Some(100));
    }
}
