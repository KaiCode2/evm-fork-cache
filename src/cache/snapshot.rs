//! Immutable, shareable EVM state snapshots.
//!
//! A snapshot flattens the live cache (CacheDB overlay plus the BlockchainDb
//! backend) into a single immutable, `Send + Sync` view of accounts and
//! storage. Because it is read-only it can be wrapped in an `Arc` and shared
//! across threads, letting many parallel simulations read from one consistent
//! state while each layers its own writes through a separate overlay.

use std::collections::HashMap;

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
    pub(crate) block_hashes: HashMap<u64, B256>,
    /// Bytecode lookup by code_hash (derived from accounts at creation time).
    pub(crate) code_by_hash: HashMap<B256, Bytecode>,
    // Block context
    pub(crate) block_number: Option<u64>,
    pub(crate) basefee: Option<u64>,
    pub(crate) chain_id: u64,
    pub(crate) timestamp: Option<u64>,
    pub(crate) spec_id: SpecId,
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
            block_hashes: HashMap::new(),
            code_by_hash: HashMap::new(),
            block_number: Some(100),
            basefee: Some(1000),
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        };
        assert_eq!(snap.chain_id, 42161);
        assert_eq!(snap.block_number, Some(100));
    }
}
