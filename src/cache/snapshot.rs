//! Immutable, shareable EVM state snapshots.
//!
//! # Two-tier copy-on-write model (Pillar A)
//!
//! A snapshot is split into two tiers:
//!
//! - a **memoized immutable base** (`BaseState`) flattening the *cold* layer-2
//!   `BlockchainDb` index, shared across successive snapshots by `Arc` — both the
//!   base as a whole and each account's storage map (`Arc<HashMap<U256, U256>>`) —
//!   so taking a snapshot when the cold index is unchanged is an `Arc` handle
//!   copy, never a per-slot deep copy;
//! - a small per-snapshot **overlay** folding the *hot* layer-1 CacheDB delta
//!   (committed sim changes, write-throughs, freshness corrections), which always
//!   shadows the base on a read.
//!
//! [`super::EvmCache::create_snapshot`] memoizes the base (via the internal
//! `refresh_base`) and folds only layer 1 fresh, so its cost tracks *changed*
//! state, not *total* state. The retained
//! [`super::EvmCache::create_snapshot_deep_clone`] produces the same two-tier
//! shape with everything flattened into the base and empty overlay maps; it is the
//! A/B benchmark baseline and the read-equivalence reference.
//!
//! Reads stay O(1) `HashMap` lookups with no locks (Decision D1: `Arc` sharing,
//! not a persistent/HAMT map), so the snapshot is `Send + Sync` and an
//! [`EvmOverlay`] built from it is `Send`.
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
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use revm::primitives::hardfork::SpecId;
use revm::state::{AccountInfo, Bytecode};

/// Memoized, immutable flatten of the **cold layer-2** index (Pillar A).
///
/// Holds layer-2 (`BlockchainDb`) account info and storage only; the layer-1
/// `StorageCleared` / `NotExisting` classification is purely a layer-1 property
/// and lives on [`EvmSnapshot`], not here (see the read rules on
/// [`EvmSnapshot::storage_value`]). Each account's storage is wrapped in an `Arc`
/// so that rebuilding the base on a partial change (copy-on-write) shares the
/// `Arc` handles of unchanged accounts instead of deep-copying their slots.
///
/// Built and memoized by [`EvmCache::refresh_base`](super::EvmCache::refresh_base);
/// shared across snapshots and across threads via `Arc<BaseState>`.
pub(crate) struct BaseState {
    /// Layer-2 account info, by address. (Layer 2 has no `NotExisting` concept;
    /// that classification is purely a layer-1 property — see [`EvmSnapshot`].)
    pub(crate) accounts: HashMap<Address, AccountInfo>,
    /// Layer-2 storage, per account, **shared by `Arc`** so cloning a base — or
    /// rebuilding it for an unchanged account — is a handle copy, never a per-slot
    /// copy.
    pub(crate) storage: HashMap<Address, Arc<HashMap<U256, U256>>>,
    /// Bytecode by `code_hash`, derived from `accounts` at build time.
    pub(crate) code_by_hash: HashMap<B256, Bytecode>,
}

/// Immutable EVM state snapshot — `Send + Sync`, shared via `Arc` across threads.
///
/// A two-tier copy-on-write view (see the [module docs](self)): an `Arc`-shared,
/// memoized cold base (layer 2) plus a small per-snapshot overlay folding the hot
/// layer-1 CacheDB delta, which shadows the base on reads. Lookups (including the
/// public [`storage_value`](Self::storage_value)) are O(1) and lock-free, and
/// reproduce the live cache's layered semantics bit-for-bit.
///
/// Created via [`super::EvmCache::create_snapshot()`]. Each parallel simulation
/// task gets its own [`super::EvmOverlay`] backed by a cheap `Arc::clone` of
/// the snapshot.
pub struct EvmSnapshot {
    /// Memoized, `Arc`-shared cold layer-2 base.
    pub(crate) base: Arc<BaseState>,
    /// Layer-1 accounts that are present to the EVM (`NotExisting` excluded).
    /// Shadows [`BaseState::accounts`] on a read.
    pub(crate) overlay_accounts: HashMap<Address, AccountInfo>,
    /// Layer-1 storage delta, per account. A cleared account (revm
    /// `StorageCleared` / `NotExisting`) ALWAYS has an entry here (possibly empty)
    /// so the cleared rule is decided without consulting the base.
    pub(crate) overlay_storage: HashMap<Address, HashMap<U256, U256>>,
    /// Bytecode introduced by layer 1 (checked before [`BaseState::code_by_hash`]).
    pub(crate) overlay_code_by_hash: HashMap<B256, Bytecode>,
    /// Accounts whose storage is locally complete (revm `StorageCleared` /
    /// `NotExisting`): a slot absent from `overlay_storage` for such an account
    /// reads as ZERO and must NOT fall through to the base or an `ext_db`,
    /// mirroring the live EVM SLOAD and
    /// [`EvmCache::cached_storage_value`](super::EvmCache::cached_storage_value).
    pub(crate) storage_cleared: HashSet<Address>,
    /// Accounts that are absent to the EVM (revm `NotExisting`):
    /// [`account_info`](Self::account_info) returns `None` for them and must NOT
    /// fall through to the base or an `ext_db`, mirroring revm `DbAccount::info()`
    /// and [`EvmCache`](super::EvmCache)'s live account read. These addresses are
    /// excluded from `overlay_accounts` / `overlay_code_by_hash`.
    pub(crate) accounts_not_existing: HashSet<Address>,
    pub(crate) block_hashes: HashMap<u64, B256>,
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
    /// Account info as the EVM sees it: overlay (layer 1) wins, else the base
    /// (layer 2), else `None`.
    ///
    /// Returns `None` for a `NotExisting` account without consulting the base,
    /// mirroring revm `DbAccount::info()` and the live `EvmCache` account read.
    pub(crate) fn account_info(&self, address: Address) -> Option<&AccountInfo> {
        if self.accounts_not_existing.contains(&address) {
            return None;
        }
        self.overlay_accounts
            .get(&address)
            .or_else(|| self.base.accounts.get(&address))
    }

    /// Return the snapshot's value for a storage slot, mirroring the live read.
    ///
    /// Used by the freshness validator to compare a freshly-fetched value against
    /// the value the snapshot was built from. Resolution matches
    /// [`EvmCache::cached_storage_value`](super::EvmCache::cached_storage_value)
    /// over the two tiers: an overlay (layer-1) slot wins; for a cleared account
    /// an absent overlay slot returns `Some(ZERO)` (its storage is locally
    /// complete — the base is never consulted); otherwise the base (layer-2) slot
    /// is returned, or `None` if neither tier has seen the slot.
    pub fn storage_value(&self, address: Address, slot: U256) -> Option<U256> {
        if let Some(account_storage) = self.overlay_storage.get(&address) {
            if let Some(value) = account_storage.get(&slot) {
                return Some(*value);
            }
            // A StorageCleared / NotExisting account's storage is locally complete:
            // an absent slot reads ZERO and never falls through to the base.
            if self.storage_cleared.contains(&address) {
                return Some(U256::ZERO);
            }
            // Non-cleared overlay account: fall through to the base below.
        }
        self.base
            .storage
            .get(&address)
            .and_then(|s| s.get(&slot).copied())
    }

    /// Bytecode by `code_hash`: overlay (layer 1) wins, else the base (layer 2).
    pub(crate) fn code(&self, code_hash: B256) -> Option<&Bytecode> {
        self.overlay_code_by_hash
            .get(&code_hash)
            .or_else(|| self.base.code_by_hash.get(&code_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an empty `Arc<BaseState>` for snapshot literals in tests.
    fn empty_base() -> Arc<BaseState> {
        Arc::new(BaseState {
            accounts: HashMap::new(),
            storage: HashMap::new(),
            code_by_hash: HashMap::new(),
        })
    }

    #[test]
    fn test_snapshot_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EvmSnapshot>();
        assert_send_sync::<Arc<EvmSnapshot>>();
    }

    #[test]
    fn test_empty_snapshot() {
        let snap = EvmSnapshot {
            base: empty_base(),
            overlay_accounts: HashMap::new(),
            overlay_storage: HashMap::new(),
            overlay_code_by_hash: HashMap::new(),
            storage_cleared: HashSet::new(),
            accounts_not_existing: HashSet::new(),
            block_hashes: HashMap::new(),
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
