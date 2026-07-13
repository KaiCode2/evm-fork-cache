//! Protocol-neutral, hash-pinned storage fetch artifacts for background cold start.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;

use crate::cache::{
    EvmCache, StorageBatchConfig, StorageBatchFetchFn, StorageFetchStrategy,
    provider_storage_fetcher,
};
use crate::errors::StorageFetchResult;
use crate::freshness::{SlotFetch, SlotOutcome};
use crate::state_update::{StateDiff, StateUpdate};

/// One storage identity requested by a split cold-start round.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageSlotRequest {
    address: Address,
    slot: U256,
}

impl StorageSlotRequest {
    /// Construct one storage identity.
    pub const fn new(address: Address, slot: U256) -> Self {
        Self { address, slot }
    }

    /// Storage-owning contract.
    pub const fn address(self) -> Address {
        self.address
    }

    /// Storage key.
    pub const fn slot(self) -> U256 {
        self.slot
    }
}

impl From<(Address, U256)> for StorageSlotRequest {
    fn from((address, slot): (Address, U256)) -> Self {
        Self::new(address, slot)
    }
}

/// A slot value fetched authoritatively at one exact block hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PreparedStorageValue {
    address: Address,
    slot: U256,
    value: U256,
}

impl PreparedStorageValue {
    /// Construct one prepared storage value.
    pub const fn new(address: Address, slot: U256, value: U256) -> Self {
        Self {
            address,
            slot,
            value,
        }
    }

    /// Storage-owning contract.
    pub const fn address(self) -> Address {
        self.address
    }

    /// Storage key.
    pub const fn slot(self) -> U256 {
        self.slot
    }

    /// Authoritative value at [`PreparedStoragePatch::block_hash`].
    pub const fn value(self) -> U256 {
        self.value
    }
}

/// Worker-produced storage values ready for one serialized cache commit.
///
/// Construction is intentionally infallible: the cache-owner commit remains the
/// trust boundary and validates duplicate identities and baseline freshness
/// immediately before mutating either cache layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedStoragePatch {
    block_hash: B256,
    values: Vec<PreparedStorageValue>,
}

impl EvmCache {
    /// Atomically apply a worker-produced storage patch at the cache's exact hash.
    ///
    /// Validation completes before the first write: duplicate identities and a
    /// cache pin that does not match the patch hash reject the entire patch. A
    /// valid patch is written through the cache's batched absolute-slot funnel,
    /// healing layer 2 and every already-materialized layer-1 account together.
    /// This method performs no provider work.
    pub fn apply_prepared_storage_patch(
        &mut self,
        patch: &PreparedStoragePatch,
    ) -> Result<StateDiff, PreparedStoragePatchError> {
        let mut identities = HashSet::with_capacity(patch.values.len());
        for value in &patch.values {
            if !identities.insert((value.address, value.slot)) {
                return Err(PreparedStoragePatchError::DuplicateSlot {
                    address: value.address,
                    slot: value.slot,
                });
            }
        }

        let cache_hash = match self.block() {
            BlockId::Hash(hash) => Some(hash.block_hash),
            BlockId::Number(_) => None,
        };
        if cache_hash != Some(patch.block_hash) {
            return Err(PreparedStoragePatchError::BaselineMismatch {
                prepared: patch.block_hash,
                cache: cache_hash,
            });
        }

        let updates: Vec<_> = patch
            .values
            .iter()
            .map(|value| StateUpdate::slot(value.address, value.slot, value.value))
            .collect();
        Ok(self.apply_updates(&updates))
    }
}

impl PreparedStoragePatch {
    /// Construct a patch tied to one exact post-block hash.
    pub fn new(block_hash: B256, values: impl IntoIterator<Item = PreparedStorageValue>) -> Self {
        Self {
            block_hash,
            values: values.into_iter().collect(),
        }
    }

    /// Exact hash used to fetch every value.
    pub const fn block_hash(&self) -> B256 {
        self.block_hash
    }

    /// Values in request order.
    pub fn values(&self) -> &[PreparedStorageValue] {
        &self.values
    }
}

/// Slot-only work for one exact-hash cold-start round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRoundRequest {
    block_hash: B256,
    verify: Vec<StorageSlotRequest>,
    probe: Vec<StorageSlotRequest>,
}

impl StorageRoundRequest {
    /// Construct a slot-only round. Request identity validation happens at
    /// [`StorageRoundFetcher::fetch`] before provider work begins.
    pub fn new<V, P, VI, PI>(block_hash: B256, verify: V, probe: P) -> Self
    where
        V: IntoIterator<Item = VI>,
        P: IntoIterator<Item = PI>,
        VI: Into<StorageSlotRequest>,
        PI: Into<StorageSlotRequest>,
    {
        Self {
            block_hash,
            verify: verify.into_iter().map(Into::into).collect(),
            probe: probe.into_iter().map(Into::into).collect(),
        }
    }

    /// Exact canonical hash all provider reads must use.
    pub const fn block_hash(&self) -> B256 {
        self.block_hash
    }

    /// Slots whose successful values should become a prepared commit patch.
    pub fn verify(&self) -> &[StorageSlotRequest] {
        &self.verify
    }

    /// Slots whose outcomes are observational and must not be committed.
    pub fn probe(&self) -> &[StorageSlotRequest] {
        &self.probe
    }
}

/// Complete, non-mutating result of one storage-only provider round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRoundFetch {
    block_hash: B256,
    verified: Vec<SlotOutcome>,
    probed: Vec<SlotOutcome>,
    patch: PreparedStoragePatch,
}

impl StorageRoundFetch {
    /// Exact hash used by the provider read.
    pub const fn block_hash(&self) -> B256 {
        self.block_hash
    }

    /// One classified outcome per verify request, in request order.
    pub fn verified(&self) -> &[SlotOutcome] {
        &self.verified
    }

    /// One classified outcome per probe request, in request order.
    pub fn probed(&self) -> &[SlotOutcome] {
        &self.probed
    }

    /// Successfully fetched verify values ready for atomic cache-owner commit.
    pub const fn patch(&self) -> &PreparedStoragePatch {
        &self.patch
    }

    /// Consume the result and return its prepared commit patch.
    pub fn into_patch(self) -> PreparedStoragePatch {
        self.patch
    }

    /// Consume the result without cloning planner outcomes or prepared values.
    pub fn into_parts(self) -> (Vec<SlotOutcome>, Vec<SlotOutcome>, PreparedStoragePatch) {
        (self.verified, self.probed, self.patch)
    }
}

/// Cloneable, thread-safe provider handle for non-mutating storage rounds.
#[derive(Clone)]
pub struct StorageRoundFetcher {
    fetcher: StorageBatchFetchFn,
}

impl fmt::Debug for StorageRoundFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRoundFetcher")
            .finish_non_exhaustive()
    }
}

impl StorageRoundFetcher {
    /// Wrap an existing protocol-neutral storage batch provider.
    pub fn new(fetcher: StorageBatchFetchFn) -> Self {
        Self { fetcher }
    }

    /// Build a worker-owned provider fetcher without constructing an
    /// [`EvmCache`].
    pub fn from_provider<P>(
        provider: Arc<P>,
        batch_config: StorageBatchConfig,
        strategy: StorageFetchStrategy,
    ) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        Self::new(provider_storage_fetcher(provider, batch_config, strategy))
    }

    /// Fetch one slot-only round without holding or mutating an [`EvmCache`](crate::cache::EvmCache).
    ///
    /// The provider receives an exact canonical hash pin. Its response is
    /// checked for missing, unexpected, or duplicate identities before a result
    /// is returned; per-slot provider failures remain classified outcomes so a
    /// resumable planner can decide its next round.
    pub fn fetch(
        &self,
        request: &StorageRoundRequest,
    ) -> Result<StorageRoundFetch, StorageRoundFetchError> {
        let mut requested = HashSet::with_capacity(request.verify.len() + request.probe.len());
        for slot in request.verify.iter().chain(&request.probe) {
            if !requested.insert((slot.address, slot.slot)) {
                return Err(StorageRoundFetchError::DuplicateRequest {
                    address: slot.address,
                    slot: slot.slot,
                });
            }
        }

        let provider_requests: Vec<_> = request
            .verify
            .iter()
            .chain(&request.probe)
            .map(|slot| (slot.address, slot.slot))
            .collect();
        let response = (self.fetcher)(
            provider_requests,
            BlockId::from((request.block_hash, Some(true))),
        );

        let mut returned: HashMap<(Address, U256), StorageFetchResult<U256>> =
            HashMap::with_capacity(response.len());
        for (address, slot, value) in response {
            if !requested.contains(&(address, slot)) {
                return Err(StorageRoundFetchError::UnexpectedResult { address, slot });
            }
            if returned.insert((address, slot), value).is_some() {
                return Err(StorageRoundFetchError::DuplicateResult { address, slot });
            }
        }

        for &(address, slot) in &requested {
            if !returned.contains_key(&(address, slot)) {
                return Err(StorageRoundFetchError::MissingResult { address, slot });
            }
        }

        let mut patch = Vec::with_capacity(request.verify.len());
        let verified = take_outcomes(&request.verify, &mut returned, Some(&mut patch));
        let probed = take_outcomes(&request.probe, &mut returned, None);
        Ok(StorageRoundFetch {
            block_hash: request.block_hash,
            verified,
            probed,
            patch: PreparedStoragePatch::new(request.block_hash, patch),
        })
    }
}

fn take_outcomes(
    requests: &[StorageSlotRequest],
    returned: &mut HashMap<(Address, U256), StorageFetchResult<U256>>,
    mut patch: Option<&mut Vec<PreparedStorageValue>>,
) -> Vec<SlotOutcome> {
    requests
        .iter()
        .map(|request| {
            let fetched = returned
                .remove(&(request.address, request.slot))
                .expect("provider response completeness validated above");
            let fetch = match fetched {
                Ok(value) => {
                    if let Some(values) = patch.as_deref_mut() {
                        values.push(PreparedStorageValue::new(
                            request.address,
                            request.slot,
                            value,
                        ));
                    }
                    if value == U256::ZERO {
                        SlotFetch::Zero
                    } else {
                        SlotFetch::Value(value)
                    }
                }
                Err(error) => SlotFetch::FetchFailed {
                    reason: error.to_string(),
                },
            };
            SlotOutcome {
                address: request.address,
                slot: request.slot,
                fetch,
            }
        })
        .collect()
}

/// Invalid storage-round request or malformed provider response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StorageRoundFetchError {
    /// The same identity appeared more than once across verify/probe work.
    #[error("duplicate storage-round request for ({address}, {slot})")]
    DuplicateRequest {
        /// Storage-owning contract.
        address: Address,
        /// Storage key.
        slot: U256,
    },
    /// The provider returned an identity that was not requested.
    #[error("storage provider returned unexpected slot ({address}, {slot})")]
    UnexpectedResult {
        /// Storage-owning contract.
        address: Address,
        /// Storage key.
        slot: U256,
    },
    /// The provider returned the same identity more than once.
    #[error("storage provider returned duplicate slot ({address}, {slot})")]
    DuplicateResult {
        /// Storage-owning contract.
        address: Address,
        /// Storage key.
        slot: U256,
    },
    /// The provider omitted a requested identity.
    #[error("storage provider omitted requested slot ({address}, {slot})")]
    MissingResult {
        /// Storage-owning contract.
        address: Address,
        /// Storage key.
        slot: U256,
    },
}

/// Invalid worker-produced storage patch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PreparedStoragePatchError {
    /// The artifact contains multiple values for one storage identity.
    #[error("prepared storage patch contains duplicate slot ({address}, {slot})")]
    DuplicateSlot {
        /// Storage-owning contract.
        address: Address,
        /// Storage key.
        slot: U256,
    },
    /// The actor cache is no longer pinned to the artifact's exact block hash.
    #[error("prepared storage patch baseline mismatch: prepared {prepared}, cache {cache:?}")]
    BaselineMismatch {
        /// Hash used by the background provider fetch.
        prepared: B256,
        /// Current cache hash, or `None` when the cache is number/tag-pinned.
        cache: Option<B256>,
    },
}
