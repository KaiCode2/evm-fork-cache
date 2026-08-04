//! Cache-owned execution read-set discovery and bulk warming.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_primitives::{Address, U256};
use alloy_rpc_types_eth::TransactionRequest;

use super::{EvmCache, PrewarmReport};
use crate::access_set::StorageAccessList;
use crate::errors::{AccessListError, StorageFetchError};

/// One exact-hydration failure, with enough structure for callers to decide
/// whether to retry, re-warm, or reject a candidate.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadSetHydrationFailure {
    /// The cache has no account-proof callback installed.
    #[error("no account proof fetcher is installed for {address}")]
    ProofFetcherUnavailable {
        /// Account that could not be refreshed.
        address: Address,
    },
    /// The callback returned no result for a requested account.
    #[error("account proof fetcher omitted requested address {address}")]
    ProofResultMissing {
        /// Requested account omitted by the callback.
        address: Address,
    },
    /// The callback returned more than one result for a requested account.
    #[error("account proof fetcher returned duplicate results for {address}")]
    ProofResultDuplicate {
        /// Requested account with ambiguous results.
        address: Address,
    },
    /// The callback returned a result for an account that was not requested.
    #[error("account proof fetcher returned unexpected address {address}")]
    ProofResultUnexpected {
        /// Unrequested account returned by the callback.
        address: Address,
    },
    /// A successful account proof returned one requested storage slot more
    /// than once, making its value ambiguous.
    #[error("account proof for {address} returned duplicate storage slot {slot}")]
    StorageSlotDuplicate {
        /// Account whose proof contained the duplicate slot.
        address: Address,
        /// Requested slot returned more than once.
        slot: U256,
    },
    /// A successful account proof returned a storage slot that was not
    /// requested.
    #[error("account proof for {address} returned unexpected storage slot {slot}")]
    StorageSlotUnexpected {
        /// Account whose proof contained the unrequested slot.
        address: Address,
        /// Unrequested slot returned by the callback.
        slot: U256,
    },
    /// The provider or custom callback failed for one requested account.
    #[error("account proof fetch failed for {address}: {source}")]
    ProofFetch {
        /// Account whose proof failed.
        address: Address,
        /// Typed provider/callback failure.
        #[source]
        source: StorageFetchError,
    },
    /// A deployed account's runtime code is not resident, so its code identity
    /// cannot be validated from a hash-only proof.
    #[error("runtime code {code_hash} is not resident for deployed account {address}")]
    RuntimeCodeUnavailable {
        /// Deployed account requiring runtime code.
        address: Address,
        /// Code hash reported by the exact-block account proof.
        code_hash: alloy_primitives::B256,
    },
    /// A successful account proof omitted one requested storage slot.
    #[error("account proof for {address} omitted requested storage slot {slot}")]
    StorageSlotMissing {
        /// Account whose proof was incomplete.
        address: Address,
        /// Requested slot omitted from the proof result.
        slot: U256,
    },
}

/// Exact-block hydration result for one learned execution read set.
#[derive(Clone, Debug)]
pub struct ReadSetHydrationReport {
    /// Block identity passed to every provider read.
    pub block: BlockId,
    /// Account headers refreshed from proofs.
    pub accounts_refreshed: usize,
    /// Storage slots refreshed from proofs.
    pub slots_refreshed: usize,
    /// Typed provider, callback, code-residency, or proof-shape failures.
    pub failures: Vec<ReadSetHydrationFailure>,
    /// Runtime-code identity changes that invalidate the learned layout.
    pub code_changes: Vec<(Address, alloy_primitives::B256, alloy_primitives::B256)>,
    /// Required reads still unavailable after hydration.
    pub missing_after: StorageAccessList,
}

impl ReadSetHydrationReport {
    /// Whether every requested dependency is resident and every code identity
    /// still matches the learned layout.
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty() && self.code_changes.is_empty() && self.missing_after.is_empty()
    }
}

/// Callback for deriving calls' read sets via `eth_createAccessList`.
///
/// The returned vector must contain exactly one result for each request, in
/// request order. [`EvmCache::prewarm_read_sets`] rejects the whole discovery
/// batch when that cardinality contract is violated.
pub type AccessListFetchFn = Arc<
    dyn Fn(
            Vec<TransactionRequest>,
            BlockId,
        ) -> Vec<std::result::Result<StorageAccessList, AccessListError>>
        + Send
        + Sync,
>;

/// A cache-owned read-set warmup could not honor its selected discovery policy.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadSetWarmupError {
    /// Access-list discovery was selected, but the cache has no discovery
    /// callback installed.
    #[error("access-list discovery was required for {calls} call(s), but no fetcher is installed")]
    AccessListFetcherUnavailable {
        /// Number of calls that could not be discovered.
        calls: usize,
    },
    /// The callback violated the one-result-per-request contract.
    #[error("access-list fetcher returned {actual} result(s) for {expected} request(s)")]
    AccessListResultCountMismatch {
        /// Number of access-list requests issued.
        expected: usize,
        /// Number of callback results returned.
        actual: usize,
    },
}

/// One call whose storage read set may be remotely discovered.
#[derive(Clone, Debug, Default)]
pub struct ReadSetWarmupCall {
    /// RPC transaction passed to `eth_createAccessList`.
    pub tx: TransactionRequest,
    /// Approximate expected slot count used by the automatic strategy.
    pub expected_slots: Option<usize>,
    /// Optional account filter applied before hydration.
    pub restrict_to: Option<Vec<Address>>,
}

/// Declared known slots plus calls with unknown read sets.
#[derive(Clone, Debug, Default)]
pub struct ReadSetWarmupBatch {
    /// Slots to load directly.
    pub known_slots: Vec<(Address, U256)>,
    /// Calls eligible for remote read-set discovery.
    pub calls: Vec<ReadSetWarmupCall>,
}

/// Read-set discovery policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadSetWarmupStrategy {
    /// Use access-list discovery only when the call hints justify its round trip.
    #[default]
    Auto,
    /// Warm declared slots only; leave every call for later local simulation.
    LocalOnly,
    /// Attempt access-list discovery for every declared call.
    AccessList,
}

/// Heuristic configuration for [`EvmCache::prewarm_read_sets`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSetWarmupConfig {
    /// Discovery policy.
    pub strategy: ReadSetWarmupStrategy,
    /// Total hinted slots that activates access-list discovery in automatic mode.
    pub min_expected_slots_for_access_list: usize,
    /// Number of unhinted calls that activates discovery in automatic mode.
    pub min_unhinted_calls_for_access_list: usize,
}

impl Default for ReadSetWarmupConfig {
    fn default() -> Self {
        Self {
            strategy: ReadSetWarmupStrategy::Auto,
            min_expected_slots_for_access_list: 32,
            min_unhinted_calls_for_access_list: 8,
        }
    }
}

impl ReadSetWarmupConfig {
    fn should_use_access_lists(&self, calls: &[ReadSetWarmupCall]) -> bool {
        match self.strategy {
            ReadSetWarmupStrategy::LocalOnly => false,
            ReadSetWarmupStrategy::AccessList => !calls.is_empty(),
            ReadSetWarmupStrategy::Auto => {
                let expected = calls
                    .iter()
                    .filter_map(|call| call.expected_slots)
                    .fold(0usize, usize::saturating_add);
                let unhinted = calls
                    .iter()
                    .filter(|call| call.expected_slots.is_none())
                    .count();
                expected >= self.min_expected_slots_for_access_list
                    || unhinted >= self.min_unhinted_calls_for_access_list
            }
        }
    }
}

/// Outcome of cache-owned read-set warming.
#[derive(Debug, Default)]
pub struct ReadSetWarmupReport {
    /// Direct known-slot hydration.
    pub known: PrewarmReport,
    /// Whether remote access-list discovery was attempted.
    pub used_access_lists: bool,
    /// Calls skipped because the selected policy did not request discovery.
    pub skipped_calls: usize,
    /// Successful access-list probes.
    pub access_list_successes: usize,
    /// Failed probes keyed by call index.
    pub access_list_failures: Vec<(usize, AccessListError)>,
    /// Union of successful filtered read sets.
    pub discovered_access: StorageAccessList,
    /// Hydration result for discovered slots.
    pub discovered: PrewarmReport,
}

impl EvmCache {
    /// Installed access-list discovery callback, if any.
    pub fn access_list_fetcher(&self) -> Option<&AccessListFetchFn> {
        self.access_list_fetcher.as_ref()
    }

    /// Replace the access-list discovery callback.
    pub fn set_access_list_fetcher(&mut self, fetcher: AccessListFetchFn) {
        self.access_list_fetcher = Some(fetcher);
    }

    /// Warm known slots and, when selected by policy, discover and bulk-load
    /// unknown call read sets through cache-owned provider plumbing.
    ///
    /// An access-list callback is mandatory when [`ReadSetWarmupStrategy::AccessList`]
    /// is selected, or when [`ReadSetWarmupStrategy::Auto`] crosses its configured
    /// threshold. A callback result remains a per-call success or failure, but
    /// the callback itself must return exactly one result per request.
    ///
    /// # Errors
    ///
    /// Returns [`ReadSetWarmupError::AccessListFetcherUnavailable`] when remote
    /// discovery was selected without an installed callback, or
    /// [`ReadSetWarmupError::AccessListResultCountMismatch`] when the callback
    /// violates the one-result-per-request contract.
    pub fn prewarm_read_sets(
        &mut self,
        batch: ReadSetWarmupBatch,
        config: ReadSetWarmupConfig,
    ) -> Result<ReadSetWarmupReport, ReadSetWarmupError> {
        let discovery_results =
            if batch.calls.is_empty() || !config.should_use_access_lists(&batch.calls) {
                None
            } else {
                let Some(fetcher) = self.access_list_fetcher.clone() else {
                    return Err(ReadSetWarmupError::AccessListFetcherUnavailable {
                        calls: batch.calls.len(),
                    });
                };
                let requests = batch.calls.iter().map(|call| call.tx.clone()).collect();
                let results = fetcher(requests, self.block);
                if results.len() != batch.calls.len() {
                    return Err(ReadSetWarmupError::AccessListResultCountMismatch {
                        expected: batch.calls.len(),
                        actual: results.len(),
                    });
                }
                Some(results)
            };

        let known = if batch.known_slots.is_empty() {
            PrewarmReport::default()
        } else {
            self.prewarm_slots(&batch.known_slots)
        };
        let mut report = ReadSetWarmupReport {
            known,
            ..Default::default()
        };
        if batch.calls.is_empty() {
            return Ok(report);
        }
        let Some(results) = discovery_results else {
            report.skipped_calls = batch.calls.len();
            return Ok(report);
        };

        report.used_access_lists = true;
        let mut results = results.into_iter();
        let mut discovered = StorageAccessList::default();
        for (index, call) in batch.calls.iter().enumerate() {
            let result = results
                .next()
                .expect("access-list result count was checked above");
            match result {
                Ok(mut access) => {
                    if let Some(restrict_to) = &call.restrict_to {
                        let keep: HashSet<_> = restrict_to.iter().copied().collect();
                        access.accounts.retain(|address| keep.contains(address));
                        access.slots.retain(|(address, _)| keep.contains(address));
                    }
                    discovered.extend(&access);
                    report.access_list_successes += 1;
                }
                Err(error) => report.access_list_failures.push((index, error)),
            }
        }

        let mut slots: Vec<_> = discovered.slots.iter().copied().collect();
        slots.sort_unstable();
        report.discovered_access = discovered;
        if !slots.is_empty() {
            report.discovered = self.prewarm_slots(&slots);
        }
        Ok(report)
    }

    /// Refresh a learned execution read set at this cache's exact block pin.
    ///
    /// Account headers and requested storage values are fetched together with
    /// `eth_getProof`, preventing values from different provider observations
    /// from being combined. Existing runtime bytecode is retained only when the
    /// proof reports the same code hash; a changed hash is surfaced explicitly
    /// so an AMM manifest can be invalidated instead of simulating against a new
    /// layout with stale slot identifiers.
    ///
    /// Hash-only proofs cannot supply runtime bytecode. Code required by the
    /// read set must therefore already be resident, and its identity must match
    /// the proof. Historical `BLOCKHASH` values are likewise never fetched by
    /// this method: they must already be present in the canonical cache. Any
    /// missing code, slot, account, or block hash keeps the returned report
    /// incomplete.
    pub fn hydrate_read_set(&mut self, required: &StorageAccessList) -> ReadSetHydrationReport {
        let block = self.block;
        let mut requests: BTreeMap<Address, Vec<U256>> = BTreeMap::new();
        for address in &required.accounts {
            requests.entry(*address).or_default();
        }
        for (address, slot) in &required.slots {
            requests.entry(*address).or_default().push(*slot);
        }
        for slots in requests.values_mut() {
            slots.sort_unstable();
            slots.dedup();
        }

        let mut report = ReadSetHydrationReport {
            block,
            accounts_refreshed: 0,
            slots_refreshed: 0,
            failures: Vec::new(),
            code_changes: Vec::new(),
            missing_after: required.clone(),
        };
        if requests.is_empty() {
            report.missing_after = self.snapshot().missing_read_set(required);
            return report;
        }
        let Some(fetcher) = self.account_proof_fetcher.clone() else {
            report.failures.extend(
                requests
                    .keys()
                    .copied()
                    .map(|address| ReadSetHydrationFailure::ProofFetcherUnavailable { address }),
            );
            return report;
        };

        let requested: Vec<_> = requests
            .iter()
            .map(|(address, slots)| (*address, slots.clone()))
            .collect();
        let mut fetched = HashMap::new();
        let mut duplicate_addresses = HashSet::new();
        for (address, result) in fetcher(requested, block) {
            if !requests.contains_key(&address) {
                report
                    .failures
                    .push(ReadSetHydrationFailure::ProofResultUnexpected { address });
                continue;
            }
            if duplicate_addresses.contains(&address) || fetched.contains_key(&address) {
                if duplicate_addresses.insert(address) {
                    report
                        .failures
                        .push(ReadSetHydrationFailure::ProofResultDuplicate { address });
                }
                fetched.remove(&address);
                continue;
            }
            fetched.insert(address, result);
        }
        let mut fresh_slots = Vec::new();

        for (address, expected_slots) in requests {
            if duplicate_addresses.contains(&address) {
                continue;
            }
            let Some(result) = fetched.get(&address) else {
                report
                    .failures
                    .push(ReadSetHydrationFailure::ProofResultMissing { address });
                continue;
            };
            let proof = match result {
                Ok(proof) => proof,
                Err(error) => {
                    report.failures.push(ReadSetHydrationFailure::ProofFetch {
                        address,
                        source: error.clone(),
                    });
                    continue;
                }
            };

            let current = self.local_account_info(address);
            if let Some(current) = current.as_ref()
                && current.code_hash != proof.code_hash
            {
                report
                    .code_changes
                    .push((address, current.code_hash, proof.code_hash));
                continue;
            }
            if current.is_none()
                && proof.code_hash != alloy_primitives::B256::ZERO
                && proof.code_hash != revm::primitives::KECCAK_EMPTY
            {
                report
                    .failures
                    .push(ReadSetHydrationFailure::RuntimeCodeUnavailable {
                        address,
                        code_hash: proof.code_hash,
                    });
                continue;
            }

            let mut info = current.unwrap_or_default();
            info.balance = proof.balance;
            info.nonce = proof.nonce;
            info.code_hash = proof.code_hash;
            self.write_account_info_through(address, info);
            report.accounts_refreshed += 1;

            let expected_slot_set: HashSet<_> = expected_slots.iter().copied().collect();
            let mut by_slot = HashMap::new();
            let mut duplicate_slots = HashSet::new();
            for (slot, value) in proof.slots.iter().copied() {
                if !expected_slot_set.contains(&slot) {
                    report
                        .failures
                        .push(ReadSetHydrationFailure::StorageSlotUnexpected { address, slot });
                    continue;
                }
                if duplicate_slots.contains(&slot) || by_slot.contains_key(&slot) {
                    if duplicate_slots.insert(slot) {
                        report
                            .failures
                            .push(ReadSetHydrationFailure::StorageSlotDuplicate { address, slot });
                    }
                    by_slot.remove(&slot);
                    continue;
                }
                by_slot.insert(slot, value);
            }
            let mut complete_slots = true;
            for slot in expected_slots {
                if duplicate_slots.contains(&slot) {
                    complete_slots = false;
                    continue;
                }
                let Some(value) = by_slot.get(&slot).copied() else {
                    report
                        .failures
                        .push(ReadSetHydrationFailure::StorageSlotMissing { address, slot });
                    complete_slots = false;
                    continue;
                };
                fresh_slots.push((address, slot, value));
            }
            if !complete_slots {
                continue;
            }
        }

        report.slots_refreshed = fresh_slots.len();
        if !fresh_slots.is_empty() {
            self.inject_storage_batch_fresh(&fresh_slots);
        }
        report.missing_after = self.snapshot().missing_read_set(required);
        report
    }
}
