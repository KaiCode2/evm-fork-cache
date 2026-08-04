//! Cache-owned execution read-set discovery and bulk warming.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_primitives::{Address, U256};
use alloy_rpc_types_eth::TransactionRequest;

use super::{EvmCache, PrewarmReport};
use crate::access_set::StorageAccessList;
use crate::errors::AccessListError;

/// Exact-block hydration result for one learned execution read set.
#[derive(Clone, Debug)]
pub struct ReadSetHydrationReport {
    /// Block identity passed to every provider read.
    pub block: BlockId,
    /// Account headers refreshed from proofs.
    pub accounts_refreshed: usize,
    /// Storage slots refreshed from proofs.
    pub slots_refreshed: usize,
    /// Per-account provider or proof-shape failures.
    pub account_failures: Vec<(Address, String)>,
    /// Runtime-code identity changes that invalidate the learned layout.
    pub code_changes: Vec<(Address, alloy_primitives::B256, alloy_primitives::B256)>,
    /// Required reads still unavailable after hydration.
    pub missing_after: StorageAccessList,
}

impl ReadSetHydrationReport {
    /// Whether every requested dependency is resident and every code identity
    /// still matches the learned layout.
    pub fn is_complete(&self) -> bool {
        self.account_failures.is_empty()
            && self.code_changes.is_empty()
            && self.missing_after.is_empty()
    }
}

/// Callback for deriving calls' read sets via `eth_createAccessList`.
pub type AccessListFetchFn = Arc<
    dyn Fn(
            Vec<TransactionRequest>,
            BlockId,
        ) -> Vec<std::result::Result<StorageAccessList, AccessListError>>
        + Send
        + Sync,
>;

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
    /// Keep discovery local to the later simulation path.
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
                let expected: usize = calls.iter().filter_map(|call| call.expected_slots).sum();
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
    /// Calls skipped by policy or an unavailable fetcher.
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
    pub fn prewarm_read_sets(
        &mut self,
        batch: ReadSetWarmupBatch,
        config: ReadSetWarmupConfig,
    ) -> ReadSetWarmupReport {
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
            return report;
        }
        if !config.should_use_access_lists(&batch.calls) {
            report.skipped_calls = batch.calls.len();
            return report;
        }
        let Some(fetcher) = self.access_list_fetcher.clone() else {
            report.skipped_calls = batch.calls.len();
            return report;
        };

        report.used_access_lists = true;
        let requests = batch.calls.iter().map(|call| call.tx.clone()).collect();
        let mut results = fetcher(requests, self.block).into_iter();
        let mut discovered = StorageAccessList::default();
        for (index, call) in batch.calls.iter().enumerate() {
            let result = results.next().unwrap_or_else(|| {
                Err(AccessListError::query(
                    "eth_createAccessList",
                    "access-list fetcher omitted a result",
                ))
            });
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
        report
    }

    /// Refresh a learned execution read set at this cache's exact block pin.
    ///
    /// Account headers and requested storage values are fetched together with
    /// `eth_getProof`, preventing values from different provider observations
    /// from being combined. Existing runtime bytecode is retained only when the
    /// proof reports the same code hash; a changed hash is surfaced explicitly
    /// so an AMM manifest can be invalidated instead of simulating against a new
    /// layout with stale slot identifiers.
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
            account_failures: Vec::new(),
            code_changes: Vec::new(),
            missing_after: required.clone(),
        };
        if requests.is_empty() {
            report.missing_after = self.snapshot().missing_read_set(required);
            return report;
        }
        let Some(fetcher) = self.account_proof_fetcher.clone() else {
            report.account_failures.extend(
                requests
                    .keys()
                    .copied()
                    .map(|address| (address, "no account proof fetcher installed".to_owned())),
            );
            return report;
        };

        let requested: Vec<_> = requests
            .iter()
            .map(|(address, slots)| (*address, slots.clone()))
            .collect();
        let fetched: HashMap<_, _> = fetcher(requested, block).into_iter().collect();
        let mut fresh_slots = Vec::new();

        for (address, expected_slots) in requests {
            let Some(result) = fetched.get(&address) else {
                report.account_failures.push((
                    address,
                    "account proof fetcher omitted the requested address".to_owned(),
                ));
                continue;
            };
            let proof = match result {
                Ok(proof) => proof,
                Err(error) => {
                    report.account_failures.push((address, error.to_string()));
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
                report.account_failures.push((
                    address,
                    "runtime code was not resident for a deployed account".to_owned(),
                ));
                continue;
            }

            let mut info = current.unwrap_or_default();
            info.balance = proof.balance;
            info.nonce = proof.nonce;
            info.code_hash = proof.code_hash;
            self.write_account_info_through(address, info);
            report.accounts_refreshed += 1;

            let by_slot: HashMap<_, _> = proof.slots.iter().copied().collect();
            let mut complete_slots = true;
            for slot in expected_slots {
                let Some(value) = by_slot.get(&slot).copied() else {
                    report.account_failures.push((
                        address,
                        format!("account proof omitted requested storage slot {slot}"),
                    ));
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
