//! Compact account/storage touch sets captured from EVM execution.
//!
//! This is intentionally smaller than an EIP-2930 transaction access list:
//! it keeps accounts and `(account, slot)` pairs as sets so callers can merge
//! simulation traces, estimate EIP-2929 warm-access savings, and prefetch cache
//! entries without committing to a transaction encoding.

use std::collections::HashSet;

use alloy_eips::eip2930::{AccessList, AccessListItem};
use alloy_primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

/// Accounts and storage slots touched during EVM execution.
///
/// The shape is optimized for simulation bookkeeping: set union, overlap
/// checks, warm-access gas estimation, and storage prefetching.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageAccessList {
    /// Contract addresses touched during execution.
    pub accounts: HashSet<Address>,
    /// Runtime-code hashes requested during execution.
    #[serde(default)]
    pub code_hashes: HashSet<B256>,
    /// `(contract, slot)` pairs read or written during execution.
    pub slots: HashSet<(Address, U256)>,
    /// Historical block numbers requested through the `BLOCKHASH` opcode.
    #[serde(default)]
    pub block_numbers: HashSet<u64>,
}

impl StorageAccessList {
    /// Returns true when no accounts or storage slots were captured.
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
            && self.code_hashes.is_empty()
            && self.slots.is_empty()
            && self.block_numbers.is_empty()
    }

    /// Number of distinct accounts touched by the execution.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Number of distinct storage slots touched by the execution.
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Number of distinct runtime-code hashes touched by the execution.
    pub fn code_hash_count(&self) -> usize {
        self.code_hashes.len()
    }

    /// Number of distinct historical block hashes touched by the execution.
    pub fn block_hash_count(&self) -> usize {
        self.block_numbers.len()
    }

    /// Merge another touch set into this one (set union of accounts and slots).
    ///
    /// Duplicate accounts and `(account, slot)` pairs already present are not
    /// counted twice, so [`StorageAccessList::account_count`] and
    /// [`StorageAccessList::slot_count`] reflect distinct entries after merging.
    ///
    /// # Examples
    ///
    /// ```
    /// use evm_fork_cache::StorageAccessList;
    /// use alloy_primitives::{Address, U256};
    ///
    /// let acct_a = Address::repeat_byte(0x01);
    /// let acct_b = Address::repeat_byte(0x02);
    ///
    /// let mut base = StorageAccessList::default();
    /// base.accounts.insert(acct_a);
    /// base.slots.insert((acct_a, U256::from(1)));
    ///
    /// let mut other = StorageAccessList::default();
    /// other.accounts.insert(acct_a); // overlaps `base`, not double-counted
    /// other.accounts.insert(acct_b);
    /// other.slots.insert((acct_b, U256::from(2)));
    ///
    /// base.extend(&other);
    ///
    /// assert_eq!(base.account_count(), 2);
    /// assert_eq!(base.slot_count(), 2);
    /// assert!(!base.is_empty());
    /// ```
    pub fn extend(&mut self, other: &Self) {
        self.accounts.extend(&other.accounts);
        self.code_hashes.extend(&other.code_hashes);
        self.slots.extend(&other.slots);
        self.block_numbers.extend(&other.block_numbers);
    }

    /// Return the subset of this required read set absent from `available`.
    pub fn missing_from(&self, available: &Self) -> Self {
        Self {
            accounts: self
                .accounts
                .difference(&available.accounts)
                .copied()
                .collect(),
            code_hashes: self
                .code_hashes
                .difference(&available.code_hashes)
                .copied()
                .collect(),
            slots: self.slots.difference(&available.slots).copied().collect(),
            block_numbers: self
                .block_numbers
                .difference(&available.block_numbers)
                .copied()
                .collect(),
        }
    }

    /// Whether every account and slot in this read set is present in
    /// `available`.
    pub fn is_covered_by(&self, available: &Self) -> bool {
        self.accounts.is_subset(&available.accounts)
            && self.code_hashes.is_subset(&available.code_hashes)
            && self.slots.is_subset(&available.slots)
            && self.block_numbers.is_subset(&available.block_numbers)
    }

    /// Compute EIP-2929 gas saved when this touch set runs after `warm`.
    ///
    /// Cold account access costs 2600 gas versus 100 gas when warm, saving
    /// 2500 gas. Cold SLOAD costs 2100 gas versus 100 gas when warm, saving
    /// 2000 gas.
    pub fn marginal_gas_savings(&self, warm: &Self) -> u64 {
        let shared_accounts = self.accounts.intersection(&warm.accounts).count() as u64;
        let shared_slots = self.slots.intersection(&warm.slots).count() as u64;
        shared_accounts * 2500 + shared_slots * 2000
    }

    /// Convert this touch set into an EIP-2930 transaction access list.
    pub fn to_eip2930(&self) -> AccessList {
        let mut by_address: std::collections::BTreeMap<Address, Vec<B256>> = self
            .accounts
            .iter()
            .copied()
            .map(|addr| (addr, Vec::new()))
            .collect();

        for (address, slot) in &self.slots {
            by_address
                .entry(*address)
                .or_default()
                .push(B256::from(*slot));
        }

        AccessList(
            by_address
                .into_iter()
                .map(|(address, mut storage_keys)| {
                    storage_keys.sort_unstable();
                    storage_keys.dedup();
                    AccessListItem {
                        address,
                        storage_keys,
                    }
                })
                .collect(),
        )
    }
}

impl From<&StorageAccessList> for AccessList {
    fn from(value: &StorageAccessList) -> Self {
        value.to_eip2930()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marginal_gas_savings_counts_only_overlap() {
        let account_a = Address::repeat_byte(0x01);
        let account_b = Address::repeat_byte(0x02);
        let slot_1 = U256::from(1);
        let slot_2 = U256::from(2);

        let al = StorageAccessList {
            accounts: [account_a, account_b].into_iter().collect(),
            slots: [(account_a, slot_1), (account_b, slot_2)]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let warm = StorageAccessList {
            accounts: [account_a].into_iter().collect(),
            slots: [(account_b, slot_2)].into_iter().collect(),
            ..Default::default()
        };

        assert_eq!(al.marginal_gas_savings(&warm), 4500);
    }

    #[test]
    fn eip2930_conversion_includes_address_only_entries() {
        let account = Address::repeat_byte(0x01);
        let storage_contract = Address::repeat_byte(0x02);
        let mut al = StorageAccessList::default();
        al.accounts.insert(account);
        al.slots.insert((storage_contract, U256::from(4)));

        let encoded = al.to_eip2930();

        assert_eq!(encoded.0.len(), 2);
        assert!(
            encoded
                .0
                .iter()
                .any(|item| item.address == account && item.storage_keys.is_empty())
        );
        assert!(encoded.0.iter().any(|item| item.address == storage_contract
            && item.storage_keys == vec![B256::from(U256::from(4))]));
    }

    #[test]
    fn missing_from_returns_only_unwarmed_accounts_and_slots() {
        let warm_account = Address::repeat_byte(0x01);
        let cold_account = Address::repeat_byte(0x02);
        let warm_slot = (warm_account, U256::from(1));
        let cold_slot = (cold_account, U256::from(2));
        let required = StorageAccessList {
            accounts: [warm_account, cold_account].into_iter().collect(),
            slots: [warm_slot, cold_slot].into_iter().collect(),
            ..Default::default()
        };
        let available = StorageAccessList {
            accounts: [warm_account].into_iter().collect(),
            slots: [warm_slot].into_iter().collect(),
            ..Default::default()
        };

        let missing = required.missing_from(&available);

        assert_eq!(missing.accounts, [cold_account].into_iter().collect());
        assert_eq!(missing.slots, [cold_slot].into_iter().collect());
        assert!(!required.is_covered_by(&available));
        assert!(available.is_covered_by(&required));
    }

    #[test]
    fn missing_from_includes_code_and_block_hash_dependencies() {
        let warm_code = B256::repeat_byte(0x11);
        let cold_code = B256::repeat_byte(0x22);
        let required = StorageAccessList {
            code_hashes: [warm_code, cold_code].into_iter().collect(),
            block_numbers: [90, 91].into_iter().collect(),
            ..Default::default()
        };
        let available = StorageAccessList {
            code_hashes: [warm_code].into_iter().collect(),
            block_numbers: [90].into_iter().collect(),
            ..Default::default()
        };

        let missing = required.missing_from(&available);

        assert_eq!(missing.code_hashes, [cold_code].into_iter().collect());
        assert_eq!(missing.block_numbers, [91].into_iter().collect());
        assert!(!required.is_covered_by(&available));
    }
}
