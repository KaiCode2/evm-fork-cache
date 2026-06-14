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
    /// `(contract, slot)` pairs read or written during execution.
    pub slots: HashSet<(Address, U256)>,
}

impl StorageAccessList {
    /// Returns true when no accounts or storage slots were captured.
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.slots.is_empty()
    }

    /// Number of distinct accounts touched by the execution.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Number of distinct storage slots touched by the execution.
    pub fn slot_count(&self) -> usize {
        self.slots.len()
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

    /// Merge another touch set into this one.
    pub fn extend(&mut self, other: &Self) {
        self.accounts.extend(&other.accounts);
        self.slots.extend(&other.slots);
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
        };
        let warm = StorageAccessList {
            accounts: [account_a].into_iter().collect(),
            slots: [(account_b, slot_2)].into_iter().collect(),
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
}
