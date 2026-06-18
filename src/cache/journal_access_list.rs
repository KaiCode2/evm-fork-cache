use alloy_eips::eip2930::{AccessList, AccessListItem};
use alloy_primitives::B256;

/// Extract an EIP-2930 access list from the EVM journaled state.
///
/// After a transaction executes, `journaled_state.state` contains all accounts
/// and storage slots that were touched. This converts them into an `AccessList`
/// suitable for inclusion in a transaction, ensuring all accessed storage is warm.
pub(super) fn extract_access_list(state: &revm::state::EvmState) -> AccessList {
    let items: Vec<AccessListItem> = state
        .iter()
        .filter(|(_, account)| account.is_touched())
        .map(|(address, account)| AccessListItem {
            address: *address,
            storage_keys: account
                .storage
                .keys()
                .map(|slot| B256::from(*slot))
                .collect(),
        })
        .collect();
    AccessList(items)
}

pub(super) fn merge_access_lists(access_lists: impl IntoIterator<Item = AccessList>) -> AccessList {
    let mut merged: Vec<AccessListItem> = Vec::new();
    for access_list in access_lists {
        for item in access_list.0 {
            if let Some(existing) = merged
                .iter_mut()
                .find(|existing| existing.address == item.address)
            {
                for key in item.storage_keys {
                    if !existing.storage_keys.contains(&key) {
                        existing.storage_keys.push(key);
                    }
                }
            } else {
                merged.push(item);
            }
        }
    }
    AccessList(merged)
}
