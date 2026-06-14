//! Work with `StorageAccessList` — the compact account/slot touch set captured
//! from simulations.
//!
//! It is smaller than an EIP-2930 transaction access list: accounts and
//! `(account, slot)` pairs are kept as sets so traces can be merged, warm-access
//! gas savings estimated, and slots prefetched. This example builds two touch
//! sets, merges them, estimates EIP-2929 savings, and converts to EIP-2930.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example storage_access_list
//! ```

use alloy_primitives::{Address, U256};
use evm_fork_cache::StorageAccessList;

fn main() {
    let pool = Address::repeat_byte(0xAA);
    let token = Address::repeat_byte(0xBB);

    // First simulation touches the pool's slot0 and liquidity slots.
    let mut first = StorageAccessList::default();
    first.accounts.insert(pool);
    first.slots.insert((pool, U256::from(0))); // slot0
    first.slots.insert((pool, U256::from(4))); // liquidity
    println!(
        "first sim: {} accounts, {} slots",
        first.account_count(),
        first.slot_count()
    );

    // Second simulation re-touches the pool and additionally reads a token balance.
    let mut second = StorageAccessList::default();
    second.accounts.insert(pool);
    second.accounts.insert(token);
    second.slots.insert((pool, U256::from(0))); // slot0 again (overlaps)
    second.slots.insert((token, U256::from(3))); // a balance slot

    // If `second` runs after `first` has warmed state, the overlap is cheaper
    // under EIP-2929 (warm SLOAD/account access).
    let savings = second.marginal_gas_savings(&first);
    println!("estimated warm-access gas saved if run after first: {savings}");

    // Merge both traces into a single prefetch set.
    let mut merged = first.clone();
    merged.extend(&second);
    println!(
        "merged: {} accounts, {} slots",
        merged.account_count(),
        merged.slot_count()
    );

    // Convert to an EIP-2930 access list for inclusion in a transaction.
    let eip2930 = merged.to_eip2930();
    println!("\nEIP-2930 access list ({} entries):", eip2930.0.len());
    for item in &eip2930.0 {
        println!(
            "  {} -> {} storage keys",
            item.address,
            item.storage_keys.len()
        );
    }
}
