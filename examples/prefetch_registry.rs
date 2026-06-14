//! Record storage touch-sets across cycles and persist them, so the next cycle
//! can batch-prefetch slots before the EVM touches them.
//!
//! The registry stores access lists by phase: either one aggregated list per
//! phase, or per-address lists for selective prefetch. This example records both
//! shapes, round-trips them through disk, and inspects the result. (Actually
//! prefetching requires a live cache with a batch fetcher — see
//! `PrefetchRegistry::prefetch_phase` / `prefetch_keyed`.)
//!
//! Runs fully offline.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example prefetch_registry
//! ```

use alloy_primitives::{Address, U256};
use evm_fork_cache::StorageAccessList;
use evm_fork_cache::prefetch_registry::PrefetchRegistry;

fn main() {
    let pool = Address::repeat_byte(0xAA);
    let vault_a = Address::repeat_byte(0x01);
    let vault_b = Address::repeat_byte(0x02);

    let mut registry = PrefetchRegistry::default();

    // An aggregated phase: one access list covering a batch of view calls.
    let mut pool_refresh = StorageAccessList::default();
    pool_refresh.accounts.insert(pool);
    pool_refresh.slots.insert((pool, U256::from(0)));
    pool_refresh.slots.insert((pool, U256::from(4)));
    registry.record("pool_refresh", pool_refresh);

    // A keyed phase: per-address lists, so the next cycle can prefetch only the
    // addresses it is about to simulate.
    let mut al_a = StorageAccessList::default();
    al_a.slots.insert((vault_a, U256::from(10)));
    registry.record_keyed("per_vault", vault_a, al_a);

    let mut al_b = StorageAccessList::default();
    al_b.slots.insert((vault_b, U256::from(20)));
    registry.record_keyed("per_vault", vault_b, al_b);

    // Persist to disk (bincode) and reload — the shape survives the round trip.
    let path = std::env::temp_dir().join("evm_fork_cache_example_prefetch.bin");
    registry.save(&path);
    let loaded = PrefetchRegistry::load(&path);

    let aggregated = loaded.phase_slots("pool_refresh");
    println!("pool_refresh phase has {} slots", aggregated.len());
    for (addr, slot) in &aggregated {
        println!("  {addr} slot {slot}");
    }

    // A missing phase is simply empty, never an error.
    println!(
        "unknown phase has {} slots",
        loaded.phase_slots("does_not_exist").len()
    );

    let _ = std::fs::remove_file(&path);
}
