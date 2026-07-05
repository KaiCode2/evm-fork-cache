//! Many optimistic sims at once: only the sim whose state actually changed is
//! re-run, and `ValidThrough` classification ages a slot from pinned to volatile.
//!
//! This builds on `freshness_optimistic` (read that first). Three independent
//! `transfer` sims run against one frozen snapshot. A stub fetcher then reports
//! that **only the second sender's** balance has dropped below its transfer
//! amount. The background validator therefore re-runs **only that one sim** (the
//! others' read-sets were unaffected), so the `Corrected` verdict carries a
//! single changed slot and a single re-executed result.
//!
//! It also shows the classification layer: one slot is `Pinned` (never
//! verified), and one is `ValidThrough(block)` — pinned until a target block,
//! then volatile. Advancing the controller's block clock past that block ages it
//! into the volatile set.
//!
//! Runs fully offline against a mocked provider and a stubbed
//! `StorageBatchFetchFn`; no network access.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example freshness_multi_sim
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::Result;
use evm_fork_cache::cache::{SimStatus, StorageBatchFetchFn};
use evm_fork_cache::freshness::{
    AlwaysVerify, FreshnessController, FreshnessRegistry, SimRequest, Validation, Validity,
};

#[path = "support/mock.rs"]
mod mock;

/// Hashed storage slot of `balanceOf[owner]` (mapping at slot 3).
fn balance_slot(owner: Address) -> U256 {
    let key = keccak256((owner, U256::from(mock::MOCK_ERC20_BALANCE_SLOT)).abi_encode());
    U256::from_be_bytes(key.0)
}

fn transfer_calldata(to: Address, amount: u64) -> Bytes {
    Bytes::from(
        mock::MockERC20::transferCall {
            to,
            amount: U256::from(amount),
        }
        .abi_encode(),
    )
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    let token = Address::repeat_byte(0x11);
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_mock_erc20(&mut cache, token);

    // Three senders, each funded 1000, each transferring 100 to a distinct
    // recipient so their read-sets are disjoint.
    let senders = [
        Address::repeat_byte(0xA1),
        Address::repeat_byte(0xB2),
        Address::repeat_byte(0xC3),
    ];
    let recipients = [
        Address::repeat_byte(0x5A),
        Address::repeat_byte(0x5B),
        Address::repeat_byte(0x5C),
    ];
    for s in &senders {
        mock::install_default_account(&mut cache, *s);
        cache.inject_storage_batch(&[(token, balance_slot(*s), U256::from(1000))]);
    }

    // Stub fetcher: every sender's balance is unchanged EXCEPT the second, whose
    // fresh balance has dropped to 50 — too small to cover its transfer of 100.
    let fresh: HashMap<(Address, U256), U256> = HashMap::from([
        ((token, balance_slot(senders[0])), U256::from(1000)),
        ((token, balance_slot(senders[1])), U256::from(50)), // changed!
        ((token, balance_slot(senders[2])), U256::from(1000)),
    ]);
    let fetcher: StorageBatchFetchFn =
        Arc::new(move |requests: Vec<(Address, U256)>, _block: BlockId| {
            requests
                .into_iter()
                .map(|(addr, slot)| {
                    let v = fresh.get(&(addr, slot)).copied().unwrap_or(U256::ZERO);
                    (addr, slot, Ok(v))
                })
                .collect()
        });
    cache.set_storage_batch_fetcher(fetcher);

    // ── Classification layer ───────────────────────────────────────────────
    // Slot 6 is treated as immutable (Pinned, never verified). Slot 7 is valid
    // through block 100, then becomes volatile.
    let mut registry = FreshnessRegistry::new();
    registry.pin_slot(token, U256::from(6));
    registry.valid_through_slot(token, U256::from(7), 100);

    println!("classification:");
    println!(
        "  pinned slot 6 volatile? {} (never)",
        registry.is_volatile(token, U256::from(6), 100)
    );
    println!(
        "  valid-through(100) slot 7 at block 100: volatile? {} (still valid)",
        registry.is_volatile(token, U256::from(7), 100)
    );
    println!(
        "  valid-through(100) slot 7 at block 101: volatile? {} (aged into volatile)\n",
        registry.is_volatile(token, U256::from(7), 101)
    );
    debug_assert_eq!(registry.validity(token, U256::from(6)), Validity::Pinned);

    // ── Optimistic multi-sim run ───────────────────────────────────────────
    let mut controller = FreshnessController::new(registry, AlwaysVerify);
    let requests: Vec<SimRequest> = senders
        .iter()
        .zip(recipients.iter())
        .map(|(&from, &to)| SimRequest::new(from, token, transfer_calldata(to, 100)))
        .collect();

    let sim = controller.run(&mut cache, requests)?;

    // All three optimistic transfers succeed against the 1000-balance snapshot.
    let optimistic_ok: Vec<bool> = sim
        .optimistic()
        .iter()
        .map(|r| matches!(r.status, SimStatus::Success))
        .collect();
    println!("optimistic (against the snapshot): {optimistic_ok:?} (all succeed)");

    match sim.validate().await? {
        Validation::Corrected {
            results,
            changed_slots,
            ..
        } => {
            println!(
                "\nvalidation: Corrected — {} slot(s) changed:",
                changed_slots.len()
            );
            for c in &changed_slots {
                println!("  sender slot {} : {} -> {}", c.slot, c.old, c.new);
            }
            let corrected_ok: Vec<bool> = results
                .iter()
                .map(|r| matches!(r.status, SimStatus::Success))
                .collect();
            println!("corrected results: {corrected_ok:?}");

            // Exactly the second sim flipped success -> revert; the others are
            // untouched (selective re-run).
            assert_eq!(changed_slots.len(), 1, "only one sender's balance changed");
            assert_eq!(corrected_ok, vec![true, false, true]);
            println!(
                "\n→ only sim #2 was re-run (its balance fell below the transfer); \
                 sims #1 and #3 were left as-is."
            );
        }
        Validation::ConfirmedStorage => println!(
            "validation: ConfirmedStorage — storage-only success, account fields \
             NOT verified (unexpected here)"
        ),
        Validation::ConfirmedFull => {
            println!("validation: ConfirmedFull — storage + account verified (unexpected here)")
        }
        Validation::Unverified { reason } => println!("validation: Unverified — {reason}"),
    }

    Ok(())
}
