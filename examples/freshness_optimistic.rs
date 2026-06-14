//! Optimistic execution with deferred validation — a `Corrected` verdict.
//!
//! The freshness controller runs a simulation against a frozen snapshot and
//! returns its result *immediately*, while a background task concurrently
//! re-checks the volatile storage the sim read. If a value the sim depended on
//! has changed, the affected sim is re-run with the fresh value and the verdict
//! is [`Validation::Corrected`].
//!
//! Here a MockERC20 holder starts with a balance of 1000, so the optimistic
//! `transfer(100)` succeeds. A **stub fetcher** then reports the balance has
//! dropped to 50 — too small to cover the transfer — so the corrected re-run
//! reverts. One slot is pinned (immutable) to show it is never re-verified.
//!
//! Runs fully offline against a mocked provider and a stubbed
//! `StorageBatchFetchFn`; no network access.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example freshness_optimistic
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::Result;
use evm_fork_cache::cache::StorageBatchFetchFn;
use evm_fork_cache::freshness::{
    AlwaysVerify, FreshnessController, FreshnessRegistry, SimRequest, Validation,
};

#[path = "support/mock.rs"]
mod mock;

/// Hashed storage slot of `balanceOf[owner]` (mapping at slot 3).
fn balance_slot(owner: Address) -> U256 {
    let key = keccak256((owner, U256::from(mock::MOCK_ERC20_BALANCE_SLOT)).abi_encode());
    U256::from_be_bytes(key.0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;

    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);
    // Address::ZERO is the default block coinbase, touched for gas accounting.
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, owner);
    mock::install_mock_erc20(&mut cache, token);

    // Owner is funded with 1000 tokens — enough for the optimistic transfer.
    let owner_slot = balance_slot(owner);
    cache.inject_storage_batch(&[(token, owner_slot, U256::from(1000))]);

    // Stub the batch fetcher: report the owner's balance has DROPPED to 50.
    // (An unmapped slot reads as zero, matching how a sim reads an unseen slot.)
    let fresh: HashMap<(Address, U256), U256> =
        HashMap::from([((token, owner_slot), U256::from(50))]);
    let fetcher: StorageBatchFetchFn = Arc::new(move |requests: Vec<(Address, U256)>| {
        requests
            .into_iter()
            .map(|(addr, slot)| {
                let value = fresh.get(&(addr, slot)).copied().unwrap_or(U256::ZERO);
                (addr, slot, Ok(value))
            })
            .collect()
    });
    cache.set_storage_batch_fetcher(fetcher);

    // Classification: the balance slot is volatile (default), and slot 6 (a
    // would-be immutable like `token0`) is pinned so it is never re-verified.
    let mut registry = FreshnessRegistry::new();
    registry.pin_slot(token, U256::from(6));

    let mut controller = FreshnessController::new(registry, AlwaysVerify);

    // A non-committing `transfer(recipient, 100)` evaluation sim.
    let calldata = Bytes::from(
        mock::MockERC20::transferCall {
            to: recipient,
            amount: U256::from(100),
        }
        .abi_encode(),
    );
    let request = SimRequest::new(owner, token, calldata);

    // run() returns as soon as the optimistic sim finishes — without awaiting RPC.
    let sim = controller.run(&mut cache, vec![request])?;

    let optimistic = &sim.optimistic()[0];
    let optimistic_succeeded = !optimistic.logs.is_empty();
    println!("optimistic result (computed immediately, against the snapshot):");
    println!("  gas_used = {}", optimistic.gas_used);
    println!(
        "  transfer {} (emitted {} log(s))\n",
        if optimistic_succeeded {
            "SUCCEEDED"
        } else {
            "reverted"
        },
        optimistic.logs.len()
    );

    // Now await the deferred validation verdict.
    match sim.validate().await {
        Validation::Confirmed => {
            println!("validation: Confirmed — nothing the sim read had changed");
        }
        Validation::Corrected { results, changed } => {
            println!("validation: Corrected — a slot the sim read had changed:");
            for c in &changed {
                println!("  {} slot {} : {} -> {}", c.address, c.slot, c.old, c.new);
            }
            let corrected = &results[0];
            let corrected_succeeded = !corrected.logs.is_empty();
            println!(
                "\ncorrected re-run: gas_used = {}, transfer {} (emitted {} log(s))",
                corrected.gas_used,
                if corrected_succeeded {
                    "SUCCEEDED"
                } else {
                    "REVERTED (insufficient fresh balance)"
                },
                corrected.logs.len()
            );
            assert!(
                optimistic_succeeded && !corrected_succeeded,
                "this example demonstrates an optimistic success corrected to a revert"
            );
        }
        Validation::Unverified { reason } => {
            println!("validation: Unverified — {reason}");
        }
    }

    Ok(())
}
