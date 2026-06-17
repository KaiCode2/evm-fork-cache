//! Fan one frozen snapshot out to many parallel, isolated simulations.
//!
//! This is the crate's headline workflow: freeze the cache into an immutable
//! `Arc<EvmSnapshot>` with `create_snapshot()`, then give each task its own
//! `EvmOverlay` (a cheap `Arc::clone` of the snapshot plus a private dirty
//! layer). Overlays are `Send`, so the tasks can run on separate threads, and a
//! write committed in one overlay is invisible to its siblings.
//!
//! Here three threads each commit a different transfer from the same starting
//! state and read back the sender's balance — each result reflects only that
//! overlay's own transfer, proving the isolation.
//!
//! Runs fully offline against a mocked provider (overlays use `ext_db: None`).
//!
//! Run with:
//!
//! ```sh
//! cargo run --example parallel_overlays
//! ```

use std::sync::Arc;
use std::thread;

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::{Result, anyhow};
use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot};
use revm::context::result::ExecutionResult;

#[path = "support/mock.rs"]
mod mock;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;

    let token = Address::repeat_byte(0x11);
    let sender = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);
    // Address::ZERO is the default block coinbase, touched for gas accounting.
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, sender);
    mock::install_default_account(&mut cache, recipient);
    mock::install_mock_erc20(&mut cache, token);

    let slot = U256::from(mock::MOCK_ERC20_BALANCE_SLOT);
    let start = U256::from(1_000u64);
    cache.insert_mapping_storage_slot(token, slot, sender, start)?;
    cache.insert_mapping_storage_slot(token, slot, recipient, U256::ZERO)?;

    // Freeze the current state into an immutable, Send + Sync snapshot.
    let snapshot = cache.create_snapshot();
    println!("frozen snapshot: sender starts with {start}\n");

    // Fan out: each thread gets a cheap Arc::clone of the snapshot and its own
    // overlay, commits a different transfer, and reads the sender's balance back.
    let amounts = [100u64, 250, 600];
    let mut handles = Vec::new();
    for amount in amounts {
        let snap: Arc<EvmSnapshot> = snapshot.clone();
        handles.push(thread::spawn(move || -> Result<U256> {
            let mut overlay = EvmOverlay::new(snap, None);

            let calldata = Bytes::from(
                mock::MockERC20::transferCall {
                    to: recipient,
                    amount: U256::from(amount),
                }
                .abi_encode(),
            );
            // commit = true writes into THIS overlay's private dirty layer only.
            overlay
                .simulate_with_transfer_tracking(
                    sender,
                    token,
                    calldata,
                    sender,
                    Some([token]),
                    true,
                )
                .map_err(|e| anyhow!("overlay simulation failed: {e}"))?;

            overlay_balance_of(&mut overlay, token, sender)
        }));
    }

    for (amount, handle) in amounts.iter().zip(handles) {
        let remaining = handle
            .join()
            .map_err(|_| anyhow!("overlay thread panicked"))??;
        println!("overlay that sent {amount}: sender balance now {remaining}");
        assert_eq!(
            remaining,
            start - U256::from(*amount),
            "each overlay must be isolated from its siblings"
        );
    }

    println!("\nall overlays started from the same snapshot and stayed isolated.");
    Ok(())
}

/// Read `balanceOf(owner)` through an overlay (a non-committing call).
fn overlay_balance_of(overlay: &mut EvmOverlay, token: Address, owner: Address) -> Result<U256> {
    let call = mock::MockERC20::balanceOfCall { account: owner };
    let result = overlay.call_raw(owner, token, Bytes::from(call.abi_encode()))?;
    match result {
        ExecutionResult::Success { output, .. } => Ok(
            mock::MockERC20::balanceOfCall::abi_decode_returns(&output.into_data())?,
        ),
        other => Err(anyhow!("balanceOf call failed: {other:?}")),
    }
}
