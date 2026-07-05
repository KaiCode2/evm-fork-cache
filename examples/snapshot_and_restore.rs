//! Checkpoint cache state, mutate it, then roll back — the core primitive for
//! evaluating many candidate transactions from the same starting point.
//!
//! `checkpoint()` captures a cheap in-memory copy of the cache's state; `restore()`
//! resets to it. Here we transfer tokens (committing the change), observe the new
//! balances, then restore and confirm the transfer was undone.
//!
//! This is the in-place rollback API on a single `EvmCache`. It is distinct from
//! `snapshot()`, which returns an `Arc<EvmSnapshot>` for sharing one frozen
//! state across many parallel `EvmOverlay` simulations — see the
//! `parallel_overlays` example for that workflow.
//!
//! Runs fully offline against a mocked provider.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example snapshot_and_restore
//! ```

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;

#[path = "support/mock.rs"]
mod mock;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;

    let token = Address::repeat_byte(0x11);
    let alice = Address::repeat_byte(0x22);
    let bob = Address::repeat_byte(0x33);
    // Address::ZERO is the default block coinbase; committing a tx credits gas
    // to it, so it must be present in the offline cache.
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, alice);
    mock::install_default_account(&mut cache, bob);
    mock::install_mock_erc20(&mut cache, token);

    let slot = U256::from(mock::MOCK_ERC20_BALANCE_SLOT);
    let start = U256::from(1_000u64);
    cache.insert_mapping_storage_slot(token, slot, alice, start)?;
    cache.insert_mapping_storage_slot(token, slot, bob, U256::ZERO)?;

    println!(
        "before: alice={}, bob={}",
        mock::balance_of(&mut cache, token, alice)?,
        mock::balance_of(&mut cache, token, bob)?
    );

    // Capture a restore point.
    let checkpoint = cache.checkpoint();

    // Commit a transfer of 250 from alice to bob.
    let transfer = mock::MockERC20::transferCall {
        to: bob,
        amount: U256::from(250u64),
    };
    cache.call_raw(alice, token, Bytes::from(transfer.abi_encode()), true)?;
    println!(
        "after transfer: alice={}, bob={}",
        mock::balance_of(&mut cache, token, alice)?,
        mock::balance_of(&mut cache, token, bob)?
    );

    // Roll back to the checkpoint — the transfer is undone.
    cache.restore(checkpoint);
    println!(
        "after restore: alice={}, bob={}",
        mock::balance_of(&mut cache, token, alice)?,
        mock::balance_of(&mut cache, token, bob)?
    );

    Ok(())
}
