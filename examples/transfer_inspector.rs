//! Measure ERC20 balance changes from a simulation without manual pre/post
//! balance reads.
//!
//! `simulate_with_transfer_tracking` runs the call under an inspector that
//! captures every ERC20 `Transfer` event, then reports the net per-token delta
//! for an owner. This is the cheap way to answer "how much did this account gain
//! or lose?" after a swap, deposit, or multi-step execution.
//!
//! Runs fully offline against a mocked provider.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example transfer_inspector
//! ```

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;

#[path = "support/mock.rs"]
mod mock;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;

    let token = Address::repeat_byte(0x44);
    let sender = Address::repeat_byte(0x55);
    let receiver = Address::repeat_byte(0x66);
    // Address::ZERO is the default block coinbase, touched for gas accounting.
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, sender);
    mock::install_default_account(&mut cache, receiver);
    mock::install_mock_erc20(&mut cache, token);

    let slot = U256::from(mock::MOCK_ERC20_BALANCE_SLOT);
    cache.insert_mapping_storage_slot(token, slot, sender, U256::from(1_000u64))?;

    // Build a transfer of 250 tokens, then simulate it tracking the sender's
    // balance changes. `commit = false` discards the state change afterward.
    let calldata = Bytes::from(
        mock::MockERC20::transferCall {
            to: receiver,
            amount: U256::from(250u64),
        }
        .abi_encode(),
    );

    let result = cache.simulate_with_transfer_tracking(
        sender,
        token,
        calldata,
        sender,        // owner whose deltas we want
        Some([token]), // restrict to this token
        false,         // do not commit
    )?;

    println!("gas used: {}", result.gas_used);
    println!("captured {} log(s)", result.logs.len());
    for (tok, delta) in &result.token_deltas {
        println!("  token {tok}: net delta {delta}");
    }

    // The simulation did not commit, so the sender's balance is unchanged.
    println!(
        "\nsender balance after (uncommitted) sim: {}",
        mock::balance_of(&mut cache, token, sender)?
    );

    Ok(())
}
