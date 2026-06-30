//! Override an ERC20 balance in a simulation by scanning for its storage slot.
//!
//! `set_erc20_balance_with_slot_scan` probes mapping slots until a probe write
//! is reflected by `balanceOf`, then writes the desired balance there. Once the
//! slot is known it is cached; you can also seed it up front to skip scanning
//! entirely (handy for proxy tokens whose balance slot you already know).
//!
//! This example runs fully offline against a mocked provider (see
//! `support/mock.rs` and `fixtures/MockERC20.sol`).
//!
//! Run with:
//!
//! ```sh
//! cargo run --example erc20_balance_override
//! ```

use alloy_primitives::{Address, U256};
use anyhow::Result;

#[path = "support/mock.rs"]
mod mock;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;

    let token = Address::repeat_byte(0x90);
    let whale = Address::repeat_byte(0x91);
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, whale);
    mock::install_mock_erc20(&mut cache, token);

    println!(
        "initial balance: {}",
        mock::balance_of(&mut cache, token, whale)?
    );

    // Discover the balance slot by scanning slots 0..=8 and give the whale 1M units.
    let target = U256::from(1_000_000u64);
    let found = cache.set_erc20_balance_with_slot_scan(token, whale, target, 8)?;
    println!("slot scan found the balance slot: {found}");
    println!(
        "balance after override: {}",
        mock::balance_of(&mut cache, token, whale)?
    );

    // A second override is fast: the discovered slot is now cached.
    let doubled = target * U256::from(2);
    cache.set_erc20_balance_with_slot_scan(token, whale, doubled, 8)?;
    println!(
        "balance after second override: {}",
        mock::balance_of(&mut cache, token, whale)?
    );

    // If you already know the balance slot, seed it and skip scanning (max_slot=0).
    let other_token = Address::repeat_byte(0xA0);
    mock::install_mock_erc20(&mut cache, other_token);
    cache.seed_erc20_balance_slots([(other_token, U256::from(mock::MOCK_ERC20_BALANCE_SLOT))]);
    let seeded = cache.set_erc20_balance_with_slot_scan(other_token, whale, target, 0)?;
    println!("\nseeded slot bypassed scanning: {seeded}");
    println!(
        "seeded-token balance: {}",
        mock::balance_of(&mut cache, other_token, whale)?
    );

    Ok(())
}
