//! Etch a locally compiled Foundry artifact (loaded from a JSON file on disk)
//! over a forked contract, preserving the target's storage, balance, and nonce.
//!
//! This is the on-disk counterpart to `deploy_and_override`: instead of handing
//! raw creation bytecode, you point at a Foundry build artifact
//! (`out/MyContract.sol/MyContract.json`). `etch_foundry_artifact_or_create`
//! reads `bytecode.object`, appends the ABI-encoded constructor args, runs the
//! constructor in the EVM, and copies the resulting runtime bytecode onto the
//! target — the standard way to run a locally-modified contract against forked
//! state.
//!
//! Here the artifact is the checked-in `fixtures/MockERC20.foundry.json` (a
//! minimal Foundry-shaped artifact wrapping the `MockERC20` creation bytecode).
//! It is etched over a target that already holds a token balance, and we show
//! that balance survives the code swap.
//!
//! Runs fully offline against a mocked provider.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example foundry_artifact_etching
//! ```

use alloy_primitives::{Address, U256};
use anyhow::Result;
use evm_fork_cache::deploy::{encode_constructor_args, etch_foundry_artifact_or_create};

#[path = "support/mock.rs"]
mod mock;

/// Path to the checked-in Foundry artifact (resolved relative to the crate root
/// so the example runs from any working directory).
const ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/MockERC20.foundry.json"
);

/// Deterministic CREATE address for `Address::ZERO` at nonce 0 (the scratch
/// address the artifact is deployed to before being etched onto the target).
const CREATE_ADDRESS_ZERO_NONCE_0: Address = Address::new(alloy_primitives::hex!(
    "bd770416a3345f91e4b34576cb804a576fa48eb1"
));

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    mock::install_default_account(&mut cache, Address::ZERO);
    // Pre-insert the scratch CREATE address so the mocked provider is never queried.
    mock::install_default_account(&mut cache, CREATE_ADDRESS_ZERO_NONCE_0);

    // A target that already holds storage on the fork (a holder balance).
    let target = Address::repeat_byte(0xCC);
    let holder = Address::repeat_byte(0xDD);
    mock::install_mock_erc20(&mut cache, target);
    mock::install_default_account(&mut cache, holder);
    cache.insert_mapping_storage_slot(
        target,
        U256::from(mock::MOCK_ERC20_BALANCE_SLOT),
        holder,
        U256::from(7_777u64),
    )?;
    println!(
        "target {target} holder balance (before etch): {}",
        mock::balance_of(&mut cache, target, holder)?
    );

    // Constructor args for MockERC20(string name, string symbol, uint8 decimals).
    let constructor_args = encode_constructor_args((
        String::from("Etched Token"),
        String::from("ETCH"),
        U256::from(18u8),
    ));

    // Load the artifact from disk, deploy it, and etch its runtime code onto the
    // target. Only the bytecode is replaced; the target's storage is preserved.
    let etched = etch_foundry_artifact_or_create(
        &mut cache,
        target,
        ARTIFACT,
        Address::ZERO,
        constructor_args,
    )?;

    println!(
        "etched {} bytes from {} over {}",
        etched.code_size, etched.deployed_address, etched.target_address,
    );
    println!(
        "target holder balance (after etch): {}  (storage preserved)",
        mock::balance_of(&mut cache, target, holder)?
    );

    assert_eq!(
        mock::balance_of(&mut cache, target, holder)?,
        U256::from(7_777u64),
        "etching runtime bytecode must preserve the target's storage"
    );

    Ok(())
}
