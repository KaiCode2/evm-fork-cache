//! Deploy a contract from creation bytecode and etch its code over another
//! address while preserving that address's storage, balance, and nonce.
//!
//! This is the pattern for running a locally-modified contract against forked
//! state: deploy your build to a scratch address, then `override_account_code`
//! onto the real on-chain address. `deploy_contract` runs the constructor in the
//! EVM; `override_account_code` copies only the runtime bytecode, leaving the
//! target's storage intact. (For loading Foundry artifacts from disk, see the
//! `deploy::etch_foundry_artifact*` helpers.)
//!
//! Runs fully offline against a mocked provider.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example deploy_and_override
//! ```

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolValue;
use anyhow::Result;

#[path = "support/mock.rs"]
mod mock;

/// Deterministic CREATE address for `Address::ZERO` at nonce 0.
const CREATE_ADDRESS_ZERO_NONCE_0: Address = Address::new(alloy_primitives::hex!(
    "bd770416a3345f91e4b34576cb804a576fa48eb1"
));

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    mock::install_default_account(&mut cache, Address::ZERO);
    // Pre-insert the CREATE target so the mocked provider is never queried.
    mock::install_default_account(&mut cache, CREATE_ADDRESS_ZERO_NONCE_0);

    // ── Deploy a fresh MockERC20 by running its constructor in the EVM ──
    let mut creation_code = mock::mock_erc20_creation_code();
    let constructor_args = (
        String::from("Example Token"),
        String::from("EXMPL"),
        U256::from(18u8),
    )
        .abi_encode_params();
    creation_code.extend_from_slice(&constructor_args);

    let deployed = cache.deploy_contract(Address::ZERO, Bytes::from(creation_code))?;
    println!("deployed MockERC20 at {deployed}");
    println!(
        "fresh balance: {}",
        mock::balance_of(&mut cache, deployed, Address::ZERO)?
    );

    // ── Set up a separate target that already holds storage ──
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
        "\ntarget {target} holder balance (before override): {}",
        mock::balance_of(&mut cache, target, holder)?
    );

    // ── Etch the freshly-deployed code over the target ──
    // Only the bytecode is copied; the target's storage (the holder balance)
    // survives the override.
    cache.override_account_code(deployed, target)?;
    println!(
        "target holder balance (after override): {}",
        mock::balance_of(&mut cache, target, holder)?
    );

    Ok(())
}
