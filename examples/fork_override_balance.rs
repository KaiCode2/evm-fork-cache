//! Override a real token's balance on a fork by discovering its storage slot.
//!
//! Against forked mainnet state, `set_erc20_balance_with_slot_scan` probes the
//! token's mapping slots until a write is reflected by `balanceOf`, then writes
//! the target balance. This is how you fund an arbitrary account in a simulation
//! without holding the tokens. (WETH9's balance mapping happens to live at slot 3.)
//!
//! Requires an Ethereum mainnet RPC endpoint. Run with:
//!
//! ```sh
//! RPC_URL=https://eth.llamarpc.com cargo run --example fork_override_balance
//! ```

use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_primitives::{Address, U256, address};
use alloy_provider::ProviderBuilder;
use alloy_provider::network::AnyNetwork;
use anyhow::Result;
use evm_fork_cache::cache::EvmCache;

/// Canonical WETH9 on Ethereum mainnet.
const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Ok(rpc_url) = std::env::var("RPC_URL") else {
        eprintln!("This example needs an Ethereum mainnet RPC endpoint. Run with:");
        eprintln!("  RPC_URL=https://eth.llamarpc.com cargo run --example fork_override_balance");
        return Ok(());
    };

    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_http(rpc_url.parse()?);
    let mut cache = EvmCache::new(Arc::new(provider), Some(BlockId::latest())).await;

    // An arbitrary account that holds no WETH on-chain.
    let beneficiary = Address::repeat_byte(0xBE);
    let before = cache.erc20_balance_of(WETH, beneficiary)?;
    println!("WETH balance before override: {before}");

    // Give the beneficiary 100 WETH by finding the balance slot (scan 0..=8).
    let target = U256::from(100u128) * U256::from(10u64).pow(U256::from(18));
    let found = cache.set_erc20_balance_with_slot_scan(WETH, beneficiary, target, 8)?;
    println!("slot scan succeeded: {found}");

    let after = cache.erc20_balance_of(WETH, beneficiary)?;
    println!("WETH balance after override: {after} wei (~100 WETH)");
    assert_eq!(after, target, "override should be reflected by balanceOf");

    Ok(())
}
