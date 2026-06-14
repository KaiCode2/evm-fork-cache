//! Fork real mainnet state over RPC and read it lazily through the cache.
//!
//! The cache fetches account/storage data from RPC on first access and serves it
//! locally thereafter — so the first read of a slot pays a network round-trip and
//! every subsequent read is in-memory. This example reads WETH's decimals and a
//! holder's balance, timing a cold read against a warm one.
//!
//! Requires an Ethereum mainnet RPC endpoint. Run with:
//!
//! ```sh
//! RPC_URL=https://eth.llamarpc.com cargo run --example fork_token_balance
//! ```

use std::sync::Arc;
use std::time::Instant;

use alloy_eips::BlockId;
use alloy_primitives::{Address, address};
use alloy_provider::ProviderBuilder;
use alloy_provider::network::AnyNetwork;
use anyhow::Result;
use evm_fork_cache::cache::EvmCache;

/// Canonical WETH9 on Ethereum mainnet.
const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
/// The Uniswap V3 USDC/WETH 0.05% pool — a large, stable WETH holder.
const HOLDER: Address = address!("88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Ok(rpc_url) = std::env::var("RPC_URL") else {
        eprintln!("This example needs an Ethereum mainnet RPC endpoint. Run with:");
        eprintln!("  RPC_URL=https://eth.llamarpc.com cargo run --example fork_token_balance");
        return Ok(());
    };

    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_http(rpc_url.parse()?);
    let mut cache = EvmCache::new(Arc::new(provider), Some(BlockId::latest())).await;

    let decimals = cache.erc20_decimals(WETH)?;
    println!("WETH decimals: {decimals}");

    // First read is cold — storage is fetched from RPC and cached.
    let t0 = Instant::now();
    let cold_balance = cache.erc20_balance_of(WETH, HOLDER)?;
    let cold = t0.elapsed();

    // Second read is warm — served from the local cache, no RPC round-trip.
    let t1 = Instant::now();
    let warm_balance = cache.erc20_balance_of(WETH, HOLDER)?;
    let warm = t1.elapsed();

    let whole = cold_balance
        / alloy_primitives::U256::from(10u64).pow(alloy_primitives::U256::from(decimals));
    println!("holder WETH balance: {cold_balance} wei (~{whole} WETH)");
    println!("cold read: {cold:?}");
    println!("warm read: {warm:?}  (served from cache)");
    assert_eq!(cold_balance, warm_balance, "cached read must match");

    Ok(())
}
