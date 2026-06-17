//! Simulate a multi-hop Uniswap V2 swap quote against live mainnet state.
//!
//! This calls the real Uniswap V2 router's `getAmountsOut(amountIn, path)` for a
//! two-hop path (WETH → USDC → DAI) inside the fork. The router reads each pair's
//! reserves from chain state — fetched lazily through the cache on first access —
//! and returns the output amount after both hops. It is a pure view call, so no
//! funding or approvals are needed, yet it exercises the real multi-contract
//! state a swap simulation depends on.
//!
//! To go further (a state-changing swap), you would override the caller's input
//! token balance (see `fork_override_balance`) and call the router's
//! `swapExactTokensForTokens`, then read the balance deltas with
//! `simulate_with_transfer_tracking`.
//!
//! Requires an Ethereum mainnet RPC endpoint. Run with:
//!
//! ```sh
//! RPC_URL=https://eth.llamarpc.com cargo run --example multi_hop_swap
//! ```

use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256, address};
use alloy_provider::ProviderBuilder;
use alloy_provider::network::AnyNetwork;
use alloy_sol_types::{SolCall, sol};
use anyhow::{Result, anyhow};
use evm_fork_cache::cache::EvmCache;
use revm::context::result::ExecutionResult;

const ROUTER: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const DAI: Address = address!("6B175474E89094C44Da98b954EedeAC495271d0F");

sol! {
    interface IUniswapV2Router {
        function getAmountsOut(uint256 amountIn, address[] path) external view returns (uint256[] amounts);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Ok(rpc_url) = std::env::var("RPC_URL") else {
        eprintln!("This example needs an Ethereum mainnet RPC endpoint. Run with:");
        eprintln!("  RPC_URL=https://eth.llamarpc.com cargo run --example multi_hop_swap");
        return Ok(());
    };

    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_http(rpc_url.parse()?);
    let mut cache = EvmCache::new(Arc::new(provider)).await;

    // Quote 1 WETH swapped along WETH -> USDC -> DAI.
    let amount_in = U256::from(10u64).pow(U256::from(18u64)); // 1 WETH (1e18)
    let path = vec![WETH, USDC, DAI];
    let calldata = Bytes::from(
        IUniswapV2Router::getAmountsOutCall {
            amountIn: amount_in,
            path: path.clone(),
        }
        .abi_encode(),
    );

    let result = cache.call_raw(Address::ZERO, ROUTER, calldata, false)?;
    let output = match result {
        ExecutionResult::Success { output, .. } => output.into_data(),
        other => return Err(anyhow!("getAmountsOut did not succeed: {other:?}")),
    };

    let amounts = IUniswapV2Router::getAmountsOutCall::abi_decode_returns(&output)?;
    if amounts.len() != path.len() {
        return Err(anyhow!("unexpected amounts length: {}", amounts.len()));
    }

    // USDC has 6 decimals, DAI has 18; print human-readable figures.
    let usdc_mid = amounts[1] / U256::from(10u64).pow(U256::from(6u64));
    let dai_out = amounts[2] / U256::from(10u64).pow(U256::from(18u64));

    println!("two-hop quote (Uniswap V2, live reserves):");
    println!("  in:  1 WETH");
    println!("  hop1 -> ~{usdc_mid} USDC  ({} raw)", amounts[1]);
    println!("  hop2 -> ~{dai_out} DAI   ({} raw)", amounts[2]);

    Ok(())
}
