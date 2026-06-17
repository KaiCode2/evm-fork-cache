//! Batch many read-only calls into a single EVM execution via Multicall3.
//!
//! Instead of one `eth_call` (and its lazy storage fetches) per contract, a
//! `MulticallBatch` aggregates calls and runs them in one pass over the fork.
//! Here we read `decimals()` and `symbol()` for several mainnet tokens at once.
//!
//! Requires an Ethereum mainnet RPC endpoint. Run with:
//!
//! ```sh
//! RPC_URL=https://eth.llamarpc.com cargo run --example multicall_batch
//! ```

use std::sync::Arc;

use alloy_primitives::{Address, address};
use alloy_provider::ProviderBuilder;
use alloy_provider::network::AnyNetwork;
use alloy_sol_types::sol;
use anyhow::Result;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::multicall::{MulticallBatch, try_decode_result};

sol! {
    interface IERC20Meta {
        function decimals() external view returns (uint8);
        function symbol() external view returns (string);
    }
}

const TOKENS: &[(&str, Address)] = &[
    ("WETH", address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")),
    ("USDC", address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")),
    ("DAI", address!("6B175474E89094C44Da98b954EedeAC495271d0F")),
    ("USDT", address!("dAC17F958D2ee523a2206206994597C13D831ec7")),
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Ok(rpc_url) = std::env::var("RPC_URL") else {
        eprintln!("This example needs an Ethereum mainnet RPC endpoint. Run with:");
        eprintln!("  RPC_URL=https://eth.llamarpc.com cargo run --example multicall_batch");
        return Ok(());
    };

    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_http(rpc_url.parse()?);
    let mut cache = EvmCache::new(Arc::new(provider)).await;

    // Build one batch with two calls per token.
    let mut batch = MulticallBatch::with_capacity(TOKENS.len() * 2);
    for (_, token) in TOKENS {
        batch.add_call(*token, IERC20Meta::decimalsCall {}, true);
        batch.add_call(*token, IERC20Meta::symbolCall {}, true);
    }

    let results = batch.execute(&mut cache)?;

    println!("queried {} tokens in one multicall:\n", TOKENS.len());
    for (i, (name, token)) in TOKENS.iter().enumerate() {
        let decimals = try_decode_result::<IERC20Meta::decimalsCall>(&results[i * 2]);
        let symbol = try_decode_result::<IERC20Meta::symbolCall>(&results[i * 2 + 1]);
        println!(
            "  {name} ({token}): symbol={:?}, decimals={:?}",
            symbol.unwrap_or_default(),
            decimals
        );
    }

    Ok(())
}
