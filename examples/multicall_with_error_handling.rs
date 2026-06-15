//! Batch calls with `allowFailure` and read partial results.
//!
//! Multicall3's `aggregate3` lets each call opt into failure tolerance. With
//! `allow_failure = true`, a call that reverts does **not** abort the batch —
//! it comes back with `success = false` and whatever revert data it produced,
//! so a search loop can probe many calls in one pass and gracefully skip the
//! ones that fail. (With `allow_failure = false`, a revert makes the whole
//! `aggregate3` revert, surfacing here as an `Err` from `execute`.)
//!
//! This batch mixes calls that succeed (`USDC.decimals()`, `USDC.balanceOf(..)`)
//! with one that reverts (`USDC.transfer(..)` from the Multicall3 contract, which
//! holds no USDC). `try_decode_result` returns `None` for the failed call instead
//! of erroring.
//!
//! Requires an Ethereum mainnet RPC endpoint. Run with:
//!
//! ```sh
//! RPC_URL=https://eth.llamarpc.com cargo run --example multicall_with_error_handling
//! ```

use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_primitives::{Address, U256, address};
use alloy_provider::ProviderBuilder;
use alloy_provider::network::AnyNetwork;
use alloy_sol_types::sol;
use anyhow::Result;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::multicall::{MulticallBatch, try_decode_result};

const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const HOLDER: Address = address!("28C6c06298d514Db089934071355E5743bf21d60");

sol! {
    interface IUsdc {
        function decimals() external view returns (uint8);
        function balanceOf(address account) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let Ok(rpc_url) = std::env::var("RPC_URL") else {
        eprintln!("This example needs an Ethereum mainnet RPC endpoint. Run with:");
        eprintln!(
            "  RPC_URL=https://eth.llamarpc.com cargo run --example multicall_with_error_handling"
        );
        return Ok(());
    };

    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_http(rpc_url.parse()?);
    let mut cache = EvmCache::new(Arc::new(provider), Some(BlockId::latest())).await;

    // Three calls, all failure-tolerant. The transfer reverts (the Multicall3
    // contract — the msg.sender of each sub-call — holds no USDC), but the batch
    // still completes and the other two results are usable.
    let mut batch = MulticallBatch::with_capacity(3);
    batch.add_call(USDC, IUsdc::decimalsCall {}, true);
    batch.add_call(USDC, IUsdc::balanceOfCall { account: HOLDER }, true);
    batch.add_call(
        USDC,
        IUsdc::transferCall {
            to: HOLDER,
            amount: U256::MAX,
        },
        true,
    );

    let results = batch.execute(&mut cache)?;
    println!(
        "batch of {} calls completed despite a revert:\n",
        results.len()
    );

    let decimals = try_decode_result::<IUsdc::decimalsCall>(&results[0]);
    let balance = try_decode_result::<IUsdc::balanceOfCall>(&results[1]);

    println!(
        "  [0] decimals()          success={} -> {:?}",
        results[0].success, decimals
    );
    println!(
        "  [1] balanceOf(holder)   success={} -> {:?}",
        results[1].success, balance
    );
    println!(
        "  [2] transfer(.., MAX)   success={} -> {} (gracefully skipped)",
        results[2].success,
        if results[2].success {
            "ok"
        } else {
            "reverted, no value"
        }
    );

    assert!(
        results[0].success && results[1].success,
        "view calls succeed"
    );
    assert!(
        !results[2].success,
        "the unfunded transfer reverts but does not abort the batch"
    );

    Ok(())
}
