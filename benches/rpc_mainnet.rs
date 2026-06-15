//! RPC-gated real-contract benchmarks against live forked mainnet state.
//!
//! Unlike the other benches, these fork real chain state, so they are gated
//! behind the `RPC_URL` environment variable and **skip** (rather than fail)
//! when it is unset. This keeps `cargo bench` offline and reproducible by
//! default while still letting you measure real-contract behavior on demand:
//!
//! ```sh
//! RPC_URL=https://eth.llamarpc.com cargo bench --bench rpc_mainnet
//! ```
//!
//! They measure warm-cache throughput of view calls against well-known mainnet
//! contracts (USDC `balanceOf`, a Uniswap V2 pair `getReserves`). The cache is
//! warmed once before timing so each measured iteration reads from the local
//! cache rather than re-fetching over RPC — that warm-reuse path is exactly what
//! a search loop hammers between block updates.
//!
//! RPC-touching calls run inside `rt.block_on(..)` because `EvmCache` fetches
//! missing state via `tokio::task::block_in_place`, which requires a
//! multi-thread runtime context.

use std::hint::black_box;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, address};
use alloy_provider::ProviderBuilder;
use alloy_provider::network::AnyNetwork;
use alloy_sol_types::{SolCall, sol};
use criterion::{Criterion, criterion_group, criterion_main};
use evm_fork_cache::cache::EvmCache;
use revm::context::result::ExecutionResult;
use revm::primitives::hardfork::SpecId;
use tokio::runtime::Runtime;

/// USDC (6 decimals) — a ubiquitous mainnet ERC20.
const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
/// A consistently USDC-holding address (an exchange hot wallet). The exact
/// balance is irrelevant to a perf benchmark; `balanceOf` succeeds regardless.
const HOLDER: Address = address!("28C6c06298d514Db089934071355E5743bf21d60");
/// The Uniswap V2 USDC/WETH pair.
const UNIV2_USDC_WETH: Address = address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");

sol! {
    interface IErc20 {
        function balanceOf(address account) external view returns (uint256);
    }
    interface IUniswapV2Pair {
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    }
}

fn bench_rpc_mainnet(c: &mut Criterion) {
    let rpc_url = match std::env::var("RPC_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            eprintln!(
                "RPC_URL not set — skipping rpc_mainnet benchmarks. \
                 Set RPC_URL=<https endpoint> to run them."
            );
            return;
        }
    };

    // Multi-thread runtime so the cache's lazy fetch (`block_in_place`) is valid.
    let rt = Runtime::new().expect("tokio runtime");
    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_http(rpc_url.parse().expect("valid RPC_URL"));
    let mut cache = rt.block_on(
        EvmCache::builder(Arc::new(provider))
            .latest_block()
            .spec(SpecId::CANCUN)
            .build(),
    );

    let balance_of = Bytes::from(IErc20::balanceOfCall { account: HOLDER }.abi_encode());
    let get_reserves = Bytes::from(IUniswapV2Pair::getReservesCall {}.abi_encode());

    // Warm the cache once per target so the timed iterations are warm reads.
    let warm = rt.block_on(async {
        let a = cache.call_raw(HOLDER, USDC, balance_of.clone(), false);
        let b = cache.call_raw(Address::ZERO, UNIV2_USDC_WETH, get_reserves.clone(), false);
        (a, b)
    });
    assert!(
        matches!(warm.0, Ok(ExecutionResult::Success { .. })),
        "USDC balanceOf warm-up should succeed: {:?}",
        warm.0
    );
    assert!(
        matches!(warm.1, Ok(ExecutionResult::Success { .. })),
        "Uniswap V2 getReserves warm-up should succeed: {:?}",
        warm.1
    );

    let mut group = c.benchmark_group("rpc_mainnet_warm");
    group.bench_function("usdc_balanceOf", |b| {
        b.iter(|| {
            let r = rt
                .block_on(async { cache.call_raw(HOLDER, USDC, balance_of.clone(), false) })
                .unwrap();
            black_box(r);
        })
    });
    group.bench_function("univ2_getReserves", |b| {
        b.iter(|| {
            let r = rt
                .block_on(async {
                    cache.call_raw(Address::ZERO, UNIV2_USDC_WETH, get_reserves.clone(), false)
                })
                .unwrap();
            black_box(r);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_rpc_mainnet);
criterion_main!(benches);
