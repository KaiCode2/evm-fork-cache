//! Fork → mock (overlay-scoped) → simulate.
//!
//! [`EvmCache::mock_overlay`] hands you a throwaway [`EvmOverlay`] over the
//! current chain snapshot, wired to the cache's backend for lazy fetch. Mock
//! balances, approvals, and even arbitrary getter return values on it, then
//! simulate against the mocked state. Every override lives **only in the
//! overlay** and vanishes when it is dropped — the cache (true chain state) is
//! never polluted, so mocks can't leak into later simulations.
//!
//! This demonstrates all three: `mock_balance`, `mock_allowance`, and
//! `mock_call` (a general "make this getter return X"). Runs fully offline
//! against a mocked `MockERC20`.
//!
//! ```sh
//! cargo run --example mock_and_simulate
//! ```
//!
//! [`EvmCache::mock_overlay`]: evm_fork_cache::cache::EvmCache::mock_overlay
//! [`EvmOverlay`]: evm_fork_cache::cache::EvmOverlay

#[path = "support/mock.rs"]
mod mock;

use alloy_primitives::{Address, Bytes, U256, address};
use alloy_sol_types::{SolCall, sol};
use anyhow::Result;
use evm_fork_cache::CallTracer;
use evm_fork_cache::cache::TxConfig;
use revm::context::result::ExecutionResult;

sol! {
    interface IErc20 {
        function balanceOf(address account) returns (uint256);
        function allowance(address owner, address spender) returns (uint256);
        function totalSupply() returns (uint256);
        function transferFrom(address from, address to, uint256 amount) returns (bool);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    let alice = address!("00000000000000000000000000000000000000A1");
    let bob = address!("00000000000000000000000000000000000000B2");
    let router = address!("00000000000000000000000000000000000000C3");

    for a in [Address::ZERO, alice, bob, router] {
        mock::install_default_account(&mut cache, a);
    }
    mock::install_mock_erc20(&mut cache, usdc);
    // Seed a distinctive on-chain totalSupply (slot 2) so mock_call has an
    // unambiguous slot to match on.
    cache.insert_storage_slot(usdc, U256::from(2u64), U256::from(500_000_000_000u64))?;

    let one_m = U256::from(1_000_000_000_000u64); // 1,000,000 USDC (6 dp)

    // ---- A throwaway overlay over the current snapshot; cache stays pristine ----
    let mut sim = cache.mock_overlay();
    let ok_bal = sim.mock_balance(usdc, alice, one_m)?;
    let ok_appr = sim.mock_allowance(usdc, alice, router, U256::MAX)?;
    let ok_ts = sim.mock_call(
        usdc,
        IErc20::totalSupplyCall {},
        U256::from(2_000_000_000_000u64),
    )?;
    println!("mocks applied → balance={ok_bal}  approval={ok_appr}  totalSupply={ok_ts}");

    // Read them back with native typed calls (no manual ABI decode).
    println!("\non the overlay:");
    println!(
        "  balanceOf(alice) = {}",
        sim.call_sol(usdc, IErc20::balanceOfCall { account: alice })?
    );
    println!(
        "  allowance(a→r)   = {}",
        sim.call_sol(
            usdc,
            IErc20::allowanceCall {
                owner: alice,
                spender: router
            }
        )?
    );
    println!(
        "  totalSupply()    = {}",
        sim.call_sol(usdc, IErc20::totalSupplyCall {})?
    );

    // ---- Simulate router.transferFrom(alice → bob, 250k), committing to the overlay ----
    let cd = Bytes::from(
        IErc20::transferFromCall {
            from: alice,
            to: bob,
            amount: U256::from(250_000_000_000u64),
        }
        .abi_encode(),
    );
    let (res, _) = sim.call_raw_with_inspector(
        router,
        usdc,
        cd,
        &TxConfig::default(),
        CallTracer::new(),
        true,
    )?;
    let status = if matches!(res, ExecutionResult::Success { .. }) {
        "Success"
    } else {
        "FAILED"
    };
    println!("\nrouter.transferFrom(alice → bob, 250k): {status}");
    println!(
        "  alice = {}",
        sim.call_sol(usdc, IErc20::balanceOfCall { account: alice })?
    );
    println!(
        "  bob   = {}",
        sim.call_sol(usdc, IErc20::balanceOfCall { account: bob })?
    );

    // ---- Isolation: drop the overlay; the cache (true state) never saw the mocks ----
    drop(sim);
    let cache_alice: U256 = cache.call_sol(usdc, IErc20::balanceOfCall { account: alice })?;
    println!("\ncache (untouched): balanceOf(alice) = {cache_alice}   ← mocks never persisted");

    Ok(())
}
