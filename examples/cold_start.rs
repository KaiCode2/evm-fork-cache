//! Cold-start working-set warming: discover-then-verify.
//!
//! Before going reactive, a searcher warms a *working set* — the accounts and
//! storage slots their strategy reads — into the fork so simulations don't pay an
//! RPC round-trip per slot. The hard part is that you often don't know the exact
//! slots a contract uses. Cold-start solves this declaratively over a bounded,
//! planner-driven loop:
//!
//! 1. **Discover** — run a read-only view-call and capture the `(address, slot)`
//!    pairs it touches. Here `balanceOf(owner)` SLOADs the hashed balance slot, so
//!    the discover phase learns the slot without us hardcoding its layout.
//! 2. **Verify** — authoritatively re-fetch those discovered slots through the
//!    batched [`StorageBatchFetchFn`] and inject the fresh values into the cache.
//!
//! The driver performs all IO; the [`ColdStartPlanner`] stays pure (it is handed
//! only a read-only [`StateView`] and the round's results). Runs fully offline
//! against a mocked provider, with a stub fetcher standing in for the chain.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example cold_start
//! ```

#[path = "support/mock.rs"]
mod mock;

use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::Result;
use evm_fork_cache::cache::StorageBatchFetchFn;
use evm_fork_cache::events::StateView;
use evm_fork_cache::{
    ColdStartCall, ColdStartConfig, ColdStartPlan, ColdStartPlanner, ColdStartResults,
    ColdStartStep,
};

sol! {
    interface MockERC20 {
        function balanceOf(address account) returns (uint256);
    }
}

/// The hashed `balanceOf[owner]` slot for the MockERC20 fixture (mapping at slot 3).
fn balance_slot(owner: Address) -> U256 {
    let key = keccak256((owner, U256::from(mock::MOCK_ERC20_BALANCE_SLOT)).abi_encode());
    U256::from_be_bytes(key.0)
}

/// Warms a token's working set in two rounds: round 1 *discovers* the slots a
/// `balanceOf(owner)` call touches, round 2 *verifies* (re-fetches + injects) them.
struct WorkingSetWarmer {
    token: Address,
    owner: Address,
    discovered: Vec<(Address, U256)>,
    verified_round: bool,
}

impl ColdStartPlanner for WorkingSetWarmer {
    fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
        // Round 1: discover. A read-only `balanceOf` view-call; `restrict_to`
        // keeps only the token's own slots in the captured access list.
        ColdStartPlan {
            discover: vec![ColdStartCall {
                from: self.owner,
                to: self.token,
                calldata: Bytes::from(
                    MockERC20::balanceOfCall {
                        account: self.owner,
                    }
                    .abi_encode(),
                ),
                restrict_to: Some(vec![self.token]),
            }],
            ..Default::default()
        }
    }

    fn on_results(&mut self, results: &ColdStartResults, _state: &dyn StateView) -> ColdStartStep {
        if self.verified_round {
            return ColdStartStep::Done;
        }
        // Collect every slot the discover call touched, then verify them next round.
        for call in &results.discovered {
            self.discovered.extend(call.access.slots.iter().copied());
        }
        self.verified_round = true;
        ColdStartStep::Continue(ColdStartPlan {
            verify: self.discovered.clone(),
            ..Default::default()
        })
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    // Pre-install the coinbase (Address::ZERO) so the view-call's gas accounting
    // doesn't trigger a lazy RPC fetch against the mocked (offline) provider.
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, owner);
    mock::install_mock_erc20(&mut cache, token);

    let slot = balance_slot(owner);

    // A stub fetcher standing in for the chain: the owner's true on-chain balance
    // is 1_000_000; everything else reads as a genuine zero.
    let fetcher: StorageBatchFetchFn =
        Arc::new(move |requests: Vec<(Address, U256)>, _block: BlockId| {
            requests
                .into_iter()
                .map(|(a, s)| {
                    let value = if (a, s) == (token, slot) {
                        U256::from(1_000_000u64)
                    } else {
                        U256::ZERO
                    };
                    (a, s, Ok(value))
                })
                .collect()
        });
    cache.set_storage_batch_fetcher(fetcher);

    println!(
        "before cold start: balanceOf(owner) = {}  (slot uncached, reads 0)",
        mock::balance_of(&mut cache, token, owner)?
    );

    let mut planner = WorkingSetWarmer {
        token,
        owner,
        discovered: Vec::new(),
        verified_round: false,
    };
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    println!("\n=== cold-start report ===");
    println!("  rounds executed:   {}", report.rounds);
    println!("  slots discovered:  {}", report.discovered_slots);
    println!(
        "  slots verified:    {} requested, {} changed + injected",
        report.verified_slots, report.changed_slots
    );
    println!("  fetch failures:    {}", report.failed_slots);

    println!(
        "\nafter cold start:  balanceOf(owner) = {}  (warmed from the fetcher; reads are now local)",
        mock::balance_of(&mut cache, token, owner)?
    );

    Ok(())
}
