//! Offline tests for the cold-start sync driver.
//!
//! - S1: the verify-only, single- and multi-round path that closes the
//!   archive-miss gap.
//! - S2: the accounts (ensure) and discover (view-call access-list capture)
//!   phases, `restrict_to` filtering, and the mid-round partial-failure contract
//!   (`NotAttempted`), including the Balancer-style two-round discover→verify.
//! - S3: the probe phase (classify at the pinned block without injecting).
//! - S4: `ColdStartPin::Hash` pins every round to the hash and restores the
//!   prior block on completion and on the error path.
//!
//! Every test runs fully offline over a mocked provider; none reach the network
//! (an unexpected RPC fetch errors against the empty mock queue, failing the
//! test). These are manager-authored red-green acceptance tests. The
//! implementation agent must make them pass without weakening, skipping, or
//! rewriting them. Where they disagree with the original feature request, the
//! implementation spec (`...cold-start-implementation-spec.md`) and these tests win.
#![cfg(feature = "reactive")]

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_eips::BlockId;
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::Result;
use revm::primitives::hardfork::SpecId;

use evm_fork_cache::cache::{EvmCache, StorageBatchFetchFn};
use evm_fork_cache::cold_start::{
    ColdStartCall, ColdStartConfig, ColdStartError, ColdStartPin, ColdStartPlan, ColdStartPlanner,
    ColdStartResults, ColdStartStep, SlotFetch,
};
use evm_fork_cache::errors::StorageFetchError;
use evm_fork_cache::events::StateView;

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20, setup_cache,
    setup_cache_with_asserter, stub_fetcher,
};

/// The hashed storage slot for `MockERC20.balanceOf[owner]` (mapping at slot 3):
/// `keccak256(abi.encode(owner, 3))`. A `balanceOf(owner)` view-call SLOADs
/// exactly this slot, so the discover phase captures it.
fn balance_slot_hashed(owner: Address) -> U256 {
    let key = keccak256((owner, U256::from(MOCK_ERC20_BALANCE_SLOT)).abi_encode());
    U256::from_be_bytes(key.0)
}

/// Calldata for `MockERC20.balanceOf(owner)`.
fn balance_of_calldata(owner: Address) -> Bytes {
    Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode())
}

// ---------------------------------------------------------------------------
// Test fixtures: planners and fetchers
// ---------------------------------------------------------------------------

/// A planner that emits a fixed `initial_plan` and immediately returns `Done`.
struct OneShotPlanner {
    plan: ColdStartPlan,
}

impl ColdStartPlanner for OneShotPlanner {
    fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
        self.plan.clone()
    }
    fn on_results(&mut self, _results: &ColdStartResults, _state: &dyn StateView) -> ColdStartStep {
        ColdStartStep::Done
    }
}

/// A planner that re-emits the same (empty) plan every round, returning `Done`
/// on the `done_on_call`-th `on_results` call (`None` = never, i.e. always
/// `Continue`). `on_results_calls` records how many rounds actually executed.
struct LoopPlanner {
    on_results_calls: usize,
    done_on_call: Option<usize>,
}

impl LoopPlanner {
    fn always_continue() -> Self {
        Self {
            on_results_calls: 0,
            done_on_call: None,
        }
    }
    fn done_after(n: usize) -> Self {
        Self {
            on_results_calls: 0,
            done_on_call: Some(n),
        }
    }
}

impl ColdStartPlanner for LoopPlanner {
    fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
        ColdStartPlan::default()
    }
    fn on_results(&mut self, _results: &ColdStartResults, _state: &dyn StateView) -> ColdStartStep {
        self.on_results_calls += 1;
        match self.done_on_call {
            Some(n) if self.on_results_calls >= n => ColdStartStep::Done,
            _ => ColdStartStep::Continue(ColdStartPlan::default()),
        }
    }
}

/// A two-round planner: round 1 verifies `slot_a`, then in `on_results` (which
/// runs after round 1's injection) it records what `slot_a` reads as via the
/// `StateView`, continues into a round that verifies `slot_b`, and finishes.
struct TwoRoundPlanner {
    pool: Address,
    slot_a: U256,
    slot_b: U256,
    phase: usize,
    observed_slot_a_after_round1: Option<U256>,
}

impl ColdStartPlanner for TwoRoundPlanner {
    fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
        ColdStartPlan {
            verify: vec![(self.pool, self.slot_a)],
            ..Default::default()
        }
    }
    fn on_results(&mut self, _results: &ColdStartResults, state: &dyn StateView) -> ColdStartStep {
        self.phase += 1;
        if self.phase == 1 {
            // on_results sees post-injection state for round 1.
            self.observed_slot_a_after_round1 = state.storage(self.pool, self.slot_a);
            ColdStartStep::Continue(ColdStartPlan {
                verify: vec![(self.pool, self.slot_b)],
                ..Default::default()
            })
        } else {
            ColdStartStep::Done
        }
    }
}

/// A fetcher that returns a chosen non-zero value for `value_slot`, a hard
/// `Err` for `fail_slot`, and a genuine `Ok(ZERO)` for everything else — so a
/// single round exercises all three `SlotFetch` arms.
fn mixed_fetcher(
    value_slot: (Address, U256),
    value: U256,
    fail_slot: (Address, U256),
) -> StorageBatchFetchFn {
    Arc::new(move |requests: Vec<(Address, U256)>, _block: BlockId| {
        requests
            .into_iter()
            .map(|(addr, slot)| {
                if (addr, slot) == fail_slot {
                    (addr, slot, Err(StorageFetchError::custom("archive miss")))
                } else if (addr, slot) == value_slot {
                    (addr, slot, Ok(value))
                } else {
                    (addr, slot, Ok(U256::ZERO))
                }
            })
            .collect()
    })
}

/// Build a cache with NO storage batch fetcher (mirrors the canonical pattern in
/// `tests/freshness.rs`): `EvmCache::new` installs a default RPC fetcher, but a
/// `from_backend` cache does not capture one.
async fn no_fetcher_cache() -> Result<EvmCache> {
    let base = setup_cache().await?;
    let cache = EvmCache::from_backend(
        base.unchecked_backend().clone(),
        base.unchecked_blockchain_db().clone(),
        base.block(),
        base.chain_id(),
        None,
        None,
        SpecId::CANCUN,
    );
    assert!(
        cache.storage_batch_fetcher().is_none(),
        "from_backend cache has no fetcher"
    );
    Ok(cache)
}

/// Find the `SlotFetch` recorded for a slot in `results.fetched`.
fn fetch_of(results: &ColdStartResults, addr: Address, slot: U256) -> SlotFetch {
    results
        .fetched
        .iter()
        .find(|o| o.address == addr && o.slot == slot)
        .map(|o| o.fetch.clone())
        .unwrap_or_else(|| panic!("slot {slot} not present in results.fetched"))
}

// ---------------------------------------------------------------------------
// S1 acceptance tests
// ---------------------------------------------------------------------------

/// A single round classifies each verify slot as `Value` / `Zero` /
/// `FetchFailed` — closing the archive-miss gap (a fetch failure is `FetchFailed`,
/// NOT absence, and is distinct from a genuine on-chain `Zero`).
#[tokio::test(flavor = "multi_thread")]
async fn verify_classifies_value_zero_and_failed() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x11);
    install_mock_erc20(&mut cache, pool); // StorageCleared: unseen slots read ZERO

    let slot_zero = U256::from(1);
    let slot_value = U256::from(2);
    let slot_fail = U256::from(3);

    cache.set_storage_batch_fetcher(mixed_fetcher(
        (pool, slot_value),
        U256::from(7),
        (pool, slot_fail),
    ));

    let plan = ColdStartPlan {
        verify: vec![(pool, slot_zero), (pool, slot_value), (pool, slot_fail)],
        ..Default::default()
    };
    let outcome = cache.execute_cold_start_round(&plan);

    assert!(
        outcome.error.is_none(),
        "verify-only round has no hard-error surface"
    );
    let results = outcome.results;

    assert_eq!(results.fetched.len(), 3, "one outcome per verify slot");
    assert_eq!(fetch_of(&results, pool, slot_zero), SlotFetch::Zero);
    assert_eq!(
        fetch_of(&results, pool, slot_value),
        SlotFetch::Value(U256::from(7))
    );
    assert!(
        matches!(
            fetch_of(&results, pool, slot_fail),
            SlotFetch::FetchFailed { .. }
        ),
        "a fetcher Err must surface as FetchFailed, not absence"
    );

    // Only the non-zero, changed slot was injected and recorded as changed.
    assert_eq!(results.verified.len(), 1, "only slot_value changed");
    assert_eq!(results.verified[0].slot, slot_value);
    assert_eq!(results.verified[0].new, U256::from(7));

    Ok(())
}

/// A changed slot appears in BOTH `verified` (as a `SlotChange`) and `fetched`
/// (as `Value`); an unchanged slot appears only in `fetched`.
#[tokio::test(flavor = "multi_thread")]
async fn changed_slot_in_verified_and_fetched_unchanged_only_fetched() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x22);
    install_mock_erc20(&mut cache, pool);

    let slot_changed = U256::from(8);
    let slot_unchanged = U256::from(9);
    // Seed EVM-visible cached baselines.
    cache
        .db_mut()
        .insert_account_storage(pool, slot_changed, U256::from(100))?;
    cache
        .db_mut()
        .insert_account_storage(pool, slot_unchanged, U256::from(200))?;

    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([
        ((pool, slot_changed), U256::from(999)),
        ((pool, slot_unchanged), U256::from(200)),
    ])));

    let plan = ColdStartPlan {
        verify: vec![(pool, slot_changed), (pool, slot_unchanged)],
        ..Default::default()
    };
    let results = cache.execute_cold_start_round(&plan).results;

    // fetched has both (Value for each).
    assert_eq!(results.fetched.len(), 2);
    assert_eq!(
        fetch_of(&results, pool, slot_changed),
        SlotFetch::Value(U256::from(999))
    );
    assert_eq!(
        fetch_of(&results, pool, slot_unchanged),
        SlotFetch::Value(U256::from(200))
    );

    // verified has only the changed slot.
    assert_eq!(results.verified.len(), 1);
    assert_eq!(results.verified[0].slot, slot_changed);
    assert_eq!(results.verified[0].old, U256::from(100));
    assert_eq!(results.verified[0].new, U256::from(999));

    // The change was injected; the unchanged slot is untouched.
    assert_eq!(
        cache.cached_storage_value(pool, slot_changed),
        Some(U256::from(999))
    );
    assert_eq!(
        cache.cached_storage_value(pool, slot_unchanged),
        Some(U256::from(200))
    );

    Ok(())
}

/// An empty plan is a valid no-op round.
#[tokio::test(flavor = "multi_thread")]
async fn empty_plan_is_noop_round() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut planner = OneShotPlanner {
        plan: ColdStartPlan::default(),
    };
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;
    assert_eq!(
        report.rounds, 1,
        "the initial empty plan still executes as one round"
    );
    assert_eq!(report.changed_slots, 0);
    assert_eq!(report.failed_slots, 0);
    Ok(())
}

/// A verify-bearing round on a cache with no fetcher errors `NoBatchFetcher`
/// (the per-round guard), rather than silently no-opping.
#[tokio::test(flavor = "multi_thread")]
async fn verify_round_without_fetcher_errors_no_batch_fetcher() -> Result<()> {
    let mut cache = no_fetcher_cache().await?;
    let pool = Address::repeat_byte(0x33);
    let mut planner = OneShotPlanner {
        plan: ColdStartPlan {
            verify: vec![(pool, U256::from(8))],
            ..Default::default()
        },
    };
    let err = cache
        .run_cold_start(&mut planner, ColdStartConfig::default())
        .expect_err("verify round with no fetcher must error");
    assert!(matches!(err, ColdStartError::NoBatchFetcher), "got {err:?}");
    Ok(())
}

/// `max_rounds` is the maximum number of EXECUTED rounds: an always-`Continue`
/// planner trips `RoundBudgetExceeded` after exactly `max_rounds` rounds.
#[tokio::test(flavor = "multi_thread")]
async fn max_rounds_boundary_always_continue_exceeds() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut planner = LoopPlanner::always_continue();
    let cfg = ColdStartConfig {
        max_rounds: 3,
        ..Default::default()
    };
    let err = cache
        .run_cold_start(&mut planner, cfg)
        .expect_err("always-continue must exceed the budget");
    assert!(
        matches!(err, ColdStartError::RoundBudgetExceeded { max_rounds: 3 }),
        "got {err:?}"
    );
    assert_eq!(
        planner.on_results_calls, 3,
        "exactly max_rounds rounds executed before erroring"
    );
    Ok(())
}

/// A planner that returns `Done` exactly on round `max_rounds` succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn max_rounds_boundary_done_on_last_round_succeeds() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut planner = LoopPlanner::done_after(3);
    let cfg = ColdStartConfig {
        max_rounds: 3,
        ..Default::default()
    };
    let report = cache.run_cold_start(&mut planner, cfg)?;
    assert_eq!(report.rounds, 3);
    Ok(())
}

/// A multi-round cold start runs `initial_plan` then one `Continue` then `Done`
/// (exactly two rounds), and round 2's `on_results` sees round 1's injection via
/// the `StateView`.
#[tokio::test(flavor = "multi_thread")]
async fn multi_round_continuation_sees_injection() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x44);
    install_mock_erc20(&mut cache, pool);

    let slot_a = U256::from(8);
    let slot_b = U256::from(9);
    cache
        .db_mut()
        .insert_account_storage(pool, slot_a, U256::from(100))?;

    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([
        ((pool, slot_a), U256::from(999)),
        ((pool, slot_b), U256::from(7)),
    ])));

    let mut planner = TwoRoundPlanner {
        pool,
        slot_a,
        slot_b,
        phase: 0,
        observed_slot_a_after_round1: None,
    };
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    assert_eq!(report.rounds, 2, "initial + one continue = two rounds");
    assert_eq!(
        planner.observed_slot_a_after_round1,
        Some(U256::from(999)),
        "on_results must see round 1's dual-layer injection via StateView"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// S2 acceptance tests: accounts + discover, restrict_to, mid-round failure
// ---------------------------------------------------------------------------

/// A discover view-call captures the `(address, slot)` pairs and accounts it
/// touches. `balanceOf(owner)` SLOADs the token's balance mapping slot, so that
/// slot and the token account appear in the captured access list.
#[tokio::test(flavor = "multi_thread")]
async fn discover_captures_touched_slots() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x55);
    let owner = Address::repeat_byte(0x56);
    // The block beneficiary (default `Address::ZERO`) is credited gas during a
    // discover call's transact; install it so the offline run does not fetch it
    // (mirrors `overlay_call_raw_with_access_list_captures_read_set`).
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    let plan = ColdStartPlan {
        discover: vec![ColdStartCall {
            from: owner,
            to: token,
            calldata: balance_of_calldata(owner),
            restrict_to: None,
        }],
        ..Default::default()
    };
    let outcome = cache.execute_cold_start_round(&plan);

    assert!(
        outcome.error.is_none(),
        "discover on a local contract succeeds"
    );
    let results = outcome.results;
    assert_eq!(results.discovered.len(), 1, "one result per discover call");
    let call = &results.discovered[0];
    assert!(
        call.result.is_success(),
        "balanceOf succeeds: {:?}",
        call.result
    );
    assert!(
        call.access.accounts.contains(&token),
        "token account captured"
    );
    assert!(
        call.access
            .slots
            .contains(&(token, balance_slot_hashed(owner))),
        "balance mapping slot captured in the access list"
    );
    Ok(())
}

/// `restrict_to` filters the captured slots and accounts to the named addresses:
/// restricting to the token keeps only its entries; restricting to an address the
/// call never touched yields an empty (but observable) capture.
#[tokio::test(flavor = "multi_thread")]
async fn restrict_to_filters_captured_slots_and_accounts() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x57);
    let owner = Address::repeat_byte(0x58);
    let unrelated = Address::repeat_byte(0x59);
    // The block beneficiary (default `Address::ZERO`) is credited gas during a
    // discover call's transact; install it so the offline run does not fetch it
    // (mirrors `overlay_call_raw_with_access_list_captures_read_set`).
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    // restrict_to = [token]: only token's slots/accounts survive.
    let plan_keep = ColdStartPlan {
        discover: vec![ColdStartCall {
            from: owner,
            to: token,
            calldata: balance_of_calldata(owner),
            restrict_to: Some(vec![token]),
        }],
        ..Default::default()
    };
    let kept = cache.execute_cold_start_round(&plan_keep).results;
    let access = &kept.discovered[0].access;
    assert!(
        access.slots.contains(&(token, balance_slot_hashed(owner))),
        "token's slot survives restrict_to=[token]"
    );
    assert!(
        access.accounts.iter().all(|a| *a == token),
        "only the token account survives restrict_to=[token]: {:?}",
        access.accounts
    );
    assert!(
        access.slots.iter().all(|(a, _)| *a == token),
        "only token slots survive restrict_to=[token]"
    );

    // restrict_to = [unrelated]: nothing the call touched matches → empty capture.
    let plan_empty = ColdStartPlan {
        discover: vec![ColdStartCall {
            from: owner,
            to: token,
            calldata: balance_of_calldata(owner),
            restrict_to: Some(vec![unrelated]),
        }],
        ..Default::default()
    };
    let empty = cache.execute_cold_start_round(&plan_empty).results;
    let access = &empty.discovered[0].access;
    assert!(
        access.slots.is_empty(),
        "restrict_to an untouched address yields empty slots"
    );
    assert!(
        access.accounts.is_empty(),
        "and empty accounts — distinct from a non-empty capture"
    );
    Ok(())
}

/// A round declaring only `accounts`/`discover` (no verify/probe) runs even when
/// no batch fetcher is configured — the per-round `NoBatchFetcher` guard fires
/// only for verify/probe-bearing rounds. This is the Balancer round-1 case.
#[tokio::test(flavor = "multi_thread")]
async fn discover_only_round_runs_without_fetcher() -> Result<()> {
    let mut cache = no_fetcher_cache().await?;
    let token = Address::repeat_byte(0x5a);
    let owner = Address::repeat_byte(0x5b);
    // The block beneficiary (default `Address::ZERO`) is credited gas during a
    // discover call's transact; install it so the offline run does not fetch it
    // (mirrors `overlay_call_raw_with_access_list_captures_read_set`).
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    let mut planner = OneShotPlanner {
        plan: ColdStartPlan {
            accounts: vec![token], // already installed → ensure is a no-op
            discover: vec![ColdStartCall {
                from: owner,
                to: token,
                calldata: balance_of_calldata(owner),
                restrict_to: Some(vec![token]),
            }],
            ..Default::default()
        },
    };
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;
    assert_eq!(report.rounds, 1);
    assert!(
        report.discovered_slots >= 1,
        "the discover-only round captured at least the balance slot"
    );
    Ok(())
}

/// An accounts-phase hard error (the first phase) leaves every declared verify
/// slot `NotAttempted` and injects nothing — the partial results are still
/// returned.
#[tokio::test(flavor = "multi_thread")]
async fn accounts_failure_marks_verify_slots_not_attempted() -> Result<()> {
    let (mut cache, asserter) = setup_cache_with_asserter().await?;
    let pool = Address::repeat_byte(0x5c);
    let uninstalled = Address::repeat_byte(0x5d);
    install_mock_erc20(&mut cache, pool);
    let slot = U256::from(8);
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (pool, slot),
        U256::from(42),
    )])));
    // Force the account fetch for `uninstalled` to fail deterministically.
    for _ in 0..8 {
        asserter.push_failure_msg("account fetch failed (offline test)");
    }

    let plan = ColdStartPlan {
        accounts: vec![uninstalled],
        verify: vec![(pool, slot)],
        ..Default::default()
    };
    let outcome = cache.execute_cold_start_round(&plan);

    assert!(
        outcome.error.is_some(),
        "an accounts-phase failure is a hard error"
    );
    // verify never ran (it follows the accounts phase) → its slot is NotAttempted.
    assert_eq!(
        fetch_of(&outcome.results, pool, slot),
        SlotFetch::NotAttempted,
        "an unreached verify slot is NotAttempted, not silently dropped"
    );
    assert!(
        outcome.results.verified.is_empty(),
        "nothing injected on accounts failure"
    );
    assert_eq!(
        cache.cached_storage_value(pool, slot),
        Some(U256::ZERO),
        "the slot was not warmed (StorageCleared reads 0, not the fetcher's 42)"
    );
    Ok(())
}

/// A mid-round hard error propagates from `run_cold_start` and `on_results` is
/// NOT called for the errored round.
#[tokio::test(flavor = "multi_thread")]
async fn mid_round_failure_propagates_and_skips_on_results() -> Result<()> {
    let (mut cache, asserter) = setup_cache_with_asserter().await?;
    let uninstalled = Address::repeat_byte(0x5e);
    for _ in 0..8 {
        asserter.push_failure_msg("account fetch failed (offline test)");
    }

    struct FlagPlanner {
        acct: Address,
        on_results_called: bool,
    }
    impl ColdStartPlanner for FlagPlanner {
        fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
            ColdStartPlan {
                accounts: vec![self.acct],
                ..Default::default()
            }
        }
        fn on_results(&mut self, _r: &ColdStartResults, _s: &dyn StateView) -> ColdStartStep {
            self.on_results_called = true;
            ColdStartStep::Done
        }
    }

    let mut planner = FlagPlanner {
        acct: uninstalled,
        on_results_called: false,
    };
    let err = cache
        .run_cold_start(&mut planner, ColdStartConfig::default())
        .expect_err("an accounts-phase failure errors the run");
    assert!(matches!(err, ColdStartError::Fetch(_)), "got {err:?}");
    assert!(
        !planner.on_results_called,
        "on_results must not run for an errored round"
    );
    Ok(())
}

/// A discover-phase hard error (the last phase) preserves the verify outcomes
/// already computed earlier in the round — they are classified, not
/// `NotAttempted` — while the failed discover call yields no result.
#[tokio::test(flavor = "multi_thread")]
async fn discover_failure_preserves_earlier_verify_outcomes() -> Result<()> {
    let (mut cache, asserter) = setup_cache_with_asserter().await?;
    let pool = Address::repeat_byte(0x5f);
    let uninstalled_callee = Address::repeat_byte(0x60);
    install_default_account(&mut cache, Address::ZERO);
    install_mock_erc20(&mut cache, pool);
    let slot = U256::from(8);
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (pool, slot),
        U256::from(99),
    )])));
    // The uninstalled callee's code load fails → the discover call errors.
    for _ in 0..8 {
        asserter.push_failure_msg("code fetch failed (offline test)");
    }

    let plan = ColdStartPlan {
        verify: vec![(pool, slot)],
        discover: vec![ColdStartCall {
            from: Address::ZERO,
            to: uninstalled_callee,
            calldata: Bytes::new(),
            restrict_to: None,
        }],
        ..Default::default()
    };
    let outcome = cache.execute_cold_start_round(&plan);

    assert!(
        outcome.error.is_some(),
        "the discover call failure is a hard error"
    );
    // verify ran BEFORE discover, so its outcome is classified, not NotAttempted.
    assert_eq!(
        fetch_of(&outcome.results, pool, slot),
        SlotFetch::Value(U256::from(99)),
        "verify outcomes computed before the discover failure are preserved"
    );
    assert_eq!(
        outcome.results.verified.len(),
        1,
        "verify injected before discover failed"
    );
    assert!(
        outcome.results.discovered.is_empty(),
        "the failed discover call yields no result"
    );
    Ok(())
}

/// The canonical Balancer-style two-round cold start, fully offline: round 1
/// discovers the touched slots via a view call (`restrict_to` the target), round
/// 2 verifies exactly those discovered slots, and the discovered slot ends up
/// warm — with no RPC issued.
#[tokio::test(flavor = "multi_thread")]
async fn two_round_discover_then_verify_offline() -> Result<()> {
    let (mut cache, asserter) = setup_cache_with_asserter().await?;
    let token = Address::repeat_byte(0x61);
    let owner = Address::repeat_byte(0x62);
    // The block beneficiary (default `Address::ZERO`) is credited gas during a
    // discover call's transact; install it so the offline run does not fetch it
    // (mirrors `overlay_call_raw_with_access_list_captures_read_set`).
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    let hashed = balance_slot_hashed(owner);
    // Round 2's verify fetcher returns a fresh value for the discovered slot.
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, hashed),
        U256::from(1000),
    )])));

    struct BalancerLike {
        token: Address,
        owner: Address,
        phase: usize,
    }
    impl ColdStartPlanner for BalancerLike {
        fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
            ColdStartPlan {
                accounts: vec![self.token],
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
        fn on_results(
            &mut self,
            results: &ColdStartResults,
            _state: &dyn StateView,
        ) -> ColdStartStep {
            self.phase += 1;
            if self.phase == 1 {
                // Verify exactly the slots discovered in round 1.
                let verify: Vec<_> = results.discovered[0].access.slots.iter().copied().collect();
                assert!(
                    !verify.is_empty(),
                    "round 1 must discover at least one slot"
                );
                ColdStartStep::Continue(ColdStartPlan {
                    verify,
                    ..Default::default()
                })
            } else {
                ColdStartStep::Done
            }
        }
    }

    let mut planner = BalancerLike {
        token,
        owner,
        phase: 0,
    };
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    assert_eq!(report.rounds, 2, "discover round + verify round");
    assert_eq!(
        cache.cached_storage_value(token, hashed),
        Some(U256::from(1000)),
        "the discovered slot was warmed by round 2's verify"
    );
    assert!(
        asserter.read_q().is_empty(),
        "no RPC was issued during the fully-offline cold start"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// S3 acceptance tests: probe phase (classify at the pinned block, no inject)
// ---------------------------------------------------------------------------

/// A probe reads and classifies a slot at the pinned block but does NOT inject
/// it: the fetched value is reported in `results.probed`, yet the cache keeps its
/// prior value and no `SlotChange` is recorded.
#[tokio::test(flavor = "multi_thread")]
async fn probe_classifies_without_injecting() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x63);
    install_mock_erc20(&mut cache, pool);
    let slot = U256::from(8);
    // Cache holds 100; the fetcher reports a different value the probe must NOT inject.
    cache
        .db_mut()
        .insert_account_storage(pool, slot, U256::from(100))?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (pool, slot),
        U256::from(777),
    )])));

    let plan = ColdStartPlan {
        probe: vec![(pool, slot)],
        ..Default::default()
    };
    let outcome = cache.execute_cold_start_round(&plan);
    assert!(outcome.error.is_none());
    let results = outcome.results;

    // The probe classified the freshly-fetched value...
    assert_eq!(results.probed.len(), 1, "one outcome per probe slot");
    assert_eq!(results.probed[0].address, pool);
    assert_eq!(results.probed[0].slot, slot);
    assert_eq!(results.probed[0].fetch, SlotFetch::Value(U256::from(777)));

    // ...but did not inject it, did not record a change, and is not a verify slot.
    assert!(
        results.verified.is_empty(),
        "probe never records a SlotChange"
    );
    assert!(
        results.fetched.is_empty(),
        "probe slots are not verify slots"
    );
    assert_eq!(
        cache.cached_storage_value(pool, slot),
        Some(U256::from(100)),
        "probe must not write the fetched value into the cache"
    );
    Ok(())
}

/// A probe classifies each slot as `Value` / `Zero` / `FetchFailed`, using the
/// same shared classification as verify, while injecting nothing.
#[tokio::test(flavor = "multi_thread")]
async fn probe_classifies_value_zero_and_failed() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x64);
    install_mock_erc20(&mut cache, pool);
    let slot_zero = U256::from(1);
    let slot_value = U256::from(2);
    let slot_fail = U256::from(3);
    cache.set_storage_batch_fetcher(mixed_fetcher(
        (pool, slot_value),
        U256::from(7),
        (pool, slot_fail),
    ));

    let plan = ColdStartPlan {
        probe: vec![(pool, slot_zero), (pool, slot_value), (pool, slot_fail)],
        ..Default::default()
    };
    let results = cache.execute_cold_start_round(&plan).results;

    assert_eq!(results.probed.len(), 3);
    let probe_of = |s: U256| {
        results
            .probed
            .iter()
            .find(|o| o.slot == s)
            .map(|o| o.fetch.clone())
            .unwrap_or_else(|| panic!("slot {s} not present in results.probed"))
    };
    assert_eq!(probe_of(slot_zero), SlotFetch::Zero);
    assert_eq!(probe_of(slot_value), SlotFetch::Value(U256::from(7)));
    assert!(matches!(probe_of(slot_fail), SlotFetch::FetchFailed { .. }));
    assert!(results.verified.is_empty(), "probe never injects");
    Ok(())
}

/// Verify and probe coexist in one round independently: the verify slot is
/// injected, the probe slot is classified but left untouched in the cache.
#[tokio::test(flavor = "multi_thread")]
async fn probe_and_verify_in_one_round_are_independent() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x65);
    install_mock_erc20(&mut cache, pool);
    let v_slot = U256::from(8);
    let p_slot = U256::from(9);
    cache
        .db_mut()
        .insert_account_storage(pool, p_slot, U256::from(100))?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([
        ((pool, v_slot), U256::from(500)),
        ((pool, p_slot), U256::from(600)),
    ])));

    let plan = ColdStartPlan {
        verify: vec![(pool, v_slot)],
        probe: vec![(pool, p_slot)],
        ..Default::default()
    };
    let results = cache.execute_cold_start_round(&plan).results;

    // verify injected v_slot.
    assert_eq!(results.verified.len(), 1);
    assert_eq!(
        cache.cached_storage_value(pool, v_slot),
        Some(U256::from(500))
    );
    // probe classified p_slot but did not inject it.
    assert_eq!(results.probed.len(), 1);
    assert_eq!(results.probed[0].fetch, SlotFetch::Value(U256::from(600)));
    assert_eq!(
        cache.cached_storage_value(pool, p_slot),
        Some(U256::from(100)),
        "probe leaves its slot untouched even alongside a verify"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// S4 acceptance tests: ColdStartPin::Hash pins the run and restores afterward
// ---------------------------------------------------------------------------

/// A fetcher that records the `BlockId` it is called with, so a test can
/// assert which block the run's reads were pinned to.
fn block_recording_fetcher(seen: Arc<Mutex<Vec<BlockId>>>) -> StorageBatchFetchFn {
    Arc::new(move |requests: Vec<(Address, U256)>, block: BlockId| {
        seen.lock().unwrap().push(block);
        requests
            .into_iter()
            .map(|(addr, slot)| (addr, slot, Ok(U256::ZERO)))
            .collect()
    })
}

/// `ColdStartPin::Hash { require_canonical }` pins every round's reads to
/// `BlockId::from((hash, Some(require_canonical)))` and restores the cache's
/// prior block when the run completes.
#[tokio::test(flavor = "multi_thread")]
async fn hash_pin_reads_at_hash_and_restores_prior_block() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x66);
    install_mock_erc20(&mut cache, pool);
    let slot = U256::from(8);

    let seen = Arc::new(Mutex::new(Vec::new()));
    cache.set_storage_batch_fetcher(block_recording_fetcher(Arc::clone(&seen)));

    let prior_block = cache.block();
    let hash = B256::repeat_byte(0xab);
    let expected = BlockId::from((hash, Some(true)));

    let mut planner = OneShotPlanner {
        plan: ColdStartPlan {
            verify: vec![(pool, slot)],
            ..Default::default()
        },
    };
    let report = cache.run_cold_start(
        &mut planner,
        ColdStartConfig {
            max_rounds: 8,
            pin: ColdStartPin::Hash {
                number: 100,
                hash,
                require_canonical: true,
            },
        },
    )?;

    assert_eq!(report.rounds, 1);
    let seen = seen.lock().unwrap();
    assert!(!seen.is_empty(), "the verify phase issued a pinned read");
    assert!(
        seen.iter().all(|b| *b == expected),
        "every read was pinned to the hash (with require_canonical): {seen:?}"
    );
    assert_eq!(
        cache.block(),
        prior_block,
        "the prior block is restored after the run"
    );
    Ok(())
}

/// The prior block is restored even when the run ends in an error
/// (`RoundBudgetExceeded`), so a failed hash-pinned run never leaves the cache
/// stuck on the pinned hash.
#[tokio::test(flavor = "multi_thread")]
async fn hash_pin_restores_prior_block_on_error() -> Result<()> {
    let mut cache = setup_cache().await?;
    let prior_block = cache.block();
    let hash = B256::repeat_byte(0xcd);

    // Empty plans → no verify/probe → no fetcher needed; the planner always
    // continues, so the run trips RoundBudgetExceeded.
    let mut planner = LoopPlanner::always_continue();
    let err = cache
        .run_cold_start(
            &mut planner,
            ColdStartConfig {
                max_rounds: 2,
                pin: ColdStartPin::Hash {
                    number: 200,
                    hash,
                    require_canonical: true,
                },
            },
        )
        .expect_err("an always-continue planner exceeds the budget");
    assert!(
        matches!(err, ColdStartError::RoundBudgetExceeded { max_rounds: 2 }),
        "got {err:?}"
    );
    assert_eq!(
        cache.block(),
        prior_block,
        "the prior block is restored even on the error path"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// verify_code phase (verified code seeding, spec §3.5 / acceptance item 9)
// ---------------------------------------------------------------------------

/// Runtime bytes for code-seed driver tests: returns one 32-byte word and
/// touches no storage, so nothing here ever reaches the mocked backend.
const SEED_RUNTIME: [u8; 10] = [0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];

/// The `NoAccountFieldsFetcher` guard fires only for pending-bearing rounds:
/// the same fetcher-less cache runs an empty round cleanly, then errors once
/// a pending seed exists.
#[tokio::test(flavor = "multi_thread")]
async fn verify_code_guard_fires_only_for_pending_bearing_rounds() -> Result<()> {
    let mut cache = no_fetcher_cache().await?;
    assert!(cache.account_fields_fetcher().is_none());

    // No pending seeds: the round runs without any fetcher and the phase is
    // a no-op.
    let outcome = cache.execute_cold_start_round(&ColdStartPlan::default());
    assert!(outcome.error.is_none(), "got {:?}", outcome.error);
    assert!(outcome.results.code_verifications.is_none());

    // A pending seed with no fields fetcher short-circuits before any read.
    cache.seed_account_code(
        Address::repeat_byte(0xc0),
        Bytes::from(SEED_RUNTIME.to_vec()),
    )?;
    let outcome = cache.execute_cold_start_round(&ColdStartPlan::default());
    assert!(
        matches!(outcome.error, Some(ColdStartError::NoAccountFieldsFetcher)),
        "got {:?}",
        outcome.error
    );
    assert!(outcome.results.code_verifications.is_none());
    Ok(())
}

/// verify_code runs before accounts, and its report survives an
/// accounts-phase hard error: the mismatched seed is purged first, then the
/// accounts phase's refetch of that same (now cold) address fails against the
/// empty mock queue — proving both the ordering and the partial-outcome
/// contract.
#[tokio::test(flavor = "multi_thread")]
async fn verify_code_report_survives_accounts_hard_error() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0xc1);
    let expected = cache.seed_account_code(pool, Bytes::from(SEED_RUNTIME.to_vec()))?;

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let actual = B256::repeat_byte(0xdd);
    cache.set_account_fields_fetcher(common::stub_fields_fetcher(
        HashMap::from([(pool, (U256::ZERO, actual))]),
        calls.clone(),
    ));

    let plan = ColdStartPlan {
        accounts: vec![pool],
        ..ColdStartPlan::default()
    };
    let outcome = cache.execute_cold_start_round(&plan);

    // The accounts phase fails: verify_code purged the mismatched seed, so
    // ensure_account falls through to the (empty) mocked backend.
    assert!(
        matches!(outcome.error, Some(ColdStartError::Fetch(_))),
        "got {:?}",
        outcome.error
    );
    // ...but the verify_code report, computed first, is preserved.
    let report = outcome
        .results
        .code_verifications
        .expect("the verify_code report must survive an accounts-phase hard error");
    assert_eq!(report.mismatched.len(), 1);
    assert_eq!(report.mismatched[0].address, pool);
    assert_eq!(report.mismatched[0].expected, expected);
    assert_eq!(report.mismatched[0].actual, actual);
    assert!(
        cache.code_seed_state(&pool).is_none(),
        "the contradicted claim was purged before the accounts phase ran"
    );
    Ok(())
}

/// Happy path: a matching seed settles in round one (report recorded, mark
/// Verified) and the phase is a no-op in round two — the fetcher is consulted
/// exactly once across both rounds.
#[tokio::test(flavor = "multi_thread")]
async fn verify_code_settles_in_one_round_then_noops() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0xc2);
    let expected = cache.seed_account_code(pool, Bytes::from(SEED_RUNTIME.to_vec()))?;

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    cache.set_account_fields_fetcher(common::stub_fields_fetcher(
        HashMap::from([(pool, (U256::from(9u64), expected))]),
        calls.clone(),
    ));

    let first = cache.execute_cold_start_round(&ColdStartPlan::default());
    assert!(first.error.is_none(), "got {:?}", first.error);
    let report = first
        .results
        .code_verifications
        .expect("a pending-bearing round records a report");
    assert_eq!(report.verified, vec![pool]);

    let second = cache.execute_cold_start_round(&ColdStartPlan::default());
    assert!(second.error.is_none());
    assert!(
        second.results.code_verifications.is_none(),
        "a settled set makes the phase a no-op"
    );
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the fields fetcher is consulted exactly once across both rounds"
    );
    Ok(())
}
