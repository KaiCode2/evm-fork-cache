//! Offline integration tests for the Phase 2 freshness primitives and the
//! optimistic verify-and-rerun loop.
//!
//! Everything runs fully offline: the cache is built over a mocked provider and
//! all "current" on-chain values come from a stubbed [`StorageBatchFetchFn`]
//! injected via `set_storage_batch_fetcher`, so no test reaches the network.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, failing_fetcher, install_default_account,
    install_mock_erc20, setup_cache, stub_fetcher,
};
use evm_fork_cache::cache::{EvmCache, EvmOverlay};
use evm_fork_cache::freshness::{
    AlwaysVerify, FreshnessController, FreshnessRegistry, NeverVerify, SimRequest, Validation,
    WallClock,
};

/// Hashed storage slot of `balanceOf[owner]` for the MockERC20 fixture.
fn balance_slot_for(owner: Address) -> U256 {
    use alloy_sol_types::SolValue;
    let key =
        alloy_primitives::keccak256((owner, U256::from(MOCK_ERC20_BALANCE_SLOT)).abi_encode());
    U256::from_be_bytes(key.0)
}

/// Encode a `transfer(to, amount)` call.
fn transfer_calldata(to: Address, amount: U256) -> Bytes {
    Bytes::from(MockERC20::transferCall { to, amount }.abi_encode())
}

// ---------------------------------------------------------------------------
// EvmCache::verify_slots
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn verify_slots_detects_and_injects_changes() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x11);
    install_mock_erc20(&mut cache, contract);

    let slot_a = U256::from(10);
    let slot_b = U256::from(20);
    // Cache holds these values.
    cache.inject_storage_batch(&[
        (contract, slot_a, U256::from(100)),
        (contract, slot_b, U256::from(200)),
    ]);

    // Stub reports slot_a changed, slot_b unchanged.
    let values = HashMap::from([
        ((contract, slot_a), U256::from(999)),
        ((contract, slot_b), U256::from(200)),
    ]);
    cache.set_storage_batch_fetcher(stub_fetcher(values));

    let changed = cache.verify_slots(&[(contract, slot_a), (contract, slot_b)])?;

    assert_eq!(changed.len(), 1, "only slot_a changed");
    let change = &changed[0];
    assert_eq!(change.address, contract);
    assert_eq!(change.slot, slot_a);
    assert_eq!(change.old, U256::from(100));
    assert_eq!(change.new, U256::from(999));

    // The fresh value was injected; the unchanged one is untouched.
    assert_eq!(
        cache.cached_storage_value(contract, slot_a),
        Some(U256::from(999))
    );
    assert_eq!(
        cache.cached_storage_value(contract, slot_b),
        Some(U256::from(200))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_slots_unchanged_returns_empty() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x22);
    install_mock_erc20(&mut cache, contract);

    let slot = U256::from(7);
    cache.inject_storage_batch(&[(contract, slot, U256::from(42))]);
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (contract, slot),
        U256::from(42),
    )])));

    let changed = cache.verify_slots(&[(contract, slot)])?;
    assert!(changed.is_empty(), "no change should be reported");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_slots_treats_unseen_slot_as_zero() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x33);
    install_mock_erc20(&mut cache, contract);

    // Slot never cached; fetcher reports a non-zero value → counts as a change
    // from the implicit zero a sim would have read.
    let slot = U256::from(5);
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (contract, slot),
        U256::from(77),
    )])));

    let changed = cache.verify_slots(&[(contract, slot)])?;
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].old, U256::ZERO);
    assert_eq!(changed[0].new, U256::from(77));
    assert_eq!(
        cache.cached_storage_value(contract, slot),
        Some(U256::from(77))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_slots_skips_failed_fetches() -> Result<()> {
    let mut cache = setup_cache().await?;
    // A fetcher that errors every request: failed fetches are skipped (not
    // treated as changes), so verify_slots returns no changes and does not panic.
    cache.set_storage_batch_fetcher(failing_fetcher());
    let contract = Address::repeat_byte(0x44);
    cache.inject_storage_batch(&[(contract, U256::from(1), U256::from(5))]);
    let changed = cache.verify_slots(&[(contract, U256::from(1))])?;
    assert!(
        changed.is_empty(),
        "failed fetches are skipped, not changes"
    );
    // Cached value is unchanged.
    assert_eq!(
        cache.cached_storage_value(contract, U256::from(1)),
        Some(U256::from(5))
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// EvmCache::purge_account
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn purge_account_drops_account_and_storage_from_both_layers() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x55);
    let owner = Address::repeat_byte(0x66);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    // Populate the CacheDB overlay (layer 1) via an EVM read and the
    // BlockchainDb backend (layer 2) directly.
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(1000),
    )?;
    let _ = common::balance_of(&mut cache, token, owner)?;
    assert!(
        cache.cache_db_storage_slot_count(token) > 0,
        "overlay populated"
    );

    cache.inject_storage_batch(&[(token, U256::from(99), U256::from(1))]);
    assert!(
        cache.pool_storage_slot_count(token) > 0,
        "backend populated"
    );

    // The account info exists in the overlay (from the EVM read / install).
    assert!(
        cache.db_mut().cache.accounts.contains_key(&token),
        "overlay account present before purge"
    );

    cache.purge_account(token);

    // Account gone from the overlay accounts map (which also holds its storage).
    assert!(
        !cache.db_mut().cache.accounts.contains_key(&token),
        "overlay account removed"
    );
    assert_eq!(
        cache.cache_db_storage_slot_count(token),
        0,
        "overlay storage gone"
    );
    // Storage gone from the backend.
    assert_eq!(
        cache.pool_storage_slot_count(token),
        0,
        "backend storage gone"
    );
    // Account gone from the backend accounts map.
    {
        let accounts = cache.blockchain_db().accounts().read();
        assert!(!accounts.contains_key(&token), "backend account removed");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// EvmOverlay::call_raw_with_access_list (read-set capture)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn overlay_call_raw_with_access_list_captures_read_set() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x77);
    let owner = Address::repeat_byte(0x88);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    let balance_slot = U256::from(MOCK_ERC20_BALANCE_SLOT);
    cache.insert_mapping_storage_slot(token, balance_slot, owner, U256::from(1000))?;

    let snapshot = cache.create_snapshot();
    let mut overlay = EvmOverlay::new(Arc::clone(&snapshot), None);

    // balanceOf(owner) reads the token's balance mapping slot.
    let call = common::MockERC20::balanceOfCall { account: owner };
    let (result, access) =
        overlay.call_raw_with_access_list(owner, token, Bytes::from(call.abi_encode()))?;

    assert!(result.is_success(), "balanceOf should succeed: {result:?}");
    assert!(access.accounts.contains(&token), "token account touched");
    // The hashed balance slot for owner should be in the read set.
    let hashed = {
        use alloy_sol_types::SolValue;
        let key = alloy_primitives::keccak256((owner, balance_slot).abi_encode());
        U256::from_be_bytes(key.0)
    };
    assert!(
        access.slots.contains(&(token, hashed)),
        "balance mapping slot captured in read set"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_override_slot_takes_precedence() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x99);
    install_mock_erc20(&mut cache, contract);
    let slot = U256::from(3);
    cache.inject_storage_batch(&[(contract, slot, U256::from(1))]);

    let snapshot = cache.create_snapshot();
    let mut overlay = EvmOverlay::new(snapshot, None);
    overlay.override_slot(contract, slot, U256::from(999));

    use revm::database_interface::Database;
    assert_eq!(overlay.storage(contract, slot)?, U256::from(999));

    Ok(())
}

// Compile-time guard: a cache built over a mocked provider exposes a fetcher.
#[tokio::test(flavor = "multi_thread")]
async fn cache_has_fetcher_over_mock_provider() -> Result<()> {
    let cache: EvmCache = setup_cache().await?;
    assert!(
        cache.storage_batch_fetcher().is_some(),
        "mock-provider cache has a fetcher"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// FreshnessController::run — the optimistic loop
// ---------------------------------------------------------------------------

/// Build a cache with a MockERC20 whose `owner` balance is `balance`.
async fn cache_with_balance(token: Address, owner: Address, balance: U256) -> Result<EvmCache> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    if balance > U256::ZERO {
        cache.inject_storage_batch(&[(token, balance_slot_for(owner), balance)]);
    }
    Ok(cache)
}

#[tokio::test(flavor = "multi_thread")]
async fn run_match_path_confirmed() -> Result<()> {
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    // Owner funded; the optimistic transfer succeeds.
    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    // Fetcher reports the SAME balance → nothing changed.
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(1000),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let req = SimRequest::new(owner, token, transfer_calldata(recipient, U256::from(100)));
    let sim = controller.run(&mut cache, vec![req])?;

    // optimistic() is readable before validate().
    assert_eq!(sim.optimistic().len(), 1);
    let optimistic_gas = sim.optimistic()[0].gas_used;
    assert!(optimistic_gas > 0);

    let validation = sim.validate().await;
    assert!(
        matches!(validation, Validation::Confirmed),
        "unchanged values should confirm: {validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_mismatch_path_corrected_only_affected_rerun() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    // A second, independent token whose slot will NOT change.
    let token2 = Address::repeat_byte(0x77);
    let owner2 = Address::repeat_byte(0x88);

    // Both owners are funded so the optimistic transfers SUCCEED (and so their
    // balance slots land in the captured read set). The captured read set is the
    // basis for reconciliation; a reverting sim records no SLOADs.
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, owner2);
    install_mock_erc20(&mut cache, token);
    install_mock_erc20(&mut cache, token2);
    cache.inject_storage_batch(&[
        (token, balance_slot_for(owner), U256::from(1000)),
        (token2, balance_slot_for(owner2), U256::from(5000)),
    ]);

    // Fetcher: owner's balance slot DROPPED to 50 (< the 100 transfer, so the
    // re-run now reverts); owner2's slot unchanged; recipient slots read as zero
    // (matching the snapshot → no change).
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([
        ((token, balance_slot_for(owner)), U256::from(50)),
        ((token2, balance_slot_for(owner2)), U256::from(5000)),
    ])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let req1 = SimRequest::new(owner, token, transfer_calldata(recipient, U256::from(100)));
    let req2 = SimRequest::new(
        owner2,
        token2,
        transfer_calldata(recipient, U256::from(100)),
    );
    let sim = controller.run(&mut cache, vec![req1, req2])?;

    // Optimistic: both transfers succeeded (each emits a Transfer log).
    let opt = sim.optimistic().to_vec();
    assert_eq!(opt.len(), 2);
    assert!(
        !opt[0].logs.is_empty(),
        "req1 optimistic should succeed (a log)"
    );
    assert!(
        !opt[1].logs.is_empty(),
        "req2 optimistic should succeed (a log)"
    );

    let validation = sim.validate().await;
    match validation {
        Validation::Corrected { results, changed } => {
            // Exactly owner's balance slot changed.
            assert_eq!(
                changed.len(),
                1,
                "only owner's balance changed: {changed:?}"
            );
            assert_eq!(changed[0].address, token);
            assert_eq!(changed[0].slot, balance_slot_for(owner));
            assert_eq!(changed[0].old, U256::from(1000));
            assert_eq!(changed[0].new, U256::from(50));

            // req1 was re-run with the reduced balance → now reverts (no log) and
            // differs from its optimistic (successful) result.
            assert!(
                results[0].logs.is_empty(),
                "corrected req1 should now revert and emit no log"
            );
            assert_ne!(
                results[0].gas_used, opt[0].gas_used,
                "corrected req1 gas should differ from the optimistic success"
            );

            // req2's slot did not change → its result is untouched (== optimistic).
            assert_eq!(results[1].gas_used, opt[1].gas_used, "req2 not re-run");
            assert_eq!(results[1].logs.len(), opt[1].logs.len(), "req2 unchanged");
        }
        other => panic!("expected Corrected, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_drains_pending_on_next_run() -> Result<()> {
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    // Owner funded with 1000 so the optimistic transfer succeeds (read set
    // captures the balance slot). Fetcher reports a CHANGED balance of 2000.
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache.inject_storage_batch(&[(token, balance_slot_for(owner), U256::from(1000))]);
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(2000),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);

    // First run: detects the change and queues a correction.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let validation = sim.validate().await;
    assert!(matches!(validation, Validation::Corrected { .. }));
    assert_eq!(controller.pending_len(), 1, "a correction was queued");

    // The live cache still holds the OLD value (no cross-thread mutation).
    assert_eq!(
        cache.cached_storage_value(token, balance_slot_for(owner)),
        Some(U256::from(1000))
    );

    // Second run: drains the pending correction into the cache before snapshotting.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert_eq!(controller.pending_len(), 0, "pending drained");
    assert_eq!(
        cache.cached_storage_value(token, balance_slot_for(owner)),
        Some(U256::from(2000)),
        "correction applied to the live cache"
    );

    // The optimistic transfer still succeeds and the fetcher now matches the
    // applied value → Confirmed.
    assert!(
        !sim.optimistic()[0].logs.is_empty(),
        "optimistic still succeeds"
    );
    let validation = sim.validate().await;
    assert!(
        matches!(validation, Validation::Confirmed),
        "{validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_unverified_on_fetcher_error() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(failing_fetcher());

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let validation = sim.validate().await;
    assert!(
        matches!(validation, Validation::Unverified { .. }),
        "fetcher error should yield Unverified: {validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_into_optimistic_aborts_validation() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(1000),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let results = sim.into_optimistic();
    assert_eq!(results.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn never_verify_skips_predicted_but_reconciles_read_set() -> Result<()> {
    // NeverVerify selects nothing from the predicted candidates, but the
    // validator still reconciles the actual read set, so a real change is caught.
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    // Owner funded so the optimistic transfer succeeds and the balance slot is
    // captured in the read set; the fetcher then reports a changed value.
    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(50),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), NeverVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let validation = sim.validate().await;
    assert!(
        matches!(validation, Validation::Corrected { .. }),
        "actual-read-set reconcile should still catch the change: {validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn pinned_slot_is_not_verified() -> Result<()> {
    // Pin the owner's balance slot: even though the fetcher would report a
    // change, a pinned slot is excluded from verification → Confirmed.
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(9999), // would be a change if verified
    )])));

    let mut registry = FreshnessRegistry::new();
    registry.pin_slot(token, balance_slot_for(owner));
    let mut controller = FreshnessController::new(registry, AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let validation = sim.validate().await;
    assert!(
        matches!(validation, Validation::Confirmed),
        "pinned slot must not be verified: {validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn wall_clock_controller_runs() -> Result<()> {
    // Exercise the WallClock variant end-to-end (BlockClock is the default).
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(1000),
    )])));

    let mut controller =
        FreshnessController::with_clock(FreshnessRegistry::new(), AlwaysVerify, WallClock);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let validation = sim.validate().await;
    assert!(
        matches!(validation, Validation::Confirmed),
        "{validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_through_becomes_volatile_after_boundary() -> Result<()> {
    use evm_fork_cache::freshness::BlockClock;

    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(2000), // a change, if verified
    )])));

    // Valid through block 100. At block 100 it's still pinned; at 101 volatile.
    let mut registry = FreshnessRegistry::new();
    registry.valid_through_slot(token, balance_slot_for(owner), 100);

    let clock = BlockClock::at(100);
    let mut controller = FreshnessController::with_clock(registry, AlwaysVerify, clock.clone());

    // At block 100: still valid → not verified → Confirmed.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert!(matches!(sim.validate().await, Validation::Confirmed));

    // Advance past the boundary: now volatile → the change is caught.
    clock.set_block(101);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert!(
        matches!(sim.validate().await, Validation::Corrected { .. }),
        "past ValidThrough boundary the slot is volatile and the change is caught"
    );
    Ok(())
}
