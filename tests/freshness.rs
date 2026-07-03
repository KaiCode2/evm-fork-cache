//! Offline integration tests for the Phase 2 freshness primitives and the
//! optimistic verify-and-rerun loop.
//!
//! Everything runs fully offline: the cache is built over a mocked provider and
//! all "current" on-chain values come from a stubbed [`StorageBatchFetchFn`]
//! injected via `set_storage_batch_fetcher`, so no test reaches the network.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;

use common::{
    Gate, MOCK_ERC20_BALANCE_SLOT, MockERC20, failing_fetcher, gated_tracking_fetcher,
    install_default_account, install_mock_erc20, panicking_fetcher, setup_cache, stub_fetcher,
};
use evm_fork_cache::cache::{
    EvmCache, EvmOverlay, SimStatus, SlotObservationTracker, StorageBatchFetchFn,
};
use evm_fork_cache::errors::StorageFetchResult;
use evm_fork_cache::freshness::{
    AlwaysVerify, BlockClock, FreshnessController, FreshnessParams, FreshnessRegistry, NeverVerify,
    ObservationDriven, SimRequest, Validation, WallClock,
};
use revm::state::{AccountInfo, Bytecode};

/// Runtime bytecode that returns `blockhash(0)`:
/// `PUSH1 0 BLOCKHASH PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN`.
///
/// Control flow doesn't branch on the hash, but the *result* embeds it — the
/// exact shape the validator cannot vouch for, since its overlays resolve
/// `BLOCKHASH` to ZERO.
const BLOCKHASH_READER_RUNTIME: &[u8] = &[
    0x60, 0x00, 0x40, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xF3,
];

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

/// Encode a `balanceOf(account)` view call.
fn balance_of_calldata(account: Address) -> Bytes {
    Bytes::from(MockERC20::balanceOfCall { account }.abi_encode())
}

/// Decode a `balanceOf` return value from a [`CallSimulationResult`] `output`.
fn decode_balance(output: &Bytes) -> U256 {
    MockERC20::balanceOfCall::abi_decode_returns(output).expect("decode balanceOf return")
}

/// Yield and briefly sleep so that any background validation task that survived
/// (i.e. was *not* aborted) would get a chance to run and mutate shared state.
/// Used by the abort tests: if the task were alive it would queue a correction
/// within this window, so a subsequent `pending_len() == 0` assertion is
/// meaningful rather than merely racing the spawn.
async fn settle() {
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    tokio::task::yield_now().await;
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
    // Cache holds these values, seeded OVERLAY-resident so they are EVM-visible:
    // `contract` is a StorageCleared MockERC20, and after the §16.0 fix a
    // backend-only `inject_storage_batch` seed on a StorageCleared account is
    // shadowed to ZERO by `cached_storage_value` (it mirrors the EVM SLOAD). The
    // test's intent is that the cache *holds* these values, so seed the layer that
    // actually wins (mirrors `state_update::balance_tracking_scenario`).
    cache
        .db_mut()
        .insert_account_storage(contract, slot_a, U256::from(100))?;
    cache
        .db_mut()
        .insert_account_storage(contract, slot_b, U256::from(200))?;

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
    // Overlay-resident seed so the value is EVM-visible (see the note in
    // `verify_slots_detects_and_injects_changes`): a backend-only seed on this
    // StorageCleared MockERC20 would read as ZERO under the §16.0 fix, so the
    // fetcher's matching 42 would (incorrectly) look like a 0 -> 42 change.
    cache
        .db_mut()
        .insert_account_storage(contract, slot, U256::from(42))?;
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
        cache.contract_storage_slot_count(token) > 0,
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
        cache.contract_storage_slot_count(token),
        0,
        "backend storage gone"
    );
    // Account gone from the backend accounts map.
    {
        let accounts = cache.unchecked_blockchain_db().accounts().read();
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

    let snapshot = cache.snapshot();
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

    let snapshot = cache.snapshot();
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
        // Overlay-resident seed so the balance is EVM-visible: `token` is a
        // StorageCleared MockERC20, so a backend-only seed reads as ZERO via the
        // account_state-aware read path (invisible to the optimistic sim and the
        // snapshot). Mirrors `state_update::balance_tracking_scenario`.
        cache
            .db_mut()
            .insert_account_storage(token, balance_slot_for(owner), balance)?;
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

    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::ConfirmedStorage),
        "unchanged values should confirm: {validation:?}"
    );
    Ok(())
}

/// G5 / WS-9: a sim that reads `BLOCKHASH` must fail closed as `Unverified`.
///
/// The controller's overlays carry no block hashes, so the opcode resolves to
/// ZERO — storage verification cannot vouch for the result. Before 0.2.0 this
/// sim (which touches no volatile storage at all) sailed through the
/// empty-verify-set early path and was silently `ConfirmedStorage`.
#[tokio::test(flavor = "multi_thread")]
async fn run_blockhash_reading_sim_fails_closed_as_unverified() -> Result<()> {
    let caller = Address::repeat_byte(0x0c);
    let target = Address::repeat_byte(0x0d);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, caller);
    let bytecode = Bytecode::new_raw(Bytes::from_static(BLOCKHASH_READER_RUNTIME));
    let code_hash = bytecode.hash_slow();
    cache.db_mut().insert_account_info(
        target,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 0,
            code: Some(bytecode),
            code_hash,
            account_id: None,
        },
    );
    cache
        .db_mut()
        .replace_account_storage(target, Default::default())?;
    // Pin a concrete height so `blockhash(0)` is IN the EVM's valid lookback
    // range and revm actually consults the database. (Out-of-range requests
    // return spec-mandated ZERO without a DB call — correct on-chain too, so
    // they are deliberately not flagged.)
    cache.set_block(BlockId::number(100));
    // A fetcher that would happily "confirm" anything — it must never get the
    // chance to vouch for a hash-dependent result.
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::new()));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(caller, target, Bytes::new())],
    )?;
    assert_eq!(
        sim.optimistic().len(),
        1,
        "the optimistic run still executes"
    );

    let validation = sim.validate().await?;
    match validation {
        Validation::Unverified { reason } => {
            assert!(
                reason.contains("BLOCKHASH"),
                "the reason must name the unverifiable read: {reason}"
            );
        }
        other => panic!("BLOCKHASH-reading sim must fail closed, got {other:?}"),
    }
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
    // Overlay-resident seeds (EVM-visible): both tokens are StorageCleared, so a
    // backend-only seed would read ZERO via the account_state-aware read path.
    cache
        .db_mut()
        .insert_account_storage(token, balance_slot_for(owner), U256::from(1000))?;
    cache
        .db_mut()
        .insert_account_storage(token2, balance_slot_for(owner2), U256::from(5000))?;

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
    // T9: the optimistic path does not run transfer tracking, so token_deltas is
    // always empty — pin that documented stub behavior on both results.
    assert!(
        opt[0].token_deltas.is_empty(),
        "optimistic token_deltas are empty (no transfer tracking)"
    );
    assert!(
        opt[1].token_deltas.is_empty(),
        "optimistic token_deltas empty"
    );

    let validation = sim.validate().await?;
    match validation {
        Validation::Corrected {
            results,
            changed_slots,
            ..
        } => {
            // Exactly owner's balance slot changed.
            assert_eq!(
                changed_slots.len(),
                1,
                "only owner's balance changed: {changed_slots:?}"
            );
            assert_eq!(changed_slots[0].address, token);
            assert_eq!(changed_slots[0].slot, balance_slot_for(owner));
            assert_eq!(changed_slots[0].old, U256::from(1000));
            assert_eq!(changed_slots[0].new, U256::from(50));

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

            // T9: the corrected re-run also skips transfer tracking → empty deltas.
            assert!(
                results[0].token_deltas.is_empty(),
                "corrected result token_deltas are empty (no transfer tracking)"
            );
        }
        other => panic!("expected Corrected, got {other:?}"),
    }

    // The discriminating assertion: exactly ONE sim (req1) was re-run. If the
    // `intersects` filter were removed, the validator would re-run BOTH req1 and
    // req2, and this would be 2 — so this test fails on that regression. The
    // value-equality checks above alone cannot tell a skip from an identical
    // re-run; the counter can.
    assert_eq!(
        controller.rerun_count(),
        1,
        "only the affected sim (req1) should be re-run, not req2"
    );
    Ok(())
}

// T1: a VIEW call corrected from one success to a *different* success. The
// observable return data (not logs) carries the change.
#[tokio::test(flavor = "multi_thread")]
async fn run_view_call_corrected_success_to_different_success() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);

    // Cache holds balanceOf(owner) == 1000.
    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    // Fetcher reports the balance slot changed to 250 (still a success on re-run,
    // but a different return value).
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(250),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    // A pure view call: balanceOf(owner). Its return value depends on the slot.
    let req = SimRequest::new(owner, token, balance_of_calldata(owner));
    let sim = controller.run(&mut cache, vec![req])?;

    // Optimistic view call succeeds and returns the OLD balance (1000).
    let opt = sim.optimistic().to_vec();
    assert_eq!(opt.len(), 1);
    assert!(!opt[0].output.is_empty(), "view call returns data");
    assert_eq!(
        decode_balance(&opt[0].output),
        U256::from(1000),
        "optimistic returns the old balance"
    );

    let validation = sim.validate().await?;
    match validation {
        Validation::Corrected {
            results,
            changed_slots,
            ..
        } => {
            assert_eq!(changed_slots.len(), 1, "exactly the balance slot changed");
            assert_eq!(changed_slots[0].address, token);
            assert_eq!(changed_slots[0].slot, balance_slot_for(owner));
            assert_eq!(changed_slots[0].old, U256::from(1000));
            assert_eq!(changed_slots[0].new, U256::from(250));

            // The corrected re-run STILL succeeds (a balanceOf view never reverts)
            // but its return data reflects the NEW balance.
            assert_eq!(
                decode_balance(&results[0].output),
                U256::from(250),
                "corrected re-run returns the new balance"
            );
            // Both runs succeed (non-empty return data) yet the outputs differ —
            // this is the success→different-success contract, not keyed off logs.
            assert!(
                !results[0].output.is_empty(),
                "corrected run still succeeds"
            );
            assert_ne!(
                results[0].output, opt[0].output,
                "corrected output differs from optimistic output"
            );
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
    // Overlay-resident seed so the balance is EVM-visible on the StorageCleared
    // token account (see the note in `verify_slots_detects_and_injects_changes`):
    // after the §16.0 fix, a backend-only `inject_storage_batch` seed here reads as
    // ZERO via `cached_storage_value` (mirroring the SLOAD), so the live-cache
    // assertions below would observe 0 instead of the seeded value. This mirrors
    // `state_update::balance_tracking_scenario`.
    cache
        .db_mut()
        .insert_account_storage(token, balance_slot_for(owner), U256::from(1000))?;
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
    let validation = sim.validate().await?;
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
    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::ConfirmedStorage),
        "{validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_converges_when_corrected_slot_is_overlay_resident() -> Result<()> {
    // F1 regression: a correction must reach the layer that *wins* in the
    // snapshot. When the verified slot lives in the CacheDB overlay (layer 1) —
    // e.g. seeded via insert_account_storage or written by a committed call —
    // draining the correction into BlockchainDb (layer 2) alone leaves the stale
    // overlay value shadowing it, so the cache never converges and re-corrects
    // forever. This test seeds the balance into the overlay and asserts the
    // second run both heals the live cache and yields Confirmed.
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    let slot = balance_slot_for(owner);
    // Seed the balance into the OVERLAY (layer 1), not layer 2.
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(1000))?;
    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::from(1000)),
        "precondition: overlay holds the seeded value"
    );

    // Live value is 2000 (changed).
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, slot),
        U256::from(2000),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);

    // First run: detects the change, queues a correction.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert!(matches!(
        sim.validate().await?,
        Validation::Corrected { .. }
    ));
    assert_eq!(controller.pending_len(), 1);

    // Second run: drains the correction. It must overwrite the overlay-resident
    // slot, not just layer 2, so the live cache now reads the fresh value.
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
        cache.cached_storage_value(token, slot),
        Some(U256::from(2000)),
        "correction must overwrite the overlay-resident slot, not just layer 2"
    );

    // The snapshot now matches the fetcher → Confirmed, proving convergence.
    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::ConfirmedStorage),
        "must converge, got {validation:?}"
    );
    // No background re-run happened on the converged second cycle.
    assert_eq!(controller.rerun_count(), 1, "only the first cycle re-ran");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_slots_heals_overlay_resident_slot() -> Result<()> {
    // F1 regression on the synchronous primitive: verify_slots must heal a slot
    // that lives in the CacheDB overlay, so both cached_storage_value and the
    // EVM SLOAD path (here, a balanceOf call against a StorageCleared account)
    // reflect the fresh value, and a re-verify is idempotent.
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO); // coinbase, for call_raw
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    let slot = balance_slot_for(owner);
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(100))?;

    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, slot),
        U256::from(999),
    )])));

    let changed = cache.verify_slots(&[(token, slot)])?;
    assert_eq!(changed.len(), 1, "stale overlay slot detected as changed");
    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::from(999)),
        "verify_slots heals the overlay-resident slot"
    );

    // The synchronous EVM SLOAD path sees the fresh value too: the
    // StorageCleared overlay account must read the written slot (a value the
    // delete-the-slot alternative would have turned into a zero read).
    let balance = common::balance_of(&mut cache, token, owner)?;
    assert_eq!(
        balance,
        U256::from(999),
        "EVM SLOAD reflects the healed overlay slot"
    );

    // Converged: a re-verify reports nothing (no perpetual re-change).
    assert!(
        cache.verify_slots(&[(token, slot)])?.is_empty(),
        "overlay slot healed; re-verify is idempotent"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn optimistic_result_reports_status_per_outcome() -> Result<()> {
    // F6 regression: CallSimulationResult must distinguish Success from Revert
    // via an explicit status. The old example inferred success from
    // `!logs.is_empty()`, which misclassifies a zero-log success (a view call)
    // as a revert.
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    let slot = balance_slot_for(owner);
    // Overlay-resident (EVM-visible) seed: `token` is a StorageCleared MockERC20,
    // so a backend-only seed reads ZERO via the account_state-aware read path.
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(1000))?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, slot),
        U256::from(1000),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);

    // A balanceOf view call SUCCEEDS but emits NO logs — status must be Success,
    // not the revert the old logs heuristic would have inferred.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(owner, token, balance_of_calldata(owner))],
    )?;
    assert_eq!(sim.optimistic()[0].status, SimStatus::Success);
    assert!(
        sim.optimistic()[0].logs.is_empty(),
        "the view call emits no logs"
    );
    assert_eq!(
        decode_balance(&sim.optimistic()[0].output),
        U256::from(1000)
    );
    sim.into_optimistic();

    // Transferring more than the balance REVERTS — status must be Revert.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(5000)),
        )],
    )?;
    assert_eq!(sim.optimistic()[0].status, SimStatus::Revert);
    sim.into_optimistic();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_after_fetch_started_suppresses_correction() -> Result<()> {
    // F4: cancellation is best-effort, but once observed at a checkpoint it must
    // prevent side effects. The fetcher is held inside a barrier so the validator
    // is provably past `yield_now` and blocked mid-fetch; we drop the sim while it
    // is blocked, then release it. The post-fetch checkpoint must see the cancel
    // and NOT queue a correction, even though the balance slot changed.
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    let slot = balance_slot_for(owner);
    // Overlay-resident (EVM-visible) seed: `token` is a StorageCleared MockERC20,
    // so a backend-only seed reads ZERO via the account_state-aware read path.
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(1000))?;

    // Two rendezvous: R1 = "fetch started", R2 = "released by the test". After R2
    // the fetcher reports a CHANGED balance, so absent the cancel the validator
    // would queue a correction.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let fb = Arc::clone(&barrier);
    let fetcher: StorageBatchFetchFn =
        Arc::new(move |reqs: Vec<(Address, U256)>, _block: BlockId| {
            fb.wait(); // R1
            fb.wait(); // R2
            reqs.into_iter()
                .map(|(a, s)| (a, s, Ok(U256::from(2000))))
                .collect()
        });
    cache.set_storage_batch_fetcher(fetcher);

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;

    barrier.wait(); // R1: the validator is now blocked inside the fetcher.
    drop(sim); // Sets the cancel flag (abort cannot preempt the sync validator).
    barrier.wait(); // R2: release the fetcher; the validator resumes past the fetch.

    settle().await;
    assert_eq!(
        controller.pending_len(),
        0,
        "a cancel observed after the fetch must suppress the queued correction"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_corrected_rerun_verifies_newly_read_volatile_slot() -> Result<()> {
    // F2 regression: a correction can flip control flow so the re-run reads a
    // NEW volatile slot the optimistic run never touched. That slot must itself
    // be fetched and diffed (fixed-point), or the "corrected" result would still
    // rest on stale snapshot state.
    use revm::state::{AccountInfo, Bytecode};

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let caller = Address::repeat_byte(0x66);
    install_default_account(&mut cache, caller);

    // Branchy runtime: load slot 0 (A); if A != 0 return A (reads only slot 0);
    // else read slot 1 (B) and return it. A correction A: nonzero -> 0 flips the
    // branch onto slot B, which the optimistic run never read.
    let contract = Address::repeat_byte(0x55);
    let code = Bytecode::new_raw(Bytes::from(
        alloy_primitives::hex::decode("600054806013575060015460005260206000f35b60005260206000f3")
            .expect("valid runtime hex"),
    ));
    let code_hash = code.hash_slow();
    cache.db_mut().insert_account_info(
        contract,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 0,
            code: Some(code),
            code_hash,
            account_id: None,
        },
    );
    cache
        .db_mut()
        .replace_account_storage(contract, Default::default())
        .unwrap();

    let slot_a = U256::from(0);
    let slot_b = U256::from(1);
    // Snapshot: A = 5 (nonzero) → optimistic takes "return A" and never reads B.
    // Overlay-resident (EVM-visible) seed: `contract` is StorageCleared.
    cache
        .db_mut()
        .insert_account_storage(contract, slot_a, U256::from(5))?;
    // Fresh chain: A dropped to 0 (flips the branch) and B is 777.
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([
        ((contract, slot_a), U256::from(0)),
        ((contract, slot_b), U256::from(777)),
    ])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(caller, contract, Bytes::new())],
    )?;

    // Optimistic: A != 0 branch returns 5.
    assert_eq!(
        U256::from_be_slice(&sim.optimistic()[0].output),
        U256::from(5)
    );

    match sim.validate().await? {
        Validation::Corrected {
            results,
            changed_slots,
            ..
        } => {
            let keys: std::collections::HashSet<(Address, U256)> =
                changed_slots.iter().map(|c| (c.address, c.slot)).collect();
            assert!(keys.contains(&(contract, slot_a)), "A reported as changed");
            assert!(
                keys.contains(&(contract, slot_b)),
                "B (read only on the corrected branch) must be verified and reported"
            );
            assert_eq!(
                U256::from_be_slice(&results[0].output),
                U256::from(777),
                "corrected result must use the FRESH value of the newly-read slot, not stale 0"
            );
        }
        other => panic!("expected Corrected, got {other:?}"),
    }
    // The one affected sim, re-run across multiple rounds, is counted once.
    assert_eq!(controller.rerun_count(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_call_with_tx_config_threads_value() -> Result<()> {
    // F3 regression: the overlay must honor TxConfig.value, not hardcode zero.
    use evm_fork_cache::cache::TxConfig;
    use revm::context::result::ExecutionResult;
    use revm::state::{AccountInfo, Bytecode};

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let caller = Address::repeat_byte(0x66);
    install_default_account(&mut cache, caller);

    // Runtime bytecode that returns msg.value:
    // CALLVALUE; PUSH1 0; MSTORE; PUSH1 32; PUSH1 0; RETURN.
    let callee = Address::repeat_byte(0x55);
    let code = Bytecode::new_raw(Bytes::from(
        alloy_primitives::hex::decode("3460005260206000f3").expect("valid runtime hex"),
    ));
    let code_hash = code.hash_slow();
    cache.db_mut().insert_account_info(
        callee,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 0,
            code: Some(code),
            code_hash,
            account_id: None,
        },
    );
    cache
        .db_mut()
        .replace_account_storage(callee, Default::default())
        .unwrap();

    let snapshot = cache.snapshot();
    let mut overlay = EvmOverlay::new(snapshot, None);

    fn returned_value(res: ExecutionResult) -> U256 {
        match res {
            ExecutionResult::Success { output, .. } => U256::from_be_slice(&output.into_data()),
            other => panic!("expected success, got {other:?}"),
        }
    }

    // The zero-value shorthand observes value 0.
    let (res, _) = overlay.call_raw_with_access_list(caller, callee, Bytes::new())?;
    assert_eq!(returned_value(res), U256::ZERO);

    // The TxConfig variant threads the native value through to CALLVALUE.
    let tx = TxConfig {
        value: U256::from(12_345u64),
        ..Default::default()
    };
    let (res, _) = overlay.call_raw_with_access_list_with(caller, callee, Bytes::new(), &tx)?;
    assert_eq!(returned_value(res), U256::from(12_345u64));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_honors_tx_gas_limit() -> Result<()> {
    // F3 regression: SimRequest.tx.gas_limit must reach the optimistic call. A
    // limit well below the ~51k an ERC20 transfer needs (but above intrinsic gas)
    // halts out-of-gas; ignoring it would run at the default limit → Success.
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    let slot = balance_slot_for(owner);
    // Overlay-resident (EVM-visible) seed: `token` is a StorageCleared MockERC20,
    // so a backend-only seed reads ZERO via the account_state-aware read path.
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(1000))?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, slot),
        U256::from(1000),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);

    let req = SimRequest::new(owner, token, transfer_calldata(recipient, U256::from(100)))
        .with_gas_limit(30_000);
    let sim = controller.run(&mut cache, vec![req])?;
    assert!(
        matches!(sim.optimistic()[0].status, SimStatus::Halt { .. }),
        "gas-bounded transfer must halt, got {:?}",
        sim.optimistic()[0].status
    );
    sim.into_optimistic();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validator_fetches_at_captured_latest_pin_despite_repin() -> Result<()> {
    use alloy_eips::BlockNumberOrTag;

    let token = Address::repeat_byte(0x91);
    let owner = Address::repeat_byte(0x92);
    let recipient = Address::repeat_byte(0x93);
    let slot = balance_slot_for(owner);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(1000))?;
    assert_eq!(
        cache.block(),
        BlockId::latest(),
        "default construction must expose an explicit latest pin"
    );

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let fb = Arc::clone(&barrier);
    let seen_block: Arc<Mutex<Option<BlockId>>> = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&seen_block);
    let fetcher: StorageBatchFetchFn =
        Arc::new(move |reqs: Vec<(Address, U256)>, block: BlockId| {
            *seen.lock().unwrap() = Some(block);
            fb.wait();
            fb.wait();
            let at_snapshot_pin = block == BlockId::latest();
            reqs.into_iter()
                .map(|(a, s)| {
                    let v = if s == slot {
                        if at_snapshot_pin {
                            U256::from(1000)
                        } else {
                            U256::from(2000)
                        }
                    } else {
                        U256::ZERO
                    };
                    (a, s, Ok(v))
                })
                .collect()
        });
    cache.set_storage_batch_fetcher(fetcher);

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;

    barrier.wait();
    cache.set_block(BlockId::Number(BlockNumberOrTag::Number(101)));
    barrier.wait();

    let verdict = sim.validate().await?;
    assert!(
        matches!(verdict, Validation::ConfirmedStorage),
        "validator must fetch at the captured latest pin, not the later numeric repin; got {verdict:?}"
    );
    assert_eq!(
        *seen_block.lock().unwrap(),
        Some(BlockId::latest()),
        "the fetch must receive the concrete snapshot pin"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validator_fetches_at_snapshot_block_despite_repin() -> Result<()> {
    // F5 regression: the deferred validator must fetch at the block its snapshot
    // was built from, even if the cache is re-pinned while validation is pending.
    // Otherwise it would compare snapshot(N) values against fresh(N+1) values and
    // emit a spurious Corrected.
    use alloy_eips::BlockNumberOrTag;

    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);
    let slot = balance_slot_for(owner);
    let n = 100u64;
    let block_n = BlockId::Number(BlockNumberOrTag::Number(n));

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache.set_block(block_n);
    // Overlay-resident seed so the balance is EVM-visible on the StorageCleared
    // token account (see the note in `verify_slots_detects_and_injects_changes`):
    // a backend-only seed would read as ZERO under the §16.0 `cached_storage_value`
    // fix, failing the precondition below.
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(1000))?;
    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::from(1000)),
        "PRECONDITION: seeded balance present after set_block + insert"
    );

    // Block-aware fetcher: the snapshot value (1000) at block N, a CHANGED value
    // (2000) at any other block. Records the block it was asked for, and blocks on
    // a barrier so the test can repin before the fetch resolves.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let fb = Arc::clone(&barrier);
    let seen_block: Arc<Mutex<Option<BlockId>>> = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&seen_block);
    let fetcher: StorageBatchFetchFn =
        Arc::new(move |reqs: Vec<(Address, U256)>, block: BlockId| {
            *seen.lock().unwrap() = Some(block);
            fb.wait(); // R1: fetch entered
            fb.wait(); // R2: released after the test repins
            let at_n = block == block_n;
            reqs.into_iter()
                .map(|(a, s)| {
                    // At block N every slot matches the snapshot (sender = 1000,
                    // everything else = 0) → Confirmed. At any other block the
                    // sender balance reads as changed (2000) → would be Corrected.
                    let v = if s == slot {
                        if at_n {
                            U256::from(1000)
                        } else {
                            U256::from(2000)
                        }
                    } else {
                        U256::ZERO
                    };
                    (a, s, Ok(v))
                })
                .collect()
        });
    cache.set_storage_batch_fetcher(fetcher);

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;

    barrier.wait(); // R1: the validator is inside the fetcher.
    // Re-pin the cache to N+1 while validation is still outstanding.
    cache.set_block(BlockId::Number(BlockNumberOrTag::Number(n + 1)));
    barrier.wait(); // R2: release the fetcher.

    let verdict = sim.validate().await?;
    assert!(
        matches!(verdict, Validation::ConfirmedStorage),
        "validator must fetch at the snapshot's block N, not the re-pinned N+1; got {verdict:?}"
    );
    assert_eq!(
        *seen_block.lock().unwrap(),
        Some(block_n),
        "the fetch must be pinned to the snapshot block N"
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
    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::Unverified { .. }),
        "fetcher error should yield Unverified: {validation:?}"
    );
    Ok(())
}

// T3 (part 2): into_optimistic aborts the validation task. The fetcher WOULD
// queue a correction (it reports a changed value), so if the abort failed we
// would observe a non-zero pending queue. We assert it stays 0.
//
// Determinism mirrors the Drop-abort test below: the validator is allowed to
// reach the synchronous fetch, but the gated fetch cannot return until after
// `into_optimistic()` has set the cancel flag. That makes the product guarantee
// precise: a cancel observed at the post-fetch checkpoint suppresses all
// side-effects, including pending corrections and re-run accounting.
#[tokio::test(flavor = "multi_thread")]
async fn run_into_optimistic_aborts_validation() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    let gate = Gate::new();
    // A CHANGED value: if the validator ran, it would queue a correction.
    cache.set_storage_batch_fetcher(gated_tracking_fetcher(
        HashMap::from([((token, balance_slot_for(owner)), U256::from(50))]),
        gate.clone(),
    ));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let results = sim.into_optimistic(); // aborts the background validation
    gate.release();
    assert_eq!(results.len(), 1);

    // Give any (incorrectly) surviving task a chance to run, then assert no
    // correction was queued and no re-run happened.
    settle().await;
    assert_eq!(
        controller.pending_len(),
        0,
        "into_optimistic must abort validation before it queues a correction"
    );
    assert_eq!(controller.rerun_count(), 0, "no re-run after abort");
    Ok(())
}

// T3 (part 1): dropping the SpeculativeSim (no validate/into_optimistic) aborts
// the validation task before it can push a correction. The fetcher reports a
// CHANGED value, so an *uncancelled* validator would queue a correction and bump
// the re-run count; we assert neither happens after the drop.
//
// Determinism: the validator's only correction-queuing path runs *after* its
// fetch returns (the post-fetch cancel checkpoint in `run_validator` gates it).
// We make that ordering race-free with a gate the test controls — the fetcher
// blocks until `gate.release()`, and we release only *after* `drop(sim)` has set
// the cancel flag. So however the multi-thread scheduler interleaves the spawned
// task and this thread, the fetch (and thus the post-fetch checkpoint) can only
// complete once cancellation is already observable, and the correction is
// suppressed. We deliberately do NOT assert the fetcher was never reached: the
// product only guarantees a cancel seen at a checkpoint suppresses side effects,
// not that an in-flight fetch is skipped — asserting the latter was the original
// over-strict, racy condition.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_speculative_sim_aborts_before_queueing_correction() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    let gate = Gate::new();
    cache.set_storage_batch_fetcher(gated_tracking_fetcher(
        HashMap::from([((token, balance_slot_for(owner)), U256::from(50))]),
        gate.clone(),
    ));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    // Drop with NO intervening await, then release the gate. Releasing only after
    // the drop guarantees the validator's fetch (if it even reaches it) returns
    // strictly after the cancel flag is set, so its post-fetch checkpoint bails
    // out before queuing anything.
    drop(sim);
    gate.release();

    settle().await;

    assert_eq!(
        controller.pending_len(),
        0,
        "dropping the sim must abort validation before it queues a correction"
    );
    assert_eq!(controller.rerun_count(), 0, "no re-run after abort");
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
    let validation = sim.validate().await?;
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
    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::ConfirmedStorage),
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
    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::ConfirmedStorage),
        "{validation:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_through_becomes_volatile_after_boundary() -> Result<()> {
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
    assert!(matches!(
        sim.validate().await?,
        Validation::ConfirmedStorage
    ));

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
        matches!(sim.validate().await?, Validation::Corrected { .. }),
        "past ValidThrough boundary the slot is volatile and the change is caught"
    );
    Ok(())
}

// T4: a sim reads a slot absent from both the snapshot and the cache; the
// fetcher returns a NONZERO value. The validator must treat the missing slot as
// zero and report a SlotChange { old: ZERO, new: nonzero } through the
// controller.
#[tokio::test(flavor = "multi_thread")]
async fn run_missing_slot_treated_as_zero_is_corrected() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);

    // Owner's balance slot is NEVER injected → snapshot/cache have no entry, so
    // the optimistic balanceOf reads it as zero.
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    let slot = balance_slot_for(owner);
    // Fetcher reports a NONZERO current value for the unseen slot.
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, slot),
        U256::from(777),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let req = SimRequest::new(owner, token, balance_of_calldata(owner));
    let sim = controller.run(&mut cache, vec![req])?;

    // Optimistic reads the unseen slot as zero.
    let opt = sim.optimistic().to_vec();
    assert_eq!(
        decode_balance(&opt[0].output),
        U256::ZERO,
        "unseen slot reads as zero optimistically"
    );

    match sim.validate().await? {
        Validation::Corrected {
            results,
            changed_slots,
            ..
        } => {
            let change = changed_slots
                .iter()
                .find(|c| c.address == token && c.slot == slot)
                .expect("the missing balance slot should be reported as changed");
            assert_eq!(change.old, U256::ZERO, "missing slot treated as old = zero");
            assert_eq!(change.new, U256::from(777), "fetcher's nonzero value");
            // The corrected re-run now sees the fresh balance.
            assert_eq!(
                decode_balance(&results[0].output),
                U256::from(777),
                "corrected re-run returns the fresh balance"
            );
        }
        other => panic!("expected Corrected, got {other:?}"),
    }
    Ok(())
}

// T5: a queued correction, once drained on the SECOND run, changes the second
// run's *result* (not merely the cached value / verdict). First run queues a
// drop to balance 50; the second run's transfer of 100 then reverts after the
// drain.
#[tokio::test(flavor = "multi_thread")]
async fn pending_drain_alters_subsequent_result() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    // Cache holds balance 1000.
    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    // Fetcher reports the balance DROPPED to 50 (a change → queued correction).
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(50),
    )])));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);

    // First run: optimistic transfer of 100 SUCCEEDS against the cached 1000.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert!(
        !sim.optimistic()[0].logs.is_empty(),
        "first-run optimistic transfer succeeds against cached 1000"
    );
    assert!(matches!(
        sim.validate().await?,
        Validation::Corrected { .. }
    ));
    assert_eq!(controller.pending_len(), 1, "a correction (→50) is queued");

    // Second run drains the correction (balance := 50) BEFORE snapshotting, so
    // the optimistic transfer of 100 now REVERTS against the drained 50.
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert_eq!(controller.pending_len(), 0, "pending drained");
    assert!(
        sim.optimistic()[0].logs.is_empty(),
        "second-run optimistic transfer REVERTS — the drained value (50 < 100) \
         changed the *result*, not just the cached value"
    );
    Ok(())
}

// T6a: a panicking fetcher → the validator task panics → JoinError → validate Err.
#[tokio::test(flavor = "multi_thread")]
async fn validate_returns_err_on_fetcher_panic() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(panicking_fetcher());

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    let err = sim
        .validate()
        .await
        .expect_err("a panicking fetcher should make validate return Err");
    assert!(
        err.to_string().contains("validation task failed"),
        "a panicking fetcher should surface the JoinError: {err}"
    );
    Ok(())
}

// T6b: a cache with NO storage batch fetcher → Unverified with the specific
// "no storage batch fetcher available" reason.
#[tokio::test(flavor = "multi_thread")]
async fn run_unverified_without_fetcher() -> Result<()> {
    use revm::primitives::hardfork::SpecId;

    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    // A `from_backend` cache exposes no fetcher (no provider captured).
    let base = cache_with_balance(token, owner, U256::from(1000)).await?;
    let mut cache = EvmCache::from_backend(
        base.unchecked_backend().clone(),
        base.unchecked_blockchain_db().clone(),
        base.block(),
        base.chain_id(),
        None,
        None,
        SpecId::CANCUN,
    );
    // Seed the same state the simulation needs into the no-fetcher cache.
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache.inject_storage_batch(&[(token, balance_slot_for(owner), U256::from(1000))]);
    assert!(
        cache.storage_batch_fetcher().is_none(),
        "from_backend cache has no fetcher"
    );

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    match sim.validate().await? {
        Validation::Unverified { reason } => {
            assert_eq!(reason, "no storage batch fetcher available", "{reason}");
        }
        other => panic!("expected Unverified, got {other:?}"),
    }
    Ok(())
}

// T7 (controller-level): drive ObservationDriven end-to-end. Seed the tracker so
// the owner's balance slot is a well-observed, never-changed slot; with the
// adaptive policy it is NOT selected for verification this cycle, yet the
// validator's actual-read-set reconcile still catches the real change. This
// exercises the controller → policy → should_refetch path.
#[tokio::test(flavor = "multi_thread")]
async fn observation_driven_controller_end_to_end() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(50), // a real change
    )])));

    // Seed a shared tracker so the balance slot is "stable, well-observed":
    // enough never-changed observations that should_refetch() returns false.
    let params = FreshnessParams::default();
    let slot = balance_slot_for(owner);
    let tracker = {
        let mut t = SlotObservationTracker::new();
        for now in 0..params.min_observations {
            t.observe(token, slot, U256::from(1000), now as u64);
        }
        // At a now within the reuse window, a stable slot is not refetched.
        assert!(!t.should_refetch(token, slot, params.min_observations as u64, &params));
        Arc::new(Mutex::new(t))
    };

    // Use a predicted access list so the policy actually receives the slot as a
    // candidate (the predicted set drives policy.select).
    use alloy_eips::eip2930::{AccessList, AccessListItem};
    let predicted = AccessList(vec![AccessListItem {
        address: token,
        storage_keys: vec![alloy_primitives::B256::from(slot)],
    }]);

    let clock = BlockClock::at(params.min_observations as u64);
    let mut controller = FreshnessController::with_clock(
        FreshnessRegistry::new(),
        ObservationDriven::new(params),
        clock,
    )
    .with_tracker(Arc::clone(&tracker));

    let req = SimRequest::new(owner, token, transfer_calldata(recipient, U256::from(100)))
        .with_access_list(predicted);
    let sim = controller.run(&mut cache, vec![req])?;

    // Even though the policy declined to *predictively* verify the stable slot,
    // the validator's actual-read-set reconcile catches the real change.
    match sim.validate().await? {
        Validation::Corrected { changed_slots, .. } => {
            assert!(
                changed_slots
                    .iter()
                    .any(|c| c.address == token && c.slot == slot),
                "the actual-read-set reconcile catches the balance change"
            );
        }
        other => panic!("expected Corrected, got {other:?}"),
    }
    Ok(())
}

// T8: on_new_block advances the BlockClock so a ValidThrough(100) slot becomes
// volatile, driven entirely through the controller's natural API (no separate
// clock bump).
#[tokio::test(flavor = "multi_thread")]
async fn on_new_block_ages_valid_through() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, balance_slot_for(owner)),
        U256::from(2000), // a change, if the slot is verified
    )])));

    let mut registry = FreshnessRegistry::new();
    registry.valid_through_slot(token, balance_slot_for(owner), 100);

    // Start at block 100 (still valid). Advance via on_new_block(101) — NOT a
    // direct set_block — so the natural API ages the slot into volatile.
    let mut controller =
        FreshnessController::with_clock(registry, AlwaysVerify, BlockClock::at(100));

    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert!(
        matches!(sim.validate().await?, Validation::ConfirmedStorage),
        "at block 100 the ValidThrough slot is still pinned"
    );

    // Advance the clock through the controller API.
    controller.on_new_block(101);

    let sim = controller.run(
        &mut cache,
        vec![SimRequest::new(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100)),
        )],
    )?;
    assert!(
        matches!(sim.validate().await?, Validation::Corrected { .. }),
        "after on_new_block(101) the slot is volatile and the change is caught"
    );
    Ok(())
}

// ===========================================================================
// Phase 2 review (trust-contract hardening): the validator must NEVER return a
// trusted verdict on incomplete/ambiguous verification.
// ===========================================================================

/// P2: a custom fetcher that OMITS a requested slot must yield `Unverified`, not
/// a false `Confirmed`/`Corrected` (missing results must not default to zero).
#[tokio::test(flavor = "multi_thread")]
async fn run_unverified_when_fetcher_omits_requested_slot() -> Result<()> {
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    let mut cache = cache_with_balance(token, owner, U256::from(1000)).await?;
    // A fetcher that returns NOTHING — it omits every requested slot.
    cache.set_storage_batch_fetcher(Arc::new(|_req: Vec<(Address, U256)>, _block: BlockId| {
        Vec::<(Address, U256, StorageFetchResult<U256>)>::new()
    }));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let req = SimRequest::new(owner, token, transfer_calldata(recipient, U256::from(100)));
    let sim = controller.run(&mut cache, vec![req])?;

    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::Unverified { .. }),
        "a fetcher that omits a requested slot must yield Unverified, not a false \
         confirmation/correction: {validation:?}"
    );
    assert_eq!(
        controller.pending_len(),
        0,
        "Unverified must not queue any correction"
    );
    Ok(())
}

/// Build runtime bytecode that reads slots `0..n` in order, returning the first
/// nonzero one (else zero). Reading slot `i+1` is gated on slot `i` being zero, so
/// each correction (slot → 0) opens exactly one new volatile slot — driving the
/// validator's fixed-point loop one round deeper per correction.
fn chained_sload_bytecode(n: u8) -> Bytes {
    let ret_dest = 8u16 * (n as u16) + 2; // JUMPDEST offset (after the chain + PUSH1 0)
    assert!(ret_dest <= 255, "return dest must fit in PUSH1");
    let ret = ret_dest as u8;
    let mut code = Vec::new();
    for i in 0..n {
        code.extend_from_slice(&[0x60, i, 0x54, 0x80, 0x60, ret, 0x57, 0x50]);
        // PUSH1 i; SLOAD; DUP1; PUSH1 ret; JUMPI (if nonzero -> return it); POP
    }
    code.extend_from_slice(&[0x60, 0x00]); // all zero: PUSH1 0
    code.extend_from_slice(&[0x5b, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);
    // JUMPDEST; PUSH1 0; MSTORE; PUSH1 0x20; PUSH1 0; RETURN (store TOS, return 32 bytes)
    Bytes::from(code)
}

/// P1: when corrections keep opening new volatile slots past
/// `MAX_VALIDATION_ROUNDS`, the validator must return `Unverified` — NOT a
/// best-effort (trusted) `Corrected` resting on un-verified state — and must
/// queue no corrections.
#[tokio::test(flavor = "multi_thread")]
async fn run_unverified_when_fixed_point_round_cap_exceeded() -> Result<()> {
    use revm::state::{AccountInfo, Bytecode};

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let caller = Address::repeat_byte(0x66);
    install_default_account(&mut cache, caller);

    // A 12-deep chain: each corrected slot opens the next, so the loop needs one
    // round per slot — exceeding the 8-round cap well before the chain runs out.
    let contract = Address::repeat_byte(0x55);
    let code = Bytecode::new_raw(chained_sload_bytecode(12));
    let code_hash = code.hash_slow();
    cache.db_mut().insert_account_info(
        contract,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 0,
            code: Some(code),
            code_hash,
            account_id: None,
        },
    );
    cache
        .db_mut()
        .replace_account_storage(contract, Default::default())
        .unwrap();
    // Snapshot: slots 0..12 all nonzero (EVM-visible overlay seed). The optimistic
    // run reads only slot 0 (nonzero → returns).
    for i in 0..12u64 {
        cache
            .db_mut()
            .insert_account_storage(contract, U256::from(i), U256::from(1))?;
    }
    // Fresh chain: every slot dropped to 0 (stub returns 0 for all), so each
    // correction flips the next branch and the loop never reaches a fixed point.
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::new()));

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);
    let req = SimRequest::new(caller, contract, Bytes::new());
    let sim = controller.run(&mut cache, vec![req])?;

    let validation = sim.validate().await?;
    assert!(
        matches!(validation, Validation::Unverified { .. }),
        "exceeding the fixed-point round cap must yield Unverified, not a trusted \
         Corrected: {validation:?}"
    );
    assert_eq!(
        controller.pending_len(),
        0,
        "an Unverified (cap-exceeded) validation must queue no corrections"
    );
    Ok(())
}

/// WS-1c (manager-authored red-green): the verdict taxonomy distinguishes a
/// storage-only confirmation from a full (storage + account) one, so callers can
/// no longer mistake "no volatile storage slot changed" for "account state
/// verified". The storage-only success verdict is renamed `Confirmed ->
/// ConfirmedStorage`; a new `ConfirmedFull` means storage AND account fields were
/// verified; and `Corrected` carries `changed_accounts` alongside `changed_slots`.
#[test]
fn verdict_taxonomy_separates_storage_only_from_full_confirmation() {
    // Renamed storage-only success verdict (was `Confirmed`).
    assert!(matches!(
        Validation::ConfirmedStorage,
        Validation::ConfirmedStorage
    ));
    // New: storage AND account fields verified.
    assert!(matches!(
        Validation::ConfirmedFull,
        Validation::ConfirmedFull
    ));
    // Corrected now reports account changes alongside slot changes.
    let corrected = Validation::Corrected {
        results: vec![],
        changed_slots: vec![],
        changed_accounts: vec![],
    };
    assert!(matches!(corrected, Validation::Corrected { .. }));
}
