//! Offline integration tests for the snapshot/overlay isolation guarantees that
//! underpin the crate's parallel fan-out model.
//!
//! These pin the invariants a search loop relies on:
//! - [`EvmCache::snapshot`] yields an immutable, point-in-time view that
//!   later cache mutations cannot perturb.
//! - Overlays derived from one snapshot are isolated from each other and from the
//!   live cache.
//!
//! All state is injected over a mocked provider, so no test touches the network.

mod common;

use std::{sync::Arc, time::Duration};

use alloy_eips::BlockId;
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::{Result, anyhow};
use revm::context::result::ExecutionResult;
use revm::database_interface::Database;

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20,
    mock_erc20_runtime, setup_cache, transfer,
};
use evm_fork_cache::{
    SimulationCancellationToken,
    cache::{EvmOverlay, EvmSnapshot, TxConfig},
    errors::OverlayError,
};

/// The hashed storage slot of `balanceOf[owner]` for a `MockERC20` (balances at
/// the declared mapping slot 3): `keccak256(abi.encode(owner, 3))`.
fn balance_slot_for(owner: Address) -> U256 {
    let key = keccak256((owner, U256::from(MOCK_ERC20_BALANCE_SLOT)).abi_encode());
    U256::from_be_bytes(key.0)
}

/// Read `balanceOf(owner)` from a `MockERC20` through an overlay (non-committing).
fn overlay_balance_of(overlay: &mut EvmOverlay, token: Address, owner: Address) -> Result<U256> {
    let call = MockERC20::balanceOfCall { account: owner };
    let result = overlay.call_raw(owner, token, call.abi_encode().into())?;
    match result {
        ExecutionResult::Success { output, .. } => Ok(
            MockERC20::balanceOfCall::abi_decode_returns(&output.into_data())?,
        ),
        other => Err(anyhow!("overlay balanceOf failed: {other:?}")),
    }
}

/// A snapshot captures state at a point in time; committing a transfer on the
/// live cache afterward must not change what an overlay built from that snapshot
/// observes.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_is_immutable_after_later_cache_mutation() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);

    let balance_slot = U256::from(MOCK_ERC20_BALANCE_SLOT);
    let initial = U256::from(1_000u64);
    cache.insert_mapping_storage_slot(token, balance_slot, owner, initial)?;
    cache.insert_mapping_storage_slot(token, balance_slot, recipient, U256::ZERO)?;

    // Freeze the state, then mutate the live cache with a committed transfer.
    let snapshot = cache.snapshot();
    transfer(&mut cache, token, owner, recipient, U256::from(250u64))?;

    // The live cache reflects the transfer...
    assert_eq!(
        common::balance_of(&mut cache, token, owner)?,
        initial - U256::from(250u64),
        "live cache should reflect the committed transfer"
    );

    // ...but the snapshot (and any overlay built from it) is frozen at `initial`.
    assert_eq!(
        snapshot.storage_value(token, balance_slot_for(owner)),
        Some(initial),
        "snapshot storage_value is unaffected by the later mutation"
    );
    let mut overlay = EvmOverlay::new(Arc::clone(&snapshot), None);
    assert_eq!(
        overlay_balance_of(&mut overlay, token, owner)?,
        initial,
        "overlay from the snapshot sees the pre-transfer balance"
    );

    Ok(())
}

/// Two overlays built from the same snapshot are isolated: a dirty-layer write in
/// one is invisible to the other and to the live cache.
#[tokio::test(flavor = "multi_thread")]
async fn overlays_from_one_snapshot_are_isolated() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x99);
    install_mock_erc20(&mut cache, contract);

    let slot = U256::from(7);
    let original = U256::from(1u64);
    // Overlay-resident seed so the value is EVM-visible on the StorageCleared
    // MockERC20: after the §16.0 fix, a backend-only `inject_storage_batch` seed on
    // a StorageCleared account reads as ZERO via `cached_storage_value` (mirroring
    // the EVM SLOAD), so the live-cache assertion below would observe 0. Seeding
    // the overlay (the winning layer) is what the test means by "the cache holds
    // `original`" and is captured by `snapshot`.
    cache
        .db_mut()
        .insert_account_storage(contract, slot, original)?;

    let snapshot = cache.snapshot();
    let mut overlay_a = EvmOverlay::new(Arc::clone(&snapshot), None);
    let mut overlay_b = EvmOverlay::new(Arc::clone(&snapshot), None);

    // Write through overlay A only.
    overlay_a.override_slot(contract, slot, U256::from(999u64));

    assert_eq!(
        overlay_a.storage(contract, slot)?,
        U256::from(999u64),
        "overlay A sees its own dirty-layer write"
    );
    assert_eq!(
        overlay_b.storage(contract, slot)?,
        original,
        "overlay B is isolated from overlay A's write"
    );
    assert_eq!(
        cache.cached_storage_value(contract, slot),
        Some(original),
        "the live cache is unaffected by an overlay write"
    );
    assert_eq!(
        snapshot.storage_value(contract, slot),
        Some(original),
        "the shared snapshot is unaffected by an overlay write"
    );

    Ok(())
}

/// A fresh overlay (no dirty-layer writes) reads exactly the snapshot's state.
#[tokio::test(flavor = "multi_thread")]
async fn overlay_reads_reflect_snapshot_state() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(42_000u64),
    )?;

    let snapshot: Arc<EvmSnapshot> = cache.snapshot();
    let mut overlay = EvmOverlay::new(snapshot, None);

    assert_eq!(
        overlay_balance_of(&mut overlay, token, owner)?,
        U256::from(42_000u64)
    );

    // A non-committing overlay call leaves the overlay's base state intact, so a
    // repeat read returns the same value.
    assert_eq!(
        overlay_balance_of(&mut overlay, token, owner)?,
        U256::from(42_000u64),
        "overlay calls are non-committing"
    );

    Ok(())
}

/// A superseded production simulation must stop after execution has genuinely
/// entered the EVM. Dropping only the async waiter is insufficient because the
/// blocking worker and its permit would continue running. The same overlay must
/// remain reusable after its cancelled checkpoint is reverted and its shared
/// memory buffer is reclaimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_evm_execution_is_cooperatively_cancelled() -> Result<()> {
    use revm::state::{AccountInfo, Bytecode};

    let mut cache = setup_cache().await?;
    let caller = Address::repeat_byte(0x71);
    let contract = Address::repeat_byte(0x72);
    let token = Address::repeat_byte(0x73);
    let owner = Address::repeat_byte(0x74);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, caller);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(42_000_u64),
    )?;

    // PUSH1 1; PUSH1 0; SSTORE; JUMPDEST; PUSH1 5; JUMP. The storage write
    // proves checkpoint cleanup; the loop keeps execution inside REVM until the
    // inspector observes cancellation.
    let runtime = Bytecode::new_raw(Bytes::from_static(&[
        0x60, 0x01, 0x60, 0x00, 0x55, 0x5b, 0x60, 0x05, 0x56,
    ]));
    cache.db_mut().insert_account_info(
        contract,
        AccountInfo {
            code_hash: runtime.hash_slow(),
            code: Some(runtime),
            ..Default::default()
        },
    );
    cache.insert_storage_slot(contract, U256::ZERO, U256::from(9_u64))?;

    let cancellation = SimulationCancellationToken::new();
    let worker_cancellation = cancellation.clone();
    let snapshot = cache.snapshot();
    let worker = tokio::task::spawn_blocking(move || {
        let mut overlay = EvmOverlay::new(snapshot, None);
        let cancelled = overlay.call_raw_with_access_list_with_cancellation(
            caller,
            contract,
            Bytes::new(),
            &TxConfig {
                gas_limit: Some(u64::MAX),
                ..Default::default()
            },
            &worker_cancellation,
        );
        let storage_after = overlay.storage(contract, U256::ZERO)?;
        let balance_after = overlay_balance_of(&mut overlay, token, owner)?;
        Ok::<_, anyhow::Error>((
            cancelled,
            storage_after,
            balance_after,
            overlay.missing_state().clone(),
            overlay.blockhash_zero_fallback(),
        ))
    });

    tokio::time::timeout(Duration::from_millis(250), async {
        while !cancellation.has_started() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real EVM execution must start before cancellation");
    cancellation.cancel();

    let (result, storage_after, balance_after, missing_state, blockhash_zero_fallback) =
        tokio::time::timeout(Duration::from_millis(250), worker)
            .await
            .expect("cancelled EVM execution must release its blocking worker")
            .expect("blocking worker must not panic")?;
    assert!(matches!(result, Err(OverlayError::Cancelled)));
    assert_eq!(
        storage_after,
        U256::from(9_u64),
        "the cancelled SSTORE must be reverted to the snapshot value"
    );
    assert_eq!(
        balance_after,
        U256::from(42_000_u64),
        "the same overlay and reclaimed buffer must remain usable"
    );
    assert!(
        missing_state.is_empty(),
        "reused overlay recorded unexpected missing state: {missing_state:?}"
    );
    assert!(!blockhash_zero_fallback);
    Ok(())
}

/// A scope cancelled before execution begins must reject without entering REVM,
/// and repeated cancellation requests must remain harmless.
#[tokio::test(flavor = "multi_thread")]
async fn pre_cancelled_scope_is_rejected_and_cancel_is_idempotent() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut overlay = EvmOverlay::new(cache.snapshot(), None);
    let cancellation = SimulationCancellationToken::new();

    cancellation.cancel();
    cancellation.cancel();
    assert!(cancellation.is_cancelled());
    assert!(!cancellation.has_started());

    let result = overlay.call_raw_with_access_list_with_cancellation(
        Address::repeat_byte(0x75),
        Address::repeat_byte(0x76),
        Bytes::new(),
        &TxConfig::default(),
        &cancellation,
    );
    assert!(matches!(result, Err(OverlayError::Cancelled)));
    assert!(
        !cancellation.has_started(),
        "a pre-cancelled scope must reject before the first EVM instruction"
    );
    assert!(overlay.missing_state().is_empty());
    assert!(!overlay.blockhash_zero_fallback());
    Ok(())
}

/// Opting into cancellation must be byte/result-equivalent when no
/// cancellation is requested, including the captured access-list evidence.
#[tokio::test(flavor = "multi_thread")]
async fn uncancelled_execution_retains_result_and_access_list_evidence() -> Result<()> {
    let mut cache = setup_cache().await?;
    let caller = Address::repeat_byte(0x73);
    let token = Address::repeat_byte(0x74);
    let owner = Address::repeat_byte(0x75);
    install_default_account(&mut cache, caller);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(42_000_u64),
    )?;

    let calldata: Bytes = MockERC20::balanceOfCall { account: owner }
        .abi_encode()
        .into();
    let snapshot = cache.snapshot();
    let mut baseline_overlay = EvmOverlay::new(Arc::clone(&snapshot), None);
    let mut cancellable_overlay = EvmOverlay::new(snapshot, None);
    let (baseline_result, baseline_access_list) = baseline_overlay.call_raw_with_access_list_with(
        caller,
        token,
        calldata.clone(),
        &TxConfig::default(),
    )?;
    let cancellation = SimulationCancellationToken::new();
    let (cancellable_result, cancellable_access_list) = cancellable_overlay
        .call_raw_with_access_list_with_cancellation(
            caller,
            token,
            calldata.clone(),
            &TxConfig::default(),
            &cancellation,
        )?;
    let (replayed_result, replayed_access_list) = cancellable_overlay
        .call_raw_with_access_list_with_cancellation(
            caller,
            token,
            calldata,
            &TxConfig::default(),
            &cancellation,
        )?;

    assert_eq!(cancellable_result, baseline_result);
    assert_eq!(
        cancellable_access_list.accounts,
        baseline_access_list.accounts
    );
    assert_eq!(
        cancellable_access_list.code_hashes,
        baseline_access_list.code_hashes
    );
    assert_eq!(cancellable_access_list.slots, baseline_access_list.slots);
    assert_eq!(
        cancellable_access_list.block_numbers,
        baseline_access_list.block_numbers
    );
    assert_eq!(replayed_result, baseline_result);
    assert_eq!(replayed_access_list.accounts, baseline_access_list.accounts);
    assert_eq!(
        replayed_access_list.code_hashes,
        baseline_access_list.code_hashes
    );
    assert_eq!(replayed_access_list.slots, baseline_access_list.slots);
    assert_eq!(
        replayed_access_list.block_numbers,
        baseline_access_list.block_numbers
    );
    assert!(cancellation.has_started());
    assert!(!cancellation.is_cancelled());
    assert_eq!(
        overlay_balance_of(&mut cancellable_overlay, token, owner)?,
        U256::from(42_000_u64),
        "the cancellable call must revert its checkpoint"
    );
    Ok(())
}

/// An offline overlay must make an unresolved storage read observable instead
/// of silently treating its ZERO fallback as authoritative state. Readiness
/// gates use this signal to reject an incompletely warmed speculative quote.
#[tokio::test(flavor = "multi_thread")]
async fn offline_overlay_reports_missing_storage_and_reset_clears_it() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x45);
    let slot = U256::from(9);
    let snapshot = cache.snapshot();
    let mut overlay = EvmOverlay::new(snapshot, None);

    assert_eq!(overlay.storage(contract, slot)?, U256::ZERO);
    assert_eq!(
        overlay.missing_state().storage,
        [(contract, slot)].into_iter().collect()
    );
    assert!(!overlay.missing_state().is_empty());

    overlay.reset();
    assert!(overlay.missing_state().is_empty());
    Ok(())
}

/// A missing account header is distinct from an account the snapshot already
/// knows does not exist. Only the unresolved former case makes an offline
/// simulation incomplete.
#[tokio::test(flavor = "multi_thread")]
async fn offline_overlay_reports_unresolved_account_headers() -> Result<()> {
    let mut cache = setup_cache().await?;
    let unresolved = Address::repeat_byte(0x49);
    let snapshot = cache.snapshot();
    let mut overlay = EvmOverlay::new(snapshot, None);

    assert!(overlay.basic(unresolved)?.is_none());
    assert_eq!(
        overlay.missing_state().accounts,
        [unresolved].into_iter().collect()
    );
    let missing = overlay.missing_state().as_read_set();
    assert!(missing.accounts.contains(&unresolved));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_reports_its_complete_resident_read_set() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x4a);
    install_mock_erc20(&mut cache, token);
    cache.insert_storage_slot(token, U256::from(7), U256::from(9))?;

    let snapshot = cache.snapshot();
    let resident = snapshot.resident_read_set();

    assert!(resident.accounts.contains(&token));
    assert!(
        resident
            .code_hashes
            .contains(&mock_erc20_runtime().hash_slow())
    );
    assert!(resident.slots.contains(&(token, U256::from(7))));
    assert_eq!(
        snapshot.account_code_hash(token),
        Some(mock_erc20_runtime().hash_slow())
    );
    Ok(())
}

/// A call-scoped code override must affect nested execution without leaking
/// into the reusable overlay. V3 quoters need this to neutralize the output
/// token transfer that precedes their intentional quote-data revert when a
/// snapshot does not carry arbitrary ERC20 balance mappings.
#[tokio::test(flavor = "multi_thread")]
async fn call_scoped_code_override_is_applied_then_restored() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x46);
    let caller = Address::repeat_byte(0x47);
    let recipient = Address::repeat_byte(0x48);

    install_default_account(&mut cache, caller);
    install_mock_erc20(&mut cache, token);
    let snapshot = cache.snapshot();
    let mut overlay = EvmOverlay::new(snapshot, None);
    let calldata: Bytes = MockERC20::transferCall {
        to: recipient,
        amount: U256::from(1u64),
    }
    .abi_encode()
    .into();

    let before = overlay.call_raw(caller, token, calldata.clone())?;
    assert!(
        !before.is_success(),
        "the real token must reject an unfunded transfer"
    );

    // Runtime: return ABI `true` for any call without touching storage.
    let transfer_success =
        Bytes::from_static(&[0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);
    let during = overlay.call_raw_with_code_overrides(
        caller,
        token,
        calldata.clone(),
        &[(token, transfer_success)],
    )?;
    assert!(during.is_success(), "the scoped override must execute");

    let after = overlay.call_raw(caller, token, calldata)?;
    assert!(
        !after.is_success(),
        "the real token code must be restored after the scoped call"
    );
    Ok(())
}

/// Regression (§16 fix-review HIGH): `snapshot` must mirror the live
/// account-state-aware read. A `StorageCleared` account with a backend-only
/// (shadowed) slot reads ZERO live; the snapshot, `storage_value`, and a
/// snapshot-backed overlay must all agree — not the shadowed backend value. Pre-
/// fix the snapshot/overlay read the shadowed 100 while the live cache read 0.
#[tokio::test]
async fn snapshot_mirrors_live_read_for_cleared_account() -> Result<()> {
    let token = Address::repeat_byte(0x5c);
    let slot = U256::from(MOCK_ERC20_BALANCE_SLOT); // absent from the cleared overlay
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token); // sets account_state = StorageCleared
    cache.inject_storage_batch(&[(token, slot, U256::from(100))]); // backend-only shadow

    // Live read is ZERO (the §16.0 fix).
    assert_eq!(cache.cached_storage_value(token, slot), Some(U256::ZERO));

    let snapshot: Arc<EvmSnapshot> = cache.snapshot();
    assert_eq!(
        snapshot.storage_value(token, slot),
        Some(U256::ZERO),
        "snapshot.storage_value must mirror the live cleared read, not the shadowed 100"
    );

    // A snapshot-backed overlay (no ext_db, as the freshness validator uses) must
    // also read ZERO for the cleared account's absent slot.
    let mut overlay = EvmOverlay::new(Arc::clone(&snapshot), None);
    let value = overlay
        .storage(token, slot)
        .map_err(|e| anyhow!("overlay storage read failed: {e:?}"))?;
    assert_eq!(
        value,
        U256::ZERO,
        "snapshot-backed overlay must read ZERO for a cleared account's absent slot"
    );
    Ok(())
}

/// Regression (round-2 HIGH, account axis): `snapshot` / `EvmOverlay::basic`
/// must mirror the live account read for a `NotExisting` account. revm treats such
/// an account as absent (`DbAccount::info()` → None), and `loaded_account_info`
/// already does; the snapshot/parallel path must agree — not surface a phantom
/// existing account with stale info. Pre-fix `EvmOverlay::basic` returned
/// `Some(info)`.
#[tokio::test]
async fn snapshot_basic_returns_none_for_notexisting_account() -> Result<()> {
    use revm::database::AccountState;
    use revm::database_interface::Database;
    use revm::state::AccountInfo;

    let acct = Address::repeat_byte(0x6e);
    let mut cache = setup_cache().await?;
    // An overlay account revm marks NotExisting (e.g. after a selfdestruct) carries
    // (default) info but is absent to the EVM.
    cache.db_mut().insert_account_info(
        acct,
        AccountInfo {
            balance: U256::from(1000),
            ..Default::default()
        },
    );
    cache
        .db_mut()
        .cache
        .accounts
        .get_mut(&acct)
        .expect("overlay account present")
        .account_state = AccountState::NotExisting;

    let snapshot: Arc<EvmSnapshot> = cache.snapshot();
    let mut overlay = EvmOverlay::new(Arc::clone(&snapshot), None);
    let basic = overlay
        .basic(acct)
        .map_err(|e| anyhow!("overlay basic read failed: {e:?}"))?;
    assert!(
        basic.is_none(),
        "snapshot-backed overlay must read a NotExisting account as absent (None), \
         not a phantom Some(info); got {basic:?}"
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_block_hash_returns_resident_dependency_offline() -> Result<()> {
    let mut cache = setup_cache().await?;
    let number = 42_u64;
    let hash = B256::repeat_byte(0x42);
    cache
        .db_mut()
        .cache
        .block_hashes
        .insert(U256::from(number), hash);

    let snapshot = cache.snapshot();
    assert_eq!(snapshot.block_hash(number), Some(hash));
    assert!(snapshot.resident_read_set().block_numbers.contains(&number));
    let mut overlay = EvmOverlay::new(snapshot, None);
    assert_eq!(overlay.block_hash(number)?, hash);

    let mut deep = EvmOverlay::new(cache.snapshot_deep_clone(), None);
    assert_eq!(deep.block_hash(number)?, hash);
    Ok(())
}

#[tokio::test]
async fn snapshot_block_hash_does_not_infer_hash_from_block_context() -> Result<()> {
    let mut cache = setup_cache().await?;
    let number = 43_u64;
    cache.set_block_context(Some(number), None);

    let snapshot = cache.snapshot();
    assert_eq!(snapshot.block_number(), Some(number));
    assert_eq!(snapshot.block_hash(number), None);
    assert!(!snapshot.resident_read_set().block_numbers.contains(&number));
    Ok(())
}

#[tokio::test]
async fn snapshot_block_hash_replacement_preserves_prior_snapshot_lineage() -> Result<()> {
    let mut cache = setup_cache().await?;
    let number = 44_u64;
    let displaced_hash = B256::repeat_byte(0x44);
    let replacement_hash = B256::repeat_byte(0x45);

    cache
        .db_mut()
        .cache
        .block_hashes
        .insert(U256::from(number), displaced_hash);
    let displaced_snapshot = cache.snapshot();

    cache
        .db_mut()
        .cache
        .block_hashes
        .insert(U256::from(number), replacement_hash);
    let replacement_snapshot = cache.snapshot();

    assert_eq!(displaced_snapshot.block_hash(number), Some(displaced_hash));
    assert_eq!(
        replacement_snapshot.block_hash(number),
        Some(replacement_hash)
    );
    assert_eq!(
        displaced_snapshot.block_hash(number),
        Some(displaced_hash),
        "a replacement branch must not rewrite a previously issued snapshot"
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_block_context_hash_is_hash_pinned_and_immutable() -> Result<()> {
    let mut cache = setup_cache().await?;
    let displaced_hash = B256::repeat_byte(0x51);
    let replacement_hash = B256::repeat_byte(0x52);

    cache.set_block(BlockId::from((displaced_hash, Some(true))));
    cache.set_block_context(Some(51), None);
    let displaced_snapshot = cache.snapshot();
    let displaced_deep = cache.snapshot_deep_clone();

    cache.set_block(BlockId::from((replacement_hash, Some(true))));
    cache.set_block_context(Some(51), None);
    let replacement_snapshot = cache.snapshot();

    assert_eq!(
        displaced_snapshot.block_context_hash(),
        Some(displaced_hash)
    );
    assert_eq!(displaced_deep.block_context_hash(), Some(displaced_hash));
    assert_eq!(
        replacement_snapshot.block_context_hash(),
        Some(replacement_hash)
    );
    assert_eq!(
        displaced_snapshot.block_context_hash(),
        Some(displaced_hash),
        "repinning the live cache must not rewrite an issued snapshot's lineage"
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_block_context_hash_is_absent_for_number_pins() -> Result<()> {
    let mut cache = setup_cache().await?;
    cache.set_block(BlockId::number(52));

    assert_eq!(cache.snapshot().block_context_hash(), None);
    assert_eq!(cache.snapshot_deep_clone().block_context_hash(), None);
    Ok(())
}
