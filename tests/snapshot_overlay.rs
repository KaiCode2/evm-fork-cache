//! Offline integration tests for the snapshot/overlay isolation guarantees that
//! underpin the crate's parallel fan-out model.
//!
//! These pin the invariants a search loop relies on:
//! - [`EvmCache::create_snapshot`] yields an immutable, point-in-time view that
//!   later cache mutations cannot perturb.
//! - Overlays derived from one snapshot are isolated from each other and from the
//!   live cache.
//!
//! All state is injected over a mocked provider, so no test touches the network.

mod common;

use std::sync::Arc;

use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::{Result, anyhow};
use revm::context::result::ExecutionResult;
use revm::database_interface::Database;

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20, setup_cache,
    transfer,
};
use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot};

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
    let snapshot = cache.create_snapshot();
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
    // `original`" and is captured by `create_snapshot`.
    cache
        .db_mut()
        .insert_account_storage(contract, slot, original)?;

    let snapshot = cache.create_snapshot();
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

    let snapshot: Arc<EvmSnapshot> = cache.create_snapshot();
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
