//! Offline acceptance tests for the Phase 3 state-update primitives (Pillar B.1).
//!
//! These are the **contract** the implementation must satisfy: the
//! `StateUpdate` vocabulary, `EvmCache::apply_update` / `apply_updates`, the
//! `StateDiff` output, and the refold of the existing writers. Everything runs
//! fully offline (mocked provider, state injected directly), so no test reaches
//! the network.
//!
//! Layering vocabulary used throughout:
//! - **layer 1 / overlay** = the CacheDB overlay (`db_mut().cache.accounts`),
//!   which wins on reads.
//! - **layer 2 / backend** = the BlockchainDb backend
//!   (`blockchain_db().storage()` / `.accounts()`).

mod common;

use alloy_primitives::{Address, Bytes, U256};
use anyhow::Result;

use common::{
    MOCK_ERC20_BALANCE_SLOT, balance_of, install_default_account, install_mock_erc20, setup_cache,
};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::{
    AccountPatch, PurgeScope, SkippedBalanceDelta, SkippedDelta, SkippedMask, SlotChange,
    SlotDelta, StateDiff, StateUpdate,
};
use revm::state::{AccountInfo, Bytecode};

// ---------------------------------------------------------------------------
// Layer-inspection helpers (read each cache layer independently).
// ---------------------------------------------------------------------------

/// Hashed storage slot of `balanceOf[owner]` for the MockERC20 fixture.
fn balance_slot_for(owner: Address) -> U256 {
    use alloy_sol_types::SolValue;
    let key =
        alloy_primitives::keccak256((owner, U256::from(MOCK_ERC20_BALANCE_SLOT)).abi_encode());
    U256::from_be_bytes(key.0)
}

/// Value of a slot in the CacheDB overlay (layer 1) only.
fn overlay_slot(cache: &mut EvmCache, addr: Address, slot: U256) -> Option<U256> {
    cache
        .db_mut()
        .cache
        .accounts
        .get(&addr)
        .and_then(|a| a.storage.get(&slot).copied())
}

/// Value of a slot in the BlockchainDb backend (layer 2) only.
fn backend_slot(cache: &EvmCache, addr: Address, slot: U256) -> Option<U256> {
    cache
        .blockchain_db()
        .storage()
        .read()
        .get(&addr)
        .and_then(|s| s.get(&slot).copied())
}

/// Whether the overlay (layer 1) has an account entry for `addr`.
fn overlay_has_account(cache: &mut EvmCache, addr: Address) -> bool {
    cache.db_mut().cache.accounts.contains_key(&addr)
}

/// Overlay (layer 1) balance for `addr`, if an overlay account exists.
fn overlay_balance(cache: &mut EvmCache, addr: Address) -> Option<U256> {
    cache
        .db_mut()
        .cache
        .accounts
        .get(&addr)
        .map(|a| a.info.balance)
}

/// Overlay (layer 1) nonce for `addr`, if an overlay account exists.
fn overlay_nonce(cache: &mut EvmCache, addr: Address) -> Option<u64> {
    cache
        .db_mut()
        .cache
        .accounts
        .get(&addr)
        .map(|a| a.info.nonce)
}

/// Backend (layer 2) balance for `addr`, if a backend account exists.
fn backend_balance(cache: &EvmCache, addr: Address) -> Option<U256> {
    cache
        .blockchain_db()
        .accounts()
        .read()
        .get(&addr)
        .map(|i| i.balance)
}

// ===========================================================================
// Pure-data vocabulary (public API, no cache).
// ===========================================================================

#[test]
fn account_patch_builders_compose() {
    let empty = AccountPatch::default();
    assert_eq!(empty.balance, None);
    assert_eq!(empty.nonce, None);
    assert_eq!(empty.code, None);

    let patch = AccountPatch::default()
        .balance(U256::from(42))
        .nonce(7)
        .code(Bytes::from_static(&[0x60, 0x00]));
    assert_eq!(patch.balance, Some(U256::from(42)));
    assert_eq!(patch.nonce, Some(7));
    assert_eq!(patch.code, Some(Bytes::from_static(&[0x60, 0x00])));
}

#[test]
fn state_update_constructors_produce_expected_variants() {
    let a = Address::repeat_byte(0xaa);

    assert_eq!(
        StateUpdate::slot(a, U256::from(1), U256::from(2)),
        StateUpdate::Slot {
            address: a,
            slot: U256::from(1),
            value: U256::from(2),
        }
    );
    assert_eq!(
        StateUpdate::balance(a, U256::from(9)),
        StateUpdate::Account {
            address: a,
            patch: AccountPatch::default().balance(U256::from(9)),
        }
    );
    assert_eq!(
        StateUpdate::purge(a, PurgeScope::Account),
        StateUpdate::Purge {
            address: a,
            scope: PurgeScope::Account,
        }
    );
}

#[test]
fn state_diff_merge_and_is_empty() {
    let a = Address::repeat_byte(0xbb);
    let mut left = StateDiff::default();
    assert!(left.is_empty());
    assert_eq!(left.len(), 0);

    let mut right = StateDiff::default();
    right.slots.push(SlotChange {
        address: a,
        slot: U256::from(1),
        old: U256::ZERO,
        new: U256::from(5),
    });

    left.merge(right);
    assert!(!left.is_empty());
    assert_eq!(left.len(), 1);
    assert_eq!(left.slots.len(), 1);
    assert_eq!(left.slots[0].new, U256::from(5));
}

// ===========================================================================
// Slot updates — write-through semantics (mirror inject_storage_batch_fresh).
// ===========================================================================

#[tokio::test]
async fn apply_slot_writes_through_overlay_resident() -> Result<()> {
    // An overlay-resident slot must be healed in BOTH layers, and the change
    // must be observable on the synchronous EVM SLOAD path (here a balanceOf
    // against a StorageCleared MockERC20 account).
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

    let diff = cache.apply_update(&StateUpdate::slot(token, slot, U256::from(999)));

    // Both layers reflect the new value.
    assert_eq!(overlay_slot(&mut cache, token, slot), Some(U256::from(999)));
    assert_eq!(backend_slot(&cache, token, slot), Some(U256::from(999)));
    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::from(999))
    );

    // The diff records exactly the one change.
    assert_eq!(
        diff.slots,
        vec![SlotChange {
            address: token,
            slot,
            old: U256::from(100),
            new: U256::from(999),
        }]
    );
    assert!(diff.accounts.is_empty() && diff.purged.is_empty());

    // The EVM SLOAD path sees the healed value.
    assert_eq!(balance_of(&mut cache, token, owner)?, U256::from(999));
    Ok(())
}

#[tokio::test]
async fn apply_slot_no_overlay_account_is_not_materialized() -> Result<()> {
    // Writing to an address with no overlay entry must populate the backend
    // (layer 2) and NOT materialize a layer-1 overlay account — preserving the
    // cold-prefetch / layer-2-only invariant.
    let addr = Address::repeat_byte(0x33);
    let slot = U256::from(7);

    let mut cache = setup_cache().await?;
    assert!(
        !overlay_has_account(&mut cache, addr),
        "precondition: no overlay account"
    );

    let diff = cache.apply_update(&StateUpdate::slot(addr, slot, U256::from(5)));

    assert_eq!(backend_slot(&cache, addr, slot), Some(U256::from(5)));
    assert!(
        !overlay_has_account(&mut cache, addr),
        "no overlay account may be materialized for a layer-2-only slot write"
    );
    // The read falls through to the backend.
    assert_eq!(cache.cached_storage_value(addr, slot), Some(U256::from(5)));
    assert_eq!(
        diff.slots,
        vec![SlotChange {
            address: addr,
            slot,
            old: U256::ZERO,
            new: U256::from(5),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn apply_slot_unchanged_value_yields_empty_diff() -> Result<()> {
    let addr = Address::repeat_byte(0x44);
    let slot = U256::from(1);

    let mut cache = setup_cache().await?;
    // Seed the backend (layer 2) directly — inject_storage_batch does not load
    // an account, unlike insert_account_storage, which would fetch a fresh
    // address from the (mocked, empty) provider.
    cache.inject_storage_batch(&[(addr, slot, U256::from(50))]);

    let diff = cache.apply_update(&StateUpdate::slot(addr, slot, U256::from(50)));
    assert!(
        diff.is_empty(),
        "writing the cached value records no change"
    );
    Ok(())
}

#[tokio::test]
async fn apply_slot_is_idempotent() -> Result<()> {
    let addr = Address::repeat_byte(0x55);
    let slot = U256::from(2);

    let mut cache = setup_cache().await?;
    // Backend-direct seed (no account load) — see the note in the no-op test.
    cache.inject_storage_batch(&[(addr, slot, U256::from(1))]);

    let first = cache.apply_update(&StateUpdate::slot(addr, slot, U256::from(8)));
    assert_eq!(first.slots.len(), 1, "first apply records the change");

    let second = cache.apply_update(&StateUpdate::slot(addr, slot, U256::from(8)));
    assert!(second.is_empty(), "re-applying the same value is a no-op");
    Ok(())
}

// ===========================================================================
// Account updates — partial patch, write-through.
// ===========================================================================

#[tokio::test]
async fn apply_account_balance_patch_preserves_other_fields() -> Result<()> {
    let token = Address::repeat_byte(0x66);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token); // balance 0, nonce 0, code present

    let diff = cache.apply_update(&StateUpdate::Account {
        address: token,
        patch: AccountPatch::default().balance(U256::from(500)),
    });

    // Balance changed in the overlay (the winning layer); nonce/code preserved.
    assert_eq!(overlay_balance(&mut cache, token), Some(U256::from(500)));
    assert_eq!(overlay_nonce(&mut cache, token), Some(0));

    assert_eq!(diff.accounts.len(), 1);
    let change = &diff.accounts[0];
    assert_eq!(change.address, token);
    assert_eq!(change.balance, Some((U256::ZERO, U256::from(500))));
    assert_eq!(change.nonce, None, "nonce unchanged → no delta");
    assert_eq!(change.code_hash, None, "code unchanged → no delta");
    assert!(diff.slots.is_empty() && diff.purged.is_empty());
    Ok(())
}

#[tokio::test]
async fn apply_account_code_patch_recomputes_hash() -> Result<()> {
    let addr = Address::repeat_byte(0x77);
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, addr); // empty code

    let new_code = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xf3]);
    let expected_hash = Bytecode::new_raw(new_code.clone()).hash_slow();

    let diff = cache.apply_update(&StateUpdate::Account {
        address: addr,
        patch: AccountPatch::default().code(new_code.clone()),
    });

    assert_eq!(diff.accounts.len(), 1);
    let change = &diff.accounts[0];
    let (old_hash, new_hash) = change.code_hash.expect("code hash changed");
    assert_ne!(old_hash, new_hash);
    assert_eq!(
        new_hash, expected_hash,
        "code hash recomputed from the patched code"
    );
    assert_eq!(change.balance, None);
    assert_eq!(change.nonce, None);
    Ok(())
}

#[tokio::test]
async fn apply_account_patch_materializes_absent_account() -> Result<()> {
    // An account absent from both layers is created (in the backend) by a patch,
    // and the value is readable.
    let addr = Address::repeat_byte(0x88);
    let mut cache = setup_cache().await?;
    assert!(!overlay_has_account(&mut cache, addr));
    assert_eq!(backend_balance(&cache, addr), None);

    let diff = cache.apply_update(&StateUpdate::balance(addr, U256::from(1234)));

    assert_eq!(backend_balance(&cache, addr), Some(U256::from(1234)));
    assert_eq!(diff.accounts.len(), 1);
    assert_eq!(
        diff.accounts[0].balance,
        Some((U256::ZERO, U256::from(1234)))
    );
    Ok(())
}

// ===========================================================================
// Purge updates — dispatch to the existing layer logic, record what was removed.
// ===========================================================================

#[tokio::test]
async fn apply_purge_account_clears_both_layers() -> Result<()> {
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    // Populate both layers.
    common::transfer(&mut cache, token, owner, owner, U256::from(0)).ok();
    cache
        .db_mut()
        .insert_account_storage(token, U256::from(1), U256::from(9))?;
    cache.inject_storage_batch(&[(token, U256::from(2), U256::from(8))]);
    assert!(overlay_has_account(&mut cache, token));

    let diff = cache.apply_update(&StateUpdate::purge(token, PurgeScope::Account));

    assert!(
        !overlay_has_account(&mut cache, token),
        "overlay account removed"
    );
    assert_eq!(
        cache.pool_storage_slot_count(token),
        0,
        "backend storage gone"
    );
    {
        let accounts = cache.blockchain_db().accounts().read();
        assert!(!accounts.contains_key(&token), "backend account removed");
    }
    assert_eq!(diff.purged.len(), 1);
    assert_eq!(diff.purged[0].address, token);
    assert_eq!(diff.purged[0].scope, PurgeScope::Account);
    assert!(
        diff.purged[0].account_removed,
        "an account info was removed"
    );
    Ok(())
}

#[tokio::test]
async fn apply_purge_all_storage_keeps_account() -> Result<()> {
    let token = Address::repeat_byte(0x33);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    cache
        .db_mut()
        .insert_account_storage(token, U256::from(1), U256::from(9))?;
    cache.inject_storage_batch(&[(token, U256::from(2), U256::from(8))]);

    let diff = cache.apply_update(&StateUpdate::purge(token, PurgeScope::AllStorage));

    assert_eq!(
        cache.pool_storage_slot_count(token),
        0,
        "backend storage gone"
    );
    assert!(
        overlay_has_account(&mut cache, token),
        "account info preserved"
    );
    assert_eq!(diff.purged.len(), 1);
    assert_eq!(diff.purged[0].scope, PurgeScope::AllStorage);
    assert!(!diff.purged[0].account_removed);
    Ok(())
}

#[tokio::test]
async fn apply_purge_specific_slots() -> Result<()> {
    let token = Address::repeat_byte(0x44);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    cache.inject_storage_batch(&[
        (token, U256::from(1), U256::from(10)),
        (token, U256::from(2), U256::from(20)),
        (token, U256::from(3), U256::from(30)),
    ]);

    let diff = cache.apply_update(&StateUpdate::purge(
        token,
        PurgeScope::Slots(vec![U256::from(1), U256::from(3)]),
    ));

    assert_eq!(backend_slot(&cache, token, U256::from(1)), None);
    assert_eq!(
        backend_slot(&cache, token, U256::from(2)),
        Some(U256::from(20))
    );
    assert_eq!(backend_slot(&cache, token, U256::from(3)), None);
    assert_eq!(diff.purged.len(), 1);
    assert_eq!(diff.purged[0].slots_removed, 2);
    Ok(())
}

// ===========================================================================
// apply_updates — fold + merge.
// ===========================================================================

#[tokio::test]
async fn apply_updates_merges_mixed_batch() -> Result<()> {
    let acct = Address::repeat_byte(0x66);
    let pool = Address::repeat_byte(0x77);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, acct);
    cache.inject_storage_batch(&[(pool, U256::from(9), U256::from(1))]);

    let diff = cache.apply_updates(&[
        StateUpdate::slot(pool, U256::from(1), U256::from(100)),
        StateUpdate::balance(acct, U256::from(500)),
        StateUpdate::purge(pool, PurgeScope::Slots(vec![U256::from(9)])),
    ]);

    assert!(!diff.slots.is_empty(), "slot write recorded");
    assert!(!diff.accounts.is_empty(), "account patch recorded");
    assert!(!diff.purged.is_empty(), "purge recorded");
    Ok(())
}

#[tokio::test]
async fn apply_updates_same_slot_later_overrides() -> Result<()> {
    let addr = Address::repeat_byte(0x88);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;

    let diff = cache.apply_updates(&[
        StateUpdate::slot(addr, slot, U256::from(10)),
        StateUpdate::slot(addr, slot, U256::from(20)),
    ]);

    // Each apply contributes its own SlotChange (merge concatenates), so the
    // observed history is ZERO->10 then 10->20.
    assert_eq!(
        diff.slots,
        vec![
            SlotChange {
                address: addr,
                slot,
                old: U256::ZERO,
                new: U256::from(10)
            },
            SlotChange {
                address: addr,
                slot,
                old: U256::from(10),
                new: U256::from(20)
            },
        ]
    );
    assert_eq!(cache.cached_storage_value(addr, slot), Some(U256::from(20)));
    Ok(())
}

// ===========================================================================
// Refold equivalence — wrappers behave exactly as before.
// ===========================================================================

#[tokio::test]
async fn refold_purge_pool_storage_returns_same_count() -> Result<()> {
    let token = Address::repeat_byte(0x99);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    cache.inject_storage_batch(&[
        (token, U256::from(1), U256::from(10)),
        (token, U256::from(2), U256::from(20)),
    ]);

    // The wrapper still returns the backend slot count it removed.
    let removed = cache.purge_pool_storage(token);
    assert_eq!(removed, 2);
    assert_eq!(cache.pool_storage_slot_count(token), 0);
    Ok(())
}

#[tokio::test]
async fn refold_inject_storage_batch_fresh_matches_apply_updates() -> Result<()> {
    let token = Address::repeat_byte(0xa1);
    let slot = U256::from(4);

    // Path A: the existing wrapper.
    let mut a = setup_cache().await?;
    install_mock_erc20(&mut a, token);
    a.db_mut()
        .insert_account_storage(token, slot, U256::from(1))?;
    a.inject_storage_batch_fresh(&[(token, slot, U256::from(77))]);

    // Path B: the primitive it now wraps.
    let mut b = setup_cache().await?;
    install_mock_erc20(&mut b, token);
    b.db_mut()
        .insert_account_storage(token, slot, U256::from(1))?;
    let _ = b.apply_updates(&[StateUpdate::slot(token, slot, U256::from(77))]);

    assert_eq!(
        a.cached_storage_value(token, slot),
        b.cached_storage_value(token, slot),
        "wrapper and primitive leave the cache in the same state"
    );
    assert_eq!(
        overlay_slot(&mut a, token, slot),
        overlay_slot(&mut b, token, slot)
    );
    assert_eq!(backend_slot(&a, token, slot), backend_slot(&b, token, slot));
    Ok(())
}

// ===========================================================================
// Decision 2 (LOCKED: normalize) — protocols inject_v3_* now writes through to
// the backend (layer 2). Pre-fix this wrote layer 1 only.
// ===========================================================================

#[cfg(feature = "protocols")]
#[tokio::test]
async fn inject_v3_tick_bitmap_writes_through_to_backend() -> Result<()> {
    use std::collections::HashMap;

    let pool = Address::repeat_byte(0xb2);
    let mut cache = setup_cache().await?;

    let mut bitmap = HashMap::new();
    bitmap.insert(0i16, U256::from(123));
    bitmap.insert(1i16, U256::from(456));

    let injected = cache.inject_v3_tick_bitmap(pool, &bitmap)?;
    assert_eq!(injected, 2);

    // Normalized to write-through: the backend (layer 2) now holds the slots.
    // Before the refold this count was 0 (overlay-only write).
    assert!(
        cache.pool_storage_slot_count(pool) > 0,
        "inject_v3_tick_bitmap must write through to the backend (Decision 2)"
    );
    Ok(())
}

// ===========================================================================
// §15 addendum — relative / read-modify-write updates.
//
// `SlotDelta` reads the current value, applies a saturating mutation, and writes
// back (write-through). It is cold-aware: a delta on a slot the cache never
// fetched is NOT applied (it would corrupt an unknown balance) — it is skipped
// and surfaced in `StateDiff.skipped`. `modify_slot` is the general closure form.
// ===========================================================================

#[tokio::test]
async fn slot_delta_add_applies_to_hot_slot() -> Result<()> {
    let addr = Address::repeat_byte(0xc1);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    cache.inject_storage_batch(&[(addr, slot, U256::from(100))]);

    let diff = cache.apply_update(&StateUpdate::slot_delta(
        addr,
        slot,
        SlotDelta::Add(U256::from(50)),
    ));

    assert_eq!(
        cache.cached_storage_value(addr, slot),
        Some(U256::from(150))
    );
    assert_eq!(
        diff.slots,
        vec![SlotChange {
            address: addr,
            slot,
            old: U256::from(100),
            new: U256::from(150),
        }]
    );
    assert!(
        diff.skipped.is_empty(),
        "a hot slot is applied, not skipped"
    );
    Ok(())
}

#[tokio::test]
async fn slot_delta_sub_saturates_at_zero() -> Result<()> {
    let addr = Address::repeat_byte(0xc2);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    cache.inject_storage_batch(&[(addr, slot, U256::from(30))]);

    let diff = cache.apply_update(&StateUpdate::slot_delta(
        addr,
        slot,
        SlotDelta::Sub(U256::from(50)),
    ));

    assert_eq!(
        cache.cached_storage_value(addr, slot),
        Some(U256::ZERO),
        "Sub saturates at zero rather than underflowing"
    );
    assert_eq!(diff.slots.len(), 1);
    assert_eq!(diff.slots[0].new, U256::ZERO);
    Ok(())
}

#[tokio::test]
async fn slot_delta_add_saturates_at_max() -> Result<()> {
    let addr = Address::repeat_byte(0xc3);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    cache.inject_storage_batch(&[(addr, slot, U256::MAX - U256::from(1))]);

    cache.apply_update(&StateUpdate::slot_delta(
        addr,
        slot,
        SlotDelta::Add(U256::from(10)),
    ));

    assert_eq!(
        cache.cached_storage_value(addr, slot),
        Some(U256::MAX),
        "Add saturates at U256::MAX"
    );
    Ok(())
}

#[tokio::test]
async fn slot_delta_cold_slot_is_skipped_and_surfaced() -> Result<()> {
    // The correctness guarantee: a delta against an unknown (cold) value must not
    // be applied (it would corrupt the balance) — it is surfaced instead.
    let addr = Address::repeat_byte(0xc4);
    let slot = U256::from(7);
    let mut cache = setup_cache().await?;
    assert_eq!(
        cache.cached_storage_value(addr, slot),
        None,
        "precondition: slot is cold"
    );

    let diff = cache.apply_update(&StateUpdate::slot_delta(
        addr,
        slot,
        SlotDelta::Add(U256::from(50)),
    ));

    assert!(diff.slots.is_empty(), "nothing applied");
    assert_eq!(
        diff.skipped,
        vec![SkippedDelta {
            address: addr,
            slot,
            delta: SlotDelta::Add(U256::from(50)),
        }]
    );
    assert_eq!(
        cache.cached_storage_value(addr, slot),
        None,
        "the cold slot is left untouched so the next read fetches the truth"
    );
    Ok(())
}

#[tokio::test]
async fn slot_delta_writes_through_both_layers() -> Result<()> {
    let token = Address::repeat_byte(0xc5);
    let slot = U256::from(2);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    // Overlay-resident seed (account already installed, so no fetch).
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(100))?;

    cache.apply_update(&StateUpdate::slot_delta(
        token,
        slot,
        SlotDelta::Add(U256::from(5)),
    ));

    assert_eq!(overlay_slot(&mut cache, token, slot), Some(U256::from(105)));
    assert_eq!(backend_slot(&cache, token, slot), Some(U256::from(105)));
    Ok(())
}

#[tokio::test]
async fn modify_slot_applies_transform() -> Result<()> {
    let addr = Address::repeat_byte(0xc6);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    cache.inject_storage_batch(&[(addr, slot, U256::from(10))]);

    let change = cache.modify_slot(addr, slot, |cur| cur.map(|v| v * U256::from(2)));

    assert_eq!(
        change,
        Some(SlotChange {
            address: addr,
            slot,
            old: U256::from(10),
            new: U256::from(20),
        })
    );
    assert_eq!(cache.cached_storage_value(addr, slot), Some(U256::from(20)));
    Ok(())
}

#[tokio::test]
async fn modify_slot_closure_skips_cold() -> Result<()> {
    let addr = Address::repeat_byte(0xc7);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;

    // The closure returns None for a cold slot, so nothing is written.
    let change = cache.modify_slot(addr, slot, |cur| cur.map(|v| v + U256::from(1)));

    assert_eq!(change, None);
    assert_eq!(cache.cached_storage_value(addr, slot), None);
    Ok(())
}

#[tokio::test]
async fn modify_slot_can_write_absolute_on_cold() -> Result<()> {
    // The caller may choose to write an absolute value even on a cold slot (it
    // had external knowledge). The closure ignores the `None` and returns a value.
    let addr = Address::repeat_byte(0xc8);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;

    let change = cache.modify_slot(addr, slot, |_| Some(U256::from(7)));

    assert_eq!(
        change,
        Some(SlotChange {
            address: addr,
            slot,
            old: U256::ZERO,
            new: U256::from(7),
        })
    );
    assert_eq!(cache.cached_storage_value(addr, slot), Some(U256::from(7)));
    Ok(())
}

#[test]
fn state_diff_merge_includes_skipped() {
    let a = Address::repeat_byte(0xd9);
    let mut left = StateDiff::default();
    let mut right = StateDiff::default();
    right.skipped.push(SkippedDelta {
        address: a,
        slot: U256::from(1),
        delta: SlotDelta::Add(U256::from(5)),
    });

    left.merge(right);
    assert_eq!(left.skipped.len(), 1);
    assert_eq!(left.skipped[0].delta, SlotDelta::Add(U256::from(5)));
    // A skip is metadata, not a change: it does not affect is_empty/len.
    assert!(left.is_empty(), "a skipped delta is not a recorded change");
    assert_eq!(left.len(), 0);
}

#[tokio::test]
async fn balance_tracking_scenario() -> Result<()> {
    // The motivating use case: index an ERC-20 `Transfer(alice -> bob, amount)`
    // as two relative slot updates to keep the tracked balances hot, without ever
    // knowing the resulting absolute balances up front.
    let token = Address::repeat_byte(0xe0);
    let alice = Address::repeat_byte(0x0a);
    let bob = Address::repeat_byte(0x0b);
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO); // coinbase, for the SLOAD calls
    install_default_account(&mut cache, alice);
    install_default_account(&mut cache, bob);
    install_mock_erc20(&mut cache, token);

    let alice_slot = balance_slot_for(alice);
    let bob_slot = balance_slot_for(bob);

    // Seed the tracked balances once (the "make it hot" step) in an EVM-VISIBLE
    // way: overlay-resident, so the StorageCleared token account actually reads
    // them on the SLOAD path. (A backend-only inject is invisible here — see
    // `cached_storage_value_matches_evm_sload_for_cleared_account`.)
    cache
        .db_mut()
        .insert_account_storage(token, alice_slot, U256::from(1000))?;
    cache
        .db_mut()
        .insert_account_storage(token, bob_slot, U256::ZERO)?;

    // Sanity: the EVM actually sees the seeded balances.
    assert_eq!(balance_of(&mut cache, token, alice)?, U256::from(1000));

    // Transfer(alice -> bob, 300) decodes to two relative updates.
    let amount = U256::from(300);
    let diff = cache.apply_updates(&[
        StateUpdate::slot_delta(token, alice_slot, SlotDelta::Sub(amount)),
        StateUpdate::slot_delta(token, bob_slot, SlotDelta::Add(amount)),
    ]);

    // Validate via a real SLOAD (`balanceOf`), not just the cached accessor.
    assert_eq!(balance_of(&mut cache, token, alice)?, U256::from(700));
    assert_eq!(balance_of(&mut cache, token, bob)?, U256::from(300));
    assert_eq!(
        cache.cached_storage_value(token, alice_slot),
        Some(U256::from(700))
    );
    assert_eq!(
        cache.cached_storage_value(token, bob_slot),
        Some(U256::from(300))
    );
    assert!(diff.skipped.is_empty(), "both slots were seeded (hot)");
    assert_eq!(diff.slots.len(), 2);

    // Conservation: total supply across the two holders is unchanged.
    let total = cache.cached_storage_value(token, alice_slot).unwrap()
        + cache.cached_storage_value(token, bob_slot).unwrap();
    assert_eq!(total, U256::from(1000));
    Ok(())
}

// ===========================================================================
// §16.0 — the audit HIGH correctness bug: cached_storage_value must match the
// EVM SLOAD for a StorageCleared overlay account (else SlotDelta corrupts an
// EVM-invisible base). This test uses only existing symbols so it runs against
// the CURRENT (buggy) code: it is RED before the §16.0 fix, GREEN after.
// ===========================================================================

#[tokio::test]
async fn cached_storage_value_matches_evm_sload_for_cleared_account() -> Result<()> {
    let token = Address::repeat_byte(0x5c);
    let owner = Address::repeat_byte(0x5d);
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO); // coinbase, for call_raw
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token); // account_state = StorageCleared

    let slot = balance_slot_for(owner);
    // Backend-only seed: invisible to a StorageCleared overlay account's SLOAD.
    cache.inject_storage_batch(&[(token, slot, U256::from(100))]);

    // The real EVM SLOAD reads ZERO (StorageCleared, slot absent from overlay,
    // backend NOT consulted).
    let evm_seen = balance_of(&mut cache, token, owner)?;
    assert_eq!(
        evm_seen,
        U256::ZERO,
        "precondition: the EVM cannot see a backend-only seed on a StorageCleared account"
    );

    // cached_storage_value MUST agree with the EVM, not report the shadowed
    // backend value (100). Pre-fix it returns Some(100) -> this assert fails.
    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::ZERO),
        "cached_storage_value must mirror the EVM SLOAD (ZERO), not the shadowed backend value"
    );
    Ok(())
}

// ===========================================================================
// §16.0 — present-as-ZERO is HOT (delta applies to 0), distinct from cold (skip).
// ===========================================================================

#[tokio::test]
async fn slot_delta_on_present_zero_is_hot_not_skipped() -> Result<()> {
    let token = Address::repeat_byte(0x6a);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    // Overlay-resident ZERO: a *known* zero, not an absent (cold) slot.
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::ZERO)?;

    let diff = cache.apply_update(&StateUpdate::slot_delta(
        token,
        slot,
        SlotDelta::Add(U256::from(50)),
    ));

    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::from(50))
    );
    assert_eq!(
        diff.slots.len(),
        1,
        "present-as-zero is hot: the delta applies"
    );
    assert!(
        diff.skipped.is_empty(),
        "present-as-zero must NOT be treated as cold"
    );
    Ok(())
}

// ===========================================================================
// §16.5 — account-native-balance delta (BalanceDelta + modify_account_balance).
// ===========================================================================

#[tokio::test]
async fn balance_delta_applies_to_present_account() -> Result<()> {
    let acct = Address::repeat_byte(0x71);
    let mut cache = setup_cache().await?;
    cache.db_mut().insert_account_info(
        acct,
        AccountInfo {
            balance: U256::from(1000),
            nonce: 5,
            ..Default::default()
        },
    );

    let diff = cache.apply_update(&StateUpdate::balance_delta(
        acct,
        SlotDelta::Sub(U256::from(300)),
    ));

    assert_eq!(overlay_balance(&mut cache, acct), Some(U256::from(700)));
    assert_eq!(overlay_nonce(&mut cache, acct), Some(5), "nonce preserved");
    assert_eq!(
        backend_balance(&cache, acct),
        Some(U256::from(700)),
        "write-through to backend"
    );
    assert_eq!(diff.accounts.len(), 1);
    assert_eq!(
        diff.accounts[0].balance,
        Some((U256::from(1000), U256::from(700)))
    );
    assert!(diff.accounts[0].nonce.is_none(), "nonce unchanged");
    assert!(diff.skipped_balances.is_empty());
    Ok(())
}

#[tokio::test]
async fn balance_delta_on_cold_account_is_skipped_and_surfaced() -> Result<()> {
    let acct = Address::repeat_byte(0x72);
    let mut cache = setup_cache().await?;
    assert!(!overlay_has_account(&mut cache, acct));

    let diff = cache.apply_update(&StateUpdate::balance_delta(
        acct,
        SlotDelta::Add(U256::from(500)),
    ));

    assert!(
        diff.accounts.is_empty(),
        "nothing applied for an unknown balance"
    );
    assert_eq!(
        diff.skipped_balances,
        vec![SkippedBalanceDelta {
            address: acct,
            delta: SlotDelta::Add(U256::from(500)),
        }]
    );
    // Crucially: no account is materialized (avoids masking the real on-chain one).
    assert!(!overlay_has_account(&mut cache, acct));
    assert_eq!(backend_balance(&cache, acct), None);
    Ok(())
}

#[tokio::test]
async fn balance_delta_saturates_at_zero() -> Result<()> {
    let acct = Address::repeat_byte(0x73);
    let mut cache = setup_cache().await?;
    cache.db_mut().insert_account_info(
        acct,
        AccountInfo {
            balance: U256::from(100),
            ..Default::default()
        },
    );

    cache.apply_update(&StateUpdate::balance_delta(
        acct,
        SlotDelta::Sub(U256::from(500)),
    ));

    assert_eq!(
        overlay_balance(&mut cache, acct),
        Some(U256::ZERO),
        "Sub saturates at zero"
    );
    Ok(())
}

#[tokio::test]
async fn modify_account_balance_hot_and_cold() -> Result<()> {
    let acct = Address::repeat_byte(0x74);
    let mut cache = setup_cache().await?;
    cache.db_mut().insert_account_info(
        acct,
        AccountInfo {
            balance: U256::from(10),
            ..Default::default()
        },
    );

    let change = cache.modify_account_balance(acct, |cur| cur.map(|v| v * U256::from(3)));
    assert_eq!(
        change.and_then(|c| c.balance),
        Some((U256::from(10), U256::from(30)))
    );
    assert_eq!(overlay_balance(&mut cache, acct), Some(U256::from(30)));

    // A cold account: the closure receives None and skips; nothing materialized.
    let cold = Address::repeat_byte(0x75);
    let none = cache.modify_account_balance(cold, |cur| cur.map(|v| v + U256::from(1)));
    assert!(none.is_none());
    assert!(!overlay_has_account(&mut cache, cold));
    assert_eq!(backend_balance(&cache, cold), None);
    Ok(())
}

// ===========================================================================
// §16.6 — discoverable skip accessors over both skip kinds.
// ===========================================================================

#[tokio::test]
async fn skip_accessors_reflect_both_skip_kinds() -> Result<()> {
    let token = Address::repeat_byte(0x76);
    let acct = Address::repeat_byte(0x77);
    let mut cache = setup_cache().await?;

    // A cold slot delta and a cold balance delta: both skipped, no change recorded.
    let diff = cache.apply_updates(&[
        StateUpdate::slot_delta(token, U256::from(9), SlotDelta::Add(U256::from(1))),
        StateUpdate::balance_delta(acct, SlotDelta::Add(U256::from(1))),
    ]);
    assert!(diff.is_empty(), "changes-only: nothing applied");
    assert!(diff.has_skipped());
    assert_eq!(diff.skipped_len(), 2);
    assert!(!diff.is_fully_applied());

    // A fully-applied update reports no skips.
    let hot = Address::repeat_byte(0x78);
    cache.db_mut().insert_account_info(
        hot,
        AccountInfo {
            balance: U256::from(5),
            ..Default::default()
        },
    );
    let diff2 = cache.apply_update(&StateUpdate::balance_delta(
        hot,
        SlotDelta::Add(U256::from(5)),
    ));
    assert!(diff2.is_fully_applied());
    assert!(!diff2.has_skipped());
    Ok(())
}

// ===========================================================================
// §16.3 — serde round-trip of the vocabulary and the diff.
// ===========================================================================

#[test]
fn vocabulary_serde_round_trips() {
    let a = Address::repeat_byte(0x81);
    let updates = vec![
        StateUpdate::slot(a, U256::from(1), U256::from(2)),
        StateUpdate::slot_delta(a, U256::from(1), SlotDelta::Sub(U256::from(3))),
        StateUpdate::balance_delta(a, SlotDelta::Add(U256::from(4))),
        StateUpdate::account(a, AccountPatch::default().balance(U256::from(9)).nonce(2)),
        StateUpdate::purge(a, PurgeScope::Slots(vec![U256::from(1)])),
    ];
    let json = serde_json::to_string(&updates).expect("serialize updates");
    let back: Vec<StateUpdate> = serde_json::from_str(&json).expect("deserialize updates");
    assert_eq!(updates, back);

    let mut diff = StateDiff::default();
    diff.slots.push(SlotChange {
        address: a,
        slot: U256::from(1),
        old: U256::ZERO,
        new: U256::from(2),
    });
    diff.skipped.push(SkippedDelta {
        address: a,
        slot: U256::from(2),
        delta: SlotDelta::Add(U256::from(1)),
    });
    diff.skipped_balances.push(SkippedBalanceDelta {
        address: a,
        delta: SlotDelta::Sub(U256::from(1)),
    });
    let djson = serde_json::to_string(&diff).expect("serialize diff");
    let dback: StateDiff = serde_json::from_str(&djson).expect("deserialize diff");
    assert_eq!(diff, dback);
}

// ===========================================================================
// §16.1 — a no-op Account patch must not materialize a backend account.
// ===========================================================================

#[tokio::test]
async fn account_patch_noop_does_not_materialize_backend() -> Result<()> {
    let acct = Address::repeat_byte(0x95);
    let mut cache = setup_cache().await?;
    assert!(!overlay_has_account(&mut cache, acct));

    // All-None patch on an absent account: no change, and crucially no write.
    let diff = cache.apply_update(&StateUpdate::account(acct, AccountPatch::default()));
    assert!(diff.is_empty());
    assert!(diff.accounts.is_empty());
    assert_eq!(
        backend_balance(&cache, acct),
        None,
        "a no-op patch must not materialize a backend account"
    );
    assert!(!overlay_has_account(&mut cache, acct));

    // balance -> current value on a present account is also a no-op.
    let acct2 = Address::repeat_byte(0x96);
    cache.db_mut().insert_account_info(
        acct2,
        AccountInfo {
            balance: U256::from(50),
            ..Default::default()
        },
    );
    let diff2 = cache.apply_update(&StateUpdate::balance(acct2, U256::from(50)));
    assert!(diff2.accounts.is_empty(), "balance -> current is a no-op");
    Ok(())
}

// ===========================================================================
// §16.8/16.9 — batched apply_updates must equal sequential apply_update (the
// safety net for the single-lock fast-path). Mixed batch: distinct addresses,
// a same-slot repeat, a hot delta, an account patch, and a purge mid-batch.
// ===========================================================================

#[tokio::test]
async fn apply_updates_batched_equals_sequential() -> Result<()> {
    let p = Address::repeat_byte(0xa1);
    let q = Address::repeat_byte(0xa2);
    let slot1 = U256::from(1);
    let slot2 = U256::from(2);
    let slot3 = U256::from(3);

    // Build two identically-seeded caches.
    async fn seeded(p: Address, q: Address, slot2: U256) -> Result<EvmCache> {
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, p); // StorageCleared overlay account
        install_default_account(&mut cache, q);
        cache.inject_storage_batch(&[(p, slot2, U256::from(99))]); // backend slot for the purge
        Ok(cache)
    }
    let batch = vec![
        StateUpdate::slot(p, slot1, U256::from(500)),
        StateUpdate::slot(p, slot1, U256::from(600)), // same slot again (order matters)
        StateUpdate::slot_delta(p, slot1, SlotDelta::Add(U256::from(10))), // hot -> 610
        StateUpdate::balance(q, U256::from(1000)),
        StateUpdate::purge(p, PurgeScope::Slots(vec![slot2])), // purge mid-batch
        StateUpdate::slot(p, slot3, U256::from(7)),            // write after the purge
    ];

    let mut batched = seeded(p, q, slot2).await?;
    let diff_batched = batched.apply_updates(&batch);

    let mut sequential = seeded(p, q, slot2).await?;
    let mut diff_seq = StateDiff::default();
    for u in &batch {
        diff_seq.merge(sequential.apply_update(u));
    }

    assert_eq!(
        diff_batched, diff_seq,
        "batched diff must equal the sequential fold"
    );
    for (addr, slot) in [(p, slot1), (p, slot2), (p, slot3)] {
        assert_eq!(
            batched.cached_storage_value(addr, slot),
            sequential.cached_storage_value(addr, slot),
            "slot {slot} state diverged between batched and sequential"
        );
    }
    assert_eq!(
        overlay_balance(&mut batched, q),
        overlay_balance(&mut sequential, q)
    );
    assert_eq!(
        backend_balance(&batched, q),
        backend_balance(&sequential, q)
    );
    // Concrete expected end-state.
    assert_eq!(
        batched.cached_storage_value(p, slot1),
        Some(U256::from(610))
    );
    // Verify the purge via the backend layer directly: `p` is a StorageCleared
    // MockERC20, so cached_storage_value reads an absent slot as 0 (mirroring the
    // SLOAD) regardless of the purge — the backend map is the meaningful check.
    assert_eq!(
        backend_slot(&batched, p, slot2),
        None,
        "slot2 purged from the backend"
    );
    assert_eq!(batched.cached_storage_value(p, slot3), Some(U256::from(7)));
    Ok(())
}

// ===========================================================================
// §16.8 — Account-patch coverage gaps.
// ===========================================================================

#[tokio::test]
async fn account_patch_writes_through_to_backend_on_overlay_present() -> Result<()> {
    let acct = Address::repeat_byte(0x90);
    let mut cache = setup_cache().await?;
    cache.db_mut().insert_account_info(
        acct,
        AccountInfo {
            balance: U256::from(100),
            ..Default::default()
        },
    );

    let diff = cache.apply_update(&StateUpdate::balance(acct, U256::from(500)));

    assert_eq!(overlay_balance(&mut cache, acct), Some(U256::from(500)));
    assert_eq!(
        backend_balance(&cache, acct),
        Some(U256::from(500)),
        "backend is always written, even when an overlay account exists"
    );
    assert_eq!(
        diff.accounts[0].balance,
        Some((U256::from(100), U256::from(500)))
    );
    Ok(())
}

#[tokio::test]
async fn account_patch_on_backend_only_account_does_not_materialize_overlay() -> Result<()> {
    let acct = Address::repeat_byte(0x91);
    let mut cache = setup_cache().await?;
    // Seed only the backend (the cold-prefetched, layer-2-only case).
    cache.blockchain_db().accounts().write().insert(
        acct,
        AccountInfo {
            balance: U256::from(100),
            nonce: 3,
            ..Default::default()
        },
    );

    let diff = cache.apply_update(&StateUpdate::balance(acct, U256::from(500)));

    assert_eq!(
        diff.accounts[0].balance,
        Some((U256::from(100), U256::from(500))),
        "old value loaded from the backend"
    );
    assert_eq!(backend_balance(&cache, acct), Some(U256::from(500)));
    assert!(
        !overlay_has_account(&mut cache, acct),
        "no overlay account materialized for a backend-only patch"
    );
    Ok(())
}

#[tokio::test]
async fn account_patch_nonce_only() -> Result<()> {
    let acct = Address::repeat_byte(0x92);
    let mut cache = setup_cache().await?;
    cache.db_mut().insert_account_info(
        acct,
        AccountInfo {
            nonce: 1,
            ..Default::default()
        },
    );

    let diff = cache.apply_update(&StateUpdate::nonce(acct, 9));

    assert_eq!(diff.accounts[0].nonce, Some((1, 9)));
    assert!(diff.accounts[0].balance.is_none());
    assert!(diff.accounts[0].code_hash.is_none());
    assert_eq!(overlay_nonce(&mut cache, acct), Some(9));
    Ok(())
}

#[tokio::test]
async fn account_patch_multi_field() -> Result<()> {
    let acct = Address::repeat_byte(0x93);
    let mut cache = setup_cache().await?;
    cache
        .db_mut()
        .insert_account_info(acct, AccountInfo::default());

    let diff = cache.apply_update(&StateUpdate::account(
        acct,
        AccountPatch::default()
            .balance(U256::from(42))
            .nonce(7)
            .code(Bytes::from_static(&[0x60, 0x00])),
    ));

    assert!(diff.accounts[0].balance.is_some());
    assert!(diff.accounts[0].nonce.is_some());
    assert!(diff.accounts[0].code_hash.is_some());
    Ok(())
}

#[tokio::test]
async fn account_patch_empty_code_clears_to_empty_hash() -> Result<()> {
    let token = Address::repeat_byte(0x94);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token); // non-empty code

    let diff = cache.apply_update(&StateUpdate::code(token, Bytes::new()));
    let empty_hash = Bytecode::new_raw(Bytes::new()).hash_slow();
    assert_eq!(
        diff.accounts[0].code_hash.expect("code changed").1,
        empty_hash
    );

    // Patching empty over already-empty is a no-op.
    let diff2 = cache.apply_update(&StateUpdate::code(token, Bytes::new()));
    assert!(diff2.accounts.is_empty(), "empty over empty is a no-op");
    Ok(())
}

// ===========================================================================
// §16.8 — modify_slot write-through layer policy.
// ===========================================================================

#[tokio::test]
async fn modify_slot_writes_through_both_layers() -> Result<()> {
    let token = Address::repeat_byte(0x97);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(10))?;

    cache.modify_slot(token, slot, |c| c.map(|v| v + U256::from(5)));

    assert_eq!(overlay_slot(&mut cache, token, slot), Some(U256::from(15)));
    assert_eq!(backend_slot(&cache, token, slot), Some(U256::from(15)));
    Ok(())
}

// ===========================================================================
// §16.8 — purge edges.
// ===========================================================================

#[tokio::test]
async fn purge_absent_account_is_noop_record() -> Result<()> {
    let acct = Address::repeat_byte(0x98);
    let mut cache = setup_cache().await?;

    let diff = cache.apply_update(&StateUpdate::purge(acct, PurgeScope::Account));

    assert_eq!(diff.purged.len(), 1);
    assert!(!diff.purged[0].account_removed);
    assert_eq!(diff.purged[0].slots_removed, 0);
    Ok(())
}

#[tokio::test]
async fn purge_slots_counts_present_backend_slots_only() -> Result<()> {
    let token = Address::repeat_byte(0x99);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    cache.inject_storage_batch(&[(token, U256::from(1), U256::from(10))]); // only slot 1 present

    let diff = cache.apply_update(&StateUpdate::purge(
        token,
        PurgeScope::Slots(vec![U256::from(1), U256::from(2)]), // slot 2 absent
    ));

    assert_eq!(
        diff.purged[0].slots_removed, 1,
        "only the present backend slot is counted"
    );
    Ok(())
}

// ===========================================================================
// §16.7 — account-field convenience constructors.
// ===========================================================================

#[test]
fn state_update_account_field_constructors() {
    let a = Address::repeat_byte(0x9a);
    assert_eq!(
        StateUpdate::nonce(a, 7),
        StateUpdate::Account {
            address: a,
            patch: AccountPatch::default().nonce(7),
        }
    );
    assert_eq!(
        StateUpdate::code(a, Bytes::from_static(&[0x60])),
        StateUpdate::Account {
            address: a,
            patch: AccountPatch::default().code(Bytes::from_static(&[0x60])),
        }
    );
    let patch = AccountPatch::default().balance(U256::from(1)).nonce(2);
    assert_eq!(
        StateUpdate::account(a, patch.clone()),
        StateUpdate::Account { address: a, patch }
    );
}

// ===========================================================================
// §16.8 — Decision-2 write-through pins for the remaining protocols injectors.
// ===========================================================================

#[cfg(feature = "protocols")]
#[tokio::test]
async fn inject_v2_pool_metadata_writes_through_to_backend() -> Result<()> {
    use evm_fork_cache::cache::V2PoolMetadata;

    let pool = Address::repeat_byte(0xb3);
    let mut cache = setup_cache().await?;
    let meta = V2PoolMetadata {
        token0: Address::repeat_byte(0x01),
        token1: Address::repeat_byte(0x02),
        last_block_timestamp: 0,
    };

    cache.inject_v2_pool_metadata(pool, &meta)?;

    assert!(
        cache.pool_storage_slot_count(pool) > 0,
        "inject_v2_pool_metadata must write through to the backend (Decision 2)"
    );
    Ok(())
}

#[cfg(feature = "protocols")]
#[tokio::test]
async fn inject_v3_ticks_writes_through_to_backend() -> Result<()> {
    use evm_fork_cache::cache::TickInfo;
    use std::collections::HashMap;

    let pool = Address::repeat_byte(0xb4);
    let mut cache = setup_cache().await?;
    let mut ticks = HashMap::new();
    ticks.insert(
        0i32,
        TickInfo {
            liquidity_gross: 100,
            liquidity_net: 50,
            initialized: true,
        },
    );

    let injected = cache.inject_v3_ticks(pool, &ticks)?;
    assert!(injected > 0);
    assert!(
        cache.pool_storage_slot_count(pool) > 0,
        "inject_v3_ticks must write through to the backend (Decision 2)"
    );
    Ok(())
}

// ===========================================================================
// §16 fix-review regressions — account_state-awareness on the account axis.
// ===========================================================================

#[tokio::test]
async fn balance_delta_on_notexisting_overlay_account_is_skipped() -> Result<()> {
    // A NotExisting overlay account is absent to the EVM (revm DbAccount::info()
    // returns None), even if it carries a stale info.balance. loaded_account_info
    // must treat it as cold so a BalanceDelta skips rather than applying to 1000.
    use revm::database::AccountState;
    let acct = Address::repeat_byte(0x7e);
    let mut cache = setup_cache().await?;
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

    let diff = cache.apply_update(&StateUpdate::balance_delta(
        acct,
        SlotDelta::Add(U256::from(500)),
    ));

    assert!(
        diff.accounts.is_empty(),
        "NotExisting account is EVM-absent: the delta must skip, not apply to the stale 1000"
    );
    assert_eq!(
        diff.skipped_balances,
        vec![SkippedBalanceDelta {
            address: acct,
            delta: SlotDelta::Add(U256::from(500)),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn account_patch_normalizes_zero_code_hash_across_layers() -> Result<()> {
    // write_account_info_through normalizes a ZERO code_hash to KECCAK_EMPTY so both
    // layers agree (the overlay write does this via insert_contract; the backend
    // write must too). Seed the backend (unnormalized) with a ZERO hash, then patch.
    use alloy_primitives::B256;
    use revm::primitives::KECCAK_EMPTY;
    let acct = Address::repeat_byte(0x7f);
    let mut cache = setup_cache().await?;
    cache.blockchain_db().accounts().write().insert(
        acct,
        AccountInfo {
            balance: U256::from(1),
            code_hash: B256::ZERO,
            ..Default::default()
        },
    );

    cache.apply_update(&StateUpdate::balance(acct, U256::from(2)));

    let backend_hash = cache
        .blockchain_db()
        .accounts()
        .read()
        .get(&acct)
        .map(|i| i.code_hash);
    assert_eq!(
        backend_hash,
        Some(KECCAK_EMPTY),
        "backend code_hash must be normalized to KECCAK_EMPTY, not left ZERO"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 4 — SlotMasked: cold-aware read-modify-write masked slot write.
//
// `new = (old & !mask) | (value & mask)`. Only the `mask` bits are touched; the
// rest of the packed word is preserved. Cold (slot absent from both layers) is
// skipped and surfaced in `diff.skipped_masks` (the un-masked bits are unknown).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slot_masked_sets_only_masked_bits() -> Result<()> {
    let token = Address::repeat_byte(0x11);
    let slot = U256::from(0);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    // Seed a packed word with the high bits set and the low byte clear.
    let seeded = U256::MAX - U256::from(0xFF); // 0xFF..FF00
    cache.db_mut().insert_account_storage(token, slot, seeded)?;

    // Mask the low byte only, set it to 0x42.
    let diff = cache.apply_update(&StateUpdate::slot_masked(
        token,
        slot,
        U256::from(0xFF),
        U256::from(0x42),
    ));

    let expected = seeded | U256::from(0x42); // high bits preserved, low byte = 0x42
    assert_eq!(cache.cached_storage_value(token, slot), Some(expected));
    assert_eq!(
        diff.slots,
        vec![SlotChange {
            address: token,
            slot,
            old: seeded,
            new: expected,
        }]
    );
    assert!(diff.skipped_masks.is_empty());
    Ok(())
}

#[tokio::test]
async fn slot_masked_noop_when_masked_bits_already_equal() -> Result<()> {
    let token = Address::repeat_byte(0x12);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    cache
        .db_mut()
        .insert_account_storage(token, slot, U256::from(0x42))?;

    // The masked bits already equal the target → no change.
    let diff = cache.apply_update(&StateUpdate::slot_masked(
        token,
        slot,
        U256::from(0xFF),
        U256::from(0x42),
    ));

    assert!(
        diff.is_empty(),
        "masked write that changes nothing is a no-op"
    );
    assert!(diff.skipped_masks.is_empty());
    Ok(())
}

#[tokio::test]
async fn slot_masked_cold_slot_is_skipped_and_surfaced() -> Result<()> {
    // Fresh address with no overlay account and no backend value: the slot is
    // cold. A masked write cannot know the un-masked bits, so it is skipped.
    let pool = Address::repeat_byte(0x13);
    let slot = U256::from(0);
    let mut cache = setup_cache().await?;

    let diff = cache.apply_update(&StateUpdate::slot_masked(
        pool,
        slot,
        U256::from(0xFF),
        U256::from(0x42),
    ));

    assert!(diff.slots.is_empty());
    assert_eq!(
        diff.skipped_masks,
        vec![SkippedMask {
            address: pool,
            slot,
            mask: U256::from(0xFF),
            value: U256::from(0x42),
        }]
    );
    assert!(diff.has_skipped());
    assert!(!diff.is_fully_applied());
    assert_eq!(diff.skipped_len(), 1);
    // Still cold — nothing was written.
    assert_eq!(cache.cached_storage_value(pool, slot), None);
    Ok(())
}

#[tokio::test]
async fn slot_masked_writes_through_both_layers() -> Result<()> {
    let token = Address::repeat_byte(0x14);
    let slot = U256::from(2);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    // Overlay-resident (hot) seed so an overlay account exists.
    let seeded = U256::from(0xFF00);
    cache.db_mut().insert_account_storage(token, slot, seeded)?;

    cache.apply_update(&StateUpdate::slot_masked(
        token,
        slot,
        U256::from(0x00FF),
        U256::from(0x0042),
    ));

    let expected = U256::from(0xFF42);
    assert_eq!(
        overlay_slot(&mut cache, token, slot),
        Some(expected),
        "overlay (layer 1) updated"
    );
    assert_eq!(
        backend_slot(&cache, token, slot),
        Some(expected),
        "backend (layer 2) updated"
    );
    Ok(())
}

#[tokio::test]
async fn slot_masked_full_mask_equals_absolute_on_hot_but_skips_cold() -> Result<()> {
    // mask == U256::MAX behaves like an absolute write on a hot slot, but still
    // skip-and-surfaces on a cold one (unlike StateUpdate::Slot).
    let token = Address::repeat_byte(0x15);
    let hot = U256::from(0);
    let cold_addr = Address::repeat_byte(0x16);
    let cold = U256::from(0);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    cache
        .db_mut()
        .insert_account_storage(token, hot, U256::from(7))?;

    let hot_diff = cache.apply_update(&StateUpdate::slot_masked(
        token,
        hot,
        U256::MAX,
        U256::from(99),
    ));
    assert_eq!(cache.cached_storage_value(token, hot), Some(U256::from(99)));
    assert_eq!(hot_diff.slots.len(), 1);
    assert!(hot_diff.skipped_masks.is_empty());

    let cold_diff = cache.apply_update(&StateUpdate::slot_masked(
        cold_addr,
        cold,
        U256::MAX,
        U256::from(99),
    ));
    assert!(cold_diff.slots.is_empty());
    assert_eq!(cold_diff.skipped_masks.len(), 1);
    assert_eq!(cache.cached_storage_value(cold_addr, cold), None);
    Ok(())
}

#[tokio::test]
async fn slot_masked_serde_round_trips() -> Result<()> {
    let update = StateUpdate::slot_masked(
        Address::repeat_byte(0x17),
        U256::from(5),
        U256::from(0xFF),
        U256::from(3),
    );
    let json = serde_json::to_string(&update)?;
    let back: StateUpdate = serde_json::from_str(&json)?;
    assert_eq!(update, back);

    let mut diff = StateDiff::default();
    diff.skipped_masks.push(SkippedMask {
        address: Address::repeat_byte(0x18),
        slot: U256::from(1),
        mask: U256::from(0xFF),
        value: U256::from(2),
    });
    let json = serde_json::to_string(&diff)?;
    let back: StateDiff = serde_json::from_str(&json)?;
    assert_eq!(diff, back);
    Ok(())
}
