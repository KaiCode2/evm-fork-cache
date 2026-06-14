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
    MOCK_ERC20_BALANCE_SLOT, failing_fetcher, install_default_account, install_mock_erc20,
    setup_cache, stub_fetcher,
};
use evm_fork_cache::cache::{EvmCache, EvmOverlay};

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
