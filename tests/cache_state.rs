//! Offline integration tests for `EvmCache` state manipulation: balance
//! overrides via storage-slot scanning, snapshot/restore, two-layer cache
//! purging, and contract deployment/etching.
//!
//! All state is injected directly over a mocked provider, so these tests run
//! without any network access. Ported from the original out-of-crate suite so
//! the coverage travels with the crate.

mod common;

use alloy_primitives::{Address, Bytes, I256, U256};
use alloy_sol_types::SolValue;
use anyhow::{Context, Result};
use revm::state::{AccountInfo, Bytecode};

use common::{
    MOCK_ERC20_BALANCE_SLOT, balance_of, install_default_account, install_mock_erc20,
    mock_erc20_creation_code, mock_erc20_runtime, setup_cache, transfer,
};
use evm_fork_cache::cache::TxConfig;

/// Deterministic CREATE address for `Address::ZERO` at nonce 0:
/// `keccak256(rlp([ZERO, 0]))[12..]`.
const CREATE_ADDRESS_ZERO_NONCE_0: Address = Address::new(alloy_primitives::hex!(
    "bd770416a3345f91e4b34576cb804a576fa48eb1"
));

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_restore_reverts_token_state() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);

    let balance_slot = U256::from(MOCK_ERC20_BALANCE_SLOT);
    let initial_balance = U256::from(1_000_000u64);
    cache.insert_mapping_storage_slot(token, balance_slot, owner, initial_balance)?;
    cache.insert_mapping_storage_slot(token, balance_slot, recipient, U256::ZERO)?;

    assert_eq!(balance_of(&mut cache, token, owner)?, initial_balance);

    let snapshot = cache.snapshot();

    transfer(&mut cache, token, owner, recipient, U256::from(123u64))?;
    assert_eq!(
        balance_of(&mut cache, token, owner)?,
        initial_balance - U256::from(123u64)
    );

    cache.restore(snapshot);
    assert_eq!(balance_of(&mut cache, token, owner)?, initial_balance);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn call_raw_with_carries_native_value() -> Result<()> {
    let mut cache = setup_cache().await?;
    let sender = Address::repeat_byte(0x11);
    let recipient = Address::repeat_byte(0x22);

    // Both accounts start empty (unfunded); balance checks are disabled in the
    // simulator, so a value-bearing call still goes through.
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, sender);
    install_default_account(&mut cache, recipient);

    let value = U256::from(1_000_000_000u64);
    let tx = TxConfig {
        value,
        ..Default::default()
    };
    let result = cache.call_raw_with(sender, recipient, Bytes::new(), true, &tx)?;
    assert!(
        result.is_success(),
        "value transfer should succeed: {result:?}"
    );

    // The recipient is credited the native value.
    let recipient_balance = cache
        .db_mut()
        .cache
        .accounts
        .get(&recipient)
        .map(|a| a.info.balance)
        .unwrap_or_default();
    assert_eq!(
        recipient_balance, value,
        "recipient should receive the value"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn simulation_reports_balance_deltas() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);

    let balance_slot = U256::from(MOCK_ERC20_BALANCE_SLOT);
    cache.insert_mapping_storage_slot(token, balance_slot, owner, U256::from(1_000u64))?;
    cache.insert_mapping_storage_slot(token, balance_slot, recipient, U256::ZERO)?;

    let balance_before = balance_of(&mut cache, token, owner)?;
    transfer(&mut cache, token, owner, recipient, U256::from(250u64))?;
    let balance_after = balance_of(&mut cache, token, owner)?;

    let delta = I256::from_raw(balance_after) - I256::from_raw(balance_before);
    assert_eq!(delta, -I256::from_raw(U256::from(250u64)));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn set_erc20_balance_with_slot_scan_finds_balance_slot() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x90);
    let owner = Address::repeat_byte(0x91);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    // Seed the real balance so the scan has a value to perturb.
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(123u64),
    )?;
    assert_eq!(balance_of(&mut cache, token, owner)?, U256::from(123u64));

    let target_balance = U256::from(10_000u64);
    let updated = cache.set_erc20_balance_with_slot_scan(token, owner, target_balance, 8)?;
    assert!(updated, "slot scan should find slot 3 and update balance");
    assert_eq!(balance_of(&mut cache, token, owner)?, target_balance);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn set_erc20_balance_with_slot_scan_honors_max_slot_bound() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x92);
    let owner = Address::repeat_byte(0x93);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    // Real balance slot is 3; scanning only 0..=2 must fail and leave the
    // original balance untouched.
    let initial_balance = U256::from(456u64);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        initial_balance,
    )?;
    let updated = cache.set_erc20_balance_with_slot_scan(token, owner, U256::from(999u64), 2)?;
    assert!(
        !updated,
        "slot scan should fail when slot 3 is out of range"
    );
    assert_eq!(balance_of(&mut cache, token, owner)?, initial_balance);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn seed_erc20_balance_slots_skips_scan() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x94);
    let owner = Address::repeat_byte(0x95);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    // Pre-seed the known balance slot.
    cache.seed_erc20_balance_slots([(token, U256::from(MOCK_ERC20_BALANCE_SLOT))]);

    // With max_slot=0 the scan would never reach slot 3, but the seed bypasses scanning.
    let target = U256::from(42_000u64);
    let updated = cache.set_erc20_balance_with_slot_scan(token, owner, target, 0)?;
    assert!(updated, "seeded slot should bypass scan and succeed");
    assert_eq!(balance_of(&mut cache, token, owner)?, target);

    Ok(())
}

/// Regression test: both cache layers (the `CacheDB` overlay and the
/// `BlockchainDb` backend) must be purged together. Clearing only the backend
/// leaves stale data in the overlay; `purge_pool_storage` clears both.
#[tokio::test(flavor = "multi_thread")]
async fn two_layer_cache_staleness_requires_full_purge() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x77);
    let owner = Address::repeat_byte(0x88);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    let balance_slot = U256::from(MOCK_ERC20_BALANCE_SLOT);
    let initial_balance = U256::from(1000u64);
    cache.insert_mapping_storage_slot(token, balance_slot, owner, initial_balance)?;

    // Reading via the EVM populates the CacheDB overlay (layer 1).
    assert_eq!(balance_of(&mut cache, token, owner)?, initial_balance);
    let overlay_slots = cache.cache_db_storage_slot_count(token);
    assert!(
        overlay_slots > 0,
        "overlay should hold slots after EVM read"
    );

    // Seed the BlockchainDb backend (layer 2) directly so both layers hold data.
    cache.inject_storage_batch(&[(token, U256::from(7), U256::from(1))]);
    assert!(
        cache.pool_storage_slot_count(token) > 0,
        "backend should hold the seeded slot"
    );

    // Clearing ONLY the backend leaves the overlay serving stale data.
    {
        let mut storage = cache.blockchain_db().storage().write();
        storage.remove(&token);
    }
    assert_eq!(
        balance_of(&mut cache, token, owner)?,
        initial_balance,
        "backend-only purge left stale data in the overlay"
    );
    assert_eq!(
        cache.cache_db_storage_slot_count(token),
        overlay_slots,
        "overlay was not cleared by a backend-only purge"
    );

    // Re-seed the backend, then purge BOTH layers and confirm each is cleared.
    cache.inject_storage_batch(&[(token, U256::from(7), U256::from(1))]);
    assert!(cache.pool_storage_slot_count(token) > 0);
    let backend_cleared = cache.purge_pool_storage(token);
    assert!(
        backend_cleared > 0,
        "purge_pool_storage should report cleared backend slots"
    );
    assert_eq!(
        cache.cache_db_storage_slot_count(token),
        0,
        "overlay should be empty after purge_pool_storage"
    );
    assert_eq!(
        cache.pool_storage_slot_count(token),
        0,
        "backend should be empty after purge_pool_storage"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn purge_all_storage_clears_both_layers() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0xAA);
    let owner = Address::repeat_byte(0xBB);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);

    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(5000u64),
    )?;
    let _ = balance_of(&mut cache, token, owner)?;
    assert!(cache.cache_db_storage_slot_count(token) > 0);

    cache.purge_all_storage();
    assert_eq!(
        cache.cache_db_storage_slot_count(token),
        0,
        "purge_all_storage should clear the overlay"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn purge_pool_slots_is_selective() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0xCC);

    install_default_account(&mut cache, Address::ZERO);
    install_mock_erc20(&mut cache, contract);

    let slot_a = U256::from(10);
    let slot_b = U256::from(20);
    let slot_c = U256::from(30);
    cache
        .db_mut()
        .insert_account_storage(contract, slot_a, U256::from(111))?;
    cache
        .db_mut()
        .insert_account_storage(contract, slot_b, U256::from(222))?;
    cache
        .db_mut()
        .insert_account_storage(contract, slot_c, U256::from(333))?;
    assert_eq!(cache.cache_db_storage_slot_count(contract), 3);

    // Purge only slot_a and slot_c.
    cache.purge_pool_slots(contract, &[slot_a, slot_c]);
    assert_eq!(cache.cache_db_storage_slot_count(contract), 1);

    let remaining = cache
        .db_mut()
        .cache
        .accounts
        .get(&contract)
        .and_then(|a| a.storage.get(&slot_b))
        .copied();
    assert_eq!(remaining, Some(U256::from(222)), "slot_b should survive");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn deploy_contract_mock_erc20_is_callable() -> Result<()> {
    let mut cache = setup_cache().await?;

    let mut creation_code = mock_erc20_creation_code();
    let constructor_args = (
        String::from("Test Token"),
        String::from("TEST"),
        U256::from(18u8),
    )
        .abi_encode_params();
    creation_code.extend_from_slice(&constructor_args);

    install_default_account(&mut cache, Address::ZERO);
    // Pre-insert the deterministic CREATE address so the mock provider isn't queried.
    install_default_account(&mut cache, CREATE_ADDRESS_ZERO_NONCE_0);

    let deployed = cache.deploy_contract(Address::ZERO, Bytes::from(creation_code))?;
    assert_ne!(deployed, Address::ZERO);

    let account = cache
        .db_mut()
        .cache
        .accounts
        .get(&deployed)
        .expect("deployed account should exist");
    assert!(
        account.info.code.as_ref().is_some_and(|c| !c.is_empty()),
        "deployed contract should have non-empty bytecode"
    );

    // A fresh token reports a zero balance.
    assert_eq!(balance_of(&mut cache, deployed, Address::ZERO)?, U256::ZERO);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn override_account_code_preserves_storage() -> Result<()> {
    let mut cache = setup_cache().await?;
    let target = Address::repeat_byte(0xAA);
    let owner = Address::repeat_byte(0xBB);

    // Target starts with MockERC20 code, a non-zero ETH balance/nonce, and a token balance.
    let runtime = mock_erc20_runtime();
    let code_hash = runtime.hash_slow();
    cache.db_mut().insert_account_info(
        target,
        AccountInfo {
            balance: U256::from(42u64),
            nonce: 5,
            code: Some(runtime),
            code_hash,
            account_id: None,
        },
    );
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);

    cache.insert_mapping_storage_slot(
        target,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(1000u64),
    )?;
    assert_eq!(balance_of(&mut cache, target, owner)?, U256::from(1000u64));

    // Deploy a fresh MockERC20 to use as the override source.
    let mut creation_code = mock_erc20_creation_code();
    let constructor_args = (
        String::from("Test Token V2"),
        String::from("TEST2"),
        U256::from(18u8),
    )
        .abi_encode_params();
    creation_code.extend_from_slice(&constructor_args);
    install_default_account(&mut cache, CREATE_ADDRESS_ZERO_NONCE_0);
    let source = cache.deploy_contract(Address::ZERO, Bytes::from(creation_code))?;

    cache.override_account_code(source, target)?;

    // Storage, ETH balance, and nonce all survive a bytecode-only override.
    assert_eq!(
        balance_of(&mut cache, target, owner)?,
        U256::from(1000u64),
        "storage should be preserved"
    );
    let target_account = cache
        .db_mut()
        .cache
        .accounts
        .get(&target)
        .expect("target exists");
    assert_eq!(target_account.info.balance, U256::from(42u64));
    assert_eq!(target_account.info.nonce, 5);

    let source_hash = cache
        .db_mut()
        .cache
        .accounts
        .get(&source)
        .map(|a| a.info.code_hash)
        .unwrap();
    let target_hash = cache
        .db_mut()
        .cache
        .accounts
        .get(&target)
        .map(|a| a.info.code_hash)
        .unwrap();
    assert_eq!(target_hash, source_hash, "code hash should match source");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn override_account_code_requires_known_target_unless_create_requested() -> Result<()> {
    let mut cache = setup_cache().await?;
    let source = Address::repeat_byte(0x12);
    let target = Address::repeat_byte(0x34);

    let source_code = Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]));
    let source_hash = source_code.hash_slow();
    cache.db_mut().insert_account_info(
        source,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code: Some(source_code),
            code_hash: source_hash,
            account_id: None,
        },
    );

    assert!(
        cache.override_account_code(source, target).is_err(),
        "strict override should fail for an unknown target"
    );
    assert!(
        !cache.db_mut().cache.accounts.contains_key(&target),
        "strict override should not create a target after a backend miss"
    );

    cache.override_or_create_account_code(source, target)?;
    let target_account = cache
        .db_mut()
        .cache
        .accounts
        .get(&target)
        .context("explicit create should insert target")?;
    assert_eq!(target_account.info.code_hash, source_hash);
    assert_eq!(target_account.info.balance, U256::ZERO);
    assert_eq!(target_account.info.nonce, 0);

    Ok(())
}
