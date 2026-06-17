//! Phase 5 (Pillar A) acceptance tests — the **red contract** for copy-on-write
//! snapshots, authored before implementation.
//!
//! The gate is a *differential-equivalence* property: the new, memoized
//! [`EvmCache::create_snapshot`] must be **read-indistinguishable** from the
//! retained reference [`EvmCache::create_snapshot_deep_clone`] after every kind of
//! cache mutation. Because the two use different internal representations (the COW
//! snapshot shares an `Arc`-ed cold base; the reference is a full flatten), they are
//! compared *through reads only* — `storage_value`, overlay `basic`/`storage`, and a
//! `MockERC20` `balanceOf`. Any base-invalidation miss surfaces here as a failed
//! assertion, never as a silent stale read.
//!
//! Also pins the Pillar A.2 overlay-reuse contract: [`EvmOverlay::reset`] recycles
//! an overlay equivalently to a fresh one, and buffer reuse does not change results.
//!
//! All state is injected over a mocked provider — no test touches the network.
//!
//! These reference `create_snapshot_deep_clone` and `EvmOverlay::reset`, which do
//! not exist until the Phase 5 implementation lands; until then this file fails to
//! compile (red), exactly as intended.

mod common;

use std::sync::Arc;

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{Address, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::{Result, anyhow};
use revm::database::AccountState;
use revm::database_interface::Database;
use revm::state::{AccountInfo, Bytecode};

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20, setup_cache,
    transfer,
};
use evm_fork_cache::cache::{EvmCache, EvmOverlay, EvmSnapshot};
use evm_fork_cache::{SlotDelta, StateUpdate};

/// `keccak256(abi.encode(owner, slot))` — the hashed mapping slot of
/// `balanceOf[owner]`.
fn mapping_slot(owner: Address, slot: u64) -> U256 {
    let key = keccak256((owner, U256::from(slot)).abi_encode());
    U256::from_be_bytes(key.0)
}

/// Two `AccountInfo`s are equal as the EVM sees them (code identity via code_hash).
fn account_eq(a: &Option<AccountInfo>, b: &Option<AccountInfo>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            x.balance == y.balance
                && x.nonce == y.nonce
                && x.code_hash == y.code_hash
                && x.code.is_some() == y.code.is_some()
        }
        _ => false,
    }
}

/// Read `balanceOf(owner)` through an overlay (non-committing).
fn overlay_balance_of(overlay: &mut EvmOverlay, token: Address, owner: Address) -> Result<U256> {
    let call = MockERC20::balanceOfCall { account: owner };
    match overlay.call_raw(owner, token, call.abi_encode().into())? {
        revm::context::result::ExecutionResult::Success { output, .. } => Ok(
            MockERC20::balanceOfCall::abi_decode_returns(&output.into_data())?,
        ),
        other => Err(anyhow!("overlay balanceOf failed: {other:?}")),
    }
}

/// Assert the COW snapshot and the deep-clone reference are read-indistinguishable
/// across a probe set of addresses and slots. `label` identifies the mutation step.
fn assert_equivalent(cache: &mut EvmCache, addrs: &[Address], slots: &[U256], label: &str) {
    let cow = cache.create_snapshot();
    let deep = cache.create_snapshot_deep_clone();

    let mut ov_cow = EvmOverlay::new(Arc::clone(&cow), None);
    let mut ov_deep = EvmOverlay::new(Arc::clone(&deep), None);

    // Block context must match.
    assert_eq!(ov_cow.chain_id(), ov_deep.chain_id(), "{label}: chain_id");
    assert_eq!(
        ov_cow.block_number(),
        ov_deep.block_number(),
        "{label}: block_number"
    );
    assert_eq!(ov_cow.basefee(), ov_deep.basefee(), "{label}: basefee");
    assert_eq!(
        ov_cow.timestamp(),
        ov_deep.timestamp(),
        "{label}: timestamp"
    );

    for &a in addrs {
        let bc = ov_cow.basic(a).expect("cow basic");
        let bd = ov_deep.basic(a).expect("deep basic");
        assert!(
            account_eq(&bc, &bd),
            "{label}: basic mismatch at {a}: cow={bc:?} deep={bd:?}"
        );
        // Code lookup for the account's code hash must agree (spec §8.1).
        if let Some(info) = &bc {
            let h = info.code_hash;
            assert_eq!(
                ov_cow.code_by_hash(h).expect("cow code").original_bytes(),
                ov_deep.code_by_hash(h).expect("deep code").original_bytes(),
                "{label}: code_by_hash mismatch at {a} (hash {h})"
            );
        }
        for &s in slots {
            assert_eq!(
                cow.storage_value(a, s),
                deep.storage_value(a, s),
                "{label}: snapshot.storage_value mismatch at {a} / {s}"
            );
            let scow = ov_cow.storage(a, s).expect("cow storage");
            let sdeep = ov_deep.storage(a, s).expect("deep storage");
            assert_eq!(
                scow, sdeep,
                "{label}: overlay storage mismatch at {a} / {s}"
            );
        }
    }
}

/// The core gate: drive one cache through every mutation kind and assert the COW
/// snapshot stays read-identical to the deep-clone reference after each step.
#[tokio::test(flavor = "multi_thread")]
async fn cow_snapshot_matches_deep_clone_through_mutations() -> Result<()> {
    let mut cache = setup_cache().await?;

    let token = Address::repeat_byte(0x11); // cleared layer-1 account (MockERC20)
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);
    let pool = Address::repeat_byte(0x77); // layer-2-only, non-cleared
    let pool2 = Address::repeat_byte(0x88); // write-through target absent from layer 1
    let pool3 = Address::repeat_byte(0x99); // appears via simulated lazy fetch
    let ghost = Address::repeat_byte(0xEE); // becomes NotExisting

    let balance_slot = U256::from(MOCK_ERC20_BALANCE_SLOT);
    let owner_bal = mapping_slot(owner, MOCK_ERC20_BALANCE_SLOT);
    let recip_bal = mapping_slot(recipient, MOCK_ERC20_BALANCE_SLOT);

    let addrs = [token, owner, recipient, pool, pool2, pool3, ghost];
    // Probe real slots plus an always-absent slot (both must agree on None).
    let slots = [
        balance_slot,
        owner_bal,
        recip_bal,
        U256::from(0u64),
        U256::from(1u64),
        U256::from(7u64),
        U256::from(424_242u64), // never set anywhere
    ];

    // 1. Empty cache.
    assert_equivalent(&mut cache, &addrs, &slots, "empty");

    // 2. Layer-1 account inserts.
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);
    assert_equivalent(&mut cache, &addrs, &slots, "after account inserts");

    // 3. Layer-1 storage inserts (mapping balances on the cleared token).
    cache.insert_mapping_storage_slot(token, balance_slot, owner, U256::from(1_000u64))?;
    cache.insert_mapping_storage_slot(token, balance_slot, recipient, U256::ZERO)?;
    assert_equivalent(&mut cache, &addrs, &slots, "after layer-1 storage");

    // 4. write-through to an address PRESENT in layer 1 (shadowed there).
    cache.apply_updates(&[StateUpdate::slot(token, owner_bal, U256::from(2_000u64))]);
    assert_equivalent(
        &mut cache,
        &addrs,
        &slots,
        "after write-through (in layer 1)",
    );

    // 5. write-through to an address ABSENT from layer 1 (layer-2-only — the §3
    //    footgun: the base must capture it).
    cache.apply_updates(&[StateUpdate::slot(
        pool2,
        U256::from(7u64),
        U256::from(55u64),
    )]);
    assert_equivalent(
        &mut cache,
        &addrs,
        &slots,
        "after write-through (layer-2-only)",
    );

    // 6. relative native-balance delta.
    cache.apply_updates(&[StateUpdate::balance_delta(
        owner,
        SlotDelta::Add(U256::from(500)),
    )]);
    assert_equivalent(&mut cache, &addrs, &slots, "after balance delta");

    // 7. committing revm call (mutates layer 1 only — never stales the base).
    transfer(&mut cache, token, owner, recipient, U256::from(250u64))?;
    assert_equivalent(&mut cache, &addrs, &slots, "after committed transfer");

    // 8. layer-2-only cold backfill, including OVERWRITING an existing slot at an
    //    unchanged length (must still invalidate the base).
    cache.inject_storage_batch(&[(pool, U256::from(0u64), U256::from(111u64))]);
    assert_equivalent(&mut cache, &addrs, &slots, "after inject (new)");
    cache.inject_storage_batch(&[(pool, U256::from(0u64), U256::from(222u64))]);
    assert_equivalent(
        &mut cache,
        &addrs,
        &slots,
        "after inject (overwrite, same len)",
    );

    // 9. simulated UNCONTROLLED layer-2 growth (a lazy RPC fetch / prefetch writes
    //    `BlockchainDb` from inside foundry-fork-db, bypassing our write funnel):
    //    a brand-new account+slot, and a NEW slot on the existing `pool`.
    {
        let bdb = cache.unchecked_blockchain_db();
        bdb.storage()
            .write()
            .entry(pool3)
            .or_default()
            .insert(U256::from(1u64), U256::from(909u64));
        bdb.accounts().write().insert(
            pool3,
            AccountInfo {
                balance: U256::from(5u64),
                ..Default::default()
            },
        );
        // New slot on an existing base account (len changes → growth scan must catch).
        bdb.storage()
            .write()
            .entry(pool)
            .or_default()
            .insert(U256::from(1u64), U256::from(333u64));
    }
    assert_equivalent(
        &mut cache,
        &addrs,
        &slots,
        "after uncontrolled layer-2 growth",
    );

    // 10. purge.
    cache.purge_account(owner);
    assert_equivalent(&mut cache, &addrs, &slots, "after purge_account");

    // 11. NotExisting account (absent to the EVM; storage reads ZERO).
    cache.db_mut().insert_account_info(
        ghost,
        AccountInfo {
            balance: U256::from(1u64),
            ..Default::default()
        },
    );
    cache
        .db_mut()
        .cache
        .accounts
        .get_mut(&ghost)
        .expect("ghost present")
        .account_state = AccountState::NotExisting;
    assert_equivalent(&mut cache, &addrs, &slots, "after NotExisting");

    // 12. set_block (re-pin → full base rebuild path).
    cache.set_block(BlockId::Number(BlockNumberOrTag::Number(1)));
    assert_equivalent(&mut cache, &addrs, &slots, "after set_block");

    Ok(())
}

/// Escape-hatch re-honest hook (adversarial-review finding). A direct, out-of-band
/// layer-2 write through `unchecked_blockchain_db()` that overwrites an existing slot at an
/// unchanged slot count is the one mutation the count-based growth scan cannot see,
/// so the memoized base can go stale. `invalidate_snapshot_base()` must restore
/// read-equivalence with the deep-clone reference.
#[tokio::test(flavor = "multi_thread")]
async fn invalidate_snapshot_base_rehonest_after_escape_hatch_write() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x77); // layer-2-only, non-shadowed
    let slot = U256::from(0u64);

    cache.inject_storage_batch(&[(pool, slot, U256::from(111u64))]);
    let _warm = cache.create_snapshot(); // memoize the base at 111

    // Out-of-band overwrite at unchanged length (bypasses the write funnel).
    {
        let bdb = cache.unchecked_blockchain_db();
        bdb.storage()
            .write()
            .entry(pool)
            .or_default()
            .insert(slot, U256::from(222u64));
    }

    // The documented re-honest hook must make the next snapshot reflect the write.
    cache.invalidate_snapshot_base();
    let cow = cache.create_snapshot();
    let deep = cache.create_snapshot_deep_clone();
    assert_eq!(
        cow.storage_value(pool, slot),
        deep.storage_value(pool, slot),
        "invalidate_snapshot_base must re-honest the base after an out-of-band write"
    );
    assert_eq!(cow.storage_value(pool, slot), Some(U256::from(222u64)));
    Ok(())
}

/// Escape-hatch re-honest hook for account-map overwrites. A direct update of an
/// existing layer-2 account's balance/code at an unchanged account count is also
/// invisible to the count/absence growth scan, so callers must invalidate the
/// memoized base after the direct write lands.
#[tokio::test(flavor = "multi_thread")]
async fn invalidate_snapshot_base_rehonest_after_existing_account_write() -> Result<()> {
    let mut cache = setup_cache().await?;
    let account = Address::repeat_byte(0xA1);
    let code_v1 = Bytecode::new_raw(Bytes::from(vec![0x60u8, 0x01]));
    let code_v2 = Bytecode::new_raw(Bytes::from(vec![0x60u8, 0x02, 0x60, 0x03]));
    let h1 = code_v1.hash_slow();
    let h2 = code_v2.hash_slow();
    assert_ne!(h1, h2);

    let original = AccountInfo {
        balance: U256::from(111u64),
        nonce: 1,
        code_hash: h1,
        code: Some(code_v1.clone()),
        account_id: None,
    };
    let updated = AccountInfo {
        balance: U256::from(222u64),
        nonce: 2,
        code_hash: h2,
        code: Some(code_v2.clone()),
        account_id: None,
    };

    {
        let bdb = cache.unchecked_blockchain_db();
        bdb.accounts().write().insert(account, original.clone());
    }
    let warm = cache.create_snapshot(); // memoize the base with `original`.
    let mut ov_warm = EvmOverlay::new(Arc::clone(&warm), None);
    let warm_info = ov_warm
        .basic(account)
        .expect("warm basic")
        .expect("warm account");
    assert_eq!(warm_info.balance, original.balance);
    assert_eq!(warm_info.nonce, original.nonce);
    assert_eq!(warm_info.code_hash, h1);
    assert_eq!(
        ov_warm
            .code_by_hash(h1)
            .expect("warm code")
            .original_bytes(),
        code_v1.original_bytes()
    );

    // Out-of-band account overwrite at unchanged account count (bypasses the
    // write funnel and is not detectable by the growth scan).
    {
        let bdb = cache.unchecked_blockchain_db();
        let mut accounts = bdb.accounts().write();
        assert!(
            accounts.contains_key(&account),
            "test must update an existing account"
        );
        let len_before = accounts.len();
        accounts.insert(account, updated.clone());
        assert_eq!(
            accounts.len(),
            len_before,
            "test must keep the account count unchanged"
        );
    }

    cache.invalidate_snapshot_base();
    let cow = cache.create_snapshot();
    let deep = cache.create_snapshot_deep_clone();
    let mut ov_cow = EvmOverlay::new(Arc::clone(&cow), None);
    let mut ov_deep = EvmOverlay::new(Arc::clone(&deep), None);

    let cow_basic = ov_cow.basic(account).expect("cow basic");
    let deep_basic = ov_deep.basic(account).expect("deep basic");
    assert!(
        account_eq(&cow_basic, &deep_basic),
        "invalidate_snapshot_base must re-honest the base after an out-of-band account write: cow={cow_basic:?} deep={deep_basic:?}"
    );
    let cow_info = cow_basic.expect("updated cow account");
    assert_eq!(cow_info.balance, updated.balance);
    assert_eq!(cow_info.nonce, updated.nonce);
    assert_eq!(cow_info.code_hash, h2);
    assert_eq!(
        ov_cow.code_by_hash(h2).expect("cow h2").original_bytes(),
        ov_deep.code_by_hash(h2).expect("deep h2").original_bytes(),
        "updated code hash must match the deep clone"
    );
    assert_eq!(
        ov_cow.code_by_hash(h2).expect("cow h2").original_bytes(),
        code_v2.original_bytes()
    );
    assert!(
        ov_deep.code_by_hash(h1).expect("deep h1").is_empty(),
        "sanity: deep clone drops the unreferenced old hash"
    );
    assert_eq!(
        ov_cow.code_by_hash(h1).expect("cow h1").original_bytes(),
        ov_deep.code_by_hash(h1).expect("deep h1").original_bytes(),
        "the old code hash must not linger after invalidation"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn with_blockchain_db_mut_rehonest_after_storage_overwrite() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x78);
    let slot = U256::from(0u64);

    cache.inject_storage_batch(&[(pool, slot, U256::from(111u64))]);
    let _warm = cache.create_snapshot();

    cache.with_blockchain_db_mut(|bdb| {
        bdb.storage()
            .write()
            .entry(pool)
            .or_default()
            .insert(slot, U256::from(222u64));
    });

    let cow = cache.create_snapshot();
    let deep = cache.create_snapshot_deep_clone();
    assert_eq!(
        cow.storage_value(pool, slot),
        deep.storage_value(pool, slot),
        "with_blockchain_db_mut must invalidate the COW base after storage writes"
    );
    assert_eq!(cow.storage_value(pool, slot), Some(U256::from(222u64)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn with_blockchain_db_mut_rehonest_after_account_overwrite() -> Result<()> {
    let mut cache = setup_cache().await?;
    let account = Address::repeat_byte(0xA2);
    let code_v1 = Bytecode::new_raw(Bytes::from(vec![0x60u8, 0x01]));
    let code_v2 = Bytecode::new_raw(Bytes::from(vec![0x60u8, 0x02]));
    let h1 = code_v1.hash_slow();
    let h2 = code_v2.hash_slow();
    let original = AccountInfo {
        balance: U256::from(111u64),
        nonce: 1,
        code_hash: h1,
        code: Some(code_v1),
        account_id: None,
    };
    let updated = AccountInfo {
        balance: U256::from(222u64),
        nonce: 2,
        code_hash: h2,
        code: Some(code_v2.clone()),
        account_id: None,
    };

    cache.with_blockchain_db_mut(|bdb| {
        bdb.accounts().write().insert(account, original);
    });
    let _warm = cache.create_snapshot();

    cache.with_blockchain_db_mut(|bdb| {
        let mut accounts = bdb.accounts().write();
        let len_before = accounts.len();
        accounts.insert(account, updated.clone());
        assert_eq!(accounts.len(), len_before);
    });

    let cow = cache.create_snapshot();
    let deep = cache.create_snapshot_deep_clone();
    let mut ov_cow = EvmOverlay::new(Arc::clone(&cow), None);
    let mut ov_deep = EvmOverlay::new(Arc::clone(&deep), None);
    let cow_basic = ov_cow.basic(account).expect("cow basic");
    let deep_basic = ov_deep.basic(account).expect("deep basic");
    assert!(
        account_eq(&cow_basic, &deep_basic),
        "with_blockchain_db_mut must invalidate the COW base after account writes: cow={cow_basic:?} deep={deep_basic:?}"
    );
    assert_eq!(
        cow_basic.expect("updated cow account").balance,
        updated.balance
    );
    assert_eq!(
        ov_cow.code_by_hash(h2).expect("cow h2").original_bytes(),
        code_v2.original_bytes()
    );
    Ok(())
}

/// Regression (review finding P2): the COW partial rebuild must not leave a stale
/// `code_by_hash` entry when a base account is recoded or purged. Warm the base
/// with a code-bearing account, recode it in layer 2, dirty it via a controlled
/// per-address write (so `refresh_base` takes the Case-4 *partial* rebuild path,
/// not a full rebuild), and assert the old hash no longer resolves — matching the
/// deep-clone reference, which rebuilds its code index from current accounts.
#[tokio::test(flavor = "multi_thread")]
async fn cow_code_index_matches_deep_clone_after_base_account_recoded() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0xc0);
    let code_v1 = Bytecode::new_raw(Bytes::from(vec![0x60u8, 0x01]));
    let code_v2 = Bytecode::new_raw(Bytes::from(vec![0x60u8, 0x02, 0x60, 0x03]));
    let h1 = code_v1.hash_slow();
    let h2 = code_v2.hash_slow();
    assert_ne!(h1, h2);

    // Seed a code-bearing account (code_v1) directly into the cold base (layer 2),
    // then re-honest the memoized base.
    let put_account = |cache: &EvmCache, code: &Bytecode, hash| {
        cache.unchecked_blockchain_db().accounts().write().insert(
            contract,
            AccountInfo {
                balance: U256::from(1u64),
                nonce: 1,
                code_hash: hash,
                code: Some(code.clone()),
                account_id: None,
            },
        );
    };
    put_account(&cache, &code_v1, h1);
    cache.invalidate_snapshot_base();
    let warm = cache.create_snapshot(); // base now indexes h1 -> code_v1
    let mut ov_warm = EvmOverlay::new(Arc::clone(&warm), None);
    assert_eq!(
        ov_warm
            .code_by_hash(h1)
            .expect("warm code")
            .original_bytes(),
        code_v1.original_bytes(),
        "warm snapshot must resolve the seeded code"
    );

    // Recode the base account to code_v2 (out-of-band), then dirty `contract` via a
    // controlled per-address write so the next snapshot takes the Case-4 partial
    // rebuild — exactly the path that previously failed to prune the old hash.
    put_account(&cache, &code_v2, h2);
    cache.apply_updates(&[StateUpdate::slot(
        contract,
        U256::from(0u64),
        U256::from(9u64),
    )]);

    let cow = cache.create_snapshot();
    let deep = cache.create_snapshot_deep_clone();
    let mut ov_cow = EvmOverlay::new(Arc::clone(&cow), None);
    let mut ov_deep = EvmOverlay::new(Arc::clone(&deep), None);

    // The new hash resolves identically...
    assert_eq!(
        ov_cow.code_by_hash(h2).expect("cow h2").original_bytes(),
        ov_deep.code_by_hash(h2).expect("deep h2").original_bytes(),
        "new code hash must match the deep clone"
    );
    // ...and the now-unreferenced old hash must NOT linger in the COW base: both
    // resolve to empty (the deep clone never had it after the recode).
    assert!(
        ov_deep.code_by_hash(h1).expect("deep h1").is_empty(),
        "sanity: deep clone drops the unreferenced old hash"
    );
    assert_eq!(
        ov_cow.code_by_hash(h1).expect("cow h1").original_bytes(),
        ov_deep.code_by_hash(h1).expect("deep h1").original_bytes(),
        "COW must not return stale bytecode for the recoded account's old hash"
    );
    Ok(())
}

/// COW must not alias: a snapshot taken earlier is unaffected by a later mutation
/// of the same address (the memoized base is rebuilt copy-on-write, not mutated).
#[tokio::test(flavor = "multi_thread")]
async fn earlier_snapshot_unaffected_by_later_base_mutation() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x77);
    let slot = U256::from(3u64);

    cache.inject_storage_batch(&[(pool, slot, U256::from(100u64))]);
    let early = cache.create_snapshot();
    assert_eq!(early.storage_value(pool, slot), Some(U256::from(100u64)));

    // Mutate the same base slot, then take a second snapshot.
    cache.inject_storage_batch(&[(pool, slot, U256::from(200u64))]);
    let late = cache.create_snapshot();

    assert_eq!(
        early.storage_value(pool, slot),
        Some(U256::from(100u64)),
        "the earlier snapshot must still read the pre-mutation value"
    );
    assert_eq!(
        late.storage_value(pool, slot),
        Some(U256::from(200u64)),
        "the later snapshot reflects the mutation"
    );
    Ok(())
}

/// `EvmOverlay::reset` clears the dirty layer so the overlay reads the pristine
/// snapshot again — equivalent to a fresh overlay.
#[tokio::test(flavor = "multi_thread")]
async fn overlay_reset_restores_pristine_snapshot_reads() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x44);
    let owner = Address::repeat_byte(0x55);
    let recipient = Address::repeat_byte(0x66);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(10_000u64),
    )?;
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        recipient,
        U256::ZERO,
    )?;

    let snapshot = cache.create_snapshot();
    let mut overlay = EvmOverlay::new(Arc::clone(&snapshot), None);

    // Mutate the dirty layer with a committing transfer through the overlay.
    overlay.simulate_with_transfer_tracking(
        owner,
        token,
        MockERC20::transferCall {
            to: recipient,
            amount: U256::from(4_000u64),
        }
        .abi_encode()
        .into(),
        owner,
        Some([token]),
        true, // commit into the overlay's dirty layer
    )?;
    assert_eq!(
        overlay_balance_of(&mut overlay, token, owner)?,
        U256::from(6_000u64),
        "post-transfer dirty-layer balance"
    );

    // reset() drops the dirty layer; reads see the pristine snapshot again.
    overlay.reset();
    assert_eq!(
        overlay_balance_of(&mut overlay, token, owner)?,
        U256::from(10_000u64),
        "after reset the overlay reads the pristine snapshot"
    );

    // A reset-recycled overlay matches a brand-new overlay across two sims.
    let mut fresh = EvmOverlay::new(Arc::clone(&snapshot), None);
    assert_eq!(
        overlay_balance_of(&mut overlay, token, owner)?,
        overlay_balance_of(&mut fresh, token, owner)?,
        "recycled overlay == fresh overlay"
    );
    Ok(())
}

/// Buffer reuse must not change results: repeated calls on one overlay return the
/// same value as the first (the reusable shared-memory buffer is cleared, not
/// corrupted, between builds).
#[tokio::test(flavor = "multi_thread")]
async fn overlay_buffer_reuse_is_result_stable() -> Result<()> {
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
        U256::from(7_777u64),
    )?;

    let snapshot = cache.create_snapshot();
    let mut overlay = EvmOverlay::new(snapshot, None);

    let first = overlay_balance_of(&mut overlay, token, owner)?;
    for _ in 0..16 {
        assert_eq!(
            overlay_balance_of(&mut overlay, token, owner)?,
            first,
            "repeated calls reusing the buffer must be stable"
        );
    }
    assert_eq!(first, U256::from(7_777u64));
    Ok(())
}

/// Compile-time guards: the COW representation keeps the thread-safety contract.
#[test]
fn snapshot_send_sync_overlay_send() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<EvmSnapshot>();
    assert_send_sync::<Arc<EvmSnapshot>>();
    assert_send::<EvmOverlay>();
}
