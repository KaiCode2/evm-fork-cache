//! Manager-authored red-green acceptance tests for Phase 6 Track A+B:
//! ordered multi-transaction bundle simulation over cumulative state, revert
//! policy, and coinbase/payment accounting.
//!
//! These describe the public contract before the implementation exists. The
//! implementation agent must make them pass WITHOUT weakening, skipping, or
//! rewriting them; if a test encodes a wrong assumption about EVM/mock behavior
//! (as opposed to the feature contract), surface it to the manager with a
//! justification rather than silently changing it.
//!
//! Fully offline (mocked provider, injected state).
#![cfg(feature = "reactive")]

mod common;

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;
use revm::context::result::ExecutionResult;
use revm::state::AccountInfo;

use common::{MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20, setup_cache};
use evm_fork_cache::{
    BundleOptions, BundleTx, EvmOverlay, GasAccounting, RevertPolicy, TxConfig,
};

/// transfer(to, amount) calldata.
fn transfer_calldata(to: Address, amount: u64) -> Bytes {
    Bytes::from(
        MockERC20::transferCall {
            to,
            amount: U256::from(amount),
        }
        .abi_encode(),
    )
}

/// Read balanceOf(owner) on `token` through an overlay (so we observe the
/// overlay's committed cumulative state, not the cache).
fn overlay_balance(overlay: &mut EvmOverlay, token: Address, owner: Address) -> Result<U256> {
    let calldata = Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode());
    match overlay.call_raw(owner, token, calldata)? {
        ExecutionResult::Success { output, .. } => {
            Ok(MockERC20::balanceOfCall::abi_decode_returns(&output.into_data())?)
        }
        other => anyhow::bail!("balanceOf failed: {other:?}"),
    }
}

/// A token with `alice` funded; returns (token, alice, bob, carol).
async fn token_with_funded_alice() -> Result<(evm_fork_cache::EvmCache, Address, Address, Address, Address)> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let alice = Address::repeat_byte(0x22);
    let bob = Address::repeat_byte(0x33);
    let carol = Address::repeat_byte(0x44);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, alice);
    install_default_account(&mut cache, bob);
    install_default_account(&mut cache, carol);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(token, U256::from(MOCK_ERC20_BALANCE_SLOT), alice, U256::from(1_000u64))?;
    Ok((cache, token, alice, bob, carol))
}

/// AB1 — ordered cumulative state: tx 2 (bob -> carol) only nets correctly if it
/// observed tx 1's write (alice -> bob). Final balances pin cumulative semantics.
#[tokio::test(flavor = "multi_thread")]
async fn bundle_applies_txs_over_cumulative_state() -> Result<()> {
    let (mut cache, token, alice, bob, carol) = token_with_funded_alice().await?;
    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);

    let txs = vec![
        BundleTx::new(alice, token, transfer_calldata(bob, 100)),
        BundleTx::new(bob, token, transfer_calldata(carol, 30)),
    ];
    let result = overlay.simulate_bundle(
        &txs,
        &BundleOptions {
            commit: true,
            ..Default::default()
        },
    )?;

    assert!(result.succeeded, "atomic bundle should succeed");
    assert_eq!(result.per_tx.len(), 2);
    assert!(result.per_tx.iter().all(|o| !o.reverted));

    // Cumulative: alice 1000-100=900, bob 0+100-30=70, carol 0+30=30.
    assert_eq!(overlay_balance(&mut overlay, token, alice)?, U256::from(900u64));
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::from(70u64));
    assert_eq!(overlay_balance(&mut overlay, token, carol)?, U256::from(30u64));
    Ok(())
}

/// AB2 — atomic revert: the 2nd tx (bad selector -> revert) aborts the whole
/// bundle; nothing persists and `succeeded` is false.
#[tokio::test(flavor = "multi_thread")]
async fn atomic_bundle_reverts_whole_on_failure() -> Result<()> {
    let (mut cache, token, alice, bob, _carol) = token_with_funded_alice().await?;
    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);

    let txs = vec![
        BundleTx::new(alice, token, transfer_calldata(bob, 100)),
        // Unknown selector -> the contract reverts.
        BundleTx::new(alice, token, Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])),
    ];
    let result = overlay.simulate_bundle(
        &txs,
        &BundleOptions {
            revert_policy: RevertPolicy::Atomic,
            commit: true,
            ..Default::default()
        },
    )?;

    assert!(!result.succeeded, "atomic bundle must fail when a tx reverts");
    assert!(result.per_tx.last().map(|o| o.reverted).unwrap_or(false));
    // The whole bundle rolled back: alice keeps her full balance.
    assert_eq!(overlay_balance(&mut overlay, token, alice)?, U256::from(1_000u64));
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::ZERO);
    Ok(())
}

/// AB3 — allow-reverts: the same bundle with the failing tx whitelisted; tx 0
/// persists, tx 1 is rolled back individually, and the bundle succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn allow_reverts_keeps_prior_effects() -> Result<()> {
    let (mut cache, token, alice, bob, _carol) = token_with_funded_alice().await?;
    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);

    let txs = vec![
        BundleTx::new(alice, token, transfer_calldata(bob, 100)),
        BundleTx::new(alice, token, Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])),
    ];
    let result = overlay.simulate_bundle(
        &txs,
        &BundleOptions {
            revert_policy: RevertPolicy::AllowReverts(vec![1]),
            commit: true,
            ..Default::default()
        },
    )?;

    assert!(result.succeeded, "bundle should succeed when the revert is allowed");
    assert!(!result.per_tx[0].reverted);
    assert!(result.per_tx[1].reverted);
    // tx 0 persisted, tx 1 rolled back.
    assert_eq!(overlay_balance(&mut overlay, token, alice)?, U256::from(900u64));
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::from(100u64));
    Ok(())
}

/// AB4 — direct coinbase payment: a native-value transfer to the beneficiary
/// (Address::ZERO by default) with gas_price = 0 is captured as `coinbase_payment`,
/// identical under Raw and Mainnet (no gas credit to subtract).
#[tokio::test(flavor = "multi_thread")]
async fn direct_coinbase_payment_is_captured() -> Result<()> {
    let mut cache = setup_cache().await?;
    let searcher = Address::repeat_byte(0x22);
    // Beneficiary defaults to Address::ZERO; install it so it is not lazily fetched.
    install_default_account(&mut cache, Address::ZERO);
    cache
        .db_mut()
        .insert_account_info(searcher, AccountInfo { balance: U256::from(10_000u64), ..Default::default() });

    let pay = U256::from(777u64);
    let tx = BundleTx::with_config(
        searcher,
        Address::ZERO, // the default beneficiary
        Bytes::new(),
        TxConfig {
            value: pay,
            gas_price: Some(0),
            ..Default::default()
        },
    );

    let snapshot = cache.create_snapshot();
    for accounting in [GasAccounting::Raw, GasAccounting::Mainnet] {
        let mut overlay = EvmOverlay::new(snapshot.clone(), None);
        let result = overlay.simulate_bundle(
            std::slice::from_ref(&tx),
            &BundleOptions {
                gas_accounting: accounting,
                commit: true,
                ..Default::default()
            },
        )?;
        assert!(result.succeeded);
        assert_eq!(result.coinbase_payment, pay, "accounting={accounting:?}");
    }
    Ok(())
}

/// AB5 — Mainnet vs Raw gas accounting: with a base fee set and a tx priced above
/// it, Mainnet payment equals Raw minus the burned base fee (gas_used × basefee),
/// and is strictly smaller than Raw.
#[tokio::test(flavor = "multi_thread")]
async fn mainnet_accounting_subtracts_burned_basefee() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let caller = Address::repeat_byte(0x22);
    install_default_account(&mut cache, Address::ZERO);
    install_mock_erc20(&mut cache, token);
    // Fund the caller's native balance so gas accounting is well-defined.
    cache.db_mut().insert_account_info(
        caller,
        AccountInfo { balance: U256::from(10u64).pow(U256::from(20u64)), ..Default::default() },
    );

    let basefee: u128 = 1_000_000_000; // 1 gwei
    let priority: u128 = 2_000_000_000; // 2 gwei
    cache.set_basefee(U256::from(basefee));

    let tx = BundleTx::with_config(
        caller,
        token,
        Bytes::from(MockERC20::balanceOfCall { account: caller }.abi_encode()),
        TxConfig {
            gas_price: Some(basefee + priority),
            ..Default::default()
        },
    );

    let snapshot = cache.create_snapshot();
    let run = |accounting| -> Result<evm_fork_cache::BundleResult> {
        let mut overlay = EvmOverlay::new(snapshot.clone(), None);
        Ok(overlay.simulate_bundle(
            std::slice::from_ref(&tx),
            &BundleOptions { gas_accounting: accounting, ..Default::default() },
        )?)
    };
    let raw = run(GasAccounting::Raw)?;
    let mainnet = run(GasAccounting::Mainnet)?;

    assert!(raw.gas_used > 0);
    let burned = U256::from(raw.gas_used) * U256::from(basefee);
    assert_eq!(
        mainnet.coinbase_payment,
        raw.coinbase_payment.saturating_sub(burned),
        "Mainnet = Raw - gas_used*basefee"
    );
    assert!(mainnet.coinbase_payment < raw.coinbase_payment);
    Ok(())
}

/// AB6 — commit semantics: commit=false leaves the overlay unchanged; commit=true
/// persists cumulative state to the next overlay call.
#[tokio::test(flavor = "multi_thread")]
async fn commit_flag_controls_overlay_persistence() -> Result<()> {
    let (mut cache, token, alice, bob, _carol) = token_with_funded_alice().await?;
    let snapshot = cache.create_snapshot();

    // commit = false: overlay unchanged afterward.
    let mut overlay = EvmOverlay::new(snapshot.clone(), None);
    overlay.simulate_bundle(
        &[BundleTx::new(alice, token, transfer_calldata(bob, 100))],
        &BundleOptions { commit: false, ..Default::default() },
    )?;
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::ZERO);

    // commit = true: change visible to the next call on the same overlay.
    let mut overlay = EvmOverlay::new(snapshot, None);
    overlay.simulate_bundle(
        &[BundleTx::new(alice, token, transfer_calldata(bob, 100))],
        &BundleOptions { commit: true, ..Default::default() },
    )?;
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::from(100u64));
    Ok(())
}
