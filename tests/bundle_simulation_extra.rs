//! Agent-authored supplementary tests for Phase 6 Track A+B bundle simulation.
//!
//! These cover edge cases the manager acceptance suite
//! (`tests/bundle_simulation.rs`) does not pin: an empty bundle, the
//! `AllowReverts` atomic-fallback when a non-whitelisted index reverts,
//! `commit = false` leaving an `AllowReverts` bundle isolated, and the
//! `EvmCache::simulate_bundle` convenience never mutating the cache.
//!
//! Fully offline (mocked provider, injected state).
#![cfg(feature = "reactive")]

mod common;

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;
use revm::context::result::ExecutionResult;

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20, setup_cache,
};
use evm_fork_cache::{BundleOptions, BundleTx, EvmCache, EvmOverlay, RevertPolicy};

fn transfer_calldata(to: Address, amount: u64) -> Bytes {
    Bytes::from(
        MockERC20::transferCall {
            to,
            amount: U256::from(amount),
        }
        .abi_encode(),
    )
}

fn overlay_balance(overlay: &mut EvmOverlay, token: Address, owner: Address) -> Result<U256> {
    let calldata = Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode());
    match overlay.call_raw(owner, token, calldata)? {
        ExecutionResult::Success { output, .. } => Ok(
            MockERC20::balanceOfCall::abi_decode_returns(&output.into_data())?,
        ),
        other => anyhow::bail!("balanceOf failed: {other:?}"),
    }
}

async fn token_with_funded_alice() -> Result<(EvmCache, Address, Address, Address)> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let alice = Address::repeat_byte(0x22);
    let bob = Address::repeat_byte(0x33);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, alice);
    install_default_account(&mut cache, bob);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        alice,
        U256::from(1_000u64),
    )?;
    Ok((cache, token, alice, bob))
}

/// An empty bundle is a no-op success: zero outcomes, zero gas, zero payment.
#[tokio::test(flavor = "multi_thread")]
async fn empty_bundle_succeeds_as_noop() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);

    let result = overlay.simulate_bundle(&[], &BundleOptions::default())?;
    assert!(result.succeeded);
    assert!(result.per_tx.is_empty());
    assert_eq!(result.gas_used, 0);
    assert_eq!(result.coinbase_payment, U256::ZERO);
    Ok(())
}

/// `AllowReverts` that does NOT whitelist the failing index behaves like
/// `Atomic`: the whole bundle rolls back and `succeeded == false`.
#[tokio::test(flavor = "multi_thread")]
async fn allow_reverts_non_whitelisted_index_aborts_atomically() -> Result<()> {
    let (mut cache, token, alice, bob) = token_with_funded_alice().await?;
    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);

    let txs = vec![
        BundleTx::new(alice, token, transfer_calldata(bob, 100)),
        // Reverts, but only index 0 is whitelisted -> atomic fallback.
        BundleTx::new(alice, token, Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])),
    ];
    let result = overlay.simulate_bundle(
        &txs,
        &BundleOptions {
            revert_policy: RevertPolicy::AllowReverts(vec![0]),
            commit: true,
        },
    )?;

    assert!(!result.succeeded);
    assert_eq!(result.per_tx.len(), 2);
    assert!(result.per_tx[1].reverted);
    // Whole bundle rolled back: alice keeps her full balance, bob gets nothing.
    assert_eq!(
        overlay_balance(&mut overlay, token, alice)?,
        U256::from(1_000u64)
    );
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::ZERO);
    Ok(())
}

/// `commit = false` with `AllowReverts` leaves the overlay unchanged even though
/// the bundle "succeeded" (the kept tx is rolled back by the outer revert).
#[tokio::test(flavor = "multi_thread")]
async fn allow_reverts_commit_false_is_isolated() -> Result<()> {
    let (mut cache, token, alice, bob) = token_with_funded_alice().await?;
    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);

    let txs = vec![
        BundleTx::new(alice, token, transfer_calldata(bob, 100)),
        BundleTx::new(alice, token, Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])),
    ];
    let result = overlay.simulate_bundle(
        &txs,
        &BundleOptions {
            revert_policy: RevertPolicy::AllowReverts(vec![1]),
            commit: false,
        },
    )?;

    assert!(result.succeeded);
    assert!(!result.per_tx[0].reverted);
    assert!(result.per_tx[1].reverted);
    // commit = false: even tx 0's effect is reverted out of the overlay.
    assert_eq!(
        overlay_balance(&mut overlay, token, alice)?,
        U256::from(1_000u64)
    );
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::ZERO);
    Ok(())
}

/// `EvmCache::simulate_bundle` runs on a transient overlay and never mutates the
/// cache, even with `commit = true`.
#[tokio::test(flavor = "multi_thread")]
async fn cache_simulate_bundle_does_not_mutate_cache() -> Result<()> {
    let (mut cache, token, alice, bob) = token_with_funded_alice().await?;

    let result = cache.simulate_bundle(
        &[BundleTx::new(alice, token, transfer_calldata(bob, 100))],
        &BundleOptions {
            commit: true,
            ..Default::default()
        },
    )?;
    assert!(result.succeeded);
    assert_eq!(result.per_tx.len(), 1);

    // The cache is unchanged: a fresh snapshot still shows alice's full balance.
    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);
    assert_eq!(
        overlay_balance(&mut overlay, token, alice)?,
        U256::from(1_000u64)
    );
    assert_eq!(overlay_balance(&mut overlay, token, bob)?, U256::ZERO);
    Ok(())
}
