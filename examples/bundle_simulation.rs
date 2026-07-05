//! Bundle simulation + coinbase accounting (Phase 6 Track A+B).
//!
//! An MEV bundle is an *ordered* sequence of transactions evaluated over
//! **cumulative** state — each transaction sees the writes of the ones before it —
//! with the miner's payment accounted at the end. This is the shape a searcher
//! evaluates (victim + backrun, sandwich front/back), which a single isolated
//! `call` cannot express. This example shows three things:
//!
//! 1. **A bundle over cumulative state**: two dependent token transfers plus a
//!    native coinbase tip, with per-transaction outcomes and the miner payment.
//! 2. **The revert policy**: the same shape with a failing transaction, run
//!    `Atomic` (the whole bundle reverts) vs `AllowReverts` (the failure is rolled
//!    back individually and the rest still stands).
//! 3. **Base-fee-aware payment**: with a base fee set, the coinbase payment is the
//!    priority fee only — revm burns the base fee in-EVM (EIP-1559), so the figure
//!    is the honest miner payment automatically.
//!
//! Fully offline (mocked provider, injected state). Run with:
//!
//! ```sh
//! cargo run --example bundle_simulation
//! ```

#[path = "support/mock.rs"]
mod mock;

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;
use evm_fork_cache::cache::EvmOverlay;
use evm_fork_cache::{BundleOptions, BundleTx, RevertPolicy, TxConfig};
use revm::context::result::ExecutionResult;
use revm::state::AccountInfo;

fn transfer(to: Address, amount: u64) -> Bytes {
    Bytes::from(
        mock::MockERC20::transferCall {
            to,
            amount: U256::from(amount),
        }
        .abi_encode(),
    )
}

fn token_balance(overlay: &mut EvmOverlay, token: Address, owner: Address) -> Result<U256> {
    let cd = Bytes::from(mock::MockERC20::balanceOfCall { account: owner }.abi_encode());
    match overlay.call_raw(owner, token, cd)? {
        ExecutionResult::Success { output, .. } => Ok(
            mock::MockERC20::balanceOfCall::abi_decode_returns(&output.into_data())?,
        ),
        other => anyhow::bail!("balanceOf failed: {other:?}"),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    let token = Address::repeat_byte(0x11);
    let alice = Address::repeat_byte(0x22);
    let bob = Address::repeat_byte(0x33);
    let carol = Address::repeat_byte(0x44);
    // Address::ZERO is the default block beneficiary (the "coinbase").
    mock::install_default_account(&mut cache, Address::ZERO);
    for a in [alice, bob, carol] {
        mock::install_default_account(&mut cache, a);
    }
    mock::install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(mock::MOCK_ERC20_BALANCE_SLOT),
        alice,
        U256::from(1_000u64),
    )?;

    // ---- 1. A bundle over cumulative state, with a coinbase tip ----
    // tx0: alice -> bob 500   tx1: bob -> carol 200 (only valid after tx0)
    // tx2: alice tips the coinbase 750 wei (a direct miner payment).
    let tip = U256::from(750u64);
    let bundle = vec![
        BundleTx::new(alice, token, transfer(bob, 500)),
        BundleTx::new(bob, token, transfer(carol, 200)),
        BundleTx::with_config(
            alice,
            Address::ZERO,
            Bytes::new(),
            TxConfig {
                value: tip,
                gas_price: Some(0),
                ..Default::default()
            },
        ),
    ];

    let mut overlay = EvmOverlay::new(cache.snapshot(), None);
    let result = overlay.simulate_bundle(
        &bundle,
        &BundleOptions {
            commit: true,
            ..Default::default()
        },
    )?;

    println!("=== 1. cumulative bundle (Atomic) ===");
    println!(
        "  succeeded: {}   total gas: {}",
        result.succeeded, result.gas_used
    );
    for (i, o) in result.per_tx.iter().enumerate() {
        println!("    tx{i}: reverted={}  gas={}", o.reverted, o.gas_used);
    }
    println!("  coinbase payment: {} wei", result.coinbase_payment);
    println!(
        "  final balances -> alice: {}  bob: {}  carol: {}  (cumulative: tx1 saw tx0)",
        token_balance(&mut overlay, token, alice)?,
        token_balance(&mut overlay, token, bob)?,
        token_balance(&mut overlay, token, carol)?,
    );

    // ---- 2. Revert policy: Atomic vs AllowReverts ----
    // A bundle whose 2nd tx reverts (unknown selector).
    let with_failure = vec![
        BundleTx::new(alice, token, transfer(bob, 100)),
        BundleTx::new(alice, token, Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])),
    ];
    let snapshot = cache.snapshot();

    let mut atomic = EvmOverlay::new(snapshot.clone(), None);
    let r = atomic.simulate_bundle(
        &with_failure,
        &BundleOptions {
            commit: true,
            ..Default::default()
        },
    )?;
    println!("\n=== 2. revert policy ===");
    println!(
        "  Atomic:       succeeded={}  bob balance after: {}  (whole bundle rolled back)",
        r.succeeded,
        token_balance(&mut atomic, token, bob)?,
    );

    let mut allow = EvmOverlay::new(snapshot, None);
    let r = allow.simulate_bundle(
        &with_failure,
        &BundleOptions {
            revert_policy: RevertPolicy::AllowReverts(vec![1]),
            commit: true,
        },
    )?;
    println!(
        "  AllowReverts: succeeded={}  bob balance after: {}  (tx0 kept, tx1 rolled back)",
        r.succeeded,
        token_balance(&mut allow, token, bob)?,
    );

    // ---- 3. Base-fee-aware payment under a base fee ----
    // Fund a searcher's native balance and price a tx above the base fee.
    let searcher = Address::repeat_byte(0x55);
    cache.db_mut().insert_account_info(
        searcher,
        AccountInfo {
            balance: U256::from(10u64).pow(U256::from(20u64)),
            ..Default::default()
        },
    );
    let basefee: u128 = 1_000_000_000; // 1 gwei, burned on mainnet
    let priority: u128 = 2_000_000_000; // 2 gwei, the miner's actual cut
    cache.set_basefee(U256::from(basefee));
    let priced = vec![BundleTx::with_config(
        searcher,
        token,
        Bytes::from(mock::MockERC20::balanceOfCall { account: searcher }.abi_encode()),
        TxConfig {
            gas_price: Some(basefee + priority),
            ..Default::default()
        },
    )];

    let result = cache.simulate_bundle(&priced, &BundleOptions::default())?;
    println!("\n=== 3. base-fee-aware payment (basefee 1 gwei, gas_price 3 gwei) ===");
    println!("  gas used: {}", result.gas_used);
    println!(
        "  coinbase payment: {} wei  (= gas_used × 2 gwei priority; the 1 gwei base fee is burned, not paid)",
        result.coinbase_payment,
    );

    Ok(())
}
