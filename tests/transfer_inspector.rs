//! End-to-end integration tests for transfer-tracking simulation.
//!
//! The inline unit tests in `src/inspector.rs` populate the inspector by hand;
//! these drive it through a real EVM execution — `MockERC20.transfer` emits a
//! `Transfer` event that the [`TransferInspector`](evm_fork_cache::inspector::TransferInspector)
//! captures during [`EvmCache::simulate_with_transfer_tracking`] — and assert the
//! reconstructed balance deltas, log capture, token filtering, non-committing
//! semantics, and the revert path. All state is injected over a mocked provider.

mod common;

use alloy_primitives::{Address, I256, U256};
use alloy_sol_types::SolCall;
use anyhow::Result;

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20, setup_cache,
};
use evm_fork_cache::errors::RevertReason;

/// Build the calldata for `transfer(to, amount)`.
fn transfer_calldata(to: Address, amount: U256) -> alloy_primitives::Bytes {
    MockERC20::transferCall { to, amount }.abi_encode().into()
}

/// A transfer the inspector observes yields a signed delta for the sender, the
/// emitted `Transfer` log is captured, and the populated access list reflects the
/// touched token. The non-committing sim leaves the on-chain balance unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn transfer_tracking_reports_sender_delta_and_logs() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let recipient = Address::repeat_byte(0x33);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);

    let balance_slot = U256::from(MOCK_ERC20_BALANCE_SLOT);
    cache.insert_mapping_storage_slot(token, balance_slot, owner, U256::from(1_000u64))?;
    cache.insert_mapping_storage_slot(token, balance_slot, recipient, U256::ZERO)?;

    let result = cache.simulate_with_transfer_tracking(
        owner,
        token,
        transfer_calldata(recipient, U256::from(250u64)),
        owner,
        Some([token]),
        false, // non-committing
    )?;

    // Owner sent 250 of `token`.
    assert_eq!(
        result.token_deltas.get(&token),
        Some(&I256::try_from(-250i64).unwrap()),
        "sender's delta is -amount"
    );
    // The Transfer log was captured.
    assert_eq!(result.logs.len(), 1, "exactly one Transfer log emitted");
    // The inspector path also captures the EIP-2930 access list.
    assert!(
        result
            .access_list
            .0
            .iter()
            .any(|item| item.address == token),
        "access list includes the token account"
    );

    // Non-committing: the on-chain balance is untouched.
    assert_eq!(
        common::balance_of(&mut cache, token, owner)?,
        U256::from(1_000u64),
        "a non-committing sim must not change cache state"
    );

    Ok(())
}

/// The recipient's perspective sees the mirror-image positive delta.
#[tokio::test(flavor = "multi_thread")]
async fn transfer_tracking_reports_recipient_delta() -> Result<()> {
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
        U256::from(500u64),
    )?;

    // `owner` argument selects whose deltas to compute — here, the recipient.
    let result = cache.simulate_with_transfer_tracking(
        owner,
        token,
        transfer_calldata(recipient, U256::from(120u64)),
        recipient,
        None::<Vec<Address>>,
        false,
    )?;

    assert_eq!(
        result.token_deltas.get(&token),
        Some(&I256::try_from(120i64).unwrap()),
        "recipient's delta is +amount"
    );

    Ok(())
}

/// The `tokens` filter restricts which tokens appear in the deltas: a transfer in
/// a token absent from the filter set is dropped from the result.
#[tokio::test(flavor = "multi_thread")]
async fn transfer_tracking_token_filter_excludes_other_tokens() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x77);
    let other_token = Address::repeat_byte(0x78);
    let owner = Address::repeat_byte(0x88);
    let recipient = Address::repeat_byte(0x89);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        U256::from(1_000u64),
    )?;

    // Filter to a different token than the one transferred.
    let result = cache.simulate_with_transfer_tracking(
        owner,
        token,
        transfer_calldata(recipient, U256::from(250u64)),
        owner,
        Some([other_token]),
        false,
    )?;

    assert!(
        result.token_deltas.is_empty(),
        "the transferred token is filtered out, leaving no deltas"
    );

    Ok(())
}

/// An insufficient-balance transfer reverts; the typed error surfaces the decoded
/// `Error("balance")` reason rather than a generic failure.
#[tokio::test(flavor = "multi_thread")]
async fn transfer_tracking_surfaces_revert_reason() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0xAA);
    let owner = Address::repeat_byte(0xBB);
    let recipient = Address::repeat_byte(0xCC);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);
    // owner has zero balance, so transferring reverts in `_transfer`'s require.

    let err = cache
        .simulate_with_transfer_tracking(
            owner,
            token,
            transfer_calldata(recipient, U256::from(100u64)),
            owner,
            None::<Vec<Address>>,
            false,
        )
        .expect_err("transfer with no balance must revert");

    assert!(err.is_revert(), "expected a revert, got {err:?}");
    let revert = err.as_revert().expect("revert payload");
    assert_eq!(
        revert.reason(),
        &RevertReason::Error("balance".to_string()),
        "MockERC20._transfer reverts with require(.., \"balance\")"
    );

    Ok(())
}
