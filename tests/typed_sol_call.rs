//! Offline integration tests for typed `SolCall` helpers on `EvmCache`.

mod common;

use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_sol_types::sol;
use anyhow::Result;
use evm_fork_cache::cache::{EvmCache, TxConfig};
use revm::state::{AccountInfo, Bytecode};

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, balance_of, install_default_account, install_mock_erc20,
    setup_cache,
};

sol! {
    function who() external returns (address);
    function paid() external payable returns (uint256);
    function willRevert() external returns (uint256);
    function malformed() external returns (uint256);
}

fn install_runtime(cache: &mut EvmCache, addr: Address, runtime_hex: &str) -> Result<()> {
    let code = Bytecode::new_raw(Bytes::from(hex::decode(runtime_hex)?));
    let code_hash = code.hash_slow();
    cache.db_mut().insert_account_info(
        addr,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code: Some(code),
            code_hash,
            account_id: None,
        },
    );
    cache
        .db_mut()
        .replace_account_storage(addr, Default::default())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn call_sol_decodes_mock_erc20_balance() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let expected = U256::from(12_345u64);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        expected,
    )?;

    let balance = cache.call_sol(token, MockERC20::balanceOfCall { account: owner })?;
    assert_eq!(balance, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn call_sol_from_threads_msg_sender() -> Result<()> {
    let mut cache = setup_cache().await?;
    let sender = Address::repeat_byte(0x33);
    let target = Address::repeat_byte(0x44);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, sender);
    // CALLER; MSTORE(0); RETURN(0, 32)
    install_runtime(&mut cache, target, "3360005260206000f3")?;

    let returned = cache.call_sol_from(sender, target, whoCall {})?;
    assert_eq!(returned, sender);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn call_sol_with_threads_tx_value() -> Result<()> {
    let mut cache = setup_cache().await?;
    let sender = Address::repeat_byte(0x55);
    let target = Address::repeat_byte(0x66);
    let value = U256::from(98_765u64);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, sender);
    // CALLVALUE; MSTORE(0); RETURN(0, 32)
    install_runtime(&mut cache, target, "3460005260206000f3")?;

    let tx = TxConfig {
        value,
        ..Default::default()
    };
    let returned = cache.call_sol_with(sender, target, paidCall {}, &tx)?;
    assert_eq!(returned, value);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn call_sol_from_does_not_commit_state() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x77);
    let owner = Address::repeat_byte(0x88);
    let recipient = Address::repeat_byte(0x99);
    let initial = U256::from(1_000u64);
    let amount = U256::from(250u64);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        initial,
    )?;
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        recipient,
        U256::ZERO,
    )?;

    let ok = cache.call_sol_from(
        owner,
        token,
        MockERC20::transferCall {
            to: recipient,
            amount,
        },
    )?;
    assert!(ok);
    assert_eq!(balance_of(&mut cache, token, owner)?, initial);
    assert_eq!(balance_of(&mut cache, token, recipient)?, U256::ZERO);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn transact_sol_commits_state() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0xaa);
    let owner = Address::repeat_byte(0xbb);
    let recipient = Address::repeat_byte(0xcc);
    let initial = U256::from(1_000u64);
    let amount = U256::from(250u64);

    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, owner);
    install_default_account(&mut cache, recipient);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        owner,
        initial,
    )?;
    cache.insert_mapping_storage_slot(
        token,
        U256::from(MOCK_ERC20_BALANCE_SLOT),
        recipient,
        U256::ZERO,
    )?;

    let ok = cache.transact_sol(
        owner,
        token,
        MockERC20::transferCall {
            to: recipient,
            amount,
        },
        &TxConfig::default(),
    )?;
    assert!(ok);
    assert_eq!(balance_of(&mut cache, token, owner)?, initial - amount);
    assert_eq!(balance_of(&mut cache, token, recipient)?, amount);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reverting_sol_call_reports_function_and_target() -> Result<()> {
    let mut cache = setup_cache().await?;
    let target = Address::repeat_byte(0xdd);

    install_default_account(&mut cache, Address::ZERO);
    // REVERT(0, 0)
    install_runtime(&mut cache, target, "60006000fd")?;

    let err = cache.call_sol(target, willRevertCall {}).unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("willRevert()"), "{message}");
    assert!(message.contains(&format!("{target:?}")), "{message}");
    assert!(message.contains("did not succeed"), "{message}");
    assert!(message.contains("Revert"), "{message}");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_return_data_reports_decode_context() -> Result<()> {
    let mut cache = setup_cache().await?;
    let target = Address::repeat_byte(0xee);

    install_default_account(&mut cache, Address::ZERO);
    // STOP: succeeds with empty return data, which cannot decode as uint256.
    install_runtime(&mut cache, target, "00")?;

    let err = cache.call_sol(target, malformedCall {}).unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("malformed()"), "{message}");
    assert!(message.contains(&format!("{target:?}")), "{message}");
    assert!(message.contains("output_len=0"), "{message}");
    assert!(message.contains("failed to decode"), "{message}");

    Ok(())
}
