//! Offline integration tests for the Multicall3 helpers.
//!
//! These pin the network-free behavior end to end: the empty-batch short-circuit,
//! the result-decoding helpers, the documented batch constants, and — by etching
//! the real Multicall3 runtime at its canonical address — the live `aggregate3`
//! build/execute/decode path, including input-order results and `allowFailure`
//! partial-result semantics.

mod common;

use alloy_primitives::{Address, Bytes, U256, hex, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::Result;
use revm::state::{AccountInfo, Bytecode};

use common::{MOCK_ERC20_BALANCE_SLOT, install_default_account, install_mock_erc20, setup_cache};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::multicall::{
    IMulticall3, MAX_BATCH_SIZE, MULTICALL3_ADDRESS, MulticallBatch, decode_result,
    execute_batched, try_decode_result,
};

/// Real Multicall3 deployed (runtime) bytecode, fetched from mainnet. Multicall3
/// is deployed at the same address ([`MULTICALL3_ADDRESS`]) on virtually all EVM
/// chains, so etching it locally reproduces the real `aggregate3` behavior.
const MULTICALL3_RUNTIME_HEX: &str = include_str!("../fixtures/multicall3_runtime.hex");

sol! {
    function getValue() external returns (uint256);
    function balanceOf(address account) external returns (uint256);
}

/// `keccak256(abi.encode(owner, balanceSlot))` — the MockERC20 balance mapping slot.
fn balance_slot(owner: Address) -> U256 {
    U256::from_be_bytes(keccak256((owner, U256::from(MOCK_ERC20_BALANCE_SLOT)).abi_encode()).0)
}

/// Etch deployed runtime bytecode at `addr` and mark its storage local, so an
/// unseeded slot reads as zero rather than hitting the mocked RPC backend.
fn install_runtime(cache: &mut EvmCache, addr: Address, runtime_hex: &str) {
    let code = Bytecode::new_raw(Bytes::from(
        hex::decode(runtime_hex.trim()).expect("valid runtime hex"),
    ));
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
        .replace_account_storage(addr, Default::default())
        .expect("clear etched storage");
}

/// An empty batch returns empty results without invoking the EVM, on all three
/// entry points.
#[tokio::test(flavor = "multi_thread")]
async fn empty_batch_short_circuits() -> Result<()> {
    let mut cache = setup_cache().await?;

    let batch = MulticallBatch::new();
    assert!(batch.is_empty());
    assert!(batch.execute(&mut cache)?.is_empty());

    let (results, access) = batch.execute_tracked(&mut cache)?;
    assert!(results.is_empty());
    assert!(access.slots.is_empty() && access.accounts.is_empty());

    let batched = execute_batched(&mut cache, std::iter::empty::<(Address, Bytes, bool)>())?;
    assert!(batched.is_empty());

    Ok(())
}

/// `add` and `add_call` both append a call; length tracks the call count.
#[test]
fn batch_len_tracks_added_calls() {
    let target = Address::repeat_byte(0x11);
    let mut batch = MulticallBatch::with_capacity(2);
    assert_eq!(batch.len(), 0);

    batch.add(target, getValueCall {}.abi_encode().into(), true);
    batch.add_call(target, getValueCall {}, false);
    assert_eq!(batch.len(), 2);
    assert!(!batch.is_empty());
}

/// `decode_result` returns the typed value for a successful result and errors on
/// a failed one; `try_decode_result` mirrors this with `Option`.
#[test]
fn decode_result_honors_success_flag() {
    let ok = IMulticall3::Result {
        success: true,
        returnData: U256::from(42u64).abi_encode().into(),
    };
    let decoded = decode_result::<getValueCall>(&ok).expect("successful result decodes");
    assert_eq!(decoded, U256::from(42u64));
    assert_eq!(
        try_decode_result::<getValueCall>(&ok),
        Some(U256::from(42u64))
    );

    let failed = IMulticall3::Result {
        success: false,
        returnData: Bytes::new(),
    };
    assert!(
        decode_result::<getValueCall>(&failed).is_err(),
        "a failed call cannot be decoded"
    );
    assert_eq!(try_decode_result::<getValueCall>(&failed), None);
}

/// A successful result whose payload is undecodable errors (and yields `None`),
/// distinct from the `success == false` case.
#[test]
fn decode_result_rejects_garbage_payload() {
    let garbage = IMulticall3::Result {
        success: true,
        returnData: Bytes::from_static(&[0x01, 0x02, 0x03]),
    };
    assert!(decode_result::<getValueCall>(&garbage).is_err());
    assert_eq!(try_decode_result::<getValueCall>(&garbage), None);
}

#[test]
fn max_batch_size_constant() {
    assert_eq!(MAX_BATCH_SIZE, 200);
}

/// Etching the real Multicall3 runtime lets the live `aggregate3` path run fully
/// offline: results come back one-per-call in input order and decode to the
/// seeded values.
#[tokio::test(flavor = "multi_thread")]
async fn aggregate3_executes_offline_in_input_order() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_runtime(&mut cache, MULTICALL3_ADDRESS, MULTICALL3_RUNTIME_HEX);

    let token = Address::repeat_byte(0x22);
    let owner = Address::repeat_byte(0x33);
    let other = Address::repeat_byte(0x44);
    install_default_account(&mut cache, Address::ZERO);
    install_mock_erc20(&mut cache, token);
    // Seed the layer-1 CacheDB (the layer the EVM reads from) so the StorageCleared
    // mock reports it; a backend-only seed would read as zero via the
    // account-state-aware read path.
    cache
        .db_mut()
        .insert_account_storage(token, balance_slot(owner), U256::from(5_000u64))?;

    let mut batch = MulticallBatch::new();
    batch.add_call(token, balanceOfCall { account: owner }, false);
    batch.add_call(token, balanceOfCall { account: other }, false);

    let results = batch.execute(&mut cache)?;
    assert_eq!(results.len(), 2, "one result per input call");
    assert!(results[0].success && results[1].success);
    // Input order is preserved: owner first (5000), other second (0).
    assert_eq!(
        decode_result::<balanceOfCall>(&results[0])?,
        U256::from(5_000u64)
    );
    assert_eq!(decode_result::<balanceOfCall>(&results[1])?, U256::ZERO);
    Ok(())
}

/// `allowFailure = true` lets a reverting call surface as `success = false`
/// alongside successful calls; `allowFailure = false` makes the whole batch revert
/// (an `Err`). `execute_tracked` captures the touched accounts.
#[tokio::test(flavor = "multi_thread")]
async fn aggregate3_allow_failure_partial_results_and_strict_revert() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_runtime(&mut cache, MULTICALL3_ADDRESS, MULTICALL3_RUNTIME_HEX);

    let token = Address::repeat_byte(0x22);
    let owner = Address::repeat_byte(0x33);
    install_default_account(&mut cache, Address::ZERO);
    install_mock_erc20(&mut cache, token);
    cache
        .db_mut()
        .insert_account_storage(token, balance_slot(owner), U256::from(7u64))?;

    // An unknown selector reverts in the MockERC20 (no matching function).
    let reverting = Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]);

    let mut batch = MulticallBatch::new();
    batch.add_call(token, balanceOfCall { account: owner }, false);
    batch.add(token, reverting.clone(), true); // allowed to fail
    let (results, access) = batch.execute_tracked(&mut cache)?;

    assert_eq!(results.len(), 2);
    assert!(results[0].success);
    assert_eq!(
        decode_result::<balanceOfCall>(&results[0])?,
        U256::from(7u64)
    );
    assert!(
        !results[1].success,
        "the reverting call surfaces as success = false, not an Err"
    );
    assert!(
        access.accounts.contains(&token),
        "execute_tracked must capture the token account the inner call touched"
    );

    // The same revert with allow_failure = false fails the entire aggregate3.
    let mut strict = MulticallBatch::new();
    strict.add(token, reverting, false);
    assert!(
        strict.execute(&mut cache).is_err(),
        "a non-allowed revert reverts the whole batch"
    );
    Ok(())
}
