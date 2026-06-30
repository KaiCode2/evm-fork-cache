//! Manager-authored red-green acceptance tests for Phase 6 Track C: the
//! call-frame tracer (`CallTracer`) and the generalized public inspector seam
//! (`EvmOverlay::call_raw_with_inspector` + `InspectorStack`).
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

use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_sol_types::SolCall;
use anyhow::Result;
use revm::state::{AccountInfo, Bytecode};

use common::{
    MOCK_ERC20_BALANCE_SLOT, MockERC20, install_default_account, install_mock_erc20, setup_cache,
};
use evm_fork_cache::inspector::TransferInspector;
use evm_fork_cache::multicall::{IMulticall3, MULTICALL3_ADDRESS};
use evm_fork_cache::{CallStatus, CallTracer, EvmOverlay, InspectorStack, TxConfig};

const MULTICALL3_RUNTIME_HEX: &str = include_str!("../fixtures/multicall3_runtime.hex");

/// Etch runtime bytecode at `addr` and mark its storage local (offline contract).
fn install_runtime(cache: &mut evm_fork_cache::EvmCache, addr: Address, runtime_hex: &str) {
    let code = Bytecode::new_raw(Bytes::from(
        hex::decode(runtime_hex.trim()).expect("valid hex"),
    ));
    let code_hash = code.hash_slow();
    cache.db_mut().insert_account_info(
        addr,
        AccountInfo {
            code: Some(code),
            code_hash,
            ..Default::default()
        },
    );
    cache
        .db_mut()
        .replace_account_storage(addr, Default::default())
        .expect("mark storage local");
}

fn balance_of_calldata(owner: Address) -> Bytes {
    Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode())
}

/// C1 — a single top-level call yields a root frame with the right from/to/input
/// and a Success status.
#[tokio::test(flavor = "multi_thread")]
async fn tracer_captures_single_top_level_frame() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let caller = Address::repeat_byte(0x22);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, caller);
    install_mock_erc20(&mut cache, token);

    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);
    let calldata = balance_of_calldata(caller);
    let (_result, tracer) = overlay.call_raw_with_inspector(
        caller,
        token,
        calldata.clone(),
        &TxConfig::default(),
        CallTracer::new(),
        false,
    )?;

    let root = tracer
        .into_trace()
        .expect("a top-level call produces a root frame");
    assert_eq!(root.from, caller);
    assert_eq!(root.to, token);
    assert_eq!(root.input, calldata);
    assert_eq!(root.status, CallStatus::Success);
    Ok(())
}

/// C2 — a Multicall3 `aggregate3` that targets the token produces a root frame
/// (caller -> Multicall3) with a nested subcall to the token.
#[tokio::test(flavor = "multi_thread")]
async fn tracer_captures_nested_subcalls() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let caller = Address::repeat_byte(0x22);
    let owner = Address::repeat_byte(0x33);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, caller);
    install_mock_erc20(&mut cache, token);
    install_runtime(&mut cache, MULTICALL3_ADDRESS, MULTICALL3_RUNTIME_HEX);

    let calls = vec![IMulticall3::Call3 {
        target: token,
        allowFailure: false,
        callData: balance_of_calldata(owner),
    }];
    let calldata = Bytes::from(IMulticall3::aggregate3Call { calls }.abi_encode());

    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);
    let (_result, tracer) = overlay.call_raw_with_inspector(
        caller,
        MULTICALL3_ADDRESS,
        calldata,
        &TxConfig::default(),
        CallTracer::new(),
        false,
    )?;

    let root = tracer.into_trace().expect("root frame");
    assert_eq!(root.to, MULTICALL3_ADDRESS);
    assert!(
        root.subcalls.iter().any(|c| c.to == token),
        "the aggregate3 frame should contain a subcall to the token, got {:#?}",
        root.subcalls
    );
    Ok(())
}

/// C3 — a call into a contract that reverts (unknown selector) is attributed as a
/// reverted frame.
#[tokio::test(flavor = "multi_thread")]
async fn tracer_attributes_reverts() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    let caller = Address::repeat_byte(0x22);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, caller);
    install_mock_erc20(&mut cache, token);

    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);
    let (_result, tracer) = overlay.call_raw_with_inspector(
        caller,
        token,
        Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]), // unknown selector -> revert
        &TxConfig::default(),
        CallTracer::new(),
        false,
    )?;

    let root = tracer.into_trace().expect("root frame");
    assert_eq!(root.status, CallStatus::Revert);
    Ok(())
}

/// C4 — `InspectorStack` runs a `CallTracer` and a `TransferInspector` in one
/// pass: both produce their independent results.
#[tokio::test(flavor = "multi_thread")]
async fn inspector_stack_composes_tracer_and_transfer() -> Result<()> {
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

    let calldata = Bytes::from(
        MockERC20::transferCall {
            to: bob,
            amount: U256::from(50u64),
        }
        .abi_encode(),
    );

    let mut overlay = EvmOverlay::new(cache.create_snapshot(), None);
    let (_result, stack) = overlay.call_raw_with_inspector(
        alice,
        token,
        calldata,
        &TxConfig::default(),
        InspectorStack(CallTracer::new(), TransferInspector::new()),
        false,
    )?;

    let InspectorStack(tracer, transfer) = stack;
    assert!(
        tracer.root().is_some(),
        "tracer should have captured a frame"
    );
    assert!(
        transfer
            .transfers
            .iter()
            .any(|t| t.from == alice && t.to == bob && t.token == token),
        "transfer inspector should have captured the ERC-20 Transfer"
    );
    Ok(())
}
