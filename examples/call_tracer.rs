//! Call-frame tracing + composable inspectors (Phase 6 Track C).
//!
//! `CallTracer` is a `revm::Inspector` that reconstructs the **call-frame tree** of
//! a simulation — the top-level call plus every nested CALL/STATICCALL/CREATE — so
//! you can see who called whom, with what gas, and which frame reverted. It
//! attaches through the inspector-generic [`EvmOverlay::call_raw_with_inspector`],
//! and [`InspectorStack`] composes it with other inspectors (here the
//! [`TransferInspector`]) in a single pass.
//!
//! This example traces a real Multicall3 `aggregate3` (etched offline) fanning out
//! to a token, then composes a tracer with transfer-capture over an ERC-20 send.
//!
//! Fully offline (mocked provider, etched bytecode). Run with:
//!
//! ```sh
//! cargo run --example call_tracer
//! ```

#[path = "support/mock.rs"]
mod mock;

use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_sol_types::SolCall;
use anyhow::Result;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::inspector::TransferInspector;
use evm_fork_cache::multicall::{IMulticall3, MULTICALL3_ADDRESS};
use evm_fork_cache::{CallTrace, CallTracer, EvmOverlay, InspectorStack, TxConfig};
use revm::state::{AccountInfo, Bytecode};

const MULTICALL3_RUNTIME_HEX: &str = include_str!("../fixtures/multicall3_runtime.hex");

/// Etch runtime bytecode at `addr` and mark its storage local (an offline contract).
fn install_runtime(cache: &mut EvmCache, addr: Address, runtime_hex: &str) {
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

fn balance_of(owner: Address) -> Bytes {
    Bytes::from(mock::MockERC20::balanceOfCall { account: owner }.abi_encode())
}

/// Print a call-frame tree, indented by depth.
fn print_frame(t: &CallTrace) {
    let pad = "  ".repeat(t.depth + 1);
    println!(
        "{pad}{:?} {} → {}  [{:?}, gas {}]",
        t.kind, t.from, t.to, t.status, t.gas_used
    );
    for sub in &t.subcalls {
        print_frame(sub);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    let token = Address::repeat_byte(0x11);
    let caller = Address::repeat_byte(0x22);
    let alice = Address::repeat_byte(0x33);
    let bob = Address::repeat_byte(0x44);
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, caller);
    mock::install_default_account(&mut cache, alice);
    mock::install_default_account(&mut cache, bob);
    mock::install_mock_erc20(&mut cache, token);
    install_runtime(&mut cache, MULTICALL3_ADDRESS, MULTICALL3_RUNTIME_HEX);
    cache.insert_mapping_storage_slot(
        token,
        U256::from(mock::MOCK_ERC20_BALANCE_SLOT),
        alice,
        U256::from(1_000u64),
    )?;

    // ---- 1. Trace a nested Multicall3 aggregate3 -> token ----
    let calls = vec![
        IMulticall3::Call3 {
            target: token,
            allowFailure: false,
            callData: balance_of(alice),
        },
        IMulticall3::Call3 {
            target: token,
            allowFailure: false,
            callData: balance_of(bob),
        },
    ];
    let calldata = Bytes::from(IMulticall3::aggregate3Call { calls }.abi_encode());

    let mut overlay = EvmOverlay::new(cache.snapshot(), None);
    let (_result, tracer) = overlay.call_raw_with_inspector(
        caller,
        MULTICALL3_ADDRESS,
        calldata,
        &TxConfig::default(),
        CallTracer::new(),
        false,
    )?;

    println!("=== 1. call-frame tree: aggregate3 fanning out to the token ===");
    if let Some(root) = tracer.root() {
        print_frame(root);
        println!("  (root has {} subcall(s))", root.subcalls.len());
    }

    // ---- 2. Compose a tracer with transfer capture in one pass ----
    let calldata = Bytes::from(
        mock::MockERC20::transferCall {
            to: bob,
            amount: U256::from(250u64),
        }
        .abi_encode(),
    );
    let mut overlay = EvmOverlay::new(cache.snapshot(), None);
    let (_result, stack) = overlay.call_raw_with_inspector(
        alice,
        token,
        calldata,
        &TxConfig::default(),
        InspectorStack(CallTracer::new(), TransferInspector::new()),
        false,
    )?;
    let InspectorStack(tracer, transfers) = stack;

    println!("\n=== 2. InspectorStack: trace + transfer capture, one pass ===");
    if let Some(root) = tracer.root() {
        println!("  traced top-level call to {} ({:?})", root.to, root.status);
    }
    for t in &transfers.transfers {
        println!(
            "  transfer captured: {} → {} value {} (token {})",
            t.from, t.to, t.value, t.token
        );
    }

    Ok(())
}
