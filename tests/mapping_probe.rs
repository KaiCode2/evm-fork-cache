//! Integration tests for trace-based hash-derived slot discovery (v0.2.1):
//! layout-agnostic balance-slot discovery, layout-aware writes, the rewired
//! `set_erc20_balance_with_slot_scan`, nested-mapping tracing, and the
//! `track_erc20_balances` fan-out primitive.

mod common;

use alloy_primitives::{Address, B256, Bytes, U256, address, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::Result;
use common::{MockERC20, install_default_account, install_mock_erc20, setup_cache};
use evm_fork_cache::CallTracer;
use evm_fork_cache::cache::TxConfig;
use evm_fork_cache::mapping_probe::{SlotLayout, TrackedMapping};
use revm::context::result::ExecutionResult;
use revm::state::{AccountInfo, Bytecode};

const ALICE: Address = address!("00000000000000000000000000000000000000A1");
const BOB: Address = address!("00000000000000000000000000000000000000B2");
const CAROL: Address = address!("00000000000000000000000000000000000000C3");
const SOLADY_SEED: u32 = 0x87a2_11a2;

// --- tiny bytecode builders for non-Solidity layouts (no compiler needed) ---

fn push1(v: &mut Vec<u8>, b: u8) {
    v.push(0x60);
    v.push(b);
}
fn push4(v: &mut Vec<u8>, x: u32) {
    v.push(0x63);
    v.extend_from_slice(&x.to_be_bytes());
}

/// `balanceOf(addr)` reading `keccak256(slot ‖ addr)` — Vyper byte order.
fn vyper_runtime(slot: u8) -> Vec<u8> {
    let mut c = Vec::new();
    push1(&mut c, slot); // mstore(0x00, slot)
    push1(&mut c, 0x00);
    c.push(0x52);
    push1(&mut c, 0x04); // owner = calldataload(0x04)
    c.push(0x35);
    push1(&mut c, 0x20); // mstore(0x20, owner)
    c.push(0x52);
    push1(&mut c, 0x40); // keccak256(0x00, 0x40)
    push1(&mut c, 0x00);
    c.push(0x20);
    c.push(0x54); // sload
    push1(&mut c, 0x00); // return(0x00, 0x20)
    c.push(0x52);
    push1(&mut c, 0x20);
    push1(&mut c, 0x00);
    c.push(0xf3);
    c
}

/// `balanceOf(addr)` reading Solady's packed `keccak256(0x0c, 0x20)` slot.
fn solady_runtime(seed: u32) -> Vec<u8> {
    let mut c = Vec::new();
    push4(&mut c, seed); // mstore(0x0c, seed)
    push1(&mut c, 0x0c);
    c.push(0x52);
    push1(&mut c, 0x04); // owner = calldataload(0x04)
    c.push(0x35);
    push1(&mut c, 0x00); // mstore(0x00, owner)
    c.push(0x52);
    push1(&mut c, 0x20); // keccak256(0x0c, 0x20)
    push1(&mut c, 0x0c);
    c.push(0x20);
    c.push(0x54); // sload
    push1(&mut c, 0x00);
    c.push(0x52);
    push1(&mut c, 0x20);
    push1(&mut c, 0x00);
    c.push(0xf3);
    c
}

fn install_runtime(cache: &mut evm_fork_cache::cache::EvmCache, addr: Address, code: Vec<u8>) {
    let bytecode = Bytecode::new_raw(Bytes::from(code));
    let code_hash = bytecode.hash_slow();
    cache.db_mut().insert_account_info(
        addr,
        AccountInfo {
            code: Some(bytecode),
            code_hash,
            ..Default::default()
        },
    );
    cache
        .db_mut()
        .replace_account_storage(addr, Default::default())
        .unwrap();
}

fn vyper_slot(slot: u8, owner: Address) -> B256 {
    let mut pre = [0u8; 64];
    pre[31] = slot;
    pre[32..64].copy_from_slice(owner.into_word().as_slice());
    keccak256(pre)
}

fn solady_slot(seed: u32, owner: Address) -> B256 {
    let mut pre = [0u8; 32];
    pre[0..20].copy_from_slice(&owner.into_array());
    pre[28..32].copy_from_slice(&seed.to_be_bytes());
    keccak256(pre)
}

// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn discovers_solidity_balance_slot() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    install_default_account(&mut cache, ALICE);
    install_mock_erc20(&mut cache, token);
    cache.insert_mapping_storage_slot(token, U256::from(3u64), ALICE, U256::from(500u64))?;

    let access = cache
        .discover_erc20_balance_slot(token, ALICE)?
        .expect("balance slot discovered");

    assert_eq!(access.layout, SlotLayout::SolidityMapping);
    assert_eq!(access.base_slot, U256::from(3u64));
    assert!(access.keyed_by(ALICE.into_word()));
    assert_eq!(access.value, U256::from(500u64));
    // The exact slot matches the canonical Solidity mapping location.
    let expected = keccak256((ALICE, U256::from(3u64)).abi_encode());
    assert_eq!(access.slot, expected);
    Ok(())
}

/// With `max_slot = 0` the legacy scan could only try slot 0 — so a success here
/// proves trace discovery (not the scan) found the real slot 3.
#[tokio::test(flavor = "multi_thread")]
async fn set_balance_uses_discovery_not_scan() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    install_default_account(&mut cache, ALICE);
    install_mock_erc20(&mut cache, token);

    let ok = cache.set_erc20_balance_with_slot_scan(token, ALICE, U256::from(1_000_000u64), 0)?;
    assert!(ok, "discovery should locate slot 3 despite max_slot=0");

    let bal = MockERC20::balanceOfCall { account: ALICE };
    let got: U256 = cache.call_sol(token, bal)?;
    assert_eq!(got, U256::from(1_000_000u64));
    Ok(())
}

/// The old scan writes probes in Solidity order only, so it can NEVER override a
/// Vyper-layout token. Trace discovery + layout-aware write fixes this.
#[tokio::test(flavor = "multi_thread")]
async fn overrides_vyper_layout_token() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x22);
    install_default_account(&mut cache, ALICE);
    install_runtime(&mut cache, token, vyper_runtime(2));

    let access = cache
        .discover_erc20_balance_slot(token, ALICE)?
        .expect("vyper slot discovered");
    assert_eq!(access.layout, SlotLayout::VyperMapping);
    assert_eq!(access.base_slot, U256::from(2u64));

    // Even with a large max_slot the Solidity-order scan would miss; discovery wins.
    let ok = cache.set_erc20_balance_with_slot_scan(token, ALICE, U256::from(777u64), 16)?;
    assert!(ok, "vyper-layout override should succeed via discovery");
    let got: U256 = cache.call_sol(token, MockERC20::balanceOfCall { account: ALICE })?;
    assert_eq!(got, U256::from(777u64));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn overrides_solady_packed_token() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x33);
    install_default_account(&mut cache, ALICE);
    install_runtime(&mut cache, token, solady_runtime(SOLADY_SEED));

    let access = cache
        .discover_erc20_balance_slot(token, ALICE)?
        .expect("solady slot discovered");
    assert_eq!(
        access.layout,
        SlotLayout::PackedSeed {
            seed: U256::from(SOLADY_SEED)
        }
    );

    let ok = cache.set_erc20_balance_with_slot_scan(token, ALICE, U256::from(42u64), 0)?;
    assert!(ok, "solady packed override should succeed via discovery");
    let got: U256 = cache.call_sol(token, MockERC20::balanceOfCall { account: ALICE })?;
    assert_eq!(got, U256::from(42u64));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn traces_nested_allowance() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    install_default_account(&mut cache, ALICE);
    install_default_account(&mut cache, BOB);
    install_mock_erc20(&mut cache, token);

    // allowance[ALICE][BOB] lives at keccak(BOB ‖ keccak(ALICE ‖ 4)).
    let inner = keccak256((ALICE, U256::from(4u64)).abi_encode());
    let outer = keccak256((BOB, U256::from_be_bytes(inner.0)).abi_encode());
    cache
        .db_mut()
        .insert_account_storage(token, U256::from_be_bytes(outer.0), U256::from(999u64))
        .unwrap();

    let calldata = Bytes::from(
        MockERC20Shim::allowanceCall {
            owner: ALICE,
            spender: BOB,
        }
        .abi_encode(),
    );
    let known = [ALICE.into_word(), BOB.into_word()];
    let accesses = cache.trace_hashed_slots(ALICE, token, calldata, &known)?;

    let nested = accesses
        .iter()
        .find(|a| a.layout == SlotLayout::Nested)
        .expect("nested allowance access");
    assert_eq!(nested.base_slot, U256::from(4u64));
    assert_eq!(nested.depth, 2);
    assert_eq!(nested.keys, vec![BOB.into_word(), ALICE.into_word()]);
    assert_eq!(nested.value, U256::from(999u64));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn track_erc20_balances_fans_out() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    install_default_account(&mut cache, ALICE);
    install_default_account(&mut cache, BOB);
    install_default_account(&mut cache, CAROL);
    install_mock_erc20(&mut cache, token);

    let (tracked, pairs) = cache
        .track_erc20_balances(token, [ALICE, BOB, CAROL])?
        .expect("layout discovered");

    assert_eq!(tracked.layout, SlotLayout::SolidityMapping);
    assert_eq!(tracked.base_slot, U256::from(3u64));
    assert_eq!(pairs.len(), 3);
    for (holder, slot) in pairs {
        let expected = keccak256((holder, U256::from(3u64)).abi_encode());
        assert_eq!(slot, expected, "slot for {holder} matches Solidity layout");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn write_mapping_entry_is_layout_aware() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let vyper_tok = Address::repeat_byte(0x22);
    install_default_account(&mut cache, ALICE);
    install_runtime(&mut cache, vyper_tok, vyper_runtime(2));

    let tracked = TrackedMapping::new(vyper_tok, U256::from(2u64), SlotLayout::VyperMapping);
    let slot = cache.write_mapping_entry(&tracked, ALICE.into_word(), U256::from(555u64))?;

    // Written slot equals the Vyper-order location, and balanceOf reflects it.
    assert_eq!(slot, vyper_slot(2, ALICE));
    let got: U256 = cache.call_sol(vyper_tok, MockERC20::balanceOfCall { account: ALICE })?;
    assert_eq!(got, U256::from(555u64));
    let _ = solady_slot(SOLADY_SEED, ALICE); // silence unused in this test module
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sets_erc20_allowance_via_nested_discovery() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    install_default_account(&mut cache, ALICE);
    install_default_account(&mut cache, BOB);
    install_mock_erc20(&mut cache, token);

    let ok = cache.set_erc20_allowance(token, ALICE, BOB, U256::from(500u64))?;
    assert!(ok, "nested allowance slot discovered and written");
    assert_eq!(
        cache.erc20_allowance(token, ALICE, BOB)?,
        U256::from(500u64)
    );

    // "Unlimited" approval.
    assert!(cache.set_erc20_allowance(token, ALICE, BOB, U256::MAX)?);
    assert_eq!(cache.erc20_allowance(token, ALICE, BOB)?, U256::MAX);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_mock_balance_and_allowance_enable_transferfrom() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    let spender = CAROL; // acts as the router pulling funds
    for a in [ALICE, BOB, spender] {
        install_default_account(&mut cache, a);
    }
    install_mock_erc20(&mut cache, token);

    // Mock on a throwaway overlay — the cache is never mutated.
    let mut sim = cache.mock_overlay();
    assert!(sim.mock_balance(token, ALICE, U256::from(1_000u64))?);
    assert!(sim.mock_allowance(token, ALICE, spender, U256::MAX)?);

    // Spender pulls 250 ALICE → BOB via transferFrom, committing to the overlay.
    let cd = Bytes::from(
        MockERC20Shim::transferFromCall {
            from: ALICE,
            to: BOB,
            amount: U256::from(250u64),
        }
        .abi_encode(),
    );
    let (res, _) = sim.call_raw_with_inspector(
        spender,
        token,
        cd,
        &TxConfig::default(),
        CallTracer::new(),
        true,
    )?;
    assert!(
        matches!(res, ExecutionResult::Success { .. }),
        "transferFrom should succeed"
    );

    assert_eq!(
        sim.call_sol(token, MockERC20::balanceOfCall { account: BOB })?,
        U256::from(250u64)
    );
    assert_eq!(
        sim.call_sol(token, MockERC20::balanceOfCall { account: ALICE })?,
        U256::from(750u64)
    );

    // Isolation: the cache (true state) never saw the mocked balance.
    drop(sim);
    let cache_alice: U256 = cache.call_sol(token, MockERC20::balanceOfCall { account: ALICE })?;
    assert_eq!(cache_alice, U256::ZERO);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_mock_call_sets_total_supply() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    install_mock_erc20(&mut cache, token);
    // MockERC20.totalSupply is a plain slot (2); seed a distinctive value so the
    // value-match is unambiguous.
    cache.insert_storage_slot(token, U256::from(2u64), U256::from(1_000u64))?;

    let mut sim = cache.mock_overlay();
    assert!(sim.mock_call(
        token,
        MockERC20Shim::totalSupplyCall {},
        U256::from(5_000u64)
    )?);
    assert_eq!(
        sim.call_sol(token, MockERC20Shim::totalSupplyCall {})?,
        U256::from(5_000u64)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_mock_refuses_zero_address_balance() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    let token = Address::repeat_byte(0x11);
    install_mock_erc20(&mut cache, token);

    let mut sim = cache.mock_overlay();
    assert!(
        !sim.mock_balance(token, Address::ZERO, U256::from(100u64))?,
        "zero-address balance mock must be refused"
    );
    // The zero address's balance slot was never written.
    assert_eq!(
        sim.call_sol(
            token,
            MockERC20::balanceOfCall {
                account: Address::ZERO
            }
        )?,
        U256::ZERO
    );
    Ok(())
}

// The shared `MockERC20` interface lacks `allowance`/`transferFrom`/`totalSupply`;
// declare a shim for the nested-mapping trace test and the overlay mock tests.
alloy_sol_types::sol! {
    interface MockERC20Shim {
        function allowance(address owner, address spender) returns (uint256);
        function transferFrom(address from, address to, uint256 amount) returns (bool);
        function totalSupply() returns (uint256);
    }
}
