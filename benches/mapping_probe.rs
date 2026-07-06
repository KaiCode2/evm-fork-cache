//! Benchmarks for v0.2.1 trace-based hash-slot discovery and overlay-scoped
//! mocking. All run fully offline (mocked provider), so they're reproducible
//! and cost nothing to re-run.
//!
//! Groups:
//! - **`discover`.** `discover_erc20_balance_slot` — one instrumented `balanceOf`
//!   simulation plus KECCAK-preimage/SLOAD matching — across the three supported
//!   layouts (Solidity `keccak(key‖slot)`, Vyper `keccak(slot‖key)`, Solady
//!   packed `keccak(key‖seed)`). The point is to show the cost is dominated by the
//!   single simulation and is essentially **layout-independent**: byte-order
//!   detection is a handful of hash comparisons on top of the sim.
//! - **`forge_balance`.** `set_erc20_balance_with_slot_scan` end-to-end
//!   (discover → layout-aware write → verify). `cold` rebuilds the cache each
//!   iteration so every call re-discovers; `cached` reuses a warmed cache so the
//!   descriptor cache (`erc20_balance_slots`) is hit and discovery is skipped —
//!   the steady-state cost of forging many balances on a known token.
//! - **`mock_overlay`.** `EvmOverlay::mock_balance` — the overlay-scoped mock
//!   (discover + dirty-layer write + verify), the per-mock cost of staging state
//!   for a simulation without touching the cache.
//! - **`overlay_call`.** `EvmOverlay::call_sol` (typed, native decode) vs.
//!   `call_raw` + manual 32-byte decode, to confirm the typed wrapper adds
//!   negligible overhead over the raw path.

use std::hint::black_box;
use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes, U256, hex, keccak256};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::{SolCall, sol};
use alloy_transport::mock::Asserter;
use criterion::{Criterion, criterion_group, criterion_main};
use evm_fork_cache::cache::EvmCache;
use revm::context::result::ExecutionResult;
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::Runtime;

const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../fixtures/mock_erc20_runtime.hex");
const BALANCE_SLOT: u64 = 3;
const SOLADY_SEED: u32 = 0x87a2_11a2;

sol! {
    interface MockERC20 {
        function balanceOf(address account) returns (uint256);
    }
}

fn offline_cache(rt: &Runtime) -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    rt.block_on(EvmCache::new(Arc::new(provider)))
}

fn install_default_account(cache: &mut EvmCache, addr: Address) {
    cache
        .db_mut()
        .insert_account_info(addr, AccountInfo::default());
}

/// Etch raw runtime bytecode at `addr` and mark its storage local.
fn install_runtime(cache: &mut EvmCache, addr: Address, code: Vec<u8>) {
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

// --- tiny bytecode builders for the non-Solidity layouts (no compiler needed) ---

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

/// A cache holding a Solidity `MockERC20` (balance mapping at slot 3) with
/// `holder` funded to a distinctive non-zero balance.
fn solidity_token(rt: &Runtime, token: Address, holder: Address) -> EvmCache {
    let mut cache = offline_cache(rt);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, holder);
    let runtime = Bytecode::new_raw(Bytes::from(
        hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).unwrap(),
    ));
    let code_hash = runtime.hash_slow();
    cache.db_mut().insert_account_info(
        token,
        AccountInfo {
            code: Some(runtime),
            code_hash,
            ..Default::default()
        },
    );
    cache
        .db_mut()
        .replace_account_storage(token, Default::default())
        .unwrap();
    cache
        .insert_mapping_storage_slot(
            token,
            U256::from(BALANCE_SLOT),
            holder,
            U256::from(1_000u64),
        )
        .unwrap();
    cache
}

/// A cache holding a Vyper-layout token (`keccak(slot‖key)`) with `holder` funded.
fn vyper_token(rt: &Runtime, token: Address, holder: Address, slot: u8) -> EvmCache {
    let mut cache = offline_cache(rt);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, holder);
    install_runtime(&mut cache, token, vyper_runtime(slot));
    let s = U256::from_be_slice(vyper_slot(slot, holder).as_slice());
    cache
        .insert_storage_slot(token, s, U256::from(1_000u64))
        .unwrap();
    cache
}

/// A cache holding a Solady packed-layout token (`keccak(key‖seed)`), holder funded.
fn solady_token(rt: &Runtime, token: Address, holder: Address, seed: u32) -> EvmCache {
    let mut cache = offline_cache(rt);
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, holder);
    install_runtime(&mut cache, token, solady_runtime(seed));
    let s = U256::from_be_slice(solady_slot(seed, holder).as_slice());
    cache
        .insert_storage_slot(token, s, U256::from(1_000u64))
        .unwrap();
    cache
}

/// Discovery cost across the three layouts — one instrumented `balanceOf`
/// simulation plus preimage/SLOAD matching. Should be near-identical across
/// layouts (the sim dominates; layout detection is a few hash checks).
fn bench_discover(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let token = Address::repeat_byte(0xAA);
    let holder = Address::repeat_byte(0xBB);

    let mut group = c.benchmark_group("discover");

    let mut sol = solidity_token(&rt, token, holder);
    group.bench_function("solidity", |b| {
        b.iter(|| {
            let access = sol.discover_erc20_balance_slot(token, holder).unwrap();
            debug_assert!(access.is_some());
            black_box(access);
        })
    });

    let mut vy = vyper_token(&rt, token, holder, 2);
    group.bench_function("vyper", |b| {
        b.iter(|| {
            let access = vy.discover_erc20_balance_slot(token, holder).unwrap();
            debug_assert!(access.is_some());
            black_box(access);
        })
    });

    let mut so = solady_token(&rt, token, holder, SOLADY_SEED);
    group.bench_function("solady", |b| {
        b.iter(|| {
            let access = so.discover_erc20_balance_slot(token, holder).unwrap();
            debug_assert!(access.is_some());
            black_box(access);
        })
    });

    group.finish();
}

/// End-to-end balance forging (`set_erc20_balance_with_slot_scan`). `cold`
/// re-discovers every call; `cached` hits the warmed descriptor cache and only
/// writes + verifies — the win from caching the discovered layout.
fn bench_forge_balance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let token = Address::repeat_byte(0xAA);
    let holder = Address::repeat_byte(0xBB);

    let mut group = c.benchmark_group("forge_balance");

    // Cold: a fresh cache each iteration forces the discovery path every time.
    group.bench_function("cold", |b| {
        b.iter_batched(
            || solidity_token(&rt, token, holder),
            |mut cache| {
                let ok = cache
                    .set_erc20_balance_with_slot_scan(token, holder, U256::from(5_000u64), 8)
                    .unwrap();
                debug_assert!(ok);
                black_box(ok);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Cached: warm the descriptor once, then every call is a layout-aware write
    // + verify (no discovery).
    let mut warm = solidity_token(&rt, token, holder);
    warm.set_erc20_balance_with_slot_scan(token, holder, U256::from(1u64), 8)
        .unwrap();
    group.bench_function("cached", |b| {
        let mut amount = 1u64;
        b.iter(|| {
            amount += 1;
            let ok = warm
                .set_erc20_balance_with_slot_scan(token, holder, U256::from(amount), 8)
                .unwrap();
            debug_assert!(ok);
            black_box(ok);
        })
    });

    group.finish();
}

/// Overlay-scoped `mock_balance`: discover + dirty-layer write + verify, the
/// per-mock cost of staging state for a simulation. The overlay is built once
/// (its COW construction is measured separately in `simulation.rs`).
fn bench_mock_overlay(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let token = Address::repeat_byte(0xAA);
    let holder = Address::repeat_byte(0xBB);
    let mut cache = solidity_token(&rt, token, holder);
    let mut overlay = cache.mock_overlay();

    c.bench_function("mock_overlay/mock_balance", |b| {
        b.iter(|| {
            let ok = overlay
                .mock_balance(token, holder, U256::from(1_000_000u64))
                .unwrap();
            debug_assert!(ok);
            black_box(ok);
        })
    });
}

/// Typed `call_sol` vs. `call_raw` + manual decode on the overlay: the native
/// decode wrapper should be within noise of the raw path.
fn bench_overlay_call(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let token = Address::repeat_byte(0xAA);
    let holder = Address::repeat_byte(0xBB);
    let mut cache = solidity_token(&rt, token, holder);
    let calldata = Bytes::from(MockERC20::balanceOfCall { account: holder }.abi_encode());

    let mut group = c.benchmark_group("overlay_call");

    let mut sim = cache.mock_overlay();
    group.bench_function("call_sol", |b| {
        b.iter(|| {
            let v = sim
                .call_sol(token, MockERC20::balanceOfCall { account: holder })
                .unwrap();
            black_box(v);
        })
    });

    let mut sim = cache.mock_overlay();
    group.bench_function("call_raw_decode", |b| {
        b.iter(|| {
            let result = sim
                .call_raw(Address::ZERO, token, calldata.clone())
                .unwrap();
            let v = match result {
                ExecutionResult::Success { output, .. } => {
                    let data = output.into_data();
                    U256::from_be_slice(&data[..32])
                }
                _ => U256::ZERO,
            };
            black_box(v);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_discover,
    bench_forge_balance,
    bench_mock_overlay,
    bench_overlay_call
);
criterion_main!(benches);
