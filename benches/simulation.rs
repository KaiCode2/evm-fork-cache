//! Hot-path benchmarks for the simulation engine: snapshot creation across
//! cache sizes, parallel-overlay fan-out, single-call throughput, sequential
//! bundle simulation, and batched storage injection.
//!
//! These run fully offline (mocked provider) so they're reproducible. They
//! establish the baseline for the Pillar A (copy-on-write snapshot) rewrite:
//! `create_snapshot` is currently an O(total state) deep clone, so its cost
//! scales with the populated cache size (the `create_snapshot` group sweeps
//! 100 → 10,000 accounts to show that slope). Once Pillar A lands, the same
//! sweep should flatten toward O(changed state) — re-run this group before and
//! after to quantify the win. The `overlay_fanout` group measures the other
//! half of the value proposition: how cheaply one frozen snapshot fans out into
//! many isolated simulations.

use std::hint::black_box;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::{SolCall, sol};
use alloy_transport::mock::Asserter;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use evm_fork_cache::cache::{EvmCache, EvmOverlay};
use revm::context::result::ExecutionResult;
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::Runtime;

const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../fixtures/mock_erc20_runtime.hex");
const BALANCE_SLOT: u64 = 3;

sol! {
    interface MockERC20 {
        function balanceOf(address account) returns (uint256);
        function transfer(address to, uint256 amount) returns (bool);
    }
}

/// Distinct 20-byte address derived from an index.
fn addr(i: usize) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..20].copy_from_slice(&(i as u64 + 1).to_be_bytes());
    Address::from(bytes)
}

fn offline_cache(rt: &Runtime) -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    rt.block_on(EvmCache::new(Arc::new(provider), None))
}

/// A cache populated with `accounts` accounts, each holding `slots_per` slots.
fn populated_cache(rt: &Runtime, accounts: usize, slots_per: usize) -> EvmCache {
    let mut cache = offline_cache(rt);
    for a in 0..accounts {
        let address = addr(a);
        cache
            .db_mut()
            .insert_account_info(address, AccountInfo::default());
        for s in 0..slots_per {
            cache
                .db_mut()
                .insert_account_storage(
                    address,
                    U256::from(s as u64),
                    U256::from((a * 31 + s) as u64),
                )
                .unwrap();
        }
    }
    cache
}

fn bench_create_snapshot(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("create_snapshot");
    // Sweep from a small pool up to a production-scale index (10k contracts) so
    // the O(total state) slope of the current deep clone is visible. Pillar A
    // (copy-on-write) should flatten this curve.
    for &(accounts, slots) in &[
        (100usize, 8usize),
        (1_000, 8),
        (2_000, 16),
        (5_000, 16),
        (10_000, 16),
    ] {
        let cache = populated_cache(&rt, accounts, slots);
        group.throughput(criterion::Throughput::Elements((accounts * slots) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{accounts}acct_x{slots}slot")),
            &cache,
            |b, cache| b.iter(|| black_box(cache.create_snapshot())),
        );
    }
    group.finish();
}

fn bench_overlay_fanout(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // A cache holding a MockERC20 with one funded owner.
    let mut cache = offline_cache(&rt);
    let token = Address::repeat_byte(0xAA);
    let owner = Address::repeat_byte(0xBB);
    let runtime = Bytecode::new_raw(Bytes::from(
        hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).unwrap(),
    ));
    let code_hash = runtime.hash_slow();
    cache.db_mut().insert_account_info(
        token,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 0,
            code: Some(runtime),
            code_hash,
            account_id: None,
        },
    );
    cache
        .insert_mapping_storage_slot(token, U256::from(BALANCE_SLOT), owner, U256::from(1_000u64))
        .unwrap();

    let snapshot = cache.create_snapshot();
    let calldata = Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode());

    let mut group = c.benchmark_group("overlay_fanout");
    for &k in &[1usize, 8, 32] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{k}way")),
            &k,
            |b, &k| {
                b.iter(|| {
                    for _ in 0..k {
                        let mut overlay = EvmOverlay::new(snapshot.clone(), None);
                        let result = overlay.call_raw(owner, token, calldata.clone()).unwrap();
                        debug_assert!(matches!(result, ExecutionResult::Success { .. }));
                        black_box(result);
                    }
                })
            },
        );
    }
    group.finish();
}

/// A cache holding a `MockERC20` with `owner` funded and `recipient` at zero.
fn mock_erc20_cache(rt: &Runtime, token: Address, owner: Address, recipient: Address) -> EvmCache {
    let mut cache = offline_cache(rt);
    let runtime = Bytecode::new_raw(Bytes::from(
        hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).unwrap(),
    ));
    let code_hash = runtime.hash_slow();
    cache
        .db_mut()
        .insert_account_info(Address::ZERO, AccountInfo::default());
    cache
        .db_mut()
        .insert_account_info(owner, AccountInfo::default());
    cache
        .db_mut()
        .insert_account_info(recipient, AccountInfo::default());
    cache.db_mut().insert_account_info(
        token,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 0,
            code: Some(runtime),
            code_hash,
            account_id: None,
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
            owner,
            U256::from(1_000_000u64),
        )
        .unwrap();
    cache
        .insert_mapping_storage_slot(token, U256::from(BALANCE_SLOT), recipient, U256::ZERO)
        .unwrap();
    cache
}

/// Per-call throughput of the primary `EvmCache::call_raw` hot path (a
/// non-committing `balanceOf` view call), warm cache, no RPC.
fn bench_cache_call_raw(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let token = Address::repeat_byte(0xAA);
    let owner = Address::repeat_byte(0xBB);
    let recipient = Address::repeat_byte(0xCC);
    let mut cache = mock_erc20_cache(&rt, token, owner, recipient);
    let calldata = Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode());

    c.bench_function("cache_call_raw/balanceOf", |b| {
        b.iter(|| {
            let result = cache
                .call_raw(owner, token, calldata.clone(), false)
                .unwrap();
            debug_assert!(matches!(result, ExecutionResult::Success { .. }));
            black_box(result);
        })
    });
}

/// Sequential bundle: K committing `transfer` calls against shared cache state,
/// the shape of evaluating a multi-step MEV bundle. Measures committed-execution
/// cost as the bundle grows; each iteration starts from a fresh cache so the
/// sender's balance never drains.
fn bench_sim_bundle(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let token = Address::repeat_byte(0xAA);
    let owner = Address::repeat_byte(0xBB);
    let recipient = Address::repeat_byte(0xCC);
    let calldata = Bytes::from(
        MockERC20::transferCall {
            to: recipient,
            amount: U256::from(1u64),
        }
        .abi_encode(),
    );

    let mut group = c.benchmark_group("sim_bundle");
    for &k in &[1usize, 4, 16] {
        group.throughput(criterion::Throughput::Elements(k as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{k}tx")),
            &k,
            |b, &k| {
                b.iter_batched(
                    || mock_erc20_cache(&rt, token, owner, recipient),
                    |mut cache| {
                        for _ in 0..k {
                            let result = cache
                                .call_raw(owner, token, calldata.clone(), true)
                                .unwrap();
                            black_box(&result);
                        }
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

/// Batched direct storage injection (the bypass-RPC write path) across sizes.
fn bench_inject_storage_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cache = offline_cache(&rt);

    let mut group = c.benchmark_group("inject_storage_batch");
    for &n in &[100usize, 1_000, 10_000] {
        let batch: Vec<(Address, U256, U256)> = (0..n)
            .map(|i| (addr(i), U256::from(i as u64), U256::from(i as u64)))
            .collect();
        group.throughput(criterion::Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &batch, |b, batch| {
            b.iter(|| cache.inject_storage_batch(black_box(batch)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_create_snapshot,
    bench_overlay_fanout,
    bench_cache_call_raw,
    bench_sim_bundle,
    bench_inject_storage_batch
);
criterion_main!(benches);
