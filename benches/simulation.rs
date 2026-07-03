//! Hot-path benchmarks for the simulation engine: snapshot creation across
//! cache sizes, parallel-overlay fan-out, single-call throughput, sequential
//! bundle simulation, and batched storage injection.
//!
//! These run fully offline (mocked provider) so they're reproducible. They
//! quantify the Pillar A (copy-on-write snapshot) win.
//!
//! Expected shapes:
//! - **`snapshot` group (A/B).** The cold index is seeded into **layer 2**
//!   via `inject_storage_batch` (`populated_cache_layer2`), the way a fork cache
//!   actually holds it. For each size it benches both the COW `snapshot`
//!   and the retained `snapshot_deep_clone`. The deep clone is an O(total
//!   state) copy, so its cost slopes up with the index size; the COW path shares
//!   the memoized base and avoids cloning total storage slots. It still scans
//!   accounts and new layer-1 entries, so it should be much flatter than the deep
//!   clone, especially as slots/account grows, but not strictly flat by account
//!   count.
//! - **`resnapshot_hot_loop`.** Warms the base with one snapshot, applies a small
//!   `apply_updates` layer-1 mutation, then measures `snapshot`. This is
//!   the memoization win: the COW path avoids cloning cold storage slots but
//!   remains sensitive to account scans and new layer-1 entries.
//! - **`overlay_fanout`.** Measures fanning one frozen snapshot out into many
//!   isolated simulations, comparing a fresh `EvmOverlay::new` per sim against a
//!   single `reset()`-recycled overlay (Pillar A.2).

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
    rt.block_on(EvmCache::new(Arc::new(provider)))
}

/// A cache whose cold index lives in **layer 2** — seeded via
/// `inject_storage_batch`, the path a fork cache actually uses to bulk-load its
/// cold state. This is what the COW `snapshot` memoizes into its base.
fn populated_cache_layer2(rt: &Runtime, accounts: usize, slots_per: usize) -> EvmCache {
    let mut cache = offline_cache(rt);
    let mut batch: Vec<(Address, U256, U256)> = Vec::with_capacity(accounts * slots_per);
    for a in 0..accounts {
        let address = addr(a);
        for s in 0..slots_per {
            batch.push((
                address,
                U256::from(s as u64),
                U256::from((a * 31 + s) as u64),
            ));
        }
    }
    cache.inject_storage_batch(&batch);
    cache
}

/// A/B snapshot creation across cold-index sizes: the COW `snapshot` vs.
/// the retained `snapshot_deep_clone`, both over a layer-2-seeded index.
///
/// The deep clone slopes up with total slots; the COW path, after a warm-up
/// snapshot has memoized the base, avoids cloning those slots but still pays the
/// O(accounts) growth scan.
fn bench_snapshot(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("snapshot");
    for &(accounts, slots) in &[
        (100usize, 8usize),
        (1_000, 8),
        (2_000, 16),
        (5_000, 16),
        (10_000, 16),
    ] {
        let mut cache = populated_cache_layer2(&rt, accounts, slots);
        // Warm the memoized base once so the COW measurement reflects the
        // steady-state (reuse) cost, not the first full build.
        black_box(cache.snapshot());
        group.throughput(criterion::Throughput::Elements((accounts * slots) as u64));
        group.bench_with_input(
            BenchmarkId::new("cow", format!("{accounts}acct_x{slots}slot")),
            &accounts,
            |b, _| b.iter(|| black_box(cache.snapshot())),
        );
        group.bench_with_input(
            BenchmarkId::new("deep_clone", format!("{accounts}acct_x{slots}slot")),
            &accounts,
            |b, _| b.iter(|| black_box(cache.snapshot_deep_clone())),
        );
    }
    group.finish();
}

/// The memoization win: a hot re-snapshot loop. Warm the base once, apply a
/// *small* layer-1 mutation, then measure `snapshot`. Cost should track
/// account scanning plus new layer-1 entries, staying much flatter than the deep
/// clone as cold storage grows.
fn bench_resnapshot_hot_loop(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("resnapshot_hot_loop");
    for &(accounts, slots) in &[(1_000usize, 8usize), (5_000, 16), (10_000, 16)] {
        let mut cache = populated_cache_layer2(&rt, accounts, slots);
        // Warm the base.
        black_box(cache.snapshot());
        // A handful of layer-1 writes (does not dirty the memoized base).
        let target = addr(0);
        group.throughput(criterion::Throughput::Elements((accounts * slots) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{accounts}acct_x{slots}slot")),
            &accounts,
            |b, _| {
                b.iter(|| {
                    cache
                        .db_mut()
                        .insert_account_info(target, AccountInfo::default());
                    black_box(cache.snapshot());
                })
            },
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

    let snapshot = cache.snapshot();
    let calldata = Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode());

    let mut group = c.benchmark_group("overlay_fanout");
    for &k in &[1usize, 8, 32] {
        // Baseline: a fresh `EvmOverlay::new` (+ dirty maps + Arc clone + buffer)
        // per simulation.
        group.bench_with_input(BenchmarkId::new("new_per_sim", k), &k, |b, &k| {
            b.iter(|| {
                for _ in 0..k {
                    let mut overlay = EvmOverlay::new(snapshot.clone(), None);
                    let result = overlay.call_raw(owner, token, calldata.clone()).unwrap();
                    debug_assert!(matches!(result, ExecutionResult::Success { .. }));
                    black_box(result);
                }
            })
        });
        // Pillar A.2: one overlay built once, `reset()` between sims (reuses the
        // dirty maps, the snapshot Arc, and the shared-memory buffer).
        group.bench_with_input(BenchmarkId::new("reset_recycled", k), &k, |b, &k| {
            b.iter(|| {
                let mut overlay = EvmOverlay::new(snapshot.clone(), None);
                for _ in 0..k {
                    let result = overlay.call_raw(owner, token, calldata.clone()).unwrap();
                    debug_assert!(matches!(result, ExecutionResult::Success { .. }));
                    black_box(result);
                    overlay.reset();
                }
            })
        });
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
    let mut cache = offline_cache(&rt);

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
    bench_snapshot,
    bench_resnapshot_hot_loop,
    bench_overlay_fanout,
    bench_cache_call_raw,
    bench_sim_bundle,
    bench_inject_storage_batch
);
criterion_main!(benches);
