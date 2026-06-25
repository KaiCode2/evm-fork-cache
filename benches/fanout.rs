//! **Pillar 1 — parallel fan-out throughput.**
//!
//! Once recent chain state is snapshotted, the per-candidate cost of an isolated
//! simulation is a cheap `Arc`-clone overlay, not a fresh fork. This bench
//! contrasts the two loops a searcher can write to evaluate N candidates with
//! full per-candidate isolation, over a cache that holds realistic cold state:
//!
//! - **`snapshot_once`** (the crate): `create_snapshot()` once, then N cheap
//!   `EvmOverlay` clones. The snapshot's cost is amortized across all N.
//! - **`fork_per_candidate`** (vanilla): a full independent fork per candidate —
//!   modeled by `create_snapshot_deep_clone()` (the O(total state) flatten a
//!   searcher pays to isolate each candidate without structural sharing) — then
//!   the simulation.
//!
//! `Throughput::Elements(N)` makes Criterion report the **amortized per-candidate**
//! cost. As N grows the crate's per-candidate cost falls toward the overlay-clone
//! floor while fork-per-candidate stays flat at the full-clone cost; the gap
//! widens with the amount of state the fork holds. Wall-clock and
//! machine-dependent — read the ratio, not the absolute. (The integer fetch-count
//! win is in `examples/fetch_minimization_counted.rs`; this is the CPU side.)
//!
//! At N=1 there is little or no win: one snapshot is not yet amortized. That is
//! honest — the fan-out economics only pay off across many candidates.

use std::hint::black_box;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::{SolCall, sol};
use alloy_transport::mock::Asserter;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use evm_fork_cache::cache::{EvmCache, EvmOverlay};
use revm::context::result::ExecutionResult;
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::Runtime;

const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../fixtures/mock_erc20_runtime.hex");
const BALANCE_SLOT: u64 = 3;

sol! {
    interface MockERC20 {
        function balanceOf(address account) returns (uint256);
    }
}

fn addr(i: usize) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..20].copy_from_slice(&(i as u64 + 1).to_be_bytes());
    Address::from(bytes)
}

/// A warm cache holding `accounts x slots` of cold chain state (so a per-candidate
/// deep clone is realistically expensive) plus a MockERC20 with one funded owner.
fn warm_cache(rt: &Runtime, accounts: usize, slots: usize) -> (EvmCache, Address, Address, Bytes) {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));

    // Bulk cold state the fork is holding this block.
    let mut batch: Vec<(Address, U256, U256)> = Vec::with_capacity(accounts * slots);
    for a in 0..accounts {
        for s in 0..slots {
            batch.push((
                addr(a),
                U256::from(s as u64),
                U256::from((a * 31 + s) as u64),
            ));
        }
    }
    cache.inject_storage_batch(&batch);

    // The contract the candidates actually exercise.
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

    let calldata = Bytes::from(MockERC20::balanceOfCall { account: owner }.abi_encode());
    (cache, token, owner, calldata)
}

fn bench_candidate_fanout(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    // A fork holding ~32k cold slots (2,000 accounts x 16 slots) — the deep clone
    // a fork-per-candidate loop pays scales with this; the snapshot shares it.
    let (mut cache, token, owner, calldata) = warm_cache(&rt, 2_000, 16);
    black_box(cache.create_snapshot()); // warm the memoized base once

    let mut group = c.benchmark_group("candidate_fanout");
    for &n in &[1usize, 8, 32, 128] {
        group.throughput(Throughput::Elements(n as u64));

        // Crate: one snapshot amortized across N cheap overlay clones.
        group.bench_with_input(BenchmarkId::new("snapshot_once", n), &n, |b, &n| {
            b.iter(|| {
                let snapshot = cache.create_snapshot();
                for _ in 0..n {
                    let mut overlay = EvmOverlay::new(snapshot.clone(), None);
                    let result = overlay.call_raw(owner, token, calldata.clone()).unwrap();
                    debug_assert!(matches!(result, ExecutionResult::Success { .. }));
                    black_box(result);
                }
            })
        });

        // Vanilla: a full independent fork (deep clone) per candidate.
        group.bench_with_input(BenchmarkId::new("fork_per_candidate", n), &n, |b, &n| {
            b.iter(|| {
                for _ in 0..n {
                    let snapshot = cache.create_snapshot_deep_clone();
                    let mut overlay = EvmOverlay::new(snapshot, None);
                    let result = overlay.call_raw(owner, token, calldata.clone()).unwrap();
                    debug_assert!(matches!(result, ExecutionResult::Success { .. }));
                    black_box(result);
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_candidate_fanout);
criterion_main!(benches);
