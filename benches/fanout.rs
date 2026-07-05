//! **Pillar 1 — parallel fan-out throughput.**
//!
//! The honest, defensible win of the snapshot/overlay model is **parallelism**.
//! A live mutable fork (one `revm` EVM isolated with `checkpoint`/`revert`) can
//! only evaluate candidates *sequentially* — it cannot be shared mutably across
//! threads. `snapshot()` produces an immutable `Send + Sync` view; cloning
//! the `Arc` hands each worker thread its own cheap `EvmOverlay`, so the same N
//! candidates fan out across cores.
//!
//! This bench therefore compares like-for-like execution, sequential vs parallel,
//! over the SAME warm in-memory snapshot (no RPC on either side):
//!
//! - **`sequential`** — N candidates one after another on overlays from one
//!   snapshot. This is also where a competent single-threaded baseline lands: a
//!   single fork isolated with `checkpoint`/`revert` per candidate is O(touched)
//!   and runs in the same per-candidate range, so single-threaded the snapshot
//!   model is **~1×** — not a throughput win. We do not claim one.
//! - **`parallel`** — the same N candidates split across
//!   `available_parallelism()` worker threads, each driving its own overlays from
//!   an `Arc::clone` of the shared snapshot.
//!
//! The win is the `parallel/sequential` ratio. Honest result: it is **modest**
//! (~1.2× on a 10-core M1 Pro for these workloads), because even at 48 ops per
//! candidate the cost is dominated by per-call revm allocation, which contends on
//! the allocator rather than scaling with cores. Heavier, compute-bound candidates
//! parallelize better; we report what we measure and do not claim a core-count
//! multiplier. Wall-clock and machine-dependent — read the ratio. The internal
//! copy-on-write snapshot-construction cost model (COW vs deep clone) lives in
//! `benches/simulation.rs` / `docs/INTERNALS.md`, not here.

use std::hint::black_box;
use std::sync::Arc;
use std::thread;

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

/// A warm cache holding a MockERC20 with one funded owner (state in memory, no RPC).
fn warm_cache(rt: &Runtime) -> (EvmCache, Address, Address, Bytes) {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));

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
    let (mut cache, token, owner, calldata) = warm_cache(&rt);
    black_box(cache.snapshot()); // warm the memoized base once

    let workers = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    // Each candidate runs a non-trivial simulation (many reads), modeling a real
    // candidate tx that touches dozens of slots. Even so, the per-candidate cost
    // stays largely allocation-bound (revm allocates per call), so the measured
    // parallel speedup is modest (~1.2×) rather than core-count — reported
    // honestly, not tuned until it looks good.
    const OPS_PER_CANDIDATE: usize = 48;
    let run_candidate = |overlay: &mut EvmOverlay| {
        for _ in 0..OPS_PER_CANDIDATE {
            let result = overlay.call_raw(owner, token, calldata.clone()).unwrap();
            debug_assert!(matches!(result, ExecutionResult::Success { .. }));
            black_box(result);
        }
    };

    let mut group = c.benchmark_group("candidate_fanout");
    for &n in &[64usize, 256, 1_024] {
        group.throughput(Throughput::Elements(n as u64));

        // Single-threaded: N candidates one after another. The competent
        // single-thread baseline (one fork, checkpoint/revert per candidate) lands
        // here too — single-threaded the snapshot model is ~1×, not a win.
        group.bench_with_input(BenchmarkId::new("sequential", n), &n, |b, &n| {
            b.iter(|| {
                let snapshot = cache.snapshot();
                for _ in 0..n {
                    let mut overlay = EvmOverlay::new(snapshot.clone(), None);
                    run_candidate(&mut overlay);
                }
            })
        });

        // Parallel: the same N candidates across `workers` threads, each driving
        // its own overlays from an Arc::clone of the shared immutable snapshot —
        // which a single mutable fork cannot do.
        group.bench_with_input(BenchmarkId::new("parallel", n), &n, |b, &n| {
            b.iter(|| {
                let snapshot = cache.snapshot();
                let per = n.div_ceil(workers);
                thread::scope(|s| {
                    for start in (0..n).step_by(per) {
                        let end = (start + per).min(n);
                        let snapshot = snapshot.clone();
                        let calldata = calldata.clone();
                        s.spawn(move || {
                            for _ in start..end {
                                let mut overlay = EvmOverlay::new(snapshot.clone(), None);
                                for _ in 0..OPS_PER_CANDIDATE {
                                    let result =
                                        overlay.call_raw(owner, token, calldata.clone()).unwrap();
                                    debug_assert!(matches!(
                                        result,
                                        ExecutionResult::Success { .. }
                                    ));
                                    black_box(result);
                                }
                            }
                        });
                    }
                });
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_candidate_fanout);
criterion_main!(benches);
