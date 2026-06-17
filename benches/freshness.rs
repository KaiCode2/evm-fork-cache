//! Phase 2 benchmarks: optimistic simulation + background slot validation.
//!
//! The sim is *swap-shaped*: a `MockERC20.transfer` reads the sender's balance
//! slot and writes balances — the same "read a state slot, write new state"
//! shape as a Uniswap pool swap (reads slot0/liquidity, writes new state). The
//! freshness layer treats that read slot as `Volatile` and verifies it.
//!
//! - **Correct snapshot:** the (stub) fetcher reports the read slot unchanged →
//!   `Confirmed`, no re-run.
//! - **Stale snapshot:** the fetcher reports the read slot changed → `Corrected`,
//!   the affected sim is re-run.
//!
//! Two groups:
//! - `phase2_cpu` (zero-latency stub) — the CPU overhead the freshness layer adds.
//! - `phase2_latency_50ms` (stub with a 50 ms simulated RPC round-trip) — the
//!   latency-hiding value prop: time-to-optimistic-result vs time-to-validated vs
//!   the naive "fetch-fresh-then-simulate" baseline.
//!
//! Fully offline (mocked provider + stub fetchers), so reproducible. A
//! current-thread runtime is used because the stub fetchers are synchronous; the
//! optimistic loop's deferred validation still works (the validator is a spawned
//! task driven by `validate().await`).

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256, hex, keccak256};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::{SolCall, SolValue, sol};
use alloy_transport::mock::Asserter;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use evm_fork_cache::cache::{EvmCache, EvmOverlay, StorageBatchFetchFn};
use evm_fork_cache::freshness::{
    AlwaysVerify, FreshnessController, FreshnessRegistry, SimRequest, Validation,
};
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::{Builder, Runtime};

const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../fixtures/mock_erc20_runtime.hex");
const BALANCE_BASE_SLOT: u64 = 3;
const TOKEN: Address = Address::repeat_byte(0xAA);
const SENDER: Address = Address::repeat_byte(0xBB);
const RECIPIENT: Address = Address::repeat_byte(0xCC);

sol! {
    interface MockERC20 {
        function transfer(address to, uint256 amount) returns (bool);
    }
}

/// keccak256(abi.encode(owner, 3)) — the `balanceOf(owner)` storage slot.
fn balance_slot(owner: Address) -> U256 {
    U256::from_be_bytes(keccak256((owner, U256::from(BALANCE_BASE_SLOT)).abi_encode()).0)
}

fn current_thread_rt() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// A swap-shaped cache: MockERC20 with `SENDER` funded `bal`, `RECIPIENT` zero.
fn swap_cache(rt: &Runtime, bal: u64) -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
    let runtime = Bytecode::new_raw(Bytes::from(
        hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).unwrap(),
    ));
    let code_hash = runtime.hash_slow();
    cache
        .db_mut()
        .insert_account_info(Address::ZERO, AccountInfo::default());
    cache
        .db_mut()
        .insert_account_info(SENDER, AccountInfo::default());
    cache
        .db_mut()
        .insert_account_info(RECIPIENT, AccountInfo::default());
    cache.db_mut().insert_account_info(
        TOKEN,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 0,
            code: Some(runtime),
            code_hash,
            account_id: None,
        },
    );
    // Mark storage local so unseeded slots read as zero (no RPC fallthrough).
    cache
        .db_mut()
        .replace_account_storage(TOKEN, Default::default())
        .unwrap();
    cache
        .insert_mapping_storage_slot(
            TOKEN,
            U256::from(BALANCE_BASE_SLOT),
            SENDER,
            U256::from(bal),
        )
        .unwrap();
    cache
        .insert_mapping_storage_slot(TOKEN, U256::from(BALANCE_BASE_SLOT), RECIPIENT, U256::ZERO)
        .unwrap();
    cache
}

/// A stub fetcher reporting `values` for known slots (zero otherwise), with an
/// optional simulated RPC delay.
fn stub_fetcher(
    values: HashMap<(Address, U256), U256>,
    delay: Option<Duration>,
) -> StorageBatchFetchFn {
    Arc::new(move |reqs: Vec<(Address, U256)>, _block: Option<BlockId>| {
        if let Some(d) = delay {
            std::thread::sleep(d);
        }
        reqs.into_iter()
            .map(|(a, s)| (a, s, Ok(values.get(&(a, s)).copied().unwrap_or(U256::ZERO))))
            .collect()
    })
}

fn transfer_calldata(amount: u64) -> Bytes {
    Bytes::from(
        MockERC20::transferCall {
            to: RECIPIENT,
            amount: U256::from(amount),
        }
        .abi_encode(),
    )
}

fn controller() -> FreshnessController<AlwaysVerify, evm_fork_cache::freshness::BlockClock> {
    FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify)
}

/// `reported` is what the fetcher claims the sender balance currently is.
fn fetcher_for(reported: u64, delay: Option<Duration>) -> StorageBatchFetchFn {
    stub_fetcher(
        HashMap::from([((TOKEN, balance_slot(SENDER)), U256::from(reported))]),
        delay,
    )
}

fn bench_phase2_cpu(c: &mut Criterion) {
    let rt = current_thread_rt();
    let calldata = transfer_calldata(100);
    let mut group = c.benchmark_group("phase2_cpu");

    // Time to the OPTIMISTIC result: snapshot + optimistic sim + read-set capture
    // + spawn. The sim is dropped (validator aborted) without awaiting validation.
    group.bench_function("optimistic_run", |b| {
        b.iter_batched(
            || {
                let mut cache = swap_cache(&rt, 1000);
                cache.set_storage_batch_fetcher(fetcher_for(1000, None));
                (cache, controller())
            },
            |(mut cache, mut ctrl)| {
                rt.block_on(async {
                    let sim = ctrl
                        .run(
                            &mut cache,
                            vec![SimRequest::new(SENDER, TOKEN, calldata.clone())],
                        )
                        .unwrap();
                    black_box(sim.optimistic().len());
                });
            },
            BatchSize::SmallInput,
        )
    });

    // Full cycle, CORRECT snapshot → Confirmed (verification matches, no re-run).
    group.bench_function("confirmed_correct_snapshot", |b| {
        b.iter_batched(
            || {
                let mut cache = swap_cache(&rt, 1000);
                cache.set_storage_batch_fetcher(fetcher_for(1000, None));
                (cache, controller())
            },
            |(mut cache, mut ctrl)| {
                rt.block_on(async {
                    let sim = ctrl
                        .run(
                            &mut cache,
                            vec![SimRequest::new(SENDER, TOKEN, calldata.clone())],
                        )
                        .unwrap();
                    black_box(sim.validate().await);
                });
            },
            BatchSize::SmallInput,
        )
    });

    // Full cycle, STALE snapshot → Corrected (verification differs, 1 re-run).
    group.bench_function("corrected_stale_snapshot", |b| {
        b.iter_batched(
            || {
                let mut cache = swap_cache(&rt, 1000);
                cache.set_storage_batch_fetcher(fetcher_for(900, None));
                (cache, controller())
            },
            |(mut cache, mut ctrl)| {
                rt.block_on(async {
                    let sim = ctrl
                        .run(
                            &mut cache,
                            vec![SimRequest::new(SENDER, TOKEN, calldata.clone())],
                        )
                        .unwrap();
                    let v = sim.validate().await;
                    debug_assert!(matches!(v, Validation::Corrected { .. }));
                    black_box(v);
                });
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_phase2_latency(c: &mut Criterion) {
    let rt = current_thread_rt();
    let calldata = transfer_calldata(100);
    let delay = Duration::from_millis(50); // simulated RPC round-trip

    let mut group = c.benchmark_group("phase2_latency_50ms");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(3));

    // NAIVE baseline (the pre-optimistic model): fetch fresh state over RPC, THEN
    // simulate. Pays the full RPC latency before any result → ~L + sim.
    group.bench_function("naive_fetch_then_sim", |b| {
        b.iter_batched(
            || {
                let mut cache = swap_cache(&rt, 1000);
                cache.set_storage_batch_fetcher(fetcher_for(1000, Some(delay)));
                cache
            },
            |mut cache| {
                cache
                    .verify_slots(&[(TOKEN, balance_slot(SENDER))])
                    .unwrap(); // pays L
                let snapshot = cache.create_snapshot();
                let mut overlay = EvmOverlay::new(snapshot, None);
                black_box(overlay.call_raw(SENDER, TOKEN, calldata.clone()).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    // OPTIMISTIC: time to the actionable optimistic result. RPC verification has
    // not even started (it's a queued task, aborted on drop) → ~sim, NOT L.
    group.bench_function("optimistic_time_to_result", |b| {
        b.iter_batched(
            || {
                let mut cache = swap_cache(&rt, 1000);
                cache.set_storage_batch_fetcher(fetcher_for(1000, Some(delay)));
                (cache, controller())
            },
            |(mut cache, mut ctrl)| {
                rt.block_on(async {
                    let sim = ctrl
                        .run(
                            &mut cache,
                            vec![SimRequest::new(SENDER, TOKEN, calldata.clone())],
                        )
                        .unwrap();
                    black_box(sim.optimistic().len());
                });
            },
            BatchSize::SmallInput,
        )
    });

    // OPTIMISTIC, awaiting validation: ~L (the RPC the consumer overlapped with
    // its own work). The win is that the result was usable ~L earlier (above).
    group.bench_function("optimistic_time_to_validated", |b| {
        b.iter_batched(
            || {
                let mut cache = swap_cache(&rt, 1000);
                cache.set_storage_batch_fetcher(fetcher_for(1000, Some(delay)));
                (cache, controller())
            },
            |(mut cache, mut ctrl)| {
                rt.block_on(async {
                    let sim = ctrl
                        .run(
                            &mut cache,
                            vec![SimRequest::new(SENDER, TOKEN, calldata.clone())],
                        )
                        .unwrap();
                    black_box(sim.validate().await);
                });
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Scaling of the `verify_slots` primitive — the background validator's core
/// work — as the volatile set grows (1 → 1000 slots). The (zero-latency) stub
/// reports every slot unchanged, so this isolates the fetch + compare cost from
/// any injection churn.
fn bench_verify_slots(c: &mut Criterion) {
    let rt = current_thread_rt();
    let contract = Address::repeat_byte(0xDD);

    let mut group = c.benchmark_group("verify_slots");
    for &n in &[1usize, 10, 100, 1_000] {
        let slots: Vec<(Address, U256)> =
            (0..n).map(|i| (contract, U256::from(i as u64))).collect();
        let values: HashMap<(Address, U256), U256> =
            slots.iter().map(|&key| (key, U256::from(1u64))).collect();

        let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
        // Seed the cached values so the fetched (stub) values match → no change.
        let seed: Vec<(Address, U256, U256)> = slots
            .iter()
            .map(|&(a, s)| (a, s, U256::from(1u64)))
            .collect();
        cache.inject_storage_batch(&seed);
        cache.set_storage_batch_fetcher(stub_fetcher(values, None));

        group.throughput(criterion::Throughput::Elements(n as u64));
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(n),
            &slots,
            |b, slots| {
                b.iter(|| {
                    black_box(cache.verify_slots(slots).unwrap());
                })
            },
        );
    }
    group.finish();
}

/// Fan-out of the optimistic loop across a batch of K independent sims that all
/// validate as `Confirmed` (stub reports the read slot unchanged). Shows how the
/// per-cycle cost scales with the number of candidate transactions — one frozen
/// snapshot shared across K overlays plus K read-set captures and the unioned
/// verification.
fn bench_multi_sim(c: &mut Criterion) {
    let rt = current_thread_rt();
    let calldata = transfer_calldata(1);

    let mut group = c.benchmark_group("multi_sim_confirmed");
    for &k in &[1usize, 4, 16] {
        group.throughput(criterion::Throughput::Elements(k as u64));
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(format!("{k}sims")),
            &k,
            |b, &k| {
                b.iter_batched(
                    || {
                        let mut cache = swap_cache(&rt, 1_000_000);
                        cache.set_storage_batch_fetcher(fetcher_for(1_000_000, None));
                        let reqs: Vec<SimRequest> = (0..k)
                            .map(|_| SimRequest::new(SENDER, TOKEN, calldata.clone()))
                            .collect();
                        (cache, controller(), reqs)
                    },
                    |(mut cache, mut ctrl, reqs)| {
                        rt.block_on(async {
                            let sim = ctrl.run(&mut cache, reqs).unwrap();
                            let v = sim.validate().await;
                            debug_assert!(matches!(v, Validation::Confirmed));
                            black_box(v);
                        });
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_phase2_cpu,
    bench_phase2_latency,
    bench_verify_slots,
    bench_multi_sim
);
criterion_main!(benches);
