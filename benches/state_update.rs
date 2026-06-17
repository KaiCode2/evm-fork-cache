//! Phase 3 benchmarks: the targeted state-update apply primitive.
//!
//! Measures [`EvmCache::apply_updates`] throughput across batch sizes
//! (1 → 1000 `Slot` writes) and the per-variant cost of a single apply (`Slot`
//! vs `Account` patch vs `Purge`). The cache is built once per group; each
//! iteration re-uses it (the writes are idempotent / additive in-memory).
//!
//! Fully offline (mocked provider, state injected directly), so reproducible.
//! A current-thread runtime is used only to drive the async cache constructor;
//! `apply_updates` itself is synchronous and never touches the network.

use std::hint::black_box;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::{AccountPatch, PurgeScope, SlotDelta, StateUpdate};
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::{Builder, Runtime};

const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../fixtures/mock_erc20_runtime.hex");
const POOL: Address = Address::repeat_byte(0xAA);

fn current_thread_rt() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// A cache with `POOL` installed as a MockERC20 (overlay account present, so slot
/// writes exercise the overlay write-through branch too).
fn pool_cache(rt: &Runtime) -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
    let runtime = Bytecode::new_raw(Bytes::from(
        hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).unwrap(),
    ));
    let code_hash = runtime.hash_slow();
    cache.db_mut().insert_account_info(
        POOL,
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
        .replace_account_storage(POOL, Default::default())
        .unwrap();
    cache
}

/// `apply_updates` throughput as the `Slot` batch grows (1 → 1000).
fn bench_apply_slots_batch(c: &mut Criterion) {
    let rt = current_thread_rt();
    let mut cache = pool_cache(&rt);

    let mut group = c.benchmark_group("apply_slots_batch");
    for &n in &[1usize, 10, 100, 1_000] {
        // A fresh value each iteration is unnecessary; alternate two values so
        // every apply records a real change (the worst case: full diff).
        let updates_a: Vec<StateUpdate> = (0..n)
            .map(|i| StateUpdate::slot(POOL, U256::from(i as u64), U256::from(1u64)))
            .collect();
        let updates_b: Vec<StateUpdate> = (0..n)
            .map(|i| StateUpdate::slot(POOL, U256::from(i as u64), U256::from(2u64)))
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            let mut toggle = false;
            b.iter(|| {
                let updates = if toggle { &updates_a } else { &updates_b };
                toggle = !toggle;
                black_box(cache.apply_updates(black_box(updates)));
            })
        });
    }
    group.finish();
}

/// Per-variant cost of a single `apply_update`: `Slot` vs `Account` vs `Purge`.
fn bench_apply_per_variant(c: &mut Criterion) {
    let rt = current_thread_rt();

    let mut group = c.benchmark_group("apply_per_variant");

    group.bench_function("slot", |b| {
        let mut cache = pool_cache(&rt);
        let mut toggle = false;
        b.iter(|| {
            let value = if toggle { U256::from(1) } else { U256::from(2) };
            toggle = !toggle;
            black_box(cache.apply_update(black_box(&StateUpdate::slot(
                POOL,
                U256::from(0),
                value,
            ))));
        })
    });

    group.bench_function("account_balance", |b| {
        let mut cache = pool_cache(&rt);
        let mut toggle = false;
        b.iter(|| {
            let value = if toggle { U256::from(1) } else { U256::from(2) };
            toggle = !toggle;
            black_box(cache.apply_update(black_box(&StateUpdate::Account {
                address: POOL,
                patch: AccountPatch::default().balance(value),
            })));
        })
    });

    // A relative SlotDelta on a hot slot (seeded once, additive each iter).
    group.bench_function("slot_delta_hot", |b| {
        let mut cache = pool_cache(&rt);
        cache.inject_storage_batch(&[(POOL, U256::from(0), U256::from(1))]);
        b.iter(|| {
            // Add(0) keeps the value stable so the slot stays hot across iters.
            black_box(cache.apply_update(black_box(&StateUpdate::slot_delta(
                POOL,
                U256::from(0),
                SlotDelta::Add(U256::ZERO),
            ))));
        })
    });

    // A relative SlotDelta on a cold slot (always skipped, never applied).
    group.bench_function("slot_delta_cold", |b| {
        let mut cache = pool_cache(&rt);
        b.iter(|| {
            // POOL is StorageCleared, so an unseeded slot reads ZERO (hot). Use a
            // distinct address with no overlay account and no backend slot: cold.
            black_box(cache.apply_update(black_box(&StateUpdate::slot_delta(
                Address::repeat_byte(0xCD),
                U256::from(0),
                SlotDelta::Add(U256::from(1)),
            ))));
        })
    });

    // The general closure read-modify-write escape hatch.
    group.bench_function("modify_slot", |b| {
        let mut cache = pool_cache(&rt);
        cache.inject_storage_batch(&[(POOL, U256::from(0), U256::from(1))]);
        b.iter(|| {
            black_box(cache.modify_slot(POOL, U256::from(0), |cur| {
                cur.map(|v| v.saturating_add(U256::ZERO))
            }));
        })
    });

    // An `Account` *code* patch: `Bytecode::new_raw` + `hash_slow` (a keccak over
    // the code) — likely the most expensive single apply. Toggle two code blobs
    // so each apply records a real change.
    group.bench_function("account_code", |b| {
        let mut cache = pool_cache(&rt);
        let code_a = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xf3]);
        let code_b = Bytes::from_static(&[0x60, 0x01, 0x60, 0x01, 0xf3]);
        let mut toggle = false;
        b.iter(|| {
            let code = if toggle {
                code_a.clone()
            } else {
                code_b.clone()
            };
            toggle = !toggle;
            black_box(cache.apply_update(black_box(&StateUpdate::code(POOL, code))));
        })
    });

    // Purge mutates the cache, so re-seed each iteration via iter_batched.
    group.bench_function("purge_all_storage", |b| {
        b.iter_batched(
            || {
                let mut cache = pool_cache(&rt);
                cache.inject_storage_batch(&[
                    (POOL, U256::from(0), U256::from(1)),
                    (POOL, U256::from(1), U256::from(2)),
                    (POOL, U256::from(2), U256::from(3)),
                ]);
                cache
            },
            |mut cache| {
                black_box(
                    cache
                        .apply_update(black_box(&StateUpdate::purge(POOL, PurgeScope::AllStorage))),
                );
            },
            BatchSize::SmallInput,
        )
    });

    // `PurgeScope::Account` (full account + storage removal).
    group.bench_function("purge_account", |b| {
        b.iter_batched(
            || {
                let mut cache = pool_cache(&rt);
                cache.inject_storage_batch(&[
                    (POOL, U256::from(0), U256::from(1)),
                    (POOL, U256::from(1), U256::from(2)),
                ]);
                cache
            },
            |mut cache| {
                black_box(
                    cache.apply_update(black_box(&StateUpdate::purge(POOL, PurgeScope::Account))),
                );
            },
            BatchSize::SmallInput,
        )
    });

    // `PurgeScope::Slots` (a few specific slots).
    group.bench_function("purge_slots", |b| {
        b.iter_batched(
            || {
                let mut cache = pool_cache(&rt);
                cache.inject_storage_batch(&[
                    (POOL, U256::from(0), U256::from(1)),
                    (POOL, U256::from(1), U256::from(2)),
                    (POOL, U256::from(2), U256::from(3)),
                ]);
                cache
            },
            |mut cache| {
                black_box(cache.apply_update(black_box(&StateUpdate::purge(
                    POOL,
                    PurgeScope::Slots(vec![U256::from(0), U256::from(2)]),
                ))));
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// A *heterogeneous* `apply_updates` batch (Slot + Account + Purge) — exercises
/// the single-lock fast-path drop/re-acquire discipline around the non-slot
/// updates.
fn bench_apply_heterogeneous(c: &mut Criterion) {
    let rt = current_thread_rt();
    let mut group = c.benchmark_group("apply_updates_mixed");

    group.bench_function("slot_account_purge", |b| {
        b.iter_batched(
            || {
                let mut cache = pool_cache(&rt);
                cache.inject_storage_batch(&[(POOL, U256::from(9), U256::from(1))]);
                cache
            },
            |mut cache| {
                // The cache is re-seeded each iteration, so a fixed value still
                // records real changes (slots start at ZERO, balance/purge act on
                // the fresh seed).
                let value = U256::from(2);
                black_box(cache.apply_updates(black_box(&[
                    StateUpdate::slot(POOL, U256::from(0), value),
                    StateUpdate::slot(POOL, U256::from(1), value),
                    StateUpdate::balance(POOL, value),
                    StateUpdate::purge(POOL, PurgeScope::Slots(vec![U256::from(9)])),
                    StateUpdate::slot(POOL, U256::from(2), value),
                ])));
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// A *distinct-address* `apply_updates` batch — the only fair apples-to-apples
/// comparison against the raw `inject_storage_batch` baseline (each write targets
/// a different address, so no overlay account exists and the fast-path holds the
/// backend storage guard once for the whole run).
fn bench_apply_distinct_addresses(c: &mut Criterion) {
    let rt = current_thread_rt();
    let mut group = c.benchmark_group("apply_distinct_addresses");

    for &n in &[10usize, 100, 1_000] {
        let updates_a: Vec<StateUpdate> = (0..n)
            .map(|i| StateUpdate::slot(Address::repeat_byte(i as u8), U256::from(0), U256::from(1)))
            .collect();
        let updates_b: Vec<StateUpdate> = (0..n)
            .map(|i| StateUpdate::slot(Address::repeat_byte(i as u8), U256::from(0), U256::from(2)))
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            let mut cache = pool_cache(&rt);
            let mut toggle = false;
            b.iter(|| {
                let updates = if toggle { &updates_a } else { &updates_b };
                toggle = !toggle;
                black_box(cache.apply_updates(black_box(updates)));
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_apply_slots_batch,
    bench_apply_per_variant,
    bench_apply_heterogeneous,
    bench_apply_distinct_addresses,
);
criterion_main!(benches);
