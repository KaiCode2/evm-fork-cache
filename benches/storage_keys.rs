//! Microbenchmarks for Uniswap V3-style storage-key derivation.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use evm_fork_cache::cache::{v3_tick_bitmap_storage_key, v3_tick_info_storage_keys};

fn bench_storage_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_keys");

    group.bench_function("tick_bitmap_key", |b| {
        b.iter(|| v3_tick_bitmap_storage_key(black_box(-128)))
    });

    group.bench_function("tick_info_keys", |b| {
        b.iter(|| v3_tick_info_storage_keys(black_box(-887_220)))
    });

    // Deriving keys for a sweep of words, as a tick prefetch would.
    group.bench_function("tick_bitmap_keys_x256", |b| {
        b.iter(|| {
            let mut acc = alloy_primitives::U256::ZERO;
            for word in -128i16..128 {
                acc ^= v3_tick_bitmap_storage_key(black_box(word));
            }
            acc
        })
    });

    group.finish();
}

criterion_group!(benches, bench_storage_keys);
criterion_main!(benches);
