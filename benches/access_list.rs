//! Microbenchmarks for `StorageAccessList` bookkeeping.

use std::hint::black_box;

use alloy_primitives::{Address, U256};
use criterion::{Criterion, criterion_group, criterion_main};
use evm_fork_cache::StorageAccessList;

/// Build a touch set spanning `n` accounts with a handful of slots each.
fn sample(n: u8, slot_base: u64) -> StorageAccessList {
    let mut al = StorageAccessList::default();
    for a in 0..n {
        let addr = Address::repeat_byte(a);
        al.accounts.insert(addr);
        for s in 0..8u64 {
            al.slots.insert((addr, U256::from(slot_base + s)));
        }
    }
    al
}

fn bench_access_list(c: &mut Criterion) {
    let warm = sample(32, 0);
    let candidate = sample(32, 4); // overlapping slot ranges

    let mut group = c.benchmark_group("access_list");

    group.bench_function("marginal_gas_savings", |b| {
        b.iter(|| black_box(&candidate).marginal_gas_savings(black_box(&warm)))
    });

    group.bench_function("extend", |b| {
        b.iter(|| {
            let mut merged = warm.clone();
            merged.extend(black_box(&candidate));
            merged
        })
    });

    group.bench_function("to_eip2930", |b| b.iter(|| black_box(&warm).to_eip2930()));

    group.finish();
}

criterion_group!(benches, bench_access_list);
criterion_main!(benches);
