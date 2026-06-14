//! Microbenchmark for CREATE3 address derivation.

use std::hint::black_box;

use alloy_primitives::{Address, B256, b256};
use criterion::{Criterion, criterion_group, criterion_main};
use evm_fork_cache::create3::derive_universal_create3_address;

fn bench_create3(c: &mut Criterion) {
    let deployer = Address::repeat_byte(0xAB);
    let salt: B256 = b256!("3e423a81e6ff85145e727e92fd89e4775e1fb188ed74b9f1f6e3679b7af66626");

    c.bench_function("create3/derive_universal", |b| {
        b.iter(|| derive_universal_create3_address(black_box(deployer), black_box(salt)))
    });
}

criterion_group!(benches, bench_create3);
criterion_main!(benches);
