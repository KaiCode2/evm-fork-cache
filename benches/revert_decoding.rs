//! Microbenchmarks for revert-reason decoding.

use std::hint::black_box;

use alloy_primitives::{Bytes, U256};
use alloy_sol_types::{SolError, sol};
use criterion::{Criterion, criterion_group, criterion_main};
use evm_fork_cache::errors::{RevertDecoder, decode_revert_reason};

sol! {
    #[derive(Debug)]
    error Error(string);
    #[derive(Debug)]
    error Panic(uint256);
    #[derive(Debug)]
    error SwapFailed(address router, bytes data);
}

fn error_string_data() -> Bytes {
    Bytes::from(Error::abi_encode(&Error(
        "transfer amount exceeds balance".into(),
    )))
}

fn panic_data() -> Bytes {
    Bytes::from(Panic::abi_encode(&Panic(U256::from(0x11))))
}

fn custom_data() -> Bytes {
    Bytes::from(
        SwapFailed {
            router: alloy_primitives::Address::repeat_byte(0x42),
            data: Bytes::from_static(b"reverted"),
        }
        .abi_encode(),
    )
}

fn bench_decode(c: &mut Criterion) {
    let error = error_string_data();
    let panic = panic_data();
    let custom = custom_data();
    let unknown = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x01]);

    let decoder = RevertDecoder::new().with_error::<SwapFailed>();

    let mut group = c.benchmark_group("revert_decode");

    // Standard built-ins via the free function (no registry).
    group.bench_function("standard/error_string", |b| {
        b.iter(|| decode_revert_reason(black_box(&error)))
    });
    group.bench_function("standard/panic", |b| {
        b.iter(|| decode_revert_reason(black_box(&panic)))
    });

    // Through a decoder that also knows a custom error.
    group.bench_function("decoder/custom", |b| {
        b.iter(|| decoder.decode(black_box(&custom)))
    });
    group.bench_function("decoder/unknown", |b| {
        b.iter(|| decoder.decode(black_box(&unknown)))
    });

    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
