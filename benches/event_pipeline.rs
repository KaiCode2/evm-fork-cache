//! Phase 4 benchmarks: the event → state pipeline (Pillar B.2).
//!
//! Measures three things, all offline (mocked provider, in-memory logs):
//! - **decode** cost per decoder kind (ERC-20 `Transfer`, generic slot marker),
//!   isolating the pure `EventDecoder::decode` work (no apply);
//! - **ingest** throughput — [`EventPipeline::ingest_logs`] decoding **and**
//!   applying a block of logs, across batch sizes (1 → 1000);
//! - **reorg** purge cost — [`EventPipeline::reorg_to`] over a touched set of
//!   1 → 1000 addresses.
//!
//! A current-thread runtime drives only the async cache constructor; the pipeline
//! itself is synchronous and never touches the network.

use std::hint::black_box;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, Log, U256, hex, keccak256};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::events::{DecoderRegistry, EventDecoder, EventPipeline, StateView};
use evm_fork_cache::{Erc20TransferDecoder, StateUpdate};
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::{Builder, Runtime};

const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../fixtures/mock_erc20_runtime.hex");
const TOKEN: Address = Address::repeat_byte(0xAA);
const MARKER: Address = Address::repeat_byte(0xBB);

fn current_thread_rt() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// A cache with `TOKEN` and `MARKER` installed as storage-cleared accounts (so
/// unseeded slots read as zero — no RPC fallthrough).
fn seeded_cache(rt: &Runtime) -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
    let runtime = Bytecode::new_raw(Bytes::from(
        hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).unwrap(),
    ));
    let code_hash = runtime.hash_slow();
    for addr in [TOKEN, MARKER] {
        cache.db_mut().insert_account_info(
            addr,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 0,
                code: Some(runtime.clone()),
                code_hash,
                account_id: None,
            },
        );
        cache
            .db_mut()
            .replace_account_storage(addr, Default::default())
            .unwrap();
    }
    cache
}

fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
    let sig = keccak256(b"Transfer(address,address,uint256)");
    Log::new_unchecked(
        token,
        vec![sig, from.into_word(), to.into_word()],
        Bytes::copy_from_slice(&value.to_be_bytes::<32>()),
    )
}

struct EmptyView;
impl StateView for EmptyView {
    fn storage(&self, _address: Address, _slot: U256) -> Option<U256> {
        None
    }
}

/// A bench-local decoder that emits one absolute `Slot` write per log, keyed by
/// the log's address — so repeated ingest is idempotent (stable across iters).
struct AbsDecoder;
impl EventDecoder for AbsDecoder {
    fn decode(&self, log: &Log, _view: &dyn StateView) -> Vec<StateUpdate> {
        vec![StateUpdate::slot(log.address, U256::from(0), U256::from(1))]
    }
}

/// Pure `decode` cost per event kind (no apply).
fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    let erc20 = Erc20TransferDecoder::new(U256::from(3));
    let tlog = transfer_log(
        TOKEN,
        Address::repeat_byte(0x21),
        Address::repeat_byte(0x22),
        U256::from(100),
    );
    let empty = EmptyView;
    group.bench_function("erc20_transfer", |b| {
        b.iter(|| black_box(erc20.decode(black_box(&tlog), &empty)))
    });

    let marker = AbsDecoder;
    let marker_log = Log::new_unchecked(MARKER, vec![], Bytes::new());
    group.bench_function("absolute_slot_marker", |b| {
        b.iter(|| black_box(marker.decode(black_box(&marker_log), &empty)))
    });

    group.finish();
}

/// `ingest_logs` decode+apply throughput as the per-block log batch grows.
fn bench_ingest_batch(c: &mut Criterion) {
    let rt = current_thread_rt();
    let mut cache = seeded_cache(&rt);

    let mut group = c.benchmark_group("ingest_logs");
    for &n in &[1usize, 10, 100, 1_000] {
        let logs: Vec<Log> = (0..n)
            .map(|i| {
                Log::new_unchecked(
                    Address::repeat_byte((i % 251 + 1) as u8),
                    vec![],
                    Bytes::new(),
                )
            })
            .collect();
        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(AbsDecoder));
        let mut pipeline = EventPipeline::new(registry);

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &logs, |b, logs| {
            let mut block = 0u64;
            b.iter(|| {
                block += 1;
                black_box(pipeline.ingest_logs(&mut cache, block, black_box(logs)))
            })
        });
    }
    group.finish();
}

/// `reorg_to` purge cost over a touched set of N distinct addresses.
fn bench_reorg(c: &mut Criterion) {
    let rt = current_thread_rt();

    let mut group = c.benchmark_group("reorg_to");
    for &n in &[10usize, 100, 1_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    // Setup: a cache + pipeline with N addresses touched at block 1.
                    let mut cache = seeded_cache(&rt);
                    let mut registry = DecoderRegistry::new();
                    registry.register(Arc::new(AbsDecoder));
                    let mut pipeline = EventPipeline::new(registry);
                    let logs: Vec<Log> = (0..n)
                        .map(|i| {
                            let mut bytes = [0u8; 20];
                            bytes[0..8].copy_from_slice(&(i as u64).to_be_bytes());
                            Log::new_unchecked(Address::from(bytes), vec![], Bytes::new())
                        })
                        .collect();
                    pipeline.ingest_logs(&mut cache, 1, &logs);
                    (pipeline, cache)
                },
                |(mut pipeline, mut cache)| {
                    black_box(pipeline.reorg_to(&mut cache, 0));
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, bench_decode, bench_ingest_batch, bench_reorg);
criterion_main!(benches);
