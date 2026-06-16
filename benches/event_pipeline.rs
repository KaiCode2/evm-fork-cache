//! Phase 4 benchmarks: the event → state pipeline (Pillar B.2).
//!
//! Measures three things, all offline (mocked provider, in-memory logs):
//! - **decode** cost per event kind (ERC-20 `Transfer`, UniswapV3 `Swap`/`Mint`),
//!   isolating the pure `EventDecoder::decode` work (no apply);
//! - **ingest** throughput — [`EventPipeline::ingest_logs`] decoding **and**
//!   applying a block of logs, across batch sizes (1 → 1000);
//! - **reorg** purge cost — [`EventPipeline::reorg_to`] over a touched set of
//!   1 → 1000 addresses.
//!
//! A current-thread runtime drives only the async cache constructor; the pipeline
//! itself is synchronous and never touches the network.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use alloy_primitives::aliases::{I24, U160};
use alloy_primitives::{Address, Bytes, I256, Log, U256, hex, keccak256};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::{SolEvent, sol};
use alloy_transport::mock::Asserter;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use evm_fork_cache::cache::{EvmCache, V3_SLOT0_SLOT, v3_tick_info_storage_keys_with_base};
use evm_fork_cache::events::{DecoderRegistry, EventDecoder, EventPipeline, StateView};
use evm_fork_cache::{Erc20TransferDecoder, StateUpdate, UniswapV3Decoder, UniswapV3Layout};
use revm::state::{AccountInfo, Bytecode};
use tokio::runtime::{Builder, Runtime};

const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../fixtures/mock_erc20_runtime.hex");
const TOKEN: Address = Address::repeat_byte(0xAA);
const POOL: Address = Address::repeat_byte(0xBB);

fn current_thread_rt() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// A cache with `TOKEN` and `POOL` installed as storage-cleared accounts (so
/// unseeded slots read as zero — no RPC fallthrough).
fn seeded_cache(rt: &Runtime) -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = rt.block_on(EvmCache::new(Arc::new(provider), None));
    let runtime = Bytecode::new_raw(Bytes::from(
        hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).unwrap(),
    ));
    let code_hash = runtime.hash_slow();
    for addr in [TOKEN, POOL] {
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

sol! {
    event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick);
    event Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1);
}

fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
    let sig = keccak256(b"Transfer(address,address,uint256)");
    Log::new_unchecked(
        token,
        vec![sig, from.into_word(), to.into_word()],
        Bytes::copy_from_slice(&value.to_be_bytes::<32>()),
    )
}

fn swap_log(pool: Address, sqrt_price: u128, liquidity: u128, tick: i32) -> Log {
    let ev = Swap {
        sender: Address::repeat_byte(0x01),
        recipient: Address::repeat_byte(0x02),
        amount0: I256::try_from(-1i64).unwrap(),
        amount1: I256::try_from(1i64).unwrap(),
        sqrtPriceX96: U160::from(sqrt_price),
        liquidity,
        tick: I24::try_from(tick).unwrap(),
    };
    Log {
        address: pool,
        data: ev.encode_log_data(),
    }
}

fn mint_log(pool: Address, lower: i32, upper: i32, amount: u128) -> Log {
    let ev = Mint {
        sender: Address::repeat_byte(0x03),
        owner: Address::repeat_byte(0x04),
        tickLower: I24::try_from(lower).unwrap(),
        tickUpper: I24::try_from(upper).unwrap(),
        amount,
        amount0: U256::from(1),
        amount1: U256::from(1),
    };
    Log {
        address: pool,
        data: ev.encode_log_data(),
    }
}

/// A bench-local read-only [`StateView`] over a fixed map (for the V3 `Mint`
/// decode, which reads the current tick word).
struct MapView(HashMap<(Address, U256), U256>);
impl StateView for MapView {
    fn storage(&self, address: Address, slot: U256) -> Option<U256> {
        self.0.get(&(address, slot)).copied()
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
    let empty = MapView(HashMap::new());
    group.bench_function("erc20_transfer", |b| {
        b.iter(|| black_box(erc20.decode(black_box(&tlog), &empty)))
    });

    let v3 = UniswapV3Decoder::new().with_pool(POOL, UniswapV3Layout::uniswap(60));
    let slog = swap_log(POOL, 2_000_000, 7_500, 120);
    let mut slot0_view = HashMap::new();
    slot0_view.insert(
        (POOL, V3_SLOT0_SLOT),
        (U256::from(1u64) << 240) | U256::from(1_000_000u64),
    );
    // Seed the tick words the Mint reads (lower/upper) so it computes (not skips).
    let lo = v3_tick_info_storage_keys_with_base(60, evm_fork_cache::cache::V3_TICKS_BASE_SLOT)[0];
    let hi = v3_tick_info_storage_keys_with_base(120, evm_fork_cache::cache::V3_TICKS_BASE_SLOT)[0];
    slot0_view.insert((POOL, lo), U256::ZERO);
    slot0_view.insert((POOL, hi), U256::ZERO);
    let view = MapView(slot0_view);
    group.bench_function("v3_swap", |b| {
        b.iter(|| black_box(v3.decode(black_box(&slog), &view)))
    });
    let mlog = mint_log(POOL, 60, 120, 1_000);
    group.bench_function("v3_mint", |b| {
        b.iter(|| black_box(v3.decode(black_box(&mlog), &view)))
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
