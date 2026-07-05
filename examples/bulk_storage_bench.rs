//! Live benchmark: bulk storage extraction vs batched `eth_getStorageAt`.
//!
//! Measures the call-override bulk loader (`bulk_storage` module) against the
//! crate's default point-read batch fetcher on a real endpoint, and captures
//! the numbers recorded in `docs/bulk-storage-extraction.md`:
//!
//! 1. **Correctness spot-check** — bulk values must equal `eth_getStorageAt`
//!    ground truth at the same pinned block.
//! 2. **Single-target scaling** — N slots of one contract (WETH), N up to 15k.
//! 3. **Multi-contract multicall** — 20 mainnet tokens × 25 slots in one call.
//! 4. **Uniswap V3 pool tick-range load** — the `evm-amm-state` cold-start
//!    shape: statics + full tickBitmap, then every initialized tick +
//!    observations, in 2-3 `eth_call`s total.
//! 5. **Gzip on/off** — end-to-end latency and raw wire bytes for the largest
//!    nonzero-heavy response.
//! 6. **`eth_callMany` vs `eth_call`** — same payloads through both dispatch
//!    modes (20 CU/request vs 26 CU/call on Alchemy).
//! 7. **Contract fleet** — 100 distinct contracts × 30 slots in one dispatch.
//! 8. **Custom storage program** — the one-shot V3 observation-ring loader
//!    (data-dependent loads derived in-EVM, zero calldata).
//! 9. **Companion extractors** — account fields (balance + codehash) and
//!    block context in one call each.
//! 10. **Chunk-ceiling probe** — raise slots-per-call until the provider
//!     rejects it, to find the endpoint's real `eth_call` budget.
//! 11. **Verified code seeding** — cold-start materialization of N known
//!     contracts: locally seeded templates + one bulk `verify_code_seeds`
//!     call vs per-account `ensure_account` (balance + nonce + code reads).
//!
//! Gated on `RPC_URL` (skips when unset, like the other live benches):
//!
//! ```sh
//! RPC_URL=https://eth-mainnet.g.alchemy.com/v2/<key> \
//!     cargo run --release --example bulk_storage_bench
//! ```
//!
//! Optional env knobs: `BULK_BENCH_SAMPLES` (default 3),
//! `BULK_BENCH_BASELINE_MAX` (default 1000 — caps how many slots the
//! *point-read* baseline fetches, since that path costs 20 CU per slot),
//! `BULK_BENCH_PROBE=0` (skip the ceiling probe),
//! `BULK_BENCH_SCENARIOS=4,11` (comma-separated scenario numbers to run;
//! default all — handy for refreshing one table without paying for the rest).
//!
//! The full default run costs roughly 130k CU on Alchemy, dominated by the
//! point-read baselines the bulk path is being compared against.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{Address, Bytes, I256, U256, address, hex, keccak256};
use alloy_provider::network::AnyNetwork;
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_client::RpcClient;
use alloy_transport_http::Http;
use anyhow::{Context, Result, bail};
use evm_fork_cache::bulk_storage::{
    BulkCallConfig, CallDispatch, STORAGE_EXTRACTOR_CODE, StorageProgram,
    bulk_call_storage_fetcher, fetch_account_fields_bulk, fetch_block_context, pack_slots_calldata,
    planned_call_count, run_storage_program,
};
use evm_fork_cache::cache::{EvmCache, StorageBatchFetchFn};

/// Alchemy CU prices (https://www.alchemy.com/docs/reference/compute-unit-costs).
const CU_GET_STORAGE_AT: u64 = 20;
const CU_ETH_CALL: u64 = 26;

const WETH: Address = address!("C02aaa39b223FE8D0A0e5C4F27eAD9083C756Cc2");
/// Uniswap V3 USDC/WETH 0.05% pool (fee 500, tick spacing 10).
const USDC_WETH_V3_POOL: Address = address!("88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640");
const POOL_TICK_SPACING: i32 = 10;
/// Uniswap V3 pool storage layout: `ticks` mapping and `tickBitmap` mapping.
const POOL_TICKS_SLOT: u64 = 5;
const POOL_TICK_BITMAP_SLOT: u64 = 6;
const POOL_OBSERVATIONS_SLOT: u64 = 8;

/// Well-known mainnet ERC-20s for the multi-contract scenario. Only used as
/// storage sources — the measured cost is identical whatever the slots hold.
const TOKENS: [Address; 20] = [
    WETH,
    address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"), // USDC
    address!("dAC17F958D2ee523a2206206994597C13D831ec7"), // USDT
    address!("6B175474E89094C44Da98b954EedeAC495271d0F"), // DAI
    address!("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"), // WBTC
    address!("1f9840a85d5aF5bf1D1762F925BDADdC4201F984"), // UNI
    address!("514910771AF9Ca656af840dff83E8264EcF986CA"), // LINK
    address!("7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9"), // AAVE
    address!("9f8F72aA9304c8B593d555F12eF6589cC3A579A2"), // MKR
    address!("5A98FcBEA516Cf06857215779Fd812CA3beF1B32"), // LDO
    address!("ae7ab96520DE3A18E5e111B5EaAb095312D7fE84"), // stETH
    address!("D533a949740bb3306d119CC777fa900bA034cd52"), // CRV
    address!("c00e94Cb662C3520282E6f5717214004A7f26888"), // COMP
    address!("C011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F"), // SNX
    address!("0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e"), // YFI
    address!("6B3595068778DD592e39A122f4f5a5cF09C90fE2"), // SUSHI
    address!("111111111117dC0aa78b770fA6A738034120C302"), // 1INCH
    address!("c944E90C64B2c07662A292be6244BDf05Cda44a7"), // GRT
    address!("7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0"), // MATIC
    address!("95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE"), // SHIB
];

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn make_provider(rpc_url: &str, gzip: bool) -> Result<Arc<RootProvider<AnyNetwork>>> {
    let mut builder = reqwest::Client::builder();
    builder = if gzip {
        builder.gzip(true)
    } else {
        builder.no_gzip()
    };
    let client = builder.build().context("build reqwest client")?;
    let http = Http::with_client(client, rpc_url.parse().context("parse RPC_URL")?);
    Ok(Arc::new(RootProvider::<AnyNetwork>::new(RpcClient::new(
        http, false,
    ))))
}

/// Deterministic pseudo-random slot keys (uniform 256-bit, like the Dedaub
/// harness) so runs are reproducible at a pinned block.
fn synthetic_slots(n: usize) -> Vec<U256> {
    (0..n)
        .map(|i| {
            let mut seed = [0u8; 40];
            seed[..8].copy_from_slice(b"efc-bulk");
            seed[8..16].copy_from_slice(&(i as u64).to_be_bytes());
            U256::from_be_bytes(keccak256(seed).0)
        })
        .collect()
}

/// Storage key of `mapping(intN => ..)` entry: `keccak256(int256(key) . slot)`.
fn signed_mapping_key(key: i32, mapping_slot: u64) -> U256 {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(
        &I256::try_from(key)
            .expect("fits i32")
            .into_raw()
            .to_be_bytes::<32>(),
    );
    buf[32..].copy_from_slice(&U256::from(mapping_slot).to_be_bytes::<32>());
    U256::from_be_bytes(keccak256(buf).0)
}

struct SampleStats {
    median: Duration,
    min: Duration,
    max: Duration,
    ok: usize,
    err: usize,
    first_error: Option<String>,
}

/// Run `fetcher` `samples` times over the same request set and report medians.
/// Fetchers are the crate's synchronous seam; on this multi-thread runtime the
/// internal `block_in_place` bridge is valid.
fn run_samples(
    fetcher: &StorageBatchFetchFn,
    requests: &[(Address, U256)],
    block: BlockId,
    samples: usize,
) -> SampleStats {
    let mut durations = Vec::with_capacity(samples);
    let (mut ok, mut err, mut first_error) = (0usize, 0usize, None);
    for _ in 0..samples {
        let started = Instant::now();
        let results = fetcher(requests.to_vec(), block);
        durations.push(started.elapsed());
        ok = results.iter().filter(|(_, _, r)| r.is_ok()).count();
        err = results.len() - ok;
        if first_error.is_none() {
            first_error = results
                .iter()
                .find_map(|(_, _, r)| r.as_ref().err().map(|e| e.to_string()));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    durations.sort();
    SampleStats {
        median: durations[durations.len() / 2],
        min: durations[0],
        max: durations[durations.len() - 1],
        ok,
        err,
        first_error,
    }
}

fn fetch_map(
    fetcher: &StorageBatchFetchFn,
    requests: &[(Address, U256)],
    block: BlockId,
) -> Result<std::collections::HashMap<(Address, U256), U256>> {
    let mut map = std::collections::HashMap::with_capacity(requests.len());
    for (addr, slot, result) in fetcher(requests.to_vec(), block) {
        match result {
            Ok(value) => {
                map.insert((addr, slot), value);
            }
            Err(e) => bail!("fetch failed for {addr} slot {slot:#x}: {e}"),
        }
    }
    Ok(map)
}

fn ms(d: Duration) -> String {
    format!("{:.0} ms", d.as_secs_f64() * 1000.0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let rpc_url = match std::env::var("RPC_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            eprintln!(
                "RPC_URL not set — skipping bulk_storage_bench. \
                 Set RPC_URL=<https endpoint> to run it."
            );
            return Ok(());
        }
    };
    let samples = env_usize("BULK_BENCH_SAMPLES", 3).max(1);
    let baseline_max = env_usize("BULK_BENCH_BASELINE_MAX", 1000);
    let run_probe = std::env::var("BULK_BENCH_PROBE").as_deref() != Ok("0");
    let scenarios: Option<std::collections::HashSet<usize>> =
        std::env::var("BULK_BENCH_SCENARIOS").ok().map(|raw| {
            raw.split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect()
        });
    let enabled = |n: usize| scenarios.as_ref().is_none_or(|set| set.contains(&n));

    let gzip_provider = make_provider(&rpc_url, true)?;
    let identity_provider = make_provider(&rpc_url, false)?;

    let chain_id = gzip_provider.get_chain_id().await?;
    let latest = gzip_provider.get_block_number().await?;
    // Pin a few blocks back so every sample reads settled, identical state.
    let pinned = latest.saturating_sub(8);
    let block = BlockId::Number(BlockNumberOrTag::Number(pinned));
    println!("# Bulk storage extraction — live benchmark\n");
    println!("- chain id: {chain_id}");
    println!("- pinned block: {pinned}");
    println!("- samples per measurement: {samples}");
    println!("- CU prices: eth_getStorageAt = {CU_GET_STORAGE_AT}, eth_call = {CU_ETH_CALL}\n");

    // The production baseline: the cache's own default point-read batch
    // fetcher (JSON-RPC batches of eth_getStorageAt, Slow preset by default).
    let cache = EvmCache::builder(gzip_provider.clone())
        .block(block)
        .build()
        .await;
    let point_fetcher = cache
        .storage_batch_fetcher()
        .cloned()
        .context("provider-backed cache exposes the default fetcher")?;
    let config = BulkCallConfig::default();
    let bulk = bulk_call_storage_fetcher(gzip_provider.clone(), config);
    let bulk_identity = bulk_call_storage_fetcher(identity_provider.clone(), config);

    if enabled(1) {
        scenario_correctness(&bulk, &point_fetcher, block)?;
    }
    if enabled(2) {
        scenario_single_target(&bulk, &point_fetcher, block, samples, baseline_max, config)?;
    }
    if enabled(3) {
        scenario_multi_target(&bulk, &point_fetcher, block, samples, baseline_max, config)?;
    }
    let tick_slots = if enabled(4) {
        scenario_univ3_pool(&bulk, &point_fetcher, block, samples, config)?
    } else {
        Vec::new()
    };
    if enabled(5) {
        scenario_gzip(
            &bulk,
            &bulk_identity,
            &rpc_url,
            block,
            pinned,
            samples,
            &tick_slots,
        )
        .await?;
    }
    if enabled(6) {
        scenario_call_many(&gzip_provider, &bulk, block, samples, &tick_slots)?;
    }
    if enabled(7) {
        scenario_fleet(&bulk, block, samples, config)?;
    }
    if enabled(8) {
        scenario_custom_program(&gzip_provider, &bulk, block).await?;
    }
    if enabled(9) {
        scenario_companion_extractors(&gzip_provider, block, pinned).await?;
    }
    if run_probe && enabled(10) {
        scenario_ceiling(&gzip_provider, block).await;
    }
    if enabled(11) {
        scenario_code_seeding(&gzip_provider, block, samples).await?;
    }

    println!("\nDone. Copy the tables above into docs/bulk-storage-extraction.md.");
    Ok(())
}

/// 1. Bulk values must be byte-identical to eth_getStorageAt ground truth.
fn scenario_correctness(
    bulk: &StorageBatchFetchFn,
    point: &StorageBatchFetchFn,
    block: BlockId,
) -> Result<()> {
    println!("## 1. Correctness spot-check\n");
    let mut requests: Vec<(Address, U256)> = Vec::new();
    for slot in 0..5u64 {
        requests.push((WETH, U256::from(slot)));
    }
    for slot in 0..9u64 {
        requests.push((USDC_WETH_V3_POOL, U256::from(slot)));
    }
    // Two real WETH balance slots: keccak256(holder . 3).
    for holder in [
        address!("28C6c06298d514Db089934071355E5743bf21d60"),
        address!("F04a5cC80B1E94C69B48f5ee68a08CD2F09A7c3E"),
    ] {
        let mut buf = [0u8; 64];
        buf[12..32].copy_from_slice(holder.as_slice());
        buf[32..].copy_from_slice(&U256::from(3u64).to_be_bytes::<32>());
        requests.push((WETH, U256::from_be_bytes(keccak256(buf).0)));
    }

    let bulk_values = fetch_map(bulk, &requests, block)?;
    let point_values = fetch_map(point, &requests, block)?;
    let mut nonzero = 0usize;
    for key in bulk_values.keys() {
        let (b, p) = (bulk_values[key], point_values[key]);
        if b != p {
            bail!(
                "MISMATCH at {} slot {:#x}: bulk={b:#x} point={p:#x}",
                key.0,
                key.1
            );
        }
        if !b.is_zero() {
            nonzero += 1;
        }
    }
    println!(
        "{} slots verified identical to eth_getStorageAt ({} nonzero).\n",
        requests.len(),
        nonzero
    );
    Ok(())
}

/// 2. N slots of one contract: bulk vs point-read baseline.
fn scenario_single_target(
    bulk: &StorageBatchFetchFn,
    point: &StorageBatchFetchFn,
    block: BlockId,
    samples: usize,
    baseline_max: usize,
    config: BulkCallConfig,
) -> Result<()> {
    println!("## 2. Single-target scaling (WETH, pseudo-random slots)\n");
    println!("| Slots | Bulk calls | Bulk median | Bulk CU | Point median | Point CU | CU ratio |");
    println!("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for n in [10usize, 100, 1_000, 5_000, 10_000, 15_000] {
        let requests: Vec<(Address, U256)> =
            synthetic_slots(n).into_iter().map(|s| (WETH, s)).collect();
        let calls = planned_call_count(&requests, &config);
        let bulk_stats = run_samples(bulk, &requests, block, samples);
        let bulk_cu = calls as u64 * CU_ETH_CALL;
        let point_cu = n as u64 * CU_GET_STORAGE_AT;

        let point_cell = if n <= baseline_max {
            let stats = run_samples(point, &requests, block, samples);
            if stats.err > 0 {
                format!(
                    "{} ({} errs: {})",
                    ms(stats.median),
                    stats.err,
                    stats.first_error.as_deref().unwrap_or("?")
                )
            } else {
                ms(stats.median)
            }
        } else {
            "— (skipped)".to_string()
        };
        let bulk_cell = if bulk_stats.err > 0 {
            format!(
                "{} ({} errs: {})",
                ms(bulk_stats.median),
                bulk_stats.err,
                bulk_stats.first_error.as_deref().unwrap_or("?")
            )
        } else {
            ms(bulk_stats.median)
        };
        println!(
            "| {n} | {calls} | {bulk_cell} | {bulk_cu} | {point_cell} | {point_cu} | {:.0}x |",
            point_cu as f64 / bulk_cu as f64
        );
        let _ = bulk_stats.ok;
    }
    println!();
    Ok(())
}

/// 3. Many contracts in one multicall dispatch.
fn scenario_multi_target(
    bulk: &StorageBatchFetchFn,
    point: &StorageBatchFetchFn,
    block: BlockId,
    samples: usize,
    baseline_max: usize,
    config: BulkCallConfig,
) -> Result<()> {
    println!("## 3. Multi-contract multicall (20 tokens x 25 slots = 500 slots)\n");
    let mut requests: Vec<(Address, U256)> = Vec::new();
    for token in TOKENS {
        for slot in 0..25u64 {
            requests.push((token, U256::from(slot)));
        }
    }
    let calls = planned_call_count(&requests, &config);
    let bulk_stats = run_samples(bulk, &requests, block, samples);
    println!(
        "- bulk: {} calls, median {} (min {}, max {}), {} CU, {} errs",
        calls,
        ms(bulk_stats.median),
        ms(bulk_stats.min),
        ms(bulk_stats.max),
        calls as u64 * CU_ETH_CALL,
        bulk_stats.err,
    );
    if requests.len() <= baseline_max {
        let point_stats = run_samples(point, &requests, block, samples);
        println!(
            "- point reads: median {} (min {}, max {}), {} CU, {} errs",
            ms(point_stats.median),
            ms(point_stats.min),
            ms(point_stats.max),
            requests.len() as u64 * CU_GET_STORAGE_AT,
            point_stats.err,
        );
    }
    println!();
    Ok(())
}

/// Scenario 4 — the evm-amm-state shape: statics + full tickBitmap, then
/// every initialized tick + observations. Returns the phase-2 slot list for
/// the gzip scenario.
fn scenario_univ3_pool(
    bulk: &StorageBatchFetchFn,
    point: &StorageBatchFetchFn,
    block: BlockId,
    samples: usize,
    config: BulkCallConfig,
) -> Result<Vec<(Address, U256)>> {
    println!("## 4. Uniswap V3 USDC/WETH 0.05% pool — full tick-range load\n");

    // Phase 1: statics (slot0..8) + every tickBitmap word over the full range.
    let compressed_bound = 887_272 / POOL_TICK_SPACING; // MIN/MAX_TICK / spacing
    let (min_word, max_word) = (
        (-compressed_bound) >> 8, // arithmetic shift = floor division
        compressed_bound >> 8,
    );
    let mut phase1: Vec<(Address, U256)> = (0..9u64)
        .map(|slot| (USDC_WETH_V3_POOL, U256::from(slot)))
        .collect();
    let mut word_keys = Vec::new();
    for word in min_word..=max_word {
        let key = signed_mapping_key(word, POOL_TICK_BITMAP_SLOT);
        word_keys.push((word, key));
        phase1.push((USDC_WETH_V3_POOL, key));
    }

    let phase1_calls = planned_call_count(&phase1, &config);
    let phase1_stats = run_samples(bulk, &phase1, block, samples);
    let values = fetch_map(bulk, &phase1, block)?;

    // Decode: observation cardinality from slot0, initialized ticks from the
    // bitmap words.
    let slot0 = values[&(USDC_WETH_V3_POOL, U256::from(0u64))];
    let cardinality: u64 = ((slot0 >> 200usize) & U256::from(0xffffu64)).to::<u64>();
    let mut ticks: Vec<i32> = Vec::new();
    for (word, key) in &word_keys {
        let bitmap = values[&(USDC_WETH_V3_POOL, *key)];
        if bitmap.is_zero() {
            continue;
        }
        for bit in 0..256usize {
            if bitmap.bit(bit) {
                ticks.push((word * 256 + bit as i32) * POOL_TICK_SPACING);
            }
        }
    }

    // Phase 2: 4 slots per initialized tick + the observation ring.
    let mut phase2: Vec<(Address, U256)> = Vec::new();
    for tick in &ticks {
        let base = signed_mapping_key(*tick, POOL_TICKS_SLOT);
        for offset in 0..4u64 {
            phase2.push((USDC_WETH_V3_POOL, base + U256::from(offset)));
        }
    }
    for i in 0..cardinality {
        phase2.push((USDC_WETH_V3_POOL, U256::from(POOL_OBSERVATIONS_SLOT + i)));
    }
    let phase2_calls = planned_call_count(&phase2, &config);
    let phase2_stats = run_samples(bulk, &phase2, block, samples);
    let phase2_values = fetch_map(bulk, &phase2, block)?;

    // Sanity: every initialized tick must have nonzero liquidityGross, and a
    // spot sample must match point-read ground truth.
    let mut zero_gross = 0usize;
    for tick in &ticks {
        let base = signed_mapping_key(*tick, POOL_TICKS_SLOT);
        if phase2_values[&(USDC_WETH_V3_POOL, base)].is_zero() {
            zero_gross += 1;
        }
    }
    let spot: Vec<(Address, U256)> = phase2
        .iter()
        .step_by(phase2.len().max(1) / 8 + 1)
        .copied()
        .collect();
    let ground_truth = fetch_map(point, &spot, block)?;
    for (key, expected) in &ground_truth {
        if phase2_values[key] != *expected {
            bail!("tick-slot mismatch at {:#x}", key.1);
        }
    }

    let total_slots = phase1.len() + phase2.len();
    let total_calls = phase1_calls + phase2_calls;
    println!(
        "- initialized ticks: {}, observation cardinality: {cardinality}",
        ticks.len()
    );
    println!(
        "- phase 1 (statics + {} bitmap words): {} slots, {} call(s), median {}",
        word_keys.len(),
        phase1.len(),
        phase1_calls,
        ms(phase1_stats.median),
    );
    println!(
        "- phase 2 (ticks + observations): {} slots, {} call(s), median {}",
        phase2.len(),
        phase2_calls,
        ms(phase2_stats.median),
    );
    println!(
        "- total: {} slots in {} eth_calls = {} CU (vs {} CU as point reads, {:.0}x cheaper)",
        total_slots,
        total_calls,
        total_calls as u64 * CU_ETH_CALL,
        total_slots as u64 * CU_GET_STORAGE_AT,
        (total_slots as u64 * CU_GET_STORAGE_AT) as f64 / (total_calls as u64 * CU_ETH_CALL) as f64,
    );
    println!(
        "- spot-verified {} slots against eth_getStorageAt; {} ticks with zero liquidityGross\n",
        ground_truth.len(),
        zero_gross,
    );
    Ok(phase2)
}

/// 5. Gzip vs identity on the largest nonzero-heavy payload.
async fn scenario_gzip(
    bulk_gzip: &StorageBatchFetchFn,
    bulk_identity: &StorageBatchFetchFn,
    rpc_url: &str,
    block: BlockId,
    pinned: u64,
    samples: usize,
    tick_slots: &[(Address, U256)],
) -> Result<()> {
    println!("## 5. Gzip vs identity (tick-range payload)\n");
    if tick_slots.is_empty() {
        println!("(no tick slots — skipped)\n");
        return Ok(());
    }
    let gzip_stats = run_samples(bulk_gzip, tick_slots, block, samples);
    let identity_stats = run_samples(bulk_identity, tick_slots, block, samples);
    println!(
        "- end-to-end ({} slots): gzip median {}, identity median {}",
        tick_slots.len(),
        ms(gzip_stats.median),
        ms(identity_stats.median),
    );

    // Wire-level: one raw eth_call with auto-decompression disabled so the
    // compressed byte count is observable.
    let slots: Vec<U256> = tick_slots.iter().map(|(_, s)| *s).collect();
    let calldata = pack_slots_calldata(&slots);
    let mut overrides = serde_json::Map::new();
    overrides.insert(
        format!("{USDC_WETH_V3_POOL}"),
        serde_json::json!({ "code": format!("0x{}", hex::encode(STORAGE_EXTRACTOR_CODE)) }),
    );
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            { "to": USDC_WETH_V3_POOL, "data": format!("0x{}", hex::encode(&calldata)) },
            format!("{pinned:#x}"),
            overrides,
        ],
    });
    let raw_client = reqwest::Client::builder().no_gzip().build()?;
    for encoding in ["identity", "gzip"] {
        let started = Instant::now();
        let response = raw_client
            .post(rpc_url)
            .header("Accept-Encoding", encoding)
            .json(&body)
            .send()
            .await?;
        let served = response
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("identity")
            .to_string();
        let bytes = response.bytes().await?;
        println!(
            "- wire ({encoding:>8}): {:>9} bytes in {}, content-encoding: {served}",
            bytes.len(),
            ms(started.elapsed()),
        );
    }
    println!();
    Ok(())
}

/// Scenario 6 — eth_callMany (20 CU/request on Alchemy) vs per-call
/// eth_call (26 CU each): same payloads, both dispatch modes.
fn scenario_call_many(
    provider: &Arc<RootProvider<AnyNetwork>>,
    bulk_per_call: &StorageBatchFetchFn,
    block: BlockId,
    samples: usize,
    tick_slots: &[(Address, U256)],
) -> Result<()> {
    println!("## 6. eth_callMany vs eth_call dispatch\n");
    let call_many = bulk_call_storage_fetcher(
        provider.clone(),
        BulkCallConfig {
            dispatch: CallDispatch::CallMany,
            ..BulkCallConfig::default()
        },
    );

    if !tick_slots.is_empty() {
        let per_call = run_samples(bulk_per_call, tick_slots, block, samples);
        let many = run_samples(&call_many, tick_slots, block, samples);
        println!(
            "- tick payload ({} slots, 1 chunk): eth_call median {} / 26 CU, eth_callMany median {} / 20 CU ({} errs)",
            tick_slots.len(),
            ms(per_call.median),
            ms(many.median),
            many.err,
        );
    }

    // A 25k-slot job: three 10k chunks per-call vs one callMany request.
    let big: Vec<(Address, U256)> = synthetic_slots(25_000)
        .into_iter()
        .map(|s| (WETH, s))
        .collect();
    let per_call = run_samples(bulk_per_call, &big, block, samples);
    let many = run_samples(&call_many, &big, block, samples);
    println!(
        "- 25,000 slots: eth_call 3 chunks median {} / 78 CU, eth_callMany 1 request median {} / 20 CU ({} errs)\n",
        ms(per_call.median),
        ms(many.median),
        many.err,
    );
    Ok(())
}

/// Scenario 7 — fleet dispatch: 100 distinct contracts × 30 slots through one
/// multicall. Synthetic addresses (empty accounts) keep this honest about
/// dispatch overhead — gas costs are identical whatever the slots hold.
fn scenario_fleet(
    bulk: &StorageBatchFetchFn,
    block: BlockId,
    samples: usize,
    config: BulkCallConfig,
) -> Result<()> {
    println!("## 7. Contract fleet (100 contracts x 30 slots = 3,000 slots)\n");
    let mut requests: Vec<(Address, U256)> = Vec::new();
    for c in 0..100u64 {
        let mut seed = [0u8; 16];
        seed[..8].copy_from_slice(b"efcfleet");
        seed[8..].copy_from_slice(&c.to_be_bytes());
        let addr = Address::from_slice(&keccak256(seed)[12..]);
        for slot in 0..30u64 {
            requests.push((addr, U256::from(slot)));
        }
    }
    let calls = planned_call_count(&requests, &config);
    let stats = run_samples(bulk, &requests, block, samples);
    println!(
        "- {} slots across 100 contracts: {} call(s), median {} (min {}, max {}), {} CU vs {} CU as point reads ({} errs)\n",
        requests.len(),
        calls,
        ms(stats.median),
        ms(stats.min),
        ms(stats.max),
        calls as u64 * CU_ETH_CALL,
        requests.len() as u64 * CU_GET_STORAGE_AT,
        stats.err,
    );
    Ok(())
}

/// The one-shot Uniswap V3 observation-ring loader: reads the ring
/// cardinality from slot0 *inside the EVM*, then returns the whole ring —
/// zero calldata, one call. The offline revm test for this exact bytecode
/// lives in `tests/bulk_storage.rs`.
const OBSERVATION_RING_PROGRAM: &[u8] =
    &hex!("5f5460c81c61ffff165f5b81811460215780600801548160051b52600101600a565b5060051b5ff3");

/// Scenario 8 — a custom storage program (data-dependent loads in-EVM).
async fn scenario_custom_program(
    provider: &Arc<RootProvider<AnyNetwork>>,
    bulk: &StorageBatchFetchFn,
    block: BlockId,
) -> Result<()> {
    println!("## 8. Custom storage program: one-shot V3 observation ring\n");
    let program = StorageProgram {
        target: USDC_WETH_V3_POOL,
        code: alloy_primitives::Bytes::from_static(OBSERVATION_RING_PROGRAM),
        calldata: alloy_primitives::Bytes::new(),
    };
    let started = Instant::now();
    let bytes = run_storage_program(provider.as_ref(), block, &program)
        .await
        .map_err(|e| anyhow::anyhow!("program failed: {e}"))?;
    let elapsed = started.elapsed();
    let cardinality = bytes.len() / 32;

    // Ground truth: the same ring via slot-list extraction.
    let ring_slots: Vec<(Address, U256)> = (0..cardinality as u64)
        .map(|i| (USDC_WETH_V3_POOL, U256::from(POOL_OBSERVATIONS_SLOT + i)))
        .collect();
    let expected = fetch_map(bulk, &ring_slots, block)?;
    for (i, chunk) in bytes.chunks_exact(32).enumerate() {
        let key = (
            USDC_WETH_V3_POOL,
            U256::from(POOL_OBSERVATIONS_SLOT + i as u64),
        );
        if U256::from_be_slice(chunk) != expected[&key] {
            anyhow::bail!("program output diverged from slot-list extraction at index {i}");
        }
    }
    println!(
        "- {cardinality} observation slots in ONE call with ZERO calldata, {} — the program \
         derived the ring size from slot0 in-EVM; all values match slot-list extraction.\n",
        ms(elapsed),
    );
    Ok(())
}

/// Scenario 9 — companion extractors: account fields + block context.
async fn scenario_companion_extractors(
    provider: &Arc<RootProvider<AnyNetwork>>,
    block: BlockId,
    pinned: u64,
) -> Result<()> {
    println!("## 9. Companion extractors\n");
    let started = Instant::now();
    let fields = fetch_account_fields_bulk(provider.as_ref(), &TOKENS, block)
        .await
        .map_err(|e| anyhow::anyhow!("account fields failed: {e}"))?;
    let nonzero_balances = fields.iter().filter(|(_, f)| !f.balance.is_zero()).count();
    println!(
        "- account fields: balance + codehash for {} contracts in one 26-CU call, {} \
         (vs {} CU via eth_getBalance + eth_getCode); {} with nonzero native balance",
        fields.len(),
        ms(started.elapsed()),
        fields.len() as u64 * 2 * 20,
        nonzero_balances,
    );

    let started = Instant::now();
    let ctx = fetch_block_context(provider.as_ref(), block)
        .await
        .map_err(|e| anyhow::anyhow!("block context failed: {e}"))?;
    anyhow::ensure!(ctx.number == pinned, "context block must match the pin");
    println!(
        "- block context: number={} timestamp={} basefee={} gas_limit={} chain_id={} in one call, {}\n",
        ctx.number,
        ctx.timestamp,
        ctx.basefee,
        ctx.gas_limit,
        ctx.chain_id,
        ms(started.elapsed()),
    );
    Ok(())
}

/// Scenario 10 — raise slots-per-call until the provider says no.
async fn scenario_ceiling(provider: &Arc<RootProvider<AnyNetwork>>, block: BlockId) {
    println!("## 10. Chunk-ceiling probe (single eth_call, ~2,664 gas/slot)\n");
    println!("| Slots/call | Est. gas | Result |");
    println!("| ---: | ---: | --- |");
    for n in [15_000usize, 20_000, 25_000, 30_000, 40_000, 50_000] {
        let config = BulkCallConfig {
            max_slots_per_call: n,
            max_concurrent_calls: 1,
            ..BulkCallConfig::default()
        };
        let fetcher = bulk_call_storage_fetcher(provider.clone(), config);
        let requests: Vec<(Address, U256)> =
            synthetic_slots(n).into_iter().map(|s| (WETH, s)).collect();
        let started = Instant::now();
        let results = tokio::task::spawn_blocking({
            let requests = requests.clone();
            move || fetcher(requests, block)
        })
        .await
        .expect("probe task");
        let elapsed = started.elapsed();
        let errs = results.iter().filter(|(_, _, r)| r.is_err()).count();
        let est_gas = n as u64 * 2_664;
        if errs == 0 {
            println!("| {n} | {est_gas} | ok in {} |", ms(elapsed));
        } else {
            let first = results
                .iter()
                .find_map(|(_, _, r)| r.as_ref().err().map(|e| e.to_string()))
                .unwrap_or_default();
            let trimmed: String = first.chars().take(120).collect();
            println!("| {n} | {est_gas} | FAILED: {trimmed} |");
            break;
        }
    }
    println!();
}

/// Scenario 11 — verified code seeding: the 0.2.0 cold-start path where the
/// adapter already embeds each contract's deployed bytecode. Seeding writes
/// the templates locally and ONE bulk account-fields `eth_call` verifies every
/// claim (and materializes real balances), versus the classic materialization
/// of the same accounts via `ensure_account` — three point reads each
/// (`eth_getBalance` + `eth_getTransactionCount` + `eth_getCode`), with the
/// full runtime bytecode on the wire.
async fn scenario_code_seeding(
    provider: &Arc<RootProvider<AnyNetwork>>,
    block: BlockId,
    samples: usize,
) -> Result<()> {
    println!("## 11. Verified code seeding (cold-start account materialization)\n");

    // The templates an adapter would embed at build time — fetched once here,
    // untimed, purely to have byte-exact runtime code for the pinned block.
    let mut templates: Vec<(Address, Bytes)> = Vec::with_capacity(TOKENS.len());
    for token in TOKENS {
        let code = provider.get_code_at(token).block_id(block).await?;
        anyhow::ensure!(!code.is_empty(), "{token} should have runtime code");
        templates.push((token, code));
    }
    let code_bytes: usize = templates.iter().map(|(_, code)| code.len()).sum();

    // Baseline: fresh cache, `ensure_account` per contract — the pre-seeding
    // way to materialize known accounts before simulating against them.
    let mut baseline = Vec::with_capacity(samples);
    for _ in 0..samples {
        let mut cache = EvmCache::builder(provider.clone())
            .block(block)
            .build()
            .await;
        let started = Instant::now();
        for (token, _) in &templates {
            cache
                .ensure_account(*token)
                .await
                .map_err(|e| anyhow::anyhow!("ensure_account({token}): {e}"))?;
        }
        baseline.push(started.elapsed());
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    baseline.sort();

    // Seeded: fresh cache, templates written locally, one bulk verify call.
    let mut seeded = Vec::with_capacity(samples);
    for _ in 0..samples {
        let mut cache = EvmCache::builder(provider.clone())
            .block(block)
            .build()
            .await;
        let started = Instant::now();
        for (token, code) in &templates {
            cache
                .seed_account_code(*token, code.clone())
                .map_err(|e| anyhow::anyhow!("seed_account_code({token}): {e}"))?;
        }
        let report = cache
            .verify_code_seeds()
            .map_err(|e| anyhow::anyhow!("verify_code_seeds: {e}"))?;
        seeded.push(started.elapsed());
        anyhow::ensure!(
            report.verified.len() == templates.len(),
            "every template should verify: {report:?}"
        );
        anyhow::ensure!(
            cache.pending_code_seeds().is_empty(),
            "no claim should stay pending after a clean sweep"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    seeded.sort();

    let n = templates.len() as u64;
    // ensure_account = eth_getBalance + eth_getTransactionCount + eth_getCode,
    // each 20 CU on Alchemy.
    let baseline_cu = n * 3 * CU_GET_STORAGE_AT;
    let (baseline_median, seeded_median) = (baseline[samples / 2], seeded[samples / 2]);
    println!(
        "- templates: {} contracts, {} bytes of runtime code",
        templates.len(),
        code_bytes,
    );
    println!(
        "- baseline ensure_account x {}: median {} (min {}, max {}), {} RPCs, {} CU, ~{} KB code on the wire",
        templates.len(),
        ms(baseline_median),
        ms(baseline[0]),
        ms(baseline[samples - 1]),
        n * 3,
        baseline_cu,
        code_bytes * 2 / 1024, // hex-encoded JSON roughly doubles the bytes
    );
    println!(
        "- seed + verify_code_seeds: median {} (min {}, max {}), ONE eth_call, {} CU, 0 code bytes on the wire ({:.0}x cheaper, {:.1}x faster; real balances materialized from the same call)",
        ms(seeded_median),
        ms(seeded[0]),
        ms(seeded[samples - 1]),
        CU_ETH_CALL,
        baseline_cu as f64 / CU_ETH_CALL as f64,
        baseline_median.as_secs_f64() / seeded_median.as_secs_f64(),
    );

    // Fail-closed spot check: a wrong template and a nonexistent address are
    // classified (and purged) by the same single call, cache left clean.
    let mut cache = EvmCache::builder(provider.clone())
        .block(block)
        .build()
        .await;
    let wrong_template = templates[0].1.clone(); // WETH runtime...
    cache
        .seed_account_code(templates[1].0, wrong_template.clone())
        .map_err(|e| anyhow::anyhow!("seed wrong template: {e}"))?; // ...claimed at USDC
    let ghost = Address::from_slice(&keccak256(b"efc-seed-ghost")[12..]);
    cache
        .seed_account_code(ghost, wrong_template)
        .map_err(|e| anyhow::anyhow!("seed ghost: {e}"))?;
    let report = cache
        .verify_code_seeds()
        .map_err(|e| anyhow::anyhow!("verify fail-closed pair: {e}"))?;
    anyhow::ensure!(
        report.mismatched.len() == 1 && report.not_deployed.len() == 1,
        "expected one mismatch + one not-deployed: {report:?}"
    );
    println!(
        "- fail-closed: wrong template -> mismatched (expected {}.. vs actual {}..) and purged; \
         unknown address -> not_deployed; one call classified both\n",
        &format!("{:#x}", report.mismatched[0].expected)[..10],
        &format!("{:#x}", report.mismatched[0].actual)[..10],
    );
    Ok(())
}
