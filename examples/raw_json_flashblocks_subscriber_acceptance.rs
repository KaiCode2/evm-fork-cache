//! Compare a caller-owned raw JSON Flashblocks socket with canonical WebSocket
//! logs through the standard `AlloySubscriber` delivery path.
//!
//! This opt-in acceptance probe is chain-neutral: the caller supplies both
//! endpoints and the chain id. The raw lane sends no application message and
//! performs no JSON-RPC request. It connects once, converts receipt-enriched
//! indexed JSON frames, and forwards standardized updates through the bounded
//! subscriber channel. Socket retry and backoff deliberately remain outside
//! this example and the library.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloy_consensus::BlockHeader as _;
use alloy_network::Ethereum;
use alloy_primitives::{B256, keccak256};
use alloy_provider::{ProviderBuilder, WsConnect};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::{Context as _, Result, bail, ensure};
use evm_fork_cache::reactive::{
    AlloySubscriber, BlockInterest, ChainStatus, EventSubscriber, FlashblockUpdateSender,
    LogInterest, PreconfirmationMode, ProviderRef, RawJsonFlashblocksAdapter, ReactiveInput,
    ReactiveInterest, SubscriberConfig, SubscriberInputScope, SubscriberMode,
};
use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct LogKey {
    transaction_hash: B256,
    log_index: u64,
}

#[derive(Clone)]
struct TimedLog {
    observed_at: tokio::time::Instant,
    log: Log,
}

#[derive(Default)]
struct Stats {
    raw_logs: u64,
    canonical_logs: u64,
    canonical_heads: u64,
    invalidations: u64,
    raw_duplicates: u64,
    canonical_duplicates: u64,
    content_mismatches: u64,
    raw_first: u64,
    canonical_first: u64,
    simultaneous: u64,
    raw_first_lead_micros: Vec<u64>,
    absolute_correlation_micros: Vec<u64>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    install_rustls_provider();
    let raw_url = std::env::var("RAW_FLASHBLOCKS_WS_URL")
        .context("RAW_FLASHBLOCKS_WS_URL must name a receipt-enriched raw WebSocket")?;
    let canonical_url = std::env::var("CANONICAL_WS_URL")
        .context("CANONICAL_WS_URL must name an independent canonical WebSocket")?;
    let chain_id = env_u64("FLASHBLOCKS_CHAIN_ID", 10)?;
    let run_seconds = env_u64("FLASHBLOCKS_ACCEPTANCE_SECONDS", 300)?;
    let target_pairs = env_usize("FLASHBLOCKS_ACCEPTANCE_TARGET_PAIRS", 100)?;
    ensure!(run_seconds > 0, "acceptance window must be nonzero");
    ensure!(target_pairs > 0, "target pair count must be nonzero");

    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(canonical_url))
        .await
        .context("connect canonical WebSocket")?;
    let swap_topics = [
        keccak256(b"Swap(address,address,int256,int256,uint160,uint128,int24)"),
        keccak256(b"Swap(address,uint256,uint256,uint256,uint256,address)"),
    ];
    let source = ProviderRef::new("raw-json-acceptance", 1);
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::PubSub,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            hydrate_pending_transactions: false,
            verify_log_block_context: false,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .context("configure external Flashblocks source")?;
    let updates = subscriber
        .open_external_flashblock_update_channel(1_024)
        .context("open bounded external update channel")?;
    subscriber
        .register_interests(&[
            ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().event_signature(swap_topics.to_vec()),
                local_matcher: None,
                route_key: None,
            }),
            ReactiveInterest::Blocks(BlockInterest::default()),
        ])
        .await
        .context("register canonical swap and head interests")?;
    subscriber
        .establish_flashblocks_preflight(chain_id)
        .await
        .context("verify canonical chain identity and external Flashblocks topology")?;

    let mut raw_task = tokio::spawn(forward_raw_frames(raw_url, source, updates));
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_secs(run_seconds);
    let mut raw_pending = HashMap::<LogKey, TimedLog>::new();
    let mut canonical_pending = HashMap::<LogKey, TimedLog>::new();
    let mut matched = HashSet::<LogKey>::new();
    let mut emitters = HashMap::new();
    let mut stats = Stats::default();
    let mut first_raw_block = None::<u64>;
    let mut latest_canonical_head = None::<u64>;

    while matched.len() < target_pairs {
        let batch = tokio::select! {
            raw_result = &mut raw_task => {
                raw_result.context("raw source task failed")??;
                bail!("raw source ended before acceptance completed");
            }
            result = tokio::time::timeout_at(deadline, subscriber.next_scoped_batch()) => {
                match result {
                    Err(_) => break,
                    Ok(Ok(Some(batch))) => batch,
                    Ok(Ok(None)) => bail!("subscriber ended before acceptance completed"),
                    Ok(Err(error)) => return Err(error).context("poll standardized subscriber output"),
                }
            }
        };
        if batch.preconfirmation_invalidated() {
            stats.invalidations = stats.invalidations.saturating_add(1);
        }
        for record in batch.records() {
            match &record.record().input {
                ReactiveInput::BlockHeader(header) if record.scope().is_canonical() => {
                    stats.canonical_heads = stats.canonical_heads.saturating_add(1);
                    latest_canonical_head = Some(
                        latest_canonical_head
                            .map_or(header.number(), |current| current.max(header.number())),
                    );
                }
                ReactiveInput::Log(log) => {
                    let Some(key) = log_key(log) else {
                        stats.content_mismatches = stats.content_mismatches.saturating_add(1);
                        continue;
                    };
                    let now = tokio::time::Instant::now();
                    match record.scope() {
                        SubscriberInputScope::Preconfirmed => {
                            ensure!(
                                matches!(
                                    record.record().context.chain_status,
                                    ChainStatus::Preconfirmed { .. }
                                ),
                                "speculative log lacked preconfirmation provenance"
                            );
                            if let Some(block_number) = log.block_number {
                                first_raw_block = Some(
                                    first_raw_block
                                        .map_or(block_number, |first| first.min(block_number)),
                                );
                            }
                            *emitters.entry(log.address()).or_insert(0_u64) += 1;
                            observe_raw(
                                key,
                                log.clone(),
                                now,
                                &mut raw_pending,
                                &mut canonical_pending,
                                &mut matched,
                                &mut stats,
                            );
                        }
                        scope if scope.is_canonical() => {
                            ensure!(
                                matches!(
                                    record.record().context.chain_status,
                                    ChainStatus::Included { .. }
                                ),
                                "canonical log lacked included provenance"
                            );
                            observe_canonical(
                                key,
                                log.clone(),
                                now,
                                &mut raw_pending,
                                &mut canonical_pending,
                                &mut matched,
                                &mut stats,
                            );
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    raw_task.abort();
    let _ = raw_task.await;
    stats.raw_first_lead_micros.sort_unstable();
    stats.absolute_correlation_micros.sort_unstable();
    let mut ranked_emitters = emitters.into_iter().collect::<Vec<_>>();
    ranked_emitters.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.1));
    let matched_count = matched.len();
    let latest_head = latest_canonical_head.unwrap_or_default();
    let first_raw = first_raw_block.unwrap_or(u64::MAX);
    let matured_raw_unmatched = raw_pending
        .values()
        .filter(|entry| {
            entry
                .log
                .block_number
                .is_some_and(|number| number < latest_head)
        })
        .count();
    let edge_raw_unmatched = raw_pending.len().saturating_sub(matured_raw_unmatched);
    let interior_canonical_unmatched = canonical_pending
        .values()
        .filter(|entry| {
            entry
                .log
                .block_number
                .is_some_and(|number| number >= first_raw)
        })
        .count();
    let edge_canonical_unmatched = canonical_pending
        .len()
        .saturating_sub(interior_canonical_unmatched);
    let reconciliation_bps = u64::try_from(matched_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(10_000)
        .checked_div(
            u64::try_from(matched_count.saturating_add(matured_raw_unmatched)).unwrap_or(u64::MAX),
        )
        .unwrap_or_default();
    println!(
        "elapsed_ms={} chain_id={} raw_logs={} canonical_logs={} paired_logs={} reconciliation_bps={} raw_first={} canonical_first={} simultaneous={} canonical_heads={} latest_canonical_head={} overlay_invalidations={} raw_duplicates={} canonical_duplicates={} content_mismatches={} matured_raw_unmatched={} edge_raw_unmatched={} interior_canonical_unmatched={} edge_canonical_unmatched={} raw_lead_p50_ms={:.3} raw_lead_p95_ms={:.3} raw_lead_p99_ms={:.3} raw_lead_max_ms={:.3} correlation_p95_ms={:.3}",
        started.elapsed().as_millis(),
        chain_id,
        stats.raw_logs,
        stats.canonical_logs,
        matched_count,
        reconciliation_bps,
        stats.raw_first,
        stats.canonical_first,
        stats.simultaneous,
        stats.canonical_heads,
        latest_head,
        stats.invalidations,
        stats.raw_duplicates,
        stats.canonical_duplicates,
        stats.content_mismatches,
        matured_raw_unmatched,
        edge_raw_unmatched,
        interior_canonical_unmatched,
        edge_canonical_unmatched,
        percentile_millis(&stats.raw_first_lead_micros, 50),
        percentile_millis(&stats.raw_first_lead_micros, 95),
        percentile_millis(&stats.raw_first_lead_micros, 99),
        percentile_millis(&stats.raw_first_lead_micros, 100),
        percentile_millis(&stats.absolute_correlation_micros, 95),
    );
    for (rank, (address, count)) in ranked_emitters.into_iter().take(5).enumerate() {
        println!(
            "raw_emitter_rank={} address={address} logs={count}",
            rank + 1
        );
    }

    ensure!(
        matched_count >= target_pairs,
        "subscriber did not reconcile the target speculative/canonical swap pairs"
    );
    ensure!(
        stats.canonical_heads >= 2,
        "canonical head delivery was not continuous"
    );
    ensure!(stats.content_mismatches == 0, "paired log content differed");
    ensure!(
        stats.raw_duplicates == 0,
        "speculative delivery duplicated a swap"
    );
    ensure!(
        stats.canonical_duplicates == 0,
        "canonical delivery duplicated a swap"
    );
    ensure!(
        stats.raw_first > stats.canonical_first,
        "raw delivery was not usually first"
    );
    ensure!(
        reconciliation_bps >= 9_900,
        "fewer than 99% of matured raw swaps reconciled canonically"
    );
    ensure!(
        interior_canonical_unmatched == 0,
        "the raw stream missed an interior canonical swap"
    );
    Ok(())
}

async fn forward_raw_frames(
    url: String,
    source: ProviderRef,
    updates: FlashblockUpdateSender,
) -> Result<()> {
    let (mut socket, _) = connect_async(url).await.context("connect raw WebSocket")?;
    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    while let Some(message) = socket.next().await {
        let message = message.context("read raw WebSocket frame")?;
        let decoded = match message {
            Message::Text(text) => Some(adapter.ingest_json(text.as_bytes())),
            Message::Binary(bytes) => Some(adapter.ingest_json(bytes.as_ref())),
            Message::Ping(_) | Message::Pong(_) => {
                socket
                    .flush()
                    .await
                    .context("flush WebSocket control response")?;
                None
            }
            Message::Close(_) => bail!("raw WebSocket closed"),
            Message::Frame(_) => None,
        };
        let Some(decoded) = decoded else {
            continue;
        };
        if let Some(update) = decoded.context("normalize raw Flashblocks frame")? {
            updates
                .send(update)
                .await
                .context("forward standardized Flashblocks update")?;
        }
    }
    bail!("raw WebSocket ended")
}

fn observe_raw(
    key: LogKey,
    log: Log,
    now: tokio::time::Instant,
    raw_pending: &mut HashMap<LogKey, TimedLog>,
    canonical_pending: &mut HashMap<LogKey, TimedLog>,
    matched: &mut HashSet<LogKey>,
    stats: &mut Stats,
) {
    stats.raw_logs = stats.raw_logs.saturating_add(1);
    if matched.contains(&key) || raw_pending.contains_key(&key) {
        stats.raw_duplicates = stats.raw_duplicates.saturating_add(1);
        return;
    }
    let raw = TimedLog {
        observed_at: now,
        log,
    };
    if let Some(canonical) = canonical_pending.remove(&key) {
        record_match(key, raw, canonical, matched, stats);
    } else {
        raw_pending.insert(key, raw);
    }
}

fn observe_canonical(
    key: LogKey,
    log: Log,
    now: tokio::time::Instant,
    raw_pending: &mut HashMap<LogKey, TimedLog>,
    canonical_pending: &mut HashMap<LogKey, TimedLog>,
    matched: &mut HashSet<LogKey>,
    stats: &mut Stats,
) {
    stats.canonical_logs = stats.canonical_logs.saturating_add(1);
    if matched.contains(&key) || canonical_pending.contains_key(&key) {
        stats.canonical_duplicates = stats.canonical_duplicates.saturating_add(1);
        return;
    }
    let canonical = TimedLog {
        observed_at: now,
        log,
    };
    if let Some(raw) = raw_pending.remove(&key) {
        record_match(key, raw, canonical, matched, stats);
    } else {
        canonical_pending.insert(key, canonical);
    }
}

fn record_match(
    key: LogKey,
    raw: TimedLog,
    canonical: TimedLog,
    matched: &mut HashSet<LogKey>,
    stats: &mut Stats,
) {
    if !equivalent_log_content(&raw.log, &canonical.log) {
        stats.content_mismatches = stats.content_mismatches.saturating_add(1);
    }
    let correlation = if canonical.observed_at > raw.observed_at {
        let lead = canonical
            .observed_at
            .duration_since(raw.observed_at)
            .as_micros() as u64;
        stats.raw_first = stats.raw_first.saturating_add(1);
        stats.raw_first_lead_micros.push(lead);
        lead
    } else if raw.observed_at > canonical.observed_at {
        stats.canonical_first = stats.canonical_first.saturating_add(1);
        raw.observed_at
            .duration_since(canonical.observed_at)
            .as_micros() as u64
    } else {
        stats.simultaneous = stats.simultaneous.saturating_add(1);
        0
    };
    stats.absolute_correlation_micros.push(correlation);
    matched.insert(key);
}

fn equivalent_log_content(raw: &Log, canonical: &Log) -> bool {
    raw.address() == canonical.address()
        && raw.topics() == canonical.topics()
        && raw.inner.data.data == canonical.inner.data.data
        && raw.block_number == canonical.block_number
        && raw.transaction_hash == canonical.transaction_hash
        && raw.transaction_index == canonical.transaction_index
        && raw.log_index == canonical.log_index
        && !canonical.removed
}

fn log_key(log: &Log) -> Option<LogKey> {
    Some(LogKey {
        transaction_hash: log.transaction_hash?,
        log_index: log.log_index?,
    })
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    std::env::var(name).map_or(Ok(default), |value| {
        value
            .parse::<u64>()
            .with_context(|| format!("parse {name} as an unsigned integer"))
    })
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    std::env::var(name).map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .with_context(|| format!("parse {name} as an unsigned integer"))
    })
}

fn percentile_millis(sorted_micros: &[u64], percentile: usize) -> f64 {
    if sorted_micros.is_empty() {
        return 0.0;
    }
    let index = sorted_micros
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted_micros.len() - 1);
    sorted_micros[index] as f64 / 1_000.0
}

fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
