//! Reactive runtime end-to-end (Pillar 2): handler → `StateUpdate` → apply, plus
//! journaled reorg rollback — the part a searcher would otherwise hand-roll.
//!
//! The reactive runtime is the crate's headline differentiator over bare `revm`:
//! `revm` executes, but it does not keep your forked state correct as the chain
//! moves. This example shows the full loop with no RPC and no manual bookkeeping:
//!
//! 1. Register a provider-neutral [`ReactiveHandler`] that turns a matching log
//!    into a targeted [`StateUpdate`]. The key insight is that **events already
//!    carry the post-state** — here the new value rides in the log's `data`, so we
//!    *decode-and-write* it straight into a slot instead of re-fetching from RPC.
//! 2. Feed a canonical block's log through [`ReactiveRuntime::ingest_batch`]; the
//!    runtime validates the handler's effects and applies them to the cache with
//!    **0 RPC fetches** (counted below).
//! 3. Re-deliver that same log as `removed` (a reorg). The runtime unwinds the
//!    journaled write back to its prior value automatically and emits a
//!    [`ReactiveReport::Reorg`] describing exactly what it rolled back.
//!
//! A [`ReactiveHook`] observes the applied reports out-of-band (metrics, logging).
//!
//! Runs fully offline against a mocked provider — no network, no RPC key.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example reactive_runtime
//! ```

#[path = "support/mock.rs"]
mod mock;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use alloy_eips::BlockId;
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;
use evm_fork_cache::StateUpdate;
use evm_fork_cache::cache::StorageBatchFetchFn;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AppliedReport, BlockRef, ChainStatus, HandlerError, HandlerId, HandlerOutcome, HookSignal,
    InputSource, LogInterest, ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveHandler,
    ReactiveHook, ReactiveInput, ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest,
    ReactiveReport, ReactiveRuntime, ReportTag, RouteKeySpec, StateEffectQuality,
};

/// A storage slot standing in for any piece of hot state whose new value an event
/// already carries (e.g. a pool's packed price word after a swap).
const TARGET_SLOT: u64 = 7;

/// A protocol-neutral handler. Every matching log carries the post-state value in
/// its `data`, so we decode it and write it straight into `slot` — no RPC. This is
/// the "events already carry the post-state" idea that the reactive runtime exists
/// to package safely (validation, journaling, reorg recovery).
struct PostStateWriter {
    emitter: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for PostStateWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("post-state-writer")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        // Subscribe to every log emitted by `emitter`. `provider_filter` is what a
        // live transport (e.g. the bundled `AlloySubscriber`) would push upstream;
        // `route_key` lets the runtime fan many emitters out to the right handler.
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.emitter),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };

        // The new value is the 32-byte log payload. A real decoder would pull it
        // from the event's ABI; the runtime does not care how — it only sees the
        // resulting `StateUpdate`.
        let data = &log.inner.data.data;
        if data.len() != 32 {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        }
        let value = U256::from_be_slice(data);

        Ok(HandlerOutcome {
            effects: vec![
                ReactiveEffect::StateUpdate(StateUpdate::slot(log.address(), self.slot, value)),
                // A hook signal is an out-of-band notification (metrics, logging);
                // it never mutates the cache.
                ReactiveEffect::Hook(HookSignal {
                    namespace: "demo".into(),
                    kind: "slot.write".into(),
                    labels: vec![ReportTag::new("slot", self.slot.to_string())],
                    payload: None,
                }),
            ],
            // `ExactFromInput`: the value is authoritative from the event itself,
            // not a guess that needs reconciling.
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

/// An out-of-band observer of applied reports. Hooks run synchronously on the
/// ingest thread, so they must be cheap and non-blocking.
#[derive(Default)]
struct AppliedLog {
    writes: Arc<Mutex<Vec<U256>>>,
    signals: Arc<Mutex<Vec<String>>>,
}

impl ReactiveHook<Ethereum> for AppliedLog {
    fn on_report(&self, report: Arc<ReactiveReport<Ethereum>>) {
        if let ReactiveReport::Applied(AppliedReport {
            diff, hook_signals, ..
        }) = report.as_ref()
        {
            self.writes
                .lock()
                .unwrap()
                .extend(diff.slots.iter().map(|change| change.new));
            self.signals.lock().unwrap().extend(
                hook_signals
                    .iter()
                    .map(|signal| format!("{}:{}", signal.namespace, signal.kind)),
            );
        }
    }
}

/// Build a log from `emitter` carrying a 32-byte big-endian `value` in its data.
fn value_log(
    emitter: Address,
    topic: B256,
    value: U256,
    block: &BlockRef,
    log_index: u64,
    removed: bool,
) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(
            emitter,
            vec![topic],
            Bytes::copy_from_slice(&value.to_be_bytes::<32>()),
        ),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(0xfe)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed,
    }
}

fn block_ref(number: u64, hash: u8, parent: u8) -> BlockRef {
    BlockRef {
        number,
        hash: B256::repeat_byte(hash),
        parent_hash: Some(B256::repeat_byte(parent)),
        timestamp: Some(1_700_000_000 + number),
    }
}

/// A canonical-inclusion context for a log at `block` / `log_index`.
fn included(block: BlockRef, log_index: u64) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Batch,
        chain_status: ChainStatus::Included {
            block: block.clone(),
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

/// A reorg context: `dropped` is the block that fell off the canonical chain.
fn reorged(dropped: BlockRef, log_index: u64) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Batch,
        chain_status: ChainStatus::Reorged {
            dropped_from: dropped.clone(),
        },
        block: Some(dropped),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

/// Wrap one (input, context) pair into a single-record batch.
fn one(input: ReactiveInput<Ethereum>, ctx: ReactiveContext) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, ctx)])
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;
    let emitter = Address::repeat_byte(0x42);
    mock::install_mock_erc20(&mut cache, emitter);

    let slot = U256::from(TARGET_SLOT);
    // Seed a known prior value so the reorg rollback has something to restore.
    cache
        .db_mut()
        .insert_account_storage(emitter, slot, U256::from(10))?;

    // A counting fetcher so we can *prove* that ingest applies writes with no RPC.
    // `ingest_batch` decodes logs into writes; if it ever reached for RPC, this
    // counter would catch it.
    let fetches = Arc::new(AtomicUsize::new(0));
    let counter = fetches.clone();
    let fetcher: StorageBatchFetchFn = Arc::new(
        move |requests: Vec<(Address, U256)>, _block: Option<BlockId>| {
            counter.fetch_add(requests.len(), Ordering::Relaxed);
            requests
                .into_iter()
                .map(|(a, s)| (a, s, Ok(U256::ZERO)))
                .collect()
        },
    );
    cache.set_storage_batch_fetcher(fetcher);

    let hook = Arc::new(AppliedLog::default());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PostStateWriter { emitter, slot }))?;
    runtime.register_hook(hook.clone())?;

    let topic = keccak256(b"PostState(uint256)");
    println!(
        "slot {} starts at {:?} (the on-chain value before any event)",
        TARGET_SLOT,
        cache.cached_storage_value(emitter, slot)
    );

    // 1) Canonical block 100: the log carries the new value (20). Decode-and-write.
    let canonical = block_ref(100, 0x64, 0x63);
    fetches.store(0, Ordering::Relaxed);
    let report = runtime.ingest_batch(
        &mut cache,
        one(
            ReactiveInput::Log(value_log(
                emitter,
                topic,
                U256::from(20),
                &canonical,
                0,
                false,
            )),
            included(canonical.clone(), 0),
        ),
    )?;
    println!("\n=== block {} ingested ===", canonical.number);
    println!(
        "  applied {} update(s) with {} RPC fetch(es)  <- events carry the post-state",
        report.applied.len(),
        fetches.load(Ordering::Relaxed),
    );
    println!(
        "  slot {} = {:?}",
        TARGET_SLOT,
        cache.cached_storage_value(emitter, slot)
    );

    // 2) Reorg: the same log is re-delivered as `removed`. The runtime unwinds the
    //    journaled write back to the prior value — no RPC, no manual undo log.
    let report = runtime.ingest_batch(
        &mut cache,
        one(
            ReactiveInput::Log(value_log(
                emitter,
                topic,
                U256::from(20),
                &canonical,
                0,
                true,
            )),
            reorged(canonical.clone(), 0),
        ),
    )?;
    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(reorg) => Some(reorg),
            _ => None,
        })
        .expect("a removed log emits a reorg report");
    println!("\n=== reorg: block {} dropped ===", canonical.number);
    println!("  rolled back {} write(s):", reorg.rollback_updates.len());
    for change in &reorg.rollback_diff.slots {
        println!("    slot rolled {} -> {}", change.old, change.new);
    }
    println!(
        "  slot {} restored to {:?}  <- automatic, journaled recovery",
        TARGET_SLOT,
        cache.cached_storage_value(emitter, slot)
    );

    println!(
        "\nhook observed: writes={:?}, signals={:?}",
        hook.writes.lock().unwrap(),
        hook.signals.lock().unwrap()
    );

    Ok(())
}
