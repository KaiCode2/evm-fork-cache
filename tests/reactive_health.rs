//! Manager-authored red-green acceptance tests for WS-4/WS-5: the queryable
//! `CacheHealth` state and `CacheMetrics` counters on `ReactiveRuntime`.
//!
//! These describe the public contract before the implementation exists:
//! - health defaults to `Healthy`;
//! - a reorg deeper than the journal (aged-out / parent-not-in-journal) degrades
//!   health to `Degraded`, increments the `deep_reorgs` counter, and emits a
//!   `ReactiveReport::Health` transition;
//! - an in-journal reorg is fully recovered, increments `reorgs_recovered`, and
//!   does NOT degrade health.
//!
//! Fully offline (mocked provider, injected state).
#![cfg(feature = "reactive")]

mod common;

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AccountFieldMask, BlockRef, CacheHealth, ChainStatus, HandlerError, HandlerId, HandlerOutcome,
    InputSource, LogInterest, PendingTxInterest, ReactiveConfig, ReactiveContext, ReactiveEffect,
    ReactiveError, ReactiveHandler, ReactiveInput, ReactiveInputBatch, ReactiveInputRecord,
    ReactiveInterest, ReactiveReport, ReactiveRuntime, ResyncBlock, ResyncId, ResyncPriority,
    ResyncReason, ResyncRequest, ResyncTarget, RouteKeySpec, StateEffectQuality,
};
use evm_fork_cache::{PurgeScope, StateUpdate};

fn block(number: u64, hash: B256, parent_hash: B256) -> BlockRef {
    BlockRef {
        number,
        hash,
        parent_hash: Some(parent_hash),
        timestamp: Some(1_700_000_000 + number),
    }
}

fn rpc_log(address: Address, block: &BlockRef, log_index: u64) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, vec![B256::repeat_byte(0xee)], Bytes::new()),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(0x01)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn included_context(block: BlockRef, log_index: u64) -> ReactiveContext {
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

fn batch(input: ReactiveInput<Ethereum>, ctx: ReactiveContext) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, ctx)])
}

/// A handler that writes `log_index` to a fixed slot, so each canonical block
/// leaves a distinct reversible storage effect in the journal.
struct SlotWriter {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for SlotWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("health-slot-writer")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                U256::from(ctx.log_index.expect("test context carries a log index")),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

/// WS-4/WS-5: a fresh runtime is `Healthy` with zeroed counters.
#[tokio::test]
async fn health_defaults_to_healthy() -> Result<()> {
    let runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    assert_eq!(runtime.health(), CacheHealth::Healthy);
    let m = runtime.metrics();
    assert_eq!(m.deep_reorgs, 0);
    assert_eq!(m.reorgs_recovered, 0);
    Ok(())
}

/// WS-4/WS-5: a reorg that references a block no longer in the journal (here
/// forced via `journal_depth = 1`) degrades health to `Degraded`, increments the
/// `deep_reorgs` counter, and emits a `ReactiveReport::Health` transition.
#[tokio::test]
async fn deep_reorg_beyond_journal_degrades_health() -> Result<()> {
    let address = Address::repeat_byte(0xd1);
    let slot = U256::from(7);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
        journal_depth: 1,
        ..ReactiveConfig::default()
    });
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    let b11 = block(11, B256::repeat_byte(0x11), b10.hash);
    // Replacement for block 11 whose parent is NOT in the (depth-1) journal.
    let b11_alt = block(11, B256::repeat_byte(0x1b), B256::repeat_byte(0x1a));

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b10, 10)),
            included_context(b10.clone(), 10),
        ),
    )?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b11, 11)),
            included_context(b11.clone(), 11),
        ),
    )?;
    assert_eq!(runtime.health(), CacheHealth::Healthy, "healthy so far");

    // Ingesting the replacement block references a parent aged out of the
    // depth-1 journal: under-recovery => degrade.
    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b11_alt, 12)),
            included_context(b11_alt.clone(), 12),
        ),
    )?;

    assert!(
        matches!(runtime.health(), CacheHealth::Degraded { .. }),
        "a reorg beyond the journal must degrade health, got {:?}",
        runtime.health()
    );
    assert_eq!(
        runtime.metrics().deep_reorgs,
        1,
        "the deep-reorg counter must increment"
    );
    assert!(
        report
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::Health(_))),
        "a Health transition report must be emitted"
    );
    Ok(())
}

/// WS-4/WS-5: an in-journal reorg (parent present in the default-depth journal)
/// is fully recovered — it increments `reorgs_recovered` and does NOT degrade
/// health.
#[tokio::test]
async fn in_journal_reorg_recovers_without_degrading() -> Result<()> {
    let address = Address::repeat_byte(0xd2);
    let slot = U256::from(8);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let dropped = block(80, B256::repeat_byte(0x80), parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), parent.hash);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &parent, 10)),
            included_context(parent.clone(), 10),
        ),
    )?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &dropped, 20)),
            included_context(dropped.clone(), 20),
        ),
    )?;
    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &replacement, 30)),
            included_context(replacement.clone(), 30),
        ),
    )?;

    // An in-journal parent-mismatch reorg was recovered.
    assert!(
        report
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::Reorg(_))),
        "replacement block emits a reorg report"
    );
    assert_eq!(
        runtime.metrics().reorgs_recovered,
        1,
        "one in-journal reorg increments reorgs_recovered exactly once"
    );
    assert_eq!(
        runtime.health(),
        CacheHealth::Healthy,
        "a fully-recovered in-journal reorg must NOT degrade health"
    );
    Ok(())
}

/// A handler that requests an account-field resync, which the provider-neutral
/// cache seam does not support — the resync execution pass reports it as a
/// failure. Used to drive both `resync_requests` and `resync_failures`.
struct AccountResyncHandler {
    address: Address,
    block: ResyncBlock,
}

impl ReactiveHandler<Ethereum> for AccountResyncHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("health-account-resync")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::Resync(ResyncRequest {
                id: ResyncId::new("account-repair"),
                reason: ResyncReason::HandlerRequested,
                block: self.block.clone(),
                targets: vec![ResyncTarget::Account {
                    address: self.address,
                    fields: AccountFieldMask {
                        balance: true,
                        nonce: false,
                        code: false,
                    },
                }],
                priority: ResyncPriority::High,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

/// WS-5: `ingest_batch_with_resync` counts the resync targets it considers and
/// the ones that fail. An unsupported account-field target moves both counters.
#[tokio::test]
async fn resync_requests_and_failures_increment() -> Result<()> {
    let address = Address::repeat_byte(0xa9);
    let b5 = block(5, B256::repeat_byte(0x05), B256::repeat_byte(0x04));
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(AccountResyncHandler {
        address,
        block: ResyncBlock::Hash {
            number: b5.number,
            hash: b5.hash,
            require_canonical: true,
        },
    }))?;

    assert_eq!(runtime.metrics().resync_requests, 0);
    assert_eq!(runtime.metrics().resync_failures, 0);

    runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b5, 5)),
            included_context(b5.clone(), 5),
        ),
    )?;

    let m = runtime.metrics();
    assert_eq!(
        m.resync_requests, 1,
        "one considered resync target must be counted"
    );
    assert_eq!(
        m.resync_failures, 1,
        "the unsupported account target must be counted as a failure"
    );
    Ok(())
}

/// A handler that, on a pending input, emits a canonical cache effect — which
/// the runtime rejects as `InvalidPendingEffect`.
struct PendingCanonicalWriter;

impl ReactiveHandler<Ethereum> for PendingCanonicalWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("health-pending-writer")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::PendingTransactions(
            PendingTxInterest::default(),
        )]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::purge(
                Address::repeat_byte(0x75),
                PurgeScope::AllStorage,
            ))],
            quality: StateEffectQuality::RequiresRepair,
            tags: vec![],
        })
    }
}

/// WS-5: a pending-source input that attempts a canonical effect increments
/// `pending_contamination` (without changing the error's public behavior).
#[tokio::test]
async fn pending_contamination_increments_on_invalid_pending_effect() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PendingCanonicalWriter))?;

    assert_eq!(runtime.metrics().pending_contamination, 0);

    let err = runtime
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::PendingTxHash(B256::repeat_byte(0x99)),
                ReactiveContext {
                    chain_id: Some(1),
                    source: InputSource::Batch,
                    chain_status: ChainStatus::Pending,
                    block: None,
                    transaction_index: None,
                    log_index: None,
                },
            ),
        )
        .expect_err("pending inputs must not mutate canonical cache state");

    assert!(matches!(err, ReactiveError::InvalidPendingEffect { .. }));
    assert_eq!(
        runtime.metrics().pending_contamination,
        1,
        "pending contamination must be counted"
    );
    Ok(())
}

/// WS-5: a fresh runtime's metrics snapshot is all-zero.
#[tokio::test]
async fn metrics_snapshot_starts_all_zero() -> Result<()> {
    let runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let m = runtime.metrics();
    assert_eq!(m.deep_reorgs, 0);
    assert_eq!(m.reorgs_recovered, 0);
    assert_eq!(m.resync_requests, 0);
    assert_eq!(m.resync_failures, 0);
    assert_eq!(m.missed_ranges, 0);
    assert_eq!(m.coverage_gaps, 0);
    assert_eq!(m.pending_contamination, 0);
    assert_eq!(m.stale_verdicts, 0);
    Ok(())
}

/// WS-4 (manager-authored red-green): a forward gap in the canonical block
/// sequence (block N followed by N+k, k>1) is no longer silently accepted. The
/// runtime emits a `ReactiveReport::MissedBlockRange { from, to }` for the skipped
/// span, increments `missed_ranges`, and degrades health — while STILL accepting
/// and applying the arriving block (the chain extends).
#[tokio::test]
async fn forward_block_gap_is_detected_and_degrades() -> Result<()> {
    let address = Address::repeat_byte(0xd3);
    let slot = U256::from(9);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    // Block 15 arrives after 10, skipping 11..=14 (e.g. a disconnect gap).
    let b15 = block(15, B256::repeat_byte(0x15), B256::repeat_byte(0x14));

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b10, 10)),
            included_context(b10.clone(), 10),
        ),
    )?;
    assert_eq!(runtime.health(), CacheHealth::Healthy);

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b15, 15)),
            included_context(b15.clone(), 15),
        ),
    )?;

    // A missed-range report identifies the skipped span 11..=14.
    let gap = report
        .reports
        .iter()
        .find_map(|r| match r.as_ref() {
            ReactiveReport::MissedBlockRange(report) => Some(report),
            _ => None,
        })
        .expect("a forward gap must emit a MissedBlockRange report");
    assert_eq!(gap.from, 11, "gap starts one past the last-seen block");
    assert_eq!(gap.to, 14, "gap ends one before the arriving block");

    assert_eq!(runtime.metrics().missed_ranges, 1);
    assert!(
        matches!(runtime.health(), CacheHealth::Degraded { .. }),
        "a missed range must degrade health, got {:?}",
        runtime.health()
    );
    // The arriving block is still accepted and applied (chain extends).
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(15u64)),
        "the gap block's effects must still be applied"
    );
    Ok(())
}

/// WS-4 (manager-authored red-green): repeated trust-loss events escalate the
/// health state — the first degrades to `Degraded`, a second (here a second gap)
/// escalates to `Unhealthy` (the "stop until rebuilt" signal).
#[tokio::test]
async fn repeated_trust_loss_escalates_to_unhealthy() -> Result<()> {
    let address = Address::repeat_byte(0xd4);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    let b15 = block(15, B256::repeat_byte(0x15), B256::repeat_byte(0x14));
    let b20 = block(20, B256::repeat_byte(0x20), B256::repeat_byte(0x1e));

    for b in [&b10, &b15, &b20] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(address, b, b.number)),
                included_context(b.clone(), b.number),
            ),
        )?;
    }

    // First gap (10 -> 15) degraded; second gap (15 -> 20) escalates.
    assert!(
        matches!(runtime.health(), CacheHealth::Unhealthy { .. }),
        "repeated gaps must escalate to Unhealthy, got {:?}",
        runtime.health()
    );
    assert_eq!(runtime.metrics().missed_ranges, 2);
    Ok(())
}

/// WS-4 (manager-authored red-green): after the caller has repaired/resynced,
/// `reset_health` returns the runtime to `Healthy` (the self-heal completion).
#[tokio::test]
async fn reset_health_restores_healthy() -> Result<()> {
    let address = Address::repeat_byte(0xd5);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    let b15 = block(15, B256::repeat_byte(0x15), B256::repeat_byte(0x14));
    for b in [&b10, &b15] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(address, b, b.number)),
                included_context(b.clone(), b.number),
            ),
        )?;
    }
    assert!(matches!(runtime.health(), CacheHealth::Degraded { .. }));

    runtime.reset_health();
    assert_eq!(runtime.health(), CacheHealth::Healthy);
    Ok(())
}

/// WS-4 (implementation agent): mixed trust-loss event types share the same
/// escalation ladder. A deep reorg (journal_depth=1, parent aged out) degrades to
/// `Degraded`, then a subsequent forward gap escalates to `Unhealthy`.
#[tokio::test]
async fn mixed_trust_loss_events_escalate_to_unhealthy() -> Result<()> {
    let address = Address::repeat_byte(0xd6);
    let slot = U256::from(3);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
        journal_depth: 1,
        ..ReactiveConfig::default()
    });
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    let b11 = block(11, B256::repeat_byte(0x11), b10.hash);
    // Replacement for block 11 whose parent is NOT in the (depth-1) journal:
    // a deep reorg -> first trust-loss event -> Degraded.
    let b11_alt = block(11, B256::repeat_byte(0x1b), B256::repeat_byte(0x1a));
    // A forward gap after 11: 11 -> 16 skips 12..=15 -> second trust-loss event.
    let b16 = block(16, B256::repeat_byte(0x16), B256::repeat_byte(0x15));

    for (b, log_index) in [(&b10, 10u64), (&b11, 11)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(address, b, log_index)),
                included_context(b.clone(), log_index),
            ),
        )?;
    }
    assert_eq!(runtime.health(), CacheHealth::Healthy, "healthy so far");

    // Deep reorg: parent aged out of the depth-1 journal -> Degraded.
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b11_alt, 12)),
            included_context(b11_alt.clone(), 12),
        ),
    )?;
    assert!(
        matches!(runtime.health(), CacheHealth::Degraded { .. }),
        "the deep reorg degrades health, got {:?}",
        runtime.health()
    );

    // Forward gap: escalates to the terminal Unhealthy stop signal.
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b16, 16)),
            included_context(b16.clone(), 16),
        ),
    )?;
    assert!(
        matches!(runtime.health(), CacheHealth::Unhealthy { .. }),
        "a second trust-loss event of a different type escalates to Unhealthy, got {:?}",
        runtime.health()
    );
    assert_eq!(runtime.metrics().deep_reorgs, 1);
    assert_eq!(runtime.metrics().missed_ranges, 1);
    Ok(())
}

/// WS-4 (implementation agent): the `MissedBlockRange` report's `block` field
/// equals the arriving block number that revealed the gap.
#[tokio::test]
async fn missed_range_report_block_equals_arriving_block() -> Result<()> {
    let address = Address::repeat_byte(0xd7);
    let slot = U256::from(2);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    let b15 = block(15, B256::repeat_byte(0x15), B256::repeat_byte(0x14));

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b10, 10)),
            included_context(b10.clone(), 10),
        ),
    )?;

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b15, 15)),
            included_context(b15.clone(), 15),
        ),
    )?;

    let gap = report
        .reports
        .iter()
        .find_map(|r| match r.as_ref() {
            ReactiveReport::MissedBlockRange(report) => Some(report),
            _ => None,
        })
        .expect("a forward gap must emit a MissedBlockRange report");
    assert_eq!(
        gap.block, 15,
        "the report's block field equals the arriving block number"
    );
    Ok(())
}
