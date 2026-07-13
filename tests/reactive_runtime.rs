//! Manager-authored acceptance tests for the reactive runtime feature.
//!
//! These tests intentionally describe the new public contract before the
//! implementation exists. They should fail on the current log-only event pipeline
//! and pass once `evm_fork_cache::reactive` provides a provider-agnostic handler
//! runtime.
#![cfg(feature = "reactive")]

mod common;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AppliedReport, BlockRef, ChainStatus, HandlerError, HandlerId, HandlerOutcome, HookSignal,
    InputRef, InputSource, InvalidationReason, InvalidationRequest, LogInterest, LogMatcher,
    LogRouteIndex, LogRouteKey, PendingTxInterest, ReactiveConfig, ReactiveContext, ReactiveEffect,
    ReactiveError, ReactiveHandler, ReactiveHook, ReactiveInput, ReactiveInputBatch,
    ReactiveInputRecord, ReactiveInterest, ReactiveReport, ReactiveRuntime, ReportTag, ResyncBlock,
    ResyncId, ResyncPriority, ResyncReason, ResyncRequest, ResyncTarget, RouteKeySpec,
    SpeculativeId, SpeculativeRequest, StateEffectQuality,
};
use evm_fork_cache::{PurgeScope, StateUpdate};

fn rpc_log(
    address: Address,
    topics: Vec<B256>,
    block_number: u64,
    tx_index: u64,
    log_index: u64,
) -> Log {
    let block_hash = B256::repeat_byte(block_number as u8);
    Log {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::new()),
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte((tx_index + 1) as u8)),
        transaction_index: Some(tx_index),
        log_index: Some(log_index),
        removed: false,
    }
}

fn included_context(block_number: u64, log_index: u64) -> ReactiveContext {
    let block = BlockRef {
        number: block_number,
        hash: B256::repeat_byte(block_number as u8),
        parent_hash: Some(B256::repeat_byte(block_number.saturating_sub(1) as u8)),
        timestamp: Some(1_700_000_000 + block_number),
    };

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

fn batch(records: Vec<(ReactiveInput<Ethereum>, ReactiveContext)>) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(
        records
            .into_iter()
            .map(|(input, ctx)| ReactiveInputRecord::new(input, ctx))
            .collect(),
    )
}

#[derive(Clone)]
struct SlotWriter {
    id: HandlerId,
    interest: ReactiveInterest,
    slot: U256,
    value: U256,
    hook_kind: &'static str,
}

impl SlotWriter {
    fn any_log_from(id: &'static str, address: Address, slot: U256, value: U256) -> Self {
        Self {
            id: HandlerId::new(id),
            interest: ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(address),
                local_matcher: None,
                route_key: Some(RouteKeySpec::EmitterAddress),
            }),
            slot,
            value,
            hook_kind: "slot.write",
        }
    }
}

impl ReactiveHandler<Ethereum> for SlotWriter {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![self.interest.clone()]
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

        Ok(HandlerOutcome {
            effects: vec![
                ReactiveEffect::StateUpdate(StateUpdate::slot(
                    log.address(),
                    self.slot,
                    self.value,
                )),
                ReactiveEffect::Hook(HookSignal {
                    namespace: "test".into(),
                    kind: self.hook_kind.into(),
                    labels: vec![ReportTag::new("handler", self.id.as_str())],
                    payload: None,
                }),
            ],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![ReportTag::new("slot", self.slot.to_string())],
        })
    }
}

struct CountingAddressMatcher {
    address: Address,
    calls: Arc<AtomicUsize>,
}

impl LogMatcher for CountingAddressMatcher {
    fn matches(&self, log: &Log) -> bool {
        self.calls.fetch_add(1, Ordering::Relaxed);
        log.address() == self.address
    }
}

struct CountingIndexedHandler {
    id: HandlerId,
    address: Address,
    matcher_calls: Arc<AtomicUsize>,
    handle_calls: Arc<AtomicUsize>,
}

impl ReactiveHandler<Ethereum> for CountingIndexedHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new(),
            local_matcher: Some(Arc::new(CountingAddressMatcher {
                address: self.address,
                calls: self.matcher_calls.clone(),
            })),
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn log_route_index(&self) -> Option<LogRouteIndex> {
        Some(LogRouteIndex::single(LogRouteKey::Emitter(self.address)))
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        self.handle_calls.fetch_add(1, Ordering::Relaxed);
        Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect))
    }
}

#[tokio::test]
async fn reactive_runtime_executes_the_same_indexed_candidates_as_the_registry() -> Result<()> {
    let pool_a = Address::repeat_byte(0xa1);
    let pool_b = Address::repeat_byte(0xb2);
    let matcher_a = Arc::new(AtomicUsize::new(0));
    let matcher_b = Arc::new(AtomicUsize::new(0));
    let handle_a = Arc::new(AtomicUsize::new(0));
    let handle_b = Arc::new(AtomicUsize::new(0));
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(CountingIndexedHandler {
        id: HandlerId::new("pool-a"),
        address: pool_a,
        matcher_calls: matcher_a.clone(),
        handle_calls: handle_a.clone(),
    }))?;
    runtime.register_handler(Arc::new(CountingIndexedHandler {
        id: HandlerId::new("pool-b"),
        address: pool_b,
        matcher_calls: matcher_b.clone(),
        handle_calls: handle_b.clone(),
    }))?;

    let report = runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(pool_b, vec![], 10, 0, 0)),
            included_context(10, 0),
        )]),
    )?;

    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].handler_id, HandlerId::new("pool-b"));
    assert_eq!(matcher_a.load(Ordering::Relaxed), 0);
    assert_eq!(matcher_b.load(Ordering::Relaxed), 1);
    assert_eq!(handle_a.load(Ordering::Relaxed), 0);
    assert_eq!(handle_b.load(Ordering::Relaxed), 1);

    let miss = runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(Address::repeat_byte(0xff), vec![], 11, 0, 0)),
            included_context(11, 0),
        )]),
    )?;
    assert!(miss.applied.is_empty());
    assert_eq!(matcher_a.load(Ordering::Relaxed), 0);
    assert_eq!(matcher_b.load(Ordering::Relaxed), 1);
    assert_eq!(handle_a.load(Ordering::Relaxed), 0);
    assert_eq!(handle_b.load(Ordering::Relaxed), 1);
    Ok(())
}

struct LogIndexWriter {
    id: HandlerId,
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for LogIndexWriter {
    fn id(&self) -> HandlerId {
        self.id.clone()
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
        input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        let value = U256::from(ctx.log_index.expect("log context carries log index"));
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                log.address(),
                self.slot,
                value,
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

struct TopicMatcher {
    index: usize,
    value: B256,
}

impl LogMatcher for TopicMatcher {
    fn matches(&self, log: &Log) -> bool {
        log.topics().get(self.index) == Some(&self.value)
    }
}

#[derive(Default)]
struct RecordingHook {
    applied_values: Arc<Mutex<Vec<U256>>>,
    hook_signals: Arc<Mutex<Vec<String>>>,
}

impl ReactiveHook<Ethereum> for RecordingHook {
    fn on_report(&self, report: Arc<ReactiveReport<Ethereum>>) {
        if let ReactiveReport::Applied(AppliedReport {
            diff, hook_signals, ..
        }) = report.as_ref()
        {
            self.applied_values
                .lock()
                .unwrap()
                .extend(diff.slots.iter().map(|change| change.new));
            self.hook_signals.lock().unwrap().extend(
                hook_signals
                    .iter()
                    .map(|signal| format!("{}:{}", signal.namespace, signal.kind)),
            );
        }
    }
}

#[tokio::test]
async fn reactive_runtime_applies_state_updates_and_dispatches_applied_hooks() -> Result<()> {
    let emitter = Address::repeat_byte(0x71);
    let slot = U256::from(7);
    let value = U256::from(99);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, emitter);

    let hook = Arc::new(RecordingHook::default());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter::any_log_from(
        "writer", emitter, slot, value,
    )))?;
    runtime.register_hook(hook.clone())?;

    let report = runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(emitter, vec![keccak256(b"Write()")], 10, 0, 0)),
            included_context(10, 0),
        )]),
    )?;

    assert_eq!(cache.cached_storage_value(emitter, slot), Some(value));
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].handler_id, HandlerId::new("writer"));
    assert_eq!(
        report.applied[0].quality,
        StateEffectQuality::ExactFromInput
    );
    assert_eq!(*hook.applied_values.lock().unwrap(), vec![value]);
    assert_eq!(
        *hook.hook_signals.lock().unwrap(),
        vec!["test:slot.write".to_string()]
    );
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_orders_logs_dedupes_inputs_and_allows_sequential_writes() -> Result<()> {
    let emitter = Address::repeat_byte(0x72);
    let slot = U256::from(8);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, emitter);

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexWriter {
        id: HandlerId::new("log-index-writer"),
        address: emitter,
        slot,
    }))?;

    let topic = keccak256(b"Ordered()");
    let block = 20;
    let report = runtime.ingest_batch(
        &mut cache,
        batch(vec![
            (
                ReactiveInput::Log(rpc_log(emitter, vec![topic], block, 0, 2)),
                included_context(block, 2),
            ),
            (
                ReactiveInput::Log(rpc_log(emitter, vec![topic], block, 0, 1)),
                included_context(block, 1),
            ),
            (
                ReactiveInput::Log(rpc_log(emitter, vec![topic], block, 0, 1)),
                included_context(block, 1),
            ),
        ]),
    )?;

    let applied_log_indexes: Vec<u64> = report
        .applied
        .iter()
        .map(|applied| match applied.input_ref {
            InputRef::Log { log_index, .. } => log_index,
            _ => panic!("expected log input ref"),
        })
        .collect();

    assert_eq!(applied_log_indexes, vec![1, 2]);
    assert_eq!(
        cache.cached_storage_value(emitter, slot),
        Some(U256::from(2))
    );
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_routes_shared_emitters_with_local_topic_matchers() -> Result<()> {
    let vault = Address::repeat_byte(0x73);
    let pool_a = B256::repeat_byte(0xa0);
    let pool_b = B256::repeat_byte(0xb0);
    let slot = U256::from(9);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, vault);

    let mut handler_a = SlotWriter::any_log_from("pool-a", vault, slot, U256::from(100));
    handler_a.interest = ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(vault),
        local_matcher: Some(Arc::new(TopicMatcher {
            index: 1,
            value: pool_a,
        })),
        route_key: Some(RouteKeySpec::Topic { index: 1 }),
    });

    let mut handler_b = SlotWriter::any_log_from("pool-b", vault, slot, U256::from(200));
    handler_b.interest = ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(vault),
        local_matcher: Some(Arc::new(TopicMatcher {
            index: 1,
            value: pool_b,
        })),
        route_key: Some(RouteKeySpec::Topic { index: 1 }),
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(handler_a))?;
    runtime.register_handler(Arc::new(handler_b))?;

    let report = runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(
                vault,
                vec![keccak256(b"Swap(bytes32)"), pool_b],
                30,
                0,
                0,
            )),
            included_context(30, 0),
        )]),
    )?;

    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].handler_id, HandlerId::new("pool-b"));
    assert_eq!(
        cache.cached_storage_value(vault, slot),
        Some(U256::from(200))
    );
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_rejects_conflicting_effects_for_one_input() -> Result<()> {
    let emitter = Address::repeat_byte(0x74);
    let slot = U256::from(10);
    let mut cache = setup_cache().await?;
    let before = cache.cached_storage_value(emitter, slot);

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter::any_log_from(
        "first",
        emitter,
        slot,
        U256::from(1),
    )))?;
    runtime.register_handler(Arc::new(SlotWriter::any_log_from(
        "second",
        emitter,
        slot,
        U256::from(2),
    )))?;

    let err = runtime
        .ingest_batch(
            &mut cache,
            batch(vec![(
                ReactiveInput::Log(rpc_log(emitter, vec![keccak256(b"Conflict()")], 40, 0, 0)),
                included_context(40, 0),
            )]),
        )
        .expect_err("conflicting writes must be rejected before mutation");

    assert!(matches!(err, ReactiveError::ConflictingEffects { .. }));
    assert_eq!(cache.cached_storage_value(emitter, slot), before);
    Ok(())
}

struct PendingCanonicalWriter;

impl ReactiveHandler<Ethereum> for PendingCanonicalWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("pending-writer")
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

#[tokio::test]
async fn reactive_runtime_rejects_canonical_cache_effects_for_pending_inputs() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PendingCanonicalWriter))?;

    let err = runtime
        .ingest_batch(
            &mut cache,
            batch(vec![(
                ReactiveInput::PendingTxHash(B256::repeat_byte(0x99)),
                ReactiveContext {
                    chain_id: Some(1),
                    source: InputSource::Batch,
                    chain_status: ChainStatus::Pending,
                    block: None,
                    transaction_index: None,
                    log_index: None,
                },
            )]),
        )
        .expect_err("pending inputs must not mutate canonical cache state");

    assert!(matches!(err, ReactiveError::InvalidPendingEffect { .. }));
    Ok(())
}

struct InvalidateAndResyncHandler {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for InvalidateAndResyncHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("invalidate-resync")
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
            effects: vec![
                ReactiveEffect::Invalidate(InvalidationRequest {
                    scope: PurgeScope::Slots(vec![self.slot]),
                    address: self.address,
                    reason: InvalidationReason::HandlerRequested,
                }),
                ReactiveEffect::Resync(ResyncRequest {
                    id: ResyncId::new("resync-slot"),
                    reason: ResyncReason::HandlerRequested,
                    block: ResyncBlock::Hash {
                        number: 50,
                        hash: B256::repeat_byte(0x50),
                        require_canonical: true,
                    },
                    targets: vec![ResyncTarget::StorageSlot {
                        address: self.address,
                        slot: self.slot,
                    }],
                    priority: ResyncPriority::High,
                }),
            ],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

#[tokio::test]
async fn reactive_runtime_lowers_invalidations_and_surfaces_resync_requests() -> Result<()> {
    let address = Address::repeat_byte(0x76);
    let slot = U256::from(11);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(123))?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(InvalidateAndResyncHandler { address, slot }))?;

    let report = runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"Repair()")], 50, 0, 0)),
            included_context(50, 0),
        )]),
    )?;

    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].invalidations.len(), 1);
    assert_eq!(report.applied[0].resyncs.len(), 1);
    assert_eq!(report.applied[0].diff.purged.len(), 1);
    assert_eq!(report.resyncs.len(), 1);
    assert_eq!(report.resyncs[0].priority, ResyncPriority::High);
    Ok(())
}

struct PendingSpeculativeHandler;

impl ReactiveHandler<Ethereum> for PendingSpeculativeHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("pending-speculative")
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
            effects: vec![ReactiveEffect::Speculative(SpeculativeRequest {
                id: SpeculativeId::new("pending-signal"),
                input_ref: InputRef::PendingTx {
                    chain_id: None,
                    hash: B256::ZERO,
                },
                labels: vec![ReportTag::new("kind", "pending")],
            })],
            quality: StateEffectQuality::NoStateEffect,
            tags: vec![],
        })
    }
}

#[tokio::test]
async fn reactive_runtime_allows_speculative_pending_effects_without_cache_mutation() -> Result<()>
{
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PendingSpeculativeHandler))?;
    let tx_hash = B256::repeat_byte(0x77);

    let report = runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::PendingTxHash(tx_hash),
            ReactiveContext {
                chain_id: Some(1),
                source: InputSource::Batch,
                chain_status: ChainStatus::Pending,
                block: None,
                transaction_index: None,
                log_index: None,
            },
        )]),
    )?;

    assert!(report.applied[0].state_updates.is_empty());
    assert_eq!(report.speculative.len(), 1);
    assert_eq!(
        report.speculative[0].input_ref,
        InputRef::PendingTx {
            chain_id: Some(1),
            hash: tx_hash,
        }
    );
    Ok(())
}

// --- Handler lifecycle accessors (0.2.0 register/unregister support) ---

#[tokio::test]
async fn reactive_runtime_last_canonical_block_tracks_journal_head() -> Result<()> {
    let emitter = Address::repeat_byte(0x91);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, emitter);

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter::any_log_from(
        "writer",
        emitter,
        U256::from(1),
        U256::from(2),
    )))?;

    // Nothing ingested yet: no canonical position.
    assert!(runtime.last_canonical_block().is_none());

    runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(emitter, vec![keccak256(b"Write()")], 100, 0, 0)),
            included_context(100, 0),
        )]),
    )?;
    let head = runtime
        .last_canonical_block()
        .expect("a canonical block should be journaled");
    assert_eq!(head.number, 100);

    // A later canonical block advances the head.
    runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(emitter, vec![keccak256(b"Write()")], 105, 0, 0)),
            included_context(105, 0),
        )]),
    )?;
    assert_eq!(runtime.last_canonical_block().map(|b| b.number), Some(105));
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_cancel_pending_resyncs_drops_targeted_account() -> Result<()> {
    let address = Address::repeat_byte(0x76);
    let slot = U256::from(11);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(123))?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(InvalidateAndResyncHandler { address, slot }))?;

    // ingest_batch (no resync execution) leaves the request queued in the
    // pending ledger.
    runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"Repair()")], 50, 0, 0)),
            included_context(50, 0),
        )]),
    )?;
    assert_eq!(runtime.pending_resyncs().len(), 1);

    // An unrelated address cancels nothing.
    let none = runtime.cancel_pending_resyncs(Address::repeat_byte(0xff));
    assert!(none.is_empty());
    assert_eq!(runtime.pending_resyncs().len(), 1);

    // The targeted address is cancelled and returned.
    let cancelled = runtime.cancel_pending_resyncs(address);
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].id, ResyncId::new("resync-slot"));
    assert_eq!(cancelled[0].targets.len(), 1);
    assert!(runtime.pending_resyncs().is_empty());
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_cancel_pending_resyncs_preserves_other_targets() -> Result<()> {
    let keep = Address::repeat_byte(0x33);
    let drop = Address::repeat_byte(0x44);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, keep);

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(MultiTargetResyncHandler {
        emitter: keep,
        keep,
        drop,
    }))?;

    runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(keep, vec![keccak256(b"Multi()")], 60, 0, 0)),
            included_context(60, 0),
        )]),
    )?;
    assert_eq!(runtime.pending_resyncs().len(), 1);
    assert_eq!(runtime.pending_resyncs()[0].targets.len(), 2);

    // Cancelling one account keeps the request alive with its other target.
    let cancelled = runtime.cancel_pending_resyncs(drop);
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].targets.len(), 1);
    assert_eq!(runtime.pending_resyncs().len(), 1);
    assert_eq!(runtime.pending_resyncs()[0].targets.len(), 1);
    Ok(())
}

struct SameAddressResyncHandler {
    emitter: Address,
    target: Address,
}

impl ReactiveHandler<Ethereum> for SameAddressResyncHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("same-address-resyncs")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.emitter),
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
        let request = |id: &str, slot: u64| {
            ReactiveEffect::Resync(ResyncRequest {
                id: ResyncId::new(id),
                reason: ResyncReason::HandlerRequested,
                block: ResyncBlock::Latest,
                targets: vec![ResyncTarget::StorageSlot {
                    address: self.target,
                    slot: U256::from(slot),
                }],
                priority: ResyncPriority::Normal,
            })
        };
        Ok(HandlerOutcome {
            effects: vec![request("pool-a", 1), request("pool-b", 2)],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

#[tokio::test]
async fn reactive_runtime_cancels_exact_resync_id_and_preserves_same_address_work() -> Result<()> {
    let emitter = Address::repeat_byte(0x81);
    let shared_vault = Address::repeat_byte(0x82);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SameAddressResyncHandler {
        emitter,
        target: shared_vault,
    }))?;
    runtime.ingest_batch(
        &mut cache,
        batch(vec![(
            ReactiveInput::Log(rpc_log(emitter, vec![], 70, 0, 0)),
            included_context(70, 0),
        )]),
    )?;
    assert_eq!(runtime.pending_resyncs().len(), 2);

    let cancelled = runtime.cancel_pending_resync(&ResyncId::new("pool-a"));

    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].id, ResyncId::new("pool-a"));
    assert_eq!(runtime.pending_resyncs().len(), 1);
    assert_eq!(runtime.pending_resyncs()[0].id, ResyncId::new("pool-b"));
    assert_eq!(
        runtime.pending_resyncs()[0].targets,
        vec![ResyncTarget::StorageSlot {
            address: shared_vault,
            slot: U256::from(2),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_batches_exact_resync_cancellation_in_queue_order() -> Result<()> {
    let emitter = Address::repeat_byte(0x83);
    let shared_vault = Address::repeat_byte(0x84);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SameAddressResyncHandler {
        emitter,
        target: shared_vault,
    }))?;
    const BACKLOG_EVENTS: u64 = 256;
    runtime.ingest_batch(
        &mut cache,
        batch(
            (0..BACKLOG_EVENTS)
                .map(|index| {
                    (
                        ReactiveInput::Log(rpc_log(emitter, vec![], 71, 0, index)),
                        included_context(71, index),
                    )
                })
                .collect(),
        ),
    )?;
    assert_eq!(runtime.pending_resyncs().len(), 2 * BACKLOG_EVENTS as usize);
    assert!(runtime.has_journaled_handler_effects(&HandlerId::new("same-address-resyncs")));
    let journaled_handlers = runtime.journaled_handler_ids();
    assert_eq!(journaled_handlers.len(), 1);
    assert!(journaled_handlers.contains(&HandlerId::new("same-address-resyncs")));

    let cancelled = runtime.cancel_pending_resyncs_by_id(&[
        ResyncId::new("pool-b"),
        ResyncId::new("pool-a"),
        ResyncId::new("pool-a"),
        ResyncId::new("missing"),
    ]);

    assert_eq!(cancelled.len(), 2 * BACKLOG_EVENTS as usize);
    assert!(
        cancelled.chunks_exact(2).all(|requests| {
            requests[0].id == ResyncId::new("pool-a") && requests[1].id == ResyncId::new("pool-b")
        }),
        "cancelled requests retain pending-queue order, not caller ID order"
    );
    assert!(runtime.pending_resyncs().is_empty());
    Ok(())
}

struct MultiTargetResyncHandler {
    emitter: Address,
    keep: Address,
    drop: Address,
}

impl ReactiveHandler<Ethereum> for MultiTargetResyncHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("multi-target-resync")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.emitter),
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
                id: ResyncId::new("multi"),
                reason: ResyncReason::HandlerRequested,
                block: ResyncBlock::Latest,
                targets: vec![
                    ResyncTarget::Account {
                        address: self.keep,
                        fields: evm_fork_cache::reactive::AccountFieldMask {
                            balance: true,
                            ..Default::default()
                        },
                    },
                    ResyncTarget::Account {
                        address: self.drop,
                        fields: evm_fork_cache::reactive::AccountFieldMask {
                            balance: true,
                            ..Default::default()
                        },
                    },
                ],
                priority: ResyncPriority::Normal,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}
