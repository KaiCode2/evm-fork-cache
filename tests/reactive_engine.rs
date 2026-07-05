//! Implementation-owned tests for the runtime/subscriber binding layer.
#![cfg(feature = "reactive")]

mod common;

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use alloy_network::{Ethereum, Network};
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, keccak256};
use alloy_rpc_types_eth::{Filter, Log};

use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AccountFieldMask, BlockRef, ChainStatus, EventSubscriber, HandlerError, HandlerId,
    HandlerOutcome, InputSource, InterestOwnerSubscriber, LogInterest, ReactiveConfig,
    ReactiveContext, ReactiveEffect, ReactiveEngine, ReactiveEngineRegisterError, ReactiveHandler,
    ReactiveInput, ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveRegistry,
    ReactiveReport, RegisterError, ResyncBlock, ResyncId, ResyncPriority, ResyncReason,
    ResyncRequest, ResyncTarget, RouteKeySpec, StateEffectQuality, SubscriberBackfill,
    SubscriberError, SubscriberNextBatch,
};

fn rpc_log(address: Address, topic0: B256, block_number: u64) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, vec![topic0], Bytes::new()),
        block_hash: Some(B256::repeat_byte(block_number as u8)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0xcc)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

fn included_context(block_number: u64) -> ReactiveContext {
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
        log_index: Some(0),
    }
}

fn canonical_log_batch(address: Address, block_number: u64) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(address, keccak256(b"Event()"), block_number)),
        included_context(block_number),
    )])
}

struct RecordingSubscriber<N: Network = Ethereum> {
    full_replace_interests: Vec<ReactiveInterest<N>>,
    owners: HashMap<HandlerId, Vec<ReactiveInterest<N>>>,
    backfills: Vec<(HandlerId, SubscriberBackfill)>,
    batches: VecDeque<ReactiveInputBatch<N>>,
    fail_owner: Option<HandlerId>,
}

impl<N: Network> Default for RecordingSubscriber<N> {
    fn default() -> Self {
        Self {
            full_replace_interests: Vec::new(),
            owners: HashMap::new(),
            backfills: Vec::new(),
            batches: VecDeque::new(),
            fail_owner: None,
        }
    }
}

impl<N: Network> RecordingSubscriber<N> {
    fn fail_owner(owner: HandlerId) -> Self {
        Self {
            fail_owner: Some(owner),
            ..Self::default()
        }
    }
}

impl<N> EventSubscriber<N> for RecordingSubscriber<N>
where
    N: Network + Send + 'static,
{
    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError> {
        self.full_replace_interests = interests.to_vec();
        self.owners.clear();
        Ok(())
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }
}

impl<N> InterestOwnerSubscriber<N> for RecordingSubscriber<N>
where
    N: Network + Send + 'static,
{
    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError> {
        if self.fail_owner.as_ref() == Some(&owner) {
            return Err(SubscriberError::InvalidConfig("forced owner failure"));
        }
        self.owners.insert(owner, interests.to_vec());
        Ok(())
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        backfill: SubscriberBackfill,
    ) -> Result<(), SubscriberError> {
        self.add_interest_owner(owner.clone(), interests)?;
        self.backfills.push((owner, backfill));
        Ok(())
    }

    fn remove_interest_owner(&mut self, owner: &HandlerId) -> Option<Vec<ReactiveInterest<N>>> {
        self.owners.remove(owner)
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<N>]> {
        self.owners.get(owner).map(Vec::as_slice)
    }
}

struct NoopHandler {
    id: HandlerId,
    address: Address,
}

impl NoopHandler {
    fn new(id: &'static str, address: Address) -> Self {
        Self {
            id: HandlerId::new(id),
            address,
        }
    }
}

impl ReactiveHandler<Ethereum> for NoopHandler {
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
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect))
    }
}

#[test]
fn engine_register_handler_updates_runtime_and_subscriber() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );

    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect("engine registration should succeed");

    assert!(engine.runtime().contains_handler(&HandlerId::new("pool-a")));
    assert_eq!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool-a"))
            .expect("subscriber owner interests")
            .len(),
        1
    );
}

#[test]
fn engine_duplicate_handler_does_not_mutate_subscriber() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect("initial registration should succeed");

    let err = engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xb2),
        )))
        .expect_err("duplicate id should fail before subscriber mutation");

    assert!(matches!(
        err,
        ReactiveEngineRegisterError::Register(RegisterError::DuplicateHandler(id))
            if id == HandlerId::new("pool-a")
    ));
    assert_eq!(engine.subscriber().owners.len(), 1);
    assert_eq!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool-a"))
            .expect("pool-a owner")
            .len(),
        1
    );
}

#[test]
fn engine_rolls_back_runtime_when_subscriber_registration_fails() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::fail_owner(HandlerId::new("pool-a")),
    );

    let err = engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect_err("subscriber failure should fail engine registration");

    assert!(matches!(
        err,
        ReactiveEngineRegisterError::Subscriber(SubscriberError::InvalidConfig(_))
    ));
    assert!(!engine.runtime().contains_handler(&HandlerId::new("pool-a")));
    assert!(engine.subscriber().owners.is_empty());
}

#[test]
fn engine_unregister_handler_updates_subscriber_then_runtime() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect("engine registration should succeed");

    let removed = engine
        .unregister_handler(&HandlerId::new("pool-a"))
        .expect("runtime handler should be removed");

    assert_eq!(removed.id(), HandlerId::new("pool-a"));
    assert!(!engine.runtime().contains_handler(&HandlerId::new("pool-a")));
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool-a"))
            .is_none()
    );
}

#[test]
fn engine_register_handler_with_backfill_records_owner_backfill() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    let backfill = SubscriberBackfill::range(100, 120);

    engine
        .register_handler_with_backfill(
            Arc::new(NoopHandler::new("pool-a", Address::repeat_byte(0xa1))),
            backfill,
        )
        .expect("engine registration with backfill should succeed");

    assert!(engine.runtime().contains_handler(&HandlerId::new("pool-a")));
    assert_eq!(
        engine.subscriber().backfills,
        vec![(HandlerId::new("pool-a"), backfill)]
    );
}

#[test]
fn engine_sync_handler_interests_bootstraps_owner_per_handler() {
    // A runtime pre-populated with handlers (registered directly, not through
    // the engine) is bootstrapped onto a fresh subscriber via
    // `sync_handler_interests`: each handler becomes its own owner, and the
    // full-replacement blob is never touched.
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect("register pool-a on runtime");
    runtime
        .register_handler(Arc::new(NoopHandler::new(
            "pool-b",
            Address::repeat_byte(0xb2),
        )))
        .expect("register pool-b on runtime");

    let mut engine = ReactiveEngine::new(runtime, RecordingSubscriber::default());
    engine
        .sync_handler_interests()
        .expect("bootstrap sync should succeed");

    assert_eq!(engine.subscriber().owners.len(), 2);
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool-a"))
            .is_some()
    );
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool-b"))
            .is_some()
    );
    // Bootstrap uses the owner-scoped path, not the full-replacement blob.
    assert!(engine.subscriber().full_replace_interests.is_empty());

    // Re-running is idempotent (upsert semantics).
    engine
        .sync_handler_interests()
        .expect("re-sync should succeed");
    assert_eq!(engine.subscriber().owners.len(), 2);
}

#[test]
fn registry_and_engine_expose_same_interests_after_registration() {
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect("registry registration should succeed");

    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect("engine registration should succeed");

    assert_eq!(
        engine.runtime().interests().len(),
        registry.interests().len()
    );
    assert_eq!(engine.subscriber().owners.len(), 1);
}

// A handler that emits a storage resync for its account on every matching log —
// used to drive the resync-execution and teardown paths.
struct ResyncHandler {
    id: HandlerId,
    address: Address,
}

impl ReactiveHandler<Ethereum> for ResyncHandler {
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
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::Resync(ResyncRequest {
                id: ResyncId::new("acct-repair"),
                reason: ResyncReason::HandlerRequested,
                block: ResyncBlock::Latest,
                targets: vec![ResyncTarget::Account {
                    address: self.address,
                    fields: AccountFieldMask {
                        balance: true,
                        ..Default::default()
                    },
                }],
                priority: ResyncPriority::Normal,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

#[tokio::test]
async fn engine_register_handler_auto_anchors_from_last_canonical_block() {
    let emitter = Address::repeat_byte(0xa1);
    let mut cache = setup_cache().await.expect("cache");
    install_mock_erc20(&mut cache, emitter);

    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new("seed", emitter)))
        .expect("register seed handler");

    // Drive the runtime's canonical position to block 500.
    engine
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 500))
        .expect("ingest canonical block");
    assert_eq!(
        engine.runtime().last_canonical_block().map(|b| b.number),
        Some(500)
    );

    // A handler registered now should be backfilled from the canonical head.
    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-late",
            Address::repeat_byte(0xb2),
        )))
        .expect("register late handler");

    assert_eq!(
        engine.subscriber().backfills,
        vec![(
            HandlerId::new("pool-late"),
            SubscriberBackfill::from_block(500)
        )],
        "mid-lifecycle registration must anchor to the last canonical block"
    );
}

#[test]
fn engine_register_handler_on_fresh_runtime_is_live_only() {
    // With no canonical block journaled yet, default registration requests no
    // backfill (bootstrap before ingestion).
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .expect("register on fresh runtime");
    assert!(
        engine.subscriber().backfills.is_empty(),
        "no canonical block => no backfill"
    );
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool-a"))
            .is_some()
    );
}

#[tokio::test]
async fn engine_register_handler_live_only_never_backfills() {
    let emitter = Address::repeat_byte(0xa1);
    let mut cache = setup_cache().await.expect("cache");
    install_mock_erc20(&mut cache, emitter);

    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new("seed", emitter)))
        .expect("register seed");
    engine
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 500))
        .expect("ingest");

    // Even past a canonical block, live-only registration opts out of backfill.
    engine
        .register_handler_live_only(Arc::new(NoopHandler::new(
            "pool-live",
            Address::repeat_byte(0xb2),
        )))
        .expect("register live-only");
    assert!(engine.subscriber().backfills.is_empty());
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool-live"))
            .is_some()
    );
}

#[tokio::test]
async fn engine_next_ingest_with_resync_executes_surfaced_resyncs() {
    let emitter = Address::repeat_byte(0xd1);
    let mut cache = setup_cache().await.expect("cache");
    install_mock_erc20(&mut cache, emitter);

    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(ResyncHandler {
            id: HandlerId::new("repair"),
            address: emitter,
        }))
        .expect("register resync handler");

    // Queue a batch for the subscriber to hand back, then drive the
    // resync-executing loop.
    engine
        .subscriber_mut()
        .batches
        .push_back(canonical_log_batch(emitter, 700));

    let report = engine
        .next_ingest_with_resync(&mut cache)
        .await
        .expect("next_ingest_with_resync")
        .expect("a batch was queued");

    // The surfaced resync was executed (a Resynced report is present) and the
    // pending ledger was drained by the with-resync path.
    assert!(
        report
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::Resynced(_))),
        "resync execution should produce a Resynced report"
    );
    assert!(engine.runtime().pending_resyncs().is_empty());
}

#[tokio::test]
async fn engine_teardown_recipe_clears_routing_tracking_and_pending_resyncs() {
    use evm_fork_cache::reactive::TrackingPolicy;

    let emitter = Address::repeat_byte(0xe1);
    let mut cache = setup_cache().await.expect("cache");
    install_mock_erc20(&mut cache, emitter);

    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(ResyncHandler {
            id: HandlerId::new("pool"),
            address: emitter,
        }))
        .expect("register");
    engine
        .runtime_mut()
        .track_account(emitter, TrackingPolicy::WholeAccount);

    // Ingest (no resync execution) so a pending resync accumulates.
    engine
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 800))
        .expect("ingest");
    assert_eq!(engine.runtime().pending_resyncs().len(), 1);

    // Full teardown recipe.
    let removed = engine.unregister_handler(&HandlerId::new("pool"));
    assert!(removed.is_some());
    assert!(engine.runtime_mut().untrack_account(emitter));
    let cancelled = engine.runtime_mut().cancel_pending_resyncs(emitter);
    assert_eq!(cancelled.len(), 1);

    // Routing, subscriber ownership, and the pending ledger are all clear.
    assert!(!engine.runtime().contains_handler(&HandlerId::new("pool")));
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("pool"))
            .is_none()
    );
    assert!(engine.runtime().pending_resyncs().is_empty());
}
