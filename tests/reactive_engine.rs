//! Implementation-owned tests for the runtime/subscriber binding layer.
#![cfg(feature = "reactive")]

mod common;

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use alloy_eips::BlockId;
use alloy_network::{Ethereum, Network};
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};

use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AccountFieldMask, BlockRef, ChainControl, ChainStatus, DeliveryAudience, DeliveryScope,
    EventSubscriber, HandlerError, HandlerId, HandlerOutcome, InputRef, InputSource,
    InterestOwnerSubscriber, LogInterest, ReactiveBaselineError, ReactiveCanonicalBaseline,
    ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveEngine, ReactiveEngineError,
    ReactiveEngineRegisterError, ReactiveError, ReactiveHandler, ReactiveInput, ReactiveInputBatch,
    ReactiveInputDelivery, ReactiveInputRecord, ReactiveInterest, ReactiveRegistry, ReactiveReport,
    RegisterError, ResyncBlock, ResyncId, ResyncPriority, ResyncReason, ResyncRequest,
    ResyncTarget, RouteKeySpec, StateEffectQuality, SubscriberBackfill, SubscriberCapabilities,
    SubscriberCheckpoint, SubscriberDeliveryToken, SubscriberError, SubscriberNextBatch,
    SubscriberOperation,
};
use evm_fork_cache::state_update::StateUpdate;
use evm_fork_cache::{DurableCheckpointIdentity, DurableCheckpointStore};

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
            block,
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

fn owner_catchup_log_batch(
    address: Address,
    block_number: u64,
    owner: HandlerId,
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::from_deliveries([ReactiveInputDelivery::new(
        ReactiveInputRecord::new(
            ReactiveInput::Log(rpc_log(address, keccak256(b"Event()"), block_number)),
            included_context(block_number),
        ),
        DeliveryAudience::Owners(vec![owner]),
        DeliveryScope::OwnerCatchup,
    )])
}

struct RecordingSubscriber<N: Network = Ethereum> {
    full_replace_interests: Vec<ReactiveInterest<N>>,
    owners: HashMap<HandlerId, Vec<ReactiveInterest<N>>>,
    backfills: Vec<(HandlerId, SubscriberBackfill)>,
    coordinated_catchups: Vec<(HandlerId, BlockRef)>,
    batches: VecDeque<ReactiveInputBatch<N>>,
    acknowledged: Vec<SubscriberDeliveryToken>,
    bulk_upserts: usize,
    bulk_replacements: usize,
    replacement_backfill: Option<SubscriberBackfill>,
    fail_acknowledgement: bool,
    hold_acknowledgement: bool,
    fail_owner: Option<HandlerId>,
}

impl<N: Network> Default for RecordingSubscriber<N> {
    fn default() -> Self {
        Self {
            full_replace_interests: Vec::new(),
            owners: HashMap::new(),
            backfills: Vec::new(),
            coordinated_catchups: Vec::new(),
            batches: VecDeque::new(),
            acknowledged: Vec::new(),
            bulk_upserts: 0,
            bulk_replacements: 0,
            replacement_backfill: None,
            fail_acknowledgement: false,
            hold_acknowledgement: false,
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
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.full_replace_interests = interests;
            self.owners.clear();
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if self.hold_acknowledgement {
                std::future::pending::<()>().await;
            }
            if self.fail_acknowledgement {
                return Err(SubscriberError::InvalidConfig(
                    "forced acknowledgement failure",
                ));
            }
            self.acknowledged.push(token);
            Ok(())
        })
    }
}

#[test]
fn subscriber_capabilities_fail_closed_by_default() {
    let subscriber = RecordingSubscriber::<Ethereum>::default();
    assert_eq!(subscriber.capabilities(), SubscriberCapabilities::default());
    assert!(!subscriber.capabilities().supports_live());
    assert!(!subscriber.capabilities().supports_durable_replay());
    assert!(!subscriber.capabilities().supports_explicit_reorgs());
}

#[tokio::test]
async fn checkpointed_ingest_rejects_non_durable_subscriber_before_polling() {
    let emitter = Address::repeat_byte(0xda);
    let subscriber = RecordingSubscriber {
        batches: VecDeque::from([canonical_log_batch(emitter, 1)]),
        ..RecordingSubscriber::default()
    };
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );
    let mut cache = setup_cache().await.expect("cache");
    let path = std::env::temp_dir().join(format!(
        "evm-fork-cache-nondurable-{}-checkpoint.bin",
        std::process::id()
    ));
    let store = DurableCheckpointStore::new(path);
    let identity = DurableCheckpointIdentity::new(1, "ephemeral", "handlers-v1");

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity)
        .await
        .expect_err("ephemeral delivery cannot be presented as restart safe");

    assert!(matches!(error, ReactiveEngineError::SubscriberNotDurable));
    assert_eq!(engine.subscriber().batches.len(), 1);
    assert!(engine.runtime().last_canonical_block().is_none());
}

#[tokio::test]
async fn engine_rechecks_lazily_resolved_chain_before_control_only_ingest() {
    struct LazyWrongChainSubscriber {
        chain_id: Option<u64>,
        batch: Option<ReactiveInputBatch<Ethereum>>,
    }
    impl EventSubscriber<Ethereum> for LazyWrongChainSubscriber {
        fn chain_id(&self) -> Option<u64> {
            self.chain_id
        }

        fn register_interests(
            &mut self,
            _interests: &[ReactiveInterest<Ethereum>],
        ) -> SubscriberOperation<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
            Box::pin(async move {
                self.chain_id = Some(2);
                Ok(self.batch.take())
            })
        }
    }

    let progress = BlockRef {
        number: 10,
        hash: B256::repeat_byte(10),
        parent_hash: Some(B256::repeat_byte(9)),
        timestamp: Some(1_700_000_010),
    };
    let subscriber = LazyWrongChainSubscriber {
        chain_id: None,
        batch: Some(
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(2)
                .with_chain_controls([ChainControl::CanonicalProgress(progress)]),
        ),
    };
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );
    let mut cache = setup_cache().await.expect("cache");

    let error = engine
        .next_ingest(&mut cache)
        .await
        .expect_err("wrong-chain control must be rejected after lazy identity resolution");

    assert!(matches!(
        error,
        ReactiveEngineError::SubscriberChainMismatch {
            subscriber_chain_id: 2,
            cache_chain_id: 1
        }
    ));
    assert!(engine.runtime().last_canonical_block().is_none());
}

#[tokio::test]
async fn runtime_public_ingest_paths_reject_owner_catchup_outside_journal() {
    let emitter = Address::repeat_byte(0xa1);
    let owner = HandlerId::new("pool-a");
    let mut cache = setup_cache().await.expect("cache");
    let generation = cache.snapshot_generation();
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(NoopHandler::new("pool-a", emitter)))
        .expect("register handler");

    for with_resync in [false, true] {
        let batch = owner_catchup_log_batch(emitter, 50, owner.clone());
        let error = if with_resync {
            runtime
                .ingest_batch_with_resync(&mut cache, batch)
                .expect_err("owner catch-up outside journal must fail")
        } else {
            runtime
                .ingest_batch(&mut cache, batch)
                .expect_err("owner catch-up outside journal must fail")
        };
        assert!(matches!(
            error,
            ReactiveError::OwnerCatchupOutsideJournal { number: 50, .. }
        ));
        assert_eq!(runtime.last_canonical_block(), None);
        assert_eq!(cache.snapshot_generation(), generation);
    }
}

#[tokio::test]
async fn engine_public_ingest_paths_map_owner_catchup_guard() {
    let emitter = Address::repeat_byte(0xa1);
    let owner = HandlerId::new("pool-a");
    let mut cache = setup_cache().await.expect("cache");
    let generation = cache.snapshot_generation();
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new("pool-a", emitter)))
        .await
        .expect("register handler");

    for with_resync in [false, true] {
        let batch = owner_catchup_log_batch(emitter, 50, owner.clone());
        let error = if with_resync {
            engine
                .ingest_batch_with_resync(&mut cache, batch)
                .expect_err("owner catch-up outside journal must fail")
        } else {
            engine
                .ingest_batch(&mut cache, batch)
                .expect_err("owner catch-up outside journal must fail")
        };
        assert!(matches!(
            error,
            ReactiveEngineError::OwnerCatchupOutsideJournal { number: 50, .. }
        ));
        assert_eq!(engine.runtime().last_canonical_block(), None);
        assert_eq!(cache.snapshot_generation(), generation);
    }
}

#[tokio::test]
async fn combined_poll_ingest_paths_reject_owner_catchup_outside_journal() {
    let emitter = Address::repeat_byte(0xa1);
    let owner = HandlerId::new("pool-a");

    for with_resync in [false, true] {
        let mut cache = setup_cache().await.expect("cache");
        let generation = cache.snapshot_generation();
        let mut subscriber = RecordingSubscriber::default();
        subscriber
            .batches
            .push_back(owner_catchup_log_batch(emitter, 50, owner.clone()));
        let mut engine = ReactiveEngine::new(
            evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
            subscriber,
        );
        engine
            .register_handler(Arc::new(NoopHandler::new("pool-a", emitter)))
            .await
            .expect("register handler");

        let error = if with_resync {
            engine
                .next_ingest_with_resync(&mut cache)
                .await
                .expect_err("owner catch-up outside journal must fail")
        } else {
            engine
                .next_ingest(&mut cache)
                .await
                .expect_err("owner catch-up outside journal must fail")
        };
        assert!(matches!(
            error,
            ReactiveEngineError::OwnerCatchupOutsideJournal { number: 50, .. }
        ));
        assert_eq!(engine.runtime().last_canonical_block(), None);
        assert_eq!(cache.snapshot_generation(), generation);
    }
}

#[tokio::test]
async fn owner_catchup_rejected_when_same_batch_reorg_drops_its_journal_entry() {
    let emitter = Address::repeat_byte(0xa1);
    let owner = HandlerId::new("pool-a");
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(NoopHandler::new("pool-a", emitter)))
        .expect("register handler");
    runtime
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 49))
        .expect("seed ancestor");
    runtime
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 50))
        .expect("seed old tip");
    let old_tip = included_context(50).block.expect("old tip");
    let common_ancestor = included_context(49).block.expect("ancestor");
    let new_tip = BlockRef {
        number: 50,
        hash: B256::repeat_byte(0xfe),
        parent_hash: Some(common_ancestor.hash),
        timestamp: old_tip.timestamp,
    };
    let generation = cache.snapshot_generation();
    let batch =
        owner_catchup_log_batch(emitter, 50, owner).with_chain_controls([ChainControl::Reorg {
            common_ancestor,
            old_tip,
            new_tip,
        }]);

    let error = runtime
        .ingest_batch(&mut cache, batch)
        .expect_err("same-batch reorg invalidates owner rollback entry");
    assert!(matches!(
        error,
        ReactiveError::OwnerCatchupOutsideJournal { number: 50, .. }
    ));
    assert_eq!(runtime.last_canonical_block(), Some(old_tip));
    assert_eq!(cache.snapshot_generation(), generation);
}

#[tokio::test]
async fn owner_catchup_rejects_conflicting_retained_block_metadata_before_handlers() {
    let emitter = Address::repeat_byte(0xa1);
    let owner = HandlerId::new("pool-a");

    for conflict in ["parent", "context-timestamp", "payload-timestamp"] {
        let mut cache = setup_cache().await.expect("cache");
        let mut runtime =
            evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime
            .register_handler(Arc::new(NoopHandler::new("pool-a", emitter)))
            .expect("register handler");
        runtime
            .ingest_batch(&mut cache, canonical_log_batch(emitter, 50))
            .expect("seed retained journal block");
        let retained = runtime.last_canonical_block().expect("retained block");
        let conflicting = BlockRef {
            parent_hash: (conflict == "parent")
                .then_some(B256::repeat_byte(0xfd))
                .or(retained.parent_hash),
            timestamp: match conflict {
                "context-timestamp" => Some(retained.timestamp.expect("timestamp") + 1),
                "payload-timestamp" => None,
                _ => retained.timestamp,
            },
            ..retained
        };
        let mut log = rpc_log(emitter, keccak256(b"Event()"), 50);
        log.block_timestamp = if conflict == "payload-timestamp" {
            Some(retained.timestamp.expect("timestamp") + 1)
        } else {
            conflicting.timestamp
        };
        let context = ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Backfill,
            chain_status: ChainStatus::Included {
                block: conflicting,
                confirmations: 0,
            },
            block: Some(conflicting),
            transaction_index: log.transaction_index,
            log_index: log.log_index,
        };
        let batch = ReactiveInputBatch::from_deliveries([ReactiveInputDelivery::new(
            ReactiveInputRecord::new(ReactiveInput::Log(log), context),
            DeliveryAudience::Owners(vec![owner.clone()]),
            DeliveryScope::OwnerCatchup,
        )]);
        let generation = cache.snapshot_generation();

        let error = runtime
            .ingest_batch(&mut cache, batch)
            .expect_err("conflicting optional block metadata must fail closed");
        assert!(matches!(
            error,
            ReactiveError::OwnerCatchupOutsideJournal { number: 50, .. }
        ));
        assert_eq!(runtime.last_canonical_block(), Some(retained));
        assert_eq!(cache.snapshot_generation(), generation);
    }
}

impl<N> InterestOwnerSubscriber<N> for RecordingSubscriber<N>
where
    N: Network + Send + 'static,
{
    fn upsert_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if owners
                .iter()
                .any(|(owner, _)| self.fail_owner.as_ref() == Some(owner))
            {
                return Err(SubscriberError::InvalidConfig("forced owner failure"));
            }
            for (owner, interests) in owners {
                self.owners.insert(owner, interests);
            }
            self.bulk_upserts += 1;
            Ok(())
        })
    }

    fn replace_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if owners
                .iter()
                .any(|(owner, _)| self.fail_owner.as_ref() == Some(owner))
            {
                return Err(SubscriberError::InvalidConfig("forced owner failure"));
            }
            self.owners = owners.into_iter().collect();
            self.replacement_backfill = None;
            self.bulk_replacements += 1;
            Ok(())
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if owners
                .iter()
                .any(|(owner, _)| self.fail_owner.as_ref() == Some(owner))
            {
                return Err(SubscriberError::InvalidConfig("forced owner failure"));
            }
            self.owners = owners.into_iter().collect();
            self.replacement_backfill = Some(backfill);
            self.bulk_replacements += 1;
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            if self.fail_owner.as_ref() == Some(&owner) {
                return Err(SubscriberError::InvalidConfig("forced owner failure"));
            }
            self.owners.insert(owner, interests);
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            if self.fail_owner.as_ref() == Some(&owner) {
                return Err(SubscriberError::InvalidConfig("forced owner failure"));
            }
            self.owners.insert(owner.clone(), interests);
            self.backfills.push((owner, backfill));
            Ok(())
        })
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            if self.fail_owner.as_ref() == Some(&owner) {
                return Err(SubscriberError::InvalidConfig("forced owner failure"));
            }
            self.owners.insert(owner.clone(), interests);
            self.coordinated_catchups.push((owner, retained));
            Ok(())
        })
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<N>>>> {
        let owner = owner.clone();
        Box::pin(async move { Ok(self.owners.remove(&owner)) })
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

struct SlotWriter {
    id: HandlerId,
    address: Address,
    slot: U256,
    value: U256,
}

impl ReactiveHandler<Ethereum> for SlotWriter {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: None,
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
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                log.address(),
                self.slot,
                self.value,
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: Vec::new(),
        })
    }
}

#[tokio::test]
async fn engine_register_handler_updates_runtime_and_subscriber() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );

    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .await
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

#[tokio::test]
async fn owner_scoped_batch_routes_only_to_named_handlers() {
    let emitter = Address::repeat_byte(0xa2);
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for id in ["existing", "new-owner"] {
        runtime
            .register_handler(Arc::new(NoopHandler::new(id, emitter)))
            .expect("register handler");
    }

    let report = runtime
        .ingest_batch(
            &mut cache,
            canonical_log_batch(emitter, 12)
                .with_audience(DeliveryAudience::Owners(vec![HandlerId::new("new-owner")])),
        )
        .expect("owner-scoped batch");

    let decoded = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Decoded(decoded) => Some(decoded.handler_ids.clone()),
            _ => None,
        });
    assert_eq!(decoded, Some(vec![HandlerId::new("new-owner")]));
}

#[tokio::test]
async fn runtime_rejects_payload_and_context_block_identity_disagreement_atomically() {
    let emitter = Address::repeat_byte(0xa7);
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(NoopHandler::new("mismatch", emitter)))
        .expect("handler");

    let record = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(emitter, keccak256(b"Mismatch()"), 21)),
        included_context(20),
    );
    let error = runtime
        .ingest_batch(&mut cache, ReactiveInputBatch::new(vec![record]))
        .expect_err("a source cannot journal one block while delivering another");

    assert!(matches!(error, ReactiveError::InvalidInputRecord { .. }));
    assert!(runtime.last_canonical_block().is_none());
}

#[tokio::test]
async fn runtime_rejects_conflicting_payloads_claiming_one_stable_identity() {
    let emitter = Address::repeat_byte(0xa8);
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let mut first = rpc_log(emitter, keccak256(b"First()"), 22);
    let mut conflicting = rpc_log(emitter, keccak256(b"Conflicting()"), 22);
    // Be explicit that every stable identity field is identical while the
    // event payload differs.
    conflicting.block_hash = first.block_hash;
    conflicting.transaction_hash = first.transaction_hash;
    conflicting.log_index = first.log_index;
    first.transaction_index = Some(0);
    conflicting.transaction_index = Some(0);

    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![
                ReactiveInputRecord::new(ReactiveInput::Log(first), included_context(22)),
                ReactiveInputRecord::new(ReactiveInput::Log(conflicting), included_context(22)),
            ]),
        )
        .expect_err("same-identity payload conflicts must fail closed");

    assert!(matches!(error, ReactiveError::InvalidInputRecord { .. }));
    assert!(runtime.last_canonical_block().is_none());
}

#[test]
fn compatible_duplicate_merge_is_order_independent_and_preserves_enrichment() {
    let emitter = Address::repeat_byte(0xa8);
    let mut historical_log = rpc_log(emitter, keccak256(b"Enriched()"), 24);
    historical_log.block_timestamp = None;
    let mut historical_context = included_context(24);
    historical_context.source = InputSource::Backfill;
    historical_context.block.as_mut().expect("block").timestamp = None;
    historical_context.chain_status = ChainStatus::Included {
        block: *historical_context.block.as_ref().expect("block"),
        confirmations: 2,
    };
    let historical: ReactiveInputRecord<Ethereum> =
        ReactiveInputRecord::new(ReactiveInput::Log(historical_log), historical_context);

    let mut finalized_context = included_context(24);
    finalized_context.source = InputSource::Subscription;
    finalized_context.chain_status = ChainStatus::Finalized {
        block: *finalized_context.block.as_ref().expect("block"),
    };
    let finalized = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(emitter, keccak256(b"Enriched()"), 24)),
        finalized_context,
    );

    let mut left_first = historical.clone();
    assert!(
        left_first
            .merge_compatible_duplicate(&finalized)
            .expect("compatible overlap")
    );
    let mut right_first = finalized;
    assert!(
        right_first
            .merge_compatible_duplicate(&historical)
            .expect("compatible reverse overlap")
    );

    assert_eq!(left_first.context, right_first.context);
    assert!(matches!(
        (&left_first.input, &right_first.input),
        (ReactiveInput::Log(left), ReactiveInput::Log(right)) if left == right
    ));
    assert!(matches!(
        left_first.context.chain_status,
        ChainStatus::Finalized { .. }
    ));
    assert_eq!(left_first.context.source, InputSource::Subscription);
    assert!(
        left_first
            .context
            .block
            .expect("enriched block")
            .timestamp
            .is_some()
    );
}

#[test]
fn duplicate_merge_rejects_diagonal_payload_context_timestamp_conflicts_atomically() {
    let emitter = Address::repeat_byte(0xa9);
    let payload_timestamp_log = rpc_log(emitter, keccak256(b"Diagonal()"), 25);
    let payload_timestamp = payload_timestamp_log.block_timestamp.expect("timestamp");
    let mut partial_context = included_context(25);
    partial_context.block.as_mut().expect("block").timestamp = None;
    if let ChainStatus::Included { block, .. } = &mut partial_context.chain_status {
        block.timestamp = None;
    }
    let original_context = partial_context.clone();
    let original_log = payload_timestamp_log.clone();
    let mut retained: ReactiveInputRecord<Ethereum> =
        ReactiveInputRecord::new(ReactiveInput::Log(payload_timestamp_log), partial_context);

    let mut context_timestamp_log = rpc_log(emitter, keccak256(b"Diagonal()"), 25);
    context_timestamp_log.block_timestamp = None;
    let mut conflicting_context = included_context(25);
    let conflicting_timestamp = payload_timestamp + 1;
    conflicting_context.block.as_mut().expect("block").timestamp = Some(conflicting_timestamp);
    if let ChainStatus::Included { block, .. } = &mut conflicting_context.chain_status {
        block.timestamp = Some(conflicting_timestamp);
    }
    let incoming = ReactiveInputRecord::new(
        ReactiveInput::Log(context_timestamp_log),
        conflicting_context,
    );

    let error = retained
        .merge_compatible_duplicate(&incoming)
        .expect_err("the merged payload/context candidate is contradictory");
    assert!(matches!(error, ReactiveError::InvalidInputRecord { .. }));
    assert_eq!(retained.context, original_context);
    assert!(matches!(
        &retained.input,
        ReactiveInput::Log(log) if log == &original_log
    ));
}

#[tokio::test]
async fn all_except_audience_routes_to_every_other_matching_handler() {
    let emitter = Address::repeat_byte(0xa4);
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for id in ["excluded", "included-a", "included-b"] {
        runtime
            .register_handler(Arc::new(NoopHandler::new(id, emitter)))
            .expect("register handler");
    }

    let report = runtime
        .ingest_batch(
            &mut cache,
            canonical_log_batch(emitter, 13).with_audience(DeliveryAudience::AllExcept(vec![
                HandlerId::new("excluded"),
            ])),
        )
        .expect("residual canonical delivery");

    let decoded = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Decoded(decoded) => Some(decoded.handler_ids.clone()),
            _ => None,
        })
        .expect("decoded report");
    assert_eq!(
        decoded,
        vec![HandlerId::new("included-a"), HandlerId::new("included-b")]
    );
}

#[tokio::test]
async fn owner_catchup_and_live_residual_overlap_execute_each_handler_once() {
    let emitter = Address::repeat_byte(0xa5);
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for id in ["existing", "new-owner"] {
        runtime
            .register_handler(Arc::new(NoopHandler::new(id, emitter)))
            .expect("register handler");
    }
    let record = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(emitter, keccak256(b"Event()"), 14)),
        included_context(14),
    );
    let batch = ReactiveInputBatch::from_deliveries([
        ReactiveInputDelivery::new(
            record.clone(),
            DeliveryAudience::Owners(vec![HandlerId::new("new-owner")]),
            DeliveryScope::OwnerCatchup,
        ),
        ReactiveInputDelivery::new(
            record,
            DeliveryAudience::AllExcept(vec![HandlerId::new("new-owner")]),
            DeliveryScope::Canonical,
        ),
    ]);

    let report = runtime
        .ingest_batch(&mut cache, batch)
        .expect("overlap must merge to one canonical execution");
    let decoded = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Decoded(decoded) => Some(decoded.handler_ids.clone()),
            _ => None,
        })
        .expect("decoded report");

    assert_eq!(
        decoded,
        vec![HandlerId::new("existing"), HandlerId::new("new-owner")]
    );
    assert_eq!(
        runtime.last_canonical_block().map(|block| block.number),
        Some(14)
    );
}

#[test]
fn lossless_batch_parts_preserve_per_record_delivery_contract() {
    let emitter = Address::repeat_byte(0xa5);
    let canonical: ReactiveInputRecord<Ethereum> = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(emitter, keccak256(b"Canonical()"), 20)),
        included_context(20),
    );
    let mut historical_context = included_context(10);
    historical_context.source = InputSource::Backfill;
    let historical: ReactiveInputRecord<Ethereum> = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(emitter, keccak256(b"Historical()"), 10)),
        historical_context,
    );
    let progress = BlockRef {
        number: 20,
        hash: B256::repeat_byte(20),
        parent_hash: Some(B256::repeat_byte(19)),
        timestamp: Some(1_700_000_020),
    };
    let batch = ReactiveInputBatch::from_deliveries([
        ReactiveInputDelivery::new(
            canonical,
            DeliveryAudience::AllExcept(vec![HandlerId::new("already-covered")]),
            DeliveryScope::Canonical,
        ),
        ReactiveInputDelivery::new(
            historical,
            DeliveryAudience::Owners(vec![HandlerId::new("new-owner")]),
            DeliveryScope::OwnerCatchup,
        ),
    ])
    .with_delivery_token(SubscriberDeliveryToken::new(b"delivery-20".to_vec()))
    .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"cursor-20".to_vec()))
    .with_chain_controls([ChainControl::CanonicalProgress(progress)]);

    let parts = batch.into_parts();
    assert_eq!(parts.deliveries.len(), 2);
    assert_eq!(
        parts.deliveries[0].audience(),
        &DeliveryAudience::AllExcept(vec![HandlerId::new("already-covered")])
    );
    assert_eq!(parts.deliveries[0].scope(), DeliveryScope::Canonical);
    assert_eq!(
        parts.deliveries[1].audience(),
        &DeliveryAudience::Owners(vec![HandlerId::new("new-owner")])
    );
    assert_eq!(parts.deliveries[1].scope(), DeliveryScope::OwnerCatchup);
    assert_eq!(
        parts.delivery_token.as_ref().map(|token| token.as_bytes()),
        Some(&b"delivery-20"[..])
    );
    assert_eq!(
        parts
            .subscriber_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.as_bytes()),
        Some(&b"cursor-20"[..])
    );
    assert_eq!(
        parts.chain_controls,
        vec![ChainControl::CanonicalProgress(progress)]
    );
}

#[tokio::test]
async fn raw_engine_ingest_rejects_delivery_metadata_it_cannot_commit() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    let mut cache = setup_cache().await.expect("cache");
    let tokenized = canonical_log_batch(Address::repeat_byte(0xa9), 23)
        .with_delivery_token(SubscriberDeliveryToken::new(b"must-ack".to_vec()));

    assert!(
        engine.ingest_batch(&mut cache, tokenized).is_err(),
        "a convenience helper must not silently discard acknowledgement state"
    );
    assert!(engine.runtime().last_canonical_block().is_none());
}

#[test]
fn input_ref_round_trips_for_durable_overlap_journals() {
    let record: ReactiveInputRecord<Ethereum> = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            Address::repeat_byte(0xa6),
            keccak256(b"PersistedIdentity()"),
            21,
        )),
        included_context(21),
    );
    let input_ref = record.input_ref();
    let encoded = serde_json::to_vec(&input_ref).expect("serialize stable input identity");
    let decoded: InputRef =
        serde_json::from_slice(&encoded).expect("deserialize stable input identity");
    assert_eq!(decoded, input_ref);
}

#[tokio::test]
async fn owner_catchup_does_not_rewind_global_canonical_state() {
    let emitter = Address::repeat_byte(0xa3);
    let global_slot = U256::from(1);
    let owner_slot = U256::from(2);
    let mut cache = setup_cache().await.expect("cache");
    install_mock_erc20(&mut cache, emitter);
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
        journal_depth: 8,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(SlotWriter {
            id: HandlerId::new("canonical-owner"),
            address: emitter,
            slot: global_slot,
            value: U256::from(101),
        }))
        .expect("canonical handler");

    for number in [100, 101] {
        runtime
            .ingest_batch(&mut cache, canonical_log_batch(emitter, number))
            .expect("canonical batch");
    }
    let finalized = runtime
        .last_canonical_block()
        .expect("canonical head before owner catch-up");
    runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([
                    ChainControl::Safe(finalized),
                    ChainControl::Finalized(finalized),
                ]),
        )
        .expect("finality controls");

    runtime
        .register_handler(Arc::new(SlotWriter {
            id: HandlerId::new("historical-owner"),
            address: emitter,
            slot: owner_slot,
            value: U256::from(90),
        }))
        .expect("historical handler");
    let mut historical_context = included_context(101);
    historical_context.source = InputSource::Backfill;
    let historical = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(emitter, keccak256(b"Event()"), 101)),
        historical_context,
    )])
    .with_audience(DeliveryAudience::Owners(vec![HandlerId::new(
        "historical-owner",
    )]))
    .with_delivery_scope(DeliveryScope::OwnerCatchup);
    runtime
        .ingest_batch(&mut cache, historical)
        .expect("owner catch-up");

    assert_eq!(
        cache.cached_storage_value(emitter, global_slot),
        Some(U256::from(101)),
        "historical owner replay must not roll back global canonical effects"
    );
    assert_eq!(
        cache.cached_storage_value(emitter, owner_slot),
        Some(U256::from(90)),
        "the targeted owner still receives catch-up at the retained head"
    );
    assert_eq!(runtime.last_canonical_block(), Some(finalized));
    assert_eq!(runtime.safe_head(), Some(&finalized));
    assert_eq!(runtime.finalized_head(), Some(&finalized));
}

#[tokio::test]
async fn engine_duplicate_handler_does_not_mutate_subscriber() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .await
        .expect("initial registration should succeed");

    let err = engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xb2),
        )))
        .await
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

#[tokio::test]
async fn engine_does_not_commit_runtime_when_subscriber_registration_fails() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::fail_owner(HandlerId::new("pool-a")),
    );

    let err = engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .await
        .expect_err("subscriber failure should fail engine registration");

    assert!(matches!(
        err,
        ReactiveEngineRegisterError::Subscriber(SubscriberError::InvalidConfig(_))
    ));
    assert!(!engine.runtime().contains_handler(&HandlerId::new("pool-a")));
    assert!(engine.subscriber().owners.is_empty());
}

#[tokio::test]
async fn engine_unregister_handler_updates_subscriber_then_runtime() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .register_handler(Arc::new(NoopHandler::new(
            "pool-a",
            Address::repeat_byte(0xa1),
        )))
        .await
        .expect("engine registration should succeed");

    let removed = engine
        .unregister_handler(&HandlerId::new("pool-a"))
        .await
        .expect("subscriber removal should succeed")
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

#[tokio::test]
async fn engine_register_handler_with_backfill_records_owner_backfill() {
    let emitter = Address::repeat_byte(0xa1);
    let mut cache = setup_cache().await.expect("cache");
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    engine
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 100))
        .expect("retain canonical owner replay anchor");
    let retained = included_context(100).block.expect("block context");
    let backfill = SubscriberBackfill::from_canonical_block_through(retained, retained.number)
        .expect("exact retained backfill");

    engine
        .register_handler_with_backfill(Arc::new(NoopHandler::new("pool-a", emitter)), backfill)
        .await
        .expect("engine registration with backfill should succeed");

    assert!(engine.runtime().contains_handler(&HandlerId::new("pool-a")));
    assert_eq!(
        engine.subscriber().backfills,
        vec![(HandlerId::new("pool-a"), backfill)]
    );
}

#[tokio::test]
async fn engine_rejects_deep_owner_backfill_before_subscriber_commit() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    let error = engine
        .register_handler_with_backfill(
            Arc::new(NoopHandler::new("pool-a", Address::repeat_byte(0xa1))),
            SubscriberBackfill::range(100, 120),
        )
        .await
        .expect_err("deep owner-only history would have no rollback attachment");

    assert!(matches!(
        error,
        ReactiveEngineRegisterError::BackfillOutsideJournal {
            start_block: 100,
            end_block: Some(120),
            retained_anchor: None,
        }
    ));
    assert!(!engine.runtime().contains_handler(&HandlerId::new("pool-a")));
    assert!(engine.subscriber().owners.is_empty());
    assert!(engine.subscriber().backfills.is_empty());
}

#[tokio::test]
async fn engine_sync_handler_interests_bootstraps_owner_per_handler() {
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

    let mut subscriber = RecordingSubscriber::default();
    subscriber
        .owners
        .insert(HandlerId::new("crash-stale"), Vec::new());
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    engine
        .sync_handler_interests()
        .await
        .expect("bootstrap sync should succeed");

    assert_eq!(engine.subscriber().owners.len(), 2);
    assert_eq!(
        engine.subscriber().bulk_replacements,
        1,
        "bootstrap must commit all owners through one subscriber revision"
    );
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
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("crash-stale"))
            .is_none(),
        "fresh bootstrap must exact-replace a service owner committed before a runtime crash"
    );
    // Bootstrap uses the owner-scoped path, not the full-replacement blob.
    assert!(engine.subscriber().full_replace_interests.is_empty());

    // Re-running is idempotent (upsert semantics).
    engine
        .sync_handler_interests()
        .await
        .expect("re-sync should succeed");
    assert_eq!(engine.subscriber().owners.len(), 2);
    assert_eq!(engine.subscriber().bulk_replacements, 2);
}

#[tokio::test]
async fn engine_continuity_sync_exact_replaces_stale_owners_after_canonical_head() {
    let emitter = Address::repeat_byte(0xa1);
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(NoopHandler::new("pool-a", emitter)))
        .expect("register runtime owner");
    let mut subscriber = RecordingSubscriber::default();
    subscriber.owners.insert(
        HandlerId::new("crash-stale"),
        NoopHandler::new("crash-stale", Address::repeat_byte(0xee)).interests(),
    );
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    let adopted = included_context(500).block.expect("cold-start baseline");
    cache.set_block(BlockId::from((adopted.hash, Some(true))));
    cache.set_block_context(Some(adopted.number), None);
    cache.set_timestamp(adopted.timestamp);
    engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(1, adopted))
        .expect("adopt cold-start snapshot baseline");
    let baseline = engine
        .runtime()
        .last_canonical_block()
        .expect("canonical baseline");

    engine
        .sync_handler_interests_with_backfill()
        .await
        .expect("continuity sync");

    assert_eq!(engine.subscriber().bulk_replacements, 1);
    assert_eq!(engine.subscriber().owners.len(), 1);
    assert!(
        engine
            .subscriber()
            .owners
            .contains_key(&HandlerId::new("pool-a"))
    );
    assert!(
        !engine
            .subscriber()
            .owners
            .contains_key(&HandlerId::new("crash-stale")),
        "the runtime registry is the exact restore-time owner topology"
    );
    assert_eq!(
        engine.subscriber().replacement_backfill,
        Some(SubscriberBackfill::after_canonical_block(baseline).expect("C + 1"))
    );
    assert_eq!(
        engine
            .subscriber()
            .replacement_backfill
            .expect("replacement backfill")
            .start_block(),
        501,
        "the cache already embodies block C, so catch-up must start at C + 1"
    );
}

#[tokio::test]
async fn failed_continuity_sync_preserves_the_previous_owner_topology() {
    let emitter = Address::repeat_byte(0xa2);
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(NoopHandler::new("pool-fail", emitter)))
        .expect("register runtime owner");
    let mut subscriber = RecordingSubscriber::fail_owner(HandlerId::new("pool-fail"));
    subscriber.owners.insert(
        HandlerId::new("previous"),
        NoopHandler::new("previous", Address::repeat_byte(0xef)).interests(),
    );
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    engine
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 600))
        .expect("establish canonical runtime baseline");

    engine
        .sync_handler_interests_with_backfill()
        .await
        .expect_err("forced replacement failure");

    assert_eq!(engine.subscriber().bulk_replacements, 0);
    assert_eq!(engine.subscriber().owners.len(), 1);
    assert!(
        engine
            .subscriber()
            .owners
            .contains_key(&HandlerId::new("previous")),
        "failed replacement must leave the old topology authoritative"
    );
}

#[tokio::test]
async fn cold_start_baseline_requires_matching_chain_and_exact_cache_pin() {
    let block = included_context(700).block.expect("baseline block");
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    let mut cache = setup_cache().await.expect("cache");

    let error = engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(2, block))
        .expect_err("cross-chain baseline");
    assert!(matches!(
        error,
        ReactiveEngineError::Baseline(ReactiveBaselineError::CacheChainMismatch { .. })
    ));

    let error = engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(1, block))
        .expect_err("latest selector is not an exact snapshot pin");
    assert!(matches!(
        error,
        ReactiveEngineError::Baseline(ReactiveBaselineError::CacheBlockMismatch { .. })
    ));

    cache.set_block(BlockId::from((block.hash, Some(true))));
    cache.set_block_context(Some(block.number), None);
    cache.set_timestamp(block.timestamp);
    engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(1, block))
        .expect("exact cold-start baseline");
    engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(1, block))
        .expect("exact repeat is idempotent");

    let conflicting = BlockRef {
        hash: B256::repeat_byte(0x7f),
        ..block
    };
    cache.set_block(BlockId::from((conflicting.hash, Some(true))));
    let error = engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(1, conflicting))
        .expect_err("conflicting repeat");
    assert!(matches!(
        error,
        ReactiveEngineError::Baseline(ReactiveBaselineError::ConflictingBaseline { .. })
    ));
}

#[tokio::test]
async fn cold_start_baseline_rejects_a_runtime_that_already_processed_input() {
    let emitter = Address::repeat_byte(0xa3);
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    let mut cache = setup_cache().await.expect("cache");
    engine
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 710))
        .expect("activate runtime");

    let requested = included_context(711).block.expect("requested baseline");
    cache.set_block(BlockId::from((requested.hash, Some(true))));
    cache.set_block_context(Some(requested.number), None);
    cache.set_timestamp(requested.timestamp);
    let error = engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(1, requested))
        .expect_err("active runtime cannot adopt a snapshot baseline");
    assert!(matches!(
        error,
        ReactiveEngineError::Baseline(ReactiveBaselineError::ActiveRuntime)
    ));
}

#[tokio::test]
async fn registry_and_engine_expose_same_interests_after_registration() {
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
        .await
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
        .await
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
        .await
        .expect("register late handler");

    assert_eq!(
        engine.subscriber().coordinated_catchups,
        vec![(
            HandlerId::new("pool-late"),
            included_context(500)
                .block
                .expect("canonical block context")
        )],
        "mid-lifecycle registration must request coordinated C/C+1 catch-up"
    );
}

#[tokio::test]
async fn engine_register_handler_on_fresh_runtime_is_live_only() {
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
        .await
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
async fn engine_register_handler_is_live_only_when_zero_depth_cannot_attach_owner_history() {
    let emitter = Address::repeat_byte(0xa1);
    let mut cache = setup_cache().await.expect("cache");
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
            journal_depth: 0,
            ..ReactiveConfig::default()
        }),
        RecordingSubscriber::default(),
    );
    engine
        .ingest_batch(&mut cache, canonical_log_batch(emitter, 500))
        .expect("advance canonical coverage without retaining rollback history");
    engine
        .register_handler(Arc::new(NoopHandler::new("pool-a", emitter)))
        .await
        .expect("fall back to live-only registration");

    assert!(engine.subscriber().coordinated_catchups.is_empty());
    assert!(engine.subscriber().backfills.is_empty());
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
        .await
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
        .await
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
        .await
        .expect("register resync handler");

    // Queue a batch for the subscriber to hand back, then drive the
    // resync-executing loop.
    let token = SubscriberDeliveryToken::new(vec![0x05]);
    engine
        .subscriber_mut()
        .batches
        .push_back(canonical_log_batch(emitter, 700).with_delivery_token(token.clone()));

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
    assert_eq!(engine.subscriber().acknowledged, vec![token]);
}

#[tokio::test]
async fn engine_acknowledges_delivery_after_successful_ingest() {
    let emitter = Address::repeat_byte(0xd2);
    let mut cache = setup_cache().await.expect("cache");
    install_mock_erc20(&mut cache, emitter);

    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    let token = SubscriberDeliveryToken::new(vec![0x01, 0x02, 0x03]);
    engine
        .subscriber_mut()
        .batches
        .push_back(canonical_log_batch(emitter, 701).with_delivery_token(token.clone()));

    engine
        .next_ingest(&mut cache)
        .await
        .expect("delivery should ingest and acknowledge")
        .expect("a batch was queued");

    assert_eq!(engine.subscriber().acknowledged, vec![token]);
}

#[tokio::test]
async fn engine_does_not_acknowledge_delivery_when_ingest_fails() {
    let emitter = Address::repeat_byte(0xd3);
    let slot = U256::from(7);
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for (id, value) in [("first", 1), ("second", 2)] {
        runtime
            .register_handler(Arc::new(SlotWriter {
                id: HandlerId::new(id),
                address: emitter,
                slot,
                value: U256::from(value),
            }))
            .expect("register conflicting writer");
    }
    let mut engine = ReactiveEngine::new(runtime, RecordingSubscriber::default());
    let token = SubscriberDeliveryToken::new(vec![0x04]);
    engine
        .subscriber_mut()
        .batches
        .push_back(canonical_log_batch(emitter, 702).with_delivery_token(token));

    let error = engine
        .next_ingest(&mut cache)
        .await
        .expect_err("conflicting effects must fail ingestion");

    assert!(matches!(error, ReactiveEngineError::Runtime(_)));
    assert!(engine.subscriber().acknowledged.is_empty());
}

#[tokio::test]
async fn engine_distinguishes_acknowledgement_failure_after_ingest() {
    let emitter = Address::repeat_byte(0xd4);
    let mut cache = setup_cache().await.expect("cache");
    let subscriber = RecordingSubscriber {
        fail_acknowledgement: true,
        ..Default::default()
    };
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );
    let failed_token = SubscriberDeliveryToken::new(vec![0x06]);
    let later_token = SubscriberDeliveryToken::new(vec![0x07]);
    engine.subscriber_mut().batches.extend([
        canonical_log_batch(emitter, 703).with_delivery_token(failed_token.clone()),
        canonical_log_batch(emitter, 704).with_delivery_token(later_token.clone()),
    ]);

    let error = engine
        .next_ingest(&mut cache)
        .await
        .expect_err("acknowledgement failure should surface after ingestion");

    assert!(matches!(error, ReactiveEngineError::Acknowledgement(_)));
    assert_eq!(
        engine
            .runtime()
            .last_canonical_block()
            .expect("batch was ingested before acknowledgement")
            .number,
        703
    );
    assert!(engine.subscriber().acknowledged.is_empty());
    assert_eq!(engine.subscriber().batches.len(), 1);

    engine.subscriber_mut().fail_acknowledgement = false;
    engine
        .next_ingest(&mut cache)
        .await
        .expect("pending acknowledgement retries")
        .expect("the already-ingested report is returned after acknowledgement");
    assert_eq!(engine.subscriber().acknowledged, vec![failed_token]);
    assert_eq!(
        engine.subscriber().batches.len(),
        1,
        "the retry must not poll a later delivery"
    );
    assert_eq!(
        engine
            .runtime()
            .last_canonical_block()
            .expect("later delivery has not been polled")
            .number,
        703
    );

    engine
        .next_ingest(&mut cache)
        .await
        .expect("later delivery")
        .expect("later batch remains queued");
    assert_eq!(
        engine.subscriber().acknowledged,
        vec![SubscriberDeliveryToken::new(vec![0x06]), later_token]
    );
}

#[tokio::test]
async fn cancelled_acknowledgement_is_retried_before_polling_a_later_batch() {
    let emitter = Address::repeat_byte(0xd5);
    let mut cache = setup_cache().await.expect("cache");
    let subscriber = RecordingSubscriber {
        hold_acknowledgement: true,
        ..Default::default()
    };
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );
    let pending_token = SubscriberDeliveryToken::new(vec![0x08]);
    let later_token = SubscriberDeliveryToken::new(vec![0x09]);
    engine.subscriber_mut().batches.extend([
        canonical_log_batch(emitter, 705).with_delivery_token(pending_token.clone()),
        canonical_log_batch(emitter, 706).with_delivery_token(later_token),
    ]);

    let mut interrupted = Box::pin(engine.next_ingest(&mut cache));
    assert!(
        futures::poll!(&mut interrupted).is_pending(),
        "the subscriber holds the acknowledgement open"
    );
    drop(interrupted);
    assert_eq!(engine.subscriber().batches.len(), 1);
    assert_eq!(
        engine
            .runtime()
            .last_canonical_block()
            .expect("delivery was applied before the cancelled acknowledgement")
            .number,
        705
    );

    engine.subscriber_mut().hold_acknowledgement = false;
    engine
        .next_ingest(&mut cache)
        .await
        .expect("cancelled acknowledgement retries")
        .expect("stored report returns");
    assert_eq!(engine.subscriber().acknowledged, vec![pending_token]);
    assert_eq!(engine.subscriber().batches.len(), 1);
    assert_eq!(engine.runtime().last_canonical_block().unwrap().number, 705);
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
        .await
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
    let removed = engine
        .unregister_handler(&HandlerId::new("pool"))
        .await
        .expect("subscriber removal should succeed");
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
