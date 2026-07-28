//! Crash-ordering and compatibility tests for durable reactive checkpoints.
#![cfg(feature = "reactive")]

mod common;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use alloy_eips::BlockId;
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Header, Log};
use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::events::StateView;
use evm_fork_cache::freshness::FreshnessRegistry;
use evm_fork_cache::reactive::{
    BlockRef, CacheHealth, CacheMetricsSnapshot, ChainControl, ChainStatus, EventSubscriber,
    HandlerError, HandlerId, HandlerOutcome, InputSource, LogInterest, ReactiveConfig,
    ReactiveContext, ReactiveEffect, ReactiveEngine, ReactiveEngineError, ReactiveError,
    ReactiveHandler, ReactiveHook, ReactiveInput, ReactiveInputBatch, ReactiveInputRecord,
    ReactiveInterest, ReactiveReport, ResyncRequest, RootGateCadence, RouteKeySpec,
    StateEffectQuality, SubscriberCapabilities, SubscriberCapability, SubscriberCheckpoint,
    SubscriberDeliveryToken, SubscriberNextBatch, SubscriberOperation, SubscriberPayloadCommitment,
    SubscriberResumePosition, TrackingPolicy,
};
use evm_fork_cache::state_update::{SlotDelta, StateDiff, StateUpdate};
use evm_fork_cache::{
    AccountProof, CheckpointedIngest, DurableCheckpointBlock, DurableCheckpointError,
    DurableCheckpointIdentity, DurableCheckpointMetadata, DurableCheckpointStore, ReactiveRuntime,
};

fn temp_path(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "evm_fork_cache_durable_{tag}_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create temp root");
    root.join("checkpoint.bin")
}

fn identity() -> DurableCheckpointIdentity {
    DurableCheckpointIdentity::new(1, "subscriber-a", "handlers-v1")
}

fn metadata(block: u64, token: &[u8]) -> DurableCheckpointMetadata {
    DurableCheckpointMetadata::new(
        identity(),
        DurableCheckpointBlock::new(block, B256::repeat_byte(block as u8))
            .with_parent_hash(B256::repeat_byte(block.saturating_sub(1) as u8))
            .with_timestamp(1_700_000_000 + block),
    )
    .with_delivery_token(token)
    .with_subscriber_checkpoint(b"provider-cursor".to_vec())
}

fn install_fixed_root(cache: &mut evm_fork_cache::EvmCache, root: B256) {
    cache.set_account_proof_fetcher(Arc::new(move |requests, _block| {
        requests
            .into_iter()
            .map(|(address, _slots)| {
                (
                    address,
                    Ok(AccountProof {
                        storage_hash: root,
                        balance: U256::ZERO,
                        nonce: 0,
                        code_hash: B256::ZERO,
                        slots: Vec::new(),
                    }),
                )
            })
            .collect()
    }));
}

#[derive(serde::Serialize)]
struct EncodedRuntimeCheckpoint {
    version: u32,
    safe_head: Option<BlockRef>,
    finalized_head: Option<BlockRef>,
    health: CacheHealth,
    pending_resyncs: Vec<ResyncRequest>,
    coverage_head: Option<BlockRef>,
    journal: Vec<EncodedBlockJournal>,
    freshness: Option<FreshnessRegistry>,
    tracking: HashMap<Address, TrackingPolicy>,
    tracked_roots: HashMap<Address, EncodedTrackedRoot>,
    root_gate_cadence: RootGateCadence,
    last_gate_block: Option<u64>,
    touched_since_gate: HashSet<Address>,
    metrics: CacheMetricsSnapshot,
}

#[derive(serde::Serialize)]
struct EncodedBlockJournal {
    block: BlockRef,
    handler_ids: Vec<HandlerId>,
    rollback_diffs: Vec<StateDiff>,
}

#[derive(serde::Serialize)]
struct EncodedTrackedRoot {
    last_root: B256,
    last_block: u64,
    balance: U256,
    nonce: u64,
    code_hash: B256,
}

fn runtime_metadata_with_journal(
    coverage: BlockRef,
    journal: impl IntoIterator<Item = BlockRef>,
) -> DurableCheckpointMetadata {
    let checkpoint = EncodedRuntimeCheckpoint {
        version: 3,
        safe_head: None,
        finalized_head: None,
        health: CacheHealth::Healthy,
        pending_resyncs: Vec::new(),
        coverage_head: Some(coverage),
        journal: journal
            .into_iter()
            .map(|block| EncodedBlockJournal {
                block,
                handler_ids: Vec::new(),
                rollback_diffs: Vec::new(),
            })
            .collect(),
        freshness: None,
        tracking: HashMap::new(),
        tracked_roots: HashMap::new(),
        root_gate_cadence: RootGateCadence::every_n_blocks(16),
        last_gate_block: None,
        touched_since_gate: HashSet::new(),
        metrics: CacheMetricsSnapshot::default(),
    };
    let mut block = DurableCheckpointBlock::new(coverage.number, coverage.hash);
    if let Some(parent_hash) = coverage.parent_hash {
        block = block.with_parent_hash(parent_hash);
    }
    if let Some(timestamp) = coverage.timestamp {
        block = block.with_timestamp(timestamp);
    }
    DurableCheckpointMetadata::new(identity(), block)
        .with_runtime_checkpoint(bincode::serialize(&checkpoint).expect("encode runtime state"))
}

fn assert_preview_and_resume_reject_without_mutation(invalid_metadata: &DurableCheckpointMetadata) {
    let subscriber = ResumeRecordingSubscriber::default();
    let restore_calls = Arc::clone(&subscriber.restored);
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );

    let error = engine
        .preview_durable_resume_position(invalid_metadata)
        .expect_err("invalid runtime state must fail preview");
    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::InvalidRuntimeCheckpoint(_)
    ));
    assert!(engine.runtime().last_canonical_block().is_none());
    assert!(restore_calls.lock().expect("restore recorder").is_none());
    engine
        .preview_durable_resume_position(&metadata(90, b"valid-after-failed-preview"))
        .expect("failed semantic preview leaves the engine reusable");

    let error = engine
        .resume_from_durable_checkpoint(invalid_metadata)
        .expect_err("invalid runtime state must fail restore");
    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::InvalidRuntimeCheckpoint(_)
    ));
    assert!(engine.runtime().last_canonical_block().is_none());
    assert!(restore_calls.lock().expect("restore recorder").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_round_trip_restores_state_and_guards_identity() {
    let path = temp_path("round_trip");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xa1);
    let slot = U256::from(7);
    let mut cache = setup_cache().await.expect("cache");
    let _ = cache.apply_update(&StateUpdate::slot(address, slot, U256::from(11)));
    store
        .save_async(&cache, metadata(42, b"delivery-42"))
        .await
        .expect("save checkpoint");

    let _ = cache.apply_update(&StateUpdate::slot(address, slot, U256::from(99)));
    let loaded = store.load().expect("load checkpoint").expect("checkpoint");
    assert_eq!(loaded.metadata().block.number, 42);
    assert_eq!(
        loaded.metadata().delivery_token.as_deref(),
        Some(&b"delivery-42"[..])
    );
    assert_eq!(
        loaded.metadata().subscriber_checkpoint.as_deref(),
        Some(&b"provider-cursor"[..])
    );

    let wrong = DurableCheckpointIdentity::new(1, "subscriber-a", "handlers-v2");
    let error = store
        .load()
        .expect("reload checkpoint")
        .expect("checkpoint")
        .restore_into(&mut cache, &wrong)
        .expect_err("handler schema mismatch must fail closed");
    assert!(matches!(
        error,
        DurableCheckpointError::IdentityMismatch { .. }
    ));
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(99))
    );

    let restored = loaded
        .restore_into(&mut cache, &identity())
        .expect("restore checkpoint");
    assert_eq!(restored.block.number, 42);
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(11))
    );
    assert_eq!(
        cache.block(),
        BlockId::from((B256::repeat_byte(42), Some(true)))
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn durable_checkpoint_file_is_owner_readable_and_writable_only() {
    use std::os::unix::fs::PermissionsExt;

    let path = temp_path("private_permissions");
    let store = DurableCheckpointStore::new(&path);
    let cache = setup_cache().await.expect("cache");
    store
        .save_async(&cache, metadata(42, b"secret-delivery-token"))
        .await
        .expect("save private checkpoint");

    assert_eq!(
        fs::metadata(path)
            .expect("checkpoint metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_metadata_normalizes_a_stale_cache_execution_context() {
    let path = temp_path("metadata_context_alignment");
    let store = DurableCheckpointStore::new(&path);
    let mut cache = setup_cache().await.expect("cache");
    cache.set_block(BlockId::number(7));
    cache.set_block_context(Some(7), Some(77));
    cache.set_timestamp(Some(1_600_000_007));
    cache.set_coinbase(Some(Address::repeat_byte(0x77)));
    cache.set_prevrandao(Some(B256::repeat_byte(0x78)));
    cache.set_block_gas_limit(Some(7_000_000));

    store
        .save(&cache, metadata(42, b"delivery-42"))
        .expect("checkpoint");
    let loaded = store.load().expect("load").expect("checkpoint");
    let mut restored = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored, &identity())
        .expect("restore");

    assert_eq!(
        restored.block(),
        BlockId::from((B256::repeat_byte(42), Some(true)))
    );
    assert_eq!(restored.block_number(), Some(42));
    assert_eq!(restored.timestamp(), Some(1_700_000_042));
    assert_eq!(restored.basefee(), None);
    assert_eq!(restored.coinbase(), None);
    assert_eq!(restored.prevrandao(), None);
    assert_eq!(restored.block_gas_limit(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_verified_header_environment_survives_checkpoint_round_trip() {
    let path = temp_path("verified_header_environment");
    let store = DurableCheckpointStore::new(&path);
    let mut cache = setup_cache().await.expect("cache");
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(header_batch(30_000_070, b"unused", None).into_records()),
        )
        .expect("ingest verified header");

    store
        .save(&cache, metadata(70, b"delivery-70"))
        .expect("save exact-header checkpoint");
    let loaded = store.load().expect("load").expect("checkpoint");
    let mut restored = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored, &identity())
        .expect("restore");

    assert_eq!(
        restored.block(),
        BlockId::from((B256::repeat_byte(70), Some(true)))
    );
    assert_eq!(restored.block_number(), Some(70));
    assert_eq!(restored.timestamp(), Some(1_700_000_070));
    assert_eq!(restored.basefee(), Some(70));
    assert_eq!(restored.coinbase(), Some(Address::repeat_byte(0x70)));
    assert_eq!(restored.prevrandao(), Some(B256::repeat_byte(0x71)));
    assert_eq!(restored.block_gas_limit(), Some(30_000_070));
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_save_leaves_previous_checkpoint_authoritative() {
    let path = temp_path("preserve_previous");
    let store = DurableCheckpointStore::new(&path);
    let cache = setup_cache().await.expect("cache");
    store
        .save(&cache, metadata(10, b"ten"))
        .expect("initial checkpoint");

    let invalid_identity = DurableCheckpointIdentity::new(2, "subscriber-a", "handlers-v1");
    let error = store
        .save(
            &cache,
            DurableCheckpointMetadata::new(
                invalid_identity,
                DurableCheckpointBlock::new(11, B256::repeat_byte(11))
                    .with_parent_hash(B256::repeat_byte(10)),
            ),
        )
        .expect_err("cross-chain checkpoint must be rejected");
    assert!(matches!(
        error,
        DurableCheckpointError::CacheChainMismatch { .. }
    ));
    assert_eq!(
        store
            .load()
            .expect("load prior")
            .expect("prior checkpoint")
            .metadata()
            .block
            .number,
        10
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupted_checkpoint_payload_is_rejected_before_decode() {
    let path = temp_path("checksum_corruption");
    let store = DurableCheckpointStore::new(&path);
    let cache = setup_cache().await.expect("cache");
    store
        .save(&cache, metadata(12, b"twelve"))
        .expect("checkpoint");

    let mut bytes = fs::read(&path).expect("read checkpoint bytes");
    let payload_offset = 8 + std::mem::size_of::<u32>();
    bytes[payload_offset] ^= 0x80;
    fs::write(&path, bytes).expect("corrupt checkpoint payload");

    let error = match store.load() {
        Err(error) => error,
        Ok(_) => panic!("checksum must reject corruption"),
    };
    assert!(matches!(
        error,
        DurableCheckpointError::ChecksumMismatch { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn load_rejects_checkpoint_over_the_configured_size_bound() {
    let path = temp_path("bounded_load");
    let store = DurableCheckpointStore::new(&path);
    let cache = setup_cache().await.expect("cache");
    store
        .save(&cache, metadata(13, b"thirteen"))
        .expect("checkpoint");
    let file_bytes = fs::metadata(&path).expect("checkpoint metadata").len();
    let bounded =
        DurableCheckpointStore::new(&path).with_max_checkpoint_bytes(file_bytes.saturating_sub(1));

    let error = match bounded.load() {
        Err(error) => error,
        Ok(_) => panic!("oversized file must be rejected"),
    };
    assert!(matches!(
        error,
        DurableCheckpointError::CheckpointTooLarge { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn save_preflights_encoded_size_before_creating_a_checkpoint_file() {
    let path = temp_path("bounded_save");
    let store = DurableCheckpointStore::new(&path).with_max_checkpoint_bytes(1);
    let cache = setup_cache().await.expect("cache");

    let error = store
        .save(&cache, metadata(14, b"fourteen"))
        .expect_err("encoded checkpoint must exceed one byte");
    assert!(matches!(
        error,
        DurableCheckpointError::CheckpointTooLarge {
            bytes,
            max_bytes: 1,
            ..
        } if bytes > 1
    ));
    assert!(
        !path.exists(),
        "size rejection must occur before a temporary or destination file is installed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_stale_writer_cannot_replace_a_newer_checkpoint() {
    let path = temp_path("cancelled_writer_generation");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xaf);
    let mut large = setup_cache().await.expect("large cache");
    install_mock_erc20(&mut large, address);
    for slot in 0..25_000_u64 {
        large
            .db_mut()
            .insert_account_storage(address, U256::from(slot), U256::from(slot + 1))
            .expect("seed large checkpoint");
    }
    let small = setup_cache().await.expect("small cache");

    let mut stale = Box::pin(store.save_async(&large, metadata(10, b"stale-ten")));
    assert!(
        futures::poll!(&mut stale).is_pending(),
        "large blocking write should have been dispatched"
    );
    drop(stale);

    store
        .save_async(&small, metadata(11, b"authoritative-eleven"))
        .await
        .expect("newer checkpoint");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let loaded = store.load().expect("load").expect("checkpoint");
    assert_eq!(loaded.metadata().block.number, 11);
    assert_eq!(
        loaded.metadata().delivery_token.as_deref(),
        Some(&b"authoritative-eleven"[..])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn independently_constructed_stores_share_path_writer_ordering() {
    let path = temp_path("independent_store_generation");
    let stale_store = DurableCheckpointStore::new(&path);
    let newer_store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xae);
    let mut large = setup_cache().await.expect("large cache");
    install_mock_erc20(&mut large, address);
    for slot in 0..20_000_u64 {
        large
            .db_mut()
            .insert_account_storage(address, U256::from(slot), U256::from(slot + 1))
            .expect("seed large checkpoint");
    }
    let small = setup_cache().await.expect("small cache");

    let mut stale = Box::pin(stale_store.save_async(&large, metadata(20, b"stale-twenty")));
    assert!(futures::poll!(&mut stale).is_pending());
    drop(stale);
    newer_store
        .save_async(&small, metadata(21, b"newer-twenty-one"))
        .await
        .expect("newer checkpoint");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let loaded = newer_store.load().expect("load").expect("checkpoint");
    assert_eq!(loaded.metadata().block.number, 21);
}

#[test]
fn lexically_equivalent_nonexistent_parent_paths_share_one_store_identity() {
    let path = temp_path("lexical_store_identity");
    let parent = path.parent().expect("checkpoint parent");
    let file_name = path.file_name().expect("checkpoint file name");
    let aliased = parent
        .join("parent-that-does-not-need-to-exist")
        .join("..")
        .join(file_name);

    assert_eq!(
        DurableCheckpointStore::new(&path),
        DurableCheckpointStore::new(aliased),
        "lexically equivalent checkpoint paths must share writer ordering"
    );
}

#[test]
fn store_identity_is_stable_when_a_missing_parent_is_created() {
    let path = temp_path("missing_parent_store_identity")
        .parent()
        .expect("temp root")
        .join("later")
        .join("nested")
        .join("checkpoint.bin");
    let before = DurableCheckpointStore::new(&path);
    fs::create_dir_all(path.parent().expect("checkpoint parent")).expect("create parent suffix");
    let after = DurableCheckpointStore::new(&path);

    assert_eq!(before, after);
    assert_eq!(before.path(), after.path());
}

#[cfg(unix)]
#[test]
fn store_identity_preserves_symlink_parent_semantics() {
    use std::os::unix::fs::symlink;

    let root = temp_path("symlink_parent_store_identity")
        .parent()
        .expect("temp root")
        .to_path_buf();
    let target = root.join("physical").join("nested");
    fs::create_dir_all(&target).expect("create symlink target");
    let link = root.join("link");
    symlink(&target, &link).expect("create directory symlink");

    let through_symlink = link.join("..").join("checkpoint.bin");
    let physical = target
        .parent()
        .expect("physical parent")
        .join("checkpoint.bin");
    let lexically_popped = root.join("checkpoint.bin");

    assert_eq!(
        DurableCheckpointStore::new(through_symlink),
        DurableCheckpointStore::new(physical),
        "OS symlink/.. resolution must identify the physical target"
    );
    assert_ne!(
        DurableCheckpointStore::new(root.join("link").join("..").join("checkpoint.bin")),
        DurableCheckpointStore::new(lexically_popped),
        "normalization must not lexically erase a symlink before resolving it"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_save_replaces_destination_symlink_without_following_it() {
    use std::os::unix::fs::symlink;

    let link_path = temp_path("destination_symlink");
    let target_path = link_path
        .parent()
        .expect("checkpoint parent")
        .join("outside-target.bin");
    fs::write(&target_path, b"do not replace through link").expect("write symlink target");
    symlink(&target_path, &link_path).expect("create destination symlink");
    let store = DurableCheckpointStore::new(&link_path);
    let cache = setup_cache().await.expect("cache");

    store
        .save(&cache, metadata(22, b"replace-link-entry"))
        .expect("save checkpoint over symlink entry");

    assert!(
        !fs::symlink_metadata(&link_path)
            .expect("destination metadata")
            .file_type()
            .is_symlink(),
        "atomic replacement must replace the symlink entry itself"
    );
    assert_eq!(
        fs::read(&target_path).expect("original target"),
        b"do not replace through link"
    );
    assert_eq!(
        store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .number,
        22
    );
}

struct CountingWriter {
    calls: Arc<AtomicUsize>,
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for CountingWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("writer")
    }

    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                log.address(),
                self.slot,
                U256::from(123),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: Vec::new(),
        })
    }
}

struct OrderingSubscriber {
    batches: VecDeque<ReactiveInputBatch<Ethereum>>,
    polls: Arc<AtomicUsize>,
    acknowledgements: Arc<AtomicUsize>,
    checkpoint_path: PathBuf,
    ack_saw_checkpoint: Arc<AtomicBool>,
}

struct ToggleAckSubscriber {
    batch: Option<ReactiveInputBatch<Ethereum>>,
    fail_acknowledgement: bool,
    acknowledgements: Arc<AtomicUsize>,
}

struct CountingHook(Arc<AtomicUsize>);

fn durable_capabilities() -> SubscriberCapabilities {
    SubscriberCapabilities::new([SubscriberCapability::DurableReplay])
}

impl ReactiveHook<Ethereum> for CountingHook {
    fn on_report(&self, _report: Arc<ReactiveReport<Ethereum>>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl EventSubscriber<Ethereum> for ToggleAckSubscriber {
    fn capabilities(&self) -> SubscriberCapabilities {
        durable_capabilities()
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move { Ok(self.batch.take()) })
    }

    fn acknowledge_delivery(
        &mut self,
        _token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if self.fail_acknowledgement {
                return Err(evm_fork_cache::reactive::SubscriberError::InvalidConfig(
                    "forced crash-window acknowledgement failure",
                ));
            }
            self.acknowledgements.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[derive(Default)]
struct ResumeRecordingSubscriber {
    restored: Arc<Mutex<Option<SubscriberResumePosition>>>,
    chain_id: Option<u64>,
}

impl EventSubscriber<Ethereum> for ResumeRecordingSubscriber {
    fn chain_id(&self) -> Option<u64> {
        self.chain_id
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        durable_capabilities()
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async { Ok(None) })
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), evm_fork_cache::reactive::SubscriberError> {
        *self.restored.lock().expect("restore recorder lock") = Some(position.clone());
        Ok(())
    }
}

struct EphemeralResumeSubscriber {
    restore_calls: Arc<AtomicUsize>,
}

impl EventSubscriber<Ethereum> for EphemeralResumeSubscriber {
    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async { Ok(None) })
    }

    fn restore_position(
        &mut self,
        _position: &SubscriberResumePosition,
    ) -> Result<(), evm_fork_cache::reactive::SubscriberError> {
        self.restore_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct RejectingRestoreSubscriber;

impl EventSubscriber<Ethereum> for RejectingRestoreSubscriber {
    fn capabilities(&self) -> SubscriberCapabilities {
        durable_capabilities()
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async { Ok(None) })
    }

    fn restore_position(
        &mut self,
        _position: &SubscriberResumePosition,
    ) -> Result<(), evm_fork_cache::reactive::SubscriberError> {
        Err(evm_fork_cache::reactive::SubscriberError::InvalidConfig(
            "reject restore for atomicity test",
        ))
    }
}

impl EventSubscriber<Ethereum> for OrderingSubscriber {
    fn capabilities(&self) -> SubscriberCapabilities {
        durable_capabilities()
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }

    fn acknowledge_delivery(
        &mut self,
        _token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.ack_saw_checkpoint
                .store(self.checkpoint_path.is_file(), Ordering::SeqCst);
            self.acknowledgements.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn batch(address: Address, block_number: u64, token: &[u8]) -> ReactiveInputBatch<Ethereum> {
    let block = BlockRef {
        number: block_number,
        hash: B256::repeat_byte(block_number as u8),
        parent_hash: Some(B256::repeat_byte(block_number.saturating_sub(1) as u8)),
        timestamp: Some(1_700_000_000 + block_number),
    };
    let log = Log {
        inner: PrimitiveLog::new_unchecked(address, vec![keccak256(b"Event()")], Bytes::new()),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(0xcc)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    };
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::Log(log),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Subscription,
            chain_status: ChainStatus::Included {
                block,
                confirmations: 0,
            },
            block: Some(block),
            transaction_index: Some(0),
            log_index: Some(0),
        },
    )])
    .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

fn batch_for_block(
    address: Address,
    block: BlockRef,
    token: &[u8],
    removed: bool,
) -> ReactiveInputBatch<Ethereum> {
    let log = Log {
        inner: PrimitiveLog::new_unchecked(address, vec![keccak256(b"Event()")], Bytes::new()),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(0xdd)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed,
    };
    let chain_status = if removed {
        ChainStatus::Reorged {
            dropped_from: block,
        }
    } else {
        ChainStatus::Included {
            block,
            confirmations: 0,
        }
    };
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::Log(log),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Subscription,
            chain_status,
            block: Some(block),
            transaction_index: Some(0),
            log_index: Some(0),
        },
    )])
    .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

fn removed_record_for_block(
    address: Address,
    block: BlockRef,
    log_index: u64,
) -> ReactiveInputRecord<Ethereum> {
    let log = Log {
        inner: PrimitiveLog::new_unchecked(
            address,
            vec![keccak256(b"RemovedEvent()")],
            Bytes::new(),
        ),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(0xee)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: true,
    };
    ReactiveInputRecord::new(
        ReactiveInput::Log(log),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Subscription,
            chain_status: ChainStatus::Reorged {
                dropped_from: block,
            },
            block: Some(block),
            transaction_index: Some(0),
            log_index: Some(log_index),
        },
    )
}

fn header_batch(
    gas_limit: u64,
    token: &[u8],
    commitment: Option<SubscriberPayloadCommitment>,
) -> ReactiveInputBatch<Ethereum> {
    let block = BlockRef {
        number: 70,
        hash: B256::repeat_byte(70),
        parent_hash: Some(B256::repeat_byte(69)),
        timestamp: Some(1_700_000_070),
    };
    let header = Header {
        hash: block.hash,
        inner: alloy_consensus::Header {
            number: block.number,
            parent_hash: block.parent_hash.expect("parent"),
            timestamp: block.timestamp.expect("timestamp"),
            base_fee_per_gas: Some(70),
            beneficiary: Address::repeat_byte(0x70),
            mix_hash: B256::repeat_byte(0x71),
            gas_limit,
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    };
    let batch = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::BlockHeader(header),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Subscription,
            chain_status: ChainStatus::Included {
                block,
                confirmations: 0,
            },
            block: Some(block),
            transaction_index: None,
            log_index: None,
        },
    )])
    .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()));
    match commitment {
        Some(commitment) => batch.with_payload_commitment(commitment),
        None => batch,
    }
}

fn direct_batch(address: Address, block_number: u64) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(batch(address, block_number, b"unused").into_records())
}

fn engine(
    subscriber: OrderingSubscriber,
    calls: Arc<AtomicUsize>,
    address: Address,
    slot: U256,
) -> ReactiveEngine<OrderingSubscriber, Ethereum> {
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls,
            address,
            slot,
        }))
        .expect("register writer");
    ReactiveEngine::new(runtime, subscriber)
}

async fn persisted_two_block_runtime_metadata(tag: &str) -> DurableCheckpointMetadata {
    let path = temp_path(tag);
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xbd);
    let slot = U256::from(13);
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 120, b"delivery-120"),
            batch(address, 121, b"delivery-121").with_subscriber_checkpoint(
                SubscriberCheckpoint::new(b"provider-after-121".to_vec()),
            ),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut engine = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..2 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("checkpoint")
            .expect("batch");
    }
    store
        .load()
        .expect("load")
        .expect("checkpoint")
        .metadata()
        .clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_resume_restores_subscriber_position_and_canonical_history() {
    let path = temp_path("subscriber_resume_position");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xbe);
    let slot = U256::from(14);
    let first_subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 120, b"delivery-120"),
            batch(address, 121, b"delivery-121").with_subscriber_checkpoint(
                SubscriberCheckpoint::new(b"provider-after-121".to_vec()),
            ),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut first = engine(
        first_subscriber,
        Arc::new(AtomicUsize::new(0)),
        address,
        slot,
    );
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..2 {
        first
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("checkpoint")
            .expect("batch");
    }

    let loaded = store.load().expect("load").expect("checkpoint");
    let metadata = loaded.metadata().clone();
    let mut restored_cache = setup_cache().await.expect("restored cache");
    let recorder = ResumeRecordingSubscriber::default();
    let restored = recorder.restored.clone();
    let mut resumed =
        ReactiveEngine::new(ReactiveRuntime::new(ReactiveConfig::default()), recorder);
    let preview = resumed
        .preview_durable_resume_position(&metadata)
        .expect("preview the exact subscriber restore position");
    let restored_metadata = resumed
        .restore_durable_checkpoint(&mut restored_cache, loaded, &identity())
        .expect("atomically restore cache, engine, and subscriber position");
    assert_eq!(restored_metadata, metadata);

    let position = restored
        .lock()
        .expect("restore recorder lock")
        .clone()
        .expect("subscriber restore hook");
    assert_eq!(
        preview, position,
        "the public preview and synchronous restore hook must share one exact position"
    );
    assert_eq!(position.coverage_head.number, 121);
    assert_eq!(position.chain_id, 1);
    assert_eq!(
        position
            .canonical_history
            .iter()
            .map(|block| block.number)
            .collect::<Vec<_>>(),
        vec![120, 121]
    );
    assert_eq!(
        position
            .delivery_token
            .as_ref()
            .map(|token| token.as_bytes()),
        Some(&b"delivery-121"[..])
    );
    assert_eq!(
        position
            .subscriber_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.as_bytes()),
        Some(&b"provider-after-121"[..])
    );
    resumed
        .ingest_batch(&mut restored_cache, direct_batch(address, 122))
        .expect("continue from restored coverage");
    assert_eq!(resumed.runtime().metrics().missed_ranges, 0);
    assert_eq!(
        resumed
            .runtime()
            .last_canonical_block()
            .expect("coverage advances")
            .number,
        122
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_and_restore_apply_the_same_configured_journal_retention() {
    let metadata = persisted_two_block_runtime_metadata("preview_journal_retention").await;

    for (journal_depth, expected_history) in [(1, vec![121]), (0, Vec::new())] {
        let config = ReactiveConfig {
            journal_depth,
            ..ReactiveConfig::default()
        };
        let recorder = ResumeRecordingSubscriber::default();
        let restored = Arc::clone(&recorder.restored);
        let mut engine = ReactiveEngine::new(ReactiveRuntime::new(config), recorder);

        let preview = engine
            .preview_durable_resume_position(&metadata)
            .expect("preview retained history");
        assert_eq!(
            preview
                .canonical_history
                .iter()
                .map(|block| block.number)
                .collect::<Vec<_>>(),
            expected_history
        );

        engine
            .resume_from_durable_checkpoint(&metadata)
            .expect("restore retained history");
        assert_eq!(
            restored.lock().expect("restore recorder lock").as_ref(),
            Some(&preview)
        );
    }
}

#[test]
fn preview_and_restore_retain_the_exact_final_sixty_four_of_sixty_five_entries() {
    let history = (100..=164)
        .map(|number| BlockRef {
            number,
            hash: B256::repeat_byte(number as u8),
            parent_hash: Some(B256::repeat_byte(number.saturating_sub(1) as u8)),
            timestamp: Some(1_700_000_000 + number),
        })
        .collect::<Vec<_>>();
    let metadata =
        runtime_metadata_with_journal(*history.last().expect("non-empty history"), history.clone());
    let subscriber = ResumeRecordingSubscriber::default();
    let restored = Arc::clone(&subscriber.restored);
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );

    let preview = engine
        .preview_durable_resume_position(&metadata)
        .expect("preview default retained history");
    assert_eq!(preview.canonical_history, history[1..]);
    assert_eq!(preview.canonical_history.len(), 64);

    engine
        .resume_from_durable_checkpoint(&metadata)
        .expect("restore default retained history");
    assert_eq!(
        restored.lock().expect("restore recorder").as_ref(),
        Some(&preview)
    );
}

#[test]
fn preview_and_restore_share_the_legacy_metadata_only_fallback() {
    let metadata = metadata(90, b"legacy-delivery");

    for (journal_depth, expected_history) in [(64, vec![90]), (0, Vec::new())] {
        let config = ReactiveConfig {
            journal_depth,
            ..ReactiveConfig::default()
        };
        let recorder = ResumeRecordingSubscriber::default();
        let restored = Arc::clone(&recorder.restored);
        let mut engine = ReactiveEngine::new(ReactiveRuntime::new(config), recorder);

        let preview = engine
            .preview_durable_resume_position(&metadata)
            .expect("preview metadata-only fallback");
        assert_eq!(
            preview
                .canonical_history
                .iter()
                .map(|block| block.number)
                .collect::<Vec<_>>(),
            expected_history
        );

        engine
            .resume_from_durable_checkpoint(&metadata)
            .expect("restore metadata-only fallback");
        assert_eq!(
            restored.lock().expect("restore recorder lock").as_ref(),
            Some(&preview)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_runtime_bytes_fail_preview_without_mutating_restore_state() {
    let valid = persisted_two_block_runtime_metadata("preview_invalid_runtime_bytes").await;
    let mut malformed = valid.clone();
    malformed.runtime_checkpoint = Some(b"not-a-runtime-checkpoint".to_vec());
    let mut trailing = valid.clone();
    trailing
        .runtime_checkpoint
        .as_mut()
        .expect("real engine checkpoint")
        .push(0xff);

    for candidate in [malformed, trailing] {
        let subscriber = ResumeRecordingSubscriber::default();
        let restore_calls = Arc::clone(&subscriber.restored);
        let engine = ReactiveEngine::new(
            ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
            subscriber,
        );

        let error = engine
            .preview_durable_resume_position(&candidate)
            .expect_err("invalid runtime bytes must fail preview");
        assert!(matches!(
            error,
            evm_fork_cache::reactive::ReactiveCheckpointRestoreError::InvalidRuntimeCheckpoint(_)
        ));
        assert!(engine.runtime().last_canonical_block().is_none());
        assert!(restore_calls.lock().expect("restore recorder").is_none());

        engine
            .preview_durable_resume_position(&valid)
            .expect("failed preview leaves the engine reusable");
    }
}

#[test]
fn preview_rejects_a_mismatched_subscriber_chain_without_restore_mutation() {
    let subscriber = ResumeRecordingSubscriber {
        chain_id: Some(2),
        ..ResumeRecordingSubscriber::default()
    };
    let restore_calls = Arc::clone(&subscriber.restored);
    let engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );

    let error = engine
        .preview_durable_resume_position(&metadata(90, b"wrong-chain"))
        .expect_err("subscriber and checkpoint chains must match");
    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::SubscriberChainMismatch {
            subscriber_chain_id: 2,
            checkpoint_chain_id: 1,
        }
    ));
    assert!(engine.runtime().last_canonical_block().is_none());
    assert!(restore_calls.lock().expect("restore recorder").is_none());
}

#[test]
fn preview_rejects_an_ephemeral_subscriber_without_invoking_restore() {
    let restore_calls = Arc::new(AtomicUsize::new(0));
    let engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        EphemeralResumeSubscriber {
            restore_calls: Arc::clone(&restore_calls),
        },
    );

    let error = engine
        .preview_durable_resume_position(&metadata(90, b"ephemeral"))
        .expect_err("durable restore requires durable replay");
    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::SubscriberNotDurable
    ));
    assert!(engine.runtime().last_canonical_block().is_none());
    assert_eq!(restore_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn atomic_restore_rolls_cache_back_when_subscriber_rejects_position() {
    let path = temp_path("atomic_restore_subscriber_failure");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xd4);
    let slot = U256::from(9);
    let mut saved_cache = setup_cache().await.expect("saved cache");
    let _ = saved_cache.apply_update(&StateUpdate::slot(address, slot, U256::from(11)));
    store
        .save(&saved_cache, metadata(42, b"delivery-42"))
        .expect("save checkpoint");

    let mut live_cache = setup_cache().await.expect("live cache");
    let _ = live_cache.apply_update(&StateUpdate::slot(address, slot, U256::from(99)));
    let loaded = store.load().expect("load").expect("checkpoint");
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RejectingRestoreSubscriber,
    );

    let error = engine
        .restore_durable_checkpoint(&mut live_cache, loaded, &identity())
        .expect_err("subscriber failure must abort the complete restore");

    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::Subscriber(_)
    ));
    assert_eq!(
        live_cache.cached_storage_value(address, slot),
        Some(U256::from(99))
    );
    assert!(engine.runtime().last_canonical_block().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_resume_rejects_a_control_only_active_runtime() {
    let mut cache = setup_cache().await.expect("cache");
    let active = BlockRef {
        number: 100,
        hash: B256::repeat_byte(100),
        parent_hash: Some(B256::repeat_byte(99)),
        timestamp: Some(1_700_000_100),
    };
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::CanonicalProgress(active)]),
        )
        .expect("advance control-only coverage");
    assert!(runtime.journaled_handler_ids().is_empty());
    let subscriber = ResumeRecordingSubscriber::default();
    let restore_calls = Arc::clone(&subscriber.restored);
    let mut engine = ReactiveEngine::new(runtime, subscriber);

    let error = engine
        .preview_durable_resume_position(&metadata(90, b"older"))
        .expect_err("preview cannot silently rewind active coverage");
    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::ActiveRuntime
    ));
    assert_eq!(engine.runtime().last_canonical_block(), Some(active));
    assert!(restore_calls.lock().expect("restore recorder").is_none());

    let error = engine
        .resume_from_durable_checkpoint(&metadata(90, b"older"))
        .expect_err("active coverage cannot be silently rewound");

    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::ActiveRuntime
    ));
    assert_eq!(engine.runtime().last_canonical_block(), Some(active));
    assert!(restore_calls.lock().expect("restore recorder").is_none());
}

#[test]
fn checkpoint_resume_rejects_semantically_invalid_runtime_state_before_mutation() {
    let block_120 = BlockRef {
        number: 120,
        hash: B256::repeat_byte(120),
        parent_hash: Some(B256::repeat_byte(119)),
        timestamp: Some(1_700_000_120),
    };
    let block_121 = BlockRef {
        number: 121,
        hash: B256::repeat_byte(121),
        parent_hash: Some(block_120.hash),
        timestamp: Some(1_700_000_121),
    };
    let malformed = EncodedRuntimeCheckpoint {
        version: 3,
        safe_head: Some(block_120),
        finalized_head: Some(block_121),
        health: CacheHealth::Healthy,
        pending_resyncs: Vec::new(),
        coverage_head: Some(block_121),
        journal: vec![
            EncodedBlockJournal {
                block: block_121,
                handler_ids: Vec::new(),
                rollback_diffs: Vec::new(),
            },
            EncodedBlockJournal {
                block: block_120,
                handler_ids: Vec::new(),
                rollback_diffs: Vec::new(),
            },
        ],
        freshness: None,
        tracking: HashMap::new(),
        tracked_roots: HashMap::new(),
        root_gate_cadence: RootGateCadence::every_n_blocks(16),
        last_gate_block: None,
        touched_since_gate: HashSet::new(),
        metrics: CacheMetricsSnapshot::default(),
    };
    let metadata = DurableCheckpointMetadata::new(
        identity(),
        DurableCheckpointBlock::new(block_121.number, block_121.hash)
            .with_parent_hash(block_121.parent_hash.expect("test block has parent"))
            .with_timestamp(block_121.timestamp.expect("test block has timestamp")),
    )
    .with_runtime_checkpoint(bincode::serialize(&malformed).expect("encode malformed state"));
    let subscriber = ResumeRecordingSubscriber::default();
    let restore_calls = subscriber.restored.clone();
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );

    let error = engine
        .preview_durable_resume_position(&metadata)
        .expect_err("semantic contradictions must fail preview");
    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::InvalidRuntimeCheckpoint(_)
    ));
    assert!(engine.runtime().last_canonical_block().is_none());
    assert!(restore_calls.lock().expect("restore recorder").is_none());

    let error = engine
        .resume_from_durable_checkpoint(&metadata)
        .expect_err("semantic contradictions must fail closed");

    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::InvalidRuntimeCheckpoint(_)
    ));
    assert!(engine.runtime().last_canonical_block().is_none());
    assert!(restore_calls.lock().expect("restore recorder").is_none());
}

#[test]
fn checkpoint_restore_rejects_same_height_journal_parent_conflict() {
    let coverage = BlockRef {
        number: 121,
        hash: B256::repeat_byte(121),
        parent_hash: Some(B256::repeat_byte(120)),
        timestamp: Some(1_700_000_121),
    };
    let conflicting_tail = BlockRef {
        parent_hash: Some(B256::repeat_byte(0xee)),
        ..coverage
    };
    let metadata = runtime_metadata_with_journal(coverage, [conflicting_tail]);

    assert_preview_and_resume_reject_without_mutation(&metadata);
}

#[test]
fn checkpoint_restore_rejects_same_height_journal_timestamp_conflict() {
    let coverage = BlockRef {
        number: 121,
        hash: B256::repeat_byte(121),
        parent_hash: Some(B256::repeat_byte(120)),
        timestamp: Some(1_700_000_121),
    };
    let conflicting_tail = BlockRef {
        timestamp: Some(1_800_000_121),
        ..coverage
    };
    let metadata = runtime_metadata_with_journal(coverage, [conflicting_tail]);

    assert_preview_and_resume_reject_without_mutation(&metadata);
}

#[test]
fn checkpoint_restore_rejects_coverage_that_does_not_descend_from_adjacent_journal_tail() {
    let tail = BlockRef {
        number: 120,
        hash: B256::repeat_byte(120),
        parent_hash: Some(B256::repeat_byte(119)),
        timestamp: Some(1_700_000_120),
    };
    let coverage = BlockRef {
        number: 121,
        hash: B256::repeat_byte(121),
        parent_hash: Some(B256::repeat_byte(0xee)),
        timestamp: Some(1_700_000_121),
    };
    let metadata = runtime_metadata_with_journal(coverage, [tail]);

    assert_preview_and_resume_reject_without_mutation(&metadata);
}

#[test]
fn checkpoint_restore_accepts_one_sided_same_height_optional_metadata() {
    let full = BlockRef {
        number: 121,
        hash: B256::repeat_byte(121),
        parent_hash: Some(B256::repeat_byte(120)),
        timestamp: Some(1_700_000_121),
    };
    let sparse = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..full
    };

    for (coverage, tail) in [(full, sparse), (sparse, full)] {
        let metadata = runtime_metadata_with_journal(coverage, [tail]);
        let subscriber = ResumeRecordingSubscriber::default();
        let restored = Arc::clone(&subscriber.restored);
        let mut engine = ReactiveEngine::new(
            ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
            subscriber,
        );

        let preview = engine
            .preview_durable_resume_position(&metadata)
            .expect("one-sided optional metadata is compatible");
        engine
            .resume_from_durable_checkpoint(&metadata)
            .expect("compatible runtime tail restores");
        assert_eq!(
            restored.lock().expect("restore recorder").as_ref(),
            Some(&preview)
        );
    }
}

#[test]
fn checkpoint_resume_rejects_finality_ahead_of_canonical_coverage() {
    let coverage = BlockRef {
        number: 121,
        hash: B256::repeat_byte(121),
        parent_hash: Some(B256::repeat_byte(120)),
        timestamp: Some(1_700_000_121),
    };
    let future_safe = BlockRef {
        number: 122,
        hash: B256::repeat_byte(122),
        parent_hash: Some(coverage.hash),
        timestamp: Some(1_700_000_122),
    };
    let malformed = EncodedRuntimeCheckpoint {
        version: 3,
        safe_head: Some(future_safe),
        finalized_head: None,
        health: CacheHealth::Healthy,
        pending_resyncs: Vec::new(),
        coverage_head: Some(coverage),
        journal: Vec::new(),
        freshness: None,
        tracking: HashMap::new(),
        tracked_roots: HashMap::new(),
        root_gate_cadence: RootGateCadence::every_n_blocks(16),
        last_gate_block: None,
        touched_since_gate: HashSet::new(),
        metrics: CacheMetricsSnapshot::default(),
    };
    let metadata = DurableCheckpointMetadata::new(
        identity(),
        DurableCheckpointBlock::new(coverage.number, coverage.hash)
            .with_parent_hash(coverage.parent_hash.expect("coverage parent"))
            .with_timestamp(coverage.timestamp.expect("coverage timestamp")),
    )
    .with_runtime_checkpoint(bincode::serialize(&malformed).expect("encode malformed state"));
    let subscriber = ResumeRecordingSubscriber::default();
    let restore_calls = subscriber.restored.clone();
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );

    let error = engine
        .resume_from_durable_checkpoint(&metadata)
        .expect_err("future finality must fail closed");

    assert!(matches!(
        error,
        evm_fork_cache::reactive::ReactiveCheckpointRestoreError::InvalidRuntimeCheckpoint(_)
    ));
    assert!(restore_calls.lock().expect("restore recorder").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_engine_persists_opaque_subscriber_checkpoint() {
    let path = temp_path("subscriber_checkpoint");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xb0);
    let slot = U256::from(4);
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 49, b"delivery-49").with_subscriber_checkpoint(
                SubscriberCheckpoint::new(b"hypersync:opaque-cursor".to_vec()),
            ),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut engine = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
    let mut cache = setup_cache().await.expect("cache");

    engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect("checkpoint commit")
        .expect("batch");

    let loaded = store.load().expect("load").expect("checkpoint");
    assert_eq!(
        loaded.metadata().subscriber_checkpoint.as_deref(),
        Some(&b"hypersync:opaque-cursor"[..])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_catchup_barrier_advances_checkpoint_coverage_anchor() {
    let path = temp_path("empty_barrier");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xb2);
    let slot = U256::from(6);
    let certified = BlockRef {
        number: 100,
        hash: B256::repeat_byte(100),
        parent_hash: Some(B256::repeat_byte(99)),
        timestamp: Some(1_700_000_100),
    };
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 90, b"delivery-90"),
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Barrier {
                    id: b"empty-91-100".to_vec(),
                    block: Some(certified),
                }])
                .with_delivery_token(SubscriberDeliveryToken::new(b"delivery-100".to_vec()))
                .with_subscriber_checkpoint(SubscriberCheckpoint::new(
                    b"cursor-after-100".to_vec(),
                )),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut engine = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
    let mut cache = setup_cache().await.expect("cache");
    // Compact event progress has no full header. Seed a deliberately stale EVM
    // context to prove the checkpoint cannot pair it with the newer exact pin.
    cache.set_block_context(Some(80), Some(80));
    cache.set_timestamp(Some(1_700_000_080));
    cache.set_coinbase(Some(Address::repeat_byte(0x80)));
    cache.set_prevrandao(Some(B256::repeat_byte(0x80)));
    cache.set_block_gas_limit(Some(8_000_000));
    for _ in 0..2 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("checkpoint")
            .expect("batch");
    }

    let loaded = store.load().expect("load").expect("checkpoint");
    assert_eq!(loaded.metadata().block.number, 100);
    assert_eq!(loaded.metadata().block.hash, certified.hash);
    let mut restored = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored, &identity())
        .expect("restore");
    assert_eq!(
        restored.block(),
        BlockId::from((certified.hash, Some(true)))
    );
    assert_eq!(restored.block_number(), Some(certified.number));
    assert_eq!(restored.timestamp(), certified.timestamp);
    assert_eq!(restored.basefee(), None);
    assert_eq!(restored.coinbase(), None);
    assert_eq!(restored.prevrandao(), None);
    assert_eq!(restored.block_gas_limit(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn tokenless_checkpoint_preserves_last_durable_replay_fence() {
    let path = temp_path("tokenless_replay_fence");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xb3);
    let slot = U256::from(8);
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 110, b"delivery-110")
                .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"cursor-110".to_vec())),
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Barrier {
                    id: b"tokenless".to_vec(),
                    block: Some(BlockRef {
                        number: 111,
                        hash: B256::repeat_byte(111),
                        parent_hash: Some(B256::repeat_byte(110)),
                        timestamp: Some(1_700_000_111),
                    }),
                }])
                .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"cursor-111".to_vec())),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path.clone(),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut first = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..2 {
        first
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("checkpoint")
            .expect("batch");
    }
    let loaded = store.load().expect("load").expect("checkpoint");
    let restored_metadata = loaded.metadata().clone();
    assert_eq!(
        restored_metadata.delivery_token.as_deref(),
        Some(&b"delivery-110"[..])
    );
    assert_eq!(
        restored_metadata.subscriber_checkpoint.as_deref(),
        Some(&b"cursor-111"[..])
    );
    assert!(restored_metadata.delivery_witness.is_some());
    let mut restored_cache = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored_cache, &identity())
        .expect("restore");

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_acks = Arc::new(AtomicUsize::new(0));
    let replay_subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 110, b"delivery-110")
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"cursor-110".to_vec()))]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&replay_acks),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut replay = engine(replay_subscriber, Arc::clone(&replay_calls), address, slot);
    replay
        .resume_from_durable_checkpoint(&restored_metadata)
        .expect("resume");
    assert!(matches!(
        replay
            .next_ingest_checkpointed(&mut restored_cache, &store, &identity())
            .await
            .expect("replay")
            .expect("outcome"),
        CheckpointedIngest::ReplayAcknowledged
    ));
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_acks.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_ingest_rejects_explicit_reorg_beyond_retained_journal() {
    let path = temp_path("deep_reorg_fail_closed");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xbd);
    let slot = U256::from(22);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let old_tip = BlockRef {
        number: 3,
        hash: B256::repeat_byte(3),
        parent_hash: Some(B256::repeat_byte(2)),
        timestamp: Some(1_700_000_003),
    };
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 1, b"delivery-1"),
            batch(address, 2, b"delivery-2"),
            batch(address, 3, b"delivery-3"),
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Reorg {
                    common_ancestor: BlockRef {
                        number: 1,
                        hash: B256::repeat_byte(1),
                        parent_hash: Some(B256::ZERO),
                        timestamp: Some(1_700_000_001),
                    },
                    old_tip,
                    new_tip: BlockRef {
                        number: 3,
                        hash: B256::repeat_byte(0xf3),
                        parent_hash: Some(B256::repeat_byte(0xf2)),
                        timestamp: Some(1_700_000_003),
                    },
                }])
                .with_delivery_token(SubscriberDeliveryToken::new(b"reorg-1-3".to_vec())),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 2,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..3 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("canonical checkpoint")
            .expect("batch");
    }
    let generation_before_reorg = cache.snapshot_generation();

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("deep explicit reorg cannot become a durable partial rollback");
    assert!(matches!(
        error,
        ReactiveEngineError::CheckpointReorgOutsideJournal {
            common_ancestor: 1,
            oldest_journaled: Some(2),
            journal_depth: 2,
        }
    ));
    assert_eq!(cache.snapshot_generation(), generation_before_reorg);
    assert_eq!(engine.runtime().last_canonical_block(), Some(old_tip));
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 3);
    assert_eq!(
        store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .number,
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_ingest_rejects_unknown_parent_replacement_before_mutation_or_ack() {
    let path = temp_path("implicit_deep_reorg_fail_closed");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc1);
    let slot = U256::from(25);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let replacement = BlockRef {
        number: 3,
        hash: B256::repeat_byte(0xf3),
        parent_hash: Some(B256::repeat_byte(0xf2)),
        timestamp: Some(1_700_000_003),
    };
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 1, b"delivery-1"),
            batch(address, 2, b"delivery-2"),
            batch(address, 3, b"delivery-3"),
            batch_for_block(address, replacement, b"implicit-replacement-3", false),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 2,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..3 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("canonical checkpoint")
            .expect("batch");
    }
    let generation_before_reorg = cache.snapshot_generation();
    let old_tip = engine.runtime().last_canonical_block();

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("an unproven implicit ancestor must not become durable");
    assert!(matches!(
        error,
        ReactiveEngineError::Runtime(ReactiveError::InvalidChainControl { .. })
    ));
    assert_eq!(cache.snapshot_generation(), generation_before_reorg);
    assert_eq!(engine.runtime().last_canonical_block(), old_tip);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 3);
    assert_eq!(
        store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .hash,
        B256::repeat_byte(3)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_ingest_rejects_aged_out_removed_log_before_mutation_or_ack() {
    let path = temp_path("removed_deep_reorg_fail_closed");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc2);
    let slot = U256::from(26);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let dropped = BlockRef {
        number: 1,
        hash: B256::repeat_byte(1),
        parent_hash: Some(B256::ZERO),
        timestamp: Some(1_700_000_001),
    };
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 1, b"delivery-1"),
            batch(address, 2, b"delivery-2"),
            batch(address, 3, b"delivery-3"),
            batch_for_block(address, dropped, b"removed-1", true),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 2,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..3 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("canonical checkpoint")
            .expect("batch");
    }
    let generation_before_reorg = cache.snapshot_generation();
    let old_tip = engine.runtime().last_canonical_block();

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("an aged-out removed log must not become durable");
    assert!(matches!(
        error,
        ReactiveEngineError::CheckpointReorgOutsideJournal {
            common_ancestor: 0,
            oldest_journaled: Some(2),
            journal_depth: 2,
        }
    ));
    assert_eq!(cache.snapshot_generation(), generation_before_reorg);
    assert_eq!(engine.runtime().last_canonical_block(), old_tip);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 3);
    assert_eq!(
        store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .number,
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_removed_tip_uses_authenticated_parent_and_rejects_parentless_state() {
    let path = temp_path("removed_tip_with_authenticated_parent");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc3);
    let slot = U256::from(28);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let dropped = BlockRef {
        number: 1,
        hash: B256::repeat_byte(1),
        parent_hash: Some(B256::ZERO),
        timestamp: Some(1_700_000_001),
    };
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 1, b"delivery-1"),
            batch_for_block(address, dropped, b"removed-1", true),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 1,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect("canonical checkpoint")
        .expect("batch");
    assert!(matches!(
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("the exact removed tip authenticates its parent")
            .expect("removed batch"),
        CheckpointedIngest::Applied(_)
    ));
    let parent = BlockRef {
        number: 0,
        hash: B256::ZERO,
        parent_hash: None,
        timestamp: None,
    };
    assert_eq!(engine.runtime().last_canonical_block(), Some(parent));
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 2);
    let loaded = store
        .load()
        .expect("load parent checkpoint")
        .expect("parent checkpoint");
    assert_eq!(loaded.metadata().block.number, 0);
    assert_eq!(loaded.metadata().block.hash, B256::ZERO);
    let restored_metadata = loaded.metadata().clone();
    let mut restored_cache = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored_cache, &identity())
        .expect("restore parent checkpoint");
    let mut restored_engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        ResumeRecordingSubscriber::default(),
    );
    restored_engine
        .resume_from_durable_checkpoint(&restored_metadata)
        .expect("restore runtime at authenticated parent");
    assert_eq!(
        restored_engine.runtime().last_canonical_block(),
        Some(parent)
    );

    let parentless_path = temp_path("removed_tip_without_any_anchor");
    let parentless_store = DurableCheckpointStore::new(&parentless_path);
    let parentless = BlockRef {
        parent_hash: None,
        ..dropped
    };
    let parentless_acknowledgements = Arc::new(AtomicUsize::new(0));
    let parentless_subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch_for_block(address, parentless, b"parentless-1", false),
            batch_for_block(address, parentless, b"parentless-removed-1", true),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&parentless_acknowledgements),
        checkpoint_path: parentless_path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut parentless_runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 1,
        ..ReactiveConfig::default()
    });
    parentless_runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register parentless writer");
    let mut parentless_engine = ReactiveEngine::new(parentless_runtime, parentless_subscriber);
    let mut parentless_cache = setup_cache().await.expect("parentless cache");
    parentless_engine
        .next_ingest_checkpointed(&mut parentless_cache, &parentless_store, &identity())
        .await
        .expect("parentless canonical checkpoint")
        .expect("parentless canonical batch");
    let generation_before_reorg = parentless_cache.snapshot_generation();

    let error = parentless_engine
        .next_ingest_checkpointed(&mut parentless_cache, &parentless_store, &identity())
        .await
        .expect_err("a parentless removed tip has no durable rollback anchor");
    assert!(matches!(
        error,
        ReactiveEngineError::CheckpointReorgOutsideJournal {
            common_ancestor: 0,
            oldest_journaled: Some(1),
            journal_depth: 1,
        }
    ));
    assert_eq!(
        parentless_cache.snapshot_generation(),
        generation_before_reorg
    );
    assert_eq!(
        parentless_engine.runtime().last_canonical_block(),
        Some(parentless)
    );
    assert_eq!(parentless_acknowledgements.load(Ordering::SeqCst), 1);
    assert_eq!(
        parentless_store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .number,
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_removed_tip_accepts_a_same_parent_replacement_in_the_same_batch() {
    let path = temp_path("removed_tip_with_proven_replacement");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc4);
    let slot = U256::from(29);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let dropped = BlockRef {
        number: 1,
        hash: B256::repeat_byte(1),
        parent_hash: Some(B256::ZERO),
        timestamp: Some(1_700_000_001),
    };
    let replacement = BlockRef {
        number: 1,
        hash: B256::repeat_byte(0xf1),
        parent_hash: dropped.parent_hash,
        timestamp: Some(1_700_000_002),
    };
    let removed = batch_for_block(address, dropped, b"unused", true)
        .into_records()
        .pop()
        .expect("removed record");
    let replacement_record = batch_for_block(address, replacement, b"unused", false)
        .into_records()
        .pop()
        .expect("replacement record");
    let replacement_batch = ReactiveInputBatch::new(vec![replacement_record, removed])
        .with_delivery_token(SubscriberDeliveryToken::new(b"replacement-1".to_vec()));
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 1, b"delivery-1"), replacement_batch]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 1,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..2 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("same-parent replacement is durably recoverable")
            .expect("batch");
    }

    assert_eq!(engine.runtime().last_canonical_block(), Some(replacement));
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 2);
    assert_eq!(
        store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .hash,
        replacement.hash
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_distinct_removals_share_the_first_rollback_and_remain_healthy() {
    let path = temp_path("coalesced_removed_span");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc6);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let block_11 = BlockRef {
        number: 11,
        hash: B256::repeat_byte(11),
        parent_hash: Some(B256::repeat_byte(10)),
        timestamp: Some(1_700_000_011),
    };
    let block_12 = BlockRef {
        number: 12,
        hash: B256::repeat_byte(12),
        parent_hash: Some(block_11.hash),
        timestamp: Some(1_700_000_012),
    };
    let removal_batch = ReactiveInputBatch::new(vec![
        removed_record_for_block(address, block_11, 0),
        removed_record_for_block(address, block_11, 1),
        removed_record_for_block(address, block_12, 2),
    ])
    .with_delivery_token(SubscriberDeliveryToken::new(b"removed-span".to_vec()));
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 10, b"delivery-10"),
            batch(address, 11, b"delivery-11"),
            batch(address, 12, b"delivery-12"),
            removal_batch,
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 3,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot: U256::from(31),
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    for _ in 0..4 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("the complete removed span remains checkpointable")
            .expect("batch");
    }

    assert_eq!(
        engine
            .runtime()
            .last_canonical_block()
            .map(|block| block.number),
        Some(10)
    );
    assert_eq!(engine.runtime().metrics().deep_reorgs, 0);
    assert_eq!(engine.runtime().health(), CacheHealth::Healthy);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 4);
    assert_eq!(
        store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .number,
        10
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_explicit_reorg_coalesces_removed_records_with_or_without_data_replacement() {
    for include_replacement in [false, true] {
        let path = temp_path(if include_replacement {
            "explicit_removed_with_replacement"
        } else {
            "explicit_removed_without_replacement"
        });
        let store = DurableCheckpointStore::new(&path);
        let address = Address::repeat_byte(if include_replacement { 0xc8 } else { 0xc7 });
        let acknowledgements = Arc::new(AtomicUsize::new(0));
        let ancestor = BlockRef {
            number: 20,
            hash: B256::repeat_byte(20),
            parent_hash: Some(B256::repeat_byte(19)),
            timestamp: Some(1_700_000_020),
        };
        let old_tip = BlockRef {
            number: 21,
            hash: B256::repeat_byte(21),
            parent_hash: Some(ancestor.hash),
            timestamp: Some(1_700_000_021),
        };
        let replacement = BlockRef {
            number: 21,
            hash: B256::repeat_byte(0xf1),
            parent_hash: Some(ancestor.hash),
            timestamp: Some(1_700_000_022),
        };
        let mut records = vec![removed_record_for_block(address, old_tip, 4)];
        if include_replacement {
            records.extend(
                batch_for_block(
                    address,
                    BlockRef {
                        parent_hash: None,
                        timestamp: None,
                        ..replacement
                    },
                    b"unused",
                    false,
                )
                .into_records(),
            );
        }
        let reorg_batch = ReactiveInputBatch::new(records)
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Reorg {
                common_ancestor: ancestor,
                old_tip,
                new_tip: replacement,
            }])
            .with_delivery_token(SubscriberDeliveryToken::new(b"explicit-reorg".to_vec()));
        let subscriber = OrderingSubscriber {
            batches: VecDeque::from([
                batch(address, 20, b"delivery-20"),
                batch(address, 21, b"delivery-21"),
                reorg_batch,
            ]),
            polls: Arc::new(AtomicUsize::new(0)),
            acknowledgements: Arc::clone(&acknowledgements),
            checkpoint_path: path,
            ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
        };
        let mut runtime = ReactiveRuntime::new(ReactiveConfig {
            journal_depth: 2,
            ..ReactiveConfig::default()
        });
        runtime
            .register_handler(Arc::new(CountingWriter {
                calls: Arc::new(AtomicUsize::new(0)),
                address,
                slot: U256::from(32),
            }))
            .expect("register writer");
        let mut engine = ReactiveEngine::new(runtime, subscriber);
        let mut cache = setup_cache().await.expect("cache");
        for _ in 0..3 {
            engine
                .next_ingest_checkpointed(&mut cache, &store, &identity())
                .await
                .expect("redundant post-control removal is checkpointable")
                .expect("batch");
        }
        assert_eq!(
            engine.runtime().last_canonical_block(),
            Some(if include_replacement {
                replacement
            } else {
                ancestor
            })
        );
        assert_eq!(engine.runtime().metrics().deep_reorgs, 0);
        assert_eq!(engine.runtime().health(), CacheHealth::Healthy);
        assert_eq!(acknowledgements.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_removed_sole_tip_accepts_zero_event_progress_and_blockful_barrier() {
    for use_barrier in [false, true] {
        let path = temp_path(if use_barrier {
            "removed_tip_blockful_barrier"
        } else {
            "removed_tip_progress"
        });
        let store = DurableCheckpointStore::new(&path);
        let address = Address::repeat_byte(if use_barrier { 0xca } else { 0xc9 });
        let acknowledgements = Arc::new(AtomicUsize::new(0));
        let dropped = BlockRef {
            number: 1,
            hash: B256::repeat_byte(1),
            parent_hash: Some(B256::ZERO),
            timestamp: Some(1_700_000_001),
        };
        let replacement = BlockRef {
            number: 1,
            hash: B256::repeat_byte(0xf1),
            parent_hash: dropped.parent_hash,
            timestamp: Some(1_700_000_002),
        };
        let progress = if use_barrier {
            ChainControl::Barrier {
                id: b"zero-event-replacement".to_vec(),
                block: Some(replacement),
            }
        } else {
            ChainControl::CanonicalProgress(replacement)
        };
        let replacement_batch =
            ReactiveInputBatch::new(vec![removed_record_for_block(address, dropped, 0)])
                .with_chain_id(1)
                .with_chain_controls([progress])
                .with_delivery_token(SubscriberDeliveryToken::new(
                    b"zero-event-replacement".to_vec(),
                ));
        let subscriber = OrderingSubscriber {
            batches: VecDeque::from([batch(address, 1, b"delivery-1"), replacement_batch]),
            polls: Arc::new(AtomicUsize::new(0)),
            acknowledgements: Arc::clone(&acknowledgements),
            checkpoint_path: path,
            ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
        };
        let mut runtime = ReactiveRuntime::new(ReactiveConfig {
            journal_depth: 1,
            ..ReactiveConfig::default()
        });
        runtime
            .register_handler(Arc::new(CountingWriter {
                calls: Arc::new(AtomicUsize::new(0)),
                address,
                slot: U256::from(33),
            }))
            .expect("register writer");
        let mut engine = ReactiveEngine::new(runtime, subscriber);
        let mut cache = setup_cache().await.expect("cache");
        for _ in 0..2 {
            engine
                .next_ingest_checkpointed(&mut cache, &store, &identity())
                .await
                .expect("same-parent zero-event replacement is durable")
                .expect("batch");
        }
        assert_eq!(engine.runtime().last_canonical_block(), Some(replacement));
        assert_eq!(acknowledgements.load(Ordering::SeqCst), 2);
        assert_eq!(
            store
                .load()
                .expect("load")
                .expect("checkpoint")
                .metadata()
                .block
                .hash,
            replacement.hash
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_removed_tip_rejects_a_different_parent_replacement_in_the_same_batch() {
    let path = temp_path("removed_tip_with_unproven_replacement");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc5);
    let slot = U256::from(30);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let dropped = BlockRef {
        number: 1,
        hash: B256::repeat_byte(1),
        parent_hash: Some(B256::ZERO),
        timestamp: Some(1_700_000_001),
    };
    let replacement = BlockRef {
        number: 1,
        hash: B256::repeat_byte(0xf1),
        parent_hash: Some(B256::repeat_byte(0xf0)),
        timestamp: Some(1_700_000_002),
    };
    let removed = batch_for_block(address, dropped, b"unused", true)
        .into_records()
        .pop()
        .expect("removed record");
    let replacement_record = batch_for_block(address, replacement, b"unused", false)
        .into_records()
        .pop()
        .expect("replacement record");
    let replacement_batch = ReactiveInputBatch::new(vec![replacement_record, removed])
        .with_delivery_token(SubscriberDeliveryToken::new(b"replacement-1".to_vec()));
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 1, b"delivery-1"), replacement_batch]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 1,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect("canonical checkpoint")
        .expect("batch");
    let generation_before_reorg = cache.snapshot_generation();

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("different-parent replacement has no retained common ancestor");
    assert!(matches!(
        error,
        ReactiveEngineError::Runtime(ReactiveError::InvalidChainControl { .. })
    ));
    assert_eq!(cache.snapshot_generation(), generation_before_reorg);
    assert_eq!(engine.runtime().last_canonical_block(), Some(dropped));
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_reorg_accepts_an_unlogged_ancestor_inside_retained_history() {
    let path = temp_path("sparse_reorg_checkpoint");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xbe);
    let slot = U256::from(24);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let ancestor = BlockRef {
        number: 2,
        hash: B256::repeat_byte(2),
        parent_hash: Some(B256::repeat_byte(1)),
        timestamp: Some(1_700_000_002),
    };
    let old_tip = BlockRef {
        number: 3,
        hash: B256::repeat_byte(3),
        parent_hash: Some(ancestor.hash),
        timestamp: Some(1_700_000_003),
    };
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([
            batch(address, 1, b"delivery-1"),
            batch(address, 3, b"delivery-3"),
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Reorg {
                    common_ancestor: ancestor,
                    old_tip,
                    new_tip: BlockRef {
                        number: 3,
                        hash: B256::repeat_byte(0xf3),
                        parent_hash: Some(ancestor.hash),
                        timestamp: Some(1_700_000_003),
                    },
                }])
                .with_delivery_token(SubscriberDeliveryToken::new(
                    b"reorg-through-empty-2".to_vec(),
                )),
        ]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig {
        journal_depth: 2,
        ..ReactiveConfig::default()
    });
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");

    for _ in 0..3 {
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect("in-window sparse reorg")
            .expect("batch");
    }

    assert_eq!(engine.runtime().last_canonical_block(), Some(ancestor));
    assert_eq!(engine.runtime().metrics().deep_reorgs, 0);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 3);
    assert_eq!(
        store
            .load()
            .expect("load")
            .expect("checkpoint")
            .metadata()
            .block
            .number,
        ancestor.number
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpointed_owner_catchup_requires_a_matching_rollback_journal_entry() {
    let path = temp_path("owner_catchup_journal_guard");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xd2);
    let slot = U256::from(23);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 50, b"owner-page-50")
            .with_delivery_scope(evm_fork_cache::reactive::DeliveryScope::OwnerCatchup)]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut engine = engine(subscriber, Arc::clone(&calls), address, slot);
    let mut cache = setup_cache().await.expect("cache");

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("unrollbackable owner history must fail before mutation");

    assert!(matches!(
        error,
        ReactiveEngineError::OwnerCatchupOutsideJournal { number: 50, .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 0);
    assert_eq!(cache.cached_storage_value(address, slot), None);
    assert!(!store.path().exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn finality_and_rollback_journal_survive_checkpoint_restore() {
    let path = temp_path("runtime_state");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xb4);
    let slot = U256::from(10);
    let safe = BlockRef {
        number: 120,
        hash: B256::repeat_byte(120),
        parent_hash: Some(B256::repeat_byte(119)),
        timestamp: Some(1_700_000_120),
    };
    let finalized = BlockRef {
        number: 119,
        hash: B256::repeat_byte(119),
        parent_hash: Some(B256::repeat_byte(118)),
        timestamp: Some(1_700_000_119),
    };
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 120, b"delivery-120")
            .with_chain_controls([ChainControl::Safe(safe), ChainControl::Finalized(finalized)])]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path.clone(),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut first = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
    let tracked = Address::repeat_byte(0xb5);
    first.runtime_mut().enable_freshness_stamping();
    first
        .runtime_mut()
        .freshness_mut()
        .expect("freshness enabled")
        .valid_through_slot(address, slot + U256::from(1), 777);
    first.runtime_mut().track_account(
        tracked,
        TrackingPolicy::Slots {
            slots: vec![U256::from(91)],
        },
    );
    first
        .runtime_mut()
        .set_root_gate_cadence(RootGateCadence::every_n_blocks(7));
    let mut cache = setup_cache().await.expect("cache");
    first
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect("checkpoint")
        .expect("batch");
    let loaded = store.load().expect("load").expect("checkpoint");
    let restored_metadata = loaded.metadata().clone();
    let mut restored_cache = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored_cache, &identity())
        .expect("restore");
    let empty_subscriber = OrderingSubscriber {
        batches: VecDeque::new(),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut restored = engine(
        empty_subscriber,
        Arc::new(AtomicUsize::new(0)),
        address,
        slot,
    );
    restored
        .resume_from_durable_checkpoint(&restored_metadata)
        .expect("resume runtime state");
    assert!(
        restored
            .runtime()
            .has_journaled_handler_effects(&HandlerId::new("writer")),
        "restored rollback journal must retain handler-generation ownership"
    );
    assert_eq!(
        restored.runtime().journaled_handler_ids(),
        std::collections::HashSet::from([HandlerId::new("writer")])
    );
    assert_eq!(restored.runtime().safe_head(), Some(&safe));
    assert_eq!(restored.runtime().finalized_head(), Some(&finalized));
    assert_eq!(
        restored
            .runtime()
            .freshness()
            .expect("freshness survives restart")
            .validity(address, slot + U256::from(1)),
        evm_fork_cache::freshness::Validity::ValidThrough(777)
    );
    assert_eq!(
        restored.runtime().root_gate_cadence(),
        RootGateCadence::every_n_blocks(7)
    );
    assert!(
        restored.runtime_mut().untrack_account(tracked),
        "root-gate tracking policy survives restart"
    );

    restored
        .runtime_mut()
        .ingest_batch(
            &mut restored_cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Reorg {
                    common_ancestor: finalized,
                    old_tip: safe,
                    new_tip: BlockRef {
                        number: 120,
                        hash: B256::repeat_byte(0xd0),
                        parent_hash: Some(finalized.hash),
                        timestamp: Some(1_700_000_120),
                    },
                }]),
        )
        .expect("rollback restored journal");
    assert_ne!(
        restored_cache.cached_storage_value(address, slot),
        Some(U256::from(123)),
        "restored journal must undo checkpointed block effects"
    );
    assert!(restored.runtime().safe_head().is_none());
    assert_eq!(restored.runtime().finalized_head(), Some(&finalized));
}

#[tokio::test(flavor = "multi_thread")]
async fn root_gate_baseline_and_cadence_survive_checkpoint_restore() {
    let path = temp_path("root_gate_state");
    let store = DurableCheckpointStore::new(&path);
    let event_address = Address::repeat_byte(0xb6);
    let tracked = Address::repeat_byte(0xb7);
    let slot = U256::from(15);
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(event_address, 120, b"delivery-120")]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path.clone(),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut first = engine(
        subscriber,
        Arc::new(AtomicUsize::new(0)),
        event_address,
        slot,
    );
    first
        .runtime_mut()
        .set_root_gate_cadence(RootGateCadence::every_n_blocks(4));
    first
        .runtime_mut()
        .track_account(tracked, TrackingPolicy::WholeAccount);
    let mut cache = setup_cache().await.expect("cache");
    install_fixed_root(&mut cache, B256::repeat_byte(0xa1));
    first
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect("checkpoint baseline")
        .expect("batch");

    let loaded = store.load().expect("load").expect("checkpoint");
    let restored_metadata = loaded.metadata().clone();
    let mut restored_cache = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored_cache, &identity())
        .expect("restore cache");
    install_fixed_root(&mut restored_cache, B256::repeat_byte(0xb2));
    let empty_subscriber = OrderingSubscriber {
        batches: VecDeque::new(),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut restored = engine(
        empty_subscriber,
        Arc::new(AtomicUsize::new(0)),
        event_address,
        slot,
    );
    restored
        .resume_from_durable_checkpoint(&restored_metadata)
        .expect("resume root gate state");

    let report = restored
        .ingest_batch(&mut restored_cache, direct_batch(event_address, 124))
        .expect("next root-gate cadence boundary");
    assert!(report.reports.iter().any(|report| matches!(
        report.as_ref(),
        ReactiveReport::CoverageGap(gap) if gap.address == tracked && gap.block == 124
    )));
    assert!(
        report
            .resyncs
            .iter()
            .any(|request| request.reason == evm_fork_cache::reactive::ResyncReason::RootMoved)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_failure_retries_before_polling_or_reingesting() {
    let root = temp_path("retry").parent().expect("parent").to_path_buf();
    let blocked_parent = root.join("blocked");
    fs::write(&blocked_parent, b"not a directory").expect("create blocking file");
    let checkpoint_path = blocked_parent.join("checkpoint.bin");
    let store = DurableCheckpointStore::new(&checkpoint_path);
    let calls = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let ack_saw_checkpoint = Arc::new(AtomicBool::new(false));
    let address = Address::repeat_byte(0xb1);
    let slot = U256::from(5);
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 50, b"delivery-50")]),
        polls: Arc::clone(&polls),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: checkpoint_path.clone(),
        ack_saw_checkpoint: Arc::clone(&ack_saw_checkpoint),
    };
    let mut engine = engine(subscriber, Arc::clone(&calls), address, slot);
    let mut cache = setup_cache().await.expect("cache");

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("blocked checkpoint path must fail");
    assert!(matches!(error, ReactiveEngineError::Checkpoint(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 0);

    fs::remove_file(&blocked_parent).expect("remove blocking file");
    fs::create_dir(&blocked_parent).expect("create checkpoint directory");
    let outcome = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect("retry commit")
        .expect("pending commit outcome");
    assert!(matches!(outcome, CheckpointedIngest::Applied(_)));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "handler must not run twice"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "retry must not poll a new batch"
    );
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 1);
    assert!(ack_saw_checkpoint.load(Ordering::SeqCst));
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(123))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledgement_retry_repersists_to_a_changed_checkpoint_store() {
    let first_path = temp_path("ack_store_a");
    let second_path = temp_path("ack_store_b");
    let first_store = DurableCheckpointStore::new(&first_path);
    let second_store = DurableCheckpointStore::new(&second_path);
    let address = Address::repeat_byte(0xb5);
    let slot = U256::from(14);
    struct RetrySubscriber {
        batch: Option<ReactiveInputBatch<Ethereum>>,
        fail_acknowledgement: bool,
    }
    impl EventSubscriber<Ethereum> for RetrySubscriber {
        fn capabilities(&self) -> SubscriberCapabilities {
            durable_capabilities()
        }

        fn register_interests(
            &mut self,
            _interests: &[ReactiveInterest<Ethereum>],
        ) -> SubscriberOperation<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
            Box::pin(async move { Ok(self.batch.take()) })
        }

        fn acknowledge_delivery(
            &mut self,
            _token: SubscriberDeliveryToken,
        ) -> SubscriberOperation<'_, ()> {
            Box::pin(async move {
                if self.fail_acknowledgement {
                    Err(evm_fork_cache::reactive::SubscriberError::InvalidConfig(
                        "forced acknowledgement failure",
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }
    let subscriber = RetrySubscriber {
        batch: Some(batch(address, 130, b"delivery-130")),
        fail_acknowledgement: true,
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    assert!(matches!(
        engine
            .next_ingest_checkpointed(&mut cache, &first_store, &identity())
            .await
            .expect_err("first acknowledgement fails"),
        ReactiveEngineError::Acknowledgement(_)
    ));
    assert!(first_path.is_file());

    engine.subscriber_mut().fail_acknowledgement = false;
    assert!(matches!(
        engine
            .next_ingest_checkpointed(&mut cache, &second_store, &identity())
            .await
            .expect("retry")
            .expect("pending outcome"),
        CheckpointedIngest::Applied(_)
    ));
    assert!(second_path.is_file());
    assert_eq!(
        second_store
            .load()
            .expect("load second")
            .expect("second checkpoint")
            .metadata()
            .block
            .number,
        130
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_mutation_after_save_before_ack_fails_closed_without_rebinding_checkpoint() {
    let path = temp_path("ack_retry_cache_mutation");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xba);
    let event_slot = U256::from(14);
    let unrelated_slot = U256::from(15);
    let subscriber = ToggleAckSubscriber {
        batch: Some(batch(address, 130, b"delivery-130")),
        fail_acknowledgement: true,
        acknowledgements: Arc::new(AtomicUsize::new(0)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot: event_slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");

    assert!(matches!(
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect_err("first acknowledgement fails after save"),
        ReactiveEngineError::Acknowledgement(_)
    ));
    let staged_generation = cache.snapshot_generation();
    let _ = cache.apply_update(&StateUpdate::slot(address, unrelated_slot, U256::from(999)));
    assert_ne!(cache.snapshot_generation(), staged_generation);
    engine.subscriber_mut().fail_acknowledgement = false;

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("newer cache state cannot be attached to the staged delivery");
    assert!(matches!(
        error,
        ReactiveEngineError::PendingCheckpointCacheChanged { .. }
    ));
    assert_eq!(
        engine.subscriber().acknowledgements.load(Ordering::SeqCst),
        0
    );

    let loaded = store
        .load()
        .expect("load original durable state")
        .expect("checkpoint");
    let mut restored = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored, &identity())
        .expect("restore original checkpoint");
    assert_eq!(
        restored.cached_storage_value(address, event_slot),
        Some(U256::from(123))
    );
    assert_ne!(
        restored.cached_storage_value(address, unrelated_slot),
        Some(U256::from(999)),
        "failed retry must not re-save unrelated state under delivery-130"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn block_context_mutation_after_save_before_ack_fails_closed() {
    let path = temp_path("ack_retry_block_context_mutation");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xbb);
    let slot = U256::from(16);
    let subscriber = ToggleAckSubscriber {
        batch: Some(batch(address, 131, b"delivery-131")),
        fail_acknowledgement: true,
        acknowledgements: Arc::new(AtomicUsize::new(0)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");

    assert!(matches!(
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect_err("first acknowledgement fails after save"),
        ReactiveEngineError::Acknowledgement(_)
    ));
    let staged_generation = cache.snapshot_generation();
    cache.set_timestamp(Some(1_900_000_000));
    assert_ne!(cache.snapshot_generation(), staged_generation);
    engine.subscriber_mut().fail_acknowledgement = false;

    assert!(matches!(
        engine
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect_err("changed persisted block context must stop acknowledgement"),
        ReactiveEngineError::PendingCheckpointCacheChanged { .. }
    ));
    assert_eq!(
        engine.subscriber().acknowledgements.load(Ordering::SeqCst),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn restored_delivery_token_suppresses_cross_process_replay() {
    let path = temp_path("replay");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc1);
    let slot = U256::from(9);
    let first_calls = Arc::new(AtomicUsize::new(0));
    let first_subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 60, b"delivery-60")]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path.clone(),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut first_engine = engine(first_subscriber, Arc::clone(&first_calls), address, slot);
    let mut first_cache = setup_cache().await.expect("first cache");
    first_engine
        .next_ingest_checkpointed(&mut first_cache, &store, &identity())
        .await
        .expect("first commit")
        .expect("first outcome");
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);

    let loaded = store.load().expect("load checkpoint").expect("checkpoint");
    let restored_metadata = loaded.metadata().clone();
    let mut restored_cache = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored_cache, &identity())
        .expect("restore cache");

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_acks = Arc::new(AtomicUsize::new(0));
    let replay_subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 60, b"delivery-60")]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&replay_acks),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut replay_engine = engine(replay_subscriber, Arc::clone(&replay_calls), address, slot);
    replay_engine
        .resume_from_durable_checkpoint(&restored_metadata)
        .expect("resume delivery bookkeeping");
    let outcome = replay_engine
        .next_ingest_checkpointed(&mut restored_cache, &store, &identity())
        .await
        .expect("acknowledge replay")
        .expect("replay outcome");
    assert!(matches!(outcome, CheckpointedIngest::ReplayAcknowledged));
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_acks.load(Ordering::SeqCst), 1);
    assert_eq!(
        restored_cache.cached_storage_value(address, slot),
        Some(U256::from(123))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn restored_token_rejects_a_different_delivery_before_ack_or_ingest() {
    let path = temp_path("replay_witness_mismatch");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc8);
    let slot = U256::from(18);
    let first_subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 60, b"delivery-60")
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"cursor-original".to_vec()))]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path.clone(),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut first = engine(
        first_subscriber,
        Arc::new(AtomicUsize::new(0)),
        address,
        slot,
    );
    let mut first_cache = setup_cache().await.expect("first cache");
    first
        .next_ingest_checkpointed(&mut first_cache, &store, &identity())
        .await
        .expect("first commit")
        .expect("first outcome");

    let loaded = store.load().expect("load").expect("checkpoint");
    let metadata = loaded.metadata().clone();
    assert!(metadata.delivery_witness.is_some());
    let mut restored_cache = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored_cache, &identity())
        .expect("restore cache");

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_acks = Arc::new(AtomicUsize::new(0));
    let replay_subscriber = OrderingSubscriber {
        // Reusing the token with a different provider cursor is a different
        // delivery even though its EVM log happens to be identical.
        batches: VecDeque::from([batch(address, 60, b"delivery-60")
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"cursor-reused".to_vec()))]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&replay_acks),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut replay = engine(replay_subscriber, Arc::clone(&replay_calls), address, slot);
    replay
        .resume_from_durable_checkpoint(&metadata)
        .expect("resume");

    let error = replay
        .next_ingest_checkpointed(&mut restored_cache, &store, &identity())
        .await
        .expect_err("token reuse must not bypass delivery comparison");
    assert!(matches!(error, ReactiveEngineError::ReplayDeliveryMismatch));
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_acks.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn tokened_header_requires_an_exact_source_payload_commitment() {
    let path = temp_path("header_commitment_required");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xca);
    let slot = U256::from(24);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([header_batch(30_000_000, b"header-70", None)]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&acknowledgements),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut engine = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
    let mut cache = setup_cache().await.expect("cache");

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("generic header payload cannot be durably witnessed by hash alone");
    assert!(matches!(
        error,
        ReactiveEngineError::MissingPayloadCommitment
    ));
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 0);
    assert!(engine.runtime().last_canonical_block().is_none());
    assert!(!store.path().exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn header_payload_commitment_accepts_exact_replay_and_rejects_body_or_digest_changes() {
    let path = temp_path("header_commitment_replay");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xcb);
    let slot = U256::from(25);
    let original_commitment = SubscriberPayloadCommitment::new(keccak256(b"header-wire-a"));
    let first_subscriber = OrderingSubscriber {
        batches: VecDeque::from([header_batch(
            30_000_000,
            b"header-70",
            Some(original_commitment),
        )]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path.clone(),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut first = engine(
        first_subscriber,
        Arc::new(AtomicUsize::new(0)),
        address,
        slot,
    );
    let mut first_cache = setup_cache().await.expect("first cache");
    first
        .next_ingest_checkpointed(&mut first_cache, &store, &identity())
        .await
        .expect("commit witnessed header")
        .expect("header batch");
    let loaded = store.load().expect("load").expect("checkpoint");
    let metadata = loaded.metadata().clone();

    let exact_acks = Arc::new(AtomicUsize::new(0));
    let exact_subscriber = OrderingSubscriber {
        batches: VecDeque::from([header_batch(
            30_000_000,
            b"header-70",
            Some(original_commitment),
        )]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&exact_acks),
        checkpoint_path: path.clone(),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut exact = engine(
        exact_subscriber,
        Arc::new(AtomicUsize::new(0)),
        address,
        slot,
    );
    exact
        .resume_from_durable_checkpoint(&metadata)
        .expect("resume exact replay");
    let mut exact_cache = setup_cache().await.expect("exact replay cache");
    loaded
        .restore_into(&mut exact_cache, &identity())
        .expect("restore exact cache");
    assert!(matches!(
        exact
            .next_ingest_checkpointed(&mut exact_cache, &store, &identity())
            .await
            .expect("exact replay")
            .expect("outcome"),
        CheckpointedIngest::ReplayAcknowledged
    ));
    assert_eq!(exact_acks.load(Ordering::SeqCst), 1);

    let changed_commitment = SubscriberPayloadCommitment::new(keccak256(b"header-wire-b"));
    for (gas_limit, commitment) in [
        (31_000_000, changed_commitment),
        (30_000_000, changed_commitment),
    ] {
        let replay_acks = Arc::new(AtomicUsize::new(0));
        let subscriber = OrderingSubscriber {
            batches: VecDeque::from([header_batch(gas_limit, b"header-70", Some(commitment))]),
            polls: Arc::new(AtomicUsize::new(0)),
            acknowledgements: Arc::clone(&replay_acks),
            checkpoint_path: path.clone(),
            ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
        };
        let mut replay = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
        replay
            .resume_from_durable_checkpoint(&metadata)
            .expect("resume mismatch replay");
        let reloaded = store.load().expect("reload").expect("checkpoint");
        let mut cache = setup_cache().await.expect("mismatch cache");
        reloaded
            .restore_into(&mut cache, &identity())
            .expect("restore mismatch cache");

        let error = replay
            .next_ingest_checkpointed(&mut cache, &store, &identity())
            .await
            .expect_err("body or commitment change must not reuse the token");
        assert!(matches!(error, ReactiveEngineError::ReplayDeliveryMismatch));
        assert_eq!(replay_acks.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn manually_stored_token_without_a_witness_cannot_skip_replay() {
    let address = Address::repeat_byte(0xc9);
    let slot = U256::from(20);
    let checkpoint = metadata(62, b"delivery-62");
    assert!(checkpoint.delivery_witness.is_none());
    let replay_acks = Arc::new(AtomicUsize::new(0));
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([batch(address, 62, b"delivery-62")]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::clone(&replay_acks),
        checkpoint_path: temp_path("missing_replay_witness"),
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut replay = engine(subscriber, Arc::new(AtomicUsize::new(0)), address, slot);
    replay
        .resume_from_durable_checkpoint(&checkpoint)
        .expect("resume low-level metadata");
    let mut cache = setup_cache().await.expect("cache");
    let store = DurableCheckpointStore::new(temp_path("missing_replay_witness_store"));

    let error = replay
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("unwitnessed token cannot use replay shortcut");
    assert!(matches!(error, ReactiveEngineError::MissingReplayWitness));
    assert_eq!(replay_acks.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_after_durable_save_before_ack_replays_only_the_acknowledgement() {
    let path = temp_path("saved_before_ack_crash");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xc2);
    let slot = U256::from(19);
    let first_calls = Arc::new(AtomicUsize::new(0));
    let first_subscriber = ToggleAckSubscriber {
        batch: Some(batch(address, 61, b"delivery-61")),
        fail_acknowledgement: true,
        acknowledgements: Arc::new(AtomicUsize::new(0)),
    };
    let mut first_runtime = ReactiveRuntime::new(ReactiveConfig::default());
    first_runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::clone(&first_calls),
            address,
            slot,
        }))
        .expect("register first writer");
    let mut first_engine = ReactiveEngine::new(first_runtime, first_subscriber);
    let mut first_cache = setup_cache().await.expect("first cache");

    assert!(matches!(
        first_engine
            .next_ingest_checkpointed(&mut first_cache, &store, &identity())
            .await
            .expect_err("source acknowledgement fails after durable save"),
        ReactiveEngineError::Acknowledgement(_)
    ));
    assert!(
        path.is_file(),
        "checkpoint committed before acknowledgement"
    );
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    drop(first_engine);
    drop(first_cache);

    let loaded = store.load().expect("load checkpoint").expect("checkpoint");
    let restored_metadata = loaded.metadata().clone();
    assert_eq!(
        restored_metadata.delivery_token.as_deref(),
        Some(&b"delivery-61"[..])
    );
    let mut restored_cache = setup_cache().await.expect("restored cache");
    loaded
        .restore_into(&mut restored_cache, &identity())
        .expect("restore cache");
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_acks = Arc::new(AtomicUsize::new(0));
    let replay_subscriber = ToggleAckSubscriber {
        batch: Some(batch(address, 61, b"delivery-61")),
        fail_acknowledgement: false,
        acknowledgements: Arc::clone(&replay_acks),
    };
    let mut replay_runtime = ReactiveRuntime::new(ReactiveConfig::default());
    replay_runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::clone(&replay_calls),
            address,
            slot,
        }))
        .expect("register replay writer");
    let mut replay_engine = ReactiveEngine::new(replay_runtime, replay_subscriber);
    replay_engine
        .resume_from_durable_checkpoint(&restored_metadata)
        .expect("resume committed token");

    assert!(matches!(
        replay_engine
            .next_ingest_checkpointed(&mut restored_cache, &store, &identity())
            .await
            .expect("replay acknowledgement")
            .expect("replay outcome"),
        CheckpointedIngest::ReplayAcknowledged
    ));
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_acks.load(Ordering::SeqCst), 1);
    assert_eq!(
        restored_cache.cached_storage_value(address, slot),
        Some(U256::from(123))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn into_parts_retains_an_engine_with_uncommitted_delivery_state() {
    let address = Address::repeat_byte(0xca);
    let slot = U256::from(27);
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let subscriber = ToggleAckSubscriber {
        batch: Some(batch(address, 63, b"delivery-63")),
        fail_acknowledgement: true,
        acknowledgements: Arc::clone(&acknowledgements),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(CountingWriter {
            calls: Arc::new(AtomicUsize::new(0)),
            address,
            slot,
        }))
        .expect("register writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");

    assert!(matches!(
        engine
            .next_ingest(&mut cache)
            .await
            .expect_err("first acknowledgement is forced to fail"),
        ReactiveEngineError::Acknowledgement(_)
    ));
    let mut engine = match engine.into_parts() {
        Err(engine) => *engine,
        Ok(_) => panic!("pending delivery state must prevent destructive extraction"),
    };
    engine.subscriber_mut().fail_acknowledgement = false;
    engine
        .next_ingest(&mut cache)
        .await
        .expect("retry commits the retained acknowledgement")
        .expect("retained report");

    assert_eq!(acknowledgements.load(Ordering::SeqCst), 1);
    assert!(engine.into_parts().is_ok());
}

struct FailOnBlockWriter {
    address: Address,
    slot: U256,
    fail_on: u64,
}

impl ReactiveHandler<Ethereum> for FailOnBlockWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("fail-on-second-record")
    }

    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: None,
        })]
    }

    fn handle(
        &self,
        ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        if ctx
            .block
            .as_ref()
            .is_some_and(|block| block.number == self.fail_on)
        {
            return Err(HandlerError::new("forced second-record failure"));
        }
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot_delta(
                self.address,
                self.slot,
                SlotDelta::Add(U256::from(1)),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: Vec::new(),
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_multi_record_ingest_rolls_back_cache_and_runtime_before_replay() {
    let path = temp_path("ingest_rollback");
    let store = DurableCheckpointStore::new(&path);
    let address = Address::repeat_byte(0xd1);
    let slot = U256::from(12);
    let mut records = batch(address, 70, b"unused").into_records();
    records.extend(batch(address, 71, b"unused").into_records());
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([ReactiveInputBatch::new(records)
            .with_delivery_token(SubscriberDeliveryToken::new(b"delivery-71".to_vec()))]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(FailOnBlockWriter {
            address,
            slot,
            fail_on: 71,
        }))
        .expect("register failing writer");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");
    let _ = cache.apply_update(&StateUpdate::slot(address, slot, U256::from(10)));

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("second record fails");
    assert!(matches!(error, ReactiveEngineError::Runtime(_)));
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(10))
    );
    assert!(engine.runtime().last_canonical_block().is_none());
    assert_eq!(
        engine.subscriber().acknowledgements.load(Ordering::SeqCst),
        0
    );
    assert!(!store.path().exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_stage_failure_rolls_back_before_dispatching_hooks() {
    let path = temp_path("stage_before_hooks");
    let store = DurableCheckpointStore::new(&path);
    let hooks = Arc::new(AtomicUsize::new(0));
    let pending = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::PendingTxHash(B256::repeat_byte(0xef)),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Subscription,
            chain_status: ChainStatus::Pending,
            block: None,
            transaction_index: None,
            log_index: None,
        },
    )])
    .with_delivery_token(SubscriberDeliveryToken::new(b"pending-only".to_vec()));
    let subscriber = OrderingSubscriber {
        batches: VecDeque::from([pending]),
        polls: Arc::new(AtomicUsize::new(0)),
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        checkpoint_path: path,
        ack_saw_checkpoint: Arc::new(AtomicBool::new(false)),
    };
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_hook(Arc::new(CountingHook(Arc::clone(&hooks))))
        .expect("register hook");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let mut cache = setup_cache().await.expect("cache");

    let error = engine
        .next_ingest_checkpointed(&mut cache, &store, &identity())
        .await
        .expect_err("pending-only state has no canonical checkpoint anchor");

    assert!(matches!(error, ReactiveEngineError::MissingCheckpointBlock));
    assert_eq!(
        hooks.load(Ordering::SeqCst),
        0,
        "rolled-back reports must not escape through hooks"
    );
    assert!(engine.runtime().last_canonical_block().is_none());
    assert!(!store.path().exists());
}
