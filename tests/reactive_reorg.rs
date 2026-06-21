//! Manager-authored acceptance tests for reactive block journaling and reorg recovery.
//!
//! These tests cover the runtime-owned machinery that downstream crates should not
//! need to rebuild: journaling canonical block effects, handling removed/reorged
//! inputs, rolling back reversible storage writes, falling back to targeted purges
//! for irreversible effects, and canceling stale hash-pinned resyncs.
#![cfg(feature = "reactive")]

mod common;

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    BlockRef, ChainStatus, HandlerError, HandlerId, HandlerOutcome, InputSource,
    InvalidationReason, InvalidationRequest, LogInterest, ReactiveConfig, ReactiveContext,
    ReactiveEffect, ReactiveHandler, ReactiveInput, ReactiveInputBatch, ReactiveInputRecord,
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

fn rpc_log(
    address: Address,
    topics: Vec<B256>,
    block: &BlockRef,
    tx_index: u64,
    log_index: u64,
    removed: bool,
) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::new()),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte((tx_index + 1) as u8)),
        transaction_index: Some(tx_index),
        log_index: Some(log_index),
        removed,
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

fn reorged_context(dropped_from: BlockRef, log_index: u64) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Batch,
        chain_status: ChainStatus::Reorged {
            dropped_from: dropped_from.clone(),
        },
        block: Some(dropped_from),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

fn batch(input: ReactiveInput<Ethereum>, ctx: ReactiveContext) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, ctx)])
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
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                self.value,
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

struct LogIndexSlotWriter {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for LogIndexSlotWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("log-index-slot-writer")
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
                U256::from(ctx.log_index.expect("test log context carries index")),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

struct PurgeHandler {
    address: Address,
}

impl ReactiveHandler<Ethereum> for PurgeHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("purge-handler")
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
            effects: vec![ReactiveEffect::Invalidate(InvalidationRequest {
                scope: PurgeScope::AllStorage,
                address: self.address,
                reason: InvalidationReason::HandlerRequested,
            })],
            quality: StateEffectQuality::RequiresRepair,
            tags: vec![],
        })
    }
}

struct ResyncOnlyHandler {
    address: Address,
    slot: U256,
    block: ResyncBlock,
}

impl ReactiveHandler<Ethereum> for ResyncOnlyHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("resync-only")
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
                id: ResyncId::new("hash-pinned-repair"),
                reason: ResyncReason::HandlerRequested,
                block: self.block.clone(),
                targets: vec![ResyncTarget::StorageSlot {
                    address: self.address,
                    slot: self.slot,
                }],
                priority: ResyncPriority::High,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

#[tokio::test]
async fn reactive_runtime_rolls_back_hash_pinned_resync_effects_with_dropped_block() -> Result<()> {
    let address = Address::repeat_byte(0xa5);
    let slot = U256::from(10);
    let dropped = block(110, B256::repeat_byte(0xbb), B256::repeat_byte(0xab));
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(10))?;
    cache.set_storage_batch_fetcher(Arc::new(move |requests, _block| {
        requests
            .into_iter()
            .map(|(address, slot)| (address, slot, Ok(U256::from(42))))
            .collect()
    }));

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: HandlerId::new("provisional-writer"),
        address,
        slot,
        value: U256::from(20),
    }))?;
    runtime.register_handler(Arc::new(ResyncOnlyHandler {
        address,
        slot,
        block: ResyncBlock::Hash {
            number: dropped.number,
            hash: dropped.hash,
            require_canonical: true,
        },
    }))?;

    runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"WriteThenRepair()")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped.clone(), 0),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(42))
    );

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"WriteThenRepair()")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped, 0),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(10)),
        "reorg rollback must unwind authoritative resync writes as well as direct effects"
    );
    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("removed log emits a reorg report");
    assert_eq!(reorg.rollback_updates.len(), 2);
    assert_eq!(reorg.rollback_diff.slots[0].old, U256::from(42));
    assert_eq!(reorg.rollback_diff.slots[0].new, U256::from(20));
    assert_eq!(reorg.rollback_diff.slots[1].old, U256::from(20));
    assert_eq!(reorg.rollback_diff.slots[1].new, U256::from(10));

    Ok(())
}

#[tokio::test]
async fn reactive_runtime_rolls_back_removed_log_storage_effects() -> Result<()> {
    let address = Address::repeat_byte(0xa1);
    let slot = U256::from(7);
    let dropped = block(70, B256::repeat_byte(0x70), B256::repeat_byte(0x6f));
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(10))?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: HandlerId::new("slot-writer"),
        address,
        slot,
        value: U256::from(20),
    }))?;

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Write(uint256)")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped.clone(), 0),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20))
    );

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Write(uint256)")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped.clone(), 0),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(10)),
        "removed logs should roll back reversible storage writes"
    );
    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("removed log emits a reorg report");
    assert_eq!(reorg.dropped_blocks, vec![dropped]);
    assert_eq!(reorg.rollback_updates.len(), 1);
    assert!(reorg.purge_updates.is_empty());
    assert_eq!(reorg.rollback_diff.slots[0].old, U256::from(20));
    assert_eq!(reorg.rollback_diff.slots[0].new, U256::from(10));

    Ok(())
}

#[tokio::test]
async fn reactive_runtime_reorgs_parent_mismatch_before_replacement_block() -> Result<()> {
    let address = Address::repeat_byte(0xa2);
    let slot = U256::from(8);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let dropped = block(80, B256::repeat_byte(0x80), parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), parent.hash);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Base(uint256)")],
                &parent,
                0,
                10,
                false,
            )),
            included_context(parent.clone(), 10),
        ),
    )?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Dropped(uint256)")],
                &dropped,
                0,
                20,
                false,
            )),
            included_context(dropped.clone(), 20),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20))
    );

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Replacement(uint256)")],
                &replacement,
                0,
                30,
                false,
            )),
            included_context(replacement.clone(), 30),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(30)),
        "replacement block should apply after the dropped block is rolled back"
    );
    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("replacement block emits a parent-mismatch reorg report");
    assert_eq!(reorg.dropped_blocks, vec![dropped]);
    assert_eq!(reorg.rollback_updates.len(), 1);
    assert!(reorg
        .dropped_inputs
        .iter()
        .any(|input| matches!(input, evm_fork_cache::reactive::InputRef::Log { block_hash, .. } if *block_hash == B256::repeat_byte(0x80))));

    Ok(())
}

#[tokio::test]
async fn reactive_runtime_falls_back_to_purge_for_irreversible_dropped_effects() -> Result<()> {
    let address = Address::repeat_byte(0xa3);
    let slot = U256::from(9);
    let dropped = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x8f));
    let replacement = block(90, B256::repeat_byte(0x91), B256::repeat_byte(0x8f));
    let unrelated = Address::repeat_byte(0xf0);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(99))?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PurgeHandler { address }))?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Purge()")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped.clone(), 0),
        ),
    )?;
    assert_eq!(cache.cached_storage_value(address, slot), Some(U256::ZERO));

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                unrelated,
                vec![keccak256(b"Replacement()")],
                &replacement,
                0,
                0,
                false,
            )),
            included_context(replacement, 0),
        ),
    )?;

    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("replacement block emits a reorg report");
    assert_eq!(reorg.dropped_blocks, vec![dropped]);
    assert!(reorg.rollback_updates.is_empty());
    assert_eq!(
        reorg.purge_updates,
        vec![StateUpdate::purge(address, PurgeScope::AllStorage)]
    );

    Ok(())
}

#[tokio::test]
async fn reactive_runtime_cancels_hash_pinned_resyncs_for_dropped_blocks() -> Result<()> {
    let address = Address::repeat_byte(0xa4);
    let dropped = block(100, B256::repeat_byte(0xaa), B256::repeat_byte(0x99));
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(ResyncOnlyHandler {
        address,
        slot: U256::from(1),
        block: ResyncBlock::Hash {
            number: dropped.number,
            hash: dropped.hash,
            require_canonical: true,
        },
    }))?;

    let first = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"NeedsRepair()")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped.clone(), 0),
        ),
    )?;
    assert_eq!(first.resyncs.len(), 1);

    let second = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"NeedsRepair()")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped, 0),
        ),
    )?;
    let reorg = second
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("dropped block emits a reorg report");
    assert_eq!(reorg.canceled_resyncs.len(), 1);
    assert_eq!(
        reorg.canceled_resyncs[0].id,
        ResyncId::new("hash-pinned-repair")
    );

    Ok(())
}
