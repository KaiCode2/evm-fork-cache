//! Manager-authored acceptance tests for trace-backed reactive resync execution.
//!
//! These tests pin the Tier-3 liveness/resync path: when handlers request sync
//! for a block, the runtime should be able to satisfy matching targets from one
//! block-level state diff before falling back to per-slot RPC reads.
#![cfg(feature = "reactive")]

mod common;

use std::sync::{Arc, Mutex};

use alloy_eips::BlockId;
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::cache::{
    BlockStateAccountDiff, BlockStateDiff, BlockStateStorageDiff, EvmCache,
};
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    BlockRef, ChainStatus, HandlerError, HandlerId, HandlerOutcome, InputSource, LogInterest,
    ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveHandler, ReactiveInput,
    ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveReport, ReactiveRuntime,
    ResyncBlock, ResyncId, ResyncPriority, ResyncReason, ResyncRequest, ResyncTarget, RouteKeySpec,
    StateEffectQuality,
};

fn rpc_log(address: Address, topics: Vec<B256>, block_number: u64) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::new()),
        block_hash: Some(B256::repeat_byte(block_number as u8)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0x44)),
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

fn batch(input: ReactiveInput<Ethereum>, ctx: ReactiveContext) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, ctx)])
}

fn diff_for_slots(
    address: Address,
    slots: impl IntoIterator<Item = (U256, U256)>,
) -> BlockStateDiff {
    BlockStateDiff {
        accounts: vec![BlockStateAccountDiff {
            address,
            balance: None,
            nonce: None,
            code: None,
            storage: slots
                .into_iter()
                .map(|(slot, value)| BlockStateStorageDiff { slot, value })
                .collect(),
        }],
    }
}

struct TraceMultiSlotResync {
    address: Address,
    slots: Vec<U256>,
    block: ResyncBlock,
}

impl ReactiveHandler<Ethereum> for TraceMultiSlotResync {
    fn id(&self) -> HandlerId {
        HandlerId::new("trace-multi-slot-resync")
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
                id: ResyncId::new("trace-slot-repair"),
                reason: ResyncReason::HandlerRequested,
                block: self.block.clone(),
                targets: vec![ResyncTarget::StorageSlots {
                    address: self.address,
                    slots: self.slots.clone(),
                }],
                priority: ResyncPriority::High,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

fn install_panic_storage_fetcher(cache: &mut EvmCache) {
    cache.set_storage_batch_fetcher(Arc::new(|requests, _block| {
        panic!("storage fetcher should not be called; requests={requests:?}")
    }));
}

#[tokio::test]
async fn trace_resync_coalesces_block_fetch_and_applies_matching_slots() -> Result<()> {
    let address = Address::repeat_byte(0xa1);
    let slot_a = U256::from(10);
    let slot_b = U256::from(11);
    let block_hash = B256::repeat_byte(0x70);
    let seen_trace_blocks = Arc::new(Mutex::new(Vec::new()));
    let mut cache = setup_cache().await?;
    cache.set_block_state_diff_fetcher({
        let seen_trace_blocks = seen_trace_blocks.clone();
        Arc::new(move |block| {
            seen_trace_blocks.lock().unwrap().push(block);
            Ok(diff_for_slots(
                address,
                [(slot_a, U256::from(700)), (slot_b, U256::from(800))],
            ))
        })
    });
    install_panic_storage_fetcher(&mut cache);

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(TraceMultiSlotResync {
        address,
        slots: vec![slot_a, slot_b],
        block: ResyncBlock::Hash {
            number: 70,
            hash: block_hash,
            require_canonical: true,
        },
    }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"TraceRepair()")], 70)),
            included_context(70),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot_a),
        Some(U256::from(700))
    );
    assert_eq!(
        cache.cached_storage_value(address, slot_b),
        Some(U256::from(800))
    );

    let trace_blocks = seen_trace_blocks.lock().unwrap();
    assert_eq!(
        trace_blocks.len(),
        1,
        "all targets in one block should share one trace request"
    );
    match &trace_blocks[0] {
        BlockId::Hash(hash) => {
            assert_eq!(hash.block_hash, block_hash);
            assert_eq!(hash.require_canonical, Some(true));
        }
        other => panic!("expected hash-pinned trace request, got {other:?}"),
    }

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert_eq!(resynced[0].state_updates.len(), 2);
    assert!(resynced[0].failed.is_empty());
    assert_eq!(runtime.metrics().resync_failures, 0);

    Ok(())
}

#[tokio::test]
async fn trace_resync_falls_back_to_storage_for_unresolved_cold_slot() -> Result<()> {
    let address = Address::repeat_byte(0xa2);
    let slot = U256::from(21);
    let seen_trace_blocks = Arc::new(Mutex::new(Vec::new()));
    let seen_storage_fetches = Arc::new(Mutex::new(Vec::new()));
    let mut cache = setup_cache().await?;
    cache.set_block_state_diff_fetcher({
        let seen_trace_blocks = seen_trace_blocks.clone();
        Arc::new(move |block| {
            seen_trace_blocks.lock().unwrap().push(block);
            Ok(BlockStateDiff { accounts: vec![] })
        })
    });
    cache.set_storage_batch_fetcher({
        let seen_storage_fetches = seen_storage_fetches.clone();
        Arc::new(move |requests, block| {
            seen_storage_fetches
                .lock()
                .unwrap()
                .push((requests.clone(), block));
            requests
                .into_iter()
                .map(|(addr, slot)| (addr, slot, Ok(U256::from(900))))
                .collect()
        })
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(TraceMultiSlotResync {
        address,
        slots: vec![slot],
        block: ResyncBlock::Number(71),
    }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"TraceRepair()")], 71)),
            included_context(71),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(900))
    );
    assert_eq!(seen_trace_blocks.lock().unwrap().len(), 1);
    let storage_fetches = seen_storage_fetches.lock().unwrap();
    assert_eq!(storage_fetches.len(), 1);
    assert_eq!(storage_fetches[0].0, vec![(address, slot)]);
    assert_eq!(storage_fetches[0].1, BlockId::number(71));

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert_eq!(resynced[0].state_updates.len(), 1);
    assert!(resynced[0].failed.is_empty());

    Ok(())
}

#[tokio::test]
async fn trace_resync_matches_storage_resync_state_with_fewer_rpc_units() -> Result<()> {
    let address = Address::repeat_byte(0xa3);
    let slot_a = U256::from(31);
    let slot_b = U256::from(32);

    let mut trace_cache = setup_cache().await?;
    let trace_blocks = Arc::new(Mutex::new(Vec::new()));
    let trace_storage_fetches = Arc::new(Mutex::new(0usize));
    trace_cache.set_block_state_diff_fetcher({
        let trace_blocks = trace_blocks.clone();
        Arc::new(move |block| {
            trace_blocks.lock().unwrap().push(block);
            Ok(diff_for_slots(
                address,
                [(slot_a, U256::from(3100)), (slot_b, U256::from(3200))],
            ))
        })
    });
    trace_cache.set_storage_batch_fetcher({
        let trace_storage_fetches = trace_storage_fetches.clone();
        Arc::new(move |requests, _block| {
            *trace_storage_fetches.lock().unwrap() += 1;
            requests
                .into_iter()
                .map(|(addr, slot)| (addr, slot, Ok(U256::ZERO)))
                .collect()
        })
    });

    let storage_fetches = Arc::new(Mutex::new(Vec::new()));
    let mut storage_cache = setup_cache().await?;
    storage_cache.set_storage_batch_fetcher({
        let storage_fetches = storage_fetches.clone();
        Arc::new(move |requests, block| {
            storage_fetches
                .lock()
                .unwrap()
                .push((requests.clone(), block));
            requests
                .into_iter()
                .map(|(addr, slot)| {
                    let value = if slot == slot_a {
                        U256::from(3100)
                    } else if slot == slot_b {
                        U256::from(3200)
                    } else {
                        U256::ZERO
                    };
                    (addr, slot, Ok(value))
                })
                .collect()
        })
    });

    let run = |cache: &mut EvmCache| -> Result<_> {
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.register_handler(Arc::new(TraceMultiSlotResync {
            address,
            slots: vec![slot_a, slot_b],
            block: ResyncBlock::Number(72),
        }))?;
        Ok(runtime.ingest_batch_with_resync(
            cache,
            batch(
                ReactiveInput::Log(rpc_log(address, vec![keccak256(b"TraceRepair()")], 72)),
                included_context(72),
            ),
        )?)
    };

    let trace_report = run(&mut trace_cache)?;
    let storage_report = run(&mut storage_cache)?;

    assert_eq!(
        trace_cache.cached_storage_value(address, slot_a),
        storage_cache.cached_storage_value(address, slot_a)
    );
    assert_eq!(
        trace_cache.cached_storage_value(address, slot_b),
        storage_cache.cached_storage_value(address, slot_b)
    );
    assert_eq!(
        trace_cache.cached_storage_value(address, slot_a),
        Some(U256::from(3100))
    );
    assert_eq!(
        trace_cache.cached_storage_value(address, slot_b),
        Some(U256::from(3200))
    );

    assert_eq!(
        trace_report.applied.len(),
        storage_report.applied.len(),
        "direct handler behavior should be identical"
    );
    assert_eq!(
        trace_blocks.lock().unwrap().len(),
        1,
        "trace path should need one block-level request"
    );
    assert_eq!(
        *trace_storage_fetches.lock().unwrap(),
        0,
        "trace-covered targets should not fall back to per-slot storage RPC"
    );
    assert_eq!(
        storage_fetches.lock().unwrap().len(),
        1,
        "storage-only path still uses the existing batched storage fetcher"
    );

    Ok(())
}
