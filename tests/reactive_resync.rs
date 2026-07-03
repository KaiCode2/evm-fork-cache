//! Manager-authored acceptance tests for reactive resync execution.
//!
//! These tests pin the next runtime slice after routing: handlers can already
//! emit `ResyncRequest`s, but the runtime must also be able to execute storage
//! resyncs after direct effects, apply authoritative values through
//! `StateUpdate`, and report both applied and failed resync targets.
#![cfg(feature = "reactive")]

mod common;

use std::sync::{Arc, Mutex};

use alloy_eips::BlockId;
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::StateUpdate;
use evm_fork_cache::cache::{AccountProof, EvmCache};
use evm_fork_cache::errors::StorageFetchError;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AccountFieldMask, BlockRef, ChainStatus, HandlerError, HandlerId, HandlerOutcome, InputSource,
    LogInterest, ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveHandler, ReactiveInput,
    ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveReport, ReactiveRuntime,
    ResyncBlock, ResyncFailureKind, ResyncId, ResyncPriority, ResyncReason, ResyncRequest,
    ResyncTarget, RouteKeySpec, StateEffectQuality,
};
use revm::primitives::hardfork::SpecId;

/// Recorded `(requests, block)` calls captured by a mock `AccountProofFetchFn`.
type ProofFetchCalls = Vec<(Vec<(Address, Vec<U256>)>, BlockId)>;

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

struct WriteThenResync {
    address: Address,
    slot: U256,
    block_hash: B256,
}

impl ReactiveHandler<Ethereum> for WriteThenResync {
    fn id(&self) -> HandlerId {
        HandlerId::new("write-then-resync")
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
                ReactiveEffect::StateUpdate(StateUpdate::slot(
                    self.address,
                    self.slot,
                    U256::from(1),
                )),
                ReactiveEffect::Resync(ResyncRequest {
                    id: ResyncId::new("slot-repair"),
                    reason: ResyncReason::HandlerRequested,
                    block: ResyncBlock::Hash {
                        number: 60,
                        hash: self.block_hash,
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

struct MixedResyncTargets {
    address: Address,
    slot_a: U256,
    slot_b: U256,
}

impl ReactiveHandler<Ethereum> for MixedResyncTargets {
    fn id(&self) -> HandlerId {
        HandlerId::new("mixed-resync")
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
                id: ResyncId::new("mixed-targets"),
                reason: ResyncReason::HandlerRequested,
                block: ResyncBlock::Number(61),
                targets: vec![
                    ResyncTarget::StorageSlots {
                        address: self.address,
                        slots: vec![self.slot_a, self.slot_b],
                    },
                    ResyncTarget::Account {
                        address: self.address,
                        fields: AccountFieldMask {
                            balance: true,
                            nonce: false,
                            code: false,
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

struct DuplicateSlotResyncs {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for DuplicateSlotResyncs {
    fn id(&self) -> HandlerId {
        HandlerId::new("duplicate-slot-resyncs")
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
                ReactiveEffect::Resync(ResyncRequest {
                    id: ResyncId::new("duplicate-a"),
                    reason: ResyncReason::HandlerRequested,
                    block: ResyncBlock::Number(62),
                    targets: vec![ResyncTarget::StorageSlot {
                        address: self.address,
                        slot: self.slot,
                    }],
                    priority: ResyncPriority::Normal,
                }),
                ReactiveEffect::Resync(ResyncRequest {
                    id: ResyncId::new("duplicate-b"),
                    reason: ResyncReason::HandlerRequested,
                    block: ResyncBlock::Number(62),
                    targets: vec![ResyncTarget::StorageSlots {
                        address: self.address,
                        slots: vec![self.slot],
                    }],
                    priority: ResyncPriority::Normal,
                }),
            ],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

#[tokio::test]
async fn reactive_runtime_executes_storage_resync_after_direct_effects() -> Result<()> {
    let address = Address::repeat_byte(0x91);
    let slot = U256::from(3);
    let block_hash = B256::repeat_byte(0x60);
    let seen_fetches = Arc::new(Mutex::new(Vec::new()));
    let mut cache = setup_cache().await?;
    cache.set_storage_batch_fetcher({
        let seen_fetches = seen_fetches.clone();
        Arc::new(move |requests, block| {
            seen_fetches.lock().unwrap().push((requests.clone(), block));
            requests
                .into_iter()
                .map(|(addr, slot)| (addr, slot, Ok(U256::from(42))))
                .collect()
        })
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(WriteThenResync {
        address,
        slot,
        block_hash,
    }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"Repair()")], 60)),
            included_context(60),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(42)),
        "authoritative resync value must overwrite direct handler effects"
    );

    let fetches = seen_fetches.lock().unwrap();
    assert_eq!(fetches.len(), 1);
    assert_eq!(fetches[0].0, vec![(address, slot)]);
    match &fetches[0].1 {
        BlockId::Hash(hash) => {
            assert_eq!(hash.block_hash, block_hash);
            assert_eq!(hash.require_canonical, Some(true));
        }
        other => panic!("expected canonical block hash fetch, got {other:?}"),
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
    assert_eq!(resynced[0].requested.len(), 1);
    assert_eq!(resynced[0].state_updates.len(), 1);
    assert_eq!(resynced[0].diff.slots.len(), 1);
    assert_eq!(resynced[0].diff.slots[0].old, U256::from(1));
    assert_eq!(resynced[0].diff.slots[0].new, U256::from(42));
    assert!(resynced[0].failed.is_empty());

    Ok(())
}

#[tokio::test]
async fn reactive_runtime_batches_resync_slots_and_reports_failed_targets() -> Result<()> {
    let address = Address::repeat_byte(0x92);
    let slot_a = U256::from(10);
    let slot_b = U256::from(11);
    let seen_fetches = Arc::new(Mutex::new(Vec::new()));
    let mut cache = setup_cache().await?;
    cache.set_storage_batch_fetcher({
        let seen_fetches = seen_fetches.clone();
        Arc::new(move |requests, block| {
            seen_fetches.lock().unwrap().push((requests.clone(), block));
            requests
                .into_iter()
                .map(|(addr, slot)| {
                    if slot == slot_b {
                        (
                            addr,
                            slot,
                            Err(StorageFetchError::custom("stub slot failure")),
                        )
                    } else {
                        (addr, slot, Ok(U256::from(777)))
                    }
                })
                .collect()
        })
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(MixedResyncTargets {
        address,
        slot_a,
        slot_b,
    }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"MixedRepair()")], 61)),
            included_context(61),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot_a),
        Some(U256::from(777))
    );
    assert_eq!(cache.cached_storage_value(address, slot_b), None);

    let fetches = seen_fetches.lock().unwrap();
    assert_eq!(fetches.len(), 1);
    assert_eq!(fetches[0].0, vec![(address, slot_a), (address, slot_b)]);
    assert_eq!(fetches[0].1, BlockId::number(61));

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert_eq!(resynced[0].requested.len(), 1);
    assert_eq!(resynced[0].state_updates.len(), 1);
    assert_eq!(
        resynced[0].state_updates[0],
        StateUpdate::slot(address, slot_a, U256::from(777))
    );
    assert_eq!(resynced[0].diff.slots.len(), 1);
    assert_eq!(resynced[0].failed.len(), 2);
    assert!(resynced[0].failed.iter().any(|failure| matches!(
        failure.target,
        ResyncTarget::StorageSlot { address: failed_address, slot: failed_slot }
            if failed_address == address && failed_slot == slot_b
    ) && failure.kind
        == ResyncFailureKind::StorageFetchFailed
        && failure.message.contains("stub slot failure")));
    // This cache is built via `setup_cache()` (a `new()` cache), so a real
    // account proof fetcher IS installed — it just fails against the offline
    // mock provider (here, because the test runs on a current-thread runtime the
    // fetcher's `block_in_place` bridge degrades to an error). The account target
    // therefore fails as `AccountFetchFailed`, not `MissingAccountFetcher`.
    assert!(resynced[0].failed.iter().any(|failure| matches!(
        failure.target,
        ResyncTarget::Account { .. }
    ) && failure.kind
        == ResyncFailureKind::AccountFetchFailed));

    Ok(())
}

/// A handler that emits only an `Account`-target resync (balance + nonce).
struct AccountOnlyResync {
    address: Address,
}

impl ReactiveHandler<Ethereum> for AccountOnlyResync {
    fn id(&self) -> HandlerId {
        HandlerId::new("account-only-resync")
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
                block: ResyncBlock::Number(61),
                targets: vec![ResyncTarget::Account {
                    address: self.address,
                    fields: AccountFieldMask {
                        balance: true,
                        nonce: true,
                        code: false,
                    },
                }],
                priority: ResyncPriority::Normal,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

/// WS-1a / Phase-8 s1 (manager-authored red-green): with an `AccountProofFetchFn`
/// installed, an `Account`-target resync now SUCCEEDS via the `eth_getProof` seam
/// — it no longer fails as `UnsupportedAccountTarget`. The fetched account fields
/// are applied through the cache (materialized, so a cold account is not silently
/// skipped) and no target fails.
#[tokio::test]
async fn account_target_resync_succeeds_via_account_proof_seam() -> Result<()> {
    let address = Address::repeat_byte(0x93);
    let seen: Arc<Mutex<ProofFetchCalls>> = Arc::new(Mutex::new(Vec::new()));
    let mut cache = setup_cache().await?;
    cache.set_account_proof_fetcher({
        let seen = seen.clone();
        Arc::new(move |requests: Vec<(Address, Vec<U256>)>, block: BlockId| {
            seen.lock().unwrap().push((requests.clone(), block));
            requests
                .into_iter()
                .map(|(addr, _keys)| {
                    (
                        addr,
                        Ok(AccountProof {
                            storage_hash: B256::ZERO,
                            balance: U256::from(999u64),
                            nonce: 7,
                            code_hash: B256::ZERO,
                            slots: vec![],
                        }),
                    )
                })
                .collect()
        })
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(AccountOnlyResync { address }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"AccountRepair()")], 61)),
            included_context(61),
        ),
    )?;

    // The seam was invoked for the account.
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(seen[0].0.iter().any(|(addr, _)| *addr == address));

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    // No target failed — the account target is now supported.
    assert!(
        resynced[0].failed.is_empty(),
        "account target must not fail once the seam is installed, got {:?}",
        resynced[0].failed
    );
    // An authoritative account update was built and applied to the cache.
    assert!(!resynced[0].state_updates.is_empty());
    assert!(
        resynced[0]
            .diff
            .accounts
            .iter()
            .any(|change| change.address == address),
        "the resynced account fields must be applied (materialized) to the cache"
    );
    assert_eq!(runtime.metrics().resync_failures, 0);
    Ok(())
}

/// With NO account proof fetcher installed, an `Account`-target resync fails with
/// `MissingAccountFetcher`. A `from_backend` cache captures no provider, so it
/// exposes no account proof fetcher (unlike a `new()` cache, whose real fetcher
/// would instead surface `AccountFetchFailed`).
#[tokio::test]
async fn account_target_resync_without_fetcher_reports_missing_account_fetcher() -> Result<()> {
    let address = Address::repeat_byte(0x94);
    let base = setup_cache().await?;
    let mut cache = EvmCache::from_backend(
        base.unchecked_backend().clone(),
        base.unchecked_blockchain_db().clone(),
        base.block(),
        base.chain_id(),
        None,
        None,
        SpecId::CANCUN,
    );
    assert!(
        cache.account_proof_fetcher().is_none(),
        "from_backend cache has no account proof fetcher"
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(AccountOnlyResync { address }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"AccountRepair()")], 61)),
            included_context(61),
        ),
    )?;

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert!(resynced[0].state_updates.is_empty());
    assert_eq!(resynced[0].failed.len(), 1);
    assert!(matches!(
        resynced[0].failed[0].target,
        ResyncTarget::Account { .. }
    ));
    assert_eq!(
        resynced[0].failed[0].kind,
        ResyncFailureKind::MissingAccountFetcher
    );
    assert_eq!(runtime.metrics().resync_failures, 1);
    Ok(())
}

/// An account proof fetcher that returns `Err` for the address produces an
/// `AccountFetchFailed` failure carrying the error message.
#[tokio::test]
async fn account_target_resync_fetch_error_reports_account_fetch_failed() -> Result<()> {
    let address = Address::repeat_byte(0x95);
    let mut cache = setup_cache().await?;
    cache.set_account_proof_fetcher(Arc::new(
        move |requests: Vec<(Address, Vec<U256>)>, _block: BlockId| {
            requests
                .into_iter()
                .map(|(addr, _keys)| (addr, Err(StorageFetchError::custom("stub account failure"))))
                .collect()
        },
    ));

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(AccountOnlyResync { address }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"AccountRepair()")], 61)),
            included_context(61),
        ),
    )?;

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert!(resynced[0].state_updates.is_empty());
    assert_eq!(resynced[0].failed.len(), 1);
    assert_eq!(
        resynced[0].failed[0].kind,
        ResyncFailureKind::AccountFetchFailed
    );
    assert!(
        resynced[0].failed[0]
            .message
            .contains("stub account failure")
    );
    assert_eq!(runtime.metrics().resync_failures, 1);
    Ok(())
}

/// An account proof fetcher that omits the requested address (returns no entry)
/// produces an `AccountFetchOmitted` failure.
#[tokio::test]
async fn account_target_resync_omitted_address_reports_account_fetch_omitted() -> Result<()> {
    let address = Address::repeat_byte(0x96);
    let mut cache = setup_cache().await?;
    cache.set_account_proof_fetcher(Arc::new(
        // Return no entries at all — the requested address is omitted.
        move |_requests: Vec<(Address, Vec<U256>)>, _block: BlockId| Vec::new(),
    ));

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(AccountOnlyResync { address }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"AccountRepair()")], 61)),
            included_context(61),
        ),
    )?;

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert!(resynced[0].state_updates.is_empty());
    assert_eq!(resynced[0].failed.len(), 1);
    assert_eq!(
        resynced[0].failed[0].kind,
        ResyncFailureKind::AccountFetchOmitted
    );
    assert_eq!(runtime.metrics().resync_failures, 1);
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_fans_out_duplicate_resync_failures_to_all_request_origins() -> Result<()>
{
    let address = Address::repeat_byte(0x93);
    let slot = U256::from(12);
    let seen_fetches = Arc::new(Mutex::new(Vec::new()));
    let mut cache = setup_cache().await?;
    cache.set_storage_batch_fetcher({
        let seen_fetches = seen_fetches.clone();
        Arc::new(move |requests, block| {
            seen_fetches.lock().unwrap().push((requests.clone(), block));
            requests
                .into_iter()
                .map(|(addr, slot)| {
                    (
                        addr,
                        slot,
                        Err(StorageFetchError::custom("shared fetch failure")),
                    )
                })
                .collect()
        })
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(DuplicateSlotResyncs { address, slot }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, vec![keccak256(b"DuplicateRepair()")], 62)),
            included_context(62),
        ),
    )?;

    let fetches = seen_fetches.lock().unwrap();
    assert_eq!(
        fetches.len(),
        1,
        "duplicate storage targets should share one provider fetch"
    );
    assert_eq!(fetches[0].0, vec![(address, slot)]);

    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert!(resynced[0].state_updates.is_empty());
    assert_eq!(
        resynced[0].failed.len(),
        2,
        "each originating request must get an explicit failure"
    );
    let failed_ids: Vec<_> = resynced[0]
        .failed
        .iter()
        .map(|failure| failure.request_id.clone())
        .collect();
    assert!(failed_ids.contains(&ResyncId::new("duplicate-a")));
    assert!(failed_ids.contains(&ResyncId::new("duplicate-b")));
    assert!(resynced[0].failed.iter().all(|failure| {
        failure.kind == ResyncFailureKind::StorageFetchFailed
            && failure.message.contains("shared fetch failure")
    }));

    Ok(())
}

/// A handler that emits ONE resync request carrying TWO account targets.
struct TwoAccountResync {
    first: Address,
    second: Address,
}

impl ReactiveHandler<Ethereum> for TwoAccountResync {
    fn id(&self) -> HandlerId {
        HandlerId::new("two-account-resync")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.first),
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
        let fields = AccountFieldMask {
            balance: true,
            nonce: true,
            code: false,
        };
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::Resync(ResyncRequest {
                id: ResyncId::new("two-account-repair"),
                reason: ResyncReason::HandlerRequested,
                block: ResyncBlock::Number(61),
                targets: vec![
                    ResyncTarget::Account {
                        address: self.first,
                        fields,
                    },
                    ResyncTarget::Account {
                        address: self.second,
                        fields,
                    },
                ],
                priority: ResyncPriority::Normal,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

/// §6.1 (spec item 13, resync side): multiple account targets pinned to the
/// same block resolve through ONE seam invocation carrying both addresses —
/// not one `eth_getProof` seam call per target.
#[tokio::test]
async fn account_target_resync_batches_targets_in_one_seam_call() -> Result<()> {
    let first = Address::repeat_byte(0x95);
    let second = Address::repeat_byte(0x96);
    let seen: Arc<Mutex<ProofFetchCalls>> = Arc::new(Mutex::new(Vec::new()));
    let mut cache = setup_cache().await?;
    cache.set_account_proof_fetcher({
        let seen = seen.clone();
        Arc::new(move |requests: Vec<(Address, Vec<U256>)>, block: BlockId| {
            seen.lock().unwrap().push((requests.clone(), block));
            requests
                .into_iter()
                .map(|(addr, _keys)| {
                    (
                        addr,
                        Ok(AccountProof {
                            storage_hash: B256::ZERO,
                            balance: U256::from(123u64),
                            nonce: 1,
                            code_hash: B256::ZERO,
                            slots: vec![],
                        }),
                    )
                })
                .collect()
        })
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(TwoAccountResync { first, second }))?;

    let report = runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(first, vec![keccak256(b"TwoAccountRepair()")], 61)),
            included_context(61),
        ),
    )?;

    // ONE invocation, carrying BOTH targets.
    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.len(),
        1,
        "same-block account targets must share one seam invocation"
    );
    assert_eq!(seen[0].0.len(), 2);
    assert!(seen[0].0.iter().any(|(addr, _)| *addr == first));
    assert!(seen[0].0.iter().any(|(addr, _)| *addr == second));

    // Both targets succeeded.
    let resynced: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::Resynced(report) => Some(report),
            _ => None,
        })
        .collect();
    assert_eq!(resynced.len(), 1);
    assert!(
        resynced[0].failed.is_empty(),
        "both batched account targets must resolve, got {:?}",
        resynced[0].failed
    );
    Ok(())
}
