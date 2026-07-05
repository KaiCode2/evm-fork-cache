//! Manager-authored red-green acceptance tests for Phase-8 step 4: the
//! `storageHash` root gate.
//!
//! A `WholeAccount`-tracked contract's storage root is a sound per-account change
//! oracle. Each canonical block the runtime probes tracked accounts' roots via the
//! account-proof seam and compares them to the baseline it adopted. If a tracked
//! account's root MOVED but no decoder touched it this block, that is a coverage
//! gap: state changed through a path no decoder covers. The runtime emits a
//! `ReactiveReport::CoverageGap`, schedules a `ResyncReason::RootMoved` repair, and
//! counts it — turning the decoder blind spot into an explicit, cheap signal. When
//! the root is unchanged, there is no gap and no resync.
//!
//! Fully offline: the account-proof seam is stubbed with an in-memory table keyed
//! on the probed block, so no test reaches the network.
#![cfg(feature = "reactive")]

mod common;

use std::sync::Arc;

use alloy_consensus::Header;
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, U256};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::cache::AccountProof;
use evm_fork_cache::reactive::{
    ChainStatus, InputSource, ReactiveConfig, ReactiveContext, ReactiveInput, ReactiveInputBatch,
    ReactiveInputRecord, ReactiveReport, ReactiveRuntime, ResyncReason, RootGateCadence,
    TrackingPolicy,
};

/// A canonical block-header input for block `number`.
fn header_input(number: u64) -> ReactiveInput<Ethereum> {
    let consensus = Header {
        number,
        timestamp: 1_700_000_000 + number,
        base_fee_per_gas: Some(7),
        beneficiary: Address::repeat_byte(0xcb),
        gas_limit: 30_000_000,
        mix_hash: B256::repeat_byte(0xab),
        ..Default::default()
    };
    ReactiveInput::BlockHeader(alloy_rpc_types_eth::Header::new(consensus))
}

fn canonical_context(number: u64) -> ReactiveContext {
    let block = evm_fork_cache::reactive::BlockRef {
        number,
        hash: B256::repeat_byte(number as u8),
        parent_hash: Some(B256::repeat_byte((number.saturating_sub(1)) as u8)),
        timestamp: Some(1_700_000_000 + number),
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

fn header_batch(number: u64) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        header_input(number),
        canonical_context(number),
    )])
}

/// Install an account-proof fetcher whose `storage_hash` for any address is a
/// function of the probed block: blocks `<= pivot` return `root_a`, blocks
/// `> pivot` return `root_b`. Deterministic regardless of probe cadence.
fn install_block_keyed_root_fetcher(
    cache: &mut evm_fork_cache::cache::EvmCache,
    pivot: u64,
    root_a: B256,
    root_b: B256,
) {
    use alloy_eips::BlockId;
    cache.set_account_proof_fetcher(Arc::new(
        move |requests: Vec<(Address, Vec<U256>)>, block: BlockId| {
            let probed = match block {
                BlockId::Number(n) => n.as_number().unwrap_or(u64::MAX),
                _ => u64::MAX,
            };
            let root = if probed <= pivot { root_a } else { root_b };
            requests
                .into_iter()
                .map(|(addr, _keys)| {
                    (
                        addr,
                        Ok(AccountProof {
                            storage_hash: root,
                            balance: U256::ZERO,
                            nonce: 0,
                            code_hash: B256::ZERO,
                            slots: vec![],
                        }),
                    )
                })
                .collect()
        },
    ));
}

/// Phase-8 s4: a `WholeAccount`-tracked account whose root MOVES on a block that
/// touched no decoder emits a `CoverageGap` report + a `RootMoved` resync, and
/// increments the `coverage_gaps` counter.
#[tokio::test]
async fn whole_account_root_move_untouched_emits_coverage_gap_and_resync() -> Result<()> {
    let tracked = Address::repeat_byte(0x77);
    let mut cache = setup_cache().await?;
    // Baseline root through block 10; a moved root from block 11 onward.
    install_block_keyed_root_fetcher(
        &mut cache,
        10,
        B256::repeat_byte(0xa1),
        B256::repeat_byte(0xb2),
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    // These tests exercise per-block gate semantics on consecutive blocks.
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(1));
    runtime.track_account(tracked, TrackingPolicy::WholeAccount);

    // Block 10: adopt the baseline root (no decoder touches `tracked`).
    let report10 = runtime.ingest_batch_with_resync(&mut cache, header_batch(10))?;
    assert!(
        !report10
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::CoverageGap(_))),
        "adopting the baseline is not a coverage gap"
    );

    // Block 11: the tracked account's root moved but nothing touched it.
    let report11 = runtime.ingest_batch_with_resync(&mut cache, header_batch(11))?;

    let gap = report11
        .reports
        .iter()
        .find_map(|r| match r.as_ref() {
            ReactiveReport::CoverageGap(report) => Some(report),
            _ => None,
        })
        .expect("a moved root with no covering decoder must emit a CoverageGap");
    assert_eq!(gap.address, tracked, "the gap names the tracked account");

    // A RootMoved resync was scheduled for the tracked account.
    let root_moved = report11
        .resyncs
        .iter()
        .any(|req| req.reason == ResyncReason::RootMoved);
    assert!(
        root_moved,
        "a moved root must schedule a RootMoved resync, got {:?}",
        report11.resyncs
    );

    assert_eq!(runtime.metrics().coverage_gaps, 1);
    Ok(())
}

/// Phase-8 s4: when a tracked account's root is unchanged, there is no coverage
/// gap and no resync — the tight, cheap steady-state path.
#[tokio::test]
async fn whole_account_root_unchanged_no_gap_or_resync() -> Result<()> {
    let tracked = Address::repeat_byte(0x78);
    let mut cache = setup_cache().await?;
    // Same root for all blocks (pivot far in the future).
    install_block_keyed_root_fetcher(
        &mut cache,
        u64::MAX,
        B256::repeat_byte(0xa1),
        B256::repeat_byte(0xb2),
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    // These tests exercise per-block gate semantics on consecutive blocks.
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(1));
    runtime.track_account(tracked, TrackingPolicy::WholeAccount);

    runtime.ingest_batch_with_resync(&mut cache, header_batch(20))?;
    let report = runtime.ingest_batch_with_resync(&mut cache, header_batch(21))?;

    assert!(
        !report
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::CoverageGap(_))),
        "an unchanged root is not a coverage gap"
    );
    assert!(
        !report
            .resyncs
            .iter()
            .any(|req| req.reason == ResyncReason::RootMoved),
        "an unchanged root schedules no RootMoved resync"
    );
    assert_eq!(runtime.metrics().coverage_gaps, 0);
    Ok(())
}

// ----------------------------------------------------------------------------
// Wave-7 (implementation-agent) tests: decoder-covered move, Scalars field move,
// and the Slots opt-out.
// ----------------------------------------------------------------------------

use alloy_primitives::{Bytes, Log as PrimitiveLog};
use alloy_rpc_types_eth::{Filter, Log};
use evm_fork_cache::StateUpdate;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    HandlerError, HandlerId, HandlerOutcome, LogInterest, ReactiveEffect, ReactiveHandler,
    ReactiveInterest, RouteKeySpec, StateEffectQuality,
};

/// A handler that writes a single absolute storage slot on `address` for every
/// matching log — enough to place `address` in the batch's touched set.
struct WriteSlotOnLog {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for WriteSlotOnLog {
    fn id(&self) -> HandlerId {
        HandlerId::new("write-slot-on-log")
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
        // Write the block number as the slot value so each block produces a real
        // `SlotChange` (a repeated identical write records no change, which would
        // leave the account out of the touched set).
        let value = U256::from(ctx.block.as_ref().map(|b| b.number).unwrap_or_default());
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                value,
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

/// A canonical log input emitted by `address` at `number`.
fn log_batch(address: Address, number: u64) -> ReactiveInputBatch<Ethereum> {
    let log = Log {
        inner: PrimitiveLog::new_unchecked(address, vec![B256::repeat_byte(0xee)], Bytes::new()),
        block_hash: Some(B256::repeat_byte(number as u8)),
        block_number: Some(number),
        block_timestamp: Some(1_700_000_000 + number),
        transaction_hash: Some(B256::repeat_byte(0x44)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    };
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::Log(log),
        canonical_context(number),
    )])
}

/// Phase-8 s4: a `WholeAccount`-tracked account whose root moved but which a
/// decoder wrote this block (addr ∈ touched) is covered — no `CoverageGap`, no
/// `RootMoved` resync, and the counter stays put.
#[tokio::test]
async fn whole_account_root_move_touched_no_coverage_gap() -> Result<()> {
    let tracked = Address::repeat_byte(0x79);
    let mut cache = setup_cache().await?;
    // Baseline root through block 10; a moved root from block 11 onward.
    install_block_keyed_root_fetcher(
        &mut cache,
        10,
        B256::repeat_byte(0xa1),
        B256::repeat_byte(0xb2),
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    // These tests exercise per-block gate semantics on consecutive blocks.
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(1));
    // A decoder that writes a slot on `tracked` for every log it emits.
    runtime.register_handler(Arc::new(WriteSlotOnLog {
        address: tracked,
        slot: U256::from(7),
    }))?;
    runtime.track_account(tracked, TrackingPolicy::WholeAccount);

    // Block 10: adopt the baseline (the log write covers the account, but there is
    // no baseline yet, so adoption — not a gap — is the outcome regardless).
    runtime.ingest_batch_with_resync(&mut cache, log_batch(tracked, 10))?;

    // Block 11: the root moved AND a decoder wrote `tracked` this block.
    let report = runtime.ingest_batch_with_resync(&mut cache, log_batch(tracked, 11))?;

    assert!(
        !report
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::CoverageGap(_))),
        "a decoder covered the move; there is no coverage gap"
    );
    assert!(
        !report
            .resyncs
            .iter()
            .any(|req| req.reason == ResyncReason::RootMoved),
        "a decoder-covered move schedules no RootMoved resync, got {:?}",
        report.resyncs
    );
    assert_eq!(runtime.metrics().coverage_gaps, 0);
    Ok(())
}

/// Install an account-proof fetcher whose balance for any address is a function
/// of the probed block: blocks `<= pivot` return `balance_a`, later blocks return
/// `balance_b`. The storage root is held constant (Scalars does not root-gate).
fn install_block_keyed_balance_fetcher(
    cache: &mut evm_fork_cache::cache::EvmCache,
    pivot: u64,
    root: B256,
    balance_a: U256,
    balance_b: U256,
) {
    use alloy_eips::BlockId;
    cache.set_account_proof_fetcher(Arc::new(
        move |requests: Vec<(Address, Vec<U256>)>, block: BlockId| {
            let probed = match block {
                BlockId::Number(n) => n.as_number().unwrap_or(u64::MAX),
                _ => u64::MAX,
            };
            let balance = if probed <= pivot {
                balance_a
            } else {
                balance_b
            };
            requests
                .into_iter()
                .map(|(addr, _keys)| {
                    (
                        addr,
                        Ok(AccountProof {
                            storage_hash: root,
                            balance,
                            nonce: 0,
                            code_hash: B256::ZERO,
                            slots: vec![],
                        }),
                    )
                })
                .collect()
        },
    ));
}

/// Phase-8 s4: a `Scalars`-tracked account whose native balance moves (baseline
/// vs probe, storage root held constant) schedules an account-field resync — the
/// balance/nonce freshness path. Root is irrelevant for `Scalars`.
#[tokio::test]
async fn scalars_balance_move_schedules_account_resync() -> Result<()> {
    let tracked = Address::repeat_byte(0x7a);
    let mut cache = setup_cache().await?;
    // Root constant across all blocks; balance moves after block 10.
    install_block_keyed_balance_fetcher(
        &mut cache,
        10,
        B256::repeat_byte(0xa1),
        U256::from(100),
        U256::from(200),
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    // These tests exercise per-block gate semantics on consecutive blocks.
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(1));
    runtime.track_account(tracked, TrackingPolicy::Scalars);

    // Block 10: adopt the baseline balance (100).
    let report10 = runtime.ingest_batch_with_resync(&mut cache, header_batch(10))?;
    assert!(
        !report10
            .resyncs
            .iter()
            .any(|req| req.reason == ResyncReason::RootMoved),
        "adopting the Scalars baseline schedules no resync"
    );

    // Block 11: the balance moved (100 -> 200) with the root unchanged.
    let report11 = runtime.ingest_batch_with_resync(&mut cache, header_batch(11))?;

    let resync = report11
        .resyncs
        .iter()
        .find(|req| req.reason == ResyncReason::RootMoved)
        .expect("a moved balance must schedule an account-field resync");
    let balances_target = resync.targets.iter().any(|target| {
        matches!(
            target,
            evm_fork_cache::reactive::ResyncTarget::Account { address, fields }
                if *address == tracked && fields.balance
        )
    });
    assert!(
        balances_target,
        "the Scalars resync targets the account's balance field, got {:?}",
        resync.targets
    );
    Ok(())
}

/// Phase-8 s4 / spec Decision 3: a `Slots`-tracked account is never root-gated —
/// even when its storage root moves, no `CoverageGap` is emitted and no
/// `RootMoved` resync is scheduled.
#[tokio::test]
async fn slots_policy_is_never_root_gated() -> Result<()> {
    let tracked = Address::repeat_byte(0x7b);
    let mut cache = setup_cache().await?;
    // Root moves after block 10 — which a Slots policy must ignore.
    install_block_keyed_root_fetcher(
        &mut cache,
        10,
        B256::repeat_byte(0xa1),
        B256::repeat_byte(0xb2),
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    // These tests exercise per-block gate semantics on consecutive blocks.
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(1));
    runtime.track_account(
        tracked,
        TrackingPolicy::Slots {
            slots: vec![U256::from(1), U256::from(2)],
        },
    );

    runtime.ingest_batch_with_resync(&mut cache, header_batch(10))?;
    let report = runtime.ingest_batch_with_resync(&mut cache, header_batch(11))?;

    assert!(
        !report
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::CoverageGap(_))),
        "a Slots-tracked account is never root-gated: no coverage gap"
    );
    assert!(
        !report
            .resyncs
            .iter()
            .any(|req| req.reason == ResyncReason::RootMoved),
        "a Slots-tracked account is never root-gated: no RootMoved resync"
    );
    assert_eq!(runtime.metrics().coverage_gaps, 0);
    Ok(())
}

/// §6.1 (spec item 13): the root gate probes ALL root-gated targets through
/// ONE seam invocation per firing — the fetcher can then fan the requests out
/// concurrently — instead of one invocation per tracked account.
#[tokio::test]
async fn root_gate_issues_one_batched_seam_invocation_per_firing() -> Result<()> {
    use std::sync::Mutex;

    let mut cache = setup_cache().await?;
    // Record (batch size) per seam invocation; every probe returns a stable
    // root so no gaps/resyncs distract the assertion.
    let invocations: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    cache.set_account_proof_fetcher({
        let invocations = invocations.clone();
        Arc::new(
            move |requests: Vec<(Address, Vec<U256>)>, _block: alloy_eips::BlockId| {
                invocations.lock().unwrap().push(requests.len());
                requests
                    .into_iter()
                    .map(|(addr, _keys)| {
                        (
                            addr,
                            Ok(AccountProof {
                                storage_hash: B256::repeat_byte(0x11),
                                balance: U256::ZERO,
                                nonce: 0,
                                code_hash: B256::ZERO,
                                slots: vec![],
                            }),
                        )
                    })
                    .collect()
            },
        )
    });

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    // These tests exercise per-block gate semantics on consecutive blocks.
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(1));
    // Three WholeAccount + one Scalars = four root-gated targets (Slots never
    // gates).
    runtime.track_account(Address::repeat_byte(0x01), TrackingPolicy::WholeAccount);
    runtime.track_account(Address::repeat_byte(0x02), TrackingPolicy::WholeAccount);
    runtime.track_account(Address::repeat_byte(0x03), TrackingPolicy::WholeAccount);
    runtime.track_account(Address::repeat_byte(0x04), TrackingPolicy::Scalars);
    runtime.track_account(
        Address::repeat_byte(0x05),
        TrackingPolicy::Slots {
            slots: vec![U256::ZERO],
        },
    );

    runtime.ingest_batch_with_resync(&mut cache, header_batch(10))?;
    assert_eq!(
        invocations.lock().unwrap().clone(),
        vec![4],
        "one firing = one seam invocation carrying every root-gated target"
    );

    runtime.ingest_batch_with_resync(&mut cache, header_batch(11))?;
    assert_eq!(
        invocations.lock().unwrap().clone(),
        vec![4, 4],
        "each firing batches all targets; none are probed individually"
    );
    Ok(())
}

/// §6.1 (spec item 15, RPC-gated): a ~50-address probe through the DEFAULT
/// proof fetcher completes in far less than 50 × single-proof latency,
/// demonstrating the bounded concurrent fan-out. Needs a live endpoint:
/// `E2E_RPC_URL=... cargo test --test liveness_root_gate -- --ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs E2E_RPC_URL (live provider); latency bound is environment-dependent"]
async fn default_proof_fetcher_fans_out_concurrently() -> Result<()> {
    use alloy_rpc_client::RpcClient;
    use alloy_transport_http::Http;
    use evm_fork_cache::cache::EvmCache;

    let Ok(rpc_url) = std::env::var("E2E_RPC_URL") else {
        eprintln!("E2E_RPC_URL not set; skipping");
        return Ok(());
    };
    let client = reqwest::Client::builder().gzip(true).build()?;
    let http = Http::with_client(client, rpc_url.parse()?);
    let provider = Arc::new(alloy_provider::RootProvider::<
        alloy_provider::network::AnyNetwork,
    >::new(RpcClient::new(http, false)));

    let cache = EvmCache::builder(provider).build().await;
    let fetcher = cache
        .account_proof_fetcher()
        .expect("provider-backed cache has a default proof fetcher")
        .clone();
    let block = cache.block();

    // 50 distinct addresses (existence is irrelevant: absent accounts also
    // return proofs).
    let batch: Vec<(Address, Vec<U256>)> = (1..=50u8)
        .map(|i| (Address::repeat_byte(i), vec![]))
        .collect();

    let single_start = std::time::Instant::now();
    let single = (fetcher)(vec![(Address::repeat_byte(0xee), vec![])], block);
    let single_elapsed = single_start.elapsed();
    assert_eq!(single.len(), 1);

    let batch_start = std::time::Instant::now();
    let results = (fetcher)(batch, block);
    let batch_elapsed = batch_start.elapsed();
    assert_eq!(results.len(), 50);

    eprintln!("single proof: {single_elapsed:?}; 50-address batch: {batch_elapsed:?}");
    assert!(
        batch_elapsed < single_elapsed * 25,
        "a 50-address batch must complete well under 50x a single proof \
         (got batch={batch_elapsed:?}, single={single_elapsed:?})"
    );
    Ok(())
}

/// §6.2 (spec item 11): the gate fires on the first canonical block ever
/// seen, then only on cadence boundaries; `Disabled` never invokes the seam;
/// `every_n_blocks(1)` reproduces per-block probing.
#[tokio::test]
async fn root_gate_fires_on_cadence_boundaries_only() -> Result<()> {
    use std::sync::Mutex;

    async fn firings_for(cadence: RootGateCadence, blocks: &[u64]) -> Result<Vec<usize>> {
        let mut cache = setup_cache().await?;
        let invocations: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        cache.set_account_proof_fetcher({
            let invocations = invocations.clone();
            Arc::new(
                move |requests: Vec<(Address, Vec<U256>)>, _block: alloy_eips::BlockId| {
                    invocations.lock().unwrap().push(requests.len());
                    requests
                        .into_iter()
                        .map(|(addr, _keys)| {
                            (
                                addr,
                                Ok(AccountProof {
                                    storage_hash: B256::repeat_byte(0x11),
                                    balance: U256::ZERO,
                                    nonce: 0,
                                    code_hash: B256::ZERO,
                                    slots: vec![],
                                }),
                            )
                        })
                        .collect()
                },
            )
        });
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.set_root_gate_cadence(cadence);
        runtime.track_account(Address::repeat_byte(0x31), TrackingPolicy::WholeAccount);
        for &block in blocks {
            runtime.ingest_batch_with_resync(&mut cache, header_batch(block))?;
        }
        let invocations = invocations.lock().unwrap().clone();
        Ok(invocations)
    }

    // Cadence 4 over blocks 10..=14: fires at 10 (first canonical block ever
    // seen) and at 14 (>= 10 + 4); 11, 12, 13 are skipped.
    assert_eq!(
        firings_for(RootGateCadence::every_n_blocks(4), &[10, 11, 12, 13, 14]).await?,
        vec![1, 1],
        "cadence 4 fires exactly at the first block and the boundary"
    );

    // Per-block cadence reproduces the old behavior: every block probes.
    assert_eq!(
        firings_for(RootGateCadence::every_n_blocks(1), &[10, 11, 12]).await?,
        vec![1, 1, 1]
    );

    // Disabled never consults the seam.
    assert_eq!(
        firings_for(RootGateCadence::Disabled, &[10, 11, 12]).await?,
        Vec::<usize>::new()
    );
    Ok(())
}

/// §6.2 (spec item 12): the decoder-touched set accumulates across skipped
/// blocks — a covered write in a skipped block does NOT report a
/// `CoverageGap` at the next firing — and the accumulator drains per firing,
/// so a later uncovered move still does.
#[tokio::test]
async fn touched_accumulates_across_skipped_blocks_and_drains_per_firing() -> Result<()> {
    let tracked = Address::repeat_byte(0x7c);
    let mut cache = setup_cache().await?;
    // Three-phase roots keyed on the probed block: baseline through 10, a
    // first move visible at the block-14 firing, a second at the block-18 one.
    cache.set_account_proof_fetcher(Arc::new(
        move |requests: Vec<(Address, Vec<U256>)>, block: alloy_eips::BlockId| {
            let probed = match block {
                alloy_eips::BlockId::Number(n) => n.as_number().unwrap_or(u64::MAX),
                _ => u64::MAX,
            };
            let root = if probed <= 10 {
                B256::repeat_byte(0xa1)
            } else if probed <= 14 {
                B256::repeat_byte(0xb2)
            } else {
                B256::repeat_byte(0xc3)
            };
            requests
                .into_iter()
                .map(|(addr, _keys)| {
                    (
                        addr,
                        Ok(AccountProof {
                            storage_hash: root,
                            balance: U256::ZERO,
                            nonce: 0,
                            code_hash: B256::ZERO,
                            slots: vec![],
                        }),
                    )
                })
                .collect()
        },
    ));

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(4));
    runtime.register_handler(Arc::new(WriteSlotOnLog {
        address: tracked,
        slot: U256::from(7u64),
    }))?;
    runtime.track_account(tracked, TrackingPolicy::WholeAccount);

    // Block 10: first firing adopts the baseline.
    runtime.ingest_batch_with_resync(&mut cache, header_batch(10))?;

    // Block 12 (skipped window): a decoder covers a write on the tracked
    // account. The gate does not fire here; the touch must be remembered.
    runtime.ingest_batch_with_resync(&mut cache, log_batch(tracked, 12))?;

    // Block 14: the gate fires; the root moved, but the accumulated touched
    // set covers it — no gap, re-adopt.
    let report14 = runtime.ingest_batch_with_resync(&mut cache, header_batch(14))?;
    assert!(
        !report14
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::CoverageGap(_))),
        "a decoder-covered write in a skipped block must not gap at the next firing"
    );
    assert_eq!(runtime.metrics().coverage_gaps, 0);

    // Block 18: the gate fires again; the root moved again with NO covering
    // touch in this window (the accumulator drained at 14) — a genuine gap.
    let report18 = runtime.ingest_batch_with_resync(&mut cache, header_batch(18))?;
    let gap = report18
        .reports
        .iter()
        .find_map(|r| match r.as_ref() {
            ReactiveReport::CoverageGap(report) => Some(report),
            _ => None,
        })
        .expect("an uncovered move after the accumulator drained must gap");
    assert_eq!(gap.address, tracked);
    assert!(
        report18
            .resyncs
            .iter()
            .any(|req| req.reason == ResyncReason::RootMoved)
    );
    assert_eq!(runtime.metrics().coverage_gaps, 1);
    Ok(())
}

/// §6.2 (spec item 14): a root move occurring inside a skipped window is
/// detected at the next firing — cadence delays detection (bounded by the
/// window), it never loses it. The gate diffs against the persisted baseline,
/// not block-over-block.
#[tokio::test]
async fn root_move_in_skipped_window_is_detected_at_next_firing() -> Result<()> {
    let tracked = Address::repeat_byte(0x7d);
    let mut cache = setup_cache().await?;
    // The root moves at block 12 — strictly inside the skipped window
    // (firings happen at 10 and 14).
    install_block_keyed_root_fetcher(
        &mut cache,
        11,
        B256::repeat_byte(0xa1),
        B256::repeat_byte(0xb2),
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.set_root_gate_cadence(RootGateCadence::every_n_blocks(4));
    runtime.track_account(tracked, TrackingPolicy::WholeAccount);

    runtime.ingest_batch_with_resync(&mut cache, header_batch(10))?;
    // Skipped blocks: no probes, no reports.
    let report12 = runtime.ingest_batch_with_resync(&mut cache, header_batch(12))?;
    assert!(
        !report12
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::CoverageGap(_))),
        "no probe happens inside the skipped window"
    );

    // The next firing sees the moved root and reports the gap: detection was
    // delayed from block 12 to block 14, never lost.
    let report14 = runtime.ingest_batch_with_resync(&mut cache, header_batch(14))?;
    let gap = report14
        .reports
        .iter()
        .find_map(|r| match r.as_ref() {
            ReactiveReport::CoverageGap(report) => Some(report),
            _ => None,
        })
        .expect("a move inside a skipped window must be detected at the next firing");
    assert_eq!(gap.address, tracked);
    assert_eq!(runtime.metrics().coverage_gaps, 1);
    Ok(())
}
