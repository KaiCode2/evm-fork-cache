//! Bulk storage extraction: offline correctness tests.
//!
//! Two layers, both fully offline:
//!
//! 1. **EVM-level dogfood tests** — the exact bytes this crate sends over RPC
//!    (extractor bytecode, Multicall3 `aggregate3` calldata, state-override
//!    code) are executed inside the crate's own revm-backed cache against
//!    seeded storage, proving the hand-written bytecode and the
//!    encode/decode round-trip behave exactly as the RPC path assumes.
//! 2. **Mocked-transport fetcher tests** — the [`StorageBatchFetchFn`]
//!    contract (one result per requested pair, per-slot error mapping,
//!    fallback repair, runtime guards) over an [`Asserter`]-mocked provider.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::SolCall;
use alloy_transport::mock::Asserter;
use anyhow::{Result, anyhow};
use evm_fork_cache::bulk_storage::{
    ACCOUNT_FIELDS_EXTRACTOR_CODE, BLOCK_CONTEXT_EXTRACTOR_CODE, BulkCallConfig, BulkFetcherStatus,
    CallDispatch, STORAGE_EXTRACTOR_CODE, STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI, StorageProgram,
    bulk_call_storage_fetcher, bulk_call_storage_fetcher_with_fallback,
    bulk_call_storage_fetcher_with_status, decode_multi_target_response, decode_packed_values,
    encode_multi_target_calldata, multicall3_runtime_code, pack_slots_calldata,
};
use evm_fork_cache::cache::{EvmCache, StorageBatchFetchFn};
use evm_fork_cache::multicall::{IMulticall3, MULTICALL3_ADDRESS};
use revm::context::result::ExecutionResult;
use revm::state::{AccountInfo, Bytecode};

use common::{install_default_account, setup_cache};

const CALLER: Address = Address::ZERO;

fn target(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

/// Install `code` at `addr` with the given seeded storage; unseeded slots read
/// as zero (never falling through to the mocked RPC), mirroring a fully-known
/// forked contract.
fn install_code_with_storage(
    cache: &mut EvmCache,
    addr: Address,
    code: Bytes,
    slots: &[(U256, U256)],
) {
    let bytecode = Bytecode::new_raw(code);
    let code_hash = bytecode.hash_slow();
    let info = AccountInfo {
        balance: U256::ZERO,
        nonce: 0,
        code: Some(bytecode),
        code_hash,
        account_id: None,
    };
    cache.db_mut().insert_account_info(addr, info);
    cache
        .db_mut()
        .replace_account_storage(addr, Default::default())
        .expect("mark storage as fully local");
    for (slot, value) in slots {
        cache
            .db_mut()
            .insert_account_storage(addr, *slot, *value)
            .expect("seed storage slot");
    }
}

fn run_call(cache: &mut EvmCache, to: Address, calldata: Bytes) -> Result<Bytes> {
    match cache.call_raw(CALLER, to, calldata, false)? {
        ExecutionResult::Success { output, .. } => Ok(output.into_data()),
        other => Err(anyhow!("extractor call did not succeed: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// EVM-level dogfood tests: run the exact override bytecode inside revm.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn extractor_bytecode_reads_seeded_and_absent_slots() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, CALLER);

    let token = target(0xaa);
    let seeded = [
        (U256::from(0u64), U256::from(0x1111u64)),
        (U256::from(3u64), U256::MAX),
        (U256::from(2u64).pow(U256::from(200u64)), U256::from(42u64)),
    ];
    install_code_with_storage(
        &mut cache,
        token,
        Bytes::from_static(STORAGE_EXTRACTOR_CODE),
        &seeded,
    );

    // Query the three seeded slots, one absent slot, and a duplicate.
    let query = vec![
        seeded[0].0,
        seeded[1].0,
        seeded[2].0,
        U256::from(999u64),
        seeded[1].0,
    ];
    let out = run_call(&mut cache, token, pack_slots_calldata(&query))?;
    let values = decode_packed_values(&out, query.len()).expect("exact word count");
    assert_eq!(
        values,
        vec![
            seeded[0].1,
            seeded[1].1,
            seeded[2].1,
            U256::ZERO,
            seeded[1].1
        ],
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn extractor_bytecode_returns_empty_for_empty_calldata() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, CALLER);
    let token = target(0xab);
    install_code_with_storage(
        &mut cache,
        token,
        Bytes::from_static(STORAGE_EXTRACTOR_CODE),
        &[],
    );
    let out = run_call(&mut cache, token, Bytes::new())?;
    assert!(out.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn pre_shanghai_extractor_matches_default_variant() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, CALLER);

    let seeded = [
        (U256::from(1u64), U256::from(0xdeadbeefu64)),
        (U256::from(7u64), U256::from(1u64) << 255),
    ];
    let (shanghai, legacy) = (target(0xa1), target(0xa2));
    install_code_with_storage(
        &mut cache,
        shanghai,
        Bytes::from_static(STORAGE_EXTRACTOR_CODE),
        &seeded,
    );
    install_code_with_storage(
        &mut cache,
        legacy,
        Bytes::from_static(STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI),
        &seeded,
    );

    let query = vec![seeded[0].0, U256::from(5u64), seeded[1].0];
    let calldata = pack_slots_calldata(&query);
    let out_shanghai = run_call(&mut cache, shanghai, calldata.clone())?;
    let out_legacy = run_call(&mut cache, legacy, calldata)?;
    assert_eq!(out_shanghai, out_legacy);
    assert_eq!(
        decode_packed_values(&out_legacy, 3).expect("exact word count"),
        vec![seeded[0].1, U256::ZERO, seeded[1].1],
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn multicall3_dispatch_extracts_across_targets() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, CALLER);

    // The dispatcher, exactly as the RPC path overrides it.
    install_code_with_storage(
        &mut cache,
        MULTICALL3_ADDRESS,
        multicall3_runtime_code().clone(),
        &[],
    );

    let a = target(0x11);
    let b = target(0x22);
    install_code_with_storage(
        &mut cache,
        a,
        Bytes::from_static(STORAGE_EXTRACTOR_CODE),
        &[
            (U256::from(0u64), U256::from(101u64)),
            (U256::from(1u64), U256::from(102u64)),
        ],
    );
    install_code_with_storage(
        &mut cache,
        b,
        Bytes::from_static(STORAGE_EXTRACTOR_CODE),
        &[(U256::from(0u64), U256::from(201u64))],
    );

    let targets = vec![
        (
            a,
            vec![U256::from(0u64), U256::from(1u64), U256::from(2u64)],
        ),
        (b, vec![U256::from(0u64)]),
    ];
    let out = run_call(
        &mut cache,
        MULTICALL3_ADDRESS,
        encode_multi_target_calldata(&targets),
    )?;

    let mut results = decode_multi_target_response(&targets, &out);
    results.sort_by_key(|(addr, slot, _)| (*addr, *slot));
    let values: Vec<U256> = results
        .iter()
        .map(|(_, _, r)| *r.as_ref().expect("all subcalls succeed"))
        .collect();
    assert_eq!(
        values,
        vec![
            U256::from(101u64),
            U256::from(102u64),
            U256::ZERO,
            U256::from(201u64),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Mocked-transport fetcher tests.
// ---------------------------------------------------------------------------

fn mocked_provider() -> (Arc<RootProvider<AnyNetwork>>, Asserter) {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter.clone());
    (Arc::new(RootProvider::<AnyNetwork>::new(client)), asserter)
}

fn counting_stub_fallback(counter: Arc<AtomicUsize>, value: u64) -> StorageBatchFetchFn {
    Arc::new(move |requests: Vec<(Address, U256)>, _block: BlockId| {
        counter.fetch_add(requests.len(), Ordering::Relaxed);
        requests
            .into_iter()
            .map(|(addr, slot)| (addr, slot, Ok(U256::from(value))))
            .collect()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn fetcher_returns_one_result_per_request_from_single_call() {
    let (provider, asserter) = mocked_provider();
    let token = target(0x0a);
    // Duplicates included: 4 requests, one eth_call, 4 packed words back.
    let requests = vec![
        (token, U256::from(1u64)),
        (token, U256::from(2u64)),
        (token, U256::from(1u64)),
        (token, U256::from(3u64)),
    ];
    let values = [11u64, 22, 11, 33].map(U256::from);
    asserter.push_success(&pack_slots_calldata(&values));

    let fetcher = bulk_call_storage_fetcher(provider, BulkCallConfig::default());
    let results = fetcher(requests.clone(), BlockId::latest());

    assert_eq!(results.len(), requests.len());
    assert!(
        asserter.read_q().is_empty(),
        "exactly one eth_call consumed"
    );
    let mut expected: HashMap<(Address, U256), Vec<U256>> = HashMap::new();
    for ((addr, slot), value) in requests.iter().zip(values) {
        expected.entry((*addr, *slot)).or_default().push(value);
    }
    for (addr, slot, result) in results {
        let bucket = expected
            .get_mut(&(addr, slot))
            .expect("result maps to a requested pair");
        let value = result.expect("mocked call succeeds");
        let pos = bucket
            .iter()
            .position(|v| *v == value)
            .expect("value matches a remaining expectation");
        bucket.remove(pos);
    }
    assert!(expected.values().all(Vec::is_empty), "all pairs consumed");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetcher_maps_transport_error_to_every_slot() {
    let (provider, asserter) = mocked_provider();
    asserter.push_failure_msg("state overrides not supported");

    let fetcher = bulk_call_storage_fetcher(provider, BulkCallConfig::default());
    let results = fetcher(
        vec![
            (target(0x0b), U256::from(1u64)),
            (target(0x0b), U256::from(2u64)),
        ],
        BlockId::latest(),
    );
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(_, _, r)| r.is_err()));
}

#[tokio::test(flavor = "multi_thread")]
async fn fetcher_repairs_failed_slots_via_fallback() {
    let (provider, asserter) = mocked_provider();
    asserter.push_failure_msg("state overrides not supported");

    let repaired = Arc::new(AtomicUsize::new(0));
    let fetcher = bulk_call_storage_fetcher_with_fallback(
        provider,
        BulkCallConfig::default(),
        counting_stub_fallback(repaired.clone(), 77),
    );
    let requests = vec![
        (target(0x0c), U256::from(1u64)),
        (target(0x0c), U256::from(2u64)),
        (target(0x0d), U256::from(3u64)),
    ];
    let results = fetcher(requests.clone(), BlockId::latest());

    assert_eq!(results.len(), requests.len());
    assert_eq!(repaired.load(Ordering::Relaxed), requests.len());
    assert!(
        results
            .iter()
            .all(|(_, _, r)| matches!(r, Ok(v) if *v == U256::from(77u64))),
        "every failed slot repaired by the fallback"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetcher_routes_tiny_requests_to_fallback_without_rpc() {
    let (provider, asserter) = mocked_provider();
    // Nothing queued: any eth_call would surface as an error result.

    let point_reads = Arc::new(AtomicUsize::new(0));
    let fetcher = bulk_call_storage_fetcher_with_fallback(
        provider,
        BulkCallConfig::default(), // point_read_threshold = 2
        counting_stub_fallback(point_reads.clone(), 5),
    );
    let results = fetcher(vec![(target(0x0e), U256::from(9u64))], BlockId::latest());

    assert_eq!(point_reads.load(Ordering::Relaxed), 1);
    assert!(matches!(results[0].2, Ok(v) if v == U256::from(5u64)));
    assert!(asserter.read_q().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn status_reports_fallback_latch_after_consecutive_provider_failures() {
    let (provider, asserter) = mocked_provider();
    // Two batches, each a single eth_call that fails at the provider level —
    // the signature of an endpoint without state-override support.
    asserter.push_failure_msg("state overrides not supported");
    asserter.push_failure_msg("state overrides not supported");

    let repaired = Arc::new(AtomicUsize::new(0));
    let (fetcher, status) = bulk_call_storage_fetcher_with_status(
        provider,
        BulkCallConfig::default(), // point_read_threshold = 2, latch threshold = 2
        counting_stub_fallback(repaired.clone(), 42),
    );
    // A clone observes the very same live counter (documented Arc-sharing).
    let observer: BulkFetcherStatus = status.clone();

    assert_eq!(status.latch_threshold(), 2);
    assert!(
        !status.fallback_latched(),
        "not latched before any failures"
    );
    assert_eq!(status.consecutive_override_failures(), 0);

    // A 2-slot single-target batch clears point_read_threshold, so it attempts
    // the bulk path rather than routing straight to the fallback.
    let batch = vec![
        (target(0x1a), U256::from(1u64)),
        (target(0x1a), U256::from(2u64)),
    ];

    // First all-provider-error batch: repaired by the fallback, streak = 1.
    let r1 = fetcher(batch.clone(), BlockId::latest());
    assert!(
        r1.iter()
            .all(|(_, _, r)| matches!(r, Ok(v) if *v == U256::from(42u64))),
        "failed slots repaired by the fallback"
    );
    assert_eq!(status.consecutive_override_failures(), 1);
    assert!(
        !status.fallback_latched(),
        "one failure is below the threshold"
    );

    // Second consecutive all-provider-error batch: streak hits the threshold.
    let _r2 = fetcher(batch.clone(), BlockId::latest());
    assert_eq!(status.consecutive_override_failures(), 2);
    assert!(
        status.fallback_latched(),
        "latched after two consecutive all-provider-error batches"
    );
    assert!(observer.fallback_latched(), "the clone sees the same latch");
    assert!(
        asserter.read_q().is_empty(),
        "both bulk attempts consumed their queued eth_call"
    );

    // Once latched, further batches skip the bulk attempt entirely and go
    // straight to the fallback — no eth_call is issued (nothing is queued, yet
    // the batch still succeeds via the fallback).
    let r3 = fetcher(batch.clone(), BlockId::latest());
    assert!(
        r3.iter()
            .all(|(_, _, r)| matches!(r, Ok(v) if *v == U256::from(42u64))),
        "latched fetcher serves via the fallback"
    );
    assert!(
        asserter.read_q().is_empty(),
        "a latched fetcher issues no eth_call"
    );
    // Latched streak does not climb past the threshold (latched batches skip
    // the bulk bookkeeping).
    assert_eq!(status.consecutive_override_failures(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn status_streak_resets_after_a_successful_batch() {
    let (provider, asserter) = mocked_provider();
    let token = target(0x2b);
    let batch = vec![(token, U256::from(1u64)), (token, U256::from(2u64))];

    // Batch 1 fails at the provider level (streak → 1); batch 2 succeeds.
    asserter.push_failure_msg("state overrides not supported");
    asserter.push_success(&pack_slots_calldata(&[U256::from(9u64), U256::from(8u64)]));

    let repaired = Arc::new(AtomicUsize::new(0));
    let (fetcher, status) = bulk_call_storage_fetcher_with_status(
        provider,
        BulkCallConfig::default(),
        counting_stub_fallback(repaired.clone(), 42),
    );

    let _ = fetcher(batch.clone(), BlockId::latest());
    assert_eq!(status.consecutive_override_failures(), 1);

    let r2 = fetcher(batch.clone(), BlockId::latest());
    assert!(
        r2.iter().all(|(_, _, r)| r.is_ok()),
        "second batch served by the bulk path"
    );
    assert_eq!(
        status.consecutive_override_failures(),
        0,
        "any successful batch resets the streak"
    );
    assert!(!status.fallback_latched());
    assert!(
        asserter.read_q().is_empty(),
        "two eth_calls consumed, none latched away"
    );
}

#[tokio::test]
async fn fetcher_degrades_to_runtime_error_on_current_thread_runtime() {
    let (provider, _asserter) = mocked_provider();
    let fetcher = bulk_call_storage_fetcher(provider, BulkCallConfig::default());
    let results = fetcher(vec![(target(0x0f), U256::from(1u64))], BlockId::latest());
    assert_eq!(results.len(), 1);
    assert!(
        matches!(
            &results[0].2,
            Err(evm_fork_cache::errors::StorageFetchError::Runtime(_))
        ),
        "current-thread runtime must degrade to a typed error, got {:?}",
        results[0].2
    );
}

#[tokio::test]
async fn fetcher_on_current_thread_runtime_still_repairs_via_fallback() {
    let (provider, _asserter) = mocked_provider();
    let repaired = Arc::new(AtomicUsize::new(0));
    let fetcher = bulk_call_storage_fetcher_with_fallback(
        provider,
        BulkCallConfig::default(),
        counting_stub_fallback(repaired.clone(), 3),
    );
    // Two slots: above the point-read threshold, so the bulk path is
    // attempted, fails the runtime guard, and the (synchronous) fallback
    // repairs both pairs.
    let results = fetcher(
        vec![
            (target(0x1f), U256::from(1u64)),
            (target(0x1f), U256::from(2u64)),
        ],
        BlockId::latest(),
    );
    assert_eq!(repaired.load(Ordering::Relaxed), 2);
    assert!(results.iter().all(|(_, _, r)| r.is_ok()));
}

#[tokio::test(flavor = "multi_thread")]
async fn fetcher_decodes_multicall_dispatch_response() {
    let (provider, asserter) = mocked_provider();
    let (a, b) = (target(0x21), target(0x22));

    // Two small targets pack into one aggregate3 dispatch; mock its response.
    let response = IMulticall3::aggregate3Call::abi_encode_returns(&vec![
        IMulticall3::Result {
            success: true,
            returnData: pack_slots_calldata(&[U256::from(1001u64), U256::from(1002u64)]),
        },
        IMulticall3::Result {
            success: true,
            returnData: pack_slots_calldata(&[U256::from(2001u64)]),
        },
    ]);
    asserter.push_success(&Bytes::from(response));

    let fetcher = bulk_call_storage_fetcher(provider, BulkCallConfig::default());
    let mut results = fetcher(
        vec![
            (a, U256::from(0u64)),
            (a, U256::from(1u64)),
            (b, U256::from(0u64)),
        ],
        BlockId::latest(),
    );
    results.sort_by_key(|(addr, slot, _)| (*addr, *slot));
    let values: Vec<U256> = results
        .into_iter()
        .map(|(_, _, r)| r.expect("mocked dispatch succeeds"))
        .collect();
    assert_eq!(
        values,
        vec![
            U256::from(1001u64),
            U256::from(1002u64),
            U256::from(2001u64)
        ]
    );
    assert!(
        asserter.read_q().is_empty(),
        "one eth_call for both targets"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetcher_empty_request_is_a_no_op() {
    let (provider, asserter) = mocked_provider();
    let fetcher = bulk_call_storage_fetcher(provider, BulkCallConfig::default());
    let results = fetcher(Vec::new(), BlockId::latest());
    assert!(results.is_empty());
    assert!(asserter.read_q().is_empty());
}

// ---------------------------------------------------------------------------
// Default-on integration, latch, dispatch modes, prewarm.
// ---------------------------------------------------------------------------

/// Bulk extraction is the cache's DEFAULT batch fetcher: one queued eth_call
/// response serves a whole multi-slot batch.
#[tokio::test(flavor = "multi_thread")]
async fn cache_default_fetcher_is_bulk_extraction() -> Result<()> {
    let (cache, asserter) = common::setup_cache_with_asserter().await?;
    let token = target(0x31);
    let values = [1u64, 2, 3].map(U256::from);
    asserter.push_success(&pack_slots_calldata(&values));

    let fetcher = cache
        .storage_batch_fetcher()
        .cloned()
        .expect("default fetcher installed");
    let results = fetcher(
        vec![
            (token, U256::from(0u64)),
            (token, U256::from(1u64)),
            (token, U256::from(2u64)),
        ],
        BlockId::latest(),
    );
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|(_, _, r)| r.is_ok()));
    assert!(
        asserter.read_q().is_empty(),
        "three slots must consume exactly one eth_call"
    );
    Ok(())
}

/// A non-default bulk config supplied through the builder is applied.
#[tokio::test(flavor = "multi_thread")]
async fn builder_bulk_call_config_is_applied() -> Result<()> {
    use alloy_provider::RootProvider;
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter.clone());
    let provider = Arc::new(RootProvider::<AnyNetwork>::new(client));
    let cache = evm_fork_cache::cache::EvmCache::builder(provider)
        .bulk_call_config(BulkCallConfig {
            max_slots_per_call: 1,
            max_concurrent_calls: 1,
            ..BulkCallConfig::default()
        })
        .build()
        .await;

    // With max_slots_per_call = 1, two slots must consume TWO eth_calls.
    let token = target(0x32);
    asserter.push_success(&pack_slots_calldata(&[U256::from(7u64)]));
    asserter.push_success(&pack_slots_calldata(&[U256::from(8u64)]));
    let fetcher = cache.storage_batch_fetcher().cloned().expect("fetcher");
    let results = fetcher(
        vec![(token, U256::from(0u64)), (token, U256::from(1u64))],
        BlockId::latest(),
    );
    assert!(results.iter().all(|(_, _, r)| r.is_ok()));
    assert!(asserter.read_q().is_empty(), "two single-slot eth_calls");
    Ok(())
}

/// After two consecutive fully-failed batches the fetcher latches to the
/// fallback and stops touching the provider.
#[tokio::test(flavor = "multi_thread")]
async fn fetcher_latches_to_fallback_after_consecutive_provider_failures() {
    let (provider, asserter) = mocked_provider();
    asserter.push_failure_msg("state overrides not supported");
    asserter.push_failure_msg("state overrides not supported");

    let repaired = Arc::new(AtomicUsize::new(0));
    let fetcher = bulk_call_storage_fetcher_with_fallback(
        provider,
        BulkCallConfig::default(),
        counting_stub_fallback(repaired.clone(), 4),
    );
    let requests = vec![
        (target(0x33), U256::from(1u64)),
        (target(0x33), U256::from(2u64)),
    ];
    fetcher(requests.clone(), BlockId::latest()); // failure 1 (repaired)
    fetcher(requests.clone(), BlockId::latest()); // failure 2 → latch

    // Queue a would-be success: a latched fetcher must leave it untouched.
    asserter.push_success(&pack_slots_calldata(&[U256::from(1u64), U256::from(2u64)]));
    let results = fetcher(requests.clone(), BlockId::latest());
    assert!(
        !asserter.read_q().is_empty(),
        "latched fetcher must not touch the provider"
    );
    assert!(
        results
            .iter()
            .all(|(_, _, r)| matches!(r, Ok(v) if *v == U256::from(4u64))),
        "latched requests served entirely by the fallback"
    );
    assert_eq!(repaired.load(Ordering::Relaxed), 6);
}

/// CallMany dispatch: one eth_callMany request carries the whole batch.
#[tokio::test(flavor = "multi_thread")]
async fn fetcher_call_many_dispatch_decodes_bundle_response() {
    let (provider, asserter) = mocked_provider();
    let token = target(0x34);
    let values = [11u64, 22].map(U256::from);
    asserter.push_success(&serde_json::json!([[
        { "value": pack_slots_calldata(&values) }
    ]]));

    let fetcher = bulk_call_storage_fetcher(
        provider,
        BulkCallConfig {
            dispatch: CallDispatch::CallMany,
            ..BulkCallConfig::default()
        },
    );
    let results = fetcher(
        vec![(token, U256::from(0u64)), (token, U256::from(1u64))],
        BlockId::Number(alloy_eips::BlockNumberOrTag::Number(1000)),
    );
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(_, _, r)| r.is_ok()));
    assert!(asserter.read_q().is_empty(), "one eth_callMany request");
}

/// CallMany on a provider without the method: the same fetch transparently
/// re-dispatches per-call and still succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn fetcher_call_many_failure_redispatches_per_call() {
    let (provider, asserter) = mocked_provider();
    let token = target(0x35);
    asserter.push_failure_msg("the method eth_callMany does not exist");
    asserter.push_success(&pack_slots_calldata(&[U256::from(5u64), U256::from(6u64)]));

    let fetcher = bulk_call_storage_fetcher(
        provider,
        BulkCallConfig {
            dispatch: CallDispatch::CallMany,
            max_concurrent_calls: 1,
            ..BulkCallConfig::default()
        },
    );
    let results = fetcher(
        vec![(token, U256::from(0u64)), (token, U256::from(1u64))],
        BlockId::Number(alloy_eips::BlockNumberOrTag::Number(1000)),
    );
    assert!(
        results.iter().all(|(_, _, r)| r.is_ok()),
        "per-call re-dispatch must succeed: {results:?}"
    );
    assert!(asserter.read_q().is_empty());
}

/// prewarm_slots loads through the (bulk) fetcher and injects into layer 2.
#[tokio::test(flavor = "multi_thread")]
async fn prewarm_slots_bulk_loads_into_cache() -> Result<()> {
    let (mut cache, asserter) = common::setup_cache_with_asserter().await?;
    let token = target(0x36);
    let values = [9u64, 8].map(U256::from);
    asserter.push_success(&pack_slots_calldata(&values));

    let report = cache.prewarm_slots(&[(token, U256::from(0u64)), (token, U256::from(1u64))]);
    assert_eq!(report.loaded, 2);
    assert!(report.failed.is_empty());
    assert_eq!(
        cache.cached_storage_value(token, U256::from(0u64)),
        Some(U256::from(9u64))
    );
    assert_eq!(
        cache.cached_storage_value(token, U256::from(1u64)),
        Some(U256::from(8u64))
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Custom programs & companion extractors: revm-level dogfood tests.
// ---------------------------------------------------------------------------

/// The account-fields extractor bytecode, executed in revm: balance +
/// EXTCODEHASH per queried address, host reading *other* accounts.
#[tokio::test(flavor = "multi_thread")]
async fn account_fields_extractor_reads_other_accounts() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, CALLER);

    let host = target(0x41);
    install_code_with_storage(
        &mut cache,
        host,
        Bytes::from_static(ACCOUNT_FIELDS_EXTRACTOR_CODE),
        &[],
    );

    // A funded contract account and an existing-but-empty account.
    let funded = target(0x42);
    let bytecode = Bytecode::new_raw(Bytes::from_static(STORAGE_EXTRACTOR_CODE));
    let code_hash = bytecode.hash_slow();
    cache.db_mut().insert_account_info(
        funded,
        AccountInfo {
            balance: U256::from(12_345u64),
            nonce: 1,
            code: Some(bytecode),
            code_hash,
            account_id: None,
        },
    );
    let empty = target(0x43);
    install_default_account(&mut cache, empty);

    let mut calldata = Vec::new();
    for addr in [funded, empty] {
        calldata.extend_from_slice(&[0u8; 12]);
        calldata.extend_from_slice(addr.as_slice());
    }
    let out = run_call(&mut cache, host, calldata.into())?;
    assert_eq!(out.len(), 128, "two words per address");
    assert_eq!(U256::from_be_slice(&out[0..32]), U256::from(12_345u64));
    assert_eq!(&out[32..64], code_hash.as_slice());
    assert_eq!(U256::from_be_slice(&out[64..96]), U256::ZERO);
    // EIP-1052: an existing empty account hashes to zero.
    assert_eq!(U256::from_be_slice(&out[96..128]), U256::ZERO);
    Ok(())
}

/// The block-context extractor bytecode, executed in revm: seven env words.
#[tokio::test(flavor = "multi_thread")]
async fn block_context_extractor_returns_env_words() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, CALLER);
    let host = target(0x44);
    install_code_with_storage(
        &mut cache,
        host,
        Bytes::from_static(BLOCK_CONTEXT_EXTRACTOR_CODE),
        &[],
    );
    cache.set_block(BlockId::Number(alloy_eips::BlockNumberOrTag::Number(42)));

    let out = run_call(&mut cache, host, Bytes::new())?;
    assert_eq!(out.len(), 7 * 32);
    let word = |i: usize| U256::from_be_slice(&out[i * 32..(i + 1) * 32]);
    assert_eq!(word(0), U256::from(42u64), "NUMBER = pinned block");
    assert_eq!(word(6), U256::from(1u64), "CHAINID = cache default");
    Ok(())
}

/// A custom storage *program*: the one-shot Uniswap-V3-style observation-ring
/// loader — reads the ring cardinality out of slot0 in-EVM, then returns the
/// whole ring, with zero calldata. Mirrors the program demonstrated live in
/// `examples/bulk_storage_bench.rs`.
const OBSERVATION_RING_PROGRAM: &[u8] = &alloy_primitives::hex!(
    "5f5460c81c61ffff165f5b81811460215780600801548160051b52600101600a565b5060051b5ff3"
);

#[tokio::test(flavor = "multi_thread")]
async fn observation_ring_program_reads_data_dependent_slots() -> Result<()> {
    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, CALLER);

    let pool = target(0x45);
    // slot0 layout puts observationCardinality at bits 200..216; pack junk
    // around it to prove the masking. Ring entries live at slots 8+i.
    let cardinality = 3u64;
    let slot0 = (U256::from(cardinality) << 200usize)
        | (U256::from(77u64) << 160usize)
        | U256::from(12_345u64);
    let ring = [1001u64, 1002, 1003].map(U256::from);
    install_code_with_storage(
        &mut cache,
        pool,
        Bytes::from_static(OBSERVATION_RING_PROGRAM),
        &[
            (U256::from(0u64), slot0),
            (U256::from(8u64), ring[0]),
            (U256::from(9u64), ring[1]),
            (U256::from(10u64), ring[2]),
        ],
    );

    let out = run_call(&mut cache, pool, Bytes::new())?;
    let values = decode_packed_values(&out, cardinality as usize).expect("cardinality words");
    assert_eq!(values, ring.to_vec());
    Ok(())
}

/// run_storage_programs batches distinct targets into one dispatch (mocked).
#[tokio::test(flavor = "multi_thread")]
async fn storage_programs_batch_distinct_targets() {
    let (provider, asserter) = mocked_provider();
    let programs = vec![
        StorageProgram {
            target: target(0x46),
            code: Bytes::from_static(OBSERVATION_RING_PROGRAM),
            calldata: Bytes::new(),
        },
        StorageProgram {
            target: target(0x47),
            code: Bytes::from_static(STORAGE_EXTRACTOR_CODE),
            calldata: pack_slots_calldata(&[U256::from(1u64)]),
        },
    ];
    let response = IMulticall3::aggregate3Call::abi_encode_returns(&vec![
        IMulticall3::Result {
            success: true,
            returnData: Bytes::from(vec![0xAA; 32]),
        },
        IMulticall3::Result {
            success: false,
            returnData: Bytes::new(),
        },
    ]);
    asserter.push_success(&Bytes::from(response));

    let rt_provider = provider.clone();
    let results = evm_fork_cache::bulk_storage::run_storage_programs(
        rt_provider.as_ref(),
        BlockId::latest(),
        &programs,
    )
    .await;
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].as_ref().expect("first program"),
        &Bytes::from(vec![0xAA; 32])
    );
    assert!(results[1].is_err(), "failed subcall surfaces per-program");
    assert!(
        asserter.read_q().is_empty(),
        "one dispatch for both programs"
    );
}
