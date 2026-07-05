//! Manager-authored red-green acceptance tests for WS-2 / Phase-8 step 2:
//! strict block-context requirements and engine-driven `advance_block` env
//! refresh.
//!
//! Contract:
//! - `BlockContextRequirements::strict()` rejects a header missing a required
//!   block-env field (e.g. EIP-1559 base fee); `lenient()` (the default) accepts
//!   it; requirements are per-field so a pre-EIP-1559 chain can opt out of the
//!   base-fee requirement.
//! - `EvmCache::advance_block(header)` refreshes the full block env
//!   (number / base fee / coinbase / prevrandao / gas limit / timestamp) from a
//!   canonical header, and under strict requirements returns an error rather than
//!   silently defaulting a missing field.
//!
//! Fully offline: block headers are constructed in memory; no network access.
#![cfg(feature = "reactive")]

mod common;

use alloy_consensus::Header;
use alloy_primitives::{Address, B256};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::cache::BlockContextRequirements;

/// A synthetic canonical header. `basefee = None` models a pre-EIP-1559 block.
fn header(number: u64, basefee: Option<u64>) -> Header {
    Header {
        number,
        timestamp: 1_700_000_000 + number,
        base_fee_per_gas: basefee,
        beneficiary: Address::repeat_byte(0xcb),
        gas_limit: 30_000_000,
        mix_hash: B256::repeat_byte(0xab),
        ..Default::default()
    }
}

/// WS-2: strict requirements reject a header missing the EIP-1559 base fee, and
/// accept a complete one.
#[test]
fn strict_requirements_reject_header_missing_basefee() {
    let reqs = BlockContextRequirements::strict();

    assert!(
        reqs.validate_header(&header(100, Some(7))).is_ok(),
        "a complete header must satisfy strict requirements"
    );

    let err = reqs
        .validate_header(&header(100, None))
        .expect_err("strict must reject a header with no base fee");
    assert!(
        err.to_string().to_lowercase().contains("basefee")
            || err.to_string().to_lowercase().contains("base fee"),
        "the error must name the missing base-fee field, got: {err}"
    );
}

/// WS-2: the lenient default accepts an incomplete header (today's behavior).
#[test]
fn lenient_requirements_accept_incomplete_header() {
    let reqs = BlockContextRequirements::lenient();
    assert!(reqs.validate_header(&header(100, None)).is_ok());
    assert!(reqs.validate_header(&header(100, Some(7))).is_ok());
}

/// WS-2: requirements are per-field — a chain without EIP-1559 can turn off the
/// base-fee requirement while still requiring the rest.
#[test]
fn per_field_requirements_allow_opting_out_of_basefee() {
    let mut reqs = BlockContextRequirements::strict();
    reqs.require_basefee = false;
    assert!(
        reqs.validate_header(&header(100, None)).is_ok(),
        "opting out of the base-fee requirement must accept a header without one"
    );
}

/// Phase-8 s2: `advance_block` refreshes every block-env field from the header.
#[tokio::test]
async fn advance_block_refreshes_all_block_env_fields() -> Result<()> {
    let mut cache = setup_cache().await?;
    let h = header(12_345, Some(42));

    cache
        .advance_block(&h)
        .expect("lenient advance_block over a complete header succeeds");

    assert_eq!(cache.block_number(), Some(12_345));
    assert_eq!(cache.basefee(), Some(42));
    assert_eq!(cache.coinbase(), Some(Address::repeat_byte(0xcb)));
    assert_eq!(cache.prevrandao(), Some(B256::repeat_byte(0xab)));
    assert_eq!(cache.block_gas_limit(), Some(30_000_000));
    assert_eq!(cache.timestamp(), Some(1_700_000_000 + 12_345));
    // The RPC pin must advance with the env: a lazy miss after the advance must
    // fetch state at the NEW block, not the previously pinned one (review
    // finding: env said N+1 while the SharedBackend still fetched at N).
    assert_eq!(
        cache.block(),
        alloy_eips::BlockId::number(12_345),
        "advance_block must re-pin RPC fetches to the advanced block"
    );
    Ok(())
}

/// WS-2 / Phase-8 s2: under strict requirements, `advance_block` fails loudly on
/// a header missing a required field instead of silently defaulting it.
#[tokio::test]
async fn advance_block_strict_rejects_incomplete_header() -> Result<()> {
    let mut cache = setup_cache().await?;
    cache.set_block_context_requirements(BlockContextRequirements::strict());

    let err = cache
        .advance_block(&header(200, None))
        .expect_err("strict advance_block must reject a header with no base fee");
    assert!(
        err.to_string().to_lowercase().contains("basefee")
            || err.to_string().to_lowercase().contains("base fee"),
        "the error must name the missing base-fee field, got: {err}"
    );

    // A complete header still refreshes under strict mode.
    cache
        .advance_block(&header(200, Some(9)))
        .expect("strict advance_block over a complete header succeeds");
    assert_eq!(cache.basefee(), Some(9));
    Ok(())
}

// --- Additional coverage (implementation agent, Wave 4) --------------------

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use evm_fork_cache::EvmCacheBuilder;
use evm_fork_cache::reactive::{
    ChainStatus, InputSource, ReactiveConfig, ReactiveContext, ReactiveInput, ReactiveInputBatch,
    ReactiveInputRecord, ReactiveReport, ReactiveRuntime,
};

/// Build a mocked provider (no network access) modelled on `common::setup_cache`.
fn mock_provider() -> Arc<RootProvider<AnyNetwork>> {
    let client = RpcClient::mocked(Asserter::new());
    Arc::new(RootProvider::<AnyNetwork>::new(client))
}

/// WS-2: a strict `try_build` over a provider that yields no header must fail
/// loudly at construction rather than silently defaulting the block env.
#[tokio::test]
async fn try_build_strict_fails_when_header_unavailable() {
    let result = EvmCacheBuilder::new(mock_provider())
        .strict_block_context(true)
        .try_build()
        .await;
    // `EvmCache` is not `Debug`, so branch manually rather than `expect_err`.
    let err = match result {
        Ok(_) => panic!("strict try_build over a header-less mock provider must error"),
        Err(err) => err,
    };
    // The mock provider returns no block, so the header fetch fails.
    assert!(
        err.to_string().to_lowercase().contains("fetch failed"),
        "expected a fetch-failure error, got: {err}"
    );
}

/// WS-2: a lenient `try_build` never errors, even when no header is available —
/// preserving the infallible/lenient default construction behavior.
#[tokio::test]
async fn try_build_lenient_succeeds_without_header() -> Result<()> {
    // Explicit lenient.
    let cache = EvmCacheBuilder::new(mock_provider())
        .strict_block_context(false)
        .try_build()
        .await?;
    // A header-less mock provider leaves the block env unset under lenient mode.
    assert_eq!(cache.block_number(), None);

    // Default (no requirements configured) is lenient and also succeeds.
    let _cache = EvmCacheBuilder::new(mock_provider()).try_build().await?;
    Ok(())
}

/// Build the RPC-flavored `HeaderResponse` (`alloy_rpc_types_eth::Header`) that
/// the `Ethereum` reactive runtime ingests, from the in-memory consensus header.
fn rpc_header(number: u64, basefee: Option<u64>) -> alloy_rpc_types_eth::Header {
    alloy_rpc_types_eth::Header::new(header(number, basefee))
}

/// A canonical (`Included`) context for a block header at `number`.
fn included_header_context(number: u64) -> ReactiveContext {
    let block = evm_fork_cache::reactive::BlockRef {
        number,
        hash: B256::repeat_byte(0x11),
        parent_hash: Some(B256::repeat_byte(0x10)),
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
        transaction_index: None,
        log_index: None,
    }
}

/// Phase-8 s2: ingesting a canonical `BlockHeader` drives `advance_block`, so
/// the cache's block env is refreshed from the header without any handler.
#[tokio::test]
async fn reactive_ingest_of_canonical_header_refreshes_block_env() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    let input = ReactiveInput::BlockHeader(rpc_header(7_777, Some(123)));
    let batch = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        input,
        included_header_context(7_777),
    )]);

    let report = runtime.ingest_batch(&mut cache, batch)?;

    // The env was refreshed from the ingested header.
    assert_eq!(cache.block_number(), Some(7_777));
    assert_eq!(cache.basefee(), Some(123));
    assert_eq!(cache.timestamp(), Some(1_700_000_000 + 7_777));
    // Lenient default: no error report surfaced.
    assert!(
        !report
            .reports
            .iter()
            .any(|r| matches!(r.as_ref(), ReactiveReport::Error(_))),
        "a lenient canonical drive must not surface an error report"
    );
    Ok(())
}

/// Phase-8 s2: a pending (non-canonical) header must NOT drive `advance_block`.
#[tokio::test]
async fn reactive_ingest_of_pending_header_does_not_refresh_block_env() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    let ctx = ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Subscription,
        chain_status: ChainStatus::Pending,
        block: None,
        transaction_index: None,
        log_index: None,
    };
    let input = ReactiveInput::BlockHeader(rpc_header(9_999, Some(55)));
    let batch = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, ctx)]);

    runtime.ingest_batch(&mut cache, batch)?;

    // Pending inputs never drive a canonical env refresh.
    assert_eq!(cache.block_number(), None);
    assert_eq!(cache.basefee(), None);
    Ok(())
}

/// WS-2 / Phase-8 s2: under strict requirements, a canonical header missing a
/// required field surfaces a non-fatal `ReactiveReport::Error` (the batch is not
/// aborted).
#[tokio::test]
async fn reactive_strict_drive_surfaces_error_report_for_incomplete_header() -> Result<()> {
    let mut cache = setup_cache().await?;
    cache.set_block_context_requirements(BlockContextRequirements::strict());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    // No base fee -> strict validation fails during the drive.
    let input = ReactiveInput::BlockHeader(rpc_header(4_242, None));
    let batch = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        input,
        included_header_context(4_242),
    )]);

    let report = runtime.ingest_batch(&mut cache, batch)?;

    let error_message = report
        .reports
        .iter()
        .find_map(|r| match r.as_ref() {
            ReactiveReport::Error(e) => Some(e.message.clone()),
            _ => None,
        })
        .expect("strict drive over an incomplete header must surface an error report");
    assert!(
        error_message.to_lowercase().contains("basefee"),
        "the error report must name the missing base-fee field, got: {error_message}"
    );
    Ok(())
}
