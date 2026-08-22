//! Attributed provider-request accounting for the Alloy subscriber.
//!
//! `SubscriberRpcStats` exists so RPC consumption is diagnosable from inside the
//! process rather than from a provider invoice. These tests pin the property
//! that makes it trustworthy: the counters agree exactly with the requests the
//! mocked transport actually served, and each one is attributed to the mechanism
//! that caused it — never to the shared fetch helper both mechanisms call.
//!
//! The polling transport is used because it is the mockable one: `eth_newFilter`
//! stands in for the pubsub `eth_subscribe` handshake, and both are counted as
//! [`SubscriberRpcMethod::EthSubscribe`] stream installation.
#![cfg(feature = "reactive")]

#[cfg(feature = "reactive-polling")]
use alloy_network::Ethereum;
#[cfg(feature = "reactive-polling")]
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
#[cfg(feature = "reactive-polling")]
use alloy_provider::ProviderBuilder;
#[cfg(feature = "reactive-polling")]
use alloy_rpc_types_eth::{Block, Filter, Header, Log};
#[cfg(feature = "reactive-polling")]
use alloy_transport::mock::Asserter;
#[cfg(feature = "reactive-polling")]
use anyhow::Result;

#[cfg(feature = "reactive-polling")]
use evm_fork_cache::reactive::{
    AlloySubscriber, BlockRef, EventSubscriber, HandlerId, LogInterest, ReactiveInterest,
    SubscriberBackfill, SubscriberConfig, SubscriberMode, SubscriberOwnerStart,
};
use evm_fork_cache::reactive::{SubscriberRpcCause, SubscriberRpcMethod, SubscriberRpcStats};

/// Every cause/method pair that carried at least one request, as a sorted vector
/// so an assertion can name the complete expected set instead of probing it one
/// counter at a time.
fn attributed(stats: &SubscriberRpcStats) -> Vec<(SubscriberRpcCause, SubscriberRpcMethod, u64)> {
    stats.nonzero().collect()
}

#[cfg(feature = "reactive-polling")]
fn rpc_log(address: Address, topic0: B256, block_number: u64, log_index: u64) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, vec![topic0], Bytes::new()),
        block_hash: Some(B256::repeat_byte(block_number as u8)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0x20 + log_index as u8)),
        transaction_index: Some(log_index),
        log_index: Some(log_index),
        removed: false,
    }
}

#[cfg(feature = "reactive-polling")]
fn rpc_block(point: &BlockRef) -> Block {
    Block::empty(Header {
        hash: point.hash,
        inner: alloy_consensus::Header {
            number: point.number,
            parent_hash: point.parent_hash.unwrap_or_default(),
            timestamp: point.timestamp.unwrap_or_default(),
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    })
}

#[cfg(feature = "reactive-polling")]
fn polling_subscriber(
    asserter: Asserter,
) -> AlloySubscriber<impl alloy_provider::Provider<Ethereum> + Clone, Ethereum> {
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    AlloySubscriber::new(
        provider,
        SubscriberMode::Polling,
        SubscriberConfig {
            max_batch_size: 16,
            ..SubscriberConfig::default()
        },
    )
}

#[cfg(feature = "reactive-polling")]
fn log_interest(address: Address, topic: B256) -> ReactiveInterest<Ethereum> {
    ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic),
        local_matcher: None,
        route_key: None,
    })
}

#[test]
fn a_fresh_subscriber_reports_no_provider_requests() {
    let stats = SubscriberRpcStats::default();

    assert_eq!(stats.total(), 0);
    assert!(attributed(&stats).is_empty());
    for cause in SubscriberRpcCause::ALL {
        assert_eq!(stats.by_cause(cause), 0);
    }
    for method in SubscriberRpcMethod::ALL {
        assert_eq!(stats.by_method(method), 0);
    }
}

/// The two reporting dimensions are views of one matrix, so they must always
/// sum to the same total. A regression here would make a report silently
/// under-count.
#[test]
fn reporting_dimensions_are_consistent_by_construction() {
    let stats = SubscriberRpcStats::default();
    let by_cause: u64 = SubscriberRpcCause::ALL
        .into_iter()
        .map(|cause| stats.by_cause(cause))
        .sum();
    let by_method: u64 = SubscriberRpcMethod::ALL
        .into_iter()
        .map(|method| stats.by_method(method))
        .sum();
    let by_entry: u64 = stats.entries().map(|(_, _, requests)| requests).sum();

    assert_eq!(by_cause, stats.total());
    assert_eq!(by_method, stats.total());
    assert_eq!(by_entry, stats.total());
    assert_eq!(
        stats.entries().count(),
        SubscriberRpcCause::COUNT * SubscriberRpcMethod::COUNT
    );
}

/// Bulk owner catch-up: exactly one chain-identity read, one stream
/// installation, two target certifications, and one `eth_getLogs` — all
/// attributed to [`SubscriberRpcCause::OwnerReconcile`].
#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn owner_reconcile_attributes_every_provider_request() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xb1);
    let topic = keccak256(b"Swap()");
    let baseline = BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    };
    let through = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x65),
        parent_hash: Some(B256::repeat_byte(0x64)),
        timestamp: Some(1_700_000_101),
    };

    // eth_newFilter, eth_chainId, target certification, eth_getLogs, final
    // certification — the exact sequence `reconcile_interest_owners` issues.
    asserter.push_success(&U256::from(1));
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![rpc_log(pool, topic, 101, 0)]);
    asserter.push_success(&Some(rpc_block(&through)));

    let mut subscriber = polling_subscriber(asserter.clone());
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("stats-pool"),
        &[log_interest(pool, topic)],
        SubscriberOwnerStart::PostBlock(baseline),
    )?;
    subscriber
        .reconcile_interest_owners(std::slice::from_ref(&epoch), through)
        .await?;

    assert!(
        asserter.read_q().is_empty(),
        "the mocked transport must have served every queued response"
    );

    let stats = subscriber.rpc_stats();
    assert_eq!(
        attributed(&stats),
        vec![
            (
                SubscriberRpcCause::ChainIdentity,
                SubscriberRpcMethod::EthChainId,
                1
            ),
            (
                SubscriberRpcCause::StreamSubscription,
                SubscriberRpcMethod::EthSubscribe,
                1
            ),
            (
                SubscriberRpcCause::OwnerReconcile,
                SubscriberRpcMethod::EthGetBlockByNumber,
                2
            ),
            (
                SubscriberRpcCause::OwnerReconcile,
                SubscriberRpcMethod::EthGetLogs,
                1
            ),
        ],
        "every served request must be attributed, and nothing else counted"
    );
    assert_eq!(stats.total(), 5);
    assert_eq!(stats.by_method(SubscriberRpcMethod::EthGetLogs), 1);
    assert_eq!(stats.by_cause(SubscriberRpcCause::OwnerReconcile), 3);

    Ok(())
}

/// A queued lazy backfill issues the same shaped requests through the same
/// helper as bulk reconcile, and must still be attributed to its own cause.
/// This is the property that makes an unexpected bill diagnosable: knowing the
/// method alone would not distinguish these two mechanisms.
#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn lazy_backfill_is_attributed_apart_from_owner_reconcile() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xcd);
    let topic = keccak256(b"Swap()");
    let through = BlockRef {
        number: 42,
        hash: B256::repeat_byte(42),
        parent_hash: Some(B256::repeat_byte(41)),
        timestamp: Some(1_700_000_042),
    };

    // A bounded range needs no head resolution: certification, logs,
    // certification, behind the stream installation and chain-identity read.
    asserter.push_success(&U256::from(1));
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![rpc_log(pool, topic, 42, 3)]);
    asserter.push_success(&Some(rpc_block(&through)));

    let mut subscriber = polling_subscriber(asserter.clone());
    subscriber.add_interest_owner_with_backfill(
        HandlerId::new("stats-lazy-pool"),
        &[log_interest(pool, topic)],
        SubscriberBackfill::range(40, 42),
    )?;
    let batch = subscriber
        .next_batch()
        .await?
        .expect("queued backfill must deliver its owner-scoped batch");
    assert_eq!(batch.records().len(), 1);

    assert!(
        asserter.read_q().is_empty(),
        "the mocked transport must have served every queued response"
    );

    let stats = subscriber.rpc_stats();
    assert_eq!(
        attributed(&stats),
        vec![
            (
                SubscriberRpcCause::ChainIdentity,
                SubscriberRpcMethod::EthChainId,
                1
            ),
            (
                SubscriberRpcCause::StreamSubscription,
                SubscriberRpcMethod::EthSubscribe,
                1
            ),
            (
                SubscriberRpcCause::LazyBackfill,
                SubscriberRpcMethod::EthGetBlockByNumber,
                2
            ),
            (
                SubscriberRpcCause::LazyBackfill,
                SubscriberRpcMethod::EthGetLogs,
                1
            ),
        ]
    );
    assert_eq!(
        stats.by_cause(SubscriberRpcCause::OwnerReconcile),
        0,
        "a lazy backfill must not be charged to bulk owner reconciliation"
    );
    assert_eq!(stats.by_cause(SubscriberRpcCause::LazyBackfill), 3);

    Ok(())
}

/// `reset_rpc_stats` opens a new measurement window without disturbing the
/// subscriber, and takes `&self` so it can be called while catch-up work holds
/// the subscriber mutably elsewhere.
#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn reset_rpc_stats_opens_a_new_measurement_window() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xa7);
    let topic = keccak256(b"Swap()");
    let through = BlockRef {
        number: 42,
        hash: B256::repeat_byte(42),
        parent_hash: Some(B256::repeat_byte(41)),
        timestamp: Some(1_700_000_042),
    };
    asserter.push_success(&U256::from(1));
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![rpc_log(pool, topic, 42, 3)]);
    asserter.push_success(&Some(rpc_block(&through)));

    let mut subscriber = polling_subscriber(asserter.clone());
    subscriber.add_interest_owner_with_backfill(
        HandlerId::new("stats-window-pool"),
        &[log_interest(pool, topic)],
        SubscriberBackfill::range(40, 42),
    )?;
    let _ = subscriber.next_batch().await?;
    assert!(subscriber.rpc_stats().total() > 0);

    subscriber.reset_rpc_stats();

    let stats = subscriber.rpc_stats();
    assert_eq!(stats.total(), 0);
    assert!(attributed(&stats).is_empty());
    assert_eq!(
        subscriber.chain_id(),
        Some(1),
        "resetting a counter window must not disturb resolved subscriber state"
    );

    Ok(())
}

/// Labels are part of the diagnostic contract: a metrics export keys on them, so
/// they must stay stable and must not collide.
#[test]
fn cause_and_method_labels_are_stable_and_distinct() {
    assert_eq!(SubscriberRpcMethod::EthGetLogs.as_str(), "eth_getLogs");
    assert_eq!(
        SubscriberRpcMethod::EthGetBlockByNumber.to_string(),
        "eth_getBlockByNumber"
    );
    assert_eq!(
        SubscriberRpcCause::CanonicalHeadCertification.as_str(),
        "canonical_head_certification"
    );
    assert_eq!(
        SubscriberRpcCause::LazyBackfill.to_string(),
        "lazy_backfill"
    );

    let mut method_labels: Vec<_> = SubscriberRpcMethod::ALL
        .into_iter()
        .map(SubscriberRpcMethod::as_str)
        .collect();
    method_labels.sort_unstable();
    method_labels.dedup();
    assert_eq!(method_labels.len(), SubscriberRpcMethod::COUNT);

    let mut cause_labels: Vec<_> = SubscriberRpcCause::ALL
        .into_iter()
        .map(SubscriberRpcCause::as_str)
        .collect();
    cause_labels.sort_unstable();
    cause_labels.dedup();
    assert_eq!(cause_labels.len(), SubscriberRpcCause::COUNT);
}
