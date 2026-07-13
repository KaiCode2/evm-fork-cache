//! Manager-authored acceptance tests for the out-of-the-box Alloy subscriber.
//!
//! These tests pin the default WebSocket/pubsub subscriber surface and the
//! opt-in HTTP polling fallback.
#![cfg(feature = "reactive")]

use std::time::Duration;

use alloy_network::Ethereum;
#[cfg(feature = "reactive-polling")]
use alloy_primitives::U256;
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use alloy_primitives::{Address, keccak256};
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use alloy_primitives::{B256, Bytes, Log as PrimitiveLog};
use alloy_provider::ProviderBuilder;
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use alloy_rpc_types_eth::Filter;
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use alloy_rpc_types_eth::Log;
#[cfg(feature = "reactive-polling")]
use alloy_rpc_types_eth::{Block, Header};
use alloy_transport::mock::Asserter;
use anyhow::Result;
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use anyhow::bail;

#[cfg(feature = "reactive-ws")]
use evm_fork_cache::reactive::BlockInterestMode;
#[cfg(feature = "reactive-ws")]
use evm_fork_cache::reactive::SubscriberBackfill;
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use evm_fork_cache::reactive::SubscriberOwnerError;
use evm_fork_cache::reactive::{
    AlloySubscriber, EventSubscriber, PendingTxInterest, ReactiveInterest, SubscriberConfig,
    SubscriberError, SubscriberMode, SubscriberReconnectConfig,
};
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use evm_fork_cache::reactive::{BlockInterest, LogInterest};
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use evm_fork_cache::reactive::{
    BlockRef, HandlerId, SubscriberDriverPoll, SubscriberInputScope, SubscriberOwnerStart,
    SubscriberOwnerState,
};
#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
use evm_fork_cache::reactive::{ChainStatus, InputSource, ReactiveInput};

#[cfg(any(feature = "reactive-polling", feature = "reactive-ws"))]
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
fn removed_rpc_log(address: Address, topic0: B256, block_number: u64, log_index: u64) -> Log {
    let mut log = rpc_log(address, topic0, block_number, log_index);
    log.removed = true;
    log
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
fn indexed_address(index: u64) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..].copy_from_slice(&index.to_be_bytes());
    Address::from(bytes)
}

#[cfg(feature = "reactive-polling")]
fn polling_subscriber(
    asserter: Asserter,
    max_batch_size: usize,
) -> AlloySubscriber<impl alloy_provider::Provider<Ethereum> + Clone, Ethereum> {
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    AlloySubscriber::new(
        provider,
        SubscriberMode::Polling,
        SubscriberConfig {
            hydrate_pending_transactions: false,
            max_batch_size,
            ..SubscriberConfig::default()
        },
    )
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn staged_reconcile_subscribes_before_fetching_owner_backfill() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xa4);
    let topic = keccak256(b"Swap()");
    let through = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x65),
        parent_hash: Some(B256::repeat_byte(0x64)),
        timestamp: Some(1_700_000_101),
    };
    let log = rpc_log(pool, topic, 101, 1);
    // Strict response ordering is the assertion: eth_newFilter, pre-fetch
    // header verification, eth_getLogs, then post-fetch verification.
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![log.clone()]);
    asserter.push_success(&Some(rpc_block(&through)));
    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("pool-a"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool).event_signature(topic),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(BlockRef {
            number: 100,
            hash: B256::repeat_byte(0x64),
            parent_hash: Some(B256::repeat_byte(0x63)),
            timestamp: Some(1_700_000_100),
        }),
    )?;
    assert!(
        !subscriber.activate_interest_owner(&epoch),
        "post-block owner cannot activate before certified reconcile"
    );

    let progress = subscriber
        .reconcile_interest_owner(&epoch, through.clone())
        .await?;
    assert_eq!(progress.owner(), &epoch);
    assert_eq!(progress.through(), &through);
    assert!(subscriber.activate_interest_owner(&epoch));

    let batch = subscriber
        .next_scoped_batch()
        .await?
        .expect("owner-only catch-up record");
    assert_eq!(batch.records().len(), 1);
    assert!(
        matches!(&batch.records()[0].record().input, ReactiveInput::Log(actual) if actual == &log)
    );
    assert_eq!(
        batch.records()[0].scope(),
        &SubscriberInputScope::OwnerOnly {
            owners: vec![epoch]
        }
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn bulk_reconcile_routes_merged_backfill_to_exact_owner_epochs() -> Result<()> {
    let asserter = Asserter::new();
    let pool_a = Address::repeat_byte(0xb1);
    let pool_b = Address::repeat_byte(0xb2);
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
    let log_a = rpc_log(pool_a, topic, 101, 0);
    let log_b = rpc_log(pool_b, topic, 101, 1);

    // Both owners fan into one live polling filter, then one bounded
    // eth_getLogs call between exactly two target header certifications.
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![log_a.clone(), log_b.clone()]);
    asserter.push_success(&Some(rpc_block(&through)));
    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let epoch_a = subscriber.stage_interest_owner(
        HandlerId::new("bulk-pool-a"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool_a).event_signature(topic),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline.clone()),
    )?;
    let epoch_b = subscriber.stage_interest_owner(
        HandlerId::new("bulk-pool-b"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool_b).event_signature(topic),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline),
    )?;

    let progress = subscriber
        .reconcile_interest_owners(&[epoch_a.clone(), epoch_b.clone()], through.clone())
        .await?;
    assert_eq!(progress.len(), 2);
    assert_eq!(progress[0].owner(), &epoch_a);
    assert_eq!(progress[1].owner(), &epoch_b);
    assert!(progress.iter().all(|item| item.through() == &through));
    assert!(
        asserter.read_q().is_empty(),
        "unexpected reconciliation RPCs"
    );

    let batch = subscriber
        .next_scoped_batch()
        .await?
        .expect("both merged catch-up logs");
    assert_eq!(batch.records().len(), 2);
    assert_eq!(
        batch.records()[0].scope(),
        &SubscriberInputScope::OwnerOnly {
            owners: vec![epoch_a]
        }
    );
    assert_eq!(
        batch.records()[1].scope(),
        &SubscriberInputScope::OwnerOnly {
            owners: vec![epoch_b]
        }
    );
    assert!(matches!(&batch.records()[0].record().input, ReactiveInput::Log(log) if log == &log_a));
    assert!(matches!(&batch.records()[1].record().input, ReactiveInput::Log(log) if log == &log_b));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn bulk_reconcile_preserves_exact_windows_for_mixed_owner_baselines() -> Result<()> {
    let asserter = Asserter::new();
    let pool_a = Address::repeat_byte(0xb5);
    let pool_b = Address::repeat_byte(0xb6);
    let baseline_a = BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    };
    let baseline_b = BlockRef {
        number: 102,
        hash: B256::repeat_byte(0x66),
        parent_hash: Some(B256::repeat_byte(0x65)),
        timestamp: Some(1_700_000_102),
    };
    let through = BlockRef {
        number: 104,
        hash: B256::repeat_byte(0x68),
        parent_hash: Some(B256::repeat_byte(0x67)),
        timestamp: Some(1_700_000_104),
    };
    let log_a = rpc_log(pool_a, keccak256(b"Swap()"), 101, 0);
    let log_b = rpc_log(pool_b, keccak256(b"Swap()"), 103, 1);

    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&Some(rpc_block(&baseline_a)));
    asserter.push_success(&Some(rpc_block(&baseline_b)));
    // The two different N+1 windows cannot be broadened into one request.
    asserter.push_success(&vec![log_a.clone()]);
    asserter.push_success(&vec![log_b.clone()]);
    asserter.push_success(&Some(rpc_block(&through)));

    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let epoch_a = subscriber.stage_interest_owner(
        HandlerId::new("mixed-a"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool_a),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline_a),
    )?;
    let epoch_b = subscriber.stage_interest_owner(
        HandlerId::new("mixed-b"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool_b),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline_b),
    )?;

    subscriber
        .reconcile_interest_owners(&[epoch_a.clone(), epoch_b.clone()], through)
        .await?;
    let batch = subscriber
        .next_scoped_batch()
        .await?
        .expect("both exact owner windows publish");
    assert_eq!(batch.records().len(), 2);
    assert!(matches!(&batch.records()[0].record().input, ReactiveInput::Log(log) if log == &log_a));
    assert_eq!(
        batch.records()[0].scope(),
        &SubscriberInputScope::OwnerOnly {
            owners: vec![epoch_a]
        }
    );
    assert!(matches!(&batch.records()[1].record().input, ReactiveInput::Log(log) if log == &log_b));
    assert_eq!(
        batch.records()[1].scope(),
        &SubscriberInputScope::OwnerOnly {
            owners: vec![epoch_b]
        }
    );
    assert!(asserter.read_q().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn bulk_reconcile_one_thousand_owners_uses_one_certification_and_bounded_log_chunks()
-> Result<()> {
    const OWNER_COUNT: usize = 1_024;
    const FILTERS_PER_CHUNK: usize = 256;

    let asserter = Asserter::new();
    let baseline = BlockRef {
        number: 200,
        hash: B256::repeat_byte(0xc8),
        parent_hash: Some(B256::repeat_byte(0xc7)),
        timestamp: Some(1_700_000_200),
    };
    let through = BlockRef {
        number: 201,
        hash: B256::repeat_byte(0xc9),
        parent_hash: Some(B256::repeat_byte(0xc8)),
        timestamp: Some(1_700_000_201),
    };

    // Stream installation fans all 1,024 addresses into one provider filter.
    // Reconciliation itself adds only two header RPCs plus one getLogs request
    // per bounded chunk.
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    for _ in 0..OWNER_COUNT.div_ceil(FILTERS_PER_CHUNK) {
        asserter.push_success(&Vec::<Log>::new());
    }
    asserter.push_success(&Some(rpc_block(&through)));

    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let mut epochs = Vec::with_capacity(OWNER_COUNT);
    for index in 0..OWNER_COUNT {
        epochs.push(subscriber.stage_interest_owner(
            HandlerId::new(format!("bulk-owner-{index}")),
            &[ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(indexed_address(index as u64)),
                local_matcher: None,
                route_key: None,
            })],
            SubscriberOwnerStart::PostBlock(baseline.clone()),
        )?);
    }

    let progress = subscriber
        .reconcile_interest_owners(&epochs, through.clone())
        .await?;
    assert_eq!(progress.len(), OWNER_COUNT);
    assert!(
        progress
            .iter()
            .zip(&epochs)
            .all(|(item, epoch)| item.owner() == epoch && item.through() == &through)
    );
    assert!(
        asserter.read_q().is_empty(),
        "unexpected reconciliation RPCs"
    );

    // Every epoch is already at the requested point. The bulk call still
    // certifies that exact hash once before and once after, advances all 1,024
    // progress results, and performs no empty-range eth_getLogs request.
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&Some(rpc_block(&through)));
    let current_point = subscriber
        .reconcile_interest_owners(&epochs, through.clone())
        .await?;
    assert_eq!(current_point.len(), OWNER_COUNT);
    assert!(current_point.iter().all(|item| item.through() == &through));
    assert!(asserter.read_q().is_empty(), "empty ranges issued log RPCs");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn bulk_reconcile_failure_commits_no_owner_records_or_progress() -> Result<()> {
    let asserter = Asserter::new();
    let pool_a = Address::repeat_byte(0xb3);
    let pool_b = Address::repeat_byte(0xb4);
    let baseline = BlockRef {
        number: 300,
        hash: B256::repeat_byte(0x2c),
        parent_hash: Some(B256::repeat_byte(0x2b)),
        timestamp: Some(1_700_000_300),
    };
    let through = BlockRef {
        number: 301,
        hash: B256::repeat_byte(0x2d),
        parent_hash: Some(B256::repeat_byte(0x2c)),
        timestamp: Some(1_700_000_301),
    };
    let mut reorged = through.clone();
    reorged.hash = B256::repeat_byte(0xee);

    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![rpc_log(pool_a, keccak256(b"Swap()"), 301, 0)]);
    asserter.push_success(&Some(rpc_block(&reorged)));
    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let mut epochs = Vec::new();
    for (name, pool) in [("atomic-a", pool_a), ("atomic-b", pool_b)] {
        epochs.push(subscriber.stage_interest_owner(
            HandlerId::new(name),
            &[ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(pool),
                local_matcher: None,
                route_key: None,
            })],
            SubscriberOwnerStart::PostBlock(baseline.clone()),
        )?);
    }

    let error = subscriber
        .reconcile_interest_owners(&epochs, through)
        .await
        .expect_err("a failed final certification must roll back every owner");
    assert!(matches!(error, SubscriberOwnerError::BlockMismatch { .. }));
    assert!(
        epochs
            .iter()
            .all(|epoch| subscriber.interest_owner_progress(epoch).is_none())
    );
    asserter.push_success(&Vec::<Log>::new());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), subscriber.next_scoped_batch())
            .await
            .is_err(),
        "failed reconciliation must not leave an immediately publishable record"
    );
    assert!(
        epochs
            .iter()
            .all(|epoch| subscriber.abort_interest_owner(epoch))
    );
    assert!(subscriber.next_scoped_batch().await?.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn bulk_reconcile_rejects_conflicting_global_log_positions_across_chunks() -> Result<()> {
    const OWNER_COUNT: usize = 257;
    let asserter = Asserter::new();
    let baseline = BlockRef {
        number: 400,
        hash: B256::repeat_byte(0x90),
        parent_hash: Some(B256::repeat_byte(0x8f)),
        timestamp: Some(1_700_000_400),
    };
    let through = BlockRef {
        number: 401,
        hash: B256::repeat_byte(0x91),
        parent_hash: Some(baseline.hash),
        timestamp: Some(1_700_000_401),
    };
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    let first = rpc_log(indexed_address(0), keccak256(b"Swap()"), 401, 7);
    let mut conflicting = rpc_log(
        indexed_address((OWNER_COUNT - 1) as u64),
        keccak256(b"Swap()"),
        401,
        7,
    );
    conflicting.transaction_hash = Some(B256::repeat_byte(0xfe));
    asserter.push_success(&vec![first]);
    asserter.push_success(&vec![conflicting]);
    asserter.push_success(&Some(rpc_block(&through)));

    let mut subscriber = polling_subscriber(asserter, 32);
    let mut epochs = Vec::with_capacity(OWNER_COUNT);
    for index in 0..OWNER_COUNT {
        epochs.push(subscriber.stage_interest_owner(
            HandlerId::new(format!("coordinate-owner-{index}")),
            &[ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(indexed_address(index as u64)),
                local_matcher: None,
                route_key: None,
            })],
            SubscriberOwnerStart::PostBlock(baseline.clone()),
        )?);
    }

    let error = subscriber
        .reconcile_interest_owners(&epochs, through)
        .await
        .expect_err("one canonical log position cannot identify two transactions");
    assert!(matches!(
        error,
        SubscriberOwnerError::InvalidBackfillLog(
            "conflicting logs at one canonical block position"
        )
    ));
    assert!(
        epochs
            .iter()
            .all(|epoch| subscriber.interest_owner_progress(epoch).is_none())
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn bulk_reconcile_globally_orders_canonical_logs_returned_by_different_chunks() -> Result<()>
{
    const OWNER_COUNT: usize = 257;
    let asserter = Asserter::new();
    let baseline = BlockRef {
        number: 500,
        hash: B256::repeat_byte(0xa0),
        parent_hash: Some(B256::repeat_byte(0x9f)),
        timestamp: Some(1_700_000_500),
    };
    let through = BlockRef {
        number: 501,
        hash: B256::repeat_byte(501u64 as u8),
        parent_hash: Some(baseline.hash),
        timestamp: Some(1_700_000_501),
    };
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    let later = rpc_log(indexed_address(0), keccak256(b"Swap()"), 501, 9);
    let earlier = rpc_log(
        indexed_address((OWNER_COUNT - 1) as u64),
        keccak256(b"Swap()"),
        501,
        1,
    );
    // Provider request/chunk order is deliberately the reverse of canonical
    // transaction/log order.
    asserter.push_success(&vec![later.clone()]);
    asserter.push_success(&vec![earlier.clone()]);
    asserter.push_success(&Some(rpc_block(&through)));

    let mut subscriber = polling_subscriber(asserter, OWNER_COUNT);
    let mut epochs = Vec::with_capacity(OWNER_COUNT);
    for index in 0..OWNER_COUNT {
        epochs.push(subscriber.stage_interest_owner(
            HandlerId::new(format!("ordered-owner-{index}")),
            &[ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(indexed_address(index as u64)),
                local_matcher: None,
                route_key: None,
            })],
            SubscriberOwnerStart::PostBlock(baseline.clone()),
        )?);
    }

    subscriber
        .reconcile_interest_owners(&epochs, through)
        .await?;
    let batch = subscriber
        .next_scoped_batch()
        .await?
        .expect("both chunks publish one globally ordered batch");
    assert_eq!(batch.records().len(), 2);
    assert!(
        matches!(&batch.records()[0].record().input, ReactiveInput::Log(log) if log == &earlier)
    );
    assert!(matches!(&batch.records()[1].record().input, ReactiveInput::Log(log) if log == &later));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn empty_owner_reconcile_still_publishes_exact_hash_certified_progress() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xa5);
    let through = BlockRef {
        number: 105,
        hash: B256::repeat_byte(0x69),
        parent_hash: Some(B256::repeat_byte(0x68)),
        timestamp: Some(1_700_000_105),
    };
    let next = BlockRef {
        number: 106,
        hash: B256::repeat_byte(0x6a),
        parent_hash: Some(B256::repeat_byte(0x69)),
        timestamp: Some(1_700_000_106),
    };
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&Some(rpc_block(&BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    })));
    asserter.push_success(&Vec::<Log>::new());
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&Some(rpc_block(&next)));
    asserter.push_success(&vec![rpc_log(pool, keccak256(b"Swap()"), 106, 0)]);
    asserter.push_success(&Some(rpc_block(&next)));
    let failed_target = BlockRef {
        number: 107,
        hash: B256::repeat_byte(0x6b),
        parent_hash: Some(B256::repeat_byte(0x6a)),
        timestamp: Some(1_700_000_107),
    };
    asserter.push_success(&Some(rpc_block(&failed_target)));
    asserter.push_failure_msg("incremental catch-up unavailable");
    let mut subscriber = polling_subscriber(asserter, 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("pool-empty"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(BlockRef {
            number: 100,
            hash: B256::repeat_byte(0x64),
            parent_hash: Some(B256::repeat_byte(0x63)),
            timestamp: Some(1_700_000_100),
        }),
    )?;

    let progress = subscriber
        .reconcile_interest_owner(&epoch, through.clone())
        .await?;
    assert_eq!(progress.through(), &through);
    assert_eq!(subscriber.interest_owner_progress(&epoch), Some(&progress));
    let next_progress = subscriber
        .reconcile_interest_owner(&epoch, next.clone())
        .await?;
    assert_eq!(next_progress.through(), &next);
    let batch = subscriber
        .next_scoped_batch()
        .await?
        .expect("incremental N+1 catch-up record");
    assert!(matches!(
        &batch.records()[0].record().input,
        ReactiveInput::Log(log) if log.block_number == Some(106)
    ));
    let failed = subscriber
        .reconcile_interest_owner(&epoch, failed_target)
        .await
        .expect_err("failed incremental fetch must preserve prior progress");
    assert!(matches!(failed, SubscriberOwnerError::Subscriber(_)));
    assert_eq!(
        subscriber.interest_owner_progress(&epoch),
        Some(&next_progress)
    );
    let regression = subscriber
        .reconcile_interest_owner(
            &epoch,
            BlockRef {
                number: 104,
                hash: B256::repeat_byte(0x68),
                parent_hash: None,
                timestamp: None,
            },
        )
        .await
        .expect_err("owner progress must never move backwards");
    assert!(matches!(
        regression,
        SubscriberOwnerError::ProgressRegression {
            current: 106,
            target: 104
        }
    ));
    assert_eq!(
        subscriber.interest_owner_progress(&epoch),
        Some(&next_progress)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn reconcile_at_baseline_certifies_progress_without_log_request() -> Result<()> {
    let asserter = Asserter::new();
    let point = BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    };
    // Stream install, then the two header checks. There is intentionally no
    // eth_getLogs response because [N + 1, N] is empty.
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&point)));
    asserter.push_success(&Some(rpc_block(&point)));
    let mut subscriber = polling_subscriber(asserter, 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("pool-at-baseline"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(0xa8)),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(point.clone()),
    )?;

    let progress = subscriber
        .reconcile_interest_owner(&epoch, point.clone())
        .await?;
    assert_eq!(progress.through(), &point);
    assert!(subscriber.activate_interest_owner(&epoch));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn reconcile_rejects_same_height_hash_replacement_before_provider_io() -> Result<()> {
    let asserter = Asserter::new();
    let baseline = BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    };
    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("same-height-replacement"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(0xab)),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline.clone()),
    )?;
    let replacement = BlockRef {
        hash: B256::repeat_byte(0xee),
        ..baseline
    };

    let error = subscriber
        .reconcile_interest_owner(&epoch, replacement)
        .await
        .expect_err("progress cannot be rewritten to another hash at one height");
    assert!(matches!(
        error,
        SubscriberOwnerError::ProgressConflict { number: 100, .. }
    ));
    assert!(
        asserter.read_q().is_empty(),
        "preflight must perform no RPC"
    );
    assert!(subscriber.interest_owner_progress(&epoch).is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn empty_interest_owner_reconciles_without_a_live_stream_topology() -> Result<()> {
    let asserter = Asserter::new();
    let baseline = BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    };
    let through = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x65),
        parent_hash: Some(baseline.hash),
        timestamp: Some(1_700_000_101),
    };
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&Some(rpc_block(&through)));
    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("empty-owner"),
        &[],
        SubscriberOwnerStart::PostBlock(baseline),
    )?;

    let progress = subscriber
        .reconcile_interest_owner(&epoch, through.clone())
        .await?;
    assert_eq!(progress.through(), &through);
    assert!(subscriber.activate_interest_owner(&epoch));
    assert!(subscriber.next_scoped_batch().await?.is_none());
    assert!(asserter.read_q().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn multi_block_reconcile_rejects_a_replaced_retained_baseline() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xa9);
    let baseline = BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    };
    let through = BlockRef {
        number: 105,
        hash: B256::repeat_byte(0x69),
        parent_hash: Some(B256::repeat_byte(0x68)),
        timestamp: Some(1_700_000_105),
    };
    let mut replaced_baseline = baseline.clone();
    replaced_baseline.hash = B256::repeat_byte(0xee);

    // Subscribe, certify the requested target, then prove that the retained
    // baseline is still on the provider's canonical chain before fetching.
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&Some(rpc_block(&replaced_baseline)));
    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("replaced-baseline"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline),
    )?;

    let error = subscriber
        .reconcile_interest_owner(&epoch, through)
        .await
        .expect_err("a target cannot advance from a non-canonical retained baseline");
    assert!(matches!(error, SubscriberOwnerError::BlockMismatch { .. }));
    assert!(subscriber.interest_owner_progress(&epoch).is_none());
    assert_eq!(
        subscriber.interest_owner_state(&epoch),
        Some(SubscriberOwnerState::Staged)
    );
    assert!(asserter.read_q().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn reconcile_hash_mismatch_publishes_nothing_and_remains_abortable() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xa6);
    let topic = keccak256(b"Swap()");
    let expected = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x65),
        parent_hash: Some(B256::repeat_byte(0x64)),
        timestamp: Some(1_700_000_101),
    };
    let mut actual = expected.clone();
    actual.hash = B256::repeat_byte(0xee);
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&expected)));
    asserter.push_success(&vec![rpc_log(pool, topic, 101, 0)]);
    asserter.push_success(&Some(rpc_block(&actual)));
    let mut subscriber = polling_subscriber(asserter, 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("pool-mismatch"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool).event_signature(topic),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(BlockRef {
            number: 100,
            hash: B256::repeat_byte(0x64),
            parent_hash: Some(B256::repeat_byte(0x63)),
            timestamp: Some(1_700_000_100),
        }),
    )?;

    let error = subscriber
        .reconcile_interest_owner(&epoch, expected)
        .await
        .expect_err("target hash mismatch must reject staged catch-up");
    assert!(matches!(error, SubscriberOwnerError::BlockMismatch { .. }));
    assert!(subscriber.interest_owner_progress(&epoch).is_none());
    assert_eq!(
        subscriber.interest_owner_state(&epoch),
        Some(SubscriberOwnerState::Staged)
    );
    assert!(subscriber.abort_interest_owner(&epoch));
    assert!(subscriber.next_scoped_batch().await?.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn reconcile_rejects_logs_without_canonical_transaction_position() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xaa);
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
        parent_hash: Some(baseline.hash),
        timestamp: Some(1_700_000_101),
    };
    let mut malformed = rpc_log(pool, topic, 101, 0);
    malformed.transaction_index = None;

    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![malformed]);
    let mut subscriber = polling_subscriber(asserter.clone(), 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("malformed-position"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool).event_signature(topic),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline),
    )?;

    let error = subscriber
        .reconcile_interest_owner(&epoch, through)
        .await
        .expect_err("canonical catch-up requires a transaction index");
    assert!(matches!(
        error,
        SubscriberOwnerError::InvalidBackfillLog("log missing transaction index")
    ));
    assert!(subscriber.interest_owner_progress(&epoch).is_none());
    assert!(asserter.read_q().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn reconcile_rejects_conflicting_transaction_identity_at_one_position() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xad);
    let baseline = BlockRef {
        number: 100,
        hash: B256::repeat_byte(0x64),
        parent_hash: Some(B256::repeat_byte(0x63)),
        timestamp: Some(1_700_000_100),
    };
    let through = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x65),
        parent_hash: Some(baseline.hash),
        timestamp: Some(1_700_000_101),
    };
    let first = rpc_log(pool, keccak256(b"Swap()"), 101, 0);
    let mut conflicting = rpc_log(pool, keccak256(b"Swap()"), 101, 1);
    conflicting.transaction_index = first.transaction_index;

    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![first, conflicting]);
    let mut subscriber = polling_subscriber(asserter, 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("conflicting-transaction-position"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(baseline),
    )?;

    let error = subscriber
        .reconcile_interest_owner(&epoch, through)
        .await
        .expect_err("one transaction position cannot identify two hashes");
    assert!(matches!(
        error,
        SubscriberOwnerError::InvalidBackfillLog(
            "conflicting transaction identity at one canonical block position"
        )
    ));
    assert!(subscriber.interest_owner_progress(&epoch).is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn control_priority_preserves_already_ready_scoped_batch() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xa7);
    let through = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0x65),
        parent_hash: Some(B256::repeat_byte(0x64)),
        timestamp: Some(1_700_000_101),
    };
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_success(&vec![rpc_log(pool, keccak256(b"Swap()"), 101, 0)]);
    asserter.push_success(&Some(rpc_block(&through)));
    let mut subscriber = polling_subscriber(asserter, 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("pool-control"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(BlockRef {
            number: 100,
            hash: B256::repeat_byte(0x64),
            parent_hash: Some(B256::repeat_byte(0x63)),
            timestamp: Some(1_700_000_100),
        }),
    )?;
    subscriber.reconcile_interest_owner(&epoch, through).await?;

    let control = futures::future::ready("remove-owner");
    futures::pin_mut!(control);
    let outcome = subscriber.next_scoped_batch_or(control.as_mut()).await?;
    assert!(matches!(
        outcome,
        SubscriberDriverPoll::Control("remove-owner")
    ));
    let (send_control, receive_control) = futures::channel::oneshot::channel();
    futures::pin_mut!(receive_control);
    let batch = match subscriber
        .next_scoped_batch_or(receive_control.as_mut())
        .await?
    {
        SubscriberDriverPoll::Batch(Some(batch)) => batch,
        _ => panic!("pending control must let the preserved batch win"),
    };
    assert_eq!(batch.records().len(), 1);
    send_control
        .send("shutdown")
        .expect("borrowed control receiver remains alive after batch win");
    assert_eq!(receive_control.await?, "shutdown");

    Ok(())
}

fn mock_subscriber(
    mode: SubscriberMode,
) -> AlloySubscriber<impl alloy_provider::Provider<Ethereum>, Ethereum> {
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    AlloySubscriber::new(provider, mode, SubscriberConfig::default())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-ws")]
async fn alloy_subscriber_auto_mode_uses_pubsub_by_default() -> Result<()> {
    let address = Address::repeat_byte(0xef);
    let topic0 = keccak256(b"AutoMode(uint256)");

    let mut subscriber = mock_subscriber(SubscriberMode::Auto);
    subscriber.register_interests(&[ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic0),
        local_matcher: None,
        route_key: None,
    })])?;

    assert_eq!(SubscriberMode::default(), SubscriberMode::Auto);
    assert_eq!(subscriber.registered_interests().len(), 1);

    let result = subscriber.next_batch().await;
    assert!(
        matches!(result, Err(SubscriberError::Provider(_))),
        "mock providers are not pubsub transports, so default auto/pubsub mode should report a provider error: {result:?}"
    );

    Ok(())
}

#[test]
#[cfg(feature = "reactive-ws")]
fn alloy_subscriber_pubsub_accepts_logs_pending_hashes_and_block_headers() -> Result<()> {
    let address = Address::repeat_byte(0xab);
    let topic0 = keccak256(b"SubscriberLog(uint256)");

    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    subscriber.register_interests(&[
        ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(address).event_signature(topic0),
            local_matcher: None,
            route_key: None,
        }),
        ReactiveInterest::PendingTransactions(PendingTxInterest::default()),
        ReactiveInterest::Blocks(BlockInterest::default()),
    ])?;

    assert_eq!(subscriber.registered_interests().len(), 3);

    Ok(())
}

#[test]
#[cfg(feature = "reactive-ws")]
fn alloy_subscriber_owner_interests_add_and_remove_incrementally() -> Result<()> {
    let pool_a = Address::repeat_byte(0xa1);
    let pool_b = Address::repeat_byte(0xb2);
    let topic_a = keccak256(b"PoolA(uint256)");
    let topic_b = keccak256(b"PoolB(uint256)");

    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    subscriber.add_interest_owner(
        HandlerId::new("pool-a"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool_a).event_signature(topic_a),
            local_matcher: None,
            route_key: None,
        })],
    )?;
    subscriber.add_interest_owner(
        HandlerId::new("pool-b"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool_b).event_signature(topic_b),
            local_matcher: None,
            route_key: None,
        })],
    )?;

    assert_eq!(subscriber.registered_interests().len(), 2);
    assert_eq!(
        subscriber
            .owner_interests(&HandlerId::new("pool-a"))
            .expect("pool-a interests")
            .len(),
        1
    );

    let removed = subscriber
        .remove_interest_owner(&HandlerId::new("pool-a"))
        .expect("pool-a should be removed");
    assert_eq!(removed.len(), 1);
    assert!(
        subscriber
            .owner_interests(&HandlerId::new("pool-a"))
            .is_none()
    );
    assert_eq!(subscriber.registered_interests().len(), 1);
    assert_eq!(
        subscriber
            .owner_interests(&HandlerId::new("pool-b"))
            .expect("pool-b interests")
            .len(),
        1
    );
    assert!(
        subscriber
            .remove_interest_owner(&HandlerId::new("missing"))
            .is_none()
    );

    Ok(())
}

#[test]
#[cfg(feature = "reactive-ws")]
fn alloy_subscriber_stages_activates_and_idempotently_aborts_owner_epochs() -> Result<()> {
    let pool = Address::repeat_byte(0xa1);
    let interest = ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(pool),
        local_matcher: None,
        route_key: None,
    });
    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);

    let staged = subscriber.stage_interest_owner(
        HandlerId::new("pool-a"),
        std::slice::from_ref(&interest),
        SubscriberOwnerStart::Live,
    )?;
    assert_eq!(
        subscriber.interest_owner_state(&staged),
        Some(SubscriberOwnerState::Staged)
    );
    assert!(subscriber.abort_interest_owner(&staged));
    assert!(!subscriber.abort_interest_owner(&staged));
    assert!(
        subscriber.owner_interests(staged.owner()).is_none(),
        "aborting a staged epoch must remove its desired interests"
    );

    let active = subscriber.stage_interest_owner(
        HandlerId::new("pool-a"),
        &[interest],
        SubscriberOwnerStart::Live,
    )?;
    assert!(active.sequence() > staged.sequence());
    assert!(!subscriber.activate_interest_owner(&staged));
    assert!(subscriber.activate_interest_owner(&active));
    assert_eq!(
        subscriber.interest_owner_state(&active),
        Some(SubscriberOwnerState::Active)
    );
    assert!(
        !subscriber.abort_interest_owner(&active),
        "an activated owner is canonical and must not be removed by staged abort"
    );

    Ok(())
}

#[test]
#[cfg(feature = "reactive-ws")]
fn alloy_subscriber_prepares_cancels_and_finalizes_exact_owner_removal() -> Result<()> {
    let pool = Address::repeat_byte(0xa3);
    let interest = ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(pool),
        local_matcher: None,
        route_key: None,
    });
    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("pool-a"),
        std::slice::from_ref(&interest),
        SubscriberOwnerStart::Live,
    )?;
    assert!(subscriber.activate_interest_owner(&epoch));

    assert!(subscriber.prepare_interest_owner_removal(&epoch));
    assert_eq!(
        subscriber.interest_owner_state(&epoch),
        Some(SubscriberOwnerState::Removing)
    );
    assert!(
        subscriber.owner_interests(epoch.owner()).is_some(),
        "prepare must retain interests so actor-side failure can roll back losslessly"
    );
    assert!(subscriber.abort_interest_owner(&epoch));
    assert_eq!(
        subscriber.interest_owner_state(&epoch),
        Some(SubscriberOwnerState::Active)
    );

    assert!(subscriber.prepare_interest_owner_removal(&epoch));
    let removed = subscriber
        .finalize_interest_owner_removal(&epoch)
        .expect("prepared exact epoch is removable");
    assert_eq!(removed.len(), 1);
    assert!(subscriber.interest_owner_state(&epoch).is_none());
    assert!(subscriber.finalize_interest_owner_removal(&epoch).is_none());

    let replacement = subscriber.stage_interest_owner(
        HandlerId::new("pool-a"),
        &[interest],
        SubscriberOwnerStart::Live,
    )?;
    assert_ne!(replacement, epoch);
    assert!(!subscriber.prepare_interest_owner_removal(&epoch));
    assert_eq!(
        subscriber.interest_owner_state(&replacement),
        Some(SubscriberOwnerState::Staged)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn staged_owner_abort_cleans_reconcile_state_after_provider_error() -> Result<()> {
    let asserter = Asserter::new();
    let through = BlockRef {
        number: 51,
        hash: B256::repeat_byte(51),
        parent_hash: Some(B256::repeat_byte(50)),
        timestamp: Some(1_700_000_051),
    };
    asserter.push_success(&U256::from(1));
    asserter.push_success(&Some(rpc_block(&through)));
    asserter.push_failure_msg("rate limited after owner stage");
    let mut subscriber = polling_subscriber(asserter, 16);
    let epoch = subscriber.stage_interest_owner(
        HandlerId::new("pool-failing"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(0xf1)),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(BlockRef {
            number: 50,
            hash: B256::repeat_byte(50),
            parent_hash: Some(B256::repeat_byte(49)),
            timestamp: Some(1_700_000_050),
        }),
    )?;

    let error = subscriber
        .reconcile_interest_owner(&epoch, through)
        .await
        .expect_err("subscribed owner reconcile should surface provider failure");
    assert!(matches!(error, SubscriberOwnerError::Subscriber(_)));
    assert_eq!(
        subscriber.interest_owner_state(&epoch),
        Some(SubscriberOwnerState::Staged),
        "provider failure occurs after desired interests and a live stream were installed"
    );

    assert!(subscriber.abort_interest_owner(&epoch));
    assert!(!subscriber.abort_interest_owner(&epoch));
    assert!(subscriber.next_scoped_batch().await?.is_none());

    Ok(())
}

#[test]
#[cfg(feature = "reactive-ws")]
fn staging_rejects_post_block_overflow_without_mutating_interests() {
    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    let result = subscriber.stage_interest_owner(
        HandlerId::new("pool-overflow"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(0xff)),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberOwnerStart::PostBlock(BlockRef {
            number: u64::MAX,
            hash: B256::repeat_byte(0xff),
            parent_hash: None,
            timestamp: None,
        }),
    );

    assert!(result.is_err());
    assert!(subscriber.registered_interests().is_empty());
}

#[test]
#[cfg(feature = "reactive-ws")]
fn staging_rejects_non_log_post_block_interests() {
    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    let result = subscriber.stage_interest_owner(
        HandlerId::new("historical-header"),
        &[ReactiveInterest::Blocks(BlockInterest::default())],
        SubscriberOwnerStart::PostBlock(BlockRef {
            number: 100,
            hash: B256::repeat_byte(0x64),
            parent_hash: Some(B256::repeat_byte(0x63)),
            timestamp: Some(1_700_000_100),
        }),
    );

    assert!(matches!(
        result,
        Err(SubscriberOwnerError::UnsupportedPostBlockInterest)
    ));
    assert!(subscriber.registered_interests().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-ws")]
async fn alloy_subscriber_owner_backfill_yields_before_live_streams() -> Result<()> {
    let asserter = Asserter::new();
    let pool = Address::repeat_byte(0xcd);
    let topic = keccak256(b"DiscoveredPool(uint256)");
    let log = rpc_log(pool, topic, 42, 3);
    asserter.push_success(&vec![log.clone()]);

    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let mut subscriber = AlloySubscriber::new(
        provider,
        SubscriberMode::PubSub,
        SubscriberConfig {
            hydrate_pending_transactions: false,
            max_batch_size: 16,
            ..SubscriberConfig::default()
        },
    );
    subscriber.add_interest_owner_with_backfill(
        HandlerId::new("pool-cd"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool).event_signature(topic),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberBackfill::range(40, 42),
    )?;

    let Some(batch) = subscriber.next_batch().await? else {
        bail!("expected owner-scoped backfill batch before live subscription");
    };
    let records = batch.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].context.source, InputSource::Backfill);
    assert!(
        matches!(records[0].context.chain_status, ChainStatus::Included { ref block, confirmations: 0 } if block.number == 42)
    );
    assert!(matches!(&records[0].input, ReactiveInput::Log(actual) if actual == &log));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-ws")]
async fn alloy_subscriber_owner_growth_backfills_continuity_gap_end_to_end() -> Result<()> {
    // An established owner (pool A) has delivered up to block 100. Growing it to
    // also watch pool B changes the merged filter shape; the subscriber must
    // automatically backfill the new shape from the prior anchor so nothing is
    // lost across the change. Driven entirely through the public API.
    let pool_a = Address::repeat_byte(0xaa);
    let pool_b = Address::repeat_byte(0xbb);

    let asserter = Asserter::new();
    // (1) Establish pool A's anchor at 100 via an explicit range backfill that
    //     returns a log — the returned record makes next_batch yield before it
    //     ever reaches live-stream init.
    let anchor_log = rpc_log(pool_a, keccak256(b"Swap()"), 100, 0);
    asserter.push_success(&vec![anchor_log.clone()]);
    // (2) Continuity backfill for the grown {A,B} shape is open-ended from the
    //     prior anchor: get_block_number, then get_logs over [100, 105].
    asserter.push_success(&105u64);
    let gap_log = rpc_log(pool_b, keccak256(b"Swap()"), 103, 0);
    asserter.push_success(&vec![gap_log.clone()]);

    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let mut subscriber = AlloySubscriber::new(
        provider,
        SubscriberMode::PubSub,
        SubscriberConfig {
            hydrate_pending_transactions: false,
            max_batch_size: 16,
            ..SubscriberConfig::default()
        },
    );

    // Establish pool A with an explicit backfill through block 100.
    subscriber.add_interest_owner_with_backfill(
        HandlerId::new("amm"),
        &[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(pool_a),
            local_matcher: None,
            route_key: None,
        })],
        SubscriberBackfill::range(90, 100),
    )?;
    let Some(first) = subscriber.next_batch().await? else {
        bail!("expected the establishing backfill batch");
    };
    assert!(
        matches!(&first.records()[0].input, ReactiveInput::Log(actual) if actual == &anchor_log)
    );

    // Grow the owner to A+B (same block option -> merged shape changes).
    subscriber.add_interest_owner(
        HandlerId::new("amm"),
        &[
            ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(pool_a),
                local_matcher: None,
                route_key: None,
            }),
            ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(pool_b),
                local_matcher: None,
                route_key: None,
            }),
        ],
    )?;

    let Some(batch) = subscriber.next_batch().await? else {
        bail!("expected a continuity backfill batch for the grown owner");
    };
    let records = batch.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].context.source, InputSource::Backfill);
    assert!(matches!(&records[0].input, ReactiveInput::Log(actual) if actual == &gap_log));

    Ok(())
}

#[test]
#[cfg(feature = "reactive-ws")]
fn alloy_subscriber_pubsub_rejects_full_body_modes() -> Result<()> {
    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    let full_pending = subscriber.register_interests(&[ReactiveInterest::PendingTransactions(
        PendingTxInterest {
            full_transactions: true,
            ..PendingTxInterest::default()
        },
    )]);
    assert!(matches!(full_pending, Err(SubscriberError::Unsupported(_))));

    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    let full_block = subscriber.register_interests(&[ReactiveInterest::Blocks(BlockInterest {
        mode: BlockInterestMode::FullBlock,
    })]);
    assert!(matches!(full_block, Err(SubscriberError::Unsupported(_))));

    Ok(())
}

#[test]
#[cfg(not(feature = "reactive-ws"))]
fn alloy_subscriber_pubsub_requires_ws_feature() -> Result<()> {
    let mut subscriber = mock_subscriber(SubscriberMode::PubSub);
    let result = subscriber.register_interests(&[ReactiveInterest::PendingTransactions(
        PendingTxInterest::default(),
    )]);
    assert!(matches!(result, Err(SubscriberError::Unsupported(_))));

    Ok(())
}

#[test]
#[cfg(not(feature = "reactive-polling"))]
fn alloy_subscriber_polling_requires_polling_feature() -> Result<()> {
    let mut subscriber = mock_subscriber(SubscriberMode::Polling);
    let result = subscriber.register_interests(&[ReactiveInterest::PendingTransactions(
        PendingTxInterest::default(),
    )]);
    assert!(matches!(result, Err(SubscriberError::Unsupported(_))));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn alloy_subscriber_polling_logs_yield_reactive_records() -> Result<()> {
    let asserter = Asserter::new();
    let address = Address::repeat_byte(0xab);
    let topic0 = keccak256(b"SubscriberLog(uint256)");
    let log = rpc_log(address, topic0, 42, 7);

    asserter.push_success(&U256::from(1));
    asserter.push_success(&vec![log.clone()]);

    let mut subscriber = polling_subscriber(asserter, 16);
    subscriber.register_interests(&[ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic0),
        local_matcher: None,
        route_key: None,
    })])?;

    let Some(batch) = subscriber.next_batch().await? else {
        bail!("expected one batch from the polling log stream");
    };
    let records = batch.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].context.source, InputSource::Poll);
    assert_eq!(records[0].context.transaction_index, Some(7));
    assert_eq!(records[0].context.log_index, Some(7));
    assert!(
        matches!(records[0].context.chain_status, ChainStatus::Included { ref block, confirmations: 0 } if block.number == 42 && block.hash == B256::repeat_byte(42))
    );
    assert!(matches!(&records[0].input, ReactiveInput::Log(actual) if actual == &log));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(all(feature = "reactive-polling", not(feature = "reactive-ws")))]
async fn alloy_subscriber_auto_mode_uses_polling_when_ws_is_not_compiled() -> Result<()> {
    let asserter = Asserter::new();
    let address = Address::repeat_byte(0xef);
    let topic0 = keccak256(b"AutoMode(uint256)");
    let log = rpc_log(address, topic0, 50, 0);

    asserter.push_success(&U256::from(4));
    asserter.push_success(&vec![log.clone()]);

    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let mut subscriber = AlloySubscriber::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            hydrate_pending_transactions: false,
            max_batch_size: 16,
            ..SubscriberConfig::default()
        },
    );
    subscriber.register_interests(&[ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic0),
        local_matcher: None,
        route_key: None,
    })])?;

    let Some(batch) = subscriber.next_batch().await? else {
        bail!("expected auto mode to use polling and produce one batch");
    };

    assert_eq!(batch.records().len(), 1);
    assert!(matches!(&batch.records()[0].input, ReactiveInput::Log(actual) if actual == &log));
    assert_eq!(SubscriberMode::default(), SubscriberMode::Auto);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn alloy_subscriber_polling_pending_hashes_yield_pending_records() -> Result<()> {
    let asserter = Asserter::new();
    let hash = B256::repeat_byte(0x55);

    asserter.push_success(&U256::from(2));
    asserter.push_success(&vec![hash]);

    let mut subscriber = polling_subscriber(asserter, 16);
    subscriber.register_interests(&[ReactiveInterest::PendingTransactions(
        PendingTxInterest::default(),
    )])?;

    let Some(batch) = subscriber.next_batch().await? else {
        bail!("expected one batch from the polling pending transaction stream");
    };
    let records = batch.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].context.source, InputSource::Poll);
    assert!(matches!(
        records[0].context.chain_status,
        ChainStatus::Pending
    ));
    assert!(matches!(records[0].input, ReactiveInput::PendingTxHash(actual) if actual == hash));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn alloy_subscriber_removed_logs_yield_reorged_context() -> Result<()> {
    let asserter = Asserter::new();
    let address = Address::repeat_byte(0x12);
    let topic0 = keccak256(b"Removed(uint256)");
    let log = removed_rpc_log(address, topic0, 75, 2);

    asserter.push_success(&U256::from(5));
    asserter.push_success(&vec![log.clone()]);

    let mut subscriber = polling_subscriber(asserter, 16);
    subscriber.register_interests(&[ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic0),
        local_matcher: None,
        route_key: None,
    })])?;

    let Some(batch) = subscriber.next_batch().await? else {
        bail!("expected removed log batch");
    };
    let records = batch.records();
    assert_eq!(records.len(), 1);
    assert!(
        matches!(records[0].context.chain_status, ChainStatus::Reorged { ref dropped_from } if dropped_from.number == 75 && dropped_from.hash == B256::repeat_byte(75))
    );
    assert!(matches!(&records[0].input, ReactiveInput::Log(actual) if actual == &log));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn alloy_subscriber_respects_max_batch_size_for_polled_logs() -> Result<()> {
    let asserter = Asserter::new();
    let address = Address::repeat_byte(0xcd);
    let topic0 = keccak256(b"Chunked(uint256)");
    let first = rpc_log(address, topic0, 100, 0);
    let second = rpc_log(address, topic0, 100, 1);

    asserter.push_success(&U256::from(3));
    asserter.push_success(&vec![first.clone(), second.clone()]);

    let mut subscriber = polling_subscriber(asserter, 1);
    subscriber.register_interests(&[ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic0),
        local_matcher: None,
        route_key: None,
    })])?;

    let Some(first_batch) = subscriber.next_batch().await? else {
        bail!("expected first chunk");
    };
    let Some(second_batch) = subscriber.next_batch().await? else {
        bail!("expected second chunk");
    };

    assert_eq!(first_batch.records().len(), 1);
    assert_eq!(second_batch.records().len(), 1);
    assert!(
        matches!(&first_batch.records()[0].input, ReactiveInput::Log(actual) if actual == &first)
    );
    assert!(
        matches!(&second_batch.records()[0].input, ReactiveInput::Log(actual) if actual == &second)
    );

    Ok(())
}

#[test]
#[cfg(feature = "reactive-polling")]
fn alloy_subscriber_polling_block_streams_are_explicitly_unsupported() -> Result<()> {
    let mut polling = mock_subscriber(SubscriberMode::Polling);
    let block_result =
        polling.register_interests(&[ReactiveInterest::Blocks(BlockInterest::default())]);
    assert!(matches!(block_result, Err(SubscriberError::Unsupported(_))));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn alloy_subscriber_provider_errors_are_reported() -> Result<()> {
    let asserter = Asserter::new();
    let address = Address::repeat_byte(0x34);
    let topic0 = keccak256(b"ProviderError(uint256)");

    let mut subscriber = polling_subscriber(asserter, 16);
    subscriber.register_interests(&[ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic0),
        local_matcher: None,
        route_key: None,
    })])?;

    let result = subscriber.next_batch().await;
    assert!(matches!(result, Err(SubscriberError::Provider(_))));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "reactive-polling")]
async fn alloy_subscriber_reports_dropped_polling_filters() -> Result<()> {
    let asserter = Asserter::new();
    let address = Address::repeat_byte(0x56);
    let topic0 = keccak256(b"DroppedFilter(uint256)");

    asserter.push_success(&U256::from(6));
    asserter.push_failure_msg("filter not found");

    let mut subscriber = polling_subscriber(asserter, 16);
    subscriber.register_interests(&[ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic0),
        local_matcher: None,
        route_key: None,
    })])?;

    let result = subscriber.next_batch().await;
    assert!(
        matches!(result, Err(SubscriberError::Provider(ref message)) if message.contains("terminated")),
        "dropped provider filters must be surfaced instead of returning Ok(None): {result:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn alloy_subscriber_zero_max_batch_size_is_rejected() -> Result<()> {
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            hydrate_pending_transactions: false,
            max_batch_size: 0,
            ..SubscriberConfig::default()
        },
    );

    let result = subscriber.register_interests(&[ReactiveInterest::PendingTransactions(
        PendingTxInterest::default(),
    )]);

    assert!(
        result.is_err(),
        "zero max_batch_size must be rejected instead of creating a subscriber that cannot make progress"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn alloy_subscriber_rejects_invalid_reconnect_config() -> Result<()> {
    let defaults = SubscriberReconnectConfig::default();
    assert_eq!(defaults.initial_delay, Duration::ZERO);
    assert_eq!(defaults.retry_delay, Duration::from_millis(250));
    assert_eq!(defaults.max_attempts, Some(3));

    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            reconnect: SubscriberReconnectConfig {
                retry_delay: Duration::from_secs(2),
                max_delay: Duration::from_secs(1),
                ..SubscriberReconnectConfig::default()
            },
            ..SubscriberConfig::default()
        },
    );

    let result = subscriber.register_interests(&[ReactiveInterest::PendingTransactions(
        PendingTxInterest::default(),
    )]);
    assert!(matches!(result, Err(SubscriberError::InvalidConfig(_))));

    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            reconnect: SubscriberReconnectConfig {
                max_attempts: Some(0),
                ..SubscriberReconnectConfig::default()
            },
            ..SubscriberConfig::default()
        },
    );

    let result = subscriber.register_interests(&[ReactiveInterest::PendingTransactions(
        PendingTxInterest::default(),
    )]);
    assert!(matches!(result, Err(SubscriberError::InvalidConfig(_))));

    Ok(())
}
