//! Manager-authored acceptance tests for the out-of-the-box Alloy subscriber.
//!
//! These tests pin the default WebSocket/pubsub subscriber surface and the
//! opt-in HTTP polling fallback.
#![cfg(feature = "reactive")]

use std::time::Duration;

use alloy_network::Ethereum;
use alloy_primitives::{Address, keccak256};
#[cfg(feature = "reactive-polling")]
use alloy_primitives::{B256, Bytes, Log as PrimitiveLog, U256};
use alloy_provider::ProviderBuilder;
use alloy_rpc_types_eth::Filter;
#[cfg(feature = "reactive-polling")]
use alloy_rpc_types_eth::Log;
use alloy_transport::mock::Asserter;
use anyhow::Result;
#[cfg(feature = "reactive-polling")]
use anyhow::bail;

#[cfg(feature = "reactive-ws")]
use evm_fork_cache::reactive::BlockInterestMode;
use evm_fork_cache::reactive::{
    AlloySubscriber, BlockInterest, EventSubscriber, LogInterest, PendingTxInterest,
    ReactiveInterest, SubscriberConfig, SubscriberError, SubscriberMode, SubscriberReconnectConfig,
};
#[cfg(feature = "reactive-polling")]
use evm_fork_cache::reactive::{ChainStatus, InputSource, ReactiveInput};

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
fn removed_rpc_log(address: Address, topic0: B256, block_number: u64, log_index: u64) -> Log {
    let mut log = rpc_log(address, topic0, block_number, log_index);
    log.removed = true;
    log
}

#[cfg(feature = "reactive-polling")]
fn polling_subscriber(
    asserter: Asserter,
    max_batch_size: usize,
) -> AlloySubscriber<impl alloy_provider::Provider<Ethereum>, Ethereum> {
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
