//! Offline end-to-end coverage for the optional raw JSON Flashblocks profile.
//!
//! A receipt-enriched application frame is normalized, admitted by a real
//! `AlloySubscriber`, applied to a real speculative `ReactiveRuntime` cache
//! branch, revoked, replaced, and finally reconciled by an ordinary canonical
//! subscriber batch. All providers are mocked; the speculative path performs
//! no request/response I/O.
#![cfg(all(feature = "raw-flashblocks-json", feature = "reactive-polling"))]

mod common;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256};
use alloy_provider::ProviderBuilder;
use alloy_rpc_types_eth::{Block, Filter, Header, Log};
use alloy_transport::mock::Asserter;
use anyhow::{Result, bail};
use common::{install_mock_erc20, setup_cache_with_asserter};
use evm_fork_cache::StateUpdate;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AlloySubscriber, BlockRef, ChainStatus, DeliveryScope, EventSubscriber,
    FlashblockIngressTiming, FlashblockRef, FlashblockUpdate, HandlerError, HandlerId,
    HandlerOutcome, InputSource, LogInterest, PreconfirmationMode, ProviderRef,
    RawJsonFlashblocksAdapter, ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveHandler,
    ReactiveInput, ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveRuntime,
    StateEffectQuality, SubscriberBackfill, SubscriberConfig, SubscriberMode, SubscriberRpcCause,
};

const POOL_SLOT: u64 = 0;
const CHAIN_ID: u64 = 1;
const BLOCK_NUMBER: u64 = 101;
const RAW_TRANSACTION: &str = "0x01";
const TRANSACTION_HASH: B256 =
    alloy_primitives::b256!("5fe7f977e71dba2ea1a68e21057beebb9be2ac30c6410aa38d4f3fbe41dcffd2");

struct PoolHandler {
    pool: Address,
}

impl ReactiveHandler<Ethereum> for PoolHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("raw-json-pool")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.pool),
            local_matcher: None,
            route_key: None,
        })]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                log.address(),
                U256::from(POOL_SLOT),
                U256::from_be_slice(log.data().data.as_ref()),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

fn raw_frame(pool: Address, topic: B256, value: U256) -> Vec<u8> {
    format!(
        r#"{{
            "payload_id":"0x1111111111111111",
            "index":0,
            "base":{{
                "parent_hash":"0x6464646464646464646464646464646464646464646464646464646464646464",
                "block_number":"0x65",
                "timestamp":"0x6553f165",
                "gas_limit":"0x1c9c380",
                "base_fee_per_gas":"0x7"
            }},
            "diff":{{
                "state_root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "block_hash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "transactions":["{RAW_TRANSACTION}"]
            }},
            "metadata":{{
                "block_number":{BLOCK_NUMBER},
                "receipts":{{
                    "{TRANSACTION_HASH:#x}":{{
                        "logs":[{{
                            "address":"{pool:#x}",
                            "topics":["{topic:#x}"],
                            "data":"0x{value:064x}"
                        }}]
                    }}
                }}
            }}
        }}"#,
    )
    .into_bytes()
}

fn canonical_log(pool: Address, topic: B256, value: U256) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(
            pool,
            vec![topic],
            Bytes::from(value.to_be_bytes::<32>().to_vec()),
        ),
        block_hash: Some(B256::repeat_byte(BLOCK_NUMBER as u8)),
        block_number: Some(BLOCK_NUMBER),
        block_timestamp: Some(1_700_000_000 + BLOCK_NUMBER),
        transaction_hash: Some(TRANSACTION_HASH),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

fn canonical_block() -> Block {
    Block::empty(Header {
        hash: B256::repeat_byte(BLOCK_NUMBER as u8),
        inner: alloy_consensus::Header {
            number: BLOCK_NUMBER,
            parent_hash: B256::repeat_byte(BLOCK_NUMBER.saturating_sub(1) as u8),
            timestamp: 1_700_000_000 + BLOCK_NUMBER,
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    })
}

fn canonical_parent() -> BlockRef {
    BlockRef {
        number: BLOCK_NUMBER - 1,
        hash: B256::repeat_byte((BLOCK_NUMBER - 1) as u8),
        parent_hash: Some(B256::repeat_byte((BLOCK_NUMBER - 2) as u8)),
        timestamp: Some(1_700_000_000 + BLOCK_NUMBER - 1),
    }
}

fn standardized_preview(pool: Address, topic: B256, value: U256) -> Result<(FlashblockRef, Log)> {
    let mut adapter = RawJsonFlashblocksAdapter::new(ProviderRef::new("raw-json", 1));
    let update = adapter
        .ingest_json(&raw_frame(pool, topic, value))?
        .ok_or_else(|| anyhow::anyhow!("expected standardized preview"))?;
    let FlashblockUpdate::Snapshot(snapshot) = update else {
        bail!("expected snapshot update")
    };
    let log = snapshot
        .logs
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("expected preview log"))?;
    Ok((snapshot.flashblock, log))
}

fn preconfirmed_batch(flashblock: FlashblockRef, log: Log) -> ReactiveInputBatch<Ethereum> {
    let provider = flashblock.provider.clone();
    let flashblock = Arc::new(flashblock);
    let context = ReactiveContext {
        chain_id: Some(CHAIN_ID),
        source: InputSource::Flashblocks,
        chain_status: ChainStatus::Preconfirmed {
            flashblock: flashblock.clone(),
        },
        block: Some(flashblock.block_ref()),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
    };
    ReactiveInputBatch::new(vec![
        ReactiveInputRecord::new(ReactiveInput::Log(log), context).with_provider(provider),
    ])
    .with_chain_id(CHAIN_ID)
    .with_delivery_scope(DeliveryScope::Preconfirmed)
}

fn canonical_batch(pool: Address, topic: B256, value: U256) -> ReactiveInputBatch<Ethereum> {
    let block = BlockRef {
        number: BLOCK_NUMBER,
        hash: B256::repeat_byte(BLOCK_NUMBER as u8),
        parent_hash: Some(canonical_parent().hash),
        timestamp: Some(1_700_000_000 + BLOCK_NUMBER),
    };
    let log = canonical_log(pool, topic, value);
    let context = ReactiveContext {
        chain_id: Some(CHAIN_ID),
        source: InputSource::Subscription,
        chain_status: ChainStatus::Included {
            block,
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
    };
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::Log(log),
        context,
    )])
    .with_chain_id(CHAIN_ID)
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_preview_invalidation_replacement_and_canonical_reconciliation_are_one_pipeline()
-> Result<()> {
    let pool = Address::repeat_byte(0x42);
    let topic = B256::repeat_byte(0x43);
    let value = U256::from(777);
    let (mut cache, cache_asserter) = setup_cache_with_asserter().await?;
    install_mock_erc20(&mut cache, pool);

    let subscriber_asserter = Asserter::new();
    subscriber_asserter.push_success(&U256::from(CHAIN_ID));
    subscriber_asserter.push_success(&U256::from(2));
    subscriber_asserter.push_success(&Some(canonical_block()));
    subscriber_asserter.push_success(&vec![canonical_log(pool, topic, value)]);
    subscriber_asserter.push_success(&Some(canonical_block()));
    let provider = ProviderBuilder::new().connect_mocked_client(subscriber_asserter.clone());
    let source = ProviderRef::new("raw-json", 1);
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Polling,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    subscriber.replace_interest_owners_with_global_backfill(
        vec![(
            HandlerId::new("raw-json-pool"),
            vec![ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(pool).event_signature(topic),
                local_matcher: None,
                route_key: None,
            })],
        )],
        SubscriberBackfill::range(BLOCK_NUMBER - 1, BLOCK_NUMBER),
    )?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PoolHandler { pool }))?;
    runtime.adopt_canonical_baseline(canonical_parent())?;
    let mut adapter = RawJsonFlashblocksAdapter::new(source);

    let first = adapter
        .ingest_json(&raw_frame(pool, topic, value))?
        .ok_or_else(|| anyhow::anyhow!("expected first raw snapshot"))?;
    let source_ingress = Instant::now() - Duration::from_millis(25);
    subscriber.ingest_flashblock_update_with_ingress(
        first,
        FlashblockIngressTiming::new(source_ingress),
    )?;
    let first_batch = subscriber
        .next_batch()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected first preconfirmation batch"))?;
    assert_eq!(first_batch.records().len(), 1);
    assert_eq!(
        first_batch
            .preconfirmation_timing()
            .expect("raw preview timing must survive subscriber batching")
            .source_ingress(),
        source_ingress
    );
    assert_eq!(
        first_batch.records()[0].context.source,
        InputSource::Flashblocks
    );
    runtime.ingest_batch(&mut cache, first_batch)?;
    assert_eq!(
        cache.cached_storage_value(pool, U256::from(POOL_SLOT)),
        Some(value)
    );
    assert!(runtime.active_preconfirmation().is_some());
    assert_eq!(subscriber.flashblocks_rpc_metrics().total_requests(), 0);
    // Cross-check the attributed counters against the Flashblocks-scoped ones:
    // an application-managed source must issue no provider requests of its own.
    for cause in [
        SubscriberRpcCause::FlashblocksSetup,
        SubscriberRpcCause::CanonicalHeadCertification,
        SubscriberRpcCause::PendingStateSample,
    ] {
        assert_eq!(subscriber.rpc_stats().by_cause(cause), 0);
    }
    assert!(cache_asserter.read_q().is_empty());

    let reset = adapter
        .reset(ProviderRef::new("raw-json", 2))?
        .ok_or_else(|| anyhow::anyhow!("expected active-source invalidation"))?;
    subscriber.ingest_flashblock_update(reset)?;
    let invalidation = subscriber
        .next_batch()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected invalidation batch"))?;
    assert!(invalidation.records().is_empty());
    runtime.ingest_batch(&mut cache, invalidation)?;
    assert_eq!(
        cache.cached_storage_value(pool, U256::from(POOL_SLOT)),
        Some(U256::ZERO)
    );
    assert!(runtime.active_preconfirmation().is_none());

    let replacement = adapter
        .ingest_json(&raw_frame(pool, topic, value))?
        .ok_or_else(|| anyhow::anyhow!("expected replacement raw snapshot"))?;
    subscriber.ingest_flashblock_update(replacement)?;
    let replacement_batch = subscriber
        .next_batch()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected replacement preconfirmation batch"))?;
    runtime.ingest_batch(&mut cache, replacement_batch)?;
    assert_eq!(
        runtime
            .active_preconfirmation()
            .map(|flashblock| flashblock.provider.generation),
        Some(2)
    );

    let Some(canonical) = subscriber.next_batch().await? else {
        bail!("expected canonical reconciliation batch");
    };
    assert_eq!(canonical.records().len(), 1);
    assert_eq!(canonical.records()[0].context.source, InputSource::Backfill);
    runtime.ingest_batch(&mut cache, canonical)?;
    assert_eq!(
        cache.cached_storage_value(pool, U256::from(POOL_SLOT)),
        Some(value)
    );
    assert!(runtime.active_preconfirmation().is_none());
    assert_eq!(subscriber.flashblocks_rpc_metrics().total_requests(), 0);
    // Cross-check the attributed counters against the Flashblocks-scoped ones:
    // an application-managed source must issue no provider requests of its own.
    for cause in [
        SubscriberRpcCause::FlashblocksSetup,
        SubscriberRpcCause::CanonicalHeadCertification,
        SubscriberRpcCause::PendingStateSample,
    ] {
        assert_eq!(subscriber.rpc_stats().by_cause(cause), 0);
    }
    assert!(cache_asserter.read_q().is_empty());
    assert!(subscriber_asserter.read_q().is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn replacing_interest_owners_immediately_restores_canonical_state() -> Result<()> {
    let pool = Address::repeat_byte(0x42);
    let topic = B256::repeat_byte(0x43);
    let value = U256::from(777);
    let (mut cache, cache_asserter) = setup_cache_with_asserter().await?;
    install_mock_erc20(&mut cache, pool);

    let subscriber_asserter = Asserter::new();
    subscriber_asserter.push_success(&U256::from(CHAIN_ID));
    subscriber_asserter.push_success(&U256::from(2));
    subscriber_asserter.push_success(&Some(canonical_block()));
    subscriber_asserter.push_success(&vec![canonical_log(pool, topic, value)]);
    subscriber_asserter.push_success(&Some(canonical_block()));
    let provider = ProviderBuilder::new().connect_mocked_client(subscriber_asserter.clone());
    let source = ProviderRef::new("raw-json", 1);
    let interest = ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(pool).event_signature(topic),
        local_matcher: None,
        route_key: None,
    });
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Polling,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    subscriber.replace_interest_owners_with_global_backfill(
        vec![((HandlerId::new("raw-json-pool")), vec![interest.clone()])],
        SubscriberBackfill::range(BLOCK_NUMBER - 1, BLOCK_NUMBER),
    )?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PoolHandler { pool }))?;
    runtime.adopt_canonical_baseline(canonical_parent())?;
    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    let preview = adapter
        .ingest_json(&raw_frame(pool, topic, value))?
        .ok_or_else(|| anyhow::anyhow!("expected raw snapshot"))?;
    subscriber.ingest_flashblock_update(preview)?;
    let preview_batch = subscriber
        .next_batch()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected preconfirmation batch"))?;
    runtime.ingest_batch(&mut cache, preview_batch)?;
    assert_eq!(
        cache.cached_storage_value(pool, U256::from(POOL_SLOT)),
        Some(value)
    );
    assert!(runtime.active_preconfirmation().is_some());

    subscriber.replace_interest_owners(vec![(HandlerId::new("raw-json-pool"), vec![interest])])?;
    let invalidation = subscriber
        .next_batch()
        .await?
        .ok_or_else(|| anyhow::anyhow!("owner replacement must revoke the preview"))?;
    assert!(invalidation.records().is_empty());
    runtime.ingest_batch(&mut cache, invalidation)?;

    assert_eq!(
        cache.cached_storage_value(pool, U256::from(POOL_SLOT)),
        Some(U256::ZERO)
    );
    assert!(runtime.active_preconfirmation().is_none());
    assert!(cache_asserter.read_q().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn speculative_cache_requires_the_exact_canonical_successor_lineage() -> Result<()> {
    let pool = Address::repeat_byte(0x42);
    let topic = B256::repeat_byte(0x43);
    let value = U256::from(777);
    let (preview, log) = standardized_preview(pool, topic, value)?;

    {
        let (mut cache, _) = setup_cache_with_asserter().await?;
        install_mock_erc20(&mut cache, pool);
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.register_handler(Arc::new(PoolHandler { pool }))?;
        runtime.adopt_canonical_baseline(canonical_parent())?;
        runtime.ingest_batch(&mut cache, preconfirmed_batch(preview.clone(), log.clone()))?;
        assert!(runtime.active_preconfirmation().is_some());
    }

    {
        let (mut cache, _) = setup_cache_with_asserter().await?;
        install_mock_erc20(&mut cache, pool);
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.register_handler(Arc::new(PoolHandler { pool }))?;
        runtime.adopt_canonical_baseline(canonical_parent())?;
        runtime.ingest_batch(&mut cache, preconfirmed_batch(preview.clone(), log.clone()))?;
        let mut wrong_parent = preview.clone();
        wrong_parent.parent_hash = Some(B256::repeat_byte(0x01));
        assert!(
            runtime
                .ingest_batch(&mut cache, preconfirmed_batch(wrong_parent, log.clone()))
                .expect_err("wrong-parent preview must fail closed")
                .to_string()
                .contains("parent")
        );
        assert!(runtime.active_preconfirmation().is_none());
        assert_eq!(
            cache.cached_storage_value(pool, U256::from(POOL_SLOT)),
            Some(U256::ZERO)
        );
    }

    {
        let (mut cache, _) = setup_cache_with_asserter().await?;
        install_mock_erc20(&mut cache, pool);
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.register_handler(Arc::new(PoolHandler { pool }))?;
        runtime.adopt_canonical_baseline(canonical_parent())?;
        let mut missing_parent = preview.clone();
        missing_parent.parent_hash = None;
        assert!(
            runtime
                .ingest_batch(&mut cache, preconfirmed_batch(missing_parent, log.clone()))
                .expect_err("missing-parent preview must fail closed")
                .to_string()
                .contains("parent")
        );
        assert!(runtime.active_preconfirmation().is_none());
    }

    {
        let (mut cache, _) = setup_cache_with_asserter().await?;
        install_mock_erc20(&mut cache, pool);
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.register_handler(Arc::new(PoolHandler { pool }))?;
        runtime.adopt_canonical_baseline(canonical_parent())?;
        let stale_preview = preconfirmed_batch(preview.clone(), log.clone());
        runtime.ingest_batch(&mut cache, stale_preview.clone())?;
        runtime.ingest_batch(&mut cache, canonical_batch(pool, topic, value))?;
        assert!(runtime.active_preconfirmation().is_none());
        assert!(
            runtime
                .ingest_batch(&mut cache, stale_preview)
                .expect_err("a canonical race must reject the stale preview replay")
                .to_string()
                .contains("successor")
        );
        assert!(runtime.active_preconfirmation().is_none());
    }

    Ok(())
}
