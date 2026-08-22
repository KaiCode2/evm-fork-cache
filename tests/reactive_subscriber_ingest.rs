//! End-to-end composition: real `AlloySubscriber` output → `ReactiveRuntime::ingest_batch`.
//!
//! Closes the integration gap tracked in `docs/KNOWN_ISSUES.md`: prior reactive
//! tests exercised the subscriber and the runtime separately (or the runtime with
//! a mock subscriber). This drives an actual [`AlloySubscriber`]-produced batch
//! into an actual [`ReactiveRuntime`] and asserts the handler write lands in the
//! cache — the full seam a consumer relies on.
//!
//! It runs offline by fetching the batch through the subscriber's `get_logs`
//! backfill path (mockable), which produces exactly the same
//! `ReactiveInputBatch` shape used by every live transport. Polling is selected
//! here because Alloy's mocked provider can install filter streams but cannot
//! install WebSocket subscriptions. WebSocket reconnect/termination plumbing is
//! covered separately in `tests/reactive_alloy_subscriber.rs`.
#![cfg(feature = "reactive-polling")]

mod common;

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_provider::ProviderBuilder;
use alloy_rpc_types_eth::{Block, Filter, Header, Log};
use alloy_transport::mock::Asserter;
use anyhow::{Result, bail};

use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::StateUpdate;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    AlloySubscriber, EventSubscriber, HandlerError, HandlerId, HandlerOutcome, InputSource,
    LogInterest, ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveHandler, ReactiveInput,
    ReactiveInterest, ReactiveRuntime, RouteKeySpec, StateEffectQuality, SubscriberBackfill,
    SubscriberConfig, SubscriberMode,
};

const POOL_SLOT: u64 = 0;

/// Writes the log's data word into `POOL_SLOT` of the emitting contract.
struct PoolHandler {
    pool: Address,
}

impl ReactiveHandler<Ethereum> for PoolHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("pool")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.pool),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
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
        let value = U256::from_be_slice(log.data().data.as_ref());
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                log.address(),
                U256::from(POOL_SLOT),
                value,
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

fn swap_log(pool: Address, topic: B256, block_number: u64, value: u64) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(
            pool,
            vec![topic],
            Bytes::from(U256::from(value).to_be_bytes::<32>().to_vec()),
        ),
        block_hash: Some(B256::repeat_byte(block_number as u8)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0xcc)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

fn rpc_block(block_number: u64) -> Block {
    Block::empty(Header {
        hash: B256::repeat_byte(block_number as u8),
        inner: alloy_consensus::Header {
            number: block_number,
            parent_hash: B256::repeat_byte(block_number.saturating_sub(1) as u8),
            timestamp: 1_700_000_000 + block_number,
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn alloy_subscriber_batch_feeds_reactive_runtime_ingest_end_to_end() -> Result<()> {
    let pool = Address::repeat_byte(0xcd);
    let topic = keccak256(b"Swap()");
    let new_value = 4242u64;

    // Cache + tracked pool contract.
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, pool);

    // Real AlloySubscriber over a mocked provider; the backfill get_logs returns
    // one swap log carrying the post-state value.
    let asserter = Asserter::new();
    asserter.push_success(&U256::from(1)); // eth_chainId
    asserter.push_success(&U256::from(2)); // eth_newFilter before backfill
    asserter.push_success(&Some(rpc_block(100)));
    asserter.push_success(&vec![swap_log(pool, topic, 100, new_value)]);
    asserter.push_success(&Some(rpc_block(100)));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Polling,
        SubscriberConfig {
            hydrate_pending_transactions: false,
            ..SubscriberConfig::default()
        },
    );
    subscriber.replace_interest_owners_with_global_backfill(
        vec![(
            HandlerId::new("pool"),
            vec![ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(pool).event_signature(topic),
                local_matcher: None,
                route_key: None,
            })],
        )],
        SubscriberBackfill::range(90, 100),
    )?;

    // Real runtime with the matching handler.
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PoolHandler { pool }))?;

    // Pull the batch the subscriber produced and feed it straight into ingest.
    let Some(batch) = subscriber.next_batch().await? else {
        bail!("expected a backfilled batch from the subscriber");
    };
    assert_eq!(batch.records().len(), 1);
    assert_eq!(batch.records()[0].context.source, InputSource::Backfill);

    let report = runtime.ingest_batch(&mut cache, batch)?;

    // The handler ran on the subscriber-produced record and its write landed.
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].handler_id, HandlerId::new("pool"));
    assert_eq!(
        cache.cached_storage_value(pool, U256::from(POOL_SLOT)),
        Some(U256::from(new_value)),
        "the subscriber-delivered swap should have updated the pool slot with no RPC"
    );

    Ok(())
}
