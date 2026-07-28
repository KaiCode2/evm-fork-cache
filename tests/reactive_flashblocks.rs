#![cfg(feature = "reactive")]

mod common;

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;
use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::reactive::{
    BaseFlashblockPayload, BlockRef, ChainStatus, DeliveryScope, FlashblockRef, HandlerError,
    HandlerId, HandlerOutcome, InputSource, LogInterest, PreconfirmationMode, ProviderRef,
    ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveHandler, ReactiveInput,
    ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveRuntime, StateEffectQuality,
    SubscriberConfig,
};
use evm_fork_cache::{StateUpdate, events::StateView};

fn flashblock(provider: ProviderRef, index: u64, hash: B256) -> FlashblockRef {
    FlashblockRef {
        provider,
        payload_id: Some([0x11; 8].into()),
        index: Some(index),
        block_number: 101,
        block_hash: hash,
        parent_hash: Some(B256::repeat_byte(0x64)),
        state_root: Some(B256::repeat_byte(0xaa)),
        timestamp: Some(1_700_000_101),
    }
}

fn rpc_log(address: Address, block: BlockRef, tx: u8) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, Vec::new(), Bytes::new()),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(tx)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

fn preconfirmed_record(address: Address, flashblock: FlashblockRef) -> ReactiveInputRecord {
    let block = flashblock.block_ref();
    let provider = flashblock.provider.clone();
    ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(address, block, 0x41)),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Flashblocks,
            chain_status: ChainStatus::Preconfirmed { flashblock },
            block: Some(block),
            transaction_index: Some(0),
            log_index: Some(0),
        },
    )
    .with_provider(provider)
}

fn canonical_record(address: Address) -> ReactiveInputRecord {
    let block = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0xbb),
        parent_hash: Some(B256::repeat_byte(0x64)),
        timestamp: Some(1_700_000_101),
    };
    ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(address, block, 0x42)),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Subscription,
            chain_status: ChainStatus::Included {
                block,
                confirmations: 0,
            },
            block: Some(block),
            transaction_index: Some(0),
            log_index: Some(0),
        },
    )
}

struct SlotWriter {
    address: Address,
    slot: U256,
    value: U256,
}

impl ReactiveHandler<Ethereum> for SlotWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("flashblock-slot-writer")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: None,
        })]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput,
        _state: &dyn StateView,
    ) -> std::result::Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                self.value,
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: Vec::new(),
        })
    }
}

#[test]
fn base_flashblock_wire_decodes_decimal_index_and_hex_header_quantities() -> Result<()> {
    let payload: BaseFlashblockPayload = serde_json::from_str(
        r#"{
            "payload_id":"0x1111111111111111",
            "index":4,
            "base":{
                "parent_hash":"0x6464646464646464646464646464646464646464646464646464646464646464",
                "block_number":"0x65",
                "timestamp":"0x6553f165"
            },
            "diff":{
                "state_root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "block_hash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            },
            "metadata":{"block_number":101}
        }"#,
    )?;
    assert_eq!(payload.index, 4);
    assert_eq!(payload.base.expect("index-zero header").block_number, 101);
    assert_eq!(payload.metadata.expect("metadata").block_number, 101);
    Ok(())
}

#[test]
fn flashblocks_policy_is_disabled_by_default_and_op_polling_is_explicit() {
    let default = SubscriberConfig::default();
    assert_eq!(default.preconfirmations, PreconfirmationMode::Disabled);
    assert_eq!(default.flashblock_poll_interval.as_millis(), 100);
}

#[tokio::test]
async fn preconfirmed_updates_are_visible_then_discarded_before_canonical_ingest() -> Result<()> {
    let address = Address::repeat_byte(0x77);
    let unrelated = Address::repeat_byte(0x88);
    let slot = U256::from(7);
    let canonical_value = U256::from(1);
    let speculative_value = U256::from(99);
    let provider = ProviderRef::new("base-flashblocks", 3);
    let flashblock = flashblock(provider.clone(), 2, B256::repeat_byte(0xfa));

    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, canonical_value)?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        address,
        slot,
        value: speculative_value,
    }))?;

    let record = preconfirmed_record(address, flashblock.clone());
    assert_eq!(record.provider.as_ref(), Some(&provider));
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![record]).with_delivery_scope(DeliveryScope::Preconfirmed),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(speculative_value)
    );
    assert_eq!(runtime.active_preconfirmation(), Some(&flashblock));
    assert!(runtime.last_canonical_block().is_none());

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![canonical_record(unrelated)]),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(canonical_value)
    );
    assert!(runtime.active_preconfirmation().is_none());
    assert_eq!(
        runtime.last_canonical_block().map(|block| block.number),
        Some(101)
    );
    Ok(())
}
