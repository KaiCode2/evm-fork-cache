#![cfg(feature = "reactive")]

mod common;

use std::sync::Arc;

use alloy_eips::BlockId;
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
        content_hash: hash,
        partial_block_hash: Some(hash),
        parent_hash: Some(B256::repeat_byte(0x64)),
        state_root: Some(B256::repeat_byte(0xaa)),
        transactions_root: Some(B256::repeat_byte(0x91)),
        transaction_hashes: vec![B256::repeat_byte(0x41)],
        timestamp: Some(1_700_000_101),
        base_fee_per_gas: Some(7),
        beneficiary: Some(Address::repeat_byte(0xcb)),
        prevrandao: Some(B256::repeat_byte(0x77)),
        gas_limit: Some(30_000_000),
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
            chain_status: ChainStatus::Preconfirmed {
                flashblock: Arc::new(flashblock),
            },
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
                "fee_recipient":"0xcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcb",
                "block_number":"0x65",
                "gas_limit":"0x1c9c380",
                "timestamp":"0x6553f165",
                "base_fee_per_gas":"0x7",
                "prev_randao":"0x7777777777777777777777777777777777777777777777777777777777777777"
            },
            "diff":{
                "state_root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "block_hash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            },
            "metadata":{"block_number":101}
        }"#,
    )?;
    assert_eq!(payload.index, 4);
    let base = payload.base.expect("index-zero header");
    assert_eq!(base.block_number, 101);
    assert_eq!(base.gas_limit, Some(30_000_000));
    assert_eq!(base.base_fee_per_gas, Some(7));
    assert_eq!(base.beneficiary, Some(Address::repeat_byte(0xcb)));
    assert_eq!(base.prevrandao, Some(B256::repeat_byte(0x77)));
    assert_eq!(payload.metadata.expect("metadata").block_number, 101);
    Ok(())
}

#[test]
fn flashblocks_policy_is_disabled_by_default_and_canonical_certification_is_bounded() {
    let default = SubscriberConfig::default();
    assert_eq!(default.preconfirmations, PreconfirmationMode::Disabled);
    assert_eq!(default.canonical_head_poll_interval.as_millis(), 500);
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

#[tokio::test]
async fn preconfirmed_branch_installs_pending_rpc_pin_and_complete_block_environment() -> Result<()>
{
    let address = Address::repeat_byte(0x77);
    let slot = U256::from(7);
    let mut cache = setup_cache().await?;
    cache.set_block(BlockId::number(100));
    cache.set_block_context(Some(100), Some(3));
    cache.set_coinbase(Some(Address::repeat_byte(0xca)));
    cache.set_prevrandao(Some(B256::repeat_byte(0x66)));
    cache.set_block_gas_limit(Some(29_000_000));
    cache.set_timestamp(Some(1_700_000_100));
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(1))?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        address,
        slot,
        value: U256::from(99),
    }))?;
    let pending = flashblock(
        ProviderRef::new("base-flashblocks", 3),
        2,
        B256::repeat_byte(0xfa),
    );
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![preconfirmed_record(address, pending)])
            .with_delivery_scope(DeliveryScope::Preconfirmed),
    )?;

    assert_eq!(cache.block(), BlockId::pending());
    assert_eq!(cache.block_number(), Some(101));
    assert_eq!(cache.basefee(), Some(7));
    assert_eq!(cache.coinbase(), Some(Address::repeat_byte(0xcb)));
    assert_eq!(cache.prevrandao(), Some(B256::repeat_byte(0x77)));
    assert_eq!(cache.block_gas_limit(), Some(30_000_000));
    assert_eq!(cache.timestamp(), Some(1_700_000_101));

    runtime.discard_preconfirmation(&mut cache);
    assert_eq!(cache.block(), BlockId::number(100));
    assert_eq!(cache.block_number(), Some(100));
    assert_eq!(cache.basefee(), Some(3));
    assert_eq!(cache.coinbase(), Some(Address::repeat_byte(0xca)));
    assert_eq!(cache.prevrandao(), Some(B256::repeat_byte(0x66)));
    assert_eq!(cache.block_gas_limit(), Some(29_000_000));
    assert_eq!(cache.timestamp(), Some(1_700_000_100));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cumulative_previews_preserve_generation_local_fills_without_leaking_them() -> Result<()> {
    let event_address = Address::repeat_byte(0x77);
    let canonical_warm_address = Address::repeat_byte(0x88);
    let pending_fill_address = Address::repeat_byte(0x99);
    let slot = U256::from(7);
    let canonical_warm_value = U256::from(123);
    let pending_fill_value = U256::from(456);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, event_address);
    install_mock_erc20(&mut cache, canonical_warm_address);
    install_mock_erc20(&mut cache, pending_fill_address);
    cache
        .db_mut()
        .insert_account_storage(canonical_warm_address, slot, canonical_warm_value)?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        address: event_address,
        slot,
        value: U256::from(99),
    }))?;

    let provider = ProviderRef::new("base-flashblocks", 3);
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![preconfirmed_record(
            event_address,
            flashblock(provider.clone(), 1, B256::repeat_byte(0xf1)),
        )])
        .with_delivery_scope(DeliveryScope::Preconfirmed),
    )?;
    cache
        .db_mut()
        .insert_account_storage(pending_fill_address, slot, pending_fill_value)?;

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![preconfirmed_record(
            event_address,
            flashblock(provider, 2, B256::repeat_byte(0xf2)),
        )])
        .with_delivery_scope(DeliveryScope::Preconfirmed),
    )?;
    assert_eq!(
        cache.cached_storage_value(pending_fill_address, slot),
        Some(pending_fill_value),
        "a cumulative successor reuses its generation-local pending read set"
    );

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![preconfirmed_record(
            event_address,
            flashblock(
                ProviderRef::new("base-flashblocks", 4),
                0,
                B256::repeat_byte(0xf3),
            ),
        )])
        .with_delivery_scope(DeliveryScope::Preconfirmed),
    )?;
    assert_eq!(
        cache.cached_storage_value(pending_fill_address, slot),
        Some(U256::ZERO)
    );
    assert_eq!(
        cache.cached_storage_value(canonical_warm_address, slot),
        Some(canonical_warm_value),
        "canonical warming survives every speculative replacement"
    );

    runtime.discard_preconfirmation(&mut cache);
    assert_eq!(
        cache.cached_storage_value(pending_fill_address, slot),
        Some(U256::ZERO)
    );
    assert_eq!(
        cache.cached_storage_value(canonical_warm_address, slot),
        Some(canonical_warm_value)
    );
    Ok(())
}

#[tokio::test]
async fn conflicting_duplicate_index_revokes_the_speculative_branch() -> Result<()> {
    let address = Address::repeat_byte(0x77);
    let slot = U256::from(7);
    let canonical_value = U256::from(1);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, canonical_value)?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        address,
        slot,
        value: U256::from(99),
    }))?;
    let provider = ProviderRef::new("base-flashblocks", 3);
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![preconfirmed_record(
            address,
            flashblock(provider.clone(), 1, B256::repeat_byte(0xf1)),
        )])
        .with_delivery_scope(DeliveryScope::Preconfirmed),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(99))
    );

    let result = runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![preconfirmed_record(
            address,
            flashblock(provider, 1, B256::repeat_byte(0xf2)),
        )])
        .with_delivery_scope(DeliveryScope::Preconfirmed),
    );
    assert!(result.is_err());
    assert!(runtime.active_preconfirmation().is_none());
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(canonical_value)
    );
    Ok(())
}
