//! Manager-authored red-green acceptance test for Phase-8 step 3: `Validity`
//! stamping of reactive/event-derived writes.
//!
//! With freshness stamping enabled, applying a canonical event write stamps the
//! touched `(address, slot)` as `Validity::ValidThrough(N)` in the runtime's
//! `FreshnessRegistry` (N = the canonical block number). It is therefore NOT
//! volatile at block N (event-maintained, no need to re-verify), but ages to
//! volatile once the clock advances past N. Stamping is opt-in; a runtime that
//! never enables it exposes no registry and behaves exactly as before.
//!
//! Fully offline (mocked provider, injected state).
#![cfg(feature = "reactive")]

mod common;

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::StateUpdate;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    BlockRef, ChainStatus, HandlerError, HandlerId, HandlerOutcome, InputSource, LogInterest,
    ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveHandler, ReactiveInput,
    ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveRuntime, RouteKeySpec,
    StateEffectQuality,
};

fn block(number: u64, hash: B256, parent_hash: B256) -> BlockRef {
    BlockRef {
        number,
        hash,
        parent_hash: Some(parent_hash),
        timestamp: Some(1_700_000_000 + number),
    }
}

fn rpc_log(address: Address, block: &BlockRef, log_index: u64) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, vec![B256::repeat_byte(0xee)], Bytes::new()),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(0x01)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn included_context(block: BlockRef, log_index: u64) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Batch,
        chain_status: ChainStatus::Included {
            block: block.clone(),
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

fn batch(input: ReactiveInput<Ethereum>, ctx: ReactiveContext) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, ctx)])
}

/// A handler that writes a fixed slot on every matching log.
struct SlotWriter {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for SlotWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("freshness-slot-writer")
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
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                U256::from(1u64),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

/// A handler that writes the log's block number into a fixed slot, so each
/// canonical block produces a genuinely-changed value (and therefore a
/// `SlotChange` in the applied diff, which is what freshness stamping keys off).
struct BlockValueWriter {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for BlockValueWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("freshness-block-value-writer")
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
        let number = ctx.block.as_ref().map(|b| b.number).unwrap_or_default();
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                U256::from(number),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

/// Phase-8 s3: an enabled runtime stamps a canonical event write
/// `ValidThrough(N)` — not volatile at N, volatile once past N.
#[tokio::test]
async fn reactive_write_stamps_validity_through_block() -> Result<()> {
    let address = Address::repeat_byte(0xf1);
    let slot = U256::from(3);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.enable_freshness_stamping();
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b10, 10)),
            included_context(b10.clone(), 10),
        ),
    )?;

    let freshness = runtime
        .freshness()
        .expect("stamping was enabled, so a registry is present");
    // Stamped ValidThrough(10): event-maintained, so still valid *at* block 10,
    // but ages to volatile once the clock moves past it.
    assert!(
        !freshness.is_volatile(address, slot, 10),
        "an event-stamped slot is valid through its write block"
    );
    assert!(
        freshness.is_volatile(address, slot, 11),
        "the stamp ages to volatile after its block"
    );
    Ok(())
}

/// Phase-8 s3: stamping is opt-in — a runtime that never enables it exposes no
/// registry (behavior unchanged from before the coupling).
#[tokio::test]
async fn freshness_registry_absent_unless_enabled() -> Result<()> {
    let runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    assert!(
        runtime.freshness().is_none(),
        "freshness stamping is opt-in; disabled by default"
    );
    Ok(())
}

/// A pending (mempool-only) context: it must never mutate canonical cache state,
/// and so must never stamp canonical freshness.
fn pending_context(block: BlockRef, log_index: u64) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Batch,
        chain_status: ChainStatus::Pending,
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

/// Phase-8 s3: a pending input never stamps freshness — pending/mempool writes
/// are not canonical, so the stamped slot stays volatile at its block number
/// (the registry never records a `ValidThrough`).
///
/// A pending input that emits a canonical state effect is itself rejected by the
/// runtime (`InvalidPendingEffect`); either way the freshness registry must be
/// left untouched, so the slot resolves to the default (Volatile).
#[tokio::test]
async fn pending_write_does_not_stamp_validity() -> Result<()> {
    let address = Address::repeat_byte(0xf2);
    let slot = U256::from(7);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.enable_freshness_stamping();
    runtime.register_handler(Arc::new(SlotWriter { address, slot }))?;

    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    // The pending canonical write is rejected by the runtime; the important
    // invariant for this wave is that nothing was stamped either way.
    let _ = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b10, 10)),
            pending_context(b10.clone(), 10),
        ),
    );

    // The registry exists (stamping was enabled) but was never stamped: a
    // pending write is not canonical, so the slot resolves to the registry
    // default (Volatile) and is volatile even *at* its block number.
    let freshness = runtime
        .freshness()
        .expect("stamping was enabled, so a registry is present");
    assert!(
        freshness.is_volatile(address, slot, 10),
        "a pending write must not stamp canonical freshness"
    );
    Ok(())
}

/// Phase-8 s3: re-writing a slot at a later canonical block re-stamps it — the
/// later `ValidThrough(N)` wins, so it is valid through the newer block, not the
/// older one.
#[tokio::test]
async fn later_canonical_stamp_wins() -> Result<()> {
    let address = Address::repeat_byte(0xf3);
    let slot = U256::from(9);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.enable_freshness_stamping();
    // Writes the block number as the value, so the block-11 write is a genuine
    // change (a no-op re-write of the same value produces no `SlotChange`, and
    // stamping keys off the applied diff's changed slots).
    runtime.register_handler(Arc::new(BlockValueWriter { address, slot }))?;

    // First canonical write at block 10 -> ValidThrough(10).
    let b10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x0f));
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b10, 10)),
            included_context(b10.clone(), 10),
        ),
    )?;
    assert!(
        !runtime.freshness().unwrap().is_volatile(address, slot, 10),
        "valid through its first write block"
    );
    // At this point the older stamp is already volatile past block 10.
    assert!(
        runtime.freshness().unwrap().is_volatile(address, slot, 11),
        "the first stamp is volatile once past block 10"
    );

    // Second canonical write at the next block 11 (child of block 10) re-stamps
    // the same slot -> ValidThrough(11).
    let b11 = block(11, B256::repeat_byte(0x11), b10.hash);
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(address, &b11, 11)),
            included_context(b11.clone(), 11),
        ),
    )?;

    let freshness = runtime
        .freshness()
        .expect("stamping was enabled, so a registry is present");
    // The later stamp wins: valid through block 11 (no longer volatile at 11),
    // and ages to volatile only once the clock passes 11.
    assert!(
        !freshness.is_volatile(address, slot, 11),
        "the later stamp wins: valid through block 11"
    );
    assert!(
        freshness.is_volatile(address, slot, 12),
        "the later stamp ages to volatile after block 11"
    );
    Ok(())
}
