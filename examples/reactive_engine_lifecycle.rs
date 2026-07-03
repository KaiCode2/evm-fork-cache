//! `ReactiveEngine` adapter lifecycle: register and unregister handlers while the
//! engine is live — the flow an AMM indexer runs when a pool is created or retired
//! mid-stream.
//!
//! [`ReactiveEngine`] binds a [`ReactiveRuntime`] to an [`EventSubscriber`] and
//! drives handler lifecycle as one operation, keyed by a stable [`HandlerId`].
//! This example shows the whole loop with **no network** — a small scripted
//! subscriber stands in for a live `AlloySubscriber` so the mechanics are
//! deterministic:
//!
//! 1. Register an initial pool handler. On a fresh runtime (nothing ingested yet)
//!    registration is **live-only** — there is no processed position to backfill
//!    from.
//! 2. Ingest a canonical block. The runtime now has a canonical head.
//! 3. Discover a new pool mid-stream and [`register_handler`](ReactiveEngine::register_handler)
//!    it. Because the runtime has a canonical head, the new handler is
//!    **backfilled from that block automatically** — the discovery→subscription
//!    window closes with no caller bookkeeping. (`register_handler_with_backfill`
//!    for deeper history; `register_handler_live_only` to opt out.)
//! 4. Retire a pool with the teardown recipe:
//!    [`unregister_handler`](ReactiveEngine::unregister_handler) (routing +
//!    transport) plus [`untrack_account`](ReactiveRuntime::untrack_account) (stop
//!    root-gate `eth_getProof` probes) and
//!    [`cancel_pending_resyncs`](ReactiveRuntime::cancel_pending_resyncs) (drop
//!    queued repairs). Cache eviction stays an explicit caller action.
//!
//! In production the scripted subscriber is replaced by `AlloySubscriber` (which
//! implements [`InterestOwnerSubscriber`]); the engine calls are identical.
//!
//! Runs fully offline against a mocked provider — no network, no RPC key.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example reactive_engine_lifecycle
//! ```

#[path = "support/mock.rs"]
mod mock;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::{Result, anyhow};
use evm_fork_cache::StateUpdate;
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    BlockRef, ChainStatus, EventSubscriber, HandlerError, HandlerId, HandlerOutcome, InputSource,
    InterestOwnerSubscriber, LogInterest, ReactiveConfig, ReactiveContext, ReactiveEffect,
    ReactiveEngine, ReactiveHandler, ReactiveInput, ReactiveInputBatch, ReactiveInputRecord,
    ReactiveInterest, ReactiveRuntime, RouteKeySpec, StateEffectQuality, SubscriberBackfill,
    SubscriberError, SubscriberNextBatch, TrackingPolicy,
};

/// The storage slot each pool handler maintains from its swap logs (a stand-in
/// for a packed price/liquidity word). The post-state value rides in the log
/// `data`, so the handler decodes and writes it — no RPC.
const POOL_SLOT: u64 = 0;

/// A protocol-neutral pool handler: every matching log carries the new slot value
/// in its `data`, written straight into `POOL_SLOT`.
struct PoolHandler {
    id: HandlerId,
    pool: Address,
}

impl ReactiveHandler<Ethereum> for PoolHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
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

/// A minimal in-example [`EventSubscriber`] that hands back scripted batches and
/// tracks interest owners. It exists so the example is deterministic and
/// offline; production code uses `AlloySubscriber`, which implements the same two
/// traits over a live WebSocket transport.
#[derive(Default)]
struct ScriptedSubscriber {
    owners: HashMap<HandlerId, Vec<ReactiveInterest<Ethereum>>>,
    /// Records `(owner, anchor)` for every backfill the engine requested — so the
    /// example can show that mid-lifecycle registration auto-anchors.
    backfills: Vec<(HandlerId, SubscriberBackfill)>,
    batches: VecDeque<ReactiveInputBatch<Ethereum>>,
}

impl EventSubscriber<Ethereum> for ScriptedSubscriber {
    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> Result<(), SubscriberError> {
        // Full-replacement setup path — unused here; the engine drives owners.
        self.owners.clear();
        Ok(())
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }
}

impl InterestOwnerSubscriber<Ethereum> for ScriptedSubscriber {
    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> Result<(), SubscriberError> {
        self.owners.insert(owner, interests.to_vec());
        Ok(())
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        backfill: SubscriberBackfill,
    ) -> Result<(), SubscriberError> {
        self.add_interest_owner(owner.clone(), interests)?;
        self.backfills.push((owner, backfill));
        Ok(())
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> Option<Vec<ReactiveInterest<Ethereum>>> {
        self.owners.remove(owner)
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        self.owners.get(owner).map(Vec::as_slice)
    }
}

/// Build a canonical-block batch carrying one swap log for `pool` whose data is
/// the new slot value.
fn swap_batch(pool: Address, block_number: u64, value: u64) -> ReactiveInputBatch<Ethereum> {
    let block = BlockRef {
        number: block_number,
        hash: B256::repeat_byte(block_number as u8),
        parent_hash: Some(B256::repeat_byte(block_number.saturating_sub(1) as u8)),
        timestamp: Some(1_700_000_000 + block_number),
    };
    let log = Log {
        inner: PrimitiveLog::new_unchecked(
            pool,
            vec![keccak256(b"Swap()")],
            Bytes::from(U256::from(value).to_be_bytes::<32>().to_vec()),
        ),
        block_hash: Some(block.hash),
        block_number: Some(block_number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte(0xcc)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    };
    let ctx = ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Subscription,
        chain_status: ChainStatus::Included {
            block: block.clone(),
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(0),
    };
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(ReactiveInput::Log(log), ctx)])
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let pool_a = Address::repeat_byte(0xa1);
    let pool_b = Address::repeat_byte(0xb2);

    let mut cache = mock::offline_cache().await?;
    mock::install_mock_erc20(&mut cache, pool_a);
    mock::install_mock_erc20(&mut cache, pool_b);

    let runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let mut engine = ReactiveEngine::new(runtime, ScriptedSubscriber::default());

    // 1. Register the first pool on a fresh runtime → live-only (no canonical
    //    head to backfill from yet).
    engine.register_handler(Arc::new(PoolHandler {
        id: HandlerId::new("pool-a"),
        pool: pool_a,
    }))?;
    // Track pool-A so the root gate re-verifies its storage root on a cadence.
    engine
        .runtime_mut()
        .track_account(pool_a, TrackingPolicy::WholeAccount);
    println!(
        "1. registered pool-a on a fresh runtime → backfills requested: {} (live-only)",
        engine.subscriber().backfills.len()
    );

    // 2. Ingest a canonical block. Recommended loop: next_ingest_with_resync,
    //    which executes any coverage-gap resyncs the runtime surfaces.
    engine
        .subscriber_mut()
        .batches
        .push_back(swap_batch(pool_a, 100, 111));
    engine.next_ingest_with_resync(&mut cache).await?;
    let head = engine
        .runtime()
        .last_canonical_block()
        .ok_or_else(|| anyhow!("expected a canonical head after ingest"))?;
    println!(
        "2. ingested block {head}; pool-a slot0 = {}",
        cache
            .cached_storage_value(pool_a, U256::from(POOL_SLOT))
            .unwrap_or_default(),
        head = head.number
    );

    // 3. A PoolCreated event surfaces pool-B mid-stream. Registering it now
    //    auto-anchors its log backfill to the runtime's canonical head — no
    //    caller bookkeeping, no discovery→subscription gap.
    engine.register_handler(Arc::new(PoolHandler {
        id: HandlerId::new("pool-b"),
        pool: pool_b,
    }))?;
    let (owner, backfill) = engine
        .subscriber()
        .backfills
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("expected an auto-anchored backfill for pool-b"))?;
    println!(
        "3. discovered pool-b → registered with automatic backfill: owner={} from block {}",
        owner.as_str(),
        backfill.start_block()
    );

    // 4. Both handlers are live. Ingest a block touching each.
    engine
        .subscriber_mut()
        .batches
        .push_back(swap_batch(pool_b, 101, 222));
    engine.next_ingest_with_resync(&mut cache).await?;
    println!(
        "4. ingested block 101; pool-b slot0 = {}",
        cache
            .cached_storage_value(pool_b, U256::from(POOL_SLOT))
            .unwrap_or_default()
    );

    // 5. Retire pool-A. The full teardown recipe: stop routing/transport, stop
    //    root-gate probes, and drop any queued repairs. Cache eviction (if you
    //    want the state gone) stays an explicit `StateUpdate::purge` / cache API
    //    call — deliberately not implied by unregistration.
    let removed = engine.unregister_handler(&HandlerId::new("pool-a"));
    let untracked = engine.runtime_mut().untrack_account(pool_a);
    let cancelled = engine.runtime_mut().cancel_pending_resyncs(pool_a);
    println!(
        "5. retired pool-a → handler removed: {}, untracked: {}, resyncs cancelled: {}",
        removed.is_some(),
        untracked,
        cancelled.len()
    );

    // 6. Prove the teardown took effect: a further pool-A log is no longer routed
    //    (its handler is gone), while pool-B keeps updating.
    engine
        .subscriber_mut()
        .batches
        .push_back(swap_batch(pool_a, 102, 999));
    engine
        .subscriber_mut()
        .batches
        .push_back(swap_batch(pool_b, 102, 333));
    while engine.next_ingest_with_resync(&mut cache).await?.is_some() {}
    println!(
        "6. after teardown, block 102: pool-a slot0 = {} (unchanged — not routed), pool-b slot0 = {}",
        cache
            .cached_storage_value(pool_a, U256::from(POOL_SLOT))
            .unwrap_or_default(),
        cache
            .cached_storage_value(pool_b, U256::from(POOL_SLOT))
            .unwrap_or_default()
    );

    println!("\nlifecycle complete — registered, auto-anchored, and torn down with no RPC.");
    Ok(())
}
