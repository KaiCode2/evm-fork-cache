//! Event → state pipeline (Pillar B.2 — the *reader half* of the event pipeline).
//!
//! Phase 3 ([`state_update`](crate::state_update)) built the *writer half*: the
//! generic [`StateUpdate`] vocabulary and the cold-aware
//! [`apply_updates`](crate::cache::EvmCache::apply_updates) that consumes it. This
//! module builds the *reader half*: it turns an on-chain [`Log`] into that same
//! vocabulary and drives it through the cache, keeping event-derived state
//! reactively fresh.
//!
//! # The flow
//!
//! ```text
//! Log ─▶ EventDecoder::decode(log, &StateView) ─▶ Vec<StateUpdate>
//!                                                      │
//!                                       apply_updates ▼
//!                                                  EvmCache  (+ StateDiff)
//! ```
//!
//! A [`DecoderRegistry`] dispatches a log to the decoders registered for its
//! emitting address (plus any global decoders) and concatenates their output. An
//! [`EventPipeline`] orchestrates a block's logs: [`ingest_logs`] decodes and
//! applies them **log-by-log in order** (so a later log's decode observes the
//! effects of earlier ones through the [`StateView`]), [`reorg_to`] purges the
//! addresses touched after a new head, and [`reconcile`] re-reads sampled
//! event-derived slots against chain truth (correct **and** alarm).
//!
//! [`ingest_logs`]: EventPipeline::ingest_logs
//! [`reorg_to`]: EventPipeline::reorg_to
//! [`reconcile`]: EventPipeline::reconcile
//!
//! # Decoders are pure data functions
//!
//! [`EventDecoder::decode`] is a pure function of `(log, pre-state)`: it performs
//! no I/O and emits serializable, replayable [`StateUpdate`] data. Most updates
//! need no pre-state ([`SlotDelta`](crate::StateUpdate::SlotDelta) and
//! [`SlotMasked`](crate::StateUpdate::SlotMasked) are read-modify-write *at apply
//! time*), but stateful adapters — UniswapV3 tick maintenance must read the
//! current `liquidityGross`/`liquidityNet`/`tick`/bitmap to recompute a packed
//! word — read the narrow read-only [`StateView`]. The view never touches RPC; a
//! slot absent from the cache reads `None` (cold), and a decoder that cannot
//! compute against a cold word surfaces a skip rather than inventing a value.
//!
//! # `!Send` cache discipline
//!
//! [`EvmCache`] is `!Send` (it owns the mutable fork and
//! blocks on RPC internally). All of [`EventPipeline`]'s core methods
//! ([`ingest_logs`](EventPipeline::ingest_logs) /
//! [`reorg_to`](EventPipeline::reorg_to) /
//! [`reconcile`](EventPipeline::reconcile)) take `&mut EvmCache` and are
//! **synchronous** — they never `.await`, so the cache is never held across a
//! yield point. This is what makes the core deterministically testable offline.
//! The async [`drive`] convenience holds the cache only across the *log source*
//! await (the source future is `Send`; the cache is untouched during it).
//!
//! # Freshness wiring
//!
//! [`BlockDigest::touched_slots`] surfaces the `(address, slot)` set written for a
//! block so a caller can classify event-derived slots in a
//! [`FreshnessRegistry`](crate::freshness::FreshnessRegistry) — typically pin them
//! ([`Validity::Pinned`](crate::freshness::Validity::Pinned)) or mark them
//! [`Validity::ValidThrough`](crate::freshness::Validity::ValidThrough) so the
//! optimistic validator does not waste RPC re-verifying state the pipeline keeps
//! fresh — then call
//! [`FreshnessController::on_new_block`](crate::freshness::FreshnessController::on_new_block).
//! No controller internals change. Periodically call
//! [`reconcile`](EventPipeline::reconcile) to sample-check those slots against the
//! chain (honest freshness).

pub mod erc20;
#[cfg(feature = "protocols")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocols")))]
pub mod uniswap_v3;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use alloy_primitives::{Address, Log, U256};
use anyhow::Result;

use crate::cache::EvmCache;
use crate::freshness::SlotChange;
use crate::state_update::{PurgeScope, StateDiff, StateUpdate};

/// Read-only view of current cached state handed to a decoder.
///
/// Decoders that compute post-state from pre-state (e.g. UniswapV3 tick
/// maintenance) read through this; stateless decoders (ERC-20 `Transfer`, V3
/// `Swap`) ignore it. The view never touches RPC — a slot absent from the cache
/// reads `None`.
pub trait StateView {
    /// Current cached value of `(address, slot)` (overlay ▸ backend ▸ `None`),
    /// matching what the EVM would `SLOAD` (`account_state`-aware). `None` means
    /// the slot is **cold** — neither cache layer has seen it.
    fn storage(&self, address: Address, slot: U256) -> Option<U256>;
}

/// Decode one log into zero or more targeted [`StateUpdate`]s.
///
/// `decode` is a pure function of `(log, pre-state)`: it performs no I/O and emits
/// data (the updates are serializable and replayable against matching pre-state).
/// The pipeline applies the result through
/// [`apply_updates`](crate::cache::EvmCache::apply_updates).
///
/// A decoder returns `vec![]` for any log it does not recognise (wrong topic0, an
/// unregistered emitting address, a malformed payload). The pipeline counts a log
/// as *decoded* only when some decoder produced at least one update for it.
pub trait EventDecoder: Send + Sync {
    /// Decode `log` against the read-only pre-state `view` into targeted updates.
    fn decode(&self, log: &Log, view: &dyn StateView) -> Vec<StateUpdate>;
}

/// Dispatches a log to the decoders registered for its emitting address (and any
/// global decoders) and concatenates their output.
///
/// Dispatch is by emitting address ([`Log::address`]); topic0 filtering is each
/// decoder's own concern (a decoder returns `vec![]` for a log it does not
/// recognise). Address-scoped decoders are consulted first, then global ones, and
/// the per-decoder outputs are concatenated in that order.
#[derive(Default)]
pub struct DecoderRegistry {
    /// Decoders consulted for every log, in registration order.
    global: Vec<Arc<dyn EventDecoder>>,
    /// Decoders consulted only for logs emitted by a specific address.
    per_address: HashMap<Address, Vec<Arc<dyn EventDecoder>>>,
}

impl DecoderRegistry {
    /// Create an empty registry with no decoders.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a decoder consulted for **every** log.
    pub fn register(&mut self, decoder: Arc<dyn EventDecoder>) -> &mut Self {
        self.global.push(decoder);
        self
    }

    /// Register a decoder consulted only for logs emitted by `address`.
    pub fn register_for_address(
        &mut self,
        address: Address,
        decoder: Arc<dyn EventDecoder>,
    ) -> &mut Self {
        self.per_address.entry(address).or_default().push(decoder);
        self
    }

    /// Decode `log` through every applicable decoder, concatenating the results
    /// (address-scoped decoders first, then global), preserving order.
    pub fn decode(&self, log: &Log, view: &dyn StateView) -> Vec<StateUpdate> {
        let mut out = Vec::new();
        if let Some(scoped) = self.per_address.get(&log.address) {
            for decoder in scoped {
                out.extend(decoder.decode(log, view));
            }
        }
        for decoder in &self.global {
            out.extend(decoder.decode(log, view));
        }
        out
    }
}

/// How a reorg purges the addresses touched after the new head.
///
/// `depth` bounds the per-block touched-address history retained for reorg purge
/// (the reorg horizon); older entries are dropped as new blocks are ingested.
/// `scope` is the [`PurgeScope`] applied to each touched address on
/// [`reorg_to`](EventPipeline::reorg_to).
#[derive(Clone, Debug)]
pub struct ReorgConfig {
    /// How many recent blocks of touched-address history to retain for reorg
    /// purge (the reorg horizon). Older entries are dropped.
    pub depth: usize,
    /// Purge scope used on reorg. The default ([`PurgeScope::AllStorage`]) drops
    /// storage so it re-fetches but keeps the account header;
    /// [`PurgeScope::Account`] drops the whole account.
    pub scope: PurgeScope,
}

impl Default for ReorgConfig {
    fn default() -> Self {
        Self {
            depth: 64,
            scope: PurgeScope::AllStorage,
        }
    }
}

/// Per-block result of [`EventPipeline::ingest_logs`].
#[derive(Clone, Debug, Default)]
pub struct BlockDigest {
    /// The block whose logs were ingested.
    pub block: u64,
    /// Merged diff of everything applied for the block (changes-only **and**
    /// skips — check [`StateDiff::has_skipped`]).
    pub applied: StateDiff,
    /// Number of logs that decoded to at least one update.
    pub decoded_logs: usize,
    /// The `(address, slot)` set written this block (for freshness
    /// classification — see the module docs).
    pub touched_slots: Vec<(Address, U256)>,
}

/// Result of [`EventPipeline::reconcile`].
#[derive(Clone, Debug, Default)]
pub struct ReconcileReport {
    /// How many slots were sampled.
    pub checked: usize,
    /// Slots whose event-derived value disagreed with chain truth. A non-empty
    /// list is a **drift alarm**: the cache had drifted and
    /// [`verify_slots`](crate::cache::EvmCache::verify_slots) has now injected the
    /// fresh chain values (correct + alarm).
    pub mismatched: Vec<SlotChange>,
}

/// Orchestrates decoding, applying, reorg handling, and reconciliation of a
/// block's logs against an [`EvmCache`].
///
/// Construct one from a [`DecoderRegistry`], then call
/// [`ingest_logs`](Self::ingest_logs) per block. See the [module docs](crate::events)
/// for the freshness-wiring pattern (event-derived slots →
/// [`Pinned`](crate::freshness::Validity::Pinned), reconciled periodically).
pub struct EventPipeline {
    registry: DecoderRegistry,
    reorg: ReorgConfig,
    /// Ring of `(block, touched addresses)` for reorg purge, newest at the back,
    /// bounded to `reorg.depth`.
    touched: VecDeque<(u64, Vec<Address>)>,
    /// Every event-derived `(address, slot)` seen so far (reconcile sampling
    /// source).
    derived_slots: HashSet<(Address, U256)>,
}

impl EventPipeline {
    /// Create a pipeline over `registry` with the default [`ReorgConfig`].
    pub fn new(registry: DecoderRegistry) -> Self {
        Self {
            registry,
            reorg: ReorgConfig::default(),
            touched: VecDeque::new(),
            derived_slots: HashSet::new(),
        }
    }

    /// Override the [`ReorgConfig`] (reorg horizon depth + purge scope).
    pub fn with_reorg_config(mut self, cfg: ReorgConfig) -> Self {
        self.reorg = cfg;
        self
    }

    /// Decode + apply a block's logs, **log-by-log in order**, recording touched
    /// state for reorg tracking. Returns the per-block [`BlockDigest`].
    ///
    /// Each log is decoded against the *current* cache state and applied
    /// immediately, so a later log's decode observes the effects of earlier logs
    /// in the same block through the [`StateView`] (e.g. a same-block `Burn` after
    /// a `Mint`, or two overlapping `Mint`s). The touched addresses are recorded
    /// in the depth-bounded reorg ring under `block`, and the touched
    /// `(address, slot)` pairs into the reconcile-sampling set.
    pub fn ingest_logs(&mut self, cache: &mut EvmCache, block: u64, logs: &[Log]) -> BlockDigest {
        let mut digest = BlockDigest {
            block,
            ..Default::default()
        };
        let mut touched_addrs: HashSet<Address> = HashSet::new();

        for log in logs {
            // Decode against the current cache view (immutable borrow), then drop
            // that borrow before taking the &mut borrow for apply. Decode returns
            // owned data, so the two borrows never overlap.
            let updates = self.registry.decode(log, &*cache);
            if updates.is_empty() {
                continue;
            }
            let diff = cache.apply_updates(&updates);

            // A log counts as decoded when it produced at least one update.
            digest.decoded_logs += 1;

            // Record touched addresses (for reorg) and touched slots (for
            // freshness + reconcile) from every category of the diff.
            for change in &diff.slots {
                touched_addrs.insert(change.address);
                self.note_touched_slot(&mut digest, change.address, change.slot);
            }
            for change in &diff.accounts {
                touched_addrs.insert(change.address);
            }
            for record in &diff.purged {
                touched_addrs.insert(record.address);
            }
            for skip in &diff.skipped {
                touched_addrs.insert(skip.address);
                self.note_touched_slot(&mut digest, skip.address, skip.slot);
            }
            for skip in &diff.skipped_balances {
                touched_addrs.insert(skip.address);
            }
            for skip in &diff.skipped_masks {
                touched_addrs.insert(skip.address);
                self.note_touched_slot(&mut digest, skip.address, skip.slot);
            }

            digest.applied.merge(diff);
        }

        if !touched_addrs.is_empty() {
            self.touched
                .push_back((block, touched_addrs.into_iter().collect()));
            self.trim_ring();
        }

        digest
    }

    /// Reorg to `new_head`: purge (per [`ReorgConfig::scope`]) every address
    /// touched in a block **>** `new_head`, drop those ring entries, and return the
    /// merged purge [`StateDiff`].
    ///
    /// The next read of a purged address re-fetches from RPC. The caller then
    /// re-ingests the canonical chain's logs for the reorged range (and/or the
    /// next read lazily re-fetches).
    pub fn reorg_to(&mut self, cache: &mut EvmCache, new_head: u64) -> StateDiff {
        // Collect the addresses touched strictly after the new head, deduped.
        let mut to_purge: HashSet<Address> = HashSet::new();
        for (block, addrs) in &self.touched {
            if *block > new_head {
                to_purge.extend(addrs.iter().copied());
            }
        }

        // Drop the rolled-back ring entries and the derived slots they own.
        self.touched.retain(|(block, _)| *block <= new_head);
        self.derived_slots
            .retain(|(addr, _)| !to_purge.contains(addr));

        let updates: Vec<StateUpdate> = to_purge
            .into_iter()
            .map(|addr| StateUpdate::purge(addr, self.reorg.scope.clone()))
            .collect();
        cache.apply_updates(&updates)
    }

    /// Sampled reconciliation: re-read `slots` via
    /// [`EvmCache::verify_slots`](crate::cache::EvmCache::verify_slots) (correct +
    /// alarm). Returns the mismatches.
    ///
    /// It fetches the fresh chain value for each slot, injects the ones that
    /// changed (so the cache is **corrected**), and returns the changed set — a
    /// non-empty [`ReconcileReport::mismatched`] is the **drift alarm**:
    /// event-derived state had drifted and has now been corrected to chain truth.
    /// Honest about reachability (via
    /// [`EvmCache::reconcile_slots`](crate::cache::EvmCache::reconcile_slots)): it
    /// errors when no batch fetcher is configured **or** when a non-empty request
    /// could not fetch any slot (a total fetch failure is not a silent all-clear).
    /// An empty `slots` is a no-op that returns an empty report.
    pub fn reconcile(
        &mut self,
        cache: &mut EvmCache,
        slots: &[(Address, U256)],
    ) -> Result<ReconcileReport> {
        let mismatched = cache.reconcile_slots(slots)?;
        Ok(ReconcileReport {
            checked: slots.len(),
            mismatched,
        })
    }

    /// All event-derived slots seen so far (the sampling source for
    /// [`reconcile`](Self::reconcile)).
    pub fn derived_slots(&self) -> impl Iterator<Item = (Address, U256)> + '_ {
        self.derived_slots.iter().copied()
    }

    /// Record a touched slot in both the per-block digest (deduped within the
    /// block) and the global all-time reconcile-sampling set.
    fn note_touched_slot(&mut self, digest: &mut BlockDigest, address: Address, slot: U256) {
        self.derived_slots.insert((address, slot));
        if !digest.touched_slots.contains(&(address, slot)) {
            digest.touched_slots.push((address, slot));
        }
    }

    /// Trim the reorg ring to the configured depth, dropping the oldest entries.
    fn trim_ring(&mut self) {
        while self.touched.len() > self.reorg.depth {
            self.touched.pop_front();
        }
    }
}

/// A signalled reorg accompanying a block from a [`LogSource`].
///
/// `None` means the block extends the current head; `Some(new_head)` asks the
/// driver to [`reorg_to`](EventPipeline::reorg_to) `new_head` before ingesting.
pub type ReorgSignal = Option<u64>;

/// An async source of blocks of logs for [`drive`].
///
/// This is the thin async convenience layer (§7.5): a production WS /
/// `subscribe_logs` adapter implements it; the offline example feeds a vec-backed
/// source. The synchronous [`EventPipeline`] core is the tested contract.
pub trait LogSource {
    /// Yield the next block: its number, its logs, and an optional reorg signal.
    /// `None` ends the stream.
    fn next_block(
        &mut self,
    ) -> impl std::future::Future<Output = Option<(u64, Vec<Log>, ReorgSignal)>> + Send;
}

/// Drive `pipeline` over `source`, ingesting each block (reorging first when
/// signalled) and invoking `on_block` after each ingest.
///
/// A thin async convenience over the synchronous core: it pulls a block from the
/// `Send` source (the only `.await`), then synchronously
/// [`reorg_to`](EventPipeline::reorg_to) (if signalled) and
/// [`ingest_logs`](EventPipeline::ingest_logs), holding the `!Send` cache only
/// across the synchronous section. `on_block` is where a caller wires
/// [`FreshnessController::on_new_block`](crate::freshness::FreshnessController::on_new_block)
/// and freshness classification of the digest's touched slots.
pub async fn drive<S, F>(
    pipeline: &mut EventPipeline,
    cache: &mut EvmCache,
    mut source: S,
    mut on_block: F,
) where
    S: LogSource,
    F: FnMut(&BlockDigest),
{
    while let Some((block, logs, reorg)) = source.next_block().await {
        if let Some(new_head) = reorg {
            pipeline.reorg_to(cache, new_head);
        }
        let digest = pipeline.ingest_logs(cache, block, &logs);
        on_block(&digest);
    }
}
