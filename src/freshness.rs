//! Freshness control plane and the optimistic verify-and-rerun execution loop.
//!
//! This module is the generic core of the engine's "honest freshness" model: it
//! knows which cached state it can trust, for how long, and how to keep the rest
//! correct without blocking simulations on RPC. It is built from four layers:
//!
//! 1. **Classification** — [`Validity`] (`Pinned` / `Volatile` / `ValidThrough`)
//!    and the [`FreshnessRegistry`] that resolves a validity per `(address, slot)`
//!    with the precedence **slot ▸ account ▸ default**.
//! 2. **Observation** — [`SlotObservationTracker`] records per-slot change
//!    frequency (clock-agnostic) to drive adaptive re-verification, tuned by
//!    [`FreshnessParams`].
//! 3. **Policy** — the [`FreshnessPolicy`] trait decides *which* volatile slots to
//!    verify this cycle; built-ins are [`AlwaysVerify`], [`NeverVerify`] and
//!    [`ObservationDriven`].
//! 4. **Mechanism** — `EvmCache::verify_slots` / `EvmCache::purge_account`, and
//!    the freshness controller that runs the optimistic loop.
//!
//! The clock is configurable via [`FreshnessClock`]: [`BlockClock`] (the default,
//! block-number based) or [`WallClock`] (unix seconds). The controller threads
//! `clock.now()` as `now: u64` through the tracker, the policy, and
//! [`FreshnessRegistry::is_volatile`].
//!
//! # Example
//!
//! Classification + policy selection, no network required:
//!
//! ```
//! use alloy_primitives::{Address, U256};
//! use evm_fork_cache::freshness::{
//!     AlwaysVerify, FreshnessPolicy, FreshnessRegistry, NeverVerify,
//! };
//! use evm_fork_cache::cache::SlotObservationTracker;
//!
//! let pool = Address::repeat_byte(0x01);
//! let slot0 = U256::from(0);
//! let immutable = U256::from(6); // e.g. token0
//!
//! let mut registry = FreshnessRegistry::new(); // default: Volatile
//! registry.pin_slot(pool, immutable); // never re-verified
//!
//! // `now` is in clock units (block number for the default BlockClock).
//! let now = 100;
//! assert!(registry.is_volatile(pool, slot0, now));
//! assert!(!registry.is_volatile(pool, immutable, now));
//!
//! // Policies pick which volatile candidates to verify this cycle.
//! let obs = SlotObservationTracker::new();
//! let candidates = [(pool, slot0)];
//! assert_eq!(AlwaysVerify.select(&candidates, &obs, now), vec![(pool, slot0)]);
//! assert!(NeverVerify.select(&candidates, &obs, now).is_empty());
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_eips::eip2930::AccessList;
use alloy_primitives::{Address, Bytes, U256};
use revm::context::result::ExecutionResult;
use tokio::task::JoinHandle;

use crate::cache::{
    CallSimulationResult, EvmCache, EvmOverlay, EvmSnapshot, SlotObservationTracker,
    StorageBatchFetchFn, TxConfig,
};

/// Default minimum observations before the change-frequency data is trusted.
pub const DEFAULT_MIN_OBSERVATIONS: u32 = 10;

/// Default maximum reuse window, in clock units, before a slot is rechecked.
///
/// Block-based default (≈300 blocks). Wall-clock users typically set this to
/// `7 * 86400` (one week) to reproduce the original behavior.
pub const DEFAULT_MAX_REUSE: u64 = 300;

/// Default refetch threshold on expected probability of change.
pub const DEFAULT_STALENESS_THRESHOLD: f64 = 0.05;

/// Default change-rate above which a slot is always refetched.
pub const DEFAULT_ALWAYS_REFETCH_RATE: f64 = 0.9;

/// Default clock units per "cycle" used by the probabilistic model.
pub const DEFAULT_CYCLE_INTERVAL: u64 = 1;

/// Tunable thresholds for the adaptive freshness model.
///
/// All time-like fields are expressed in **clock units** (`FreshnessClock`):
/// block numbers for a block clock, unix seconds for a wall clock. The defaults
/// are block-oriented; wall-clock users should raise [`max_reuse`](Self::max_reuse)
/// and [`cycle_interval`](Self::cycle_interval) accordingly.
#[derive(Clone, Debug, PartialEq)]
pub struct FreshnessParams {
    /// Minimum observations before the change frequency is trusted (else refetch).
    pub min_observations: u32,
    /// Maximum reuse window (clock units) before a slot is force-rechecked.
    pub max_reuse: u64,
    /// Refetch when the expected probability of change exceeds this threshold.
    pub staleness_threshold: f64,
    /// Slots changing more often than this rate are always refetched.
    pub always_refetch_rate: f64,
    /// Clock units per "cycle" for the probabilistic expected-change estimate.
    /// Must be non-zero; a zero is treated as one to avoid division by zero.
    pub cycle_interval: u64,
}

impl Default for FreshnessParams {
    fn default() -> Self {
        Self {
            min_observations: DEFAULT_MIN_OBSERVATIONS,
            max_reuse: DEFAULT_MAX_REUSE,
            staleness_threshold: DEFAULT_STALENESS_THRESHOLD,
            always_refetch_rate: DEFAULT_ALWAYS_REFETCH_RATE,
            cycle_interval: DEFAULT_CYCLE_INTERVAL,
        }
    }
}

impl FreshnessParams {
    /// Block-oriented defaults (`max_reuse ≈ 300` blocks, one cycle per block).
    pub fn for_block_clock() -> Self {
        Self::default()
    }

    /// Wall-clock defaults: reuse up to one week, ~60s cycles, matching the
    /// original (pre-Phase-2) hardcoded behavior of the observation tracker.
    pub fn for_wall_clock() -> Self {
        Self {
            max_reuse: 7 * 86400,
            cycle_interval: 60,
            ..Self::default()
        }
    }
}

// ---------------------------------------------------------------------------
// 1. Classification
// ---------------------------------------------------------------------------

/// How long a cached account or storage slot can be trusted.
///
/// Resolution precedence is **slot ▸ account ▸ default** (see
/// [`FreshnessRegistry::validity`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Validity {
    /// Caller-owned: immutable, or kept fresh out-of-band (e.g. via event
    /// writes). The freshness system never re-verifies or purges it.
    Pinned,
    /// Governed by the active [`FreshnessPolicy`]; may be re-verified each cycle.
    Volatile,
    /// Pinned until clock value `N` (inclusive), then treated as [`Volatile`].
    ///
    /// [`Volatile`]: Validity::Volatile
    ValidThrough(u64),
}

/// Per-address / per-slot validity classification.
///
/// A slot's validity is resolved with the precedence **slot ▸ account ▸
/// default**: an explicit `(address, slot)` entry wins, else the account-level
/// entry for `address`, else the registry default ([`Validity::Volatile`] unless
/// changed via [`with_default`](Self::with_default)).
///
/// The setters are builder-style (`&mut Self`) so they can be chained.
#[derive(Clone, Debug)]
pub struct FreshnessRegistry {
    default: Validity,
    accounts: HashMap<Address, Validity>,
    slots: HashMap<(Address, U256), Validity>,
}

impl Default for FreshnessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FreshnessRegistry {
    /// A registry whose default validity is [`Validity::Volatile`].
    pub fn new() -> Self {
        Self {
            default: Validity::Volatile,
            accounts: HashMap::new(),
            slots: HashMap::new(),
        }
    }

    /// A registry with a custom default validity for unclassified state.
    pub fn with_default(default: Validity) -> Self {
        Self {
            default,
            accounts: HashMap::new(),
            slots: HashMap::new(),
        }
    }

    /// The default validity applied when neither the slot nor the account is set.
    pub fn default_validity(&self) -> Validity {
        self.default
    }

    /// Pin an account ([`Validity::Pinned`]).
    pub fn pin(&mut self, addr: Address) -> &mut Self {
        self.set_account(addr, Validity::Pinned)
    }

    /// Pin a single slot ([`Validity::Pinned`]).
    pub fn pin_slot(&mut self, addr: Address, slot: U256) -> &mut Self {
        self.set_slot(addr, slot, Validity::Pinned)
    }

    /// Mark an account [`Validity::Volatile`].
    pub fn mark_volatile(&mut self, addr: Address) -> &mut Self {
        self.set_account(addr, Validity::Volatile)
    }

    /// Mark a single slot [`Validity::Volatile`].
    pub fn mark_volatile_slot(&mut self, addr: Address, slot: U256) -> &mut Self {
        self.set_slot(addr, slot, Validity::Volatile)
    }

    /// Mark an account [`Validity::ValidThrough`] block/clock `n`.
    pub fn valid_through(&mut self, addr: Address, n: u64) -> &mut Self {
        self.set_account(addr, Validity::ValidThrough(n))
    }

    /// Mark a single slot [`Validity::ValidThrough`] block/clock `n`.
    pub fn valid_through_slot(&mut self, addr: Address, slot: U256, n: u64) -> &mut Self {
        self.set_slot(addr, slot, Validity::ValidThrough(n))
    }

    /// Set the account-level validity for `addr`.
    pub fn set_account(&mut self, addr: Address, validity: Validity) -> &mut Self {
        self.accounts.insert(addr, validity);
        self
    }

    /// Set the slot-level validity for `(addr, slot)`.
    pub fn set_slot(&mut self, addr: Address, slot: U256, validity: Validity) -> &mut Self {
        self.slots.insert((addr, slot), validity);
        self
    }

    /// Resolve the validity of `(addr, slot)` with **slot ▸ account ▸ default**.
    pub fn validity(&self, addr: Address, slot: U256) -> Validity {
        if let Some(v) = self.slots.get(&(addr, slot)) {
            return *v;
        }
        if let Some(v) = self.accounts.get(&addr) {
            return *v;
        }
        self.default
    }

    /// Whether `(addr, slot)` is currently volatile (subject to verification).
    ///
    /// `true` for [`Validity::Volatile`], and for [`Validity::ValidThrough`]`(m)`
    /// once `now > m`. `false` for [`Validity::Pinned`] and a still-valid
    /// `ValidThrough` (`now <= m`).
    pub fn is_volatile(&self, addr: Address, slot: U256, now: u64) -> bool {
        match self.validity(addr, slot) {
            Validity::Pinned => false,
            Validity::Volatile => true,
            Validity::ValidThrough(m) => now > m,
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Clock
// ---------------------------------------------------------------------------

/// Source of the current clock value used throughout the freshness model.
///
/// Implementations return a monotone-ish `u64` in their own units. The two
/// built-ins are [`BlockClock`] (block number, the default) and [`WallClock`]
/// (unix seconds).
pub trait FreshnessClock: Send + Sync {
    /// The current clock value (block number or unix seconds).
    fn now(&self) -> u64;

    /// Advance the clock to `now`.
    ///
    /// Called by [`FreshnessController::on_new_block`] so the natural API drives
    /// the clock forward. The default is a no-op (for clocks like [`WallClock`]
    /// that advance on their own); [`BlockClock`] overrides it to set the block.
    fn advance(&self, _now: u64) {}
}

/// Block-number clock (the default). Cloning shares the underlying counter, so a
/// clone observed by a background task sees [`set_block`](Self::set_block)
/// updates made on the main thread.
#[derive(Clone, Debug, Default)]
pub struct BlockClock(Arc<AtomicU64>);

impl BlockClock {
    /// A block clock starting at block 0.
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// A block clock starting at `block`.
    pub fn at(block: u64) -> Self {
        Self(Arc::new(AtomicU64::new(block)))
    }

    /// Set the current block number. Shared across clones.
    pub fn set_block(&self, block: u64) {
        self.0.store(block, Ordering::Relaxed);
    }
}

impl FreshnessClock for BlockClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Set the current block to `now` (shared across clones).
    fn advance(&self, now: u64) {
        self.set_block(now);
    }
}

/// Wall-clock clock: [`now`](FreshnessClock::now) returns unix seconds.
#[derive(Clone, Copy, Debug, Default)]
pub struct WallClock;

impl FreshnessClock for WallClock {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// 3. Policy
// ---------------------------------------------------------------------------

/// Decides which volatile candidate slots must be verified this cycle.
///
/// The controller passes the volatile candidates (predicted read set) plus the
/// current observation stats and `now`; the policy returns the subset to
/// re-fetch. Correctness does not depend on the policy being complete — the
/// background validator always re-checks each sim's *actual* volatile read set
/// before trusting results — so a policy only trades RPC cost against how often a
/// `Corrected` verdict is needed.
pub trait FreshnessPolicy: Send {
    /// Of these volatile candidate slots, which must be verified this cycle?
    fn select(
        &mut self,
        candidates: &[(Address, U256)],
        obs: &SlotObservationTracker,
        now: u64,
    ) -> Vec<(Address, U256)>;

    /// Hook called when the controller advances to a new block.
    fn on_new_block(&mut self, _block: u64) {}
}

/// Verifies every volatile candidate (safe / eager). Always correct, most RPC.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysVerify;

impl FreshnessPolicy for AlwaysVerify {
    fn select(
        &mut self,
        candidates: &[(Address, U256)],
        _obs: &SlotObservationTracker,
        _now: u64,
    ) -> Vec<(Address, U256)> {
        candidates.to_vec()
    }
}

/// Verifies nothing (trust-all). Selects no slots from the predicted set, though
/// the actual-read-set reconcile in the background validator can still surface
/// changes.
#[derive(Clone, Copy, Debug, Default)]
pub struct NeverVerify;

impl FreshnessPolicy for NeverVerify {
    fn select(
        &mut self,
        _candidates: &[(Address, U256)],
        _obs: &SlotObservationTracker,
        _now: u64,
    ) -> Vec<(Address, U256)> {
        Vec::new()
    }
}

/// Adaptive policy: verifies candidates the observation tracker flags via
/// [`should_refetch`](crate::cache::SlotObservationTracker::should_refetch).
#[derive(Clone, Debug, Default)]
pub struct ObservationDriven {
    /// Thresholds for the underlying `should_refetch` heuristic.
    pub params: FreshnessParams,
}

impl ObservationDriven {
    /// An observation-driven policy with the given parameters.
    pub fn new(params: FreshnessParams) -> Self {
        Self { params }
    }
}

impl FreshnessPolicy for ObservationDriven {
    fn select(
        &mut self,
        candidates: &[(Address, U256)],
        obs: &SlotObservationTracker,
        now: u64,
    ) -> Vec<(Address, U256)> {
        candidates
            .iter()
            .copied()
            .filter(|(addr, slot)| obs.should_refetch(*addr, *slot, now, &self.params))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// 4. Results
// ---------------------------------------------------------------------------

/// A storage slot whose freshly-fetched value differs from the cached value.
///
/// Produced by [`EvmCache::verify_slots`](crate::cache::EvmCache::verify_slots)
/// and by the background validator; `old` is the value the snapshot/cache held,
/// `new` is the value the fetcher returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotChange {
    /// Contract whose storage changed.
    pub address: Address,
    /// Storage slot key.
    pub slot: U256,
    /// Value previously held in the cache/snapshot.
    pub old: U256,
    /// Freshly-fetched value.
    pub new: U256,
}

/// The deferred verdict on a [`SpeculativeSim`]'s optimistic results.
pub enum Validation {
    /// Nothing the sims read had changed; the optimistic results are correct.
    Confirmed,
    /// At least one read slot changed. `results` is the optimistic set with the
    /// affected sims re-run against the fresh values; `changed` lists the slots
    /// that differed (also queued for flow-back into the cache).
    Corrected {
        /// Optimistic results with the affected sims replaced by re-runs.
        results: Vec<CallSimulationResult>,
        /// Slots whose fresh value differed from the snapshot.
        changed: Vec<SlotChange>,
    },
    /// The fetcher failed, so the results could not be validated. The optimistic
    /// results are *not* trusted.
    Unverified {
        /// Human-readable description of why validation could not complete.
        reason: String,
    },
}

impl std::fmt::Debug for Validation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Validation::Confirmed => write!(f, "Confirmed"),
            Validation::Corrected { changed, .. } => f
                .debug_struct("Corrected")
                .field("changed", changed)
                .finish_non_exhaustive(),
            Validation::Unverified { reason } => f
                .debug_struct("Unverified")
                .field("reason", reason)
                .finish(),
        }
    }
}

/// A single non-committing simulation request for the optimistic loop.
///
/// `tx.access_list` is the *predicted* read set (a performance hint that seeds
/// the verify candidates); correctness does not depend on it because the
/// background validator re-checks each sim's actual volatile read set.
#[derive(Clone, Debug)]
pub struct SimRequest {
    /// Transaction sender.
    pub from: Address,
    /// Call target.
    pub to: Address,
    /// Calldata.
    pub calldata: Bytes,
    /// Per-call tx environment; `tx.access_list` is the predicted read set.
    pub tx: TxConfig,
}

impl SimRequest {
    /// A zero-value request with default tx environment.
    pub fn new(from: Address, to: Address, calldata: Bytes) -> Self {
        Self {
            from,
            to,
            calldata,
            tx: TxConfig::default(),
        }
    }

    /// Set the predicted read set (EIP-2930 access list hint).
    pub fn with_access_list(mut self, access_list: AccessList) -> Self {
        self.tx.access_list = Some(access_list);
        self
    }
}

/// Optimistic simulation results plus a handle to their deferred validation.
///
/// Returned by [`FreshnessController::run`] as soon as the optimistic sims
/// finish (without awaiting RPC). Read [`optimistic`](Self::optimistic)
/// immediately, then `await` [`validate`](Self::validate) for the verdict.
/// Dropping the handle aborts the background validation task.
pub struct SpeculativeSim {
    optimistic: Vec<CallSimulationResult>,
    /// `Option` so `validate`/`into_optimistic` can take the handle and skip the
    /// abort-on-drop; `Drop` only aborts a handle still left in place.
    validation: Option<JoinHandle<Validation>>,
}

impl SpeculativeSim {
    /// The optimistic results, readable before validation completes.
    pub fn optimistic(&self) -> &[CallSimulationResult] {
        &self.optimistic
    }

    /// Consume the handle and return the optimistic results, aborting the
    /// background validation task.
    pub fn into_optimistic(mut self) -> Vec<CallSimulationResult> {
        if let Some(handle) = self.validation.take() {
            handle.abort();
        }
        std::mem::take(&mut self.optimistic)
    }

    /// Await the deferred validation verdict.
    ///
    /// If the background task panicked or was cancelled, returns
    /// [`Validation::Unverified`].
    pub async fn validate(mut self) -> Validation {
        let handle = self
            .validation
            .take()
            .expect("validation handle taken twice");
        match handle.await {
            Ok(v) => v,
            Err(e) => Validation::Unverified {
                reason: format!("validation task failed: {e}"),
            },
        }
    }
}

impl Drop for SpeculativeSim {
    fn drop(&mut self) {
        if let Some(handle) = self.validation.take() {
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

/// Drives the optimistic verify-and-rerun loop over an [`EvmCache`].
///
/// Holds the freshness [`FreshnessRegistry`], the shared
/// [`SlotObservationTracker`], a [`FreshnessPolicy`], a [`FreshnessClock`], and
/// the pending-corrections queue. The tracker and the pending queue are
/// `Arc<Mutex<…>>` so the background validator can update them without touching
/// the `!Send` cache. Adaptive thresholds ([`FreshnessParams`]) live on the
/// policy that uses them ([`ObservationDriven`]), not on the controller.
///
/// # Runtime requirement
/// [`run`](Self::run) spawns a background task and the (synchronous) fetcher uses
/// `block_in_place` internally, so a **multi-thread** tokio runtime is required
/// (`#[tokio::main(flavor = "multi_thread")]` or
/// `Builder::new_multi_thread()`), mirroring the [`EvmCache`] constructor note.
pub struct FreshnessController<P: FreshnessPolicy, C: FreshnessClock> {
    registry: FreshnessRegistry,
    tracker: Arc<Mutex<SlotObservationTracker>>,
    policy: P,
    clock: C,
    pending: Arc<Mutex<Vec<SlotChange>>>,
}

impl<P: FreshnessPolicy> FreshnessController<P, BlockClock> {
    /// Build a controller with the default [`BlockClock`].
    pub fn new(registry: FreshnessRegistry, policy: P) -> Self {
        Self::with_clock(registry, policy, BlockClock::new())
    }
}

impl<P: FreshnessPolicy, C: FreshnessClock> FreshnessController<P, C> {
    /// Build a controller with an explicit clock.
    pub fn with_clock(registry: FreshnessRegistry, policy: P, clock: C) -> Self {
        Self {
            registry,
            tracker: Arc::new(Mutex::new(SlotObservationTracker::new())),
            policy,
            clock,
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Use an existing shared observation tracker (e.g. a persisted one).
    pub fn with_tracker(mut self, tracker: Arc<Mutex<SlotObservationTracker>>) -> Self {
        self.tracker = tracker;
        self
    }

    /// The shared observation tracker.
    pub fn tracker(&self) -> &Arc<Mutex<SlotObservationTracker>> {
        &self.tracker
    }

    /// The freshness registry.
    pub fn registry(&self) -> &FreshnessRegistry {
        &self.registry
    }

    /// Mutable access to the freshness registry.
    pub fn registry_mut(&mut self) -> &mut FreshnessRegistry {
        &mut self.registry
    }

    /// Number of corrections waiting to be drained into the cache on the next
    /// [`run`](Self::run).
    pub fn pending_len(&self) -> usize {
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Advance to a new block.
    ///
    /// Advances the clock to `block` (a no-op for [`WallClock`], a `set_block`
    /// for [`BlockClock`]) and then notifies the policy. Advancing the clock is
    /// what ages [`Validity::ValidThrough`] slots into [`Validity::Volatile`] and
    /// progresses the observation-tracker reuse window through the natural API.
    pub fn on_new_block(&mut self, block: u64) {
        self.clock.advance(block);
        self.policy.on_new_block(block);
    }

    /// Run the optimistic loop for a batch of requests.
    ///
    /// 1. Drain queued corrections from prior cycles into the cache.
    /// 2. Snapshot the cache and grab the batch fetcher.
    /// 3. Run each request optimistically against the snapshot, capturing its
    ///    actual volatile read set.
    /// 4. Compute the predicted volatile candidates and ask the policy which to
    ///    verify.
    /// 5. Spawn the background validator (Send data only) and return a
    ///    [`SpeculativeSim`] immediately.
    pub fn run(
        &mut self,
        cache: &mut EvmCache,
        requests: Vec<SimRequest>,
    ) -> anyhow::Result<SpeculativeSim> {
        let now = self.clock.now();

        // 1. Drain pending corrections into the cache before snapshotting.
        {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            if !pending.is_empty() {
                let injects: Vec<(Address, U256, U256)> =
                    pending.iter().map(|c| (c.address, c.slot, c.new)).collect();
                cache.inject_storage_batch(&injects);
                pending.clear();
            }
        }

        // 2. Snapshot + fetcher (Arc clones, both Send).
        let snapshot = cache.create_snapshot();
        let fetcher = cache.storage_batch_fetcher().cloned();

        // 3. Optimistic sims + per-sim actual volatile read sets.
        let mut optimistic = Vec::with_capacity(requests.len());
        let mut read_sets: Vec<Vec<(Address, U256)>> = Vec::with_capacity(requests.len());
        for req in &requests {
            let mut overlay = EvmOverlay::new(Arc::clone(&snapshot), None);
            let (result, access) =
                overlay.call_raw_with_access_list(req.from, req.to, req.calldata.clone())?;
            optimistic.push(result_to_sim(result, &access.to_eip2930()));

            let volatile: Vec<(Address, U256)> = access
                .slots
                .iter()
                .copied()
                .filter(|(addr, slot)| self.registry.is_volatile(*addr, *slot, now))
                .collect();
            read_sets.push(volatile);
        }

        // 4. Predicted candidates (union of request access lists, volatile only).
        let mut candidate_set: HashSet<(Address, U256)> = HashSet::new();
        for req in &requests {
            if let Some(al) = &req.tx.access_list {
                for item in &al.0 {
                    for key in &item.storage_keys {
                        let slot = U256::from_be_bytes(key.0);
                        if self.registry.is_volatile(item.address, slot, now) {
                            candidate_set.insert((item.address, slot));
                        }
                    }
                }
            }
        }
        let candidates: Vec<(Address, U256)> = candidate_set.into_iter().collect();
        let verify_set = {
            let tracker = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
            self.policy.select(&candidates, &tracker, now)
        };

        // 5. Spawn the validator with Send-only data.
        let registry = self.registry.clone();
        let tracker = Arc::clone(&self.tracker);
        let pending = Arc::clone(&self.pending);
        let optimistic_for_task = optimistic.clone();
        let validation = tokio::spawn(async move {
            run_validator(ValidatorInput {
                snapshot,
                fetcher,
                requests,
                read_sets,
                registry,
                tracker,
                pending,
                now,
                verify_set,
                optimistic: optimistic_for_task,
            })
        });

        Ok(SpeculativeSim {
            optimistic,
            validation: Some(validation),
        })
    }
}

/// Owned inputs handed to the background validator (all `Send`).
struct ValidatorInput {
    snapshot: Arc<EvmSnapshot>,
    fetcher: Option<StorageBatchFetchFn>,
    requests: Vec<SimRequest>,
    read_sets: Vec<Vec<(Address, U256)>>,
    registry: FreshnessRegistry,
    tracker: Arc<Mutex<SlotObservationTracker>>,
    pending: Arc<Mutex<Vec<SlotChange>>>,
    now: u64,
    verify_set: Vec<(Address, U256)>,
    optimistic: Vec<CallSimulationResult>,
}

/// The background validation routine. Touches only `Send` data — never the cache.
fn run_validator(input: ValidatorInput) -> Validation {
    let ValidatorInput {
        snapshot,
        fetcher,
        requests,
        read_sets,
        registry,
        tracker,
        pending,
        now,
        verify_set,
        optimistic,
    } = input;

    let Some(fetcher) = fetcher else {
        return Validation::Unverified {
            reason: "no storage batch fetcher available".to_string(),
        };
    };

    // verify = policy-selected set ∪ each sim's actual volatile read set,
    // re-filtered through the registry clone so only currently-volatile slots
    // are checked (defensive: read sets and the policy selection are already
    // volatile-filtered on the main thread).
    let mut verify: HashSet<(Address, U256)> = verify_set.into_iter().collect();
    for set in &read_sets {
        verify.extend(set.iter().copied());
    }
    verify.retain(|(addr, slot)| registry.is_volatile(*addr, *slot, now));
    if verify.is_empty() {
        return Validation::Confirmed;
    }
    let verify: Vec<(Address, U256)> = verify.into_iter().collect();

    // Fetch fresh values. Any error → Unverified (never trust silently).
    let results = (fetcher)(verify.clone());
    let mut fresh: HashMap<(Address, U256), U256> = HashMap::new();
    for (addr, slot, value) in results {
        match value {
            Ok(v) => {
                fresh.insert((addr, slot), v);
            }
            Err(e) => {
                return Validation::Unverified {
                    reason: format!("fetch failed for {addr}:{slot}: {e}"),
                };
            }
        }
    }

    // Compare against the snapshot, observe each checked slot, collect changes.
    let mut changed = Vec::new();
    {
        let mut tracker = tracker.lock().unwrap_or_else(|e| e.into_inner());
        for &(addr, slot) in &verify {
            let new = fresh.get(&(addr, slot)).copied().unwrap_or(U256::ZERO);
            let old = snapshot.storage_value(addr, slot).unwrap_or(U256::ZERO);
            tracker.observe(addr, slot, new, now);
            if new != old {
                changed.push(SlotChange {
                    address: addr,
                    slot,
                    old,
                    new,
                });
            }
        }
    }

    if changed.is_empty() {
        return Validation::Confirmed;
    }

    // Queue corrections for flow-back into the cache on the next run.
    {
        let mut pending = pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.extend(changed.iter().cloned());
    }

    // Re-run only the sims whose read set intersects the changed slots.
    let changed_keys: HashSet<(Address, U256)> =
        changed.iter().map(|c| (c.address, c.slot)).collect();
    let overrides: Vec<(Address, U256, U256)> =
        changed.iter().map(|c| (c.address, c.slot, c.new)).collect();

    let mut results = optimistic;
    for (i, req) in requests.iter().enumerate() {
        let read_set = &read_sets[i];
        let intersects = read_set.iter().any(|k| changed_keys.contains(k));
        if !intersects {
            continue;
        }
        let mut overlay = EvmOverlay::new(Arc::clone(&snapshot), None);
        for &(addr, slot, value) in &overrides {
            overlay.override_slot(addr, slot, value);
        }
        if let Ok((result, access)) =
            overlay.call_raw_with_access_list(req.from, req.to, req.calldata.clone())
        {
            results[i] = result_to_sim(result, &access.to_eip2930());
        }
    }

    Validation::Corrected { results, changed }
}

/// Build a [`CallSimulationResult`] from a non-committing execution result and
/// its captured access list. `token_deltas` is empty (the optimistic path does
/// not run transfer tracking); gas, logs, and return data come from the
/// execution result. `output` carries the `Success`/`Revert` payload (empty on
/// `Halt`), so a corrected view-call's new return value is observable here.
fn result_to_sim(result: ExecutionResult, access_list: &AccessList) -> CallSimulationResult {
    let (gas_used, logs, output) = match result {
        ExecutionResult::Success {
            gas_used,
            logs,
            output,
            ..
        } => (gas_used, logs, output.into_data()),
        ExecutionResult::Revert { gas_used, output } => (gas_used, Vec::new(), output),
        ExecutionResult::Halt { gas_used, .. } => (gas_used, Vec::new(), Bytes::new()),
    };
    CallSimulationResult {
        gas_used,
        token_deltas: HashMap::new(),
        logs,
        access_list: access_list.clone(),
        output,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> Address {
        Address::repeat_byte(n)
    }

    // --- Classification ----------------------------------------------------

    #[test]
    fn registry_default_is_volatile() {
        let reg = FreshnessRegistry::new();
        assert_eq!(reg.default_validity(), Validity::Volatile);
        assert_eq!(reg.validity(addr(1), U256::from(0)), Validity::Volatile);
    }

    #[test]
    fn registry_with_default_overrides_unclassified() {
        let reg = FreshnessRegistry::with_default(Validity::Pinned);
        assert_eq!(reg.validity(addr(1), U256::from(0)), Validity::Pinned);
        assert!(!reg.is_volatile(addr(1), U256::from(0), 100));
    }

    #[test]
    fn registry_resolution_order_slot_account_default() {
        let a = addr(1);
        let mut reg = FreshnessRegistry::new(); // default Volatile
        reg.pin(a); // account-level Pinned
        reg.mark_volatile_slot(a, U256::from(7)); // slot-level Volatile

        // slot-level wins over account-level
        assert_eq!(reg.validity(a, U256::from(7)), Validity::Volatile);
        // account-level wins over default for a non-overridden slot
        assert_eq!(reg.validity(a, U256::from(8)), Validity::Pinned);
        // default for an unrelated account
        assert_eq!(reg.validity(addr(2), U256::from(7)), Validity::Volatile);
    }

    #[test]
    fn is_volatile_per_variant() {
        let a = addr(1);
        let mut reg = FreshnessRegistry::new();
        reg.pin_slot(a, U256::from(1));
        reg.mark_volatile_slot(a, U256::from(2));
        reg.valid_through_slot(a, U256::from(3), 50);

        assert!(!reg.is_volatile(a, U256::from(1), 100)); // Pinned
        assert!(reg.is_volatile(a, U256::from(2), 100)); // Volatile
    }

    #[test]
    fn valid_through_boundary() {
        let a = addr(1);
        let slot = U256::from(3);
        let mut reg = FreshnessRegistry::new();
        reg.valid_through_slot(a, slot, 50);

        assert!(!reg.is_volatile(a, slot, 49)); // before
        assert!(!reg.is_volatile(a, slot, 50)); // at boundary: still valid (now == m)
        assert!(reg.is_volatile(a, slot, 51)); // after: now > m
    }

    #[test]
    fn registry_is_clone() {
        let mut reg = FreshnessRegistry::new();
        reg.pin(addr(1));
        let clone = reg.clone();
        assert_eq!(clone.validity(addr(1), U256::from(0)), Validity::Pinned);
    }

    // --- Clock -------------------------------------------------------------

    #[test]
    fn block_clock_default_and_set() {
        let clock = BlockClock::new();
        assert_eq!(clock.now(), 0);
        clock.set_block(123);
        assert_eq!(clock.now(), 123);
    }

    #[test]
    fn block_clock_clone_shares_counter() {
        let clock = BlockClock::at(10);
        let clone = clock.clone();
        clock.set_block(42);
        // The clone observes the update through the shared Arc.
        assert_eq!(clone.now(), 42);
    }

    #[test]
    fn wall_clock_is_unix_seconds() {
        let now = WallClock.now();
        // Sanity: after 2021-01-01.
        assert!(now > 1_600_000_000);
    }

    // --- Policy ------------------------------------------------------------

    #[test]
    fn always_verify_selects_all() {
        let obs = SlotObservationTracker::new();
        let candidates = [(addr(1), U256::from(0)), (addr(2), U256::from(1))];
        let mut policy = AlwaysVerify;
        assert_eq!(policy.select(&candidates, &obs, 0), candidates.to_vec());
    }

    #[test]
    fn never_verify_selects_none() {
        let obs = SlotObservationTracker::new();
        let candidates = [(addr(1), U256::from(0))];
        let mut policy = NeverVerify;
        assert!(policy.select(&candidates, &obs, 0).is_empty());
    }

    #[test]
    fn observation_driven_selects_only_should_refetch() {
        let mut obs = SlotObservationTracker::new();
        let params = FreshnessParams::default();
        let stable = (addr(1), U256::from(0));
        let unknown = (addr(2), U256::from(0));

        // Build a stable (never-changed) slot with enough observations so
        // `should_refetch` returns false for it.
        for now in 0..params.min_observations {
            obs.observe(stable.0, stable.1, U256::from(42), now as u64);
        }
        let now = params.min_observations as u64 - 1;
        assert!(!obs.should_refetch(stable.0, stable.1, now, &params));
        assert!(obs.should_refetch(unknown.0, unknown.1, now, &params));

        let mut policy = ObservationDriven::new(params);
        let selected = policy.select(&[stable, unknown], &obs, now);
        assert_eq!(selected, vec![unknown]);
    }
}
