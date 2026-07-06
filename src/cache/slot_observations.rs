//! Storage-slot change-frequency tracking and freshness heuristics.
//!
//! To decide which cached storage slots can be reused and which must be
//! re-fetched, this module records how often each observed slot changes over
//! time. Slots that change frequently are rechecked sooner; stable slots are
//! trusted longer (subject to a maximum age). The observations are persisted to
//! disk so the heuristics survive across runs.
//!
//! # Clock-agnostic
//!
//! The tracker does not read the wall clock itself: callers pass `now` (in
//! clock units) into [`observe`](SlotObservationTracker::observe) and
//! [`should_refetch`](SlotObservationTracker::should_refetch), and the thresholds
//! live in a [`crate::freshness::FreshnessParams`]. This lets the freshness
//! controller drive the tracker from either a block clock or a wall clock.

use std::{collections::HashMap, path::Path};

use alloy_primitives::{Address, U256};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::freshness::FreshnessParams;

use super::versioned;
use crate::errors::PersistenceError;

const SLOT_OBSERVATIONS_MAGIC: &[u8; 8] = b"EFC-SOBS";
const SLOT_OBSERVATIONS_VERSION: u32 = 1;

/// Per-slot observation record, persisted to disk.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SlotObservation {
    /// Most recently observed slot value.
    pub last_value: U256,
    /// Total number of times this slot has been observed.
    pub observation_count: u32,
    /// Number of observations that differed from the previous value.
    pub change_count: u32,
    /// Clock value (block number or unix seconds) of the most recent observation.
    pub last_checked: u64,
    /// Clock value of the most recent value change.
    pub last_changed: u64,
}

/// Serializable key type for bincode persistence.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
struct SlotKey {
    address: Address,
    slot: U256,
}

/// Tracks per-slot change frequency to drive intelligent purge decisions.
///
/// Before purging a slot, check `should_refetch()`. Slots that rarely change
/// are kept in the EVM cache, avoiding unnecessary RPC calls. On simulation
/// revert, `take_skipped()` returns all slots that were kept, allowing a
/// full-refresh fallback.
pub struct SlotObservationTracker {
    observations: HashMap<SlotKey, SlotObservation>,
    /// Slots skipped (not purged) this cycle — kept for revert fallback.
    skipped_this_cycle: Vec<(Address, U256)>,
    dirty: bool,
}

impl SlotObservationTracker {
    /// Create an empty tracker with no recorded observations.
    pub fn new() -> Self {
        Self {
            observations: HashMap::new(),
            skipped_this_cycle: Vec::new(),
            dirty: false,
        }
    }

    /// Load persisted observations from disk (versioned binary format).
    ///
    /// Returns a fresh tracker if the file doesn't exist, has an unrecognized
    /// magic/version header, or can't be decoded. Legacy unversioned bincode is
    /// treated as a cache miss.
    pub fn load(path: &Path) -> Self {
        match std::fs::read(path) {
            Ok(data) => {
                if let Some(observations) = versioned::decode::<HashMap<SlotKey, SlotObservation>>(
                    &data,
                    SLOT_OBSERVATIONS_MAGIC,
                    SLOT_OBSERVATIONS_VERSION,
                    "slot observations",
                ) {
                    debug!(
                        entries = observations.len(),
                        "Loaded slot observation tracker"
                    );
                    Self {
                        observations,
                        skipped_this_cycle: Vec::new(),
                        dirty: false,
                    }
                } else {
                    warn!("Slot observations cache miss, starting fresh");
                    Self::new()
                }
            }
            Err(_) => {
                debug!("No slot observations file found, starting fresh");
                Self::new()
            }
        }
    }

    /// Persist observations to disk using the versioned binary format.
    /// Called at end of cycle or on shutdown.
    pub fn save(&mut self, path: &Path) -> Result<(), PersistenceError> {
        if !self.dirty {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| PersistenceError::create_dir(parent, err))?;
        }
        let data = versioned::encode(
            SLOT_OBSERVATIONS_MAGIC,
            SLOT_OBSERVATIONS_VERSION,
            &self.observations,
            "slot observations",
        )?;
        std::fs::write(path, data).map_err(|err| PersistenceError::write(path, err))?;
        self.dirty = false;
        debug!(entries = self.observations.len(), "Saved slot observations");
        Ok(())
    }

    /// Core decision: should we re-fetch this slot from RPC?
    ///
    /// Returns `true` if the slot should be purged and re-fetched.
    /// Returns `false` if the cached value is likely still valid.
    ///
    /// `now` is the current clock value (block number or unix seconds) and
    /// `params` carries the (clock-unit) thresholds — see
    /// [`crate::freshness::FreshnessParams`].
    ///
    /// The heuristic is fully deterministic (no randomness): a never-observed slot
    /// always refetches, as does one with fewer than
    /// [`min_observations`](crate::freshness::FreshnessParams::min_observations);
    /// once enough observations accrue, a never-changed slot is reused until the
    /// [`max_reuse`](crate::freshness::FreshnessParams::max_reuse) window elapses,
    /// while changing slots refetch once the probabilistic expected-change estimate
    /// crosses [`staleness_threshold`](crate::freshness::FreshnessParams::staleness_threshold).
    ///
    /// # Examples
    /// The deterministic threshold edges around a stable (never-changed) slot:
    ///
    /// ```
    /// use alloy_primitives::{Address, U256};
    /// use evm_fork_cache::cache::SlotObservationTracker;
    /// use evm_fork_cache::freshness::FreshnessParams;
    ///
    /// let params = FreshnessParams::default();
    /// let mut tracker = SlotObservationTracker::new();
    /// let addr = Address::repeat_byte(0x01);
    /// let slot = U256::from(0);
    ///
    /// // An unobserved slot must always be fetched.
    /// assert!(tracker.should_refetch(addr, slot, 0, &params));
    ///
    /// // Record fewer than `min_observations` of the same value: still refetches
    /// // because there is not enough data to trust the change frequency.
    /// for now in 0..(params.min_observations - 1) {
    ///     tracker.observe(addr, slot, U256::from(42), now as u64);
    /// }
    /// assert!(tracker.should_refetch(addr, slot, params.min_observations as u64, &params));
    ///
    /// // One more identical observation reaches `min_observations`; the slot has
    /// // never changed, so within the reuse window it is now reused (no refetch).
    /// let last = params.min_observations as u64 - 1;
    /// tracker.observe(addr, slot, U256::from(42), last);
    /// assert!(!tracker.should_refetch(addr, slot, last, &params));
    /// ```
    pub fn should_refetch(
        &self,
        addr: Address,
        slot: U256,
        now: u64,
        params: &FreshnessParams,
    ) -> bool {
        let key = SlotKey {
            address: addr,
            slot,
        };
        let Some(obs) = self.observations.get(&key) else {
            return true; // never observed → must fetch
        };

        // Always refetch if insufficient data to make predictions
        if obs.observation_count < params.min_observations {
            return true;
        }

        // Always refetch if last check was longer than the reuse window ago
        if now.saturating_sub(obs.last_checked) > params.max_reuse {
            return true;
        }

        // Never-changed slots: reuse up to the max-reuse window
        if obs.change_count == 0 {
            return false;
        }

        let change_rate = obs.change_count as f64 / obs.observation_count as f64;

        // Always-changing slots: always refetch
        if change_rate > params.always_refetch_rate {
            return true;
        }

        // Probabilistic: estimate expected changes since last check
        let units_elapsed = now.saturating_sub(obs.last_checked) as f64;
        let cycle_interval = params.cycle_interval.max(1) as f64;
        let cycles_elapsed = (units_elapsed / cycle_interval).max(1.0);
        let expected_changes = change_rate * cycles_elapsed;
        expected_changes > params.staleness_threshold
    }

    /// Record a fresh observation after re-fetch or injection.
    ///
    /// `now` is the current clock value (block number or unix seconds).
    /// Returns `true` if the value changed from the last observation.
    pub fn observe(&mut self, addr: Address, slot: U256, value: U256, now: u64) -> bool {
        let key = SlotKey {
            address: addr,
            slot,
        };
        self.dirty = true;

        match self.observations.get_mut(&key) {
            Some(obs) => {
                let changed = obs.last_value != value;
                obs.observation_count += 1;
                if changed {
                    obs.change_count += 1;
                    obs.last_changed = now;
                    obs.last_value = value;
                }
                obs.last_checked = now;
                changed
            }
            None => {
                self.observations.insert(
                    key,
                    SlotObservation {
                        last_value: value,
                        observation_count: 1,
                        change_count: 0, // first observation = baseline, not a "change"
                        last_checked: now,
                        last_changed: 0,
                    },
                );
                false
            }
        }
    }

    /// Record that a slot was skipped (not purged) this cycle.
    /// Used for revert fallback: if simulation fails, these slots need re-fetching.
    pub fn record_skip(&mut self, addr: Address, slot: U256) {
        self.skipped_this_cycle.push((addr, slot));
    }

    /// Take all slots skipped this cycle (for revert recovery).
    /// Clears the internal list.
    pub fn take_skipped(&mut self) -> Vec<(Address, U256)> {
        std::mem::take(&mut self.skipped_this_cycle)
    }

    /// Reset all observations for a contract address.
    /// Called after a simulation revert to force full refresh next cycle.
    pub fn reset_contract(&mut self, addr: Address) {
        self.observations.retain(|k, _| k.address != addr);
        self.dirty = true;
    }

    /// Clear cycle-specific state. Call at the start of each cycle.
    pub fn begin_cycle(&mut self) {
        self.skipped_this_cycle.clear();
    }

    /// Number of tracked slot observations.
    pub fn len(&self) -> usize {
        self.observations.len()
    }

    /// Returns true if no slots are being tracked.
    pub fn is_empty(&self) -> bool {
        self.observations.is_empty()
    }

    /// Returns the last observed value for a slot, if any.
    pub fn last_value(&self, addr: Address, slot: U256) -> Option<U256> {
        let key = SlotKey {
            address: addr,
            slot,
        };
        self.observations.get(&key).map(|o| o.last_value)
    }
}

impl Default for SlotObservationTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> Address {
        Address::new([n; 20])
    }

    /// Block-clock params with a 1-unit cycle so each `observe` advances exactly
    /// one cycle — keeps the probabilistic arithmetic easy to reason about.
    fn params() -> FreshnessParams {
        FreshnessParams::default()
    }

    #[test]
    fn test_unknown_slot_always_refetches() {
        let tracker = SlotObservationTracker::new();
        assert!(tracker.should_refetch(addr(1), U256::from(0), 100, &params()));
    }

    #[test]
    fn test_insufficient_observations_refetches() {
        let mut tracker = SlotObservationTracker::new();
        let p = params();
        let a = addr(1);
        let slot = U256::from(4);
        // Record fewer than `min_observations` observations.
        for now in 0..(p.min_observations - 1) {
            tracker.observe(a, slot, U256::from(42), now as u64);
        }
        assert!(tracker.should_refetch(a, slot, p.min_observations as u64, &p));
    }

    #[test]
    fn test_never_changed_slot_skips_refetch() {
        let mut tracker = SlotObservationTracker::new();
        let p = params();
        let a = addr(1);
        let slot = U256::from(4);
        let value = U256::from(42);
        // Build up enough observations with the same value at consecutive ticks.
        for now in 0..p.min_observations {
            tracker.observe(a, slot, value, now as u64);
        }
        // Re-check immediately after the last observation (within the reuse window).
        assert!(!tracker.should_refetch(a, slot, p.min_observations as u64 - 1, &p));
    }

    #[test]
    fn test_never_changed_slot_refetches_past_max_reuse() {
        let mut tracker = SlotObservationTracker::new();
        let p = params();
        let a = addr(1);
        let slot = U256::from(4);
        for now in 0..p.min_observations {
            tracker.observe(a, slot, U256::from(42), now as u64);
        }
        // Far past the reuse window even a never-changed slot is rechecked.
        let now = p.min_observations as u64 + p.max_reuse + 1;
        assert!(tracker.should_refetch(a, slot, now, &p));
    }

    #[test]
    fn test_always_changing_slot_refetches() {
        let mut tracker = SlotObservationTracker::new();
        let p = params();
        let a = addr(1);
        let slot = U256::from(4);
        // Record observations, each with a different value, at consecutive ticks.
        for now in 0..(p.min_observations + 1) {
            tracker.observe(a, slot, U256::from(now), now as u64);
        }
        assert!(tracker.should_refetch(a, slot, p.min_observations as u64 + 1, &p));
    }

    #[test]
    fn test_observe_returns_changed() {
        let mut tracker = SlotObservationTracker::new();
        let a = addr(1);
        let slot = U256::from(0);
        assert!(!tracker.observe(a, slot, U256::from(1), 0)); // first = baseline
        assert!(!tracker.observe(a, slot, U256::from(1), 1)); // same
        assert!(tracker.observe(a, slot, U256::from(2), 2)); // changed
        assert!(!tracker.observe(a, slot, U256::from(2), 3)); // same again
    }

    #[test]
    fn test_observe_records_change_clock() {
        let mut tracker = SlotObservationTracker::new();
        let a = addr(1);
        let slot = U256::from(0);
        tracker.observe(a, slot, U256::from(1), 10); // baseline at tick 10
        tracker.observe(a, slot, U256::from(2), 25); // change at tick 25
        let key = SlotKey { address: a, slot };
        let obs = &tracker.observations[&key];
        assert_eq!(obs.last_checked, 25);
        assert_eq!(obs.last_changed, 25);
        assert_eq!(obs.change_count, 1);
        assert_eq!(obs.observation_count, 2);
    }

    #[test]
    fn test_reset_contract_clears_observations() {
        let mut tracker = SlotObservationTracker::new();
        let p = params();
        let a = addr(1);
        for i in 0..p.min_observations {
            tracker.observe(a, U256::from(i), U256::from(42), i as u64);
        }
        assert!(!tracker.is_empty());
        tracker.reset_contract(a);
        assert_eq!(tracker.len(), 0);
        // After reset, should_refetch returns true
        assert!(tracker.should_refetch(a, U256::from(0), 100, &p));
    }

    #[test]
    fn test_skipped_slots_tracking() {
        let mut tracker = SlotObservationTracker::new();
        tracker.begin_cycle();
        tracker.record_skip(addr(1), U256::from(0));
        tracker.record_skip(addr(1), U256::from(4));
        tracker.record_skip(addr(2), U256::from(8));

        let skipped = tracker.take_skipped();
        assert_eq!(skipped.len(), 3);
        // After take, list is empty
        assert!(tracker.take_skipped().is_empty());
    }

    #[test]
    fn test_begin_cycle_clears_skipped() {
        let mut tracker = SlotObservationTracker::new();
        tracker.record_skip(addr(1), U256::from(0));
        tracker.begin_cycle();
        assert!(tracker.take_skipped().is_empty());
    }

    /// A unique, freshly-created temp dir keyed by pid so concurrent `cargo
    /// test` processes never share (and never `remove_dir_all`) each other's
    /// directory; each test passes a distinct `tag`. Returns the file path to
    /// write within it.
    fn temp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "evm_fork_cache_slot_obs_{tag}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("observations.bin")
    }

    #[test]
    fn test_save_load_round_trip() {
        let path = temp_path("round_trip");

        let mut tracker = SlotObservationTracker::new();
        let a = addr(1);
        tracker.observe(a, U256::from(0), U256::from(100), 0);
        tracker.observe(a, U256::from(4), U256::from(200), 0);
        tracker.save(&path).unwrap();
        let data = std::fs::read(&path).expect("read saved observations");
        assert!(
            data.starts_with(b"EFC-SOBS"),
            "slot observation files must carry a magic/version header"
        );

        let loaded = SlotObservationTracker::load(&path);
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.last_value(a, U256::from(0)), Some(U256::from(100)));
        assert_eq!(loaded.last_value(a, U256::from(4)), Some(U256::from(200)));
    }

    #[test]
    fn legacy_raw_bincode_loads_as_default() {
        let path = temp_path("legacy");

        let a = addr(1);
        let slot = U256::from(4);
        let mut observations = HashMap::new();
        observations.insert(
            SlotKey { address: a, slot },
            SlotObservation {
                last_value: U256::from(42),
                observation_count: 3,
                change_count: 0,
                last_checked: 2,
                last_changed: 0,
            },
        );
        let legacy = bincode::serialize(&observations).expect("serialize legacy observations");
        std::fs::write(&path, legacy).expect("write legacy observations");

        let loaded = SlotObservationTracker::load(&path);
        assert!(
            loaded.is_empty(),
            "legacy raw bincode must be treated as a cache miss"
        );
    }

    #[test]
    fn test_last_value() {
        let mut tracker = SlotObservationTracker::new();
        let a = addr(1);
        assert_eq!(tracker.last_value(a, U256::from(0)), None);
        tracker.observe(a, U256::from(0), U256::from(42), 0);
        assert_eq!(tracker.last_value(a, U256::from(0)), Some(U256::from(42)));
        tracker.observe(a, U256::from(0), U256::from(99), 1);
        assert_eq!(tracker.last_value(a, U256::from(0)), Some(U256::from(99)));
    }

    // --- T7: probabilistic should_refetch coverage -------------------------

    /// Insert a fully-specified observation so the probabilistic branch can be
    /// tested with an exact `change_rate = change_count / observation_count` and
    /// a known `last_checked`, without replaying an `observe` sequence.
    fn seed_obs(
        tracker: &mut SlotObservationTracker,
        a: Address,
        slot: U256,
        observation_count: u32,
        change_count: u32,
        last_checked: u64,
    ) {
        tracker.observations.insert(
            SlotKey { address: a, slot },
            SlotObservation {
                last_value: U256::from(1),
                observation_count,
                change_count,
                last_checked,
                last_changed: last_checked,
            },
        );
    }

    #[test]
    fn test_probabilistic_refetches_at_now_equals_last_checked() {
        // change_rate = 3/20 = 0.15. At now == last_checked, units_elapsed = 0 so
        // cycles_elapsed clamps to 1.0; expected = 0.15 > 0.05 → refetch.
        let mut tracker = SlotObservationTracker::new();
        let p = params();
        let a = addr(1);
        let slot = U256::from(7);
        seed_obs(&mut tracker, a, slot, 20, 3, 100);
        // Sanity: this is the probabilistic branch (between never and always).
        assert!((3.0_f64 / 20.0) < p.always_refetch_rate);
        assert!(tracker.should_refetch(a, slot, 100, &p));
    }

    #[test]
    fn test_probabilistic_reuses_then_refetches_after_elapsed() {
        // change_rate = 1/100 = 0.01. At now == last_checked, expected = 0.01 <
        // 0.05 → reuse. After 10 cycles elapsed (cycle_interval = 1), expected =
        // 0.01 * 10 = 0.10 > 0.05 → refetch. Stays within max_reuse (300).
        let mut tracker = SlotObservationTracker::new();
        let p = params();
        let a = addr(1);
        let slot = U256::from(7);
        seed_obs(&mut tracker, a, slot, 100, 1, 100);

        // Immediately: reused.
        assert!(!tracker.should_refetch(a, slot, 100, &p));
        // After a few units: still under threshold (0.01 * 4 = 0.04 < 0.05).
        assert!(!tracker.should_refetch(a, slot, 104, &p));
        // After enough units: over threshold (0.01 * 10 = 0.10 > 0.05).
        assert!(tracker.should_refetch(a, slot, 110, &p));
    }

    #[test]
    fn test_probabilistic_cycle_interval_scaling() {
        // change_rate = 1/100 = 0.01, cycle_interval = 10. cycles_elapsed =
        // units_elapsed / 10, so it takes 10x more elapsed units than a unit
        // cycle to cross the 0.05 threshold.
        let mut tracker = SlotObservationTracker::new();
        let p = FreshnessParams {
            cycle_interval: 10,
            ..FreshnessParams::default()
        };
        let a = addr(1);
        let slot = U256::from(7);
        seed_obs(&mut tracker, a, slot, 100, 1, 100);

        // 60 units elapsed → 6 cycles → expected = 0.06 > 0.05 → refetch.
        assert!(tracker.should_refetch(a, slot, 160, &p));
        // 40 units elapsed → 4 cycles → expected = 0.04 < 0.05 → reuse. (Under a
        // unit cycle_interval this same 40-unit gap would be 40 cycles and would
        // refetch — proving the cycle_interval scaling is applied.)
        assert!(!tracker.should_refetch(a, slot, 140, &p));
        let unit = FreshnessParams::default();
        assert!(
            tracker.should_refetch(a, slot, 140, &unit),
            "with cycle_interval = 1 the same elapsed gap refetches"
        );
    }
}
