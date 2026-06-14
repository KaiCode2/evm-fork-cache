//! Freshness control plane and the optimistic verify-and-rerun execution loop.
//!
//! This module is the generic core of the engine's "honest freshness" model.
//! The four-layer model, the policy traits, and the optimistic freshness
//! controller are built up across the Phase 2 steps; this step introduces the
//! clock-agnostic [`FreshnessParams`] that tune the adaptive
//! [`SlotObservationTracker`](crate::cache::SlotObservationTracker).

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
