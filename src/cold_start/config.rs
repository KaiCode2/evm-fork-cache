//! Cold-start run configuration and the accumulated run report.

use alloy_primitives::B256;

use crate::cold_start::plan::ColdStartPlan;
use crate::cold_start::results::ColdStartResults;
use crate::freshness::SlotFetch;

/// Configuration for a cold-start run.
///
/// `Default` is hand-written (`max_rounds: 8`, `pin: ColdStartPin::CachePinned`)
/// rather than derived, because a derived `max_rounds: 0` would trip
/// [`RoundBudgetExceeded`](crate::cold_start::ColdStartError::RoundBudgetExceeded)
/// before any round ran. `max_rounds >= 1` is a precondition.
#[derive(Clone, Debug)]
pub struct ColdStartConfig {
    /// Hard upper bound on **executed** rounds. A planner that returns `Continue`
    /// past this bound yields
    /// [`RoundBudgetExceeded`](crate::cold_start::ColdStartError::RoundBudgetExceeded).
    /// Must be `>= 1`.
    pub max_rounds: usize,
    /// Block-pinning policy for the run's reads.
    pub pin: ColdStartPin,
}

impl Default for ColdStartConfig {
    fn default() -> Self {
        Self {
            max_rounds: 8,
            pin: ColdStartPin::CachePinned,
        }
    }
}

/// How the driver pins the block for the run's reads.
#[derive(Clone, Debug)]
pub enum ColdStartPin {
    /// Use the cache's currently-pinned block (`self.block`) for every round.
    CachePinned,
    /// Pin every round to an explicit hash for the whole run (reorg-stable cold
    /// start), restoring the prior block on completion. With
    /// `require_canonical: true`, the provider rejects a reorged hash so the run
    /// fails fast.
    Hash {
        /// Block number associated with the hash (advisory).
        number: u64,
        /// Block hash to pin all reads to.
        hash: B256,
        /// Whether the provider must reject a non-canonical hash.
        require_canonical: bool,
    },
}

/// Summary of a completed cold-start run, returned by
/// [`run_cold_start`](crate::cache::EvmCache::run_cold_start) on success.
///
/// Accumulated round-by-round as the run progresses. Note `run_cold_start`
/// returns this report **only on the `Ok` path**: on a hard error the partial
/// round is folded in before the error is returned, but the report itself is
/// then dropped (the `Err` carries only the cause). To observe partial progress
/// on failure, drive rounds yourself via
/// [`execute_cold_start_round`](crate::cache::EvmCache::execute_cold_start_round),
/// which always returns a [`RoundOutcome`](crate::cold_start::RoundOutcome).
#[derive(Clone, Debug, Default)]
pub struct ColdStartRunReport {
    /// Number of rounds executed.
    pub rounds: usize,
    /// Total verify slots requested across all rounds.
    pub verified_slots: usize,
    /// Total slots that changed and were injected.
    pub changed_slots: usize,
    /// Total accounts touched by discover calls, summed across calls and rounds
    /// (not de-duplicated — the same account touched twice counts twice).
    pub discovered_accounts: usize,
    /// Total slots touched by discover calls, summed across calls and rounds
    /// (not de-duplicated — the same slot touched twice counts twice).
    pub discovered_slots: usize,
    /// Total verify + probe slots whose fetch failed.
    pub failed_slots: usize,
    /// One summary per round, in execution order.
    pub per_round: Vec<ColdStartRoundSummary>,
}

/// Per-round breakdown recorded in [`ColdStartRunReport::per_round`].
#[derive(Clone, Debug, Default)]
pub struct ColdStartRoundSummary {
    /// Verify slots requested this round.
    pub verify_requested: usize,
    /// Verify slots that changed and were injected.
    pub verify_changed: usize,
    /// Verify slots whose fetch failed.
    pub verify_failed: usize,
    /// Probe slots requested this round.
    pub probe_requested: usize,
    /// Probe slots whose fetch failed.
    pub probe_failed: usize,
    /// Discover calls issued this round.
    pub discover_calls: usize,
    /// Slots touched by this round's discover calls.
    pub discovered_slots: usize,
}

impl ColdStartRunReport {
    /// Fold one round's plan and results into the report.
    ///
    /// Plain field accumulation, no IO. `failed_slots` counts
    /// [`FetchFailed`](crate::freshness::SlotFetch::FetchFailed) outcomes across
    /// both the verify (`fetched`) and probe (`probed`) phases.
    pub(crate) fn absorb_round(&mut self, plan: &ColdStartPlan, results: &ColdStartResults) {
        self.rounds += 1;
        let verify_failed = results
            .fetched
            .iter()
            .filter(|o| matches!(o.fetch, SlotFetch::FetchFailed { .. }))
            .count();
        let probe_failed = results
            .probed
            .iter()
            .filter(|o| matches!(o.fetch, SlotFetch::FetchFailed { .. }))
            .count();
        let discovered_slots: usize = results
            .discovered
            .iter()
            .map(|d| d.access.slots.len())
            .sum();
        let discovered_accounts: usize = results
            .discovered
            .iter()
            .map(|d| d.access.accounts.len())
            .sum();

        self.verified_slots += plan.verify.len();
        self.changed_slots += results.verified.len();
        self.failed_slots += verify_failed + probe_failed;
        self.discovered_slots += discovered_slots;
        self.discovered_accounts += discovered_accounts;

        self.per_round.push(ColdStartRoundSummary {
            verify_requested: plan.verify.len(),
            verify_changed: results.verified.len(),
            verify_failed,
            probe_requested: plan.probe.len(),
            probe_failed,
            discover_calls: plan.discover.len(),
            discovered_slots,
        });
    }
}
