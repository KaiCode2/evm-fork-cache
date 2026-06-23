//! The cold-start driver: inherent [`EvmCache`] methods that execute rounds and
//! run the bounded multi-round loop.
//!
//! The driver performs every fetch and call; planners are pure and IO-free. The
//! whole module is gated behind the `reactive` feature.

use std::collections::HashSet;

use alloy_eips::BlockId;
use alloy_primitives::Address;
use anyhow::Result;

use crate::cache::{EvmCache, block_in_place_handle};
use crate::cold_start::config::{ColdStartConfig, ColdStartPin, ColdStartRunReport};
use crate::cold_start::error::ColdStartError;
use crate::cold_start::plan::ColdStartPlan;
use crate::cold_start::planner::{ColdStartPlanner, ColdStartStep};
use crate::cold_start::results::{ColdStartCallResult, ColdStartResults, RoundOutcome};
use crate::events::StateView;
use crate::freshness::{SlotFetch, SlotOutcome};

impl EvmCache {
    /// View `self` as a [`StateView`] for handing to a planner.
    ///
    /// The returned borrow must be **inlined** into each planner call rather than
    /// held across [`execute_cold_start_round`](Self::execute_cold_start_round),
    /// which needs `&mut self` (holding the shared borrow across it is a borrowck
    /// error).
    fn state_view(&self) -> &dyn StateView {
        self
    }

    /// Pre-seed an account synchronously, bridging [`ensure_account`] across the
    /// async boundary.
    ///
    /// [`ensure_account`](EvmCache::ensure_account) early-returns for an
    /// already-present account; for a missing one it issues a backend fetch that
    /// can return `Err`. This is the only producer of a cold-start round hard error
    /// in the accounts phase. Requires a **multi-thread** tokio runtime (the
    /// cold-start runtime precondition); on a current-thread runtime or with no
    /// runtime present it returns a typed error via
    /// [`block_in_place_handle`](crate::cache::block_in_place_handle).
    pub(crate) fn ensure_account_blocking(&mut self, address: Address) -> Result<()> {
        let handle = block_in_place_handle()?;
        tokio::task::block_in_place(|| handle.block_on(self.ensure_account(address)))
    }

    /// Execute a single cold-start round and return its (possibly partial) outcome.
    ///
    /// Fixed phase order: **accounts → verify → probe → discover**.
    ///
    /// Per-round fetcher guard: if the plan declares any verify or probe slots and
    /// the cache has no storage batch fetcher, the round short-circuits with
    /// [`ColdStartError::NoBatchFetcher`] before issuing any read. A round
    /// declaring only accounts/discover runs without a fetcher.
    ///
    /// - **accounts (first):** each `plan.accounts` address is pre-seeded via
    ///   `ensure_account_blocking`. A failure here is a hard error in the first
    ///   phase, so nothing after it ran: every declared verify/probe slot is marked
    ///   [`SlotFetch::NotAttempted`] and the round returns with `error: Some(..)`.
    ///   This is the only producer of `NotAttempted`.
    /// - **verify:** each verify slot is re-fetched, classified into
    ///   `results.fetched`, and (when changed) injected and recorded in
    ///   `results.verified`.
    /// - **probe:** each probe slot is re-fetched at the pinned block and
    ///   classified into `results.probed` via the same shared `Result<U256>`
    ///   classification verify uses. Unlike verify, a probe injects nothing and
    ///   records no [`SlotChange`](crate::freshness::SlotChange): it is the
    ///   archive-miss classification for slots a consumer does not want to warm.
    /// - **discover (last):** each [`ColdStartCall`](crate::cold_start::ColdStartCall)
    ///   is executed via
    ///   [`call_raw_with_access_list`](EvmCache::call_raw_with_access_list), its
    ///   access list filtered by `restrict_to`, and the result pushed to
    ///   `results.discovered`. A discover failure preserves the verify/probe
    ///   outcomes already computed this round (they ran earlier, so they are *not*
    ///   `NotAttempted`); the failing call and all subsequent discover calls are
    ///   dropped, and the round returns with `error: Some(..)`.
    pub fn execute_cold_start_round(&mut self, plan: &ColdStartPlan) -> RoundOutcome {
        let mut results = ColdStartResults::default();

        // Per-round fetcher guard: only fires for verify/probe-bearing rounds.
        if (!plan.verify.is_empty() || !plan.probe.is_empty())
            && self.storage_batch_fetcher().is_none()
        {
            return RoundOutcome {
                results,
                error: Some(ColdStartError::NoBatchFetcher),
            };
        }

        // Accounts phase (first): pre-seed each declared account. A failure here
        // short-circuits the round before verify/probe/discover run, so every
        // declared verify/probe slot is synthesized as NotAttempted.
        for &address in &plan.accounts {
            if let Err(e) = self.ensure_account_blocking(address) {
                results.fetched = not_attempted_outcomes(&plan.verify);
                results.probed = not_attempted_outcomes(&plan.probe);
                return RoundOutcome {
                    results,
                    error: Some(ColdStartError::Fetch(e)),
                };
            }
        }

        // Verify phase: classify every slot, inject and record the changed ones.
        if !plan.verify.is_empty() {
            match self.verify_slots_with_outcomes(&plan.verify) {
                Ok((changed, outcomes)) => {
                    results.verified = changed;
                    results.fetched = outcomes;
                }
                Err(e) => {
                    // The only error surface of verify_slots_with_outcomes is the
                    // missing-fetcher guard, already front-run above; surface any
                    // residual error explicitly rather than panicking.
                    return RoundOutcome {
                        results,
                        error: Some(ColdStartError::Fetch(e)),
                    };
                }
            }
        }

        // Probe phase: classify each declared slot at the pinned block WITHOUT
        // injecting (the archive-miss classification for slots a consumer does
        // not want to warm). It records into `results.probed` only — never
        // `results.verified`, and never writes the cache.
        if !plan.probe.is_empty() {
            // The per-round guard already ensured a fetcher is present for a
            // probe-bearing round. Read pinned to self.block (NOT read_storage_slot,
            // which is unpinned), classify, and inject NOTHING.
            let fetcher = self
                .storage_batch_fetcher()
                .cloned()
                .expect("probe-bearing round guarded a fetcher above");
            let probed = (fetcher)(plan.probe.clone(), Some(self.block()));
            results.probed = probed
                .into_iter()
                .map(|(address, slot, fetched)| SlotOutcome {
                    address,
                    slot,
                    fetch: EvmCache::classify(fetched),
                })
                .collect();
        }

        // Discover phase (last): run each view-call, filter by restrict_to. A
        // failure drops this and every subsequent call but preserves the
        // verify/probe outcomes already computed above.
        for call in &plan.discover {
            match self.call_raw_with_access_list(call.from, call.to, call.calldata.clone()) {
                Ok((result, mut access)) => {
                    if let Some(list) = &call.restrict_to {
                        let keep: HashSet<Address> = list.iter().copied().collect();
                        access.slots.retain(|(a, _)| keep.contains(a));
                        access.accounts.retain(|a| keep.contains(a));
                    }
                    results
                        .discovered
                        .push(ColdStartCallResult { result, access });
                }
                Err(e) => {
                    return RoundOutcome {
                        results,
                        error: Some(ColdStartError::Fetch(e)),
                    };
                }
            }
        }

        RoundOutcome {
            results,
            error: None,
        }
    }

    /// Run a bounded multi-round cold start driven by `planner`.
    ///
    /// Pin handling per `config.pin`: [`ColdStartPin::CachePinned`] is a no-op;
    /// [`ColdStartPin::Hash`] pins every round to
    /// `BlockId::from((hash, Some(require_canonical)))`, capturing the prior block
    /// and restoring it on **every** exit path (success, budget-exceeded, and
    /// mid-round error).
    ///
    /// The loop checks the round budget at the top: with `max_rounds = N`, rounds
    /// `0..N` execute and a planner still returning `Continue` after round `N`
    /// yields [`RoundBudgetExceeded`](ColdStartError::RoundBudgetExceeded). Each
    /// round's results are absorbed into the report **before** its `error` is
    /// checked; a round error propagates after restoring the pin and **without**
    /// calling `on_results`.
    pub fn run_cold_start(
        &mut self,
        planner: &mut dyn ColdStartPlanner,
        config: ColdStartConfig,
    ) -> Result<ColdStartRunReport, ColdStartError> {
        // Pin handling: capture the block to restore (None == no restore needed).
        let restore: Option<BlockId> = match config.pin {
            ColdStartPin::CachePinned => None,
            ColdStartPin::Hash {
                hash,
                require_canonical,
                ..
            } => {
                let prev = self.block();
                self.set_block(BlockId::from((hash, Some(require_canonical))));
                Some(prev)
            }
        };

        let mut report = ColdStartRunReport::default();
        // Borrow inlined, not hoisted across the &mut self round call.
        let mut plan = planner.initial_plan(self.state_view());

        loop {
            if report.rounds >= config.max_rounds {
                if let Some(prev) = restore {
                    self.set_block(prev);
                }
                return Err(ColdStartError::RoundBudgetExceeded {
                    max_rounds: config.max_rounds,
                });
            }

            let outcome = self.execute_cold_start_round(&plan);
            report.absorb_round(&plan, &outcome.results);

            if let Some(err) = outcome.error {
                if let Some(prev) = restore {
                    self.set_block(prev);
                }
                return Err(err);
            }

            match planner.on_results(&outcome.results, self.state_view()) {
                ColdStartStep::Done => break,
                ColdStartStep::Continue(next) => plan = next,
            }
        }

        if let Some(prev) = restore {
            self.set_block(prev);
        }
        Ok(report)
    }
}

/// Synthesize one [`SlotFetch::NotAttempted`] outcome per declared slot.
///
/// Used when an accounts-phase hard error short-circuits a round before the
/// verify/probe phases run: every declared slot is reported as `NotAttempted`
/// rather than silently dropped.
fn not_attempted_outcomes(slots: &[(Address, alloy_primitives::U256)]) -> Vec<SlotOutcome> {
    slots
        .iter()
        .map(|&(address, slot)| SlotOutcome {
            address,
            slot,
            fetch: SlotFetch::NotAttempted,
        })
        .collect()
}
