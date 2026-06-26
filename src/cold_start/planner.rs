//! The pure planner that drives a bounded multi-round cold start.
//!
//! A [`ColdStartPlanner`] performs no IO: the driver performs every fetch and
//! call and hands the planner only a [`StateView`] and the round's
//! [`ColdStartResults`]. The planner decides the next plan (or stops).

use crate::cold_start::plan::ColdStartPlan;
use crate::cold_start::results::ColdStartResults;
use crate::events::StateView;

/// The planner's decision after a round completes.
#[derive(Clone, Debug)]
pub enum ColdStartStep {
    /// Stop the cold-start loop; the run succeeds.
    Done,
    /// Execute the carried plan as the next round.
    Continue(ColdStartPlan),
}

/// Drives a bounded multi-round cold start.
///
/// The driver calls [`initial_plan`](Self::initial_plan) once, executes it, then
/// repeatedly calls [`on_results`](Self::on_results) with the round's results and
/// the post-injection [`StateView`]; a returned [`ColdStartStep::Continue`] is run
/// as the next round and [`ColdStartStep::Done`] ends the run. The loop is bounded
/// by [`ColdStartConfig::max_rounds`](crate::cold_start::ColdStartConfig::max_rounds).
///
/// Implementations perform **no IO** — the trait hands them only borrowed,
/// read-only state, so all fetching is the driver's responsibility.
pub trait ColdStartPlanner {
    /// The first plan to execute, derived from the current cached state.
    fn initial_plan(&mut self, state: &dyn StateView) -> ColdStartPlan;
    /// Decide whether to continue (with a next plan) or finish, given the just-
    /// completed round's results and the post-injection state view.
    fn on_results(&mut self, results: &ColdStartResults, state: &dyn StateView) -> ColdStartStep;
}
