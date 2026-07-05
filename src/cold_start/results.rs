//! What a cold-start round produced.
//!
//! [`ColdStartResults`] carries a per-slot [`SlotOutcome`] for every declared
//! verify and probe slot — distinguishing a genuine on-chain zero from a fetch
//! failure — plus the injected [`SlotChange`]s and any discovered access lists.

use crate::access_set::StorageAccessList;
use crate::cache::CodeVerifyReport;
use crate::cold_start::error::ColdStartError;
use crate::cold_start::roots::RootProbeOutcome;
use crate::freshness::{SlotChange, SlotOutcome};

use revm::context::result::ExecutionResult;

/// The outcome of executing one [`ColdStartPlan`](crate::cold_start::ColdStartPlan)
/// round.
///
/// `fetched` and `probed` each carry exactly one [`SlotOutcome`] per declared
/// verify / probe slot, so a fetch failure surfaces as
/// [`SlotFetch::FetchFailed`](crate::freshness::SlotFetch::FetchFailed) rather
/// than as absence. `verified` carries only the slots whose value actually changed
/// (and were injected). `discovered` carries one [`ColdStartCallResult`] per
/// discover call.
///
/// The order of entries in `fetched` / `probed` is unspecified — look up a slot
/// by `(address, slot)` rather than relying on it matching the plan's order.
#[derive(Clone, Debug, Default)]
pub struct ColdStartResults {
    /// Slots whose value changed and were injected (one per change).
    pub verified: Vec<SlotChange>,
    /// One outcome per declared verify slot (`Value` / `Zero` / `FetchFailed`).
    pub fetched: Vec<SlotOutcome>,
    /// One outcome per declared probe slot (classified, not injected).
    pub probed: Vec<SlotOutcome>,
    /// One outcome per declared probe-roots address (`root: None` when the
    /// probe failed, the fetcher omitted the address, or the phase never ran).
    pub probed_roots: Vec<RootProbeOutcome>,
    /// One result per discover call.
    pub discovered: Vec<ColdStartCallResult>,
    /// The `verify_code` phase's report, when the round found pending code
    /// seeds (`None` means the phase was a no-op). The phase runs **first**,
    /// so this survives a later phase's hard error. Adapters that require
    /// verified code before serving gate on its `unverifiable` bucket.
    pub code_verifications: Option<CodeVerifyReport>,
}

/// The result of one discover view-call: the raw EVM execution result and the
/// storage/account access list it touched (filtered by `restrict_to`).
#[derive(Clone, Debug)]
pub struct ColdStartCallResult {
    /// The raw revm execution result of the view-call.
    pub result: ExecutionResult,
    /// The storage slots and accounts the call touched (after `restrict_to`).
    pub access: StorageAccessList,
}

/// The outcome of a single cold-start round, always carrying the
/// (possibly partial) [`ColdStartResults`].
///
/// `error` is `Some` only when a hard error short-circuited the round (an
/// accounts- or discover-phase failure in later slices). The verify-only path
/// never sets `error`. Carrying the results unconditionally lets the driver
/// absorb partial outcomes into the run report before propagating the error.
pub struct RoundOutcome {
    /// The (possibly partial) results computed before any short-circuit.
    pub results: ColdStartResults,
    /// `Some` when a hard error aborted the round mid-way.
    pub error: Option<ColdStartError>,
}
