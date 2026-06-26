//! Protocol-neutral cold-start sync for [`EvmCache`](crate::cache::EvmCache).
//!
//! Cold start declares rounds of authoritative slot work — verify, probe,
//! accounts, discover — and drives a bounded multi-round loop via a pure
//! [`ColdStartPlanner`]. Every verify and probe slot yields a per-slot
//! [`SlotOutcome`] distinguishing a genuine on-chain zero ([`SlotFetch::Zero`])
//! from a fetch failure ([`SlotFetch::FetchFailed`]) — closing the "archive-miss"
//! gap where a transient fetch failure was indistinguishable from absence.
//!
//! The module is gated behind the `reactive` feature. The per-slot
//! [`SlotOutcome`] / [`SlotFetch`] surface lives ungated in
//! [`crate::freshness`] and is re-exported here for consumer ergonomics.
//!
//! # Design
//!
//! - The driver performs every fetch and call; planners are pure and IO-free,
//!   handed only a [`StateView`](crate::events::StateView) and a
//!   [`ColdStartResults`].
//! - Verify-phase changes are injected via the dual-layer
//!   [`inject_storage_batch_fresh`](crate::cache::EvmCache::inject_storage_batch_fresh)
//!   and are visible to the next `on_results` through the state view.
//! - A run can be pinned to a block hash via [`ColdStartPin::Hash`]; with
//!   `require_canonical: true`, a reorged hash makes the run fail fast. This is
//!   the cold-start reorg defense by design.
//!
//! # Runtime requirement
//!
//! Like the rest of the crate's RPC seams, cold-start fetching drives async work
//! synchronously and must run on a **multi-thread** tokio runtime.

mod config;
mod driver;
mod error;
mod plan;
mod planner;
mod results;

pub use config::{ColdStartConfig, ColdStartPin, ColdStartRoundSummary, ColdStartRunReport};
pub use error::ColdStartError;
pub use plan::{ColdStartCall, ColdStartPlan};
pub use planner::{ColdStartPlanner, ColdStartStep};
pub use results::{ColdStartCallResult, ColdStartResults, RoundOutcome};

// The per-slot fetch surface is ungated in `freshness`; re-export so
// `evm_fork_cache::cold_start::SlotFetch` resolves for consumers.
pub use crate::freshness::{SlotFetch, SlotOutcome};
