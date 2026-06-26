//! What a cold-start round declares it wants done.
//!
//! A [`ColdStartPlan`] is a pure, IO-free description handed to the driver. All
//! four phases are optional and an empty plan is a valid no-op round.

use alloy_primitives::{Address, Bytes, U256};

/// A single round of cold-start work, declared by a
/// [`ColdStartPlanner`](crate::cold_start::ColdStartPlanner).
///
/// All four phases are optional; an empty plan is a valid no-op round. The driver
/// executes the phases in a fixed order: **accounts → verify → probe → discover**.
///
/// - `verify` slots are authoritatively re-fetched, classified, and (when changed)
///   injected into the cache via the dual-layer
///   [`inject_storage_batch_fresh`](crate::cache::EvmCache::inject_storage_batch_fresh).
/// - `probe` slots are classified at the pinned block **without** injecting.
/// - `accounts` are pre-seeded into the cache before discovery runs.
/// - `discover` view-calls capture the `(address, slot)` pairs and accounts they
///   touch.
#[derive(Clone, Debug, Default)]
pub struct ColdStartPlan {
    /// Slots to authoritatively re-fetch, classify, and inject when changed.
    pub verify: Vec<(Address, U256)>,
    /// Slots to classify at the pinned block without injecting.
    pub probe: Vec<(Address, U256)>,
    /// Accounts to pre-seed into the cache before discovery.
    pub accounts: Vec<Address>,
    /// View-calls whose touched slots and accounts are captured.
    pub discover: Vec<ColdStartCall>,
}

/// A read-only view-call whose touched storage and accounts are captured during
/// the discover phase.
///
/// When `restrict_to` is `Some`, the captured `slots` and `accounts` are filtered
/// to the named addresses; an empty restricted capture is observable as an empty
/// access list (distinct from a non-empty discovery).
#[derive(Clone, Debug)]
pub struct ColdStartCall {
    /// Transaction sender.
    pub from: Address,
    /// Call target.
    pub to: Address,
    /// Calldata.
    pub calldata: Bytes,
    /// When set, filters captured slots and accounts to these addresses.
    pub restrict_to: Option<Vec<Address>>,
}
