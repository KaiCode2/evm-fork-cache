//! What a cold-start round declares it wants done.
//!
//! A [`ColdStartPlan`] is a pure, IO-free description handed to the driver. All
//! phases are optional and an empty plan is a valid no-op round.

use alloy_primitives::{Address, Bytes, U256};

/// A single round of cold-start work, declared by a
/// [`ColdStartPlanner`](crate::cold_start::ColdStartPlanner).
///
/// All phases are optional; an empty plan is a valid no-op round. The driver
/// executes the phases in a fixed order:
/// **accounts → verify → probe → probe_roots → discover**.
///
/// - `verify` slots are authoritatively re-fetched, classified, and (when changed)
///   injected into the cache via the dual-layer
///   [`inject_storage_batch_fresh`](crate::cache::EvmCache::inject_storage_batch_fresh).
/// - `probe` slots are classified at the pinned block **without** injecting.
/// - `probe_roots` accounts have their storage root observed via the
///   account-proof fetcher at the pinned block, without injecting anything.
/// - `accounts` are pre-seeded into the cache before discovery runs.
/// - `discover` view-calls capture the `(address, slot)` pairs and accounts they
///   touch.
///
/// ```
/// use alloy_primitives::{Address, Bytes, U256};
/// use evm_fork_cache::{ColdStartCall, ColdStartPlan};
///
/// // Verify one known slot and discover more via a read-only view-call.
/// let plan = ColdStartPlan {
///     verify: vec![(Address::repeat_byte(0x11), U256::from(0))],
///     discover: vec![ColdStartCall {
///         from: Address::ZERO,
///         to: Address::repeat_byte(0x11),
///         calldata: Bytes::new(),
///         restrict_to: None,
///     }],
///     ..Default::default()
/// };
/// assert_eq!(plan.verify.len(), 1);
/// assert_eq!(plan.discover.len(), 1);
/// ```
#[derive(Clone, Debug, Default)]
pub struct ColdStartPlan {
    /// Slots to authoritatively re-fetch, classify, and inject when changed.
    pub verify: Vec<(Address, U256)>,
    /// Slots to classify at the pinned block without injecting.
    pub probe: Vec<(Address, U256)>,
    /// Accounts whose storage root is probed via the account-proof fetcher at
    /// the pinned block, without injecting anything.
    pub probe_roots: Vec<Address>,
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
