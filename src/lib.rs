//! Forked EVM state cache and simulation utilities for DeFi search.
//!
//! `evm-fork-cache` is a support layer for simulating EVM transactions against
//! recent on-chain state without re-deriving it on every call. It builds on
//! `revm` and `foundry-fork-db` to provide a lazy-loading state cache,
//! immutable snapshots that can be shared across threads, per-simulation
//! overlays, and a set of helpers for the kinds of state manipulation a search
//! loop needs (overriding ERC20 balances by scanning for the balance slot,
//! batched `eth_call` multicalls, Foundry-style bytecode etching, and CREATE3
//! address derivation).
//!
//! The entry point is [`cache::EvmCache`]: construct one over an RPC backend,
//! then snapshot it with [`cache::EvmCache::create_snapshot`] to fan out
//! parallel simulations, each driving its own [`cache::EvmOverlay`].
//!
//! Other modules:
//! - [`access_list`] / [`access_set`] — EIP-2930 access-list construction and
//!   warm-slot tracking for gas estimation.
//! - [`errors`] — structured simulation errors and an extensible revert-reason
//!   decoder you can teach your own custom Solidity error selectors.
//! - [`freshness`] — the four-layer freshness model (classification, observation,
//!   policy, mechanism) and the optimistic verify-and-rerun execution loop with
//!   deferred validation.
//! - [`inspector`] — an `Inspector` that captures ERC20 `Transfer` events to
//!   reconstruct balance deltas from a simulation.
//! - [`multicall`] — batched read-only calls.
//! - [`deploy`] / [`create3`] — contract deployment and CREATE3 address math.
//! - [`prefetch_registry`] — two-stage storage-slot pre-warming.
//!
//! The `examples/` directory has runnable, documented walkthroughs of each of
//! these — offline ones that need no network, plus a few that fork real chain
//! state over RPC. See the crate README for the full list.

pub mod access_list;
pub mod access_set;
pub mod cache;
pub mod create3;
pub mod deploy;
pub mod errors;
pub mod freshness;
pub mod inspector;
pub mod multicall;
pub mod prefetch_registry;

pub use access_set::StorageAccessList;
pub use freshness::{
    AlwaysVerify, BlockClock, FreshnessClock, FreshnessController, FreshnessParams,
    FreshnessPolicy, FreshnessRegistry, NeverVerify, ObservationDriven, SimRequest, SlotChange,
    SpeculativeSim, Validation, Validity, WallClock,
};
