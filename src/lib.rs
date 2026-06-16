//! Forked EVM **simulation engine** for DeFi search, MEV, and backtesting.
//!
//! `evm-fork-cache` simulates EVM transactions against recent on-chain state
//! without re-deriving that state on every call. It builds on [`revm`],
//! [`alloy`], and [`foundry-fork-db`] to provide a lazy-loading state cache,
//! immutable snapshots shareable across threads, per-simulation overlays, a
//! freshness control plane, and the state-manipulation helpers a search loop
//! needs (balance overrides, batched multicalls, Foundry-style bytecode etching,
//! CREATE3 address derivation, and an extensible revert decoder).
//!
//! [`revm`]: https://github.com/bluealloy/revm
//! [`alloy`]: https://github.com/alloy-rs/alloy
//! [`foundry-fork-db`]: https://github.com/foundry-rs/foundry-fork-db
//!
//! # The state stack
//!
//! Reads flow up; the fork DB lazily fetches misses from RPC. Writes and purges
//! are applied directly to the cache (no RPC on the hot path).
//!
//! ```text
//! EvmOverlay × N      isolated, Send simulations (cheap Arc clones)
//!      ▲ clone × N
//! EvmSnapshot         immutable, point-in-time, Send + Sync
//!      ▲ create_snapshot()
//! EvmCache            lazy RPC fetch + local state cache + targeted writes/purge
//!      ▲ lazy fetch
//! RPC provider
//! ```
//!
//! The entry point is [`cache::EvmCache`]: construct one over an RPC backend
//! (see [`cache::EvmCacheBuilder`]), then snapshot it with
//! [`cache::EvmCache::create_snapshot`] to fan out parallel simulations, each
//! driving its own [`cache::EvmOverlay`]. `EvmCache` is `!Send` (it owns the
//! mutable fork and blocks on RPC internally); `EvmSnapshot` is `Send + Sync`
//! and `EvmOverlay` is `Send`, so the fan-out parallelizes safely.
//!
//! # Modules
//!
//! - [`cache`] — the fork cache, snapshots, overlays, and on-disk persistence.
//! - [`access_list`] / [`access_set`] — EIP-2930 access-list construction and
//!   EIP-2929 warm-slot tracking for gas estimation.
//! - [`errors`] — structured simulation errors ([`errors::SimError`]) and an
//!   extensible revert-reason decoder you can teach your own custom Solidity
//!   error selectors.
//! - [`freshness`] — the four-layer freshness model (classification, observation,
//!   policy, mechanism) and the optimistic verify-and-rerun execution loop with
//!   deferred validation.
//! - [`state_update`] — the generic state-mutation vocabulary (`StateUpdate` /
//!   `AccountPatch` / `PurgeScope`, plus relative `SlotDelta` read-modify-write)
//!   applied by `EvmCache::apply_update` / `apply_updates` / `modify_slot`, with a
//!   structured `StateDiff` output (Pillar B.1).
//! - [`inspector`] — an [`Inspector`](revm::Inspector) that captures ERC20
//!   `Transfer` events to reconstruct balance deltas from a simulation.
//! - [`multicall`] — batched read-only calls through Multicall3.
//! - [`deploy`] / [`create3`] — contract deployment and CREATE3 address math.
//! - [`prefetch_registry`] — two-stage storage-slot pre-warming.
//!
//! # Requirements
//!
//! Any constructor or method that may touch RPC fetches missing state through a
//! synchronous façade over an async provider
//! ([`tokio::task::block_in_place`]), so it must run on a **multi-thread** tokio
//! runtime:
//!
//! ```ignore
//! #[tokio::main(flavor = "multi_thread")]
//! async fn main() { /* ... */ }
//!
//! #[tokio::test(flavor = "multi_thread")]
//! async fn my_test() { /* ... */ }
//! ```
//!
//! Running on a current-thread runtime panics when a fetch is attempted. The
//! offline examples and integration tests build the cache over a mocked provider
//! and never reach the network, so they are exempt.
//!
//! # Error handling
//!
//! Simulation entry points that distinguish failure modes return
//! [`errors::SimulationResult`] (`Result<T, SimError>`), where
//! [`SimError`](errors::SimError) separates a decoded [`Revert`](errors::SimError::Revert),
//! an EVM [`Halt`](errors::SimError::Halt), and an unexpected host-side
//! [`Other`](errors::SimError::Other) error (RPC, database, ABI encoding). The
//! freshness loop never silently trusts stale data: a transient RPC failure
//! surfaces as [`freshness::Validation::Unverified`] so callers can retry rather
//! than act on unverified results.
//!
//! # Maturity & stability
//!
//! This crate is **pre-1.0** and developed against a phased roadmap (see
//! `docs/ROADMAP.md`). Until 1.0, breaking changes may land in minor releases;
//! each is recorded in the crate `CHANGELOG.md`. MSRV is Rust 1.88 (edition 2024).
//!
//! The `examples/` directory has runnable, documented walkthroughs of each
//! module — offline ones that need no network, plus a few that fork real chain
//! state over RPC. See the crate README for the full list.
#![cfg_attr(docsrs, feature(doc_cfg))]

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
pub mod state_update;

pub use access_set::StorageAccessList;
pub use freshness::{
    AlwaysVerify, BlockClock, FreshnessClock, FreshnessController, FreshnessParams,
    FreshnessPolicy, FreshnessRegistry, NeverVerify, ObservationDriven, SimRequest, SlotChange,
    SpeculativeSim, Validation, Validity, WallClock,
};
pub use state_update::{
    AccountChange, AccountPatch, PurgeRecord, PurgeScope, SkippedBalanceDelta, SkippedDelta,
    SkippedMask, SlotDelta, StateDiff, StateUpdate,
};
