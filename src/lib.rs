//! Forked EVM **simulation engine** for EVM search, MEV, and backtesting.
//!
//! `evm-fork-cache` simulates EVM transactions against recent on-chain state
//! without re-deriving that state on every call. It builds on [`revm`],
//! [`alloy`], and [`foundry-fork-db`] to provide a lazy-loading state cache,
//! immutable snapshots shareable across threads, per-simulation overlays, a
//! freshness control plane, and the state-manipulation helpers a search loop
//! needs (balance overrides, batched multicalls, verified code seeding +
//! Foundry-style bytecode etching, CREATE3 address derivation, and an
//! extensible revert decoder).
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
//!      ▲ snapshot()
//! EvmCache            lazy RPC fetch + local state cache + targeted writes/purge
//!      ▲ lazy fetch
//! RPC provider
//! ```
//!
//! The entry point is [`cache::EvmCache`]: construct one over an RPC backend
//! (see [`cache::EvmCacheBuilder`]), then snapshot it with
//! [`cache::EvmCache::snapshot`] to fan out parallel simulations, each
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
//!   `AccountPatch` / `PurgeScope`, plus relative `SlotDelta` read-modify-write and
//!   masked `SlotMasked` writes) applied by `EvmCache::apply_update` /
//!   `apply_updates` / `modify_slot`, with a structured `StateDiff` output
//!   (Pillar B.1).
//! - [`events`] — the event → state pipeline (Pillar B.2): `EventDecoder` /
//!   `StateView` / `DecoderRegistry` decode an on-chain `Log` into `StateUpdate`s,
//!   and `EventPipeline` ingests, reorg-purges, and reconciles a block's logs.
//!   Ships an ERC-20 `Transfer` decoder plus traits for external decoders.
//! - `reactive` — default-enabled provider-neutral handler runtime for logs,
//!   blocks, and pending transaction signals. Pure handlers emit `StateUpdate`s,
//!   invalidations, resync requests, speculative signals, and hooks; the runtime
//!   validates and applies canonical cache mutations, journals canonical block
//!   effects for depth-bounded reorg recovery, and includes a live
//!   `AlloySubscriber` (WebSocket) transport.
//! - `cold_start` — default-enabled (reactive-gated) declarative warming of a
//!   working set of accounts/slots into the cache in one batched pass
//!   (`EvmCache::run_cold_start` + `ColdStartPlanner`), returning a structured
//!   `ColdStartRunReport`. Each round verifies any pending code seeds first
//!   (the `verify_code` phase), so sims never run over unverified claims.
//! - [`bundle`] — multi-transaction bundle execution over cumulative block state
//!   ([`EvmOverlay::simulate_bundle`](cache::EvmOverlay::simulate_bundle)): ordered
//!   txs, an `Atomic`/`AllowReverts` revert policy, and coinbase/miner-payment
//!   accounting.
//! - [`inspector`] — an [`Inspector`](revm::Inspector) that captures ERC20
//!   `Transfer` events to reconstruct balance deltas from a simulation.
//! - [`tracing`] — a call-frame [`Inspector`](revm::Inspector)
//!   ([`CallTracer`] building a [`CallTrace`] tree) plus [`InspectorStack`] for
//!   composing several inspectors over one pass, driven through
//!   [`EvmOverlay::call_raw_with_inspector`](cache::EvmOverlay::call_raw_with_inspector).
//! - [`bulk_storage`] — bulk storage extraction over `eth_call` state
//!   overrides (the **default** batch storage fetcher): thousands of slots —
//!   across many contracts — per call, plus custom storage programs and
//!   account-fields/block-context extractors. See
//!   `docs/bulk-storage-extraction.md` for measured economics.
//! - [`multicall`] — batched read-only calls through Multicall3.
//! - [`deploy`] / [`create3`] — contract deployment and CREATE3 address math.
//! - [`prefetch_registry`] — two-stage storage-slot pre-warming
//!   (complemented by the declarative [`cache::EvmCache::prewarm_slots`]).
//! - [`mapping_probe`] — trace-based discovery of hash-derived storage slots:
//!   derive a mapping's base slot and byte-order layout (Solidity / Vyper /
//!   Solady / nested) from one simulation, via `EvmCache::trace_hashed_slots` /
//!   `discover_erc20_balance_slot` / `track_erc20_balances`.
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
//! On a current-thread runtime, fetch paths do **not** panic: the synchronous
//! bridge checks the runtime flavor up front and returns a typed
//! [`RuntimeError`] (`CurrentThreadRuntime`) instead, so a
//! misconfigured runtime surfaces as a handled error rather than a panic deep in
//! a callback. The offline examples and integration tests build the cache over a
//! mocked provider and never reach the network, so they are exempt.
//!
//! # Error handling
//!
//! Simulation entry points that distinguish failure modes return
//! [`errors::SimulationResult`] (`Result<T, SimError>`), where
//! [`SimError`] separates a decoded [`Revert`](errors::SimError::Revert),
//! an EVM [`Halt`](errors::SimError::Halt), and an unexpected host-side
//! [`Other`](errors::SimError::Other) [`SimHostError`]
//! (RPC, database, ABI encoding). Other fallible modules expose domain errors
//! such as [`CacheError`], [`FreshnessError`], and [`StorageFetchError`] rather
//! than erasing failures
//! into a dynamic catch-all error. The
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
#![warn(missing_docs)]

pub mod access_list;
pub mod access_set;
pub mod bulk_storage;
pub mod bundle;
pub mod cache;
#[cfg(feature = "reactive")]
pub mod cold_start;
pub mod create3;
pub mod deploy;
pub mod errors;
pub mod events;
pub mod freshness;
pub mod inspector;
pub mod mapping_probe;
pub mod multicall;
pub mod prefetch_registry;
#[cfg(feature = "reactive")]
pub mod reactive;
pub mod state_update;
pub mod tracing;

pub use access_set::StorageAccessList;
// Bulk storage extraction over eth_call state overrides — the default batch
// storage fetcher since 0.2.0 (see docs/bulk-storage-extraction.md).
pub use bulk_storage::{
    AccountFieldsSample, BlockContextSample, BulkCallConfig, BulkFetcherStatus, CallDispatch,
    StorageProgram, bulk_call_storage_fetcher, bulk_call_storage_fetcher_with_fallback,
    bulk_call_storage_fetcher_with_status, fetch_account_fields_bulk, fetch_block_context,
    fetch_slots_bulk, planned_call_count, run_storage_program, run_storage_programs,
};
// Phase 6 Track A+B: bundle simulation + coinbase accounting public vocabulary.
pub use bundle::{BundleOptions, BundleResult, BundleTx, RevertPolicy, TxOutcome};
// Primary entry points, hoisted to the crate root for discoverability. The
// fully-qualified module paths (`cache::EvmCache`, `reactive::ReactiveRuntime`,
// …) remain valid, so this is purely additive.
pub use cache::{
    AccountFieldsFetchFn, AccountProof, AccountProofFetchFn, BlockContextRequirements,
    BlockStateAccountDiff, BlockStateDiff, BlockStateDiffFetchFn, BlockStateStorageDiff,
    CacheSpeedMode, CallSimulationResult, CodeMismatch, CodeSeedState, CodeVerifyReport, EvmCache,
    EvmCacheBuilder, EvmOverlay, EvmSnapshot, PrewarmReport, StorageBatchConfig,
    StorageFetchStrategy, TxConfig, point_read_storage_fetcher,
};
#[cfg(feature = "reactive")]
pub use cold_start::{
    ColdStartCall, ColdStartCallResult, ColdStartConfig, ColdStartError, ColdStartPin,
    ColdStartPlan, ColdStartPlanner, ColdStartResults, ColdStartRoundSummary, ColdStartRunReport,
    ColdStartStep, RootBaseline, RootBaselinePlanner, RootProbeOutcome, RoundOutcome,
};
pub use errors::{
    AccessListError, AccessListResult, BlockContextError, CacheError, CacheResult, DeployError,
    DeployResult, FreshnessError, FreshnessResult, MulticallError, MulticallResult, OverlayError,
    OverlayResult, PersistenceError, RpcError, RuntimeError, SimError, SimHostError,
    SimulationError, SimulationResult, StorageFetchError, StorageFetchResult,
};
pub use events::erc20::Erc20TransferDecoder;
pub use events::{
    BlockDigest, DecoderRegistry, EventDecoder, EventPipeline, ReconcileReport, ReorgConfig,
    StateView,
};
pub use freshness::{
    AlwaysVerify, BlockClock, FreshnessClock, FreshnessController, FreshnessParams,
    FreshnessPolicy, FreshnessRegistry, NeverVerify, ObservationDriven, SimRequest, SlotChange,
    SlotFetch, SlotOutcome, SpeculativeSim, Validation, Validity, WallClock,
};
// Trace-based hash-derived storage-slot discovery (v0.2.1).
pub use mapping_probe::{
    Confidence, HashSlotAccess, HashStorageProbe, SlotLayout, TrackedBalances, TrackedMapping,
};
#[cfg(feature = "reactive")]
pub use reactive::{
    InterestOwnerSubscriber, ReactiveConfig, ReactiveEngine, ReactiveEngineError,
    ReactiveEngineRegisterError, ReactiveHandler, ReactiveRuntime,
};
pub use state_update::{
    AccountChange, AccountPatch, PurgeRecord, PurgeScope, SkippedAccountPatch, SkippedBalanceDelta,
    SkippedDelta, SkippedMask, SlotDelta, StateDiff, StateUpdate,
};
pub use tracing::{CallKind, CallStatus, CallTrace, CallTracer, InspectorStack};
