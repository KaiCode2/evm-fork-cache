# Changelog

All notable changes to `evm-fork-cache` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Pre-1.0 policy:** until `1.0.0`, breaking changes may land in **minor**
versions (`0.x.0`); patch versions (`0.x.y`) are non-breaking. The roadmap in
[`docs/ROADMAP.md`](docs/ROADMAP.md) deliberately reshapes the API before the
surface freezes at 1.0.

## [Unreleased]

This is the first release line. It captures the work done across the
pre-release development phases (see [`docs/ROADMAP.md`](docs/ROADMAP.md)).

### Added

- **Forked EVM cache** (`cache::EvmCache`) backed by `foundry-fork-db` with lazy
  RPC loading and on-disk persistence for accounts, storage, bytecode, immutable
  metadata, and Uniswap V3-style tick snapshots.
- **`EvmCacheBuilder`** — a fluent constructor (`EvmCache::builder(provider)`)
  subsuming the positional `with_cache` / `from_backend` constructors, with
  per-instance cache-speed configuration.
- **Snapshots and overlays** — `create_snapshot()` produces an immutable,
  `Send + Sync` `EvmSnapshot`; `EvmOverlay` is a cheap per-simulation clone for
  isolated parallel evaluation.
- **Freshness control plane** (`freshness` module, Phase 2) — the four-layer
  model (`Validity`/`FreshnessRegistry`, `SlotObservationTracker`,
  `FreshnessPolicy`, `FreshnessController`), a configurable `FreshnessClock`
  (`BlockClock`/`WallClock`), and the optimistic verify-and-rerun execution loop
  with deferred validation (`SpeculativeSim`/`Validation`).
- **Freshness primitives on `EvmCache`** — `verify_slots`, `purge_account`,
  `set_storage_batch_fetcher`; `EvmOverlay::call_raw_with_access_list` and
  `override_slot` for read-set capture and corrected re-runs.
- **Configurable transaction & block environment** — `TxConfig` (value, gas
  limit, gas price, nonce, access list) threaded through `call_raw_with`; block
  context setters (`set_coinbase`, `set_prevrandao`, `set_block_gas_limit`).
- **Transfer-inspector simulation** (`inspector`) reporting per-token balance
  deltas from the `Transfer` event stream.
- **Access-list tooling** (`access_list`, `access_set`) — `StorageAccessList`
  touch-set capture, EIP-2930 list construction, and L2 profitability estimation.
- **Multicall3 batching** (`multicall`).
- **Deployment & etching** (`deploy`) — deploy from creation code, etch Foundry
  artifacts over forked contracts; **CREATE3** address derivation (`create3`).
- **Extensible revert decoder** (`errors`) — native `Error(string)` / `Panic(uint256)`
  decoding plus one-line custom-error registration; typed `SimError`
  (`Revert` / `Halt` / `Host`).
- **Two-stage prefetch registry** (`prefetch_registry`) for cross-cycle
  storage-slot pre-warming.
- **`protocols` feature** (default-on) gating the Uniswap V2/V3 storage layouts,
  V3 tick snapshots, and `inject_v3_*` / `inject_v2_pool_metadata` helpers, so
  the generic engine builds with `--no-default-features`.

### Changed

- Simulation entry points that distinguish failure modes return
  `SimulationResult<T>` (`Result<T, SimError>`), separating decoded reverts,
  EVM halts, and host errors. `SimulationErrorKind` remains as a deprecated alias.

### Notes

- MSRV is Rust 1.88; edition 2024. Both are enforced in CI.
- `EvmCache` requires a multi-thread tokio runtime for any RPC-touching path.
- See [`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md) for current limitations.

[Unreleased]: https://github.com/KaiCode2/evm-fork-cache/commits/main
