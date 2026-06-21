# evm-fork-cache

[![CI](https://github.com/KaiCode2/evm-fork-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/KaiCode2/evm-fork-cache/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/evm-fork-cache.svg)](https://crates.io/crates/evm-fork-cache)
[![docs.rs](https://img.shields.io/docsrs/evm-fork-cache)](https://docs.rs/evm-fork-cache)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A forked-EVM **simulation engine** for EVM search, MEV, and backtesting — built
on [`revm`], [`alloy`], and [`foundry-fork-db`].

It exists to answer one question fast and repeatedly: *"if I sent this
transaction against current on-chain state, what would happen?"* — for thousands
of candidate transactions per block, without paying an RPC round-trip or
re-deriving state on every call.

[`revm`]: https://github.com/bluealloy/revm
[`alloy`]: https://github.com/alloy-rs/alloy
[`foundry-fork-db`]: https://github.com/foundry-rs/foundry-fork-db

## Why it exists

A search loop evaluates many hypothetical transactions against the *same*
recent chain state. Doing that with a naive fork means re-fetching state, paying
RPC latency on the hot path, and either sharing mutable EVM state across tasks
(unsafe) or deep-cloning a fork per candidate (slow). `evm-fork-cache` is built
around three capabilities that target exactly this workload:

1. **Cheap parallel fan-out** — freeze state once into an immutable snapshot,
   hand a cheap `Arc` clone to each task, and run many isolated simulations in
   parallel. No task can observe another's writes.
2. **Targeted state sync** — refresh or purge *specific* accounts and storage
   slots in place (no RPC on the hot path), so hot contract state stays correct
   without re-forking.
3. **Freshness as a first-class concept** — the engine tracks what it can trust,
   for how long, and verifies the rest. The optimistic verify-and-rerun loop
   hides RPC latency: act on speculative results immediately, get a `Confirmed`
   or `Corrected` verdict when the background validation lands.

> **Maturity.** This crate is **pre-1.0** and under active development against a
> [phased roadmap](docs/ROADMAP.md). Capabilities (1) and (3) above are
> implemented today. Capability (2) has the targeted writer primitives, the
> event-to-state reader pipeline, and a default-enabled reactive handler runtime;
> live network subscription driving remains consumer-provided. The public API
> still changes between minor versions — see [Stability](#stability).

## What it provides today

- **Forked EVM cache** backed by `foundry-fork-db` with lazy RPC loading and
  on-disk persistence for accounts, storage, bytecode, and immutable metadata.
- **Snapshots and overlays** — `create_snapshot()` produces an immutable,
  `Send + Sync` point-in-time view; each `EvmOverlay` is a cheap clone that
  simulates in isolation, ideal for parallel candidate evaluation.
- **Freshness control plane** — a four-layer model (classification, observation,
  policy, mechanism) plus an optimistic verify-and-rerun execution loop with
  deferred validation. See the [`freshness`](src/freshness.rs) module.
- **Targeted state manipulation** — direct storage injection, account/slot
  purge, and balance overrides for hot-state refresh workflows.
- **Event-to-state pipeline** — decode logs into `StateUpdate`s, apply them in
  order, purge touched state on reorg, and reconcile sampled event-derived slots
  against RPC. The crate ships the generic driver, the ERC-20 `Transfer` decoder,
  and in-memory examples; production WebSocket subscription/reorg wiring and
  protocol-specific decoders stay with the consumer or companion crates.
- **Reactive runtime** — register pure handlers for logs, block notifications,
  and pending transaction signals. Handlers emit `StateUpdate`s, invalidations,
  resync requests, speculative signals, and hook signals; the runtime routes
  inputs, deduplicates and orders canonical logs, validates pending semantics,
  applies canonical cache mutations through `EvmCache::apply_updates`, and
  can optionally execute storage resync requests through the cache's
  provider-neutral storage batch fetcher before dispatching reports to hooks.
  Canonical block effects are journaled for depth-bounded reorg recovery:
  removed logs, explicit reorged inputs, and parent-hash discontinuities emit
  `ReactiveReport::Reorg`, roll back reversible storage writes, fall back to
  targeted purges for irreversible effects, and cancel stale hash-pinned
  resyncs. The
  `ReactiveRegistry` exposes consolidated Alloy log filters for provider
  subscription setup and exact local log routing with optional route keys. The
  provider-agnostic `EventSubscriber` trait and `AlloySubscriber` scaffold are
  included; live Alloy stream driving is intentionally left to a future transport
  layer.
- **ERC20 helpers** — balances, allowances, decimals, and controlled balance
  mutation (including automatic balance-slot discovery) for simulations.
- **Transfer-inspector simulation** that reports per-token balance deltas
  straight from the `Transfer` event stream, no extra pre/post balance queries.
- **Access-list tooling** — `StorageAccessList` captures the EIP-2929 warm-access
  touch set; helpers build an EIP-2930 access list and estimate whether attaching
  one is profitable on an L2.
- **Multicall3 batching** for running many view calls inside the fork in one pass.
- **Deployment & etching** — deploy from creation code, or etch locally compiled
  Foundry runtime bytecode over a forked contract while preserving its storage.
- **CREATE3 address derivation** utilities.
- **An extensible revert decoder** — the two Solidity built-ins (`Error(string)`
  and `Panic(uint256)`) decode natively; register your own contract-defined
  custom errors in one line. Duplicate custom-error selectors keep the first
  registration and can be rejected explicitly with `try_register*`.

## Quick start

```rust,no_run
use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_provider::{ProviderBuilder, network::AnyNetwork};
use alloy_primitives::{Address, Bytes};
use evm_fork_cache::cache::EvmCache;
use revm::primitives::hardfork::SpecId;

# async fn example() -> anyhow::Result<()> {
let provider = ProviderBuilder::new()
    .network::<AnyNetwork>()
    .connect_http("https://example-rpc.invalid".parse()?);

// Build a cache pinned to the latest block. (Requires a multi-thread tokio
// runtime — see the note below.)
let mut cache = EvmCache::builder(Arc::new(provider))
    .latest_block()
    .spec(SpecId::CANCUN)
    .build()
    .await;

let from = Address::ZERO;
let to = Address::repeat_byte(0x11);
let calldata = Bytes::new();

// Simulate, capturing the EIP-2929 touch set as we go.
let (_result, touched) = cache.call_raw_with_access_list(from, to, calldata)?;
println!(
    "touched {} accounts and {} storage slots",
    touched.account_count(),
    touched.slot_count()
);
# Ok(())
# }
```

> **Runtime requirement.** `EvmCache` lazily fetches missing state through a
> synchronous façade over an async provider (`tokio::task::block_in_place`), so
> its constructors and any method that may touch RPC must run on a **multi-thread**
> tokio runtime (`#[tokio::main(flavor = "multi_thread")]` or
> `#[tokio::test(flavor = "multi_thread")]`). The offline examples and tests build
> the cache over a mocked provider and never touch the network.

## Core concepts

The state stack flows bottom-to-top; reads flow up and the fork DB lazily fetches
misses from RPC:

```
EvmOverlay × N      isolated, Send simulations (cheap Arc clones)
     ▲ clone × N
EvmSnapshot         immutable, point-in-time, Send + Sync
     ▲ create_snapshot()
EvmCache            lazy RPC fetch + local state cache + targeted writes/purge
     ▲ lazy fetch
RPC provider
```

- **`EvmCache`** owns the mutable fork: it fetches, caches, persists, and applies
  targeted writes/purges. It is `!Send` (it block_on's RPC internally).
- **`EvmSnapshot`** is an immutable flattening of the cache at a point in time,
  shareable across threads via `Arc`.
- **`EvmOverlay`** wraps a snapshot with a per-simulation dirty layer; clone one
  per candidate transaction and simulate without RPC and without touching the
  live cache.

The [`freshness`](src/freshness.rs) module layers a freshness controller on top:
classify each address/slot (`Pinned` / `Volatile` / `ValidThrough`), observe how
often slots change, pick what to verify each cycle with a `FreshnessPolicy`, and
run the optimistic loop that returns speculative results immediately and a
`Confirmed`/`Corrected`/`Unverified` verdict asynchronously.

## Examples

The [`examples/`](examples) directory has runnable, documented examples. Run any
with `cargo run --example <name>`.

**Offline examples** need no network — they build the cache over a mocked provider
and inject all state directly:

| Example | Level | Shows |
| --- | --- | --- |
| `revert_decoding` | Basic | Decode the standard Solidity `Error`/`Panic`/unknown reverts. |
| `custom_revert_errors` | Basic | Register your own custom Solidity error selectors with `RevertDecoder`. |
| `create3_addresses` | Basic | Derive CREATE3 deployment addresses off-chain. |
| `storage_access_list` | Basic | Merge touch sets, estimate EIP-2929 savings, build an EIP-2930 list. |
| `erc20_balance_override` | Basic | Set an ERC20 balance by scanning for its storage slot. |
| `snapshot_and_restore` | Intermediate | In-place `snapshot()`/`restore()` rollback on one cache. |
| `parallel_overlays` | Intermediate | Fan one `create_snapshot()` out to many isolated `EvmOverlay` simulations. |
| `transfer_inspector` | Intermediate | Report per-token balance deltas from a simulation. |
| `deploy_and_override` | Intermediate | Deploy from creation code and etch it over another address. |
| `foundry_artifact_etching` | Intermediate | Etch a locally compiled Foundry artifact (from a JSON file) over a fork. |
| `prefetch_registry` | Advanced | Record and persist storage touch sets for cross-cycle prefetch. |
| `freshness_optimistic` | Advanced | Optimistic verify-and-rerun loop: a `Corrected` validation via a stub fetcher. |
| `freshness_multi_sim` | Advanced | Many sims with selective re-run, plus classification and `ValidThrough` aging. |
| `state_update_apply` | Advanced | Apply a mixed `StateUpdate` batch (`Slot`/`Account`/`Purge`) and inspect the returned `StateDiff`. |
| `reactive_cache` | Advanced | Decode ERC-20 `Transfer` logs into `StateUpdate`s, ingest a block, reconcile drift, and purge on a reorg. |

**RPC examples** fork real mainnet state. Set `RPC_URL` to an Ethereum RPC
endpoint (they print instructions and exit if it is unset):

| Example | Level | Shows |
| --- | --- | --- |
| `fork_token_balance` | Basic | Lazy RPC loading and warm-cache reuse (cold vs. warm read). |
| `multicall_batch` | Intermediate | Batch many view calls through Multicall3 in one pass. |
| `multicall_with_error_handling` | Intermediate | Batch with `allowFailure`; read partial results when a call reverts. |
| `fork_override_balance` | Intermediate | Discover a real token's balance slot and override it. |

```sh
cargo run --example revert_decoding
RPC_URL=https://eth.llamarpc.com cargo run --example fork_token_balance
```

## Foundry artifact etching

Use `etch_foundry_artifact` when replacing an existing forked contract while
preserving its storage, balance, and nonce. Use
`etch_foundry_artifact_or_create` for synthetic simulation addresses. See the
runnable [`foundry_artifact_etching`](examples/foundry_artifact_etching.rs) example.

```rust,ignore
use alloy_primitives::Address;
use evm_fork_cache::deploy::{encode_constructor_args, etch_foundry_artifact_or_create};

# fn example(cache: &mut evm_fork_cache::cache::EvmCache) -> anyhow::Result<()> {
let target = Address::repeat_byte(0x42);
let constructor_args = encode_constructor_args((Address::ZERO,));

let etched = etch_foundry_artifact_or_create(
    cache,
    target,
    "out/MyContract.sol/MyContract.json",
    Address::ZERO,
    constructor_args,
)?;

println!("installed {} bytes at {}", etched.code_size, etched.target_address);
# Ok(())
# }
```

## Benchmarks

Criterion benchmarks live in [`benches/`](benches). The offline benches exercise
the current hot paths at a range of cache sizes, including the Phase 5
copy-on-write snapshot implementation and retained deep-clone baselines where
useful for A/B comparison:

| Bench | Measures |
| --- | --- |
| `simulation` | `create_snapshot` across cache sizes (100 → 10k accounts), overlay fan-out, `call_raw` throughput, sequential bundle execution, batched storage injection. |
| `freshness` | The optimistic loop end-to-end (CPU and latency-hiding), `verify_slots` at scale (1 → 1000 slots), and multi-sim fan-out. |
| `state_update` | `apply_updates` throughput across batch sizes (1 → 1000 `Slot`s) and per-variant apply cost (`Slot` vs `Account` vs `Purge`). |
| `event_pipeline` | Per-decoder cost (ERC-20 `Transfer`, generic slot marker), `ingest_logs` decode+apply throughput (1 → 1000 logs), and `reorg_to` purge cost. |
| `access_list` | Touch-set merge and EIP-2930 list construction. |
| `revert_decoding` | Built-in and custom revert decoding, including decoder dispatch with many registered errors. |
| `create3` | CREATE3 address derivation. |

```sh
cargo bench                      # all offline benches
cargo bench --bench simulation   # one suite
```

The `rpc_mainnet` bench runs against **live mainnet state** to validate
real-contract performance (USDC `balanceOf`, `totalSupply`, and `allowance`). It is
gated behind the `RPC_URL` environment variable and is skipped (not failed) when
it is unset, so `cargo bench` stays offline and CI-reproducible by default:

```sh
RPC_URL=https://eth.llamarpc.com cargo bench --bench rpc_mainnet
```

## Crate boundary

`evm-fork-cache` is the generic simulation engine: cache, snapshots/overlays,
freshness control, access lists, revert decoding, ERC-20 helpers, multicall,
deployment, CREATE3, and event-pipeline primitives. AMM state tracking,
protocol-specific storage layouts, and DeFi adapters belong in the companion
`evm-amm-state` crate or downstream applications.

## Stability

`evm-fork-cache` is pre-1.0. Until 1.0, **breaking changes may land in minor
releases** — the roadmap deliberately reshapes the API before the surface
freezes. Each release documents its breaking changes in [`CHANGELOG.md`](CHANGELOG.md).

- **MSRV:** Rust 1.88 (enforced in CI). Edition 2024.
- **Semver:** pre-1.0 minor versions may break; patch versions will not.
- **Roadmap:** see [`docs/ROADMAP.md`](docs/ROADMAP.md) for the path to 1.0.
- **Known issues / limitations:** see [`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md).

## Contributing

Contributions are welcome — see [`CONTRIBUTING.md`](CONTRIBUTING.md) for branch
conventions, the green-bar CI expectations, and the commit format.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
