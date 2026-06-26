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
> implemented today. Capability (2) has the targeted writer primitives and the
> event-to-state reader pipeline; a production WebSocket transport remains
> consumer-provided. The public API still changes between minor versions — see
> [Stability](#stability).

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
misses from RPC. The event-log path writes hot state in with **no RPC** (the
reactive-sync control plane):

```mermaid
flowchart BT
    RPC["RPC provider"] -->|"lazy fetch · once"| CACHE
    LOGS["on-chain event logs"] -.->|"decode → write · 0 RPC"| CACHE
    CACHE["<b>EvmCache</b> · !Send<br/>fetch · cache · targeted writes/purge"] -->|"create_snapshot()"| SNAP
    SNAP["<b>EvmSnapshot</b> · Send + Sync<br/>immutable · Arc · point-in-time"] -->|"cheap Arc clone × N"| OV
    OV["<b>EvmOverlay × N</b> · Send<br/>isolated parallel simulations"]
    classDef hot fill:#102a17,stroke:#3fb950,color:#e6edf3;
    classDef cool fill:#0d1f2d,stroke:#388bfd,color:#e6edf3;
    class SNAP,OV hot;
    class RPC,CACHE,LOGS cool;
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
`Confirmed`/`Corrected`/`Unverified` verdict asynchronously. Time-to-actionable-result
is gated on local simulation, not on the RPC validation that runs behind it:

```mermaid
sequenceDiagram
    autonumber
    participant S as Search loop
    participant C as FreshnessController
    participant V as Background validator
    participant R as RPC
    S->>C: run(candidate sims)
    C->>C: snapshot + run optimistic sims
    C-->>S: SpeculativeSim — optimistic results (~µs)
    Note over S: act on speculative results now
    C->>V: spawn (Send data only)
    V->>R: verify volatile read-set (~L ms)
    R-->>V: fresh values
    alt nothing the sims read changed
        V-->>S: validate().await → Confirmed
    else a read slot changed
        V->>V: re-run only the affected sims
        V-->>S: Corrected { results, changed }
    end
```

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

## Performance &amp; honest trade-offs

It is easy to post huge multipliers against a *naive* loop (a fresh cold fork per
candidate that re-fetches everything and deep-clones to isolate). That is **not**
the loop a competent revm user writes. Measured against a **competent baseline** —
one shared [`foundry-fork-db`] `SharedBackend` (which caches and deduplicates
fetches) plus `checkpoint`/`revert` isolation on a single fork — this crate is
**roughly at parity on raw within-block speed**, and we say so plainly:

| Axis | vs a competent shared-backend / checkpoint-revert loop |
|---|---|
| RPC reads **within one block** | **~1×** — a shared `SharedBackend` also fetches each hot slot once |
| Single-threaded per-candidate CPU | **~1×** — `checkpoint`/`revert` isolation is as cheap as an overlay |
| Time-to-result vs *blocking* validation | not a fair comparison — a competent loop doesn't block on a fetch before acting |

The value of this crate is **not** a within-block speed multiplier. It is
correctness, cross-block freshness, and a structured control plane the bare
primitives don't give you:

**① Cross-block freshness — the one quantitative win (exact, CI-pinned integer).**
`foundry-fork-db`'s cache is **not block-keyed**: re-pinning to a new block does
not invalidate cached slots, so a refresh-by-refetch loop must re-read every slot
that changed *each block* to stay correct. Decoding the block's logs into targeted
writes keeps that hot state correct with **0 RPC fetches/block** — pinned in
[`tests/event_pipeline.rs`](tests/event_pipeline.rs). Sampled `reconcile()`
re-reads a fraction to catch drift (the honesty backstop). See
[`reactive_cache`](examples/reactive_cache.rs).

<p align="center">
  <img alt="Cumulative RPC slot reads over 8 blocks: refresh-by-refetch climbs linearly to 64 while event-driven writes stay flat at 8 (warmed once, then 0 per block)" width="660"
       src="https://raw.githubusercontent.com/KaiCode2/evm-fork-cache/main/assets/cross_block_freshness.svg">
</p>

> Honest caveat: an equally sophisticated peer running their *own* log
> subscription + delta applier also reaches 0 fetches/block. The crate's
> contribution is the packaged, cold-aware, reorg-safe, reconcilable vocabulary —
> not an unreachable number.

**② Parallel fan-out — available, modest, workload-dependent.**
`create_snapshot()` is an immutable `Send + Sync` view; cloning the `Arc` hands
each thread its own overlay, so candidates fan out across cores — which a single
mutable fork cannot do. The measured speedup is honest and modest: **~1.2×** over a
4,096-candidate batch on a 10-core M1 Pro, because these micro-sims are bound by
per-candidate allocation, not EVM compute. Heavier candidates (real txs doing
substantial execution) parallelize better; trivial ones barely. We don't headline a
core-count multiplier we can't reproduce. `cargo bench --bench fanout`;
[`parallel_overlays`](examples/parallel_overlays.rs).

**③ Point-in-time consistency.** Every overlay reads one frozen, consistent block
state. A lazily-filled shared backend can interleave reads taken at slightly
different moments unless carefully pinned; the snapshot removes that class of bug.

**④ Act-then-validate control plane (structure, not speed).** Run optimistically
against current state, return immediately, and validate the volatile read-set in
the background — re-running *only* the sims whose slots changed (`rerun_count`),
with `Confirmed`/`Corrected`/`Unverified` verdicts and block-pinned validation. A
searcher who simply acts on warm state is equally fast; the value is the **safe,
selective re-run**, not a latency multiplier. See
[`freshness_optimistic`](examples/freshness_optimistic.rs).

> [!NOTE]
> **Methodology &amp; candor.** Offline (mocked provider, state injected — no
> network). We deliberately do **not** lead with the headline multipliers a naive
> baseline would produce (~500× fewer reads, ~545× throughput, ~3,800× latency):
> all three collapse toward ~1× against a competent `SharedBackend` +
> `checkpoint`/`revert` loop — which is the very primitive this crate wraps. The
> "0 fetches/block" integer is real and CI-pinned (`cargo test --test
> event_pipeline`); the parallel-fan-out ratio is a Criterion median on an Apple
> M1 Pro, read as a ratio not an absolute. Live-RPC checks live behind the
> `RPC_URL` gate.

## Benchmarks

Criterion benchmarks live in [`benches/`](benches) and run fully offline (mocked
provider) so they are reproducible:

| Bench | Measures |
| --- | --- |
| `fanout` | **Parallel fan-out (②).** N candidates **sequential vs across cores** over one shared snapshot — the parallelism a live mutable fork can't do. |
| `freshness` | **Act-then-validate (④).** The optimistic loop CPU cost, selective re-run, and the latency-hiding shape (vs a baseline that elects to block). `verify_slots` at scale; multi-sim fan-out. |
| `event_pipeline` | **Cross-block freshness (①).** `ingest_logs` decode+apply throughput (1 → 1000 logs), `reorg_to` purge; the 0-fetch/block property is pinned in `tests/event_pipeline.rs`. |
| `state_update` | `apply_updates` throughput across batch sizes (1 → 1000 `Slot`s) and per-variant apply cost (`Slot` vs `Account` vs `Purge`). |
| `simulation` | Hot-path micro-benches and snapshot-implementation regression guards (`create_snapshot` vs the deep-clone reference — an internal cost model, see [`docs/INTERNALS.md`](docs/INTERNALS.md)). |
| `access_list` | Touch-set merge and EIP-2930 list construction. |
| `revert_decoding` | Built-in (`Error`/`Panic`) and custom-error revert decoding, and decoder dispatch over a registered custom error. |
| `create3` | CREATE3 address derivation. |

```sh
cargo bench                      # all offline benches
cargo bench --bench fanout       # one suite
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
