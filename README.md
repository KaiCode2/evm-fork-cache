# evm-fork-cache

`evm-fork-cache` is a Rust EVM simulation support crate built around `revm`,
`alloy`, and `foundry-fork-db`. It is intended for DeFi search systems that
need repeatable forked-state simulation, low-latency cache reuse, and safe
parallel evaluation of candidate transactions.

## What It Provides

- Forked EVM cache backed by `foundry-fork-db` with lazy RPC loading.
- Binary state persistence for accounts, storage, bytecode, immutable metadata,
  and Uniswap V3-style tick snapshots.
- Snapshot and overlay APIs for parallel simulations without sharing mutable
  REVM state across tasks.
- Direct storage injection and purge helpers for pool-state refresh workflows.
- ERC20 helpers for balances, allowances, decimals, and controlled balance
  mutation in simulations.
- Transfer-inspector simulation that reports token balance deltas without
  extra pre/post balance queries.
- Storage touch-set capture via `StorageAccessList` for EIP-2929 warm-access
  accounting and batch prefetch.
- Multicall3 batching helpers for running many view calls inside the fork.
- Foundry artifact deployment and etching helpers for installing locally
  compiled runtime bytecode into a forked simulator.
- CREATE3 address derivation utilities.
- An extensible revert decoder: the two Solidity built-ins (`Error(string)` and
  `Panic(uint256)`) are decoded natively, and you register your own
  contract-defined custom errors in one line.

## Example

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

let mut cache = EvmCache::with_cache(
    Arc::new(provider),
    Some(BlockId::latest()),
    None,
    SpecId::CANCUN,
)
.await;

let from = Address::ZERO;
let to = Address::repeat_byte(0x11);
let calldata = Bytes::new();

let (_result, touched) = cache.call_raw_with_access_list(from, to, calldata)?;
println!(
    "touched {} accounts and {} storage slots",
    touched.account_count(),
    touched.slot_count()
);
# Ok(())
# }
```

## Foundry Artifact Etching

Use `etch_foundry_artifact` when replacing an existing forked contract while
preserving its storage, balance, and nonce. Use
`etch_foundry_artifact_or_create` for synthetic simulation addresses.

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

## Examples

The [`examples/`](examples) directory has runnable, documented examples. Run any
with `cargo run --example <name>`.

Offline examples need no network — they build the cache over a mocked provider
and inject all state directly:

| Example | Shows |
| --- | --- |
| `revert_decoding` | Decode the standard Solidity `Error`/`Panic`/unknown reverts. |
| `custom_revert_errors` | Register your own custom Solidity error selectors with `RevertDecoder`. |
| `create3_addresses` | Derive CREATE3 deployment addresses off-chain. |
| `storage_access_list` | Merge touch sets, estimate EIP-2929 savings, build an EIP-2930 list. |
| `erc20_balance_override` | Set an ERC20 balance by scanning for its storage slot. |
| `snapshot_and_restore` | In-place `snapshot()`/`restore()` rollback on one cache. |
| `parallel_overlays` | Fan one `create_snapshot()` out to many isolated `EvmOverlay` simulations. |
| `transfer_inspector` | Report per-token balance deltas from a simulation. |
| `deploy_and_override` | Deploy from creation code and etch it over another address. |
| `prefetch_registry` | Record and persist storage touch sets for cross-cycle prefetch. |

RPC-gated examples fork real mainnet state. Set `RPC_URL` to an Ethereum RPC
endpoint (they print instructions and exit if it is unset):

| Example | Shows |
| --- | --- |
| `fork_token_balance` | Lazy RPC loading and warm-cache reuse (cold vs. warm read). |
| `multicall_batch` | Batch many view calls through Multicall3 in one pass. |
| `fork_override_balance` | Discover a real token's balance slot and override it. |

```sh
cargo run --example revert_decoding
RPC_URL=https://eth.llamarpc.com cargo run --example fork_token_balance
```

## Benchmarks

Offline Criterion microbenchmarks live in [`benches/`](benches) (revert
decoding, storage-key derivation, CREATE3, and access-list bookkeeping):

```sh
cargo bench
```

## Cargo features

- `protocols` *(default)* — DeFi protocol knowledge: Uniswap V2/V3-style storage
  layouts, V3 tick snapshots, and the `inject_v3_*` / `inject_v2_pool_metadata`
  helpers. Build with `--no-default-features` for the generic simulation engine
  alone (the revert decoder, snapshots/overlays, ERC20 helpers, multicall, deploy,
  CREATE3). This surface is slated to move into a separate `evm-amm-state` crate.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
