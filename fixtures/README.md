# Test fixtures

Compiled bytecode used by the integration tests in [`../tests`](../tests) and
the examples in [`../examples`](../examples). Keeping the bytecode checked in
lets the test/example suite run without a Solidity toolchain.

## `MockERC20`

A deliberately minimal ERC20 (see [`MockERC20.sol`](MockERC20.sol)) used to
exercise the cache's storage manipulation, balance-override, snapshot, and
deployment helpers without touching a real network.

- `mock_erc20_runtime.hex` — deployed (runtime) bytecode, for installing the
  token directly at an address via `db_mut().insert_account_info`.
- `mock_erc20_creation.hex` — creation bytecode, for `deploy_contract`. The
  constructor takes `(string name, string symbol, uint8 decimals)`.
- `MockERC20.foundry.json` — a minimal Foundry-shaped build artifact wrapping the
  creation bytecode in `bytecode.object`, used by the `foundry_artifact_etching`
  example to exercise `deploy::etch_foundry_artifact*` (which load from a JSON
  artifact on disk). Regenerated from `mock_erc20_creation.hex`.

### Storage layout

| Slot | Variable                          |
| ---- | --------------------------------- |
| 0    | `name` (string)                   |
| 1    | `symbol` (string)                 |
| 2    | `totalSupply` (uint256)           |
| 3    | `balanceOf` (mapping)             |
| 4    | `allowance` (nested mapping)      |

`decimals` is `immutable`, so it lives in the bytecode rather than storage. The
balance of `owner` is therefore stored at `keccak256(abi.encode(owner, 3))`.

### Regenerating

Compiled with `solc`/`forge` (Solidity ^0.8.28). To regenerate from a Foundry
build:

```sh
jq -r '.deployedBytecode.object' out/MockERC20.sol/MockERC20.json \
  | sed 's/^0x//' > fixtures/mock_erc20_runtime.hex
jq -r '.bytecode.object' out/MockERC20.sol/MockERC20.json \
  | sed 's/^0x//' > fixtures/mock_erc20_creation.hex
```

## `TestV3Pool`

A faithful UniswapV3-pool **stand-in** (see
[`EventGroundTruthPool.sol`](EventGroundTruthPool.sol)) used by the Phase 4
differential ground-truth test
([`../tests/event_ground_truth.rs`](../tests/event_ground_truth.rs)). It is *not*
verbatim Uniswap bytecode; it reproduces the two things the event decoder depends
on and lets the compiler generate them: the real `slot0` **struct packing**
(`sqrtPriceX96`/`tick`/observation/`unlocked`, matching `UniswapV3Pool.Slot0`) at
storage slot 0, and the canonical `Swap(...)` event. A `swap` performs real ERC-20
transfers (canonical `Transfer` logs) and a compiler-masked `slot0` update, so the
test can replay only the emitted logs into a twin cache and assert the
event-derived state matches the ground-truth EVM execution bit-for-bit.

- `test_v3_pool_creation.hex` — creation bytecode, for `deploy_contract`. The
  constructor takes `(address token0, address token1)`; storage mirrors Uniswap
  (slot 0 = `slot0`, slot 4 = `liquidity`).

Regenerate with `solc` (the source is `^0.8.20`-compatible):

```sh
solc --bin --optimize --optimize-runs 200 --overwrite -o out fixtures/EventGroundTruthPool.sol
cp out/TestV3Pool.bin fixtures/test_v3_pool_creation.hex
```
