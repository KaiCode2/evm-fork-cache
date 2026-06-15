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
