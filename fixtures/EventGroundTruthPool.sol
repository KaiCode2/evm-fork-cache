// SPDX-License-Identifier: MIT OR Apache-2.0
pragma solidity ^0.8.20;

/// @title TestV3Pool
/// @notice A faithful **stand-in** for a UniswapV3 pool used by the event →
///         state differential test (`tests/event_ground_truth.rs`). It is NOT
///         verbatim Uniswap bytecode; instead it reproduces the two things our
///         event decoder actually depends on, and lets the Solidity compiler —
///         not the test author — generate them:
///           1. The real UniswapV3 `slot0` **storage packing**: `slot0` is a
///              struct with the identical field order/widths as
///              `IUniswapV3PoolState.slot0`, so the compiler packs
///              `sqrtPriceX96` (bits [0,160)), `tick` (int24, [160,184)),
///              `observationIndex`/cardinality/`feeProtocol`/`unlocked` ([184,256))
///              into one word at storage slot 0 exactly as the real pool does.
///              A swap assigns only `.sqrtPriceX96`/`.tick`, so the compiler emits
///              the masked update that preserves the observation/`unlocked` bits —
///              the exact behavior our `StateUpdate::SlotMasked` must reproduce.
///           2. The canonical `Swap(...)` event signature, emitted with the same
///              `sqrtPriceX96`/`liquidity`/`tick` values written to storage.
///
/// @dev Storage layout (mirrors UniswapV3Pool so the slots match
///      `UniswapV3Layout::uniswap`):
///        slot 0: slot0 (packed)
///        slot 1: feeGrowthGlobal0X128   (unused, for layout parity)
///        slot 2: feeGrowthGlobal1X128   (unused)
///        slot 3: protocolFees           (unused)
///        slot 4: liquidity              (uint128)
///      `token0`/`token1` are immutable (baked into code, not stored), so they do
///      not perturb the slot numbering.
///
/// The swap *outcome* (amounts, new price/tick/liquidity) is supplied by the
/// caller so the test is deterministic; the pool still performs real ERC-20
/// transfers (emitting canonical `Transfer` logs from the token contracts) and a
/// real compiler-packed `slot0` update. The price math itself is irrelevant to
/// what the event processor reconstructs — it reads `sqrtPriceX96`/`tick` from the
/// emitted event, never from the pool's internals.
interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
}

contract TestV3Pool {
    /// Identical field order/widths to UniswapV3Pool.Slot0 (one packed word).
    struct Slot0 {
        uint160 sqrtPriceX96;
        int24 tick;
        uint16 observationIndex;
        uint16 observationCardinality;
        uint16 observationCardinalityNext;
        uint8 feeProtocol;
        bool unlocked;
    }

    event Swap(
        address indexed sender,
        address indexed recipient,
        int256 amount0,
        int256 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick
    );

    Slot0 public slot0; // slot 0
    uint256 private feeGrowthGlobal0X128; // slot 1
    uint256 private feeGrowthGlobal1X128; // slot 2
    uint256 private protocolFees; // slot 3
    uint128 public liquidity; // slot 4

    address public immutable token0;
    address public immutable token1;

    constructor(address _token0, address _token1) {
        token0 = _token0;
        token1 = _token1;
    }

    /// Set the initial packed `slot0` (with `unlocked = true` and a non-zero
    /// observation index, so the differential test can prove those bits survive
    /// a swap) and the initial `liquidity`.
    function initialize(uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint128 _liquidity)
        external
    {
        slot0 = Slot0({
            sqrtPriceX96: sqrtPriceX96,
            tick: tick,
            observationIndex: observationIndex,
            observationCardinality: 1,
            observationCardinalityNext: 1,
            feeProtocol: 0,
            unlocked: true
        });
        liquidity = _liquidity;
    }

    /// Execute a swap with a caller-specified outcome: pull `amountIn` of the
    /// input token (real `transferFrom` → `Transfer` log), send `amountOut` of the
    /// output token (real `transfer` → `Transfer` log), update the packed `slot0`
    /// price/tick (compiler-masked, preserving the observation/`unlocked` bits)
    /// and `liquidity`, then emit the canonical `Swap` event with those values.
    function swap(
        bool zeroForOne,
        uint256 amountIn,
        uint256 amountOut,
        uint160 newSqrtPriceX96,
        int24 newTick,
        uint128 newLiquidity
    ) external {
        address tokenIn = zeroForOne ? token0 : token1;
        address tokenOut = zeroForOne ? token1 : token0;
        IERC20(tokenIn).transferFrom(msg.sender, address(this), amountIn);
        IERC20(tokenOut).transfer(msg.sender, amountOut);

        // Real Uniswap assigns the struct fields individually; the compiler emits
        // the masked SSTORE that preserves observation/unlocked. This is exactly
        // what `StateUpdate::SlotMasked` must reproduce off the event.
        slot0.sqrtPriceX96 = newSqrtPriceX96;
        slot0.tick = newTick;
        liquidity = newLiquidity;

        int256 amount0 = zeroForOne ? int256(amountIn) : -int256(amountOut);
        int256 amount1 = zeroForOne ? -int256(amountOut) : int256(amountIn);
        emit Swap(msg.sender, msg.sender, amount0, amount1, newSqrtPriceX96, newLiquidity, newTick);
    }
}
