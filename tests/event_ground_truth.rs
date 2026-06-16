//! **Differential ground-truth test** for the event → state pipeline (Phase 4).
//!
//! The decisive correctness check: does feeding the *real emitted logs* of a swap
//! into our event processor reproduce the *exact* state a real EVM execution
//! produced? We run a swap in a ground-truth revm instance and replay only its
//! logs into a twin cache, then assert the token balances and the packed pool
//! `slot0` (price/tick) match bit-for-bit.
//!
//! Setup (an offline stand-in for RPC-fetched state):
//! 1. Deploy two ERC-20 tokens (the `MockERC20` fixture, balances at slot 3) and a
//!    `TestV3Pool` (`fixtures/EventGroundTruthPool.sol`) whose `slot0` is a Solidity
//!    **struct** with the identical field widths to `UniswapV3Pool.Slot0` — so the
//!    *compiler* (not this test) does the real bit-packing, and our
//!    `StateUpdate::SlotMasked` is the thing under test. Seed pool liquidity and a
//!    swapper balance.
//! 2. Build the identical pre-swap state in a second ("event-driven") cache. The
//!    deploy sequence is deterministic, so the token/pool addresses match.
//! 3. Execute a real `swap` against the ground-truth cache (committing) and
//!    capture the emitted logs (two ERC-20 `Transfer`s + the canonical `Swap`).
//! 4. Feed only those logs into the event-driven cache via `EventPipeline`, then
//!    assert its balances and `slot0` equal the ground-truth cache's.
//!
//! Runs fully offline. Requires the `protocols` feature (the UniswapV3 adapter).
#![cfg(feature = "protocols")]

mod common;

use std::sync::Arc;

use alloy_primitives::aliases::{I24, U160};
use alloy_primitives::{Address, Bytes, Log, U256, hex, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Result, anyhow};
use common::{MOCK_ERC20_CREATION_HEX, install_default_account, setup_cache};
use evm_fork_cache::cache::{EvmCache, V3_LIQUIDITY_SLOT, V3_SLOT0_SLOT};
use evm_fork_cache::deploy::{build_init_code, encode_constructor_args};
use evm_fork_cache::events::{DecoderRegistry, EventPipeline};
use evm_fork_cache::{Erc20TransferDecoder, UniswapV3Decoder, UniswapV3Layout};
use revm::context::result::ExecutionResult;

sol! {
    interface Token {
        function _mint(address to, uint256 amount) external;
        function approve(address spender, uint256 amount) external returns (bool);
    }
    interface Pool {
        function initialize(uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint128 liquidity) external;
        function swap(bool zeroForOne, uint256 amountIn, uint256 amountOut, uint160 newSqrtPriceX96, int24 newTick, uint128 newLiquidity) external;
    }
}

const POOL_CREATION_HEX: &str = include_str!("../fixtures/test_v3_pool_creation.hex");

/// The MockERC20 balance mapping slot (`mapping(address => uint256)` at slot 3).
const BALANCE_SLOT: u64 = 3;

/// Pre-swap parameters, shared by both caches.
const INIT_SQRT_PRICE: u128 = 1u128 << 96; // 2^96
const INIT_TICK: i32 = 100;
const INIT_OBS_INDEX: u16 = 7; // non-zero, to prove it survives the swap
const INIT_LIQUIDITY: u128 = 1_000_000;
const POOL_RESERVE: u128 = 1_000_000;
const SWAPPER_TOKEN0: u128 = 500_000;

/// Swap outcome (the test plays the role of the router specifying it). token0 in,
/// token1 out; a *negative* post-swap tick and a full-width `sqrtPriceX96` stress
/// the slot0 packing/sign handling.
const AMOUNT_IN: u128 = 120_000;
const AMOUNT_OUT: u128 = 80_000;
const NEW_TICK: i32 = -50;
const NEW_LIQUIDITY: u128 = 1_050_000;

fn deployer() -> Address {
    Address::repeat_byte(0xd0)
}
fn swapper() -> Address {
    Address::repeat_byte(0x5a)
}

/// Hashed `balanceOf[owner]` storage key.
fn balance_slot(owner: Address) -> U256 {
    U256::from_be_bytes(keccak256((owner, U256::from(BALANCE_SLOT)).abi_encode()).0)
}

fn call(cache: &mut EvmCache, from: Address, to: Address, data: Vec<u8>) -> Result<()> {
    match cache.call_raw(from, to, Bytes::from(data), true)? {
        ExecutionResult::Success { .. } => Ok(()),
        other => Err(anyhow!("call to {to} failed: {other:?}")),
    }
}

/// Build the identical pre-swap state in `cache`, returning `(token0, token1, pool)`.
///
/// The deploy order is fixed, so the deterministic `CREATE` addresses are the same
/// across caches (essential — the captured logs reference these addresses).
fn build_state(cache: &mut EvmCache) -> Result<(Address, Address, Address)> {
    install_default_account(cache, Address::ZERO); // coinbase
    install_default_account(cache, deployer());
    install_default_account(cache, swapper());

    // CREATE addresses are deterministic from (deployer, nonce). Pre-install them
    // as empty accounts so revm's CREATE collision-check reads the local overlay
    // instead of falling through to a (mocked, empty) RPC fetch.
    for nonce in 0..3 {
        install_default_account(cache, deployer().create(nonce));
    }

    let erc20_creation = hex::decode(MOCK_ERC20_CREATION_HEX.trim())?;
    let token0 = cache.deploy_contract(
        deployer(),
        build_init_code(
            &erc20_creation,
            // uint8 encodes as a right-aligned 32-byte word, identical to U256.
            encode_constructor_args(("Token0".to_string(), "T0".to_string(), U256::from(18))),
        ),
    )?;
    let token1 = cache.deploy_contract(
        deployer(),
        build_init_code(
            &erc20_creation,
            encode_constructor_args(("Token1".to_string(), "T1".to_string(), U256::from(18))),
        ),
    )?;
    let pool_creation = hex::decode(POOL_CREATION_HEX.trim())?;
    let pool = cache.deploy_contract(
        deployer(),
        build_init_code(&pool_creation, encode_constructor_args((token0, token1))),
    )?;

    // Initialize the pool's packed slot0 + liquidity.
    call(
        cache,
        deployer(),
        pool,
        Pool::initializeCall {
            sqrtPriceX96: U160::from(INIT_SQRT_PRICE),
            tick: I24::try_from(INIT_TICK).unwrap(),
            observationIndex: INIT_OBS_INDEX,
            liquidity: INIT_LIQUIDITY,
        }
        .abi_encode(),
    )?;

    // Seed reserves into the pool and the input balance into the swapper.
    for token in [token0, token1] {
        call(
            cache,
            deployer(),
            token,
            Token::_mintCall {
                to: pool,
                amount: U256::from(POOL_RESERVE),
            }
            .abi_encode(),
        )?;
    }
    call(
        cache,
        deployer(),
        token0,
        Token::_mintCall {
            to: swapper(),
            amount: U256::from(SWAPPER_TOKEN0),
        }
        .abi_encode(),
    )?;
    // Swapper approves the pool to pull the input.
    call(
        cache,
        swapper(),
        token0,
        Token::approveCall {
            spender: pool,
            amount: U256::from(AMOUNT_IN),
        }
        .abi_encode(),
    )?;

    Ok((token0, token1, pool))
}

/// Snapshot the four balances + the packed slot0 + liquidity we compare on.
fn observe(
    cache: &EvmCache,
    t0: Address,
    t1: Address,
    pool: Address,
) -> Vec<(String, Option<U256>)> {
    vec![
        (
            "swapper.t0".into(),
            cache.cached_storage_value(t0, balance_slot(swapper())),
        ),
        (
            "swapper.t1".into(),
            cache.cached_storage_value(t1, balance_slot(swapper())),
        ),
        (
            "pool.t0".into(),
            cache.cached_storage_value(t0, balance_slot(pool)),
        ),
        (
            "pool.t1".into(),
            cache.cached_storage_value(t1, balance_slot(pool)),
        ),
        (
            "pool.slot0".into(),
            cache.cached_storage_value(pool, V3_SLOT0_SLOT),
        ),
        (
            "pool.liquidity".into(),
            cache.cached_storage_value(pool, V3_LIQUIDITY_SLOT),
        ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn event_processor_reproduces_ground_truth_swap() -> Result<()> {
    // 1. Ground-truth cache: build state, snapshot the pre-swap state, execute the
    //    real swap, capture logs + the post-swap state.
    let mut truth = setup_cache().await?;
    let (token0, token1, pool) = build_state(&mut truth)?;
    let pre_swap = observe(&truth, token0, token1, pool);

    let swap_data = Pool::swapCall {
        zeroForOne: true,
        amountIn: U256::from(AMOUNT_IN),
        amountOut: U256::from(AMOUNT_OUT),
        newSqrtPriceX96: U160::MAX, // full-width: stresses the [0,160) boundary
        newTick: I24::try_from(NEW_TICK).unwrap(),
        newLiquidity: NEW_LIQUIDITY,
    }
    .abi_encode();

    let logs: Vec<Log> = match truth.call_raw(swapper(), pool, Bytes::from(swap_data), true)? {
        ExecutionResult::Success { logs, .. } => logs,
        other => return Err(anyhow!("ground-truth swap failed: {other:?}")),
    };
    // Two ERC-20 Transfers (token0 in, token1 out) + the canonical Swap.
    assert_eq!(logs.len(), 3, "expected 2 Transfer logs + 1 Swap log");

    // 2. Event-driven cache: identical pre-swap state, addresses match.
    let mut driven = setup_cache().await?;
    let (token0_d, token1_d, pool_d) = build_state(&mut driven)?;
    assert_eq!(
        (token0, token1, pool),
        (token0_d, token1_d, pool_d),
        "deterministic deploy addresses must match across caches"
    );

    // Pre-swap, the freshly-built driven cache equals truth's pre-swap snapshot
    // (sanity: the deterministic setup really is identical).
    assert_eq!(observe(&driven, token0, token1, pool), pre_swap);

    // 3. Feed ONLY the swap's logs into the event pipeline.
    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(Erc20TransferDecoder::new(U256::from(
        BALANCE_SLOT,
    ))));
    registry.register(Arc::new(
        UniswapV3Decoder::new().with_pool(pool, UniswapV3Layout::uniswap(60)),
    ));
    let mut pipeline = EventPipeline::new(registry);
    let digest = pipeline.ingest_logs(&mut driven, 1, &logs);

    // All three logs decoded to applied changes; nothing skipped (hot state).
    assert_eq!(digest.decoded_logs, 3, "all 3 logs should decode");
    assert!(
        !digest.applied.has_skipped(),
        "no cold skips: {:?}",
        digest.applied.skipped_masks
    );

    // 4. The decisive check: event-driven state == ground-truth state, field by field.
    let truth_state = observe(&truth, token0, token1, pool);
    let driven_state = observe(&driven, token0, token1, pool);
    assert_eq!(
        driven_state, truth_state,
        "event-driven state must match the ground-truth EVM execution"
    );

    // Spell out the headline invariants explicitly (defensive, human-readable).
    let slot0_truth = truth.cached_storage_value(pool, V3_SLOT0_SLOT).unwrap();
    let slot0_driven = driven.cached_storage_value(pool, V3_SLOT0_SLOT).unwrap();
    assert_eq!(
        slot0_driven, slot0_truth,
        "packed slot0 (price/tick) must match bit-for-bit"
    );
    // The price actually moved, and the observation/unlocked bits survived.
    assert_ne!(slot0_driven, U256::from(INIT_SQRT_PRICE), "slot0 changed");
    assert_eq!(
        (slot0_driven >> 240) & U256::from(1),
        U256::from(1),
        "unlocked bit preserved"
    );
    assert_eq!(
        (slot0_driven >> 184) & U256::from(0xFFFF),
        U256::from(INIT_OBS_INDEX),
        "obs index preserved"
    );

    // Balances match the ground truth (token0 in, token1 out).
    assert_eq!(
        driven
            .cached_storage_value(token0, balance_slot(swapper()))
            .unwrap(),
        U256::from(SWAPPER_TOKEN0 - AMOUNT_IN),
    );
    assert_eq!(
        driven
            .cached_storage_value(token1, balance_slot(swapper()))
            .unwrap(),
        U256::from(AMOUNT_OUT),
    );

    Ok(())
}
