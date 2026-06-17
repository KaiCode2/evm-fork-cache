//! Reactive cache updates from the event stream (Pillar B.2).
//!
//! Decodes on-chain logs into the Phase 3 [`StateUpdate`] vocabulary and applies
//! them to a fork cache — keeping hot state fresh **without** an RPC round-trip
//! per change. It wires up the three pieces Phase 4 adds:
//!
//! 1. A [`DecoderRegistry`] with an [`Erc20TransferDecoder`] (balances) and a
//!    [`UniswapV3Decoder`] (a pool's `slot0` price/tick + `liquidity`).
//! 2. An [`EventPipeline`] whose `ingest_logs` decodes + applies a block's logs
//!    (log-by-log), surfacing a [`BlockDigest`].
//! 3. The reactive maintenance: the freshness wiring (pin event-derived slots so
//!    the optimistic validator does not re-verify them, then advance the block
//!    clock), a sampled **reconcile** drift alarm against a stub fetcher, and a
//!    **reorg** purge-and-resync.
//!
//! Runs fully offline against a mocked provider and in-memory logs — no network.
//! Requires the `protocols` feature (the UniswapV3 adapter), which is on by
//! default.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example reactive_cache
//! ```

#[cfg(feature = "protocols")]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    imp::run().await
}

#[cfg(not(feature = "protocols"))]
fn main() {
    eprintln!(
        "the `reactive_cache` example requires the `protocols` feature (the \
         UniswapV3 adapter). Run it with default features: \
         `cargo run --example reactive_cache`."
    );
}

#[cfg(feature = "protocols")]
#[path = "support/mock.rs"]
mod mock;

#[cfg(feature = "protocols")]
mod imp {
    use std::collections::HashMap;
    use std::sync::Arc;

    use alloy_eips::BlockId;
    use alloy_primitives::aliases::{I24, U160};
    use alloy_primitives::{Address, Bytes, I256, Log, U256, keccak256};
    use alloy_sol_types::{SolEvent, SolValue, sol};
    use anyhow::Result;
    use evm_fork_cache::cache::{StorageBatchFetchFn, V3_LIQUIDITY_SLOT, V3_SLOT0_SLOT};
    use evm_fork_cache::events::{DecoderRegistry, EventPipeline};
    use evm_fork_cache::freshness::{
        AlwaysVerify, FreshnessController, FreshnessRegistry, Validity,
    };
    use evm_fork_cache::{Erc20TransferDecoder, UniswapV3Decoder, UniswapV3Layout};

    use super::mock;

    sol! {
        event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick);
    }

    /// Hashed `balanceOf[owner]` slot for the MockERC20 fixture (mapping at slot 3).
    fn balance_slot(owner: Address) -> U256 {
        let key = keccak256((owner, U256::from(mock::MOCK_ERC20_BALANCE_SLOT)).abi_encode());
        U256::from_be_bytes(key.0)
    }

    /// Build an ERC-20 `Transfer(from, to, value)` log.
    fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
        let sig = keccak256(b"Transfer(address,address,uint256)");
        Log::new_unchecked(
            token,
            vec![sig, from.into_word(), to.into_word()],
            Bytes::copy_from_slice(&value.to_be_bytes::<32>()),
        )
    }

    /// Build a UniswapV3 `Swap` log carrying the post-swap price/liquidity/tick.
    fn swap_log(pool: Address, sqrt_price: u128, liquidity: u128, tick: i32) -> Log {
        let ev = Swap {
            sender: Address::repeat_byte(0x5e),
            recipient: Address::repeat_byte(0x5f),
            amount0: I256::try_from(-1_000i64).unwrap(),
            amount1: I256::try_from(1_000i64).unwrap(),
            sqrtPriceX96: U160::from(sqrt_price),
            liquidity,
            tick: I24::try_from(tick).unwrap(),
        };
        Log {
            address: pool,
            data: ev.encode_log_data(),
        }
    }

    /// Pack a slot0 word: sqrtPriceX96 [0,160), tick [160,184), `unlocked` at bit 240.
    fn pack_slot0(sqrt_price: u128, tick: i32) -> U256 {
        let tick24 = U256::from((tick as u32) & 0x00FF_FFFF);
        let unlocked = U256::from(1) << 240;
        U256::from(sqrt_price) | (tick24 << 160) | unlocked
    }

    pub async fn run() -> Result<()> {
        let mut cache = mock::offline_cache().await?;

        let token = Address::repeat_byte(0x11);
        let pool = Address::repeat_byte(0x99);
        let alice = Address::repeat_byte(0x22);
        let bob = Address::repeat_byte(0x33);
        mock::install_default_account(&mut cache, Address::ZERO);
        mock::install_default_account(&mut cache, alice);
        mock::install_default_account(&mut cache, bob);
        mock::install_mock_erc20(&mut cache, token);
        mock::install_mock_erc20(&mut cache, pool); // reuse as a storage-cleared pool

        // Seed the holders' balances (EVM-visible) and the pool's slot0 + liquidity.
        cache
            .db_mut()
            .insert_account_storage(token, balance_slot(alice), U256::from(1_000))?;
        cache
            .db_mut()
            .insert_account_storage(token, balance_slot(bob), U256::from(0))?;
        cache
            .db_mut()
            .insert_account_storage(pool, V3_SLOT0_SLOT, pack_slot0(1_000_000, 100))?;
        cache
            .db_mut()
            .insert_account_storage(pool, V3_LIQUIDITY_SLOT, U256::from(5_000))?;

        // 1. Build the decoder registry: ERC-20 balances + the V3 pool.
        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(Erc20TransferDecoder::new(U256::from(
            mock::MOCK_ERC20_BALANCE_SLOT,
        ))));
        registry.register(Arc::new(
            UniswapV3Decoder::new().with_pool(pool, UniswapV3Layout::uniswap(60)),
        ));
        let mut pipeline = EventPipeline::new(registry);

        // The freshness side: a controller whose registry we pin event-derived
        // slots into so the optimistic validator never re-verifies them by RPC.
        let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);

        // 2. Ingest block 100: Alice sends Bob 250 tokens, and the pool swaps (new
        //    price/tick + liquidity). Decoded + applied log-by-log.
        let block = 100u64;
        let digest = pipeline.ingest_logs(
            &mut cache,
            block,
            &[
                transfer_log(token, alice, bob, U256::from(250)),
                swap_log(pool, 2_000_000, 7_500, 120),
            ],
        );

        println!("=== ingested block {} ===", digest.block);
        println!(
            "  decoded {} log(s) -> {} slot change(s), {} skipped",
            digest.decoded_logs,
            digest.applied.slots.len(),
            digest.applied.skipped_len(),
        );
        println!(
            "  alice balance: {}  bob balance: {}",
            mock::balance_of(&mut cache, token, alice)?,
            mock::balance_of(&mut cache, token, bob)?,
        );
        println!(
            "  pool liquidity slot: {}",
            cache.cached_storage_value(pool, V3_LIQUIDITY_SLOT).unwrap()
        );
        // slot0: the new price/tick landed, and the `unlocked` bit (240) is
        // preserved by the masked write — a clobbered `unlocked` would make a
        // quote revert LOK.
        let slot0 = cache.cached_storage_value(pool, V3_SLOT0_SLOT).unwrap();
        println!(
            "  pool slot0 sqrtPriceX96 (low 160b): {}",
            slot0 & ((U256::from(1) << 160) - U256::from(1))
        );
        println!(
            "  pool slot0 unlocked bit preserved: {}",
            (slot0 >> 240) & U256::from(1) == U256::from(1)
        );

        // 3a. Freshness wiring: pin the touched slots (kept fresh out-of-band by
        //     the pipeline) and advance the block clock.
        for (addr, slot) in &digest.touched_slots {
            controller
                .registry_mut()
                .set_slot(*addr, *slot, Validity::Pinned);
        }
        controller.on_new_block(block);
        println!(
            "\npinned {} event-derived slot(s) into the freshness registry",
            digest.touched_slots.len()
        );

        // 3b. Sampled reconcile against chain truth. Stub the fetcher so the
        //     pool's liquidity reads 7_600 on-chain (a small drift from our
        //     event-derived 7_500): reconcile corrects the cache AND alarms.
        let fresh: HashMap<(Address, U256), U256> =
            HashMap::from([((pool, V3_LIQUIDITY_SLOT), U256::from(7_600))]);
        let fetcher: StorageBatchFetchFn = Arc::new(
            move |requests: Vec<(Address, U256)>, _block: Option<BlockId>| {
                requests
                    .into_iter()
                    .map(|(a, s)| (a, s, Ok(fresh.get(&(a, s)).copied().unwrap_or(U256::ZERO))))
                    .collect()
            },
        );
        cache.set_storage_batch_fetcher(fetcher);

        let report = pipeline.reconcile(&mut cache, &[(pool, V3_LIQUIDITY_SLOT)])?;
        println!("\n=== reconcile (sampled {} slot) ===", report.checked);
        if report.mismatched.is_empty() {
            println!("  no drift — event-derived state matches chain");
        } else {
            for c in &report.mismatched {
                println!(
                    "  DRIFT: {} slot {} : {} -> {} (corrected)",
                    c.address, c.slot, c.old, c.new
                );
            }
        }

        // 4. A reorg to block 99 purges everything block 100 touched, so the next
        //    read re-fetches from RPC (the caller re-ingests the canonical logs).
        let purge = pipeline.reorg_to(&mut cache, 99);
        println!("\n=== reorg to block 99 ===");
        println!(
            "  purged {} address(es); pool liquidity now re-reads cold/zero: {:?}",
            purge.purged.len(),
            cache.cached_storage_value(pool, V3_LIQUIDITY_SLOT),
        );

        Ok(())
    }
}
