//! Offline acceptance tests for the Phase 4 event pipeline (Pillar B.2).
//!
//! These are the **contract** the implementation must satisfy: the
//! `EventDecoder` / `StateView` traits, the `DecoderRegistry`, the ERC-20
//! `Transfer` decoder, the UniswapV3 `Swap`/`Mint`/`Burn` adapter, and the
//! `EventPipeline` (`ingest_logs` / `reorg_to` / `reconcile`). Everything runs
//! fully offline (mocked provider, state injected directly, logs built in
//! memory), so no test reaches the network.
//!
//! Layering vocabulary mirrors `tests/state_update.rs`:
//! - **layer 1 / overlay** = the CacheDB overlay (`db_mut().cache.accounts`).
//! - **layer 2 / backend** = the BlockchainDb backend (`blockchain_db()`).

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, Log, U256, keccak256};
use anyhow::Result;

use common::{install_mock_erc20, setup_cache, stub_fetcher};
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::events::erc20::Erc20TransferDecoder;
use evm_fork_cache::events::{
    DecoderRegistry, EventDecoder, EventPipeline, ReorgConfig, StateView,
};
use evm_fork_cache::{PurgeScope, SlotDelta, StateUpdate};

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

/// Hashed storage slot of a `mapping(address => uint256)` at `mapping_slot`.
fn mapping_slot(owner: Address, mapping_slot: u64) -> U256 {
    use alloy_sol_types::SolValue;
    let key = keccak256((owner, U256::from(mapping_slot)).abi_encode());
    U256::from_be_bytes(key.0)
}

/// Value of a slot in the BlockchainDb backend (layer 2) only.
fn backend_slot(cache: &EvmCache, addr: Address, slot: U256) -> Option<U256> {
    cache
        .blockchain_db()
        .storage()
        .read()
        .get(&addr)
        .and_then(|s| s.get(&slot).copied())
}

/// A read-only [`StateView`] stub backed by a fixed map (for pure decoder unit
/// tests that do not need a real cache).
struct StubView(HashMap<(Address, U256), U256>);
impl StateView for StubView {
    fn storage(&self, address: Address, slot: U256) -> Option<U256> {
        self.0.get(&(address, slot)).copied()
    }
}
fn empty_view() -> StubView {
    StubView(HashMap::new())
}

/// A test-only decoder that emits a single absolute `Slot` write for every log
/// (used to exercise pipeline mechanics independent of any real protocol).
struct MarkDecoder {
    slot: U256,
    value: U256,
}
impl EventDecoder for MarkDecoder {
    fn decode(&self, log: &Log, _view: &dyn StateView) -> Vec<StateUpdate> {
        vec![StateUpdate::slot(log.address, self.slot, self.value)]
    }
}

/// A test-only decoder that fires only for logs whose first topic equals `topic`.
struct TaggedDecoder {
    topic: alloy_primitives::B256,
    slot: U256,
    value: U256,
}
impl EventDecoder for TaggedDecoder {
    fn decode(&self, log: &Log, _view: &dyn StateView) -> Vec<StateUpdate> {
        if log.topics().first() == Some(&self.topic) {
            vec![StateUpdate::slot(log.address, self.slot, self.value)]
        } else {
            vec![]
        }
    }
}

/// Build a bare log at `address` with the given topics and empty data.
fn bare_log(address: Address, topics: Vec<alloy_primitives::B256>) -> Log {
    Log::new_unchecked(address, topics, Bytes::new())
}

// ===========================================================================
// EventDecoder / DecoderRegistry — dispatch.
// ===========================================================================

#[test]
fn registry_dispatches_address_scoped_then_global() {
    let token_a = Address::repeat_byte(0x0a);
    let token_b = Address::repeat_byte(0x0b);

    let mut registry = DecoderRegistry::new();
    // Address-scoped: only fires for token_a logs.
    registry.register_for_address(
        token_a,
        Arc::new(MarkDecoder {
            slot: U256::from(1),
            value: U256::from(11),
        }),
    );
    // Global: fires for every log.
    registry.register(Arc::new(MarkDecoder {
        slot: U256::from(9),
        value: U256::from(99),
    }));

    let view = empty_view();

    // A token_a log hits both the scoped decoder and the global one.
    let updates_a = registry.decode(&bare_log(token_a, vec![]), &view);
    assert_eq!(updates_a.len(), 2);
    assert!(updates_a.contains(&StateUpdate::slot(token_a, U256::from(1), U256::from(11))));
    assert!(updates_a.contains(&StateUpdate::slot(token_a, U256::from(9), U256::from(99))));

    // A token_b log hits only the global decoder.
    let updates_b = registry.decode(&bare_log(token_b, vec![]), &view);
    assert_eq!(
        updates_b,
        vec![StateUpdate::slot(token_b, U256::from(9), U256::from(99))]
    );
}

#[test]
fn decoder_returns_empty_for_unrecognized_log() {
    let topic = keccak256(b"SomethingElse()");
    let decoder = TaggedDecoder {
        topic: keccak256(b"Wanted()"),
        slot: U256::from(0),
        value: U256::from(1),
    };
    let view = empty_view();
    let log = bare_log(Address::repeat_byte(0x01), vec![topic]);
    assert!(decoder.decode(&log, &view).is_empty());
}

// ===========================================================================
// ERC-20 Transfer decoder.
// ===========================================================================

/// Build an ERC-20 `Transfer(from, to, value)` log.
fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
    let sig = keccak256(b"Transfer(address,address,uint256)");
    let topics = vec![sig, from.into_word(), to.into_word()];
    Log::new_unchecked(
        token,
        topics,
        Bytes::copy_from_slice(&value.to_be_bytes::<32>()),
    )
}

#[test]
fn erc20_transfer_decodes_to_sub_and_add_deltas() {
    let token = Address::repeat_byte(0x20);
    let from = Address::repeat_byte(0x21);
    let to = Address::repeat_byte(0x22);

    let decoder = Erc20TransferDecoder::new(U256::from(3)); // default balance slot 3
    let view = empty_view();
    let updates = decoder.decode(&transfer_log(token, from, to, U256::from(100)), &view);

    assert_eq!(
        updates,
        vec![
            StateUpdate::slot_delta(
                token,
                mapping_slot(from, 3),
                SlotDelta::Sub(U256::from(100))
            ),
            StateUpdate::slot_delta(token, mapping_slot(to, 3), SlotDelta::Add(U256::from(100))),
        ]
    );
}

#[test]
fn erc20_mint_skips_zero_from_and_burn_skips_zero_to() {
    let token = Address::repeat_byte(0x23);
    let holder = Address::repeat_byte(0x24);
    let decoder = Erc20TransferDecoder::new(U256::from(3));
    let view = empty_view();

    // Mint: from == ZERO → only the Add leg.
    let mint = decoder.decode(
        &transfer_log(token, Address::ZERO, holder, U256::from(7)),
        &view,
    );
    assert_eq!(
        mint,
        vec![StateUpdate::slot_delta(
            token,
            mapping_slot(holder, 3),
            SlotDelta::Add(U256::from(7))
        )]
    );

    // Burn: to == ZERO → only the Sub leg.
    let burn = decoder.decode(
        &transfer_log(token, holder, Address::ZERO, U256::from(7)),
        &view,
    );
    assert_eq!(
        burn,
        vec![StateUpdate::slot_delta(
            token,
            mapping_slot(holder, 3),
            SlotDelta::Sub(U256::from(7))
        )]
    );
}

#[test]
fn erc20_uses_per_token_slot_override_else_default() {
    let token_default = Address::repeat_byte(0x25);
    let token_custom = Address::repeat_byte(0x26);
    let holder = Address::repeat_byte(0x27);

    let decoder = Erc20TransferDecoder::new(U256::from(3)).with_token(token_custom, U256::from(9));
    let view = empty_view();

    let d = decoder.decode(
        &transfer_log(token_default, Address::ZERO, holder, U256::from(1)),
        &view,
    );
    assert_eq!(
        d[0],
        StateUpdate::slot_delta(
            token_default,
            mapping_slot(holder, 3),
            SlotDelta::Add(U256::from(1))
        )
    );

    let c = decoder.decode(
        &transfer_log(token_custom, Address::ZERO, holder, U256::from(1)),
        &view,
    );
    assert_eq!(
        c[0],
        StateUpdate::slot_delta(
            token_custom,
            mapping_slot(holder, 9),
            SlotDelta::Add(U256::from(1))
        )
    );
}

#[test]
fn erc20_non_transfer_log_decodes_to_empty() {
    let decoder = Erc20TransferDecoder::new(U256::from(3));
    let view = empty_view();
    // Wrong topic0.
    let log = bare_log(
        Address::repeat_byte(0x28),
        vec![keccak256(b"Approval(address,address,uint256)")],
    );
    assert!(decoder.decode(&log, &view).is_empty());
}

#[tokio::test]
async fn erc20_ingest_updates_balances_and_conserves() -> Result<()> {
    use common::{balance_of, install_default_account};

    let token = Address::repeat_byte(0x2a);
    let alice = Address::repeat_byte(0x2b);
    let bob = Address::repeat_byte(0x2c);

    let mut cache = setup_cache().await?;
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, alice);
    install_default_account(&mut cache, bob);
    install_mock_erc20(&mut cache, token);

    // Seed both holders' balance slots (overlay-resident, EVM-visible). Balance
    // mapping is slot 3 in the MockERC20 fixture.
    cache
        .db_mut()
        .insert_account_storage(token, mapping_slot(alice, 3), U256::from(1000))?;
    cache
        .db_mut()
        .insert_account_storage(token, mapping_slot(bob, 3), U256::from(500))?;

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(Erc20TransferDecoder::new(U256::from(3))));
    let mut pipeline = EventPipeline::new(registry);

    // Alice transfers 200 to Bob.
    let digest = pipeline.ingest_logs(
        &mut cache,
        100,
        &[transfer_log(token, alice, bob, U256::from(200))],
    );

    // Two slot changes applied; nothing skipped.
    assert_eq!(digest.block, 100);
    assert_eq!(digest.applied.slots.len(), 2);
    assert!(!digest.applied.has_skipped());
    assert_eq!(digest.decoded_logs, 1);

    // Balances move by the delta — assert via real SLOAD (balanceOf).
    assert_eq!(balance_of(&mut cache, token, alice)?, U256::from(800));
    assert_eq!(balance_of(&mut cache, token, bob)?, U256::from(700));
    // Conservation.
    assert_eq!(
        balance_of(&mut cache, token, alice)? + balance_of(&mut cache, token, bob)?,
        U256::from(1500)
    );
    Ok(())
}

#[tokio::test]
async fn erc20_cold_balance_transfer_is_skipped_and_surfaced() -> Result<()> {
    // A normal forked account (NOT StorageCleared): an unseeded balance slot is
    // cold, so the Sub/Add delta is skipped and surfaced.
    let token = Address::repeat_byte(0x2d);
    let alice = Address::repeat_byte(0x2e);
    let bob = Address::repeat_byte(0x2f);

    let mut cache = setup_cache().await?;

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(Erc20TransferDecoder::new(U256::from(3))));
    let mut pipeline = EventPipeline::new(registry);

    let digest = pipeline.ingest_logs(
        &mut cache,
        1,
        &[transfer_log(token, alice, bob, U256::from(10))],
    );

    // Both legs cold → no slot changes, two surfaced skips.
    assert!(digest.applied.slots.is_empty());
    assert!(digest.applied.has_skipped());
    assert_eq!(digest.applied.skipped.len(), 2);
    Ok(())
}

// ===========================================================================
// EventPipeline — reorg + reconcile (decoder-agnostic mechanics).
// ===========================================================================

#[tokio::test]
async fn ingest_records_touched_slots() -> Result<()> {
    let token = Address::repeat_byte(0x30);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(MarkDecoder {
        slot: U256::from(5),
        value: U256::from(42),
    }));
    let mut pipeline = EventPipeline::new(registry);

    let digest = pipeline.ingest_logs(&mut cache, 10, &[bare_log(token, vec![])]);
    assert!(digest.touched_slots.contains(&(token, U256::from(5))));
    assert_eq!(
        cache.cached_storage_value(token, U256::from(5)),
        Some(U256::from(42))
    );
    Ok(())
}

#[tokio::test]
async fn reorg_to_purges_addresses_touched_after_head() -> Result<()> {
    let token_a = Address::repeat_byte(0x31); // block 10 (survives)
    let token_b = Address::repeat_byte(0x32); // block 11 (purged)
    let token_c = Address::repeat_byte(0x33); // block 12 (purged)
    let slot = U256::from(0);

    let mut cache = setup_cache().await?;
    for t in [token_a, token_b, token_c] {
        install_mock_erc20(&mut cache, t);
    }

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(MarkDecoder {
        slot,
        value: U256::from(77),
    }));
    let mut pipeline = EventPipeline::new(registry);

    pipeline.ingest_logs(&mut cache, 10, &[bare_log(token_a, vec![])]);
    pipeline.ingest_logs(&mut cache, 11, &[bare_log(token_b, vec![])]);
    pipeline.ingest_logs(&mut cache, 12, &[bare_log(token_c, vec![])]);

    // Everything written.
    assert_eq!(backend_slot(&cache, token_a, slot), Some(U256::from(77)));
    assert_eq!(backend_slot(&cache, token_b, slot), Some(U256::from(77)));
    assert_eq!(backend_slot(&cache, token_c, slot), Some(U256::from(77)));

    // Reorg back to block 10: purge B and C (touched after 10), keep A.
    let diff = pipeline.reorg_to(&mut cache, 10);

    assert_eq!(
        backend_slot(&cache, token_a, slot),
        Some(U256::from(77)),
        "A untouched"
    );
    assert_eq!(
        backend_slot(&cache, token_b, slot),
        None,
        "B storage purged"
    );
    assert_eq!(
        backend_slot(&cache, token_c, slot),
        None,
        "C storage purged"
    );

    // The returned diff records the purges (B and C only).
    let purged: Vec<Address> = diff.purged.iter().map(|r| r.address).collect();
    assert!(purged.contains(&token_b) && purged.contains(&token_c));
    assert!(!purged.contains(&token_a));
    Ok(())
}

#[tokio::test]
async fn reorg_config_account_scope_fully_drops_account() -> Result<()> {
    let token = Address::repeat_byte(0x34);
    let slot = U256::from(0);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(MarkDecoder {
        slot,
        value: U256::from(5),
    }));
    let mut pipeline = EventPipeline::new(registry).with_reorg_config(ReorgConfig {
        depth: 64,
        scope: PurgeScope::Account,
    });

    pipeline.ingest_logs(&mut cache, 20, &[bare_log(token, vec![])]);
    let diff = pipeline.reorg_to(&mut cache, 19);

    assert!(
        diff.purged
            .iter()
            .any(|r| r.address == token && r.account_removed)
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_reports_mismatch_and_corrects() -> Result<()> {
    let token = Address::repeat_byte(0x35);
    let slot = U256::from(0);

    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);

    // Event pipeline writes an (incorrect) value 50.
    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(MarkDecoder {
        slot,
        value: U256::from(50),
    }));
    let mut pipeline = EventPipeline::new(registry);
    pipeline.ingest_logs(&mut cache, 1, &[bare_log(token, vec![])]);
    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::from(50))
    );

    // Chain truth is 100. Reconcile must surface the drift AND correct the cache.
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, slot),
        U256::from(100),
    )])));
    let report = pipeline.reconcile(&mut cache, &[(token, slot)])?;

    assert_eq!(report.checked, 1);
    assert_eq!(report.mismatched.len(), 1);
    assert_eq!(report.mismatched[0].old, U256::from(50));
    assert_eq!(report.mismatched[0].new, U256::from(100));
    // Cache corrected to chain truth.
    assert_eq!(
        cache.cached_storage_value(token, slot),
        Some(U256::from(100))
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_empty_when_event_state_matches_chain() -> Result<()> {
    let token = Address::repeat_byte(0x36);
    let slot = U256::from(0);

    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(MarkDecoder {
        slot,
        value: U256::from(100),
    }));
    let mut pipeline = EventPipeline::new(registry);
    pipeline.ingest_logs(&mut cache, 1, &[bare_log(token, vec![])]);

    // Chain agrees (100).
    cache.set_storage_batch_fetcher(stub_fetcher(HashMap::from([(
        (token, slot),
        U256::from(100),
    )])));
    let report = pipeline.reconcile(&mut cache, &[(token, slot)])?;
    assert!(report.mismatched.is_empty());
    Ok(())
}

#[tokio::test]
async fn reconcile_errors_without_fetcher() -> Result<()> {
    let mut cache = setup_cache().await?;
    let registry = DecoderRegistry::new();
    let mut pipeline = EventPipeline::new(registry);
    let token = Address::repeat_byte(0x37);
    assert!(
        pipeline
            .reconcile(&mut cache, &[(token, U256::from(0))])
            .is_err()
    );
    Ok(())
}

// ===========================================================================
// UniswapV3 adapter (protocols-gated).
// ===========================================================================

#[cfg(feature = "protocols")]
mod uniswap_v3 {
    use super::*;
    use alloy_primitives::I256;
    use alloy_primitives::aliases::{I24, U160};
    use alloy_sol_types::{SolEvent, sol};
    use evm_fork_cache::cache::{
        V3_LIQUIDITY_SLOT, V3_SLOT0_SLOT, V3_TICK_BITMAP_BASE_SLOT, V3_TICKS_BASE_SLOT,
        v3_tick_bitmap_storage_key_with_base, v3_tick_info_storage_keys_with_base,
    };
    use evm_fork_cache::events::uniswap_v3::{UniswapV3Decoder, UniswapV3Layout};

    sol! {
        event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick);
        event Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1);
        event Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1);
    }

    fn swap_log(pool: Address, sqrt_price: u128, liquidity: u128, tick: i32) -> Log {
        let ev = Swap {
            sender: Address::repeat_byte(0x01),
            recipient: Address::repeat_byte(0x02),
            amount0: I256::try_from(-1i64).unwrap(),
            amount1: I256::try_from(1i64).unwrap(),
            sqrtPriceX96: U160::from(sqrt_price),
            liquidity,
            tick: I24::try_from(tick).unwrap(),
        };
        Log {
            address: pool,
            data: ev.encode_log_data(),
        }
    }

    fn mint_log(pool: Address, tick_lower: i32, tick_upper: i32, amount: u128) -> Log {
        let ev = Mint {
            sender: Address::repeat_byte(0x03),
            owner: Address::repeat_byte(0x04),
            tickLower: I24::try_from(tick_lower).unwrap(),
            tickUpper: I24::try_from(tick_upper).unwrap(),
            amount,
            amount0: U256::from(1),
            amount1: U256::from(1),
        };
        Log {
            address: pool,
            data: ev.encode_log_data(),
        }
    }

    fn burn_log(pool: Address, tick_lower: i32, tick_upper: i32, amount: u128) -> Log {
        let ev = Burn {
            owner: Address::repeat_byte(0x04),
            tickLower: I24::try_from(tick_lower).unwrap(),
            tickUpper: I24::try_from(tick_upper).unwrap(),
            amount,
            amount0: U256::from(1),
            amount1: U256::from(1),
        };
        Log {
            address: pool,
            data: ev.encode_log_data(),
        }
    }

    /// Pack a slot0 word: sqrtPriceX96 [0,160), tick [160,184) (int24), and an
    /// arbitrary `high` block of preserved bits at [184,256).
    fn pack_slot0(sqrt_price: u128, tick: i32, high: U256) -> U256 {
        let tick24 = U256::from((tick as u32) & 0x00FF_FFFF);
        U256::from(sqrt_price) | (tick24 << 160) | (high << 184)
    }

    fn tick_word(gross: u128, net: i128) -> U256 {
        U256::from(gross) | (U256::from(net as u128) << 128)
    }
    fn unpack_tick_word(w: U256) -> (u128, i128) {
        let gross = u128::try_from(w & U256::from(u128::MAX)).unwrap();
        let net = u128::try_from((w >> 128) & U256::from(u128::MAX)).unwrap() as i128;
        (gross, net)
    }

    fn pool_with_decoder(tick_spacing: i32) -> (Address, UniswapV3Decoder) {
        let pool = Address::repeat_byte(0x40);
        let decoder =
            UniswapV3Decoder::new().with_pool(pool, UniswapV3Layout::uniswap(tick_spacing));
        (pool, decoder)
    }

    #[tokio::test]
    async fn v3_swap_sets_price_and_tick_preserving_unlocked() -> Result<()> {
        let (pool, decoder) = pool_with_decoder(1);
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, pool);

        // Seed slot0 with old price/tick AND the unlocked bit (240) + a nonzero
        // observation index (bits 184+). high = unlocked(bit 56 of high) | obs(=7).
        let high = (U256::from(1) << 56) | U256::from(7);
        let seeded = pack_slot0(1_000_000, 50, high);
        cache
            .db_mut()
            .insert_account_storage(pool, V3_SLOT0_SLOT, seeded)?;

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        pipeline.ingest_logs(&mut cache, 1, &[swap_log(pool, 2_000_000, 9999, 75)]);

        let result = cache.cached_storage_value(pool, V3_SLOT0_SLOT).unwrap();
        // Low 184 bits are the new price + tick.
        let low_mask = (U256::from(1) << 184) - U256::from(1);
        let expected_low = U256::from(2_000_000u64) | (U256::from(75u64) << 160);
        assert_eq!(result & low_mask, expected_low, "price+tick updated");
        // High bits (incl. unlocked) preserved.
        assert_eq!(result >> 184, high, "observation/unlocked bits preserved");
        Ok(())
    }

    #[tokio::test]
    async fn v3_swap_sets_liquidity_absolute() -> Result<()> {
        let (pool, decoder) = pool_with_decoder(1);
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, pool);
        cache.db_mut().insert_account_storage(
            pool,
            V3_SLOT0_SLOT,
            pack_slot0(1, 0, U256::from(1) << 56),
        )?;

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        pipeline.ingest_logs(&mut cache, 1, &[swap_log(pool, 1, 123_456, 0)]);
        assert_eq!(
            cache.cached_storage_value(pool, V3_LIQUIDITY_SLOT),
            Some(U256::from(123_456u64))
        );
        Ok(())
    }

    #[tokio::test]
    async fn v3_swap_cold_slot0_is_skipped() -> Result<()> {
        // Fresh pool with no account → slot0 is cold.
        let pool = Address::repeat_byte(0x41);
        let decoder = UniswapV3Decoder::new().with_pool(pool, UniswapV3Layout::uniswap(1));
        let mut cache = setup_cache().await?;

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        let digest = pipeline.ingest_logs(&mut cache, 1, &[swap_log(pool, 5, 5, 5)]);
        // slot0 masked write skipped (un-masked bits unknown).
        assert!(digest.applied.has_skipped());
        assert_eq!(cache.cached_storage_value(pool, V3_SLOT0_SLOT), None);
        Ok(())
    }

    #[tokio::test]
    async fn v3_mint_increments_gross_and_net_with_correct_signs() -> Result<()> {
        let (pool, decoder) = pool_with_decoder(1);
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, pool); // StorageCleared → unseeded ticks read 0 (hot)

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        let (lo, hi, amount) = (10i32, 20i32, 500u128);
        pipeline.ingest_logs(&mut cache, 1, &[mint_log(pool, lo, hi, amount)]);

        let lo_key = v3_tick_info_storage_keys_with_base(lo, V3_TICKS_BASE_SLOT)[0];
        let hi_key = v3_tick_info_storage_keys_with_base(hi, V3_TICKS_BASE_SLOT)[0];
        let (lo_gross, lo_net) =
            unpack_tick_word(cache.cached_storage_value(pool, lo_key).unwrap());
        let (hi_gross, hi_net) =
            unpack_tick_word(cache.cached_storage_value(pool, hi_key).unwrap());

        assert_eq!(lo_gross, 500);
        assert_eq!(lo_net, 500, "lower tick: net += amount");
        assert_eq!(hi_gross, 500);
        assert_eq!(hi_net, -500, "upper tick: net -= amount");
        Ok(())
    }

    #[tokio::test]
    async fn v3_mint_initializes_tick_and_flips_bitmap() -> Result<()> {
        let (pool, decoder) = pool_with_decoder(1);
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, pool);

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        let (lo, hi) = (10i32, 20i32);
        pipeline.ingest_logs(&mut cache, 1, &[mint_log(pool, lo, hi, 500)]);

        // initialized flag (slot +3, bit 248) set for both ticks.
        let lo3 = v3_tick_info_storage_keys_with_base(lo, V3_TICKS_BASE_SLOT)[3];
        let hi3 = v3_tick_info_storage_keys_with_base(hi, V3_TICKS_BASE_SLOT)[3];
        let init_bit = U256::from(1) << 248;
        assert_eq!(
            cache.cached_storage_value(pool, lo3).unwrap() & init_bit,
            init_bit
        );
        assert_eq!(
            cache.cached_storage_value(pool, hi3).unwrap() & init_bit,
            init_bit
        );

        // bitmap word 0 (ticks 10 & 20 with tick_spacing 1 → word 0, bits 10 & 20).
        let word_key = v3_tick_bitmap_storage_key_with_base(0, V3_TICK_BITMAP_BASE_SLOT);
        let bitmap = cache.cached_storage_value(pool, word_key).unwrap();
        assert_eq!(bitmap & (U256::from(1) << 10), U256::from(1) << 10);
        assert_eq!(bitmap & (U256::from(1) << 20), U256::from(1) << 20);
        Ok(())
    }

    #[tokio::test]
    async fn v3_burn_to_zero_uninitializes_and_clears_bitmap() -> Result<()> {
        let (pool, decoder) = pool_with_decoder(1);
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, pool);

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        let (lo, hi) = (10i32, 20i32);
        // Same-block Mint then Burn of the full amount: the Burn decode sees the
        // Mint's applied gross/net via the StateView, returning the tick to 0.
        pipeline.ingest_logs(
            &mut cache,
            1,
            &[mint_log(pool, lo, hi, 500), burn_log(pool, lo, hi, 500)],
        );

        let lo_key = v3_tick_info_storage_keys_with_base(lo, V3_TICKS_BASE_SLOT)[0];
        let (lo_gross, lo_net) =
            unpack_tick_word(cache.cached_storage_value(pool, lo_key).unwrap());
        assert_eq!(lo_gross, 0, "gross back to zero");
        assert_eq!(lo_net, 0, "net back to zero");

        // initialized flag cleared and bitmap bit cleared.
        let lo3 = v3_tick_info_storage_keys_with_base(lo, V3_TICKS_BASE_SLOT)[3];
        let init_bit = U256::from(1) << 248;
        assert_eq!(
            cache.cached_storage_value(pool, lo3).unwrap_or(U256::ZERO) & init_bit,
            U256::ZERO
        );
        let word_key = v3_tick_bitmap_storage_key_with_base(0, V3_TICK_BITMAP_BASE_SLOT);
        assert_eq!(
            cache
                .cached_storage_value(pool, word_key)
                .unwrap_or(U256::ZERO)
                & (U256::from(1) << 10),
            U256::ZERO
        );
        Ok(())
    }

    #[tokio::test]
    async fn v3_mint_updates_global_liquidity_when_in_range() -> Result<()> {
        let (pool, decoder) = pool_with_decoder(1);
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, pool);

        // Current tick 15 is within [10, 20); liquidity seeded to 1000.
        cache.db_mut().insert_account_storage(
            pool,
            V3_SLOT0_SLOT,
            pack_slot0(1_000_000, 15, U256::from(1) << 56),
        )?;
        cache
            .db_mut()
            .insert_account_storage(pool, V3_LIQUIDITY_SLOT, U256::from(1000))?;

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        pipeline.ingest_logs(&mut cache, 1, &[mint_log(pool, 10, 20, 500)]);
        assert_eq!(
            cache.cached_storage_value(pool, V3_LIQUIDITY_SLOT),
            Some(U256::from(1500)),
            "in-range mint adds to global liquidity"
        );
        Ok(())
    }

    #[tokio::test]
    async fn v3_mint_leaves_global_liquidity_when_out_of_range() -> Result<()> {
        let (pool, decoder) = pool_with_decoder(1);
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, pool);

        // Current tick 5 is BELOW [10, 20); liquidity must not change.
        cache.db_mut().insert_account_storage(
            pool,
            V3_SLOT0_SLOT,
            pack_slot0(1_000_000, 5, U256::from(1) << 56),
        )?;
        cache
            .db_mut()
            .insert_account_storage(pool, V3_LIQUIDITY_SLOT, U256::from(1000))?;

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        pipeline.ingest_logs(&mut cache, 1, &[mint_log(pool, 10, 20, 500)]);
        assert_eq!(
            cache.cached_storage_value(pool, V3_LIQUIDITY_SLOT),
            Some(U256::from(1000)),
            "out-of-range mint does not touch global liquidity"
        );
        Ok(())
    }

    #[tokio::test]
    async fn v3_mint_cold_tick_word_is_skipped() -> Result<()> {
        // Fresh pool, no account → tick words are cold (None), so the tick
        // maintenance is skipped and surfaced rather than computed against 0.
        let pool = Address::repeat_byte(0x42);
        let decoder = UniswapV3Decoder::new().with_pool(pool, UniswapV3Layout::uniswap(1));
        let mut cache = setup_cache().await?;

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        let digest = pipeline.ingest_logs(&mut cache, 1, &[mint_log(pool, 10, 20, 500)]);
        assert!(
            digest.applied.has_skipped(),
            "cold tick words surfaced as skips"
        );
        let lo_key = v3_tick_info_storage_keys_with_base(10, V3_TICKS_BASE_SLOT)[0];
        assert_eq!(
            cache.cached_storage_value(pool, lo_key),
            None,
            "nothing written"
        );
        Ok(())
    }

    #[tokio::test]
    async fn v3_unregistered_pool_decodes_to_nothing() -> Result<()> {
        let known = Address::repeat_byte(0x43);
        let unknown = Address::repeat_byte(0x44);
        let decoder = UniswapV3Decoder::new().with_pool(known, UniswapV3Layout::uniswap(1));
        let mut cache = setup_cache().await?;
        install_mock_erc20(&mut cache, unknown);

        let mut registry = DecoderRegistry::new();
        registry.register(Arc::new(decoder));
        let mut pipeline = EventPipeline::new(registry);

        let digest = pipeline.ingest_logs(&mut cache, 1, &[swap_log(unknown, 5, 5, 5)]);
        assert!(digest.applied.is_empty() && !digest.applied.has_skipped());
        assert_eq!(digest.decoded_logs, 0);
        Ok(())
    }
}
