//! Offline acceptance tests for the Phase 4 event pipeline (Pillar B.2).
//!
//! These are the **contract** the implementation must satisfy: the
//! `EventDecoder` / `StateView` traits, the `DecoderRegistry`, the built-in
//! ERC-20 `Transfer` decoder, and the `EventPipeline` (`ingest_logs` /
//! `reorg_to` / `reconcile`). Everything runs fully offline (mocked provider,
//! state injected directly, logs built in memory), so no test reaches the
//! network.
//!
//! Layering vocabulary mirrors `tests/state_update.rs`:
//! - **layer 1 / overlay** = the CacheDB overlay (`db_mut().cache.accounts`).
//! - **layer 2 / backend** = the BlockchainDb backend (`unchecked_blockchain_db()`).

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
        .unchecked_blockchain_db()
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

/// Pillar 3 (reactive sync): ingesting a block's `Transfer` logs keeps the hot
/// balance slots correct with **zero** RPC fetches — the decode→write pipeline
/// never touches the fetcher, unlike a poller that re-reads every changed slot
/// each block. Sampled `reconcile` (which *does* fetch) is the honesty backstop.
#[tokio::test(flavor = "multi_thread")]
async fn ingest_keeps_state_fresh_with_zero_fetches() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy_eips::BlockId;
    use common::{balance_of, install_default_account};
    use evm_fork_cache::cache::StorageBatchFetchFn;

    let token = Address::repeat_byte(0x33);
    let owners: Vec<Address> = (0..8u8).map(|i| Address::repeat_byte(0x40 + i)).collect();

    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, token);
    install_default_account(&mut cache, Address::ZERO); // fee beneficiary
    for o in &owners {
        // Install each owner as an account (so it can be the balanceOf caller
        // without a lazy RPC load) and seed its balance (layer 1, readable) so the
        // deltas apply hot.
        install_default_account(&mut cache, *o);
        cache
            .db_mut()
            .insert_account_storage(token, mapping_slot(*o, 3), U256::from(1_000))?;
    }

    // A counting fetcher: `ingest_logs` must never invoke it.
    let fetches = Arc::new(AtomicUsize::new(0));
    let counter = fetches.clone();
    let f: StorageBatchFetchFn = Arc::new(move |reqs: Vec<(Address, U256)>, _b: BlockId| {
        counter.fetch_add(reqs.len(), Ordering::Relaxed);
        reqs.into_iter()
            .map(|(a, s)| (a, s, Ok(U256::from(1_000))))
            .collect()
    });
    cache.set_storage_batch_fetcher(f);

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(Erc20TransferDecoder::new(U256::from(3))));
    let mut pipeline = EventPipeline::new(registry);

    // A block of transfers around a ring (each owner sends 10 and receives 10 →
    // balances net back to 1000), touching every owner's balance slot.
    let logs: Vec<Log> = (0..owners.len())
        .map(|i| {
            transfer_log(
                token,
                owners[i],
                owners[(i + 1) % owners.len()],
                U256::from(10),
            )
        })
        .collect();

    let digest = pipeline.ingest_logs(&mut cache, 100, &logs);

    assert_eq!(
        fetches.load(Ordering::Relaxed),
        0,
        "ingest decodes logs into writes and performs ZERO RPC fetches"
    );
    assert_eq!(digest.decoded_logs, owners.len());
    assert!(
        !digest.applied.has_skipped(),
        "all hot balance deltas applied"
    );
    // Correctness: state kept fresh from the logs alone equals the true post-state.
    for o in &owners {
        assert_eq!(balance_of(&mut cache, token, *o)?, U256::from(1_000));
    }
    Ok(())
}

/// WS-3 (manager-authored red-green): `derived_slots` must be bounded to the
/// reorg horizon (`ReorgConfig::depth`), mirroring the `touched` ring, rather
/// than growing unbounded across steady-state ingestion. With `depth = 3`,
/// after ingesting 6 blocks that each touch a distinct `(address, slot)`, only
/// the 3 most-recent blocks' derived slots may be retained; the aged-out ones
/// must be evicted.
#[tokio::test]
async fn derived_slots_is_bounded_to_reorg_horizon() -> Result<()> {
    let slot = U256::from(0);
    let tokens: Vec<Address> = (0u8..6).map(|i| Address::repeat_byte(0x50 + i)).collect();

    let mut cache = setup_cache().await?;
    for t in &tokens {
        install_mock_erc20(&mut cache, *t);
    }

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(MarkDecoder {
        slot,
        value: U256::from(77),
    }));
    let mut pipeline = EventPipeline::new(registry).with_reorg_config(ReorgConfig {
        depth: 3,
        scope: PurgeScope::AllStorage,
    });

    for (i, t) in tokens.iter().enumerate() {
        pipeline.ingest_logs(&mut cache, 100 + i as u64, &[bare_log(*t, vec![])]);
    }

    let derived: std::collections::HashSet<(Address, U256)> = pipeline.derived_slots().collect();

    assert_eq!(
        derived.len(),
        3,
        "derived_slots must be bounded to the reorg horizon (depth = 3), was {}",
        derived.len()
    );
    for t in &tokens[3..] {
        assert!(
            derived.contains(&(*t, slot)),
            "recent block's derived slot must be retained"
        );
    }
    for t in &tokens[..3] {
        assert!(
            !derived.contains(&(*t, slot)),
            "aged-out derived slot must be evicted from the horizon"
        );
    }
    Ok(())
}
