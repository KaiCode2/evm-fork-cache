//! Reactive cache updates from the event stream (Pillar B.2).
//!
//! Decodes on-chain logs into the [`StateUpdate`](evm_fork_cache::StateUpdate)
//! vocabulary and applies them to a fork cache, keeping hot state fresh without
//! an RPC round-trip per change. This example wires up:
//!
//! 1. A [`DecoderRegistry`] with the built-in [`Erc20TransferDecoder`].
//! 2. An [`EventPipeline`] whose `ingest_logs` decodes and applies a block's logs.
//! 3. Freshness pinning, sampled reconcile against a stub fetcher, and reorg purge.
//!
//! Runs fully offline against a mocked provider and in-memory logs.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example reactive_cache
//! ```

#[path = "support/mock.rs"]
mod mock;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, Log, U256, keccak256};
use alloy_sol_types::SolValue;
use anyhow::Result;
use evm_fork_cache::Erc20TransferDecoder;
use evm_fork_cache::cache::StorageBatchFetchFn;
use evm_fork_cache::events::{DecoderRegistry, EventPipeline};
use evm_fork_cache::freshness::{AlwaysVerify, FreshnessController, FreshnessRegistry, Validity};

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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cache = mock::offline_cache().await?;

    let token = Address::repeat_byte(0x11);
    let alice = Address::repeat_byte(0x22);
    let bob = Address::repeat_byte(0x33);
    mock::install_default_account(&mut cache, Address::ZERO);
    mock::install_default_account(&mut cache, alice);
    mock::install_default_account(&mut cache, bob);
    mock::install_mock_erc20(&mut cache, token);

    let alice_slot = balance_slot(alice);
    let bob_slot = balance_slot(bob);

    cache
        .db_mut()
        .insert_account_storage(token, alice_slot, U256::from(1_000))?;
    cache
        .db_mut()
        .insert_account_storage(token, bob_slot, U256::from(0))?;

    let mut registry = DecoderRegistry::new();
    registry.register(Arc::new(Erc20TransferDecoder::new(U256::from(
        mock::MOCK_ERC20_BALANCE_SLOT,
    ))));
    let mut pipeline = EventPipeline::new(registry);

    let mut controller = FreshnessController::new(FreshnessRegistry::new(), AlwaysVerify);

    // Install a counting fetcher up front so we can show that *ingest* performs no
    // RPC reads (it decodes logs into writes), while *reconcile* below samples a
    // few slots on purpose. It returns the (deliberately drifted) chain value for
    // bob so the reconcile step has something to correct.
    let fetches = Arc::new(AtomicUsize::new(0));
    let counter = fetches.clone();
    let fresh: HashMap<(Address, U256), U256> =
        HashMap::from([((token, bob_slot), U256::from(260))]);
    let fetcher: StorageBatchFetchFn =
        Arc::new(move |requests: Vec<(Address, U256)>, _block: BlockId| {
            counter.fetch_add(requests.len(), Ordering::Relaxed);
            requests
                .into_iter()
                .map(|(a, s)| (a, s, Ok(fresh.get(&(a, s)).copied().unwrap_or(U256::ZERO))))
                .collect()
        });
    cache.set_storage_batch_fetcher(fetcher);

    let block = 100u64;
    let digest = pipeline.ingest_logs(
        &mut cache,
        block,
        &[transfer_log(token, alice, bob, U256::from(250))],
    );

    println!("=== ingested block {} ===", digest.block);
    println!(
        "  RPC slot fetches during ingest: {}  <- a poller would refetch {} touched slot(s)/block",
        fetches.load(Ordering::Relaxed),
        digest.touched_slots.len(),
    );
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

    fetches.store(0, Ordering::Relaxed); // measure reconcile's sampling cost
    let report = pipeline.reconcile(&mut cache, &[(token, bob_slot)])?;
    println!("\n=== reconcile (sampled {} slot) ===", report.checked);
    println!(
        "  RPC slot fetches during reconcile: {}  <- the sampled honesty backstop",
        fetches.load(Ordering::Relaxed),
    );
    if report.mismatched.is_empty() {
        println!("  no drift: event-derived state matches chain");
    } else {
        for c in &report.mismatched {
            println!(
                "  DRIFT: {} slot {} : {} -> {} (corrected)",
                c.address, c.slot, c.old, c.new
            );
        }
    }
    println!(
        "  bob balance after reconcile: {}",
        mock::balance_of(&mut cache, token, bob)?
    );

    // `EventPipeline::new` uses the default `ReorgConfig`, whose
    // `PurgeScope::AllStorage` clears the storage of every address touched after
    // the new head — so bob's balance slot reads back as uncached (`None`) and the
    // next access would re-fetch it from RPC.
    let purge = pipeline.reorg_to(&mut cache, 99);
    println!("\n=== reorg to block 99 (default scope: AllStorage) ===");
    println!(
        "  purged {} address(es); bob balance slot now cached as {:?}",
        purge.purged.len(),
        cache.cached_storage_value(token, bob_slot),
    );

    Ok(())
}
