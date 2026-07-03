//! **Pillar 2 — data-fetch minimization (the headline number).**
//!
//! A searcher evaluating N candidate transactions against the *same* recent block
//! reads the same hot working set (a pool's slots, a handful of token balances)
//! over and over. The naive loop — a fresh fork/cache per candidate — cold-fetches
//! that working set *every* candidate. `evm-fork-cache` fetches it **once**, freezes
//! it into a snapshot, and fans every candidate out over cheap in-memory overlays
//! that hold no RPC backend, so the fan-out adds **zero** further fetches.
//!
//! This example counts real fetcher invocations (an `AtomicUsize`-wrapped
//! [`StorageBatchFetchFn`]) and prints the exact integers. The count is
//! deterministic and machine-independent — it is a literal tally of RPC reads
//! avoided, not a wall-clock measurement.
//!
//! Run with: `cargo run --example fetch_minimization_counted`

#[path = "support/mock.rs"]
mod mock;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};
use anyhow::Result;
use evm_fork_cache::cache::{EvmCache, EvmOverlay, StorageBatchFetchFn};
use revm::context::result::ExecutionResult;
use revm::state::AccountInfo;

/// Candidate transactions evaluated against the head this block.
const N_CANDIDATES: usize = 500;
/// Distinct storage slots in the shared hot working set (token balance slots).
const WORKING_SET: usize = 8;

/// An owner address derived from an index.
fn owner(i: usize) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..20].copy_from_slice(&(i as u64 + 1).to_be_bytes());
    Address::from(bytes)
}

/// `keccak256(abi.encode(owner, MOCK_ERC20_BALANCE_SLOT))` — the balance slot.
fn balance_slot(owner: Address) -> U256 {
    U256::from_be_bytes(
        keccak256((owner, U256::from(mock::MOCK_ERC20_BALANCE_SLOT)).abi_encode()).0,
    )
}

/// An `AtomicUsize`-counting batch fetcher: tallies every requested slot and
/// returns a canned non-zero balance, standing in for the RPC backend.
fn counting_fetcher(counter: Arc<AtomicUsize>) -> StorageBatchFetchFn {
    Arc::new(move |requests: Vec<(Address, U256)>, _block: BlockId| {
        counter.fetch_add(requests.len(), Ordering::Relaxed);
        requests
            .into_iter()
            .map(|(addr, slot)| (addr, slot, Ok(U256::from(1_000u64))))
            .collect()
    })
}

/// Build an offline cache with a MockERC20 installed at `token` (storage left
/// non-cleared so warmed layer-2 slots are readable) plus a counting fetcher.
async fn cache_with_counter(token: Address) -> Result<(EvmCache, Arc<AtomicUsize>)> {
    let mut cache = mock::offline_cache().await?;
    let runtime = mock::mock_erc20_runtime();
    let code_hash = runtime.hash_slow();
    cache.db_mut().insert_account_info(
        token,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code: Some(runtime),
            code_hash,
            account_id: None,
        },
    );
    let counter = Arc::new(AtomicUsize::new(0));
    cache.set_storage_batch_fetcher(counting_fetcher(counter.clone()));
    Ok((cache, counter))
}

fn balance_of_calldata(account: Address) -> Bytes {
    Bytes::from(mock::MockERC20::balanceOfCall { account }.abi_encode())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let token = Address::repeat_byte(0x42);
    let working_set: Vec<(Address, U256)> = (0..WORKING_SET)
        .map(|i| (token, balance_slot(owner(i))))
        .collect();

    // ── evm-fork-cache: fetch the working set ONCE, then fan out ──────────────
    let (mut cache, counter) = cache_with_counter(token).await?;

    // Warm the hot working set in a single batch (the prefetch a searcher does
    // once per block). Every slot is fetched exactly once.
    cache.verify_slots(&working_set)?;
    let warmup_fetches = counter.load(Ordering::Relaxed);

    // Freeze the warmed state, then fan N candidates out over cheap overlays.
    let snapshot = cache.snapshot();
    counter.store(0, Ordering::Relaxed); // measure only the fan-out from here

    for c in 0..N_CANDIDATES {
        // Each candidate is an isolated simulation reading the shared hot set.
        // The overlay holds no RPC backend (`None`), so it cannot fetch — every
        // read is served from the frozen snapshot in memory.
        let mut overlay = EvmOverlay::new(snapshot.clone(), None);
        for i in 0..WORKING_SET {
            let result =
                overlay.call_raw(owner(c % WORKING_SET), token, balance_of_calldata(owner(i)))?;
            debug_assert!(matches!(result, ExecutionResult::Success { .. }));
        }
    }
    let fanout_fetches = counter.load(Ordering::Relaxed);

    // ── Vanilla baseline: a fresh cold cache per candidate ───────────────────
    // Each independent cache cold-fetches the working set it reads. We measure
    // the per-candidate cost on one real cold cache; it then repeats N times.
    let (mut cold, cold_counter) = cache_with_counter(token).await?;
    cold.verify_slots(&working_set)?;
    let per_candidate_fetches = cold_counter.load(Ordering::Relaxed);
    let vanilla_total = per_candidate_fetches * N_CANDIDATES;

    let crate_total = warmup_fetches + fanout_fetches;

    println!(
        "Workload: {N_CANDIDATES} candidate txs, each reading a shared {WORKING_SET}-slot hot set\n"
    );
    println!("evm-fork-cache");
    println!("  warm-up fetches (once):      {warmup_fetches}");
    println!(
        "  fan-out fetches ({N_CANDIDATES} sims):  {fanout_fetches}  <- overlays read the frozen snapshot, no backend"
    );
    println!("  TOTAL slots fetched:         {crate_total}");
    println!("\nVanilla fork-per-candidate");
    println!("  fetches per candidate:       {per_candidate_fetches}");
    println!(
        "  TOTAL slots fetched:         {vanilla_total}  ({N_CANDIDATES} x {per_candidate_fetches})"
    );
    println!(
        "\n=> {}x fewer reads than a NAIVE un-shared loop ({} slots avoided)",
        vanilla_total / crate_total.max(1),
        vanilla_total - crate_total
    );
    println!(
        "\nHonest caveat: this beats only the *naive* baseline that builds a fresh cold\n\
         cache per candidate. A competent searcher sharing ONE foundry-fork-db\n\
         SharedBackend across candidates ALSO fetches each slot once within a block\n\
         (its cache dedups) — so within a single block this is ~1x, not {}x.\n\
         The durable win is CROSS-block: as blocks advance and the hot set mutates,\n\
         event-driven writes (see the reactive_cache example) keep it fresh with 0\n\
         re-fetches, where a refetch loop must re-read the changed slots every block.",
        vanilla_total / crate_total.max(1),
    );

    assert_eq!(
        warmup_fetches, WORKING_SET,
        "warm-up fetches the working set once"
    );
    assert_eq!(fanout_fetches, 0, "the fan-out adds zero fetches");
    Ok(())
}
