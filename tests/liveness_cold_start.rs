//! Manager-authored red-green acceptance tests for Phase-8 step 5: the
//! cold-start root baseline (`roots.bin`).
//!
//! A process restarting after downtime should not blindly re-read its whole
//! working set. It persists each tracked account's observed storage root as a
//! baseline; on restart it probes the root *now* and, where it equals the
//! baseline, the cached tracked slots are provably current — **skip re-reading**
//! ("if no divergence, we're already synced"). Where it diverges (or no baseline
//! exists, or the probe fails), re-read the tracked slots and adopt the new root.
//! This is a *currency* gate, not a completeness gate (spec §6).
//!
//! Fully offline: proof/storage fetchers are stubbed; persistence uses a
//! pid-keyed temp dir.
#![cfg(feature = "reactive")]

mod common;

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use alloy_primitives::{Address, B256, U256};
use anyhow::Result;

use common::setup_cache;
use evm_fork_cache::cache::AccountProof;
use evm_fork_cache::errors::StorageFetchError;
use evm_fork_cache::{ColdStartConfig, RootBaseline, RootBaselinePlanner};

/// A pid-keyed temp dir so concurrent `cargo test` processes never collide.
fn temp_dir(tag: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("evm_fork_cache_roots_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Install an account-proof fetcher that always reports `root` for any address.
fn install_const_root_fetcher(cache: &mut evm_fork_cache::cache::EvmCache, root: B256) {
    cache.set_account_proof_fetcher(Arc::new(move |requests, _block| {
        requests
            .into_iter()
            .map(|(addr, _keys)| {
                (
                    addr,
                    Ok(AccountProof {
                        storage_hash: root,
                        balance: U256::ZERO,
                        nonce: 0,
                        code_hash: B256::ZERO,
                        slots: vec![],
                    }),
                )
            })
            .collect()
    }));
}

/// Install a storage batch fetcher that serves `value` for every slot and counts
/// how many slots were fetched.
fn install_counting_storage_fetcher(
    cache: &mut evm_fork_cache::cache::EvmCache,
    value: U256,
) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    cache.set_storage_batch_fetcher(Arc::new(move |requests, _block| {
        seen.fetch_add(requests.len(), Ordering::SeqCst);
        requests
            .into_iter()
            .map(|(addr, slot)| (addr, slot, Ok(value)))
            .collect()
    }));
    count
}

/// Phase-8 s5: `roots.bin` round-trips through the versioned envelope, and a
/// legacy/unknown-magic file is a cache miss (never an error, never trusted).
#[test]
fn root_baseline_round_trips_and_rejects_unknown_magic() -> Result<()> {
    let dir = temp_dir("roundtrip");
    let path = dir.join("roots.bin");
    let addr = Address::repeat_byte(0x21);
    let root = B256::repeat_byte(0xaa);

    let mut baseline = RootBaseline::default();
    baseline.insert(addr, root);
    baseline.save(&path)?;

    let loaded = RootBaseline::load(&path).expect("a just-saved baseline loads");
    assert_eq!(loaded.get(&addr), Some(root), "the baseline round-trips");

    // A missing file is a miss.
    assert!(
        RootBaseline::load(&dir.join("absent.bin")).is_none(),
        "a missing file is a cache miss"
    );

    // A legacy / unknown-magic payload is a miss, not an error and not data.
    std::fs::write(&path, b"not a versioned roots baseline")?;
    assert!(
        RootBaseline::load(&path).is_none(),
        "unknown magic must be treated as a cache miss"
    );

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// Phase-8 s5: when the probed root equals the persisted baseline, the tracked
/// slots are provably current — the planner finishes without a single storage
/// re-read ("if no divergence, we're already synced").
#[tokio::test]
async fn equal_baseline_skips_rereading_tracked_slots() -> Result<()> {
    let tracked = Address::repeat_byte(0x31);
    let slot = U256::from(4);
    let root = B256::repeat_byte(0xaa);
    let mut cache = setup_cache().await?;

    install_const_root_fetcher(&mut cache, root);
    let fetches = install_counting_storage_fetcher(&mut cache, U256::from(1));

    // The persisted baseline already matches what the chain reports now.
    let mut baseline = RootBaseline::default();
    baseline.insert(tracked, root);

    let mut planner = RootBaselinePlanner::new(vec![(tracked, vec![slot])], baseline);
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "an unchanged root must not re-read any tracked slot"
    );
    assert_eq!(report.rounds, 1, "the probe round is the only round");
    assert_eq!(
        planner.updated_baseline().get(&tracked),
        Some(root),
        "the (unchanged) observed root is retained in the updated baseline"
    );
    Ok(())
}

/// Phase-8 s5: a diverged (or missing) baseline re-reads the tracked slots and
/// adopts the newly observed root into the updated baseline.
#[tokio::test]
async fn diverged_baseline_rereads_and_adopts_new_root() -> Result<()> {
    let tracked = Address::repeat_byte(0x32);
    let slot = U256::from(7);
    let old_root = B256::repeat_byte(0xaa);
    let new_root = B256::repeat_byte(0xbb);
    let fresh_value = U256::from(777);
    let mut cache = setup_cache().await?;

    install_const_root_fetcher(&mut cache, new_root);
    let fetches = install_counting_storage_fetcher(&mut cache, fresh_value);

    // The persisted baseline predates the (moved) on-chain root.
    let mut baseline = RootBaseline::default();
    baseline.insert(tracked, old_root);

    let mut planner = RootBaselinePlanner::new(vec![(tracked, vec![slot])], baseline);
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "a diverged root must re-read exactly the tracked slots"
    );
    assert_eq!(report.rounds, 2, "probe round + re-read round");
    assert_eq!(
        cache.cached_storage_value(tracked, slot),
        Some(fresh_value),
        "the re-read value is injected into the cache"
    );
    assert_eq!(
        planner.updated_baseline().get(&tracked),
        Some(new_root),
        "the newly observed root is adopted"
    );
    Ok(())
}

/// Phase-8 s5: with NO baseline entry for a tracked account (first run), the
/// planner conservatively re-reads and adopts — absence of evidence is not
/// currency.
#[tokio::test]
async fn missing_baseline_entry_rereads_and_adopts() -> Result<()> {
    let tracked = Address::repeat_byte(0x33);
    let slot = U256::from(9);
    let root = B256::repeat_byte(0xcc);
    let mut cache = setup_cache().await?;

    install_const_root_fetcher(&mut cache, root);
    let fetches = install_counting_storage_fetcher(&mut cache, U256::from(5));

    let mut planner = RootBaselinePlanner::new(
        vec![(tracked, vec![slot])],
        RootBaseline::default(), // empty: nothing persisted yet
    );
    cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "no baseline entry means the tracked slot must be read"
    );
    assert_eq!(
        planner.updated_baseline().get(&tracked),
        Some(root),
        "the first observed root is adopted as the new baseline"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Implementation-agent tests (Wave 8): guard, probe-failure no-clobber, mixed run
// ---------------------------------------------------------------------------

/// Phase-8 s5: a probe_roots-bearing round over a cache with no account-proof
/// fetcher errors with `NoAccountProofFetcher` before issuing any read —
/// mirroring the storage-fetcher `NoBatchFetcher` guard.
#[tokio::test(flavor = "multi_thread")]
async fn probe_roots_without_account_proof_fetcher_errors() -> Result<()> {
    // `from_backend` builds a cache with no fetchers installed (the same
    // pattern the NoBatchFetcher tests use in tests/cold_start.rs).
    let base = setup_cache().await?;
    let mut cache = evm_fork_cache::cache::EvmCache::from_backend(
        base.unchecked_backend().clone(),
        base.unchecked_blockchain_db().clone(),
        base.block(),
        base.chain_id(),
        None,
        None,
        revm::primitives::hardfork::SpecId::CANCUN,
    );
    assert!(
        cache.account_proof_fetcher().is_none(),
        "from_backend cache has no account-proof fetcher"
    );

    let mut planner = RootBaselinePlanner::new(
        vec![(Address::repeat_byte(0x41), vec![U256::from(1)])],
        RootBaseline::default(),
    );
    let err = cache
        .run_cold_start(&mut planner, ColdStartConfig::default())
        .expect_err("a probe_roots round with no account-proof fetcher must error");
    assert!(
        matches!(err, evm_fork_cache::ColdStartError::NoAccountProofFetcher),
        "got {err:?}"
    );
    Ok(())
}

/// Phase-8 s5: a failed probe (`root: None`) is conservative — the tracked
/// slots ARE re-read, but no root is adopted: the updated baseline keeps the
/// OLD persisted root for that address (an unobserved root must not clobber a
/// real one).
#[tokio::test]
async fn failed_probe_rereads_but_never_clobbers_baseline_root() -> Result<()> {
    let tracked = Address::repeat_byte(0x42);
    let slot = U256::from(11);
    let old_root = B256::repeat_byte(0xdd);
    let mut cache = setup_cache().await?;

    // Every probe fails: the fetcher returns Err for each requested address.
    cache.set_account_proof_fetcher(Arc::new(|requests, _block| {
        requests
            .into_iter()
            .map(|(addr, _keys)| (addr, Err(StorageFetchError::custom("proof endpoint down"))))
            .collect()
    }));
    let fetches = install_counting_storage_fetcher(&mut cache, U256::from(3));

    let mut baseline = RootBaseline::default();
    baseline.insert(tracked, old_root);

    let mut planner = RootBaselinePlanner::new(vec![(tracked, vec![slot])], baseline);
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "an unobservable root must conservatively re-read the tracked slots"
    );
    assert_eq!(report.rounds, 2, "probe round + conservative re-read round");
    assert_eq!(
        report.per_round[0].probe_roots_requested, 1,
        "the probe round declared one root probe"
    );
    assert_eq!(
        report.per_round[0].probe_roots_failed, 1,
        "the failed probe is visible in the round summary"
    );
    assert_eq!(
        planner.updated_baseline().get(&tracked),
        Some(old_root),
        "an unobserved root must not clobber the persisted baseline entry"
    );
    Ok(())
}

/// Phase-8 s5: a mixed run — one tracked account's root equals the baseline and
/// another's diverged — re-reads ONLY the diverged account's tracked slots and
/// adopts per-account.
#[tokio::test]
async fn mixed_run_rereads_only_the_diverged_account() -> Result<()> {
    let stable = Address::repeat_byte(0x51);
    let moved = Address::repeat_byte(0x52);
    let stable_slot = U256::from(1);
    let moved_slot = U256::from(2);
    let stable_root = B256::repeat_byte(0xa1);
    let old_moved_root = B256::repeat_byte(0xb1);
    let new_moved_root = B256::repeat_byte(0xb2);
    let mut cache = setup_cache().await?;

    // Per-address roots: `stable` still matches its baseline; `moved` reports a
    // new root.
    cache.set_account_proof_fetcher(Arc::new(move |requests, _block| {
        requests
            .into_iter()
            .map(|(addr, _keys)| {
                let root = if addr == stable {
                    stable_root
                } else {
                    new_moved_root
                };
                (
                    addr,
                    Ok(AccountProof {
                        storage_hash: root,
                        balance: U256::ZERO,
                        nonce: 0,
                        code_hash: B256::ZERO,
                        slots: vec![],
                    }),
                )
            })
            .collect()
    }));

    // Record exactly which slots get re-read.
    let seen: Arc<std::sync::Mutex<Vec<(Address, U256)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = seen.clone();
    cache.set_storage_batch_fetcher(Arc::new(move |requests, _block| {
        record
            .lock()
            .unwrap()
            .extend(requests.iter().map(|&(a, s)| (a, s)));
        requests
            .into_iter()
            .map(|(a, s)| (a, s, Ok(U256::from(9))))
            .collect()
    }));

    let mut baseline = RootBaseline::default();
    baseline.insert(stable, stable_root);
    baseline.insert(moved, old_moved_root);

    let mut planner = RootBaselinePlanner::new(
        vec![(stable, vec![stable_slot]), (moved, vec![moved_slot])],
        baseline,
    );
    let report = cache.run_cold_start(&mut planner, ColdStartConfig::default())?;

    assert_eq!(report.rounds, 2, "one probe round + one re-read round");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(moved, moved_slot)],
        "only the diverged account's tracked slots are re-read"
    );
    assert_eq!(
        planner.updated_baseline().get(&stable),
        Some(stable_root),
        "the unchanged account retains its (re-observed) root"
    );
    assert_eq!(
        planner.updated_baseline().get(&moved),
        Some(new_moved_root),
        "the diverged account adopts the newly observed root"
    );
    Ok(())
}
