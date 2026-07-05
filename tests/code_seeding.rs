//! Verified code seeding & local etch — acceptance tests.
//!
//! Covers the spec's acceptance items 1–8 and 10
//! (docs/verified-code-seeding-spec.md §7): verification outcomes and their
//! fail-closed/fail-safe split, conflict rules, etch unification, persistence
//! round-trips, snapshot-generation semantics, and the no-`basic_ref`
//! guarantee for seeded accounts. Every test runs fully offline over a mocked
//! provider; verification reads go through stubbed [`AccountFieldsFetchFn`]s.

mod common;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use anyhow::Result;
use common::{
    failing_fields_fetcher, install_default_account, install_mock_erc20, mock_erc20_runtime,
    setup_cache, stub_fields_fetcher,
};
use evm_fork_cache::cache::{CacheConfig, CodeSeedState, EvmCache};
use evm_fork_cache::errors::CacheError;
use evm_fork_cache::multicall::MULTICALL3_ADDRESS;
use revm::context::result::ExecutionResult;

/// Minimal runtime: `PUSH1 01 PUSH1 00 MSTORE PUSH1 20 PUSH1 00 RETURN` —
/// returns one 32-byte word (value 1) and touches no storage or env, so a
/// call against it can never fall through to the (mocked) RPC backend.
const RETURN_ONE_RUNTIME: [u8; 10] = [0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];

/// Creation code deploying [`RETURN_ONE_RUNTIME`]:
/// `PUSH1 0a PUSH1 0c PUSH1 00 CODECOPY PUSH1 0a PUSH1 00 RETURN` + runtime.
fn return_one_creation_code() -> Vec<u8> {
    let mut creation = vec![
        0x60, 0x0a, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, 0x0a, 0x60, 0x00, 0xf3,
    ];
    creation.extend_from_slice(&RETURN_ONE_RUNTIME);
    creation
}

fn return_one_bytes() -> Bytes {
    Bytes::from(RETURN_ONE_RUNTIME.to_vec())
}

fn mock_erc20_bytes() -> Bytes {
    mock_erc20_runtime().original_bytes()
}

/// A per-test temp cache directory, keyed by pid so concurrent `cargo test`
/// processes never share (or delete) each other's directory.
fn temp_cache_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "evm_fork_cache_code_seeding_{tag}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Build a cache over a mocked provider with disk persistence at `dir`.
async fn setup_cache_with_config(dir: &PathBuf) -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    let cfg = CacheConfig::new(dir, 1, Default::default(), Default::default());
    EvmCache::builder(Arc::new(provider))
        .cache_config(cfg)
        .build()
        .await
}

/// Spec item 5a: seeding an unmarked (RPC-origin) account whose code hash
/// already matches is an instant `Verified` — zero RPC, zero writes.
#[tokio::test(flavor = "multi_thread")]
async fn seed_over_rpc_origin_equal_hash_is_instant_verified() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x10);
    install_mock_erc20(&mut cache, token);

    let generation_before = cache.snapshot_generation();
    let hash = cache.seed_account_code(token, mock_erc20_bytes())?;

    match cache.code_seed_state(&token) {
        Some(CodeSeedState::Verified { code_hash, .. }) => assert_eq!(*code_hash, hash),
        other => panic!("expected instant Verified, got {other:?}"),
    }
    assert!(
        cache.pending_code_seeds().is_empty(),
        "instant verification must not leave a Pending claim"
    );
    assert_eq!(
        cache.snapshot_generation(),
        generation_before,
        "the zero-write instant-Verified path must not bump the generation"
    );
    Ok(())
}

/// Spec item 5b: a seed contradicting RPC-origin code (or an EOA) is a
/// `CodeSeedConflict`, and the cached code stays untouched.
#[tokio::test(flavor = "multi_thread")]
async fn seed_conflicting_with_rpc_origin_errors_and_keeps_cached_code() -> Result<()> {
    let mut cache = setup_cache().await?;
    let token = Address::repeat_byte(0x11);
    install_mock_erc20(&mut cache, token);
    let original_hash = mock_erc20_runtime().hash_slow();

    let err = cache
        .seed_account_code(token, return_one_bytes())
        .expect_err("conflicting seed must be rejected");
    match err {
        CacheError::CodeSeedConflict {
            address, cached, ..
        } => {
            assert_eq!(address, token);
            assert_eq!(cached, original_hash);
        }
        other => panic!("expected CodeSeedConflict, got {other:?}"),
    }
    assert!(
        cache.code_seed_state(&token).is_none(),
        "a rejected seed must not leave a mark"
    );

    // A code-less EOA is chain knowledge too: seeding over it conflicts.
    let eoa = Address::repeat_byte(0x12);
    install_default_account(&mut cache, eoa);
    assert!(matches!(
        cache.seed_account_code(eoa, return_one_bytes()),
        Err(CacheError::CodeSeedConflict { .. })
    ));

    // Empty bytes are not a seedable claim at all.
    let fresh = Address::repeat_byte(0x13);
    assert!(matches!(
        cache.seed_account_code(fresh, Bytes::new()),
        Err(CacheError::CodeSeedEmpty { .. })
    ));
    Ok(())
}

/// Spec conflict-table rows 1 + 4: an absent address seeds as `Pending`, and
/// re-seeding a marked address overwrites and restarts the claim.
#[tokio::test(flavor = "multi_thread")]
async fn seed_absent_is_pending_and_reseed_restarts_claim() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x20);

    let first_hash = cache.seed_account_code(pool, mock_erc20_bytes())?;
    assert!(matches!(
        cache.code_seed_state(&pool),
        Some(CodeSeedState::Pending { code_hash }) if *code_hash == first_hash
    ));
    assert_eq!(cache.pending_code_seeds(), vec![pool]);

    // Re-seed with different bytes: the claim restarts as Pending under the
    // new hash — never a conflict for an already-marked address.
    let second_hash = cache.seed_account_code(pool, return_one_bytes())?;
    assert_ne!(first_hash, second_hash);
    assert!(matches!(
        cache.code_seed_state(&pool),
        Some(CodeSeedState::Pending { code_hash }) if *code_hash == second_hash
    ));
    Ok(())
}

/// Spec item 6 (first half): etched code is marked, excluded from the pending
/// set, and actually executed by simulations.
#[tokio::test(flavor = "multi_thread")]
async fn etch_marks_account_and_sims_read_etched_code() -> Result<()> {
    let mut cache = setup_cache().await?;
    let target = Address::repeat_byte(0x30);
    let caller = Address::repeat_byte(0x31);
    install_default_account(&mut cache, caller);
    // The default beneficiary; revm touches it post-execution.
    install_default_account(&mut cache, Address::ZERO);

    let hash = cache.etch_account_code(target, return_one_bytes())?;
    assert!(matches!(
        cache.code_seed_state(&target),
        Some(CodeSeedState::Etched { code_hash }) if *code_hash == hash
    ));
    assert_eq!(cache.etched_accounts(), vec![target]);
    assert!(
        cache.pending_code_seeds().is_empty(),
        "etched accounts are never part of the canonical verify set"
    );

    let result = cache.call_raw(caller, target, Bytes::new(), false)?;
    match result {
        ExecutionResult::Success { output, .. } => {
            assert_eq!(
                output.into_data(),
                Bytes::from(U256::from(1).to_be_bytes::<32>().to_vec()),
                "the simulation must execute the etched runtime"
            );
        }
        other => panic!("call against etched code failed: {other:?}"),
    }
    Ok(())
}

/// Spec item 6 (second half): every locally-divergent code site joins the
/// etched set — `override_account_code` targets and `deploy_contract`
/// creations included.
#[tokio::test(flavor = "multi_thread")]
async fn override_and_deploy_targets_join_the_etched_set() -> Result<()> {
    let mut cache = setup_cache().await?;
    let source = Address::repeat_byte(0x40);
    let target = Address::repeat_byte(0x41);
    install_mock_erc20(&mut cache, source);
    install_mock_erc20(&mut cache, target);

    cache.override_account_code(source, target)?;
    assert!(
        matches!(
            cache.code_seed_state(&target),
            Some(CodeSeedState::Etched { .. })
        ),
        "an override target is local divergence and must be etched-marked"
    );
    assert!(
        cache.code_seed_state(&source).is_none(),
        "the override source is untouched chain state"
    );

    let deployer = Address::repeat_byte(0x42);
    install_default_account(&mut cache, deployer);
    // Pre-materialize the accounts revm touches during the create so the
    // mocked backend is never consulted: the beneficiary and the (nonce-0)
    // deterministic create target.
    install_default_account(&mut cache, Address::ZERO);
    install_default_account(&mut cache, deployer.create(0));
    let deployed = cache.deploy_contract(deployer, Bytes::from(return_one_creation_code()))?;
    match cache.code_seed_state(&deployed) {
        Some(CodeSeedState::Etched { code_hash }) => {
            assert_eq!(
                *code_hash,
                revm::state::Bytecode::new_raw(return_one_bytes()).hash_slow(),
                "the etched mark must record the deployed runtime's hash"
            );
        }
        other => panic!("deployed contract must be etched-marked, got {other:?}"),
    }

    let etched = cache.etched_accounts();
    assert!(etched.contains(&target) && etched.contains(&deployed));
    Ok(())
}

/// An account-scope purge clears the mark along with both state layers (the
/// re-seed escape hatch after a believed redeploy).
#[tokio::test(flavor = "multi_thread")]
async fn purge_account_clears_the_mark() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x50);
    cache.seed_account_code(pool, return_one_bytes())?;
    assert!(cache.code_seed_state(&pool).is_some());

    cache.purge_account(pool);
    assert!(
        cache.code_seed_state(&pool).is_none(),
        "purge must clear the code-seed mark"
    );
    assert!(cache.pending_code_seeds().is_empty());
    Ok(())
}

/// Spec item 8 (C1 portion): seed and etch bump the snapshot generation; the
/// rejected-conflict path does not.
#[tokio::test(flavor = "multi_thread")]
async fn generation_bumps_on_seed_and_etch_not_on_rejects() -> Result<()> {
    let mut cache = setup_cache().await?;

    let g0 = cache.snapshot_generation();
    cache.seed_account_code(Address::repeat_byte(0x60), return_one_bytes())?;
    let g1 = cache.snapshot_generation();
    assert_ne!(g0, g1, "a fresh seed writes executable state and must bump");

    cache.etch_account_code(Address::repeat_byte(0x61), return_one_bytes())?;
    let g2 = cache.snapshot_generation();
    assert_ne!(g1, g2, "an etch mutates executable state and must bump");

    let token = Address::repeat_byte(0x62);
    install_mock_erc20(&mut cache, token);
    let g3 = cache.snapshot_generation();
    let _ = cache.seed_account_code(token, return_one_bytes());
    assert_eq!(
        cache.snapshot_generation(),
        g3,
        "a rejected (conflicting) seed writes nothing and must not bump"
    );
    Ok(())
}

/// Spec item 7: all three mark kinds survive a flush + reload; a fresh cache
/// directory has no marks (missing `code_seeds.bin` is a clean miss).
#[tokio::test(flavor = "multi_thread")]
async fn persistence_round_trip_preserves_marks() -> Result<()> {
    let dir = temp_cache_dir("roundtrip");
    let pending_addr = Address::repeat_byte(0x70);
    let verified_addr = Address::repeat_byte(0x71);
    let etched_addr = Address::repeat_byte(0x72);

    let (pending_hash, verified_hash, etched_hash) = {
        let mut cache = setup_cache_with_config(&dir).await;
        let pending_hash = cache.seed_account_code(pending_addr, mock_erc20_bytes())?;
        // Instant-Verified via the equal-hash fast path over an RPC-origin
        // account. Real RPC-origin accounts land in layer 2 (the backend
        // map, which is what persists) — mirror that here.
        let bytecode = mock_erc20_runtime();
        let info = revm::state::AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code_hash: bytecode.hash_slow(),
            code: Some(bytecode),
            account_id: None,
        };
        cache.with_blockchain_db_mut(|db| {
            db.accounts().write().insert(verified_addr, info);
        });
        let verified_hash = cache.seed_account_code(verified_addr, mock_erc20_bytes())?;
        let etched_hash = cache.etch_account_code(etched_addr, return_one_bytes())?;
        cache.flush()?;
        (pending_hash, verified_hash, etched_hash)
    };

    let reloaded = setup_cache_with_config(&dir).await;
    assert!(
        matches!(
            reloaded.code_seed_state(&pending_addr),
            Some(CodeSeedState::Pending { code_hash }) if *code_hash == pending_hash
        ),
        "Pending must reload as Pending — never masquerading as RPC-origin"
    );
    assert!(matches!(
        reloaded.code_seed_state(&verified_addr),
        Some(CodeSeedState::Verified { code_hash, .. }) if *code_hash == verified_hash
    ));
    assert!(matches!(
        reloaded.code_seed_state(&etched_addr),
        Some(CodeSeedState::Etched { code_hash }) if *code_hash == etched_hash
    ));
    assert_eq!(reloaded.pending_code_seeds(), vec![pending_addr]);
    assert_eq!(reloaded.etched_accounts(), vec![etched_addr]);

    // A brand-new cache directory has no code_seeds.bin: clean empty miss.
    let fresh_dir = temp_cache_dir("fresh");
    let fresh = setup_cache_with_config(&fresh_dir).await;
    assert!(fresh.pending_code_seeds().is_empty());
    assert!(fresh.etched_accounts().is_empty());

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&fresh_dir);
    Ok(())
}

/// Spec item 7 (pruning): a mark whose code did not survive the reload is
/// dropped rather than describing state that no longer exists.
#[tokio::test(flavor = "multi_thread")]
async fn marks_without_surviving_code_are_pruned_on_load() -> Result<()> {
    let dir = temp_cache_dir("prune");
    let pool = Address::repeat_byte(0x80);

    {
        let mut cache = setup_cache_with_config(&dir).await;
        cache.seed_account_code(pool, return_one_bytes())?;
        cache.flush()?;
    }

    // Wipe the state + bytecode files, keeping only code_seeds.bin: on
    // reload the marked account has no code, so the mark must be pruned.
    let chain_dir = dir.join("chain_1");
    std::fs::remove_file(chain_dir.join("evm_state.bin"))?;
    std::fs::remove_file(chain_dir.join("bytecodes.bin"))?;
    assert!(chain_dir.join("code_seeds.bin").exists());

    let reloaded = setup_cache_with_config(&dir).await;
    assert!(
        reloaded.code_seed_state(&pool).is_none(),
        "a mark without surviving code must be pruned on load"
    );

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// Spec item 10: a seeded account is fully materialized, so neither
/// `ensure_account` nor an EVM call ever reaches the RPC backend for it. The
/// mocked provider has an empty response queue — any RPC attempt would error.
#[tokio::test(flavor = "multi_thread")]
async fn seeded_account_never_triggers_the_backend_triple() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x90);
    let caller = Address::repeat_byte(0x91);
    install_default_account(&mut cache, caller);
    // The default beneficiary; revm touches it post-execution. Installing it
    // keeps this test's assertion focused on the *seeded* account.
    install_default_account(&mut cache, Address::ZERO);

    cache.seed_account_code(pool, return_one_bytes())?;

    // ensure_account early-returns for a present account; a backend fetch
    // against the empty mock queue would fail loudly.
    cache.ensure_account(pool).await?;

    let result = cache.call_raw(caller, pool, Bytes::new(), false)?;
    assert!(
        matches!(result, ExecutionResult::Success { .. }),
        "call against a seeded account must succeed without any RPC"
    );
    Ok(())
}

/// Spec item 1: a matching verification marks `Verified`, patches the real
/// balance from the same response without a generation bump, and the fetcher
/// is called exactly once ever — a settled set costs nothing.
#[tokio::test(flavor = "multi_thread")]
async fn verify_match_marks_verified_and_patches_balance_once() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0xa0);
    let expected = cache.seed_account_code(pool, return_one_bytes())?;

    let calls = Arc::new(AtomicUsize::new(0));
    let balance = U256::from(7_777u64);
    cache.set_account_fields_fetcher(stub_fields_fetcher(
        HashMap::from([(pool, (balance, expected))]),
        calls.clone(),
    ));

    let generation_before = cache.snapshot_generation();
    let report = cache.verify_code_seeds()?;
    assert_eq!(report.verified, vec![pool]);
    assert!(report.mismatched.is_empty() && report.unverifiable.is_empty());
    assert!(matches!(
        cache.code_seed_state(&pool),
        Some(CodeSeedState::Verified { code_hash, .. }) if *code_hash == expected
    ));
    assert_eq!(
        cache.snapshot_generation(),
        generation_before,
        "a verify-match materializes pinned-block truth and must not bump"
    );
    assert_eq!(
        cache
            .db_mut()
            .cache
            .accounts
            .get(&pool)
            .expect("seeded account present")
            .info
            .balance,
        balance,
        "the on-chain balance from the verification sample must be patched in"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // A second sweep has nothing pending: the fetcher is never re-consulted.
    let second = cache.verify_code_seeds()?;
    assert!(second.verified.is_empty());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "Verified seeds are never re-verified"
    );
    Ok(())
}

/// Spec item 1 (persistence half): `Verified` survives a save/load and is
/// still never re-queried — the reloaded cache issues zero fields calls.
#[tokio::test(flavor = "multi_thread")]
async fn verified_seed_is_not_requeried_across_save_load() -> Result<()> {
    let dir = temp_cache_dir("verified_reload");
    let pool = Address::repeat_byte(0xa1);

    {
        let mut cache = setup_cache_with_config(&dir).await;
        let expected = cache.seed_account_code(pool, return_one_bytes())?;
        let calls = Arc::new(AtomicUsize::new(0));
        cache.set_account_fields_fetcher(stub_fields_fetcher(
            HashMap::from([(pool, (U256::ZERO, expected))]),
            calls.clone(),
        ));
        cache.verify_code_seeds()?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cache.flush()?;
    }

    let mut reloaded = setup_cache_with_config(&dir).await;
    assert!(matches!(
        reloaded.code_seed_state(&pool),
        Some(CodeSeedState::Verified { .. })
    ));
    // A fetcher that would fail loudly if consulted: with nothing pending,
    // verify_code_seeds must not touch it.
    let calls = Arc::new(AtomicUsize::new(0));
    reloaded.set_account_fields_fetcher(failing_fields_fetcher(calls.clone()));
    let report = reloaded.verify_code_seeds()?;
    assert!(report.verified.is_empty() && report.unverifiable.is_empty());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a settled (Verified) set must cost zero fields calls after reload"
    );

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// Spec item 2: a mismatch purges the seed from both layers and the mark
/// (bumping the generation via the purge path), and the next touch refetches
/// authoritative chain state through the ordinary backend.
#[tokio::test(flavor = "multi_thread")]
async fn verify_mismatch_purges_and_next_touch_refetches() -> Result<()> {
    let (mut cache, asserter) = common::setup_cache_with_asserter().await?;
    let pool = Address::repeat_byte(0xa2);
    let expected = cache.seed_account_code(pool, return_one_bytes())?;

    let actual = B256::repeat_byte(0xdd);
    let calls = Arc::new(AtomicUsize::new(0));
    cache.set_account_fields_fetcher(stub_fields_fetcher(
        HashMap::from([(pool, (U256::ZERO, actual))]),
        calls.clone(),
    ));

    let generation_before = cache.snapshot_generation();
    let report = cache.verify_code_seeds()?;
    assert_eq!(report.mismatched.len(), 1);
    assert_eq!(report.mismatched[0].address, pool);
    assert_eq!(report.mismatched[0].expected, expected);
    assert_eq!(report.mismatched[0].actual, actual);
    assert!(report.verified.is_empty());

    assert!(
        cache.code_seed_state(&pool).is_none(),
        "a contradicted claim must not leave a mark"
    );
    assert!(
        !cache.db_mut().cache.accounts.contains_key(&pool),
        "the purge must clear the overlay layer"
    );
    assert_ne!(
        cache.snapshot_generation(),
        generation_before,
        "a mismatch purge changes executable state and must bump"
    );

    // The next touch goes back to the ordinary lazy backend: with the mock
    // queue empty, `ensure_account` must now *attempt* an RPC fetch and fail
    // loudly — the exact inverse of the seeded-account no-RPC guarantee.
    // (Queued-response ordering is not exercised here because the backend
    // issues the balance/nonce/code triple concurrently.)
    assert!(asserter.read_q().is_empty());
    assert!(
        cache.ensure_account(pool).await.is_err(),
        "a purged seed must fall through to the backend on the next touch"
    );
    Ok(())
}

/// Spec item 3: `EXTCODEHASH == 0` (no account) and `keccak256("")` (EOA)
/// are classified into their own buckets — both purged, since the claim is
/// contradicted either way.
#[tokio::test(flavor = "multi_thread")]
async fn verify_classifies_not_deployed_and_codeless() -> Result<()> {
    let mut cache = setup_cache().await?;
    let undeployed = Address::repeat_byte(0xa3);
    let eoa = Address::repeat_byte(0xa4);
    cache.seed_account_code(undeployed, return_one_bytes())?;
    cache.seed_account_code(eoa, return_one_bytes())?;

    let calls = Arc::new(AtomicUsize::new(0));
    cache.set_account_fields_fetcher(stub_fields_fetcher(
        HashMap::from([
            (undeployed, (U256::ZERO, B256::ZERO)),
            (eoa, (U256::from(5u64), revm::primitives::KECCAK_EMPTY)),
        ]),
        calls.clone(),
    ));

    let report = cache.verify_code_seeds()?;
    assert_eq!(report.not_deployed, vec![undeployed]);
    assert_eq!(report.codeless, vec![eoa]);
    assert!(report.verified.is_empty() && report.mismatched.is_empty());
    assert!(cache.code_seed_state(&undeployed).is_none());
    assert!(cache.code_seed_state(&eoa).is_none());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "both classifications come from the single bulk call"
    );
    Ok(())
}

/// Spec item 4: a transport failure is fail-safe — every seed stays
/// `Pending`, nothing is purged, nothing bumps, and the report says why.
#[tokio::test(flavor = "multi_thread")]
async fn verify_transport_failure_keeps_seeds_pending() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0xa5);
    cache.seed_account_code(pool, return_one_bytes())?;

    let calls = Arc::new(AtomicUsize::new(0));
    cache.set_account_fields_fetcher(failing_fields_fetcher(calls.clone()));

    let generation_before = cache.snapshot_generation();
    let report = cache.verify_code_seeds()?;
    assert_eq!(report.unverifiable.len(), 1);
    assert_eq!(report.unverifiable[0].0, pool);
    assert!(report.unverifiable[0].1.contains("stub transport failure"));
    assert!(report.verified.is_empty() && report.mismatched.is_empty());

    assert!(
        matches!(
            cache.code_seed_state(&pool),
            Some(CodeSeedState::Pending { .. })
        ),
        "a failed read proves nothing: the seed must stay Pending"
    );
    assert!(
        cache.db_mut().cache.accounts.contains_key(&pool),
        "nothing may be purged on a transport failure"
    );
    assert_eq!(cache.snapshot_generation(), generation_before);
    assert_eq!(cache.pending_code_seeds(), vec![pool]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// The extractor-host caveat: a seed at `MULTICALL3_ADDRESS` cannot be
/// verified by the fields path (the extractor is hosted there under the
/// override) — reported unverifiable without consulting the fetcher.
#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_extractor_host_seed_unverifiable() -> Result<()> {
    let mut cache = setup_cache().await?;
    cache.seed_account_code(MULTICALL3_ADDRESS, return_one_bytes())?;

    let calls = Arc::new(AtomicUsize::new(0));
    cache.set_account_fields_fetcher(stub_fields_fetcher(HashMap::new(), calls.clone()));

    let report = cache.verify_code_seeds()?;
    assert_eq!(report.unverifiable.len(), 1);
    assert_eq!(report.unverifiable[0].0, MULTICALL3_ADDRESS);
    assert!(report.unverifiable[0].1.contains("eth_getProof"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a host-only pending set must not issue a fields call at all"
    );
    assert!(matches!(
        cache.code_seed_state(&MULTICALL3_ADDRESS),
        Some(CodeSeedState::Pending { .. })
    ));
    Ok(())
}
