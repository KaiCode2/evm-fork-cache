//! Offline acceptance tests for split cold-start storage fetch and commit.
#![cfg(feature = "reactive")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use alloy_eips::BlockId;
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use anyhow::Result;

use evm_fork_cache::cache::{
    AccountProof, AccountProofFetchFn, CodeSeedState, StorageBatchConfig, StorageBatchFetchFn,
    StorageFetchStrategy,
};
use evm_fork_cache::cold_start::{
    AccountCodeClaim, AccountProofOutcome, AccountProofRoundFetchError, AccountProofRoundFetcher,
    AccountProofRoundRequest, PreparedAccountPatch, PreparedAccountPatchError,
    PreparedAccountValue, PreparedStoragePatch, PreparedStoragePatchError, PreparedStorageValue,
    SlotFetch, StorageRoundFetch, StorageRoundFetchError, StorageRoundFetcher, StorageRoundRequest,
};

use common::{install_mock_erc20, setup_cache};

#[tokio::test(flavor = "multi_thread")]
async fn exact_hash_storage_round_fetch_is_non_mutating() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x41);
    let verify_slot = U256::from(1);
    let probe_slot = U256::from(2);
    let block_hash = B256::repeat_byte(0xa1);
    install_mock_erc20(&mut cache, pool);
    cache.inject_storage_batch_fresh(&[(pool, verify_slot, U256::from(7))]);

    let seen_block = Arc::new(Mutex::new(None));
    let fetch_seen_block = Arc::clone(&seen_block);
    let fetcher: StorageBatchFetchFn = Arc::new(move |requests, block| {
        *fetch_seen_block.lock().expect("seen block lock") = Some(block);
        requests
            .into_iter()
            .map(|(address, slot)| {
                let value = if slot == verify_slot {
                    U256::from(11)
                } else {
                    U256::ZERO
                };
                (address, slot, Ok(value))
            })
            .collect()
    });
    let provider = StorageRoundFetcher::new(fetcher);
    let request = StorageRoundRequest::new(block_hash, [(pool, verify_slot)], [(pool, probe_slot)]);

    let fetched = provider.fetch(&request)?;

    assert_eq!(
        *seen_block.lock().expect("seen block lock"),
        Some(BlockId::from((block_hash, Some(true)))),
        "every provider read is pinned to the requested canonical hash"
    );
    assert_eq!(fetched.block_hash(), block_hash);
    assert_eq!(
        fetched.verified()[0].fetch,
        SlotFetch::Value(U256::from(11))
    );
    assert_eq!(fetched.probed()[0].fetch, SlotFetch::Zero);
    assert_eq!(
        fetched.patch().values(),
        &[PreparedStorageValue::new(pool, verify_slot, U256::from(11))]
    );
    assert_eq!(
        cache.cached_storage_value(pool, verify_slot),
        Some(U256::from(7)),
        "fetch workers must not mutate the cache they prepare state for"
    );
    assert_eq!(
        cache.cached_storage_value(pool, probe_slot),
        Some(U256::ZERO),
        "probe results are observational only"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_patch_atomically_heals_both_cache_layers() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x42);
    let slot_a = U256::from(3);
    let slot_b = U256::from(4);
    let block_hash = B256::repeat_byte(0xa2);
    install_mock_erc20(&mut cache, pool);
    cache.inject_storage_batch_fresh(&[
        (pool, slot_a, U256::from(7)),
        (pool, slot_b, U256::from(8)),
    ]);
    cache.set_block(BlockId::from((block_hash, Some(true))));
    let patch = PreparedStoragePatch::new(
        block_hash,
        [
            PreparedStorageValue::new(pool, slot_a, U256::from(17)),
            PreparedStorageValue::new(pool, slot_b, U256::ZERO),
        ],
    );

    let diff = cache.apply_prepared_storage_patch(&patch)?;

    assert_eq!(diff.slots.len(), 2);
    assert_eq!(
        cache.cached_storage_value(pool, slot_a),
        Some(U256::from(17)),
        "the layer-1 overlay must no longer shadow the prepared value"
    );
    assert_eq!(cache.cached_storage_value(pool, slot_b), Some(U256::ZERO));
    let backend = cache.unchecked_blockchain_db().storage().read();
    assert_eq!(
        backend.get(&pool).and_then(|slots| slots.get(&slot_a)),
        Some(&U256::from(17)),
        "the authoritative layer-2 backend must be healed in the same commit"
    );
    assert_eq!(
        backend.get(&pool).and_then(|slots| slots.get(&slot_b)),
        Some(&U256::ZERO)
    );
    drop(backend);
    let snapshot = cache.snapshot();
    assert_eq!(snapshot.storage_value(pool, slot_a), Some(U256::from(17)));
    assert_eq!(snapshot.storage_value(pool, slot_b), Some(U256::ZERO));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_prepared_slot_rejects_the_whole_patch() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x43);
    let slot = U256::from(5);
    let untouched = U256::from(6);
    let block_hash = B256::repeat_byte(0xa3);
    install_mock_erc20(&mut cache, pool);
    cache.inject_storage_batch_fresh(&[
        (pool, slot, U256::from(7)),
        (pool, untouched, U256::from(8)),
    ]);
    cache.set_block(BlockId::from((block_hash, Some(true))));
    let patch = PreparedStoragePatch::new(
        block_hash,
        [
            PreparedStorageValue::new(pool, untouched, U256::from(18)),
            PreparedStorageValue::new(pool, slot, U256::from(17)),
            PreparedStorageValue::new(pool, slot, U256::from(27)),
        ],
    );

    let error = cache
        .apply_prepared_storage_patch(&patch)
        .expect_err("duplicate identity must reject before any write");

    assert_eq!(
        error,
        PreparedStoragePatchError::DuplicateSlot {
            address: pool,
            slot,
        }
    );
    assert_eq!(cache.cached_storage_value(pool, slot), Some(U256::from(7)));
    assert_eq!(
        cache.cached_storage_value(pool, untouched),
        Some(U256::from(8)),
        "even a unique value preceding the duplicate must remain unapplied"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_prepared_hash_rejects_before_mutation() -> Result<()> {
    let mut cache = setup_cache().await?;
    let pool = Address::repeat_byte(0x44);
    let slot = U256::from(7);
    let current_hash = B256::repeat_byte(0xa4);
    let stale_hash = B256::repeat_byte(0x94);
    install_mock_erc20(&mut cache, pool);
    cache.inject_storage_batch_fresh(&[(pool, slot, U256::from(7))]);
    cache.set_block(BlockId::from((current_hash, Some(true))));
    let patch = PreparedStoragePatch::new(
        stale_hash,
        [PreparedStorageValue::new(pool, slot, U256::from(17))],
    );

    let error = cache
        .apply_prepared_storage_patch(&patch)
        .expect_err("an old-generation hash must never commit into the current cache");

    assert_eq!(
        error,
        PreparedStoragePatchError::BaselineMismatch {
            prepared: stale_hash,
            cache: Some(current_hash),
        }
    );
    assert_eq!(cache.cached_storage_value(pool, slot), Some(U256::from(7)));

    Ok(())
}

#[test]
fn storage_round_rejects_ambiguous_identities_before_publication() {
    let pool = Address::repeat_byte(0x45);
    let slot = U256::from(8);
    let block_hash = B256::repeat_byte(0xa5);
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let fetch_calls = Arc::clone(&provider_calls);
    let fetcher: StorageBatchFetchFn = Arc::new(move |requests, _| {
        fetch_calls.fetch_add(1, Ordering::SeqCst);
        requests
            .into_iter()
            .map(|(address, slot)| (address, slot, Ok(U256::from(1))))
            .collect()
    });
    let provider = StorageRoundFetcher::new(fetcher);
    let duplicate_request = StorageRoundRequest::new(block_hash, [(pool, slot)], [(pool, slot)]);

    assert_eq!(
        provider.fetch(&duplicate_request),
        Err(StorageRoundFetchError::DuplicateRequest {
            address: pool,
            slot,
        })
    );
    assert_eq!(
        provider_calls.load(Ordering::SeqCst),
        0,
        "an invalid request must fail before provider IO"
    );

    let duplicate_fetcher: StorageBatchFetchFn = Arc::new(move |requests, _| {
        let (address, slot) = requests[0];
        vec![
            (address, slot, Ok(U256::from(1))),
            (address, slot, Ok(U256::from(2))),
        ]
    });
    let provider = StorageRoundFetcher::new(duplicate_fetcher);
    let request =
        StorageRoundRequest::new(block_hash, [(pool, slot)], Vec::<(Address, U256)>::new());
    assert_eq!(
        provider.fetch(&request),
        Err(StorageRoundFetchError::DuplicateResult {
            address: pool,
            slot,
        })
    );
}

#[test]
fn worker_storage_artifacts_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<StorageRoundFetcher>();
    assert_send_sync::<StorageRoundRequest>();
    assert_send_sync::<StorageRoundFetch>();
    assert_send_sync::<PreparedStoragePatch>();
    assert_send_sync::<AccountProofRoundFetcher>();
    assert_send_sync::<AccountProofRoundRequest>();
    assert_send_sync::<evm_fork_cache::cold_start::AccountProofRoundFetch>();
    assert_send_sync::<PreparedAccountPatch>();

    let provider = Arc::new(RootProvider::<AnyNetwork>::new(RpcClient::mocked(
        Asserter::new(),
    )));
    let _storage = StorageRoundFetcher::from_provider(
        Arc::clone(&provider),
        StorageBatchConfig::default(),
        StorageFetchStrategy::PointRead,
    );
    let _accounts = AccountProofRoundFetcher::from_provider(provider, 4);
}

#[test]
fn account_code_claims_are_verified_at_the_exact_canonical_hash() -> Result<()> {
    let contract = Address::repeat_byte(0x51);
    let block_hash = B256::repeat_byte(0xb1);
    let code = Bytes::from_static(&[0x60, 0x00, 0x56]);
    let expected_code_hash = keccak256(&code);
    let seen_block = Arc::new(Mutex::new(None));
    let fetch_seen_block = Arc::clone(&seen_block);
    let fetcher: AccountProofFetchFn = Arc::new(move |requests, block| {
        *fetch_seen_block.lock().expect("seen block lock") = Some(block);
        requests
            .into_iter()
            .map(|(address, slots)| {
                assert!(slots.is_empty(), "existence checks are root-only proofs");
                (
                    address,
                    Ok(AccountProof {
                        storage_hash: B256::repeat_byte(0x71),
                        balance: U256::from(9),
                        nonce: 3,
                        code_hash: expected_code_hash,
                        slots: Vec::new(),
                    }),
                )
            })
            .collect()
    });
    let provider = AccountProofRoundFetcher::new(fetcher);
    let request = AccountProofRoundRequest::new(
        block_hash,
        [AccountCodeClaim::new(contract, expected_code_hash)],
    );

    let fetched = provider.fetch(&request)?;

    assert_eq!(
        *seen_block.lock().expect("seen block lock"),
        Some(BlockId::from((block_hash, Some(true))))
    );
    assert_eq!(fetched.block_hash(), block_hash);
    match &fetched.outcomes()[0] {
        AccountProofOutcome::Verified { address, proof } => {
            assert_eq!(*address, contract);
            assert_eq!(proof.balance, U256::from(9));
            assert_eq!(proof.nonce, 3);
            assert_eq!(proof.code_hash, expected_code_hash);
        }
        outcome => panic!("expected verified proof, got {outcome:?}"),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_account_patch_promotes_code_directly_to_verified() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x52);
    let block_hash = B256::repeat_byte(0xb2);
    let block_number = 12_345;
    let code = Bytes::from_static(&[0x60, 0x00, 0x56]);
    let code_hash = keccak256(&code);
    cache.set_block(BlockId::from((block_hash, Some(true))));
    cache.seed_account_code(contract, code.clone())?;
    assert!(matches!(
        cache.code_seed_state(&contract),
        Some(CodeSeedState::Pending { .. })
    ));
    let proof = AccountProof {
        storage_hash: B256::repeat_byte(0x72),
        balance: U256::from(91),
        nonce: 7,
        code_hash,
        slots: Vec::new(),
    };
    let patch = PreparedAccountPatch::new(
        block_hash,
        block_number,
        [PreparedAccountValue::new(contract, proof, code)],
    );

    let generation_before_validate = cache.snapshot_generation();
    cache.validate_prepared_account_patch(&patch)?;
    assert_eq!(
        cache.snapshot_generation(),
        generation_before_validate,
        "validation must not mutate snapshot provenance"
    );
    assert!(matches!(
        cache.code_seed_state(&contract),
        Some(CodeSeedState::Pending { .. })
    ));
    let provisional = cache
        .db_mut()
        .cache
        .accounts
        .get(&contract)
        .expect("pending seed remains materialized")
        .info
        .clone();
    assert_eq!(provisional.balance, U256::ZERO);
    assert_eq!(provisional.nonce, 1);

    cache.apply_prepared_account_patch(&patch)?;

    assert_eq!(cache.pending_code_seeds(), Vec::<Address>::new());
    assert_eq!(
        cache.code_seed_state(&contract),
        Some(&CodeSeedState::Verified {
            code_hash,
            verified_at_block: block_number,
        })
    );
    let overlay = cache
        .db_mut()
        .cache
        .accounts
        .get(&contract)
        .expect("verified account in overlay")
        .info
        .clone();
    assert_eq!(overlay.balance, U256::from(91));
    assert_eq!(overlay.nonce, 7);
    assert_eq!(overlay.code_hash, code_hash);
    let backend = cache.unchecked_blockchain_db().accounts().read();
    let backend = backend.get(&contract).expect("verified account in backend");
    assert_eq!(backend.balance, U256::from(91));
    assert_eq!(backend.nonce, 7);
    assert_eq!(backend.code_hash, code_hash);

    Ok(())
}

#[test]
fn account_proof_round_strictly_validates_provider_identities() {
    let contract = Address::repeat_byte(0x53);
    let unexpected = Address::repeat_byte(0x54);
    let block_hash = B256::repeat_byte(0xb3);
    let code_hash = B256::repeat_byte(0x73);
    let calls = Arc::new(AtomicUsize::new(0));
    let fetch_calls = Arc::clone(&calls);
    let fetcher: AccountProofFetchFn = Arc::new(move |_, _| {
        fetch_calls.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    });
    let provider = AccountProofRoundFetcher::new(fetcher);
    let duplicate = AccountProofRoundRequest::new(
        block_hash,
        [
            AccountCodeClaim::new(contract, code_hash),
            AccountCodeClaim::new(contract, code_hash),
        ],
    );
    assert_eq!(
        provider.fetch(&duplicate),
        Err(AccountProofRoundFetchError::DuplicateRequest { address: contract })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let proof = AccountProof {
        storage_hash: B256::repeat_byte(0x74),
        balance: U256::ZERO,
        nonce: 1,
        code_hash,
        slots: Vec::new(),
    };
    let duplicate_proof = proof.clone();
    let provider = AccountProofRoundFetcher::new(Arc::new(move |_, _| {
        vec![
            (contract, Ok(proof.clone())),
            (contract, Ok(duplicate_proof.clone())),
        ]
    }));
    let request =
        AccountProofRoundRequest::new(block_hash, [AccountCodeClaim::new(contract, code_hash)]);
    assert_eq!(
        provider.fetch(&request),
        Err(AccountProofRoundFetchError::DuplicateResult { address: contract })
    );

    let provider = AccountProofRoundFetcher::new(Arc::new(move |_, _| {
        vec![(
            unexpected,
            Err(evm_fork_cache::StorageFetchError::custom("unexpected")),
        )]
    }));
    assert_eq!(
        provider.fetch(&request),
        Err(AccountProofRoundFetchError::UnexpectedResult {
            address: unexpected,
        })
    );

    let provider = AccountProofRoundFetcher::new(Arc::new(move |_, _| Vec::new()));
    assert_eq!(
        provider.fetch(&request),
        Err(AccountProofRoundFetchError::MissingResult { address: contract })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_prepared_account_rejects_the_whole_patch() -> Result<()> {
    let mut cache = setup_cache().await?;
    let valid = Address::repeat_byte(0x55);
    let invalid = Address::repeat_byte(0x56);
    let block_hash = B256::repeat_byte(0xb4);
    let code = Bytes::from_static(&[0x60, 0x00, 0x56]);
    let code_hash = keccak256(&code);
    cache.set_block(BlockId::from((block_hash, Some(true))));
    let proof = |hash| AccountProof {
        storage_hash: B256::repeat_byte(0x75),
        balance: U256::from(11),
        nonce: 2,
        code_hash: hash,
        slots: Vec::new(),
    };
    let patch = PreparedAccountPatch::new(
        block_hash,
        19,
        [
            PreparedAccountValue::new(valid, proof(code_hash), code.clone()),
            PreparedAccountValue::new(invalid, proof(B256::repeat_byte(0xff)), code.clone()),
        ],
    );

    let error = cache
        .apply_prepared_account_patch(&patch)
        .expect_err("one invalid account must reject every prepared account");

    assert_eq!(
        error,
        PreparedAccountPatchError::CodeHashMismatch {
            address: invalid,
            expected: B256::repeat_byte(0xff),
            actual: code_hash,
        }
    );
    assert!(cache.code_seed_state(&valid).is_none());
    assert!(cache.code_seed_state(&invalid).is_none());
    assert!(
        !cache.db_mut().cache.accounts.contains_key(&valid),
        "a valid value preceding the rejection must remain unapplied"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_prepared_account_hash_cannot_promote_pending_code() -> Result<()> {
    let mut cache = setup_cache().await?;
    let contract = Address::repeat_byte(0x57);
    let current_hash = B256::repeat_byte(0xb5);
    let stale_hash = B256::repeat_byte(0xa5);
    let code = Bytes::from_static(&[0x60, 0x00, 0x56]);
    let code_hash = keccak256(&code);
    cache.set_block(BlockId::from((current_hash, Some(true))));
    cache.seed_account_code(contract, code.clone())?;
    let patch = PreparedAccountPatch::new(
        stale_hash,
        20,
        [PreparedAccountValue::new(
            contract,
            AccountProof {
                storage_hash: B256::repeat_byte(0x76),
                balance: U256::from(12),
                nonce: 3,
                code_hash,
                slots: Vec::new(),
            },
            code,
        )],
    );

    let error = cache
        .apply_prepared_account_patch(&patch)
        .expect_err("a stale worker proof must not promote its seed");

    assert_eq!(
        error,
        PreparedAccountPatchError::BaselineMismatch {
            prepared: stale_hash,
            cache: Some(current_hash),
        }
    );
    assert!(matches!(
        cache.code_seed_state(&contract),
        Some(CodeSeedState::Pending { code_hash: pending }) if *pending == code_hash
    ));
    assert_eq!(cache.pending_code_seeds(), vec![contract]);

    Ok(())
}
