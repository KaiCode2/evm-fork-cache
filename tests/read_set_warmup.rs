//! Acceptance tests for cache-owned execution read-set warming and hydration.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::{RootProvider, network::AnyNetwork};
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::TransactionRequest;
use alloy_transport::mock::Asserter;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::{
    AccountProof, ReadSetHydrationFailure, ReadSetWarmupBatch, ReadSetWarmupCall,
    ReadSetWarmupConfig, ReadSetWarmupError, ReadSetWarmupStrategy, StorageAccessList,
    StorageFetchError,
};
use revm::primitives::hardfork::SpecId;
use revm::state::{AccountInfo, Bytecode};

async fn cache() -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    EvmCache::new(Arc::new(provider)).await
}

async fn cache_without_fetchers() -> EvmCache {
    let base = cache().await;
    EvmCache::from_backend(
        base.unchecked_backend().clone(),
        base.unchecked_blockchain_db().clone(),
        base.block(),
        base.chain_id(),
        None,
        None,
        SpecId::CANCUN,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn required_access_list_discovery_fails_when_no_fetcher_is_installed() {
    let mut cache = cache_without_fetchers().await;
    let error = cache
        .prewarm_read_sets(
            ReadSetWarmupBatch {
                known_slots: Vec::new(),
                calls: vec![ReadSetWarmupCall {
                    tx: TransactionRequest::default().to(Address::repeat_byte(0x41)),
                    expected_slots: Some(1),
                    restrict_to: None,
                }],
            },
            ReadSetWarmupConfig {
                strategy: ReadSetWarmupStrategy::AccessList,
                ..Default::default()
            },
        )
        .expect_err("required access-list discovery must not silently skip");

    assert!(matches!(
        error,
        ReadSetWarmupError::AccessListFetcherUnavailable { calls: 1 }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn access_list_discovery_rejects_result_count_mismatches() {
    for actual in [0, 2] {
        let mut cache = cache().await;
        cache.set_access_list_fetcher(Arc::new(move |_requests, _block| {
            (0..actual)
                .map(|_| Ok(StorageAccessList::default()))
                .collect()
        }));

        let error = cache
            .prewarm_read_sets(
                ReadSetWarmupBatch {
                    known_slots: Vec::new(),
                    calls: vec![ReadSetWarmupCall {
                        tx: TransactionRequest::default().to(Address::repeat_byte(0x42)),
                        expected_slots: Some(1),
                        restrict_to: None,
                    }],
                },
                ReadSetWarmupConfig {
                    strategy: ReadSetWarmupStrategy::AccessList,
                    ..Default::default()
                },
            )
            .expect_err("fetcher result cardinality is part of the public contract");

        assert!(matches!(
            error,
            ReadSetWarmupError::AccessListResultCountMismatch {
                expected: 1,
                actual: observed,
            } if observed == actual
        ));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn discovery_contract_errors_do_not_partially_warm_known_slots() {
    let mut cache = cache().await;
    let target = Address::repeat_byte(0x54);
    let slot = U256::from(9);
    let fetches = Arc::new(AtomicUsize::new(0));
    let observed_fetches = Arc::clone(&fetches);
    cache.set_storage_batch_fetcher(Arc::new(move |requests, _block| {
        observed_fetches.fetch_add(1, Ordering::SeqCst);
        requests
            .into_iter()
            .map(|(address, slot)| (address, slot, Ok(U256::from(77))))
            .collect()
    }));
    cache.set_access_list_fetcher(Arc::new(|_requests, _block| Vec::new()));

    let error = cache
        .prewarm_read_sets(
            ReadSetWarmupBatch {
                known_slots: vec![(target, slot)],
                calls: vec![ReadSetWarmupCall {
                    tx: TransactionRequest::default().to(target),
                    expected_slots: Some(1),
                    restrict_to: None,
                }],
            },
            ReadSetWarmupConfig {
                strategy: ReadSetWarmupStrategy::AccessList,
                ..Default::default()
            },
        )
        .expect_err("a malformed discovery batch must fail before cache mutation");

    assert!(matches!(
        error,
        ReadSetWarmupError::AccessListResultCountMismatch {
            expected: 1,
            actual: 0
        }
    ));
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
    assert_eq!(cache.cached_storage_value(target, slot), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_discovery_saturates_extreme_slot_hints() {
    let mut cache = cache().await;
    cache.set_access_list_fetcher(Arc::new(|requests, _block| {
        requests
            .into_iter()
            .map(|_| Ok(StorageAccessList::default()))
            .collect()
    }));

    let report = cache
        .prewarm_read_sets(
            ReadSetWarmupBatch {
                known_slots: Vec::new(),
                calls: vec![
                    ReadSetWarmupCall {
                        tx: TransactionRequest::default().to(Address::repeat_byte(0x43)),
                        expected_slots: Some(usize::MAX),
                        restrict_to: None,
                    },
                    ReadSetWarmupCall {
                        tx: TransactionRequest::default().to(Address::repeat_byte(0x44)),
                        expected_slots: Some(1),
                        restrict_to: None,
                    },
                ],
            },
            ReadSetWarmupConfig::default(),
        )
        .expect("automatic access-list discovery");

    assert!(report.used_access_lists);
    assert_eq!(report.access_list_successes, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_hydration_reports_a_typed_missing_proof_fetcher() {
    let mut cache = cache_without_fetchers().await;
    let target = Address::repeat_byte(0x45);
    let required = StorageAccessList {
        accounts: [target].into_iter().collect(),
        ..Default::default()
    };

    let report = cache.hydrate_read_set(&required);

    assert!(!report.is_complete());
    assert!(matches!(
        report.failures.as_slice(),
        [ReadSetHydrationFailure::ProofFetcherUnavailable { address }] if *address == target
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_hydration_rejects_duplicate_and_unexpected_proof_results() {
    let mut cache = cache().await;
    let target = Address::repeat_byte(0x46);
    let unexpected = Address::repeat_byte(0x47);
    cache.set_account_proof_fetcher(Arc::new(move |_requests, _block| {
        let proof = AccountProof {
            storage_hash: B256::ZERO,
            balance: U256::ZERO,
            nonce: 0,
            code_hash: B256::ZERO,
            slots: Vec::new(),
        };
        vec![
            (target, Ok(proof.clone())),
            (target, Ok(proof.clone())),
            (unexpected, Ok(proof)),
        ]
    }));
    let required = StorageAccessList {
        accounts: [target].into_iter().collect(),
        ..Default::default()
    };

    let report = cache.hydrate_read_set(&required);

    assert!(!report.is_complete());
    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::ProofResultDuplicate { address } if *address == target
    )));
    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::ProofResultUnexpected { address } if *address == unexpected
    )));
    assert_eq!(report.accounts_refreshed, 0);
    assert!(report.missing_after.accounts.contains(&target));
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_hydration_rejects_duplicate_and_unexpected_proof_slots() {
    let mut cache = cache().await;
    let target = Address::repeat_byte(0x55);
    let requested = U256::from(1);
    let unexpected = U256::from(2);
    cache.set_account_proof_fetcher(Arc::new(move |_requests, _block| {
        vec![(
            target,
            Ok(AccountProof {
                storage_hash: B256::ZERO,
                balance: U256::ZERO,
                nonce: 0,
                code_hash: B256::ZERO,
                slots: vec![
                    (requested, U256::from(10)),
                    (requested, U256::from(11)),
                    (unexpected, U256::from(12)),
                ],
            }),
        )]
    }));
    let required = StorageAccessList {
        accounts: [target].into_iter().collect(),
        slots: [(target, requested)].into_iter().collect(),
        ..Default::default()
    };

    let report = cache.hydrate_read_set(&required);

    assert!(!report.is_complete());
    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::StorageSlotDuplicate { address, slot }
            if *address == target && *slot == requested
    )));
    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::StorageSlotUnexpected { address, slot }
            if *address == target && *slot == unexpected
    )));
    assert_eq!(cache.cached_storage_value(target, requested), None);
    assert!(report.missing_after.slots.contains(&(target, requested)));
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_skips_calls_only_when_remote_discovery_is_not_selected() {
    let target = Address::repeat_byte(0x48);
    for config in [
        ReadSetWarmupConfig {
            strategy: ReadSetWarmupStrategy::LocalOnly,
            ..Default::default()
        },
        ReadSetWarmupConfig {
            strategy: ReadSetWarmupStrategy::Auto,
            ..Default::default()
        },
    ] {
        let mut cache = cache_without_fetchers().await;
        let report = cache
            .prewarm_read_sets(
                ReadSetWarmupBatch {
                    known_slots: Vec::new(),
                    calls: vec![ReadSetWarmupCall {
                        tx: TransactionRequest::default().to(target),
                        expected_slots: Some(1),
                        restrict_to: None,
                    }],
                },
                config,
            )
            .expect("policy did not select remote discovery");

        assert!(!report.used_access_lists);
        assert_eq!(report.skipped_calls, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn access_list_discovery_preserves_per_call_failures() {
    let mut cache = cache().await;
    cache.set_access_list_fetcher(Arc::new(|_requests, _block| {
        vec![Err(evm_fork_cache::AccessListError::query(
            "eth_createAccessList",
            "provider rejected the call",
        ))]
    }));

    let report = cache
        .prewarm_read_sets(
            ReadSetWarmupBatch {
                known_slots: Vec::new(),
                calls: vec![ReadSetWarmupCall {
                    tx: TransactionRequest::default().to(Address::repeat_byte(0x49)),
                    expected_slots: None,
                    restrict_to: None,
                }],
            },
            ReadSetWarmupConfig {
                strategy: ReadSetWarmupStrategy::AccessList,
                ..Default::default()
            },
        )
        .expect("the batch contract was valid");

    assert!(report.used_access_lists);
    assert_eq!(report.access_list_successes, 0);
    assert_eq!(report.access_list_failures.len(), 1);
    assert_eq!(report.access_list_failures[0].0, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_hydration_preserves_typed_partial_failure_causes() {
    let mut cache = cache().await;
    let omitted = Address::repeat_byte(0x4a);
    let provider_failed = Address::repeat_byte(0x4b);
    let runtime_missing = Address::repeat_byte(0x4c);
    let slot_missing = Address::repeat_byte(0x4d);
    let slot = U256::from(9);
    let deployed_hash = B256::repeat_byte(0x4e);
    cache.set_account_proof_fetcher(Arc::new(move |_requests, _block| {
        vec![
            (
                provider_failed,
                Err(StorageFetchError::custom("archive unavailable")),
            ),
            (
                runtime_missing,
                Ok(AccountProof {
                    storage_hash: B256::ZERO,
                    balance: U256::ZERO,
                    nonce: 1,
                    code_hash: deployed_hash,
                    slots: Vec::new(),
                }),
            ),
            (
                slot_missing,
                Ok(AccountProof {
                    storage_hash: B256::ZERO,
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash: B256::ZERO,
                    slots: Vec::new(),
                }),
            ),
        ]
    }));
    let required = StorageAccessList {
        accounts: [omitted, provider_failed, runtime_missing, slot_missing]
            .into_iter()
            .collect(),
        slots: [(slot_missing, slot)].into_iter().collect(),
        ..Default::default()
    };

    let report = cache.hydrate_read_set(&required);

    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::ProofResultMissing { address } if *address == omitted
    )));
    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::ProofFetch { address, source }
            if *address == provider_failed && source.to_string().contains("archive unavailable")
    )));
    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::RuntimeCodeUnavailable { address, code_hash }
            if *address == runtime_missing && *code_hash == deployed_hash
    )));
    assert!(report.failures.iter().any(|failure| matches!(
        failure,
        ReadSetHydrationFailure::StorageSlotMissing { address, slot: missing }
            if *address == slot_missing && *missing == slot
    )));
    assert!(!report.is_complete());
}

#[tokio::test(flavor = "multi_thread")]
async fn block_hash_dependencies_are_validated_from_canonical_cache_residency() {
    let mut cache = cache().await;
    let resident_number = 100_u64;
    let missing_number = 101_u64;
    cache
        .db_mut()
        .cache
        .block_hashes
        .insert(U256::from(resident_number), B256::repeat_byte(0x4f));

    let resident = cache.hydrate_read_set(&StorageAccessList {
        block_numbers: [resident_number].into_iter().collect(),
        ..Default::default()
    });
    let missing = cache.hydrate_read_set(&StorageAccessList {
        block_numbers: [missing_number].into_iter().collect(),
        ..Default::default()
    });

    assert!(resident.is_complete(), "{resident:?}");
    assert!(missing.failures.is_empty());
    assert_eq!(
        missing.missing_after.block_numbers,
        [missing_number].into_iter().collect()
    );
    assert!(!missing.is_complete());
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_owned_warmup_discovers_filters_and_loads_slots() {
    let mut cache = cache().await;
    let target = Address::repeat_byte(0x51);
    let unrelated = Address::repeat_byte(0x52);
    let slot = U256::from(7);
    cache.set_access_list_fetcher(Arc::new(move |requests, _block| {
        assert_eq!(requests.len(), 1);
        let mut access = StorageAccessList::default();
        access.accounts.extend([target, unrelated]);
        access.slots.extend([(target, slot), (unrelated, slot)]);
        vec![Ok(access)]
    }));
    cache.set_storage_batch_fetcher(Arc::new(move |requests, _block| {
        assert_eq!(requests, vec![(target, slot)]);
        vec![(target, slot, Ok(U256::from(99)))]
    }));

    let report = cache
        .prewarm_read_sets(
            ReadSetWarmupBatch {
                known_slots: Vec::new(),
                calls: vec![ReadSetWarmupCall {
                    tx: TransactionRequest::default().to(target),
                    expected_slots: Some(32),
                    restrict_to: Some(vec![target]),
                }],
            },
            ReadSetWarmupConfig {
                strategy: ReadSetWarmupStrategy::AccessList,
                ..Default::default()
            },
        )
        .expect("access-list warmup");

    assert!(report.used_access_lists);
    assert_eq!(report.access_list_successes, 1);
    assert_eq!(
        report.discovered_access.accounts,
        [target].into_iter().collect()
    );
    assert_eq!(
        report.discovered_access.slots,
        [(target, slot)].into_iter().collect()
    );
    assert_eq!(report.discovered.loaded, 1);
    assert_eq!(
        cache.cached_storage_value(target, slot),
        Some(U256::from(99))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn provider_backed_cache_installs_access_list_discovery() {
    assert!(cache().await.access_list_fetcher().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_read_set_hydration_refreshes_accounts_code_and_storage_together() {
    let mut cache = cache().await;
    let target = Address::repeat_byte(0x61);
    let slot = U256::from(8);
    let code = Bytecode::new_raw(Bytes::from_static(&[0x00]));
    let code_hash = code.hash_slow();
    cache.db_mut().insert_account_info(
        target,
        AccountInfo {
            balance: U256::from(1),
            nonce: 2,
            code_hash,
            code: Some(code),
            account_id: None,
        },
    );
    cache
        .insert_storage_slot(target, slot, U256::from(3))
        .expect("seed storage");
    cache.set_account_proof_fetcher(Arc::new(move |requests, _block| {
        assert_eq!(requests, vec![(target, vec![slot])]);
        vec![(
            target,
            Ok(AccountProof {
                storage_hash: B256::repeat_byte(0x62),
                balance: U256::from(10),
                nonce: 11,
                code_hash,
                slots: vec![(slot, U256::from(12))],
            }),
        )]
    }));
    let required = StorageAccessList {
        accounts: [target].into_iter().collect(),
        code_hashes: [code_hash].into_iter().collect(),
        slots: [(target, slot)].into_iter().collect(),
        ..Default::default()
    };

    let report = cache.hydrate_read_set(&required);

    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.accounts_refreshed, 1);
    assert_eq!(report.slots_refreshed, 1);
    assert_eq!(
        cache.cached_storage_value(target, slot),
        Some(U256::from(12))
    );
    assert!(cache.snapshot().missing_read_set(&required).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_read_set_hydration_rejects_code_layout_changes() {
    let mut cache = cache().await;
    let target = Address::repeat_byte(0x71);
    let code = Bytecode::new_raw(Bytes::from_static(&[0x00]));
    let code_hash = code.hash_slow();
    cache.db_mut().insert_account_info(
        target,
        AccountInfo {
            code_hash,
            code: Some(code),
            ..Default::default()
        },
    );
    let changed = B256::repeat_byte(0x72);
    cache.set_account_proof_fetcher(Arc::new(move |_, _| {
        vec![(
            target,
            Ok(AccountProof {
                storage_hash: B256::ZERO,
                balance: U256::ZERO,
                nonce: 1,
                code_hash: changed,
                slots: Vec::new(),
            }),
        )]
    }));
    let required = StorageAccessList {
        accounts: [target].into_iter().collect(),
        code_hashes: [code_hash].into_iter().collect(),
        ..Default::default()
    };

    let report = cache.hydrate_read_set(&required);

    assert!(!report.is_complete());
    assert_eq!(report.code_changes, vec![(target, code_hash, changed)]);
}
