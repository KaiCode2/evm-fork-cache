//! Acceptance tests for cache-owned execution read-set warming and hydration.

use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::{RootProvider, network::AnyNetwork};
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_eth::TransactionRequest;
use alloy_transport::mock::Asserter;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::{
    AccountProof, ReadSetWarmupBatch, ReadSetWarmupCall, ReadSetWarmupConfig,
    ReadSetWarmupStrategy, StorageAccessList,
};
use revm::state::{AccountInfo, Bytecode};

async fn cache() -> EvmCache {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    EvmCache::new(Arc::new(provider)).await
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

    let report = cache.prewarm_read_sets(
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
    );

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
