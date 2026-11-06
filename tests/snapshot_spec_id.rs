//! A local projection can preserve actual configured EVM rules without inference.
use alloy_provider::{RootProvider, network::AnyNetwork};
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use evm_fork_cache::cache::EvmCacheBuilder;
use revm::primitives::hardfork::SpecId;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_exposes_each_configured_execution_fork() {
    for spec in [SpecId::SHANGHAI, SpecId::CANCUN, SpecId::PRAGUE] {
        let provider = Arc::new(RootProvider::<AnyNetwork>::new(RpcClient::mocked(
            Asserter::new(),
        )));
        let mut cache = EvmCacheBuilder::new(provider)
            .chain_id(1)
            .spec(spec)
            .build()
            .await;
        assert_eq!(cache.snapshot().spec_id(), spec);
    }
}
