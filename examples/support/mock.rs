//! Shared, network-free plumbing for the offline examples.
//!
//! This is not part of the public API — it only exists so the examples can run
//! without an RPC endpoint. It builds an [`EvmCache`] over a mocked provider and
//! installs a `MockERC20` (see `fixtures/MockERC20.sol`) directly into the cache.
//!
//! Examples pull it in with:
//!
//! ```ignore
//! #[path = "support/mock.rs"]
//! mod mock;
//! ```
#![allow(dead_code)]

use std::sync::Arc;

use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::{SolCall, sol};
use alloy_transport::mock::Asserter;
use anyhow::{Result, anyhow};
use evm_fork_cache::cache::EvmCache;
use revm::context::result::ExecutionResult;
use revm::state::{AccountInfo, Bytecode};

/// Deployed (runtime) bytecode of the test `MockERC20` (balances at slot 3).
pub const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../../fixtures/mock_erc20_runtime.hex");
/// Creation bytecode of the test `MockERC20` (constructor: name, symbol, decimals).
pub const MOCK_ERC20_CREATION_HEX: &str = include_str!("../../fixtures/mock_erc20_creation.hex");

/// Storage slot of `MockERC20.balanceOf`.
pub const MOCK_ERC20_BALANCE_SLOT: u64 = 3;

sol! {
    interface MockERC20 {
        function balanceOf(address account) returns (uint256);
        function transfer(address to, uint256 amount) returns (bool);
    }
}

/// Decode the runtime bytecode fixture into a revm [`Bytecode`].
pub fn mock_erc20_runtime() -> Bytecode {
    let bytes = hex::decode(MOCK_ERC20_RUNTIME_HEX.trim()).expect("valid runtime hex");
    Bytecode::new_raw(Bytes::from(bytes))
}

/// Decode the creation bytecode fixture into raw bytes.
pub fn mock_erc20_creation_code() -> Vec<u8> {
    hex::decode(MOCK_ERC20_CREATION_HEX.trim()).expect("valid creation hex")
}

/// Build an [`EvmCache`] over a mocked provider — no network access.
pub async fn offline_cache() -> Result<EvmCache> {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    Ok(EvmCache::new(Arc::new(provider)).await)
}

/// Install a `MockERC20` account (with runtime bytecode) at `token`.
///
/// The account's storage is marked as fully local, so any slot that is not
/// explicitly seeded reads as zero rather than falling through to the (mocked)
/// RPC backend — exactly how a freshly-loaded forked contract behaves once its
/// storage is known.
pub fn install_mock_erc20(cache: &mut EvmCache, token: Address) {
    let bytecode = mock_erc20_runtime();
    let code_hash = bytecode.hash_slow();
    let info = AccountInfo {
        balance: U256::ZERO,
        nonce: 0,
        code: Some(bytecode),
        code_hash,
        account_id: None,
    };
    cache.db_mut().insert_account_info(token, info);
    cache
        .db_mut()
        .replace_account_storage(token, Default::default())
        .expect("mark mock storage as cleared");
}

/// Insert an empty (EOA-like) account at `addr`.
pub fn install_default_account(cache: &mut EvmCache, addr: Address) {
    cache
        .db_mut()
        .insert_account_info(addr, AccountInfo::default());
}

/// Read `balanceOf(owner)` from a `MockERC20` at `token`.
pub fn balance_of(cache: &mut EvmCache, token: Address, owner: Address) -> Result<U256> {
    let call = MockERC20::balanceOfCall { account: owner };
    let result = cache.call_raw(owner, token, Bytes::from(call.abi_encode()), false)?;
    match result {
        ExecutionResult::Success { output, .. } => Ok(
            MockERC20::balanceOfCall::abi_decode_returns(&output.into_data())?,
        ),
        other => Err(anyhow!("balanceOf call failed: {other:?}")),
    }
}
