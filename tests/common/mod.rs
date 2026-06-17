//! Shared helpers and fixtures for the integration tests.
//!
//! Every test here runs fully offline: the cache is built over a mocked
//! provider and all account/storage state is injected directly, so no test
//! ever reaches the network.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, U256, hex};
use alloy_provider::RootProvider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::{SolCall, sol};
use alloy_transport::mock::Asserter;
use anyhow::{Result, anyhow};
use evm_fork_cache::cache::{EvmCache, StorageBatchFetchFn};
use revm::context::result::ExecutionResult;
use revm::state::{AccountInfo, Bytecode};

/// Deployed (runtime) bytecode of the test `MockERC20` (balances at slot 3).
pub const MOCK_ERC20_RUNTIME_HEX: &str = include_str!("../../fixtures/mock_erc20_runtime.hex");
/// Creation bytecode of the test `MockERC20` (constructor: name, symbol, decimals).
pub const MOCK_ERC20_CREATION_HEX: &str = include_str!("../../fixtures/mock_erc20_creation.hex");

/// Storage slot of `MockERC20.balanceOf` (the third declared state variable).
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

/// Build an `EvmCache` over a mocked provider (no network access).
pub async fn setup_cache() -> Result<EvmCache> {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    Ok(EvmCache::new(Arc::new(provider), None).await)
}

/// Insert a `MockERC20` account (with runtime bytecode) at `token`.
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

/// Read `balanceOf(owner)` from a `MockERC20` deployed at `token`.
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

/// Build a stub [`StorageBatchFetchFn`] that returns chosen "current" values.
///
/// `values` maps `(address, slot)` to the value the fetcher reports. Any
/// requested slot not present in the map is reported as `U256::ZERO` (matching
/// how an unseen slot reads in a simulation). This is the offline stand-in for
/// the real RPC batch fetcher.
pub fn stub_fetcher(values: HashMap<(Address, U256), U256>) -> StorageBatchFetchFn {
    Arc::new(
        move |requests: Vec<(Address, U256)>, _block: Option<BlockId>| {
            requests
                .into_iter()
                .map(|(addr, slot)| {
                    let value = values.get(&(addr, slot)).copied().unwrap_or(U256::ZERO);
                    (addr, slot, Ok(value))
                })
                .collect()
        },
    )
}

/// Build a stub [`StorageBatchFetchFn`] that fails every request.
///
/// Used to exercise the `Unverified` / error paths offline.
pub fn failing_fetcher() -> StorageBatchFetchFn {
    Arc::new(|requests: Vec<(Address, U256)>, _block: Option<BlockId>| {
        requests
            .into_iter()
            .map(|(addr, slot)| (addr, slot, Err(anyhow!("stub fetcher error"))))
            .collect()
    })
}

/// A one-shot synchronous gate: a holder blocks in [`wait`](Gate::wait) until
/// some other thread calls [`release`](Gate::release). Cloning shares the same
/// underlying state, and `release` is sticky — once released, every present and
/// future `wait` returns immediately.
///
/// Used by the Drop-abort test to make the background validator's fetch
/// deterministically ordered *after* the drop. The fetcher (running on a worker
/// thread) cannot return — and therefore the validator cannot reach its
/// post-fetch checkpoint — until the test has dropped the `SpeculativeSim` and
/// released the gate, eliminating the spawn/poll race regardless of how the
/// multi-thread scheduler interleaves the two threads.
///
/// Built on a `Mutex<bool>` + `Condvar` so the whole thing is `Send + Sync`,
/// which a [`StorageBatchFetchFn`] closure must be.
#[derive(Clone, Default)]
pub struct Gate {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Block until [`release`](Gate::release) has been called (returns
    /// immediately if it already has).
    pub fn wait(&self) {
        let (lock, cv) = &*self.inner;
        let mut released = lock.lock().unwrap_or_else(|e| e.into_inner());
        while !*released {
            released = cv.wait(released).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Wake any current waiter and let all future waiters pass.
    pub fn release(&self) {
        let (lock, cv) = &*self.inner;
        *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
        cv.notify_all();
    }
}

/// Build a stub [`StorageBatchFetchFn`] that reports chosen values but blocks on
/// `gate` before returning.
///
/// Used by the Drop-abort test: by releasing the gate only *after* dropping the
/// `SpeculativeSim`, the test guarantees the validator's fetch completes (and so
/// its post-fetch, correction-queuing checkpoint runs) strictly after the
/// cancel flag is set — so the dropped speculation can never queue a correction,
/// no matter how the scheduler races the two threads. The returned values
/// otherwise behave exactly like [`stub_fetcher`].
pub fn gated_tracking_fetcher(
    values: HashMap<(Address, U256), U256>,
    gate: Gate,
) -> StorageBatchFetchFn {
    Arc::new(
        move |requests: Vec<(Address, U256)>, _block: Option<BlockId>| {
            gate.wait();
            requests
                .into_iter()
                .map(|(addr, slot)| {
                    let value = values.get(&(addr, slot)).copied().unwrap_or(U256::ZERO);
                    (addr, slot, Ok(value))
                })
                .collect()
        },
    )
}

/// Build a stub [`StorageBatchFetchFn`] that panics, to exercise the validator's
/// `JoinError` (`Unverified`) path.
pub fn panicking_fetcher() -> StorageBatchFetchFn {
    Arc::new(
        |_requests: Vec<(Address, U256)>,
         _block: Option<BlockId>|
         -> Vec<(Address, U256, Result<U256>)> {
            panic!("panicking fetcher: deliberate failure for the Unverified test")
        },
    )
}

/// Submit a `transfer(to, amount)` to a `MockERC20`, committing the state change.
pub fn transfer(
    cache: &mut EvmCache,
    token: Address,
    from: Address,
    to: Address,
    amount: U256,
) -> Result<ExecutionResult> {
    let call = MockERC20::transferCall { to, amount };
    cache.call_raw(from, token, Bytes::from(call.abi_encode()), true)
}
