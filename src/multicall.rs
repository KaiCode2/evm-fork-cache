//! Multicall3 batching support for EvmCache.
//!
//! This module provides utilities to batch multiple view calls into a single
//! EVM execution using the Multicall3 contract. This significantly reduces
//! the number of RPC round-trips needed when loading related contract state.
//!
//! Multicall3 is deployed at the same address on all EVM chains:
//! `0xcA11bde05977b3631167028862bE2a173976CA11`

use alloy_primitives::{Address, Bytes, address};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Result, anyhow};
use tracing::{debug, instrument};

use crate::access_set::StorageAccessList;
use crate::cache::EvmCache;

/// Multicall3 contract address (same on all EVM chains).
pub const MULTICALL3_ADDRESS: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

/// Maximum number of calls to batch in a single `aggregate3` invocation.
///
/// Caps per-batch gas and calldata size. [`execute_batched`] splits larger call
/// sets into chunks of at most this many calls.
pub const MAX_BATCH_SIZE: usize = 200;

sol! {
    /// Multicall3 contract interface.
    /// See: https://github.com/mds1/multicall
    #[sol(rpc)]
    contract IMulticall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }

        struct Result {
            bool success;
            bytes returnData;
        }

        /// Aggregate calls, returning the results.
        /// Reverts if any call fails when allowFailure is false.
        function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
    }
}

/// A batch of calls to execute in a single `aggregate3` invocation via Multicall3.
///
/// Build a batch with [`add`](Self::add) / [`add_call`](Self::add_call), then run
/// it with [`execute`](Self::execute) or [`execute_tracked`](Self::execute_tracked).
/// The batch should hold at most [`MAX_BATCH_SIZE`] calls; use [`execute_batched`]
/// to chunk larger sets automatically.
pub struct MulticallBatch {
    calls: Vec<IMulticall3::Call3>,
}

impl MulticallBatch {
    /// Create a new empty batch.
    ///
    /// ```
    /// use evm_fork_cache::multicall::MulticallBatch;
    ///
    /// let batch = MulticallBatch::new();
    /// assert!(batch.is_empty());
    /// assert_eq!(batch.len(), 0);
    /// ```
    pub fn new() -> Self {
        Self { calls: Vec::new() }
    }

    /// Create a new empty batch with room for `capacity` calls before reallocating.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            calls: Vec::with_capacity(capacity),
        }
    }

    /// Add a call to the batch.
    ///
    /// - `target`: The contract address to call
    /// - `call`: The encoded call data
    /// - `allow_failure`: If true, the call can fail without reverting the entire batch
    pub fn add(&mut self, target: Address, call_data: Bytes, allow_failure: bool) -> &mut Self {
        self.calls.push(IMulticall3::Call3 {
            target,
            allowFailure: allow_failure,
            callData: call_data,
        });
        self
    }

    /// Add a typed [`SolCall`] to the batch, ABI-encoding its calldata.
    ///
    /// Convenience wrapper over [`add`](Self::add) for callers holding a generated
    /// call type rather than raw bytes. As with `add`, `allow_failure` controls
    /// whether a revert of this call fails the whole batch (`false`) or surfaces as
    /// `success = false` in the result (`true`).
    pub fn add_call<C: SolCall>(
        &mut self,
        target: Address,
        call: C,
        allow_failure: bool,
    ) -> &mut Self {
        self.add(target, call.abi_encode().into(), allow_failure)
    }

    /// Number of calls currently in the batch.
    pub fn len(&self) -> usize {
        self.calls.len()
    }

    /// Returns `true` if the batch contains no calls.
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// Execute the batch against `cache`, returning one [`IMulticall3::Result`]
    /// per input call, in order. An empty batch returns an empty vector without
    /// touching the EVM.
    ///
    /// Per-call failure is reported in the result's `success` field rather than as
    /// an `Err`: a call added with `allow_failure = true` that reverts surfaces as
    /// `success = false` with whatever revert data it returned. The batch as a whole
    /// is all-or-nothing — a call added with `allow_failure = false` that reverts
    /// makes the entire `aggregate3` call revert, which is returned here as an `Err`.
    ///
    /// Requires Multicall3 to be deployed at [`MULTICALL3_ADDRESS`] on the forked
    /// chain (it is on virtually all EVM chains).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - the underlying `call_raw` execution does not return
    ///   [`ExecutionResult::Success`](revm::context::result::ExecutionResult::Success)
    ///   — e.g. the `aggregate3` call reverted because a call with
    ///   `allow_failure = false` failed, or Multicall3 is not deployed; or
    /// - the returned data cannot be ABI-decoded into the expected result list.
    #[instrument(skip(self, cache), fields(batch_size = self.calls.len()))]
    pub fn execute(&self, cache: &mut EvmCache) -> Result<Vec<IMulticall3::Result>> {
        if self.calls.is_empty() {
            return Ok(Vec::new());
        }

        let call = IMulticall3::aggregate3Call {
            calls: self.calls.clone(),
        };

        let result = cache.call_raw(
            Address::ZERO,
            MULTICALL3_ADDRESS,
            call.abi_encode().into(),
            false,
        )?;

        match result {
            revm::context::result::ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let results: Vec<IMulticall3::Result> =
                    IMulticall3::aggregate3Call::abi_decode_returns(&out)
                        .map_err(|e| anyhow!("Failed to decode multicall result: {:?}", e))?;

                debug!(results = results.len(), "multicall batch executed");

                Ok(results)
            }
            other => Err(anyhow!("Multicall failed: {:?}", other)),
        }
    }

    /// Execute the batch and return both the results and the
    /// [`StorageAccessList`] of all accounts/storage slots touched during
    /// execution.
    ///
    /// Same all-or-nothing batch semantics and Multicall3 deployment requirement
    /// as [`execute`](Self::execute), but uses `call_raw_with_access_list` to
    /// capture the EVM state touched by the multicall, enabling prefetch on the
    /// next cycle. An empty batch returns an empty result list and a default
    /// (empty) access list.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`execute`](Self::execute):
    /// the `aggregate3` call did not succeed (revert, or Multicall3 not deployed),
    /// or the returned data failed to ABI-decode.
    #[instrument(skip(self, cache), fields(batch_size = self.calls.len()))]
    pub fn execute_tracked(
        &self,
        cache: &mut EvmCache,
    ) -> Result<(Vec<IMulticall3::Result>, StorageAccessList)> {
        if self.calls.is_empty() {
            return Ok((Vec::new(), StorageAccessList::default()));
        }

        let call = IMulticall3::aggregate3Call {
            calls: self.calls.clone(),
        };

        let (result, access_list) = cache.call_raw_with_access_list(
            Address::ZERO,
            MULTICALL3_ADDRESS,
            call.abi_encode().into(),
        )?;

        match result {
            revm::context::result::ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let results: Vec<IMulticall3::Result> =
                    IMulticall3::aggregate3Call::abi_decode_returns(&out)
                        .map_err(|e| anyhow!("Failed to decode multicall result: {:?}", e))?;

                debug!(
                    results = results.len(),
                    "multicall batch executed (tracked)"
                );

                Ok((results, access_list))
            }
            other => Err(anyhow!("Multicall failed: {:?}", other)),
        }
    }
}

impl Default for MulticallBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Execute multiple calls in batches using Multicall3.
///
/// Splits large call sets into consecutive batches of at most [`MAX_BATCH_SIZE`]
/// calls, running each via [`MulticallBatch::execute`]. Results are concatenated
/// in input order. Requires Multicall3 to be deployed at [`MULTICALL3_ADDRESS`]
/// on the forked chain.
///
/// As with a single batch, the all-or-nothing semantics are per-batch: a call
/// added with `allow_failure = true` that reverts surfaces as `success = false`
/// in its result, whereas a call with `allow_failure = false` that reverts makes
/// that batch's `aggregate3` revert (returned here as an `Err`).
///
/// # Arguments
/// * `cache` - The EvmCache to execute calls on
/// * `calls` - Iterator of (target, calldata, allow_failure) tuples
///
/// # Returns
/// A vector of results in the same order as the input calls.
///
/// # Errors
///
/// Returns an error as soon as any chunk's [`MulticallBatch::execute`] fails —
/// i.e. that chunk's `aggregate3` reverted (a `allow_failure = false` call failed,
/// or Multicall3 is not deployed) or its return data failed to decode. Results
/// from earlier successful chunks are discarded.
#[instrument(skip(cache, calls))]
pub fn execute_batched<I>(cache: &mut EvmCache, calls: I) -> Result<Vec<IMulticall3::Result>>
where
    I: IntoIterator<Item = (Address, Bytes, bool)>,
{
    let calls: Vec<_> = calls.into_iter().collect();
    let total = calls.len();

    if total == 0 {
        return Ok(Vec::new());
    }

    let mut all_results = Vec::with_capacity(total);

    for chunk in calls.chunks(MAX_BATCH_SIZE) {
        let mut batch = MulticallBatch::with_capacity(chunk.len());
        for (target, calldata, allow_failure) in chunk {
            batch.add(*target, calldata.clone(), *allow_failure);
        }

        let results = batch.execute(cache)?;
        all_results.extend(results);
    }

    debug!(
        total_calls = total,
        batches = total.div_ceil(MAX_BATCH_SIZE),
        "executed batched multicalls"
    );

    Ok(all_results)
}

/// Decode a single multicall [`IMulticall3::Result`] into the call's typed return.
///
/// # Errors
///
/// Returns an error if `result.success` is `false` (the call reverted), or if
/// `result.returnData` cannot be ABI-decoded into `C::Return`.
pub fn decode_result<C: SolCall>(result: &IMulticall3::Result) -> Result<C::Return> {
    if !result.success {
        return Err(anyhow!("Call failed"));
    }

    C::abi_decode_returns(&result.returnData)
        .map_err(|e| anyhow!("Failed to decode result: {:?}", e))
}

/// Like [`decode_result`], but returns `None` instead of an `Err` when the call
/// did not succeed or its return data fails to decode.
pub fn try_decode_result<C: SolCall>(result: &IMulticall3::Result) -> Option<C::Return> {
    if !result.success {
        return None;
    }

    C::abi_decode_returns(&result.returnData).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multicall_batch_creation() {
        let mut batch = MulticallBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.add(Address::ZERO, Bytes::new(), false);
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_multicall3_address() {
        // Verify the Multicall3 address is correct
        let expected: Address = "0xcA11bde05977b3631167028862bE2a173976CA11"
            .parse()
            .unwrap();
        assert_eq!(MULTICALL3_ADDRESS, expected);
    }
}
