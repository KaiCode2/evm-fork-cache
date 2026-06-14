//! Multicall3 batching support for EvmCache.
//!
//! This module provides utilities to batch multiple view calls into a single
//! EVM execution using the Multicall3 contract. This significantly reduces
//! the number of RPC round-trips needed when loading pool state.
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

/// Maximum number of calls to batch in a single multicall.
/// This prevents hitting gas limits or creating overly large calldata.
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

/// A batch of calls to execute via Multicall3.
pub struct MulticallBatch {
    calls: Vec<IMulticall3::Call3>,
}

impl MulticallBatch {
    /// Create a new empty batch.
    pub fn new() -> Self {
        Self { calls: Vec::new() }
    }

    /// Create a new batch with pre-allocated capacity.
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

    /// Add a typed call to the batch.
    pub fn add_call<C: SolCall>(
        &mut self,
        target: Address,
        call: C,
        allow_failure: bool,
    ) -> &mut Self {
        self.add(target, call.abi_encode().into(), allow_failure)
    }

    /// Get the number of calls in the batch.
    pub fn len(&self) -> usize {
        self.calls.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// Execute the batch using the provided EvmCache.
    ///
    /// Returns a vector of results, one for each call in the batch.
    /// Failed calls (when allow_failure was true) will have `success = false`.
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

    /// Execute the batch and return both results and the access list of all
    /// accounts/storage slots touched during execution.
    ///
    /// Same as [`Self::execute`] but uses `call_raw_with_access_list` to capture
    /// the EVM state touched by the multicall, enabling prefetch on the next cycle.
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
/// This helper handles splitting large call sets into multiple batches
/// that respect the MAX_BATCH_SIZE limit.
///
/// # Arguments
/// * `cache` - The EvmCache to execute calls on
/// * `calls` - Iterator of (target, calldata, allow_failure) tuples
///
/// # Returns
/// A vector of results in the same order as the input calls.
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

/// Decode a multicall result into the expected return type.
pub fn decode_result<C: SolCall>(result: &IMulticall3::Result) -> Result<C::Return> {
    if !result.success {
        return Err(anyhow!("Call failed"));
    }

    C::abi_decode_returns(&result.returnData)
        .map_err(|e| anyhow!("Failed to decode result: {:?}", e))
}

/// Decode a multicall result, returning None on failure instead of error.
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
