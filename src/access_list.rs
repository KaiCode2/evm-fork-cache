//! Selective EIP-2930 access list builder.
//!
//! Builds an access list containing only well-known, low-entropy storage slots
//! (V3 slot0/liquidity, V2 reserves) whose serialized form is mostly zero bytes
//! and cheap to post as L1 data. Skips keccak-derived mapping keys (tick bitmap,
//! tick info) which are 32 random bytes and expensive on L1.
//!
//! On L2 (Arbitrum): Automatically disables itself when L1 fees rise high enough
//! that the L1 data cost exceeds the L2 execution savings.
//!
//! On L1 (Ethereum): Access lists always save gas (no L1 data posting overhead),
//! so use `into_access_list_always()` to skip the profitability check.

use alloy_eips::eip2930::{AccessList, AccessListItem};
use alloy_network::Network;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::Provider;
use alloy_sol_types::{SolCall, sol};
use anyhow::Result;
use revm::context::result::ExecutionResult;
use tracing::{debug, info};

use crate::cache::EvmCache;

/// Arbitrum ArbGasInfo precompile.
const ARB_GAS_INFO: Address = address!("000000000000000000000000000000000000006C");

/// Optimism GasPriceOracle predeploy (Bedrock+).
pub const OP_GAS_PRICE_ORACLE: Address = address!("420000000000000000000000000000000000000F");

/// Chain fee model used when deciding whether an access list is worth posting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainType {
    /// Ethereum L1-like chains where access lists do not incur rollup data fees.
    L1,
    /// Arbitrum-style rollups with ArbGasInfo pricing.
    Arbitrum,
    /// OP Stack rollups with GasPriceOracle pricing.
    OpStack,
}

sol! {
    #[sol(rpc)]
    interface ArbGasInfo {
        function getPricesInWei() external view returns (
            uint256 perL2Tx,
            uint256 perL1CalldataUnit,
            uint256 perStorageUnit,
            uint256 perArbGas,
            uint256 perL1Surplus,
            uint256 baseFee
        );
        function getL1BaseFeeEstimate() external view returns (uint256);
    }

    #[sol(rpc)]
    interface OpGasPriceOracle {
        function l1BaseFee() external view returns (uint256);
        function getL1Fee(bytes _data) external view returns (uint256);
    }
}

/// A selective EIP-2930 access list built from the execution plan.
///
/// Only includes entries where the L2 execution savings (100 gas each)
/// are likely to exceed the L1 data posting cost of the serialized entry.
pub struct SmartAccessList {
    items: Vec<AccessListItem>,
}

impl SmartAccessList {
    /// Create an empty smart access-list builder.
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Create a builder from precomputed EIP-2930 items.
    pub fn from_items(items: Vec<AccessListItem>) -> Self {
        Self { items }
    }

    /// Add an address to the access list (address-only, no specific storage keys).
    /// Useful for contracts that are accessed on every call.
    pub fn add_address(&mut self, address: Address) {
        if !self.items.iter().any(|item| item.address == address) {
            self.items.push(AccessListItem {
                address,
                storage_keys: Vec::new(),
            });
        }
    }

    /// Add one storage key for an address, deduplicating both address and key.
    pub fn add_storage_key(&mut self, address: Address, storage_key: B256) {
        if let Some(item) = self.items.iter_mut().find(|item| item.address == address) {
            push_unique(&mut item.storage_keys, storage_key);
        } else {
            self.items.push(AccessListItem {
                address,
                storage_keys: vec![storage_key],
            });
        }
    }

    /// Return the access list unconditionally.
    ///
    /// On L1 chains there is no L1 data posting overhead, so access lists
    /// always save gas (100 gas per warm-vs-cold SLOAD). Returns `None`
    /// only when the list is empty.
    pub fn into_access_list_always(self) -> Option<AccessList> {
        if self.items.is_empty() {
            return None;
        }
        info!(
            items = self.items.len(),
            "Using access list unconditionally (L1 mode)"
        );
        Some(AccessList(self.items))
    }

    /// Evaluate profitability against current L1/L2 gas prices and return
    /// the access list only if it saves money.
    ///
    /// Queries the ArbGasInfo precompile for pricing, then compares the
    /// L2 execution savings (100 gas per entry) against the L1 data cost
    /// of serializing each entry.
    ///
    /// Returns `Ok(None)` if unprofitable or on pricing query failure.
    pub async fn into_access_list_if_profitable<P: Provider>(
        self,
        provider: &P,
    ) -> Result<Option<AccessList>> {
        if self.items.is_empty() {
            return Ok(None);
        }

        // Query ArbGasInfo for current pricing
        let arb = ArbGasInfo::new(ARB_GAS_INFO, provider);
        let prices_call = arb.getPricesInWei();
        let prices = match prices_call.call().await {
            Ok(p) => p,
            Err(e) => {
                debug!(error = %e, "Failed to query ArbGasInfo prices, skipping access list");
                return Ok(None);
            }
        };
        let l1_fee_call = arb.getL1BaseFeeEstimate();
        let l1_base_fee = match l1_fee_call.call().await {
            Ok(fee) => fee,
            Err(e) => {
                debug!(error = %e, "Failed to query L1 base fee, skipping access list");
                return Ok(None);
            }
        };

        let l2_gas_price = prices.perArbGas;

        if l2_gas_price.is_zero() || l1_base_fee.is_zero() {
            debug!("L1 or L2 gas price is zero, skipping access list");
            return Ok(None);
        }

        // Calculate aggregate L2 savings and L1 cost
        let mut total_entries: u64 = 0;
        let mut total_l1_data_gas: u64 = 0;

        for item in &self.items {
            total_entries += 1;
            total_l1_data_gas += l1_data_gas_for_bytes(item.address.as_slice());
            // RLP overhead per address entry (~3-4 bytes, assume non-zero)
            total_l1_data_gas += 4 * 16;

            for key in &item.storage_keys {
                total_entries += 1;
                total_l1_data_gas += l1_data_gas_for_bytes(key.as_slice());
                // RLP length prefix (1 byte, non-zero)
                total_l1_data_gas += 16;
            }
        }
        // Top-level RLP list headers (~3 bytes)
        total_l1_data_gas += 3 * 16;

        // L2 savings: 100 gas per entry × L2 gas price
        let l2_savings_wei = U256::from(total_entries) * U256::from(100) * l2_gas_price;
        // L1 cost: serialized data gas × L1 base fee
        let l1_cost_wei = U256::from(total_l1_data_gas) * l1_base_fee;

        let profitable = l2_savings_wei > l1_cost_wei;

        info!(
            entries = total_entries,
            items = self.items.len(),
            l2_savings_wei = %l2_savings_wei,
            l1_cost_wei = %l1_cost_wei,
            l2_gas_price_gwei = %format_gwei(l2_gas_price),
            l1_base_fee_gwei = %format_gwei(l1_base_fee),
            profitable,
            "Access list profitability check"
        );

        if profitable {
            Ok(Some(AccessList(self.items)))
        } else {
            Ok(None)
        }
    }
}

/// Evaluate whether an existing access list is profitable on L2 chains.
///
/// On L2, each access list entry saves L2 execution gas (warm vs cold access)
/// but costs L1 data posting gas for its serialized bytes. This function
/// computes the net and returns the access list only if profitable.
///
/// Returns `Ok(None)` if the list is empty, unprofitable, or pricing queries fail.
pub async fn access_list_if_profitable<P: Provider>(
    access_list: AccessList,
    provider: &P,
) -> Result<Option<AccessList>> {
    if access_list.0.is_empty() {
        return Ok(None);
    }

    // Query ArbGasInfo for current pricing
    let arb = ArbGasInfo::new(ARB_GAS_INFO, provider);
    let prices = match arb.getPricesInWei().call().await {
        Ok(p) => p,
        Err(e) => {
            debug!(error = %e, "Failed to query ArbGasInfo prices, skipping access list");
            return Ok(None);
        }
    };
    let l1_base_fee = match arb.getL1BaseFeeEstimate().call().await {
        Ok(fee) => fee,
        Err(e) => {
            debug!(error = %e, "Failed to query L1 base fee, skipping access list");
            return Ok(None);
        }
    };

    let l2_gas_price = prices.perArbGas;

    if l2_gas_price.is_zero() || l1_base_fee.is_zero() {
        debug!("L1 or L2 gas price is zero, skipping access list");
        return Ok(None);
    }

    // Calculate aggregate L2 savings and L1 cost
    let mut total_entries: u64 = 0;
    let mut total_l1_data_gas: u64 = 0;

    for item in &access_list.0 {
        total_entries += 1;
        total_l1_data_gas += l1_data_gas_for_bytes(item.address.as_slice());
        // RLP overhead per address entry (~3-4 bytes, assume non-zero)
        total_l1_data_gas += 4 * 16;

        for key in &item.storage_keys {
            total_entries += 1;
            total_l1_data_gas += l1_data_gas_for_bytes(key.as_slice());
            // RLP length prefix (1 byte, non-zero)
            total_l1_data_gas += 16;
        }
    }
    // Top-level RLP list headers (~3 bytes)
    total_l1_data_gas += 3 * 16;

    // L2 savings: 100 gas per entry × L2 gas price
    let l2_savings_wei = U256::from(total_entries) * U256::from(100) * l2_gas_price;
    // L1 cost: serialized data gas × L1 base fee
    let l1_cost_wei = U256::from(total_l1_data_gas) * l1_base_fee;

    let profitable = l2_savings_wei > l1_cost_wei;

    info!(
        entries = total_entries,
        items = access_list.0.len(),
        l2_savings_wei = %l2_savings_wei,
        l1_cost_wei = %l1_cost_wei,
        l2_gas_price_gwei = %format_gwei(l2_gas_price),
        l1_base_fee_gwei = %format_gwei(l1_base_fee),
        profitable,
        "Simulation access list profitability check"
    );

    if profitable {
        Ok(Some(access_list))
    } else {
        Ok(None)
    }
}

/// Select the appropriate access list strategy based on chain type.
///
/// - **L1**: Always include the simulation access list (no L1 data cost penalty).
///   Returns `None` only if the list is empty.
/// - **L2 (Arbitrum / OP stack)**: Include only when the L2 execution gas savings
///   exceed the L1 data posting cost, via [`access_list_if_profitable`].
pub async fn resolve_access_list<P: Provider>(
    sim_access_list: AccessList,
    provider: &P,
    chain_type: ChainType,
) -> Result<Option<AccessList>> {
    if chain_type == ChainType::L1 {
        if sim_access_list.0.is_empty() {
            Ok(None)
        } else {
            Ok(Some(sim_access_list))
        }
    } else {
        access_list_if_profitable(sim_access_list, provider).await
    }
}

/// Query the current L1 base fee estimate, dispatching to the correct predeploy
/// based on chain type. Returns `U256::ZERO` for L1 chains or on failure.
pub async fn query_l1_base_fee_for_chain<P, N>(provider: &P, chain_type: ChainType) -> U256
where
    P: Provider<N>,
    N: Network,
{
    match chain_type {
        ChainType::Arbitrum => {
            let arb = ArbGasInfo::new(ARB_GAS_INFO, provider);
            match arb.getL1BaseFeeEstimate().call().await {
                Ok(fee) => fee,
                Err(e) => {
                    debug!(error = %e, "Failed to query L1 base fee from ArbGasInfo");
                    U256::ZERO
                }
            }
        }
        ChainType::OpStack => {
            let oracle = OpGasPriceOracle::new(OP_GAS_PRICE_ORACLE, provider);
            match oracle.l1BaseFee().call().await {
                Ok(fee) => fee,
                Err(e) => {
                    debug!(error = %e, "Failed to query L1 base fee from OP GasPriceOracle");
                    U256::ZERO
                }
            }
        }
        ChainType::L1 => U256::ZERO,
    }
}

/// Compute the OP stack L1 data fee for a given transaction calldata.
///
/// Calls `GasPriceOracle.getL1Fee(bytes)` which handles the full Ecotone fee
/// model internally (base fee scalars, blob base fee, compression). This gives
/// the actual L1 data posting cost in wei, unlike the Arbitrum formula which
/// simply multiplies `calldata_gas * l1_base_fee`.
///
/// Returns `U256::ZERO` on any failure (e.g. predeploy not available).
pub fn compute_op_l1_fee(cache: &mut EvmCache, calldata: &[u8]) -> U256 {
    let encoded = OpGasPriceOracle::getL1FeeCall {
        _data: calldata.to_vec().into(),
    }
    .abi_encode();

    match cache.call_raw(Address::ZERO, OP_GAS_PRICE_ORACLE, encoded.into(), false) {
        Ok(ExecutionResult::Success { output, .. }) => {
            let out = output.into_data();
            OpGasPriceOracle::getL1FeeCall::abi_decode_returns(&out).unwrap_or(U256::ZERO)
        }
        Ok(_) => {
            debug!("GasPriceOracle.getL1Fee() reverted");
            U256::ZERO
        }
        Err(e) => {
            debug!(error = %e, "Failed to call GasPriceOracle.getL1Fee()");
            U256::ZERO
        }
    }
}

impl Default for SmartAccessList {
    fn default() -> Self {
        Self::new()
    }
}

fn push_unique(vec: &mut Vec<B256>, val: B256) {
    if !vec.contains(&val) {
        vec.push(val);
    }
}

/// L1 calldata gas for a byte slice: zero bytes = 4 gas, non-zero = 16 gas.
pub fn l1_data_gas_for_bytes(data: &[u8]) -> u64 {
    data.iter()
        .map(|&b| if b == 0 { 4u64 } else { 16u64 })
        .sum()
}

/// Filter already-warm and excluded addresses from an access list, then apply
/// it to the transaction request.
///
/// Removes entries for:
/// - `sender` — always warm as tx origin per EIP-2929
/// - `tx.to` — always warm as the destination per EIP-2929
/// - Any addresses in `exclude` — caller-excluded addresses
///
/// After filtering, sets the access list on `tx` (skipped if the list is empty).
pub fn apply_access_list(
    tx: &mut alloy_rpc_types_eth::TransactionRequest,
    access_list: &mut AccessList,
    sender: Address,
    exclude: &[Address],
) {
    let tx_to = tx.to.as_ref().and_then(|t| t.to().copied());
    access_list.0.retain(|item| {
        if Some(item.address) == tx_to || item.address == sender {
            return false;
        }
        if exclude.contains(&item.address) {
            return false;
        }
        true
    });
    if !access_list.0.is_empty() {
        *tx = std::mem::take(tx).access_list(access_list.clone());
    }
}

fn format_gwei(wei: U256) -> String {
    let gwei = wei / U256::from(1_000_000_000u64);
    let remainder = (wei % U256::from(1_000_000_000u64))
        .try_into()
        .unwrap_or(0u64);
    format!("{}.{:03}", gwei, remainder / 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_address_deduplicates_address_only_entries() {
        let address = Address::repeat_byte(0xAA);
        let mut al = SmartAccessList::new();

        al.add_address(address);
        al.add_address(address);

        let access_list = al.into_access_list_always().expect("non-empty");
        assert_eq!(access_list.0.len(), 1);
        assert_eq!(access_list.0[0].address, address);
        assert!(access_list.0[0].storage_keys.is_empty());
    }

    #[test]
    fn add_storage_key_deduplicates_keys() {
        let address = Address::repeat_byte(0xBB);
        let key = B256::from(U256::from(4));
        let mut al = SmartAccessList::new();

        al.add_storage_key(address, key);
        al.add_storage_key(address, key);

        let access_list = al.into_access_list_always().expect("should return Some");
        assert_eq!(access_list.0.len(), 1);
        assert_eq!(access_list.0[0].address, address);
        assert_eq!(access_list.0[0].storage_keys, vec![key]);
    }

    #[test]
    fn into_access_list_always_returns_none_when_empty() {
        let al = SmartAccessList::new();
        assert!(al.into_access_list_always().is_none());
    }

    #[test]
    fn l1_gas_for_zero_bytes_is_cheap() {
        let key = [0u8; 32];
        assert_eq!(l1_data_gas_for_bytes(&key), 128);
    }

    #[test]
    fn l1_gas_for_nonzero_address_bytes_is_expensive() {
        let addr = Address::repeat_byte(0xFF);
        assert_eq!(l1_data_gas_for_bytes(addr.as_slice()), 320);
    }
}
