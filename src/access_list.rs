//! EIP-2930 access list builder with L2 profitability accounting.
//!
//! The caller supplies the addresses and storage slots to include; this module
//! decides *whether attaching the list is profitable*, not *which slots are
//! interesting* (it carries no protocol-specific slot knowledge). The trade-off
//! is purely economic: pre-declaring an account/slot warms it (cheaper EIP-2929
//! execution) but costs L1 data to post the list. Slots whose serialized form is
//! mostly zero bytes (e.g. small, low-entropy values) are cheap to post; dense,
//! high-entropy 32-byte keys are expensive on L1 — so the value of a given entry
//! depends on its bytes, which is why the include/exclude decision is left to the
//! caller and the profitability check below.
//!
//! On L2, automatically disables itself when L1 fees rise high enough that the
//! L1 data cost exceeds the L2 execution savings. Arbitrum uses `ArbGasInfo`
//! pricing with exact EIP-2930 RLP data gas; OP Stack chains use
//! `GasPriceOracle.getL1Fee(bytes)` to compare whole transactions with and
//! without the access list.
//!
//! On L1 (Ethereum): Access lists always save gas (no L1 data posting overhead),
//! so use `into_access_list_always()` to skip the profitability check.

use alloy_eips::{
    BlockId, BlockNumberOrTag,
    eip2930::{AccessList, AccessListItem},
};
use alloy_network::{AnyNetwork, Network};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, address};
use alloy_provider::Provider;
use alloy_rlp::Encodable;
use alloy_rpc_types_eth::{TransactionInput, TransactionRequest};
use alloy_sol_types::{SolCall, sol};
use revm::context::result::ExecutionResult;
use tracing::{debug, info};

use crate::access_set::StorageAccessList;
use crate::cache::EvmCache;
use crate::errors::{AccessListError, AccessListResult as Result};

/// Arbitrum ArbGasInfo precompile.
const ARB_GAS_INFO: Address = address!("000000000000000000000000000000000000006C");

/// Optimism GasPriceOracle predeploy (Bedrock+).
///
/// Fixed predeploy address on every OP Stack chain. Queried for the L1 base fee
/// ([`query_l1_base_fee_for_chain`]) and the full Ecotone L1 data fee
/// ([`compute_op_l1_fee`]).
pub const OP_GAS_PRICE_ORACLE: Address = address!("420000000000000000000000000000000000000F");

/// Default gas cap for an `eth_createAccessList` probe.
///
/// The probe is a read-only call, but clients still run normal transaction
/// validation. This cap is intentionally generous for storage-heavy view calls
/// while keeping `gas * gasPrice` sender-funding checks bounded.
pub const DEFAULT_CREATE_ACCESS_LIST_GAS_CAP: u64 = 30_000_000;

/// Read-only call whose storage read set should be discovered with
/// `eth_createAccessList`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessListCall {
    /// Simulated sender.
    pub from: Address,
    /// Call target.
    pub to: Address,
    /// ABI-encoded call data.
    pub input: Bytes,
    /// Gas cap sent with the access-list probe.
    pub gas: u64,
    /// Optional gas price override. When unset, the probe uses the pinned
    /// block's `baseFeePerGas`, falling back to 1 gwei if it cannot be read.
    pub gas_price: Option<u128>,
}

impl AccessListCall {
    /// Build a probe request using [`DEFAULT_CREATE_ACCESS_LIST_GAS_CAP`].
    pub fn new(from: Address, to: Address, input: impl Into<Bytes>) -> Self {
        Self {
            from,
            to,
            input: input.into(),
            gas: DEFAULT_CREATE_ACCESS_LIST_GAS_CAP,
            gas_price: None,
        }
    }

    /// Override the gas cap used by the probe.
    pub fn with_gas(mut self, gas: u64) -> Self {
        self.gas = gas;
        self
    }

    /// Override the gas price used by the probe.
    pub fn with_gas_price(mut self, gas_price: u128) -> Self {
        self.gas_price = Some(gas_price);
        self
    }
}

/// Null-tolerant view of the `eth_createAccessList` response.
///
/// Some clients return `"storageKeys": null` for accounts a call touches
/// without reading storage. Alloy's EIP-2930 `AccessListItem` requires a vector,
/// so this RPC mirror accepts null and preserves the account-only touch.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAccessListProbe {
    #[serde(default)]
    access_list: Vec<CreateAccessListProbeItem>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAccessListProbeItem {
    address: Address,
    #[serde(default)]
    storage_keys: Option<Vec<B256>>,
}

/// Chain fee model used by helpers that only need to identify the chain's L1
/// base-fee oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainType {
    /// Ethereum L1-like chains where access lists do not incur rollup data fees.
    L1,
    /// Arbitrum-style rollups with ArbGasInfo pricing.
    Arbitrum,
    /// OP Stack rollups with GasPriceOracle pricing.
    OpStack,
}

/// Pricing inputs used when deciding whether to include a simulation access list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessListPricing {
    /// Ethereum L1-like chains where access lists do not incur rollup data fees.
    L1,
    /// Arbitrum-style rollups priced through the `ArbGasInfo` precompile.
    Arbitrum,
    /// OP Stack rollups priced by comparing oracle L1 fees for full tx bytes.
    OpStack {
        /// Serialized unsigned transaction bytes without an access list.
        tx_without_access_list: Bytes,
        /// Serialized unsigned transaction bytes with the candidate access list.
        tx_with_access_list: Bytes,
    },
}

/// Ask a provider to derive the account/storage touch set for `call` at `block`.
///
/// This is a generic wrapper over `eth_createAccessList`. It returns the
/// execution touch set as [`StorageAccessList`] so callers can prefetch the
/// storage pairs or convert it back to EIP-2930 form. Client execution errors
/// (for example a reverted view call) are surfaced as [`AccessListError`] rather
/// than being confused with an empty touch set.
pub async fn create_access_list_read_set<P>(
    provider: &P,
    block: BlockId,
    call: &AccessListCall,
) -> Result<StorageAccessList>
where
    P: Provider<AnyNetwork>,
{
    let gas_price = match call.gas_price {
        Some(gas_price) => gas_price,
        None => pinned_base_fee(provider, block)
            .await
            .unwrap_or(1_000_000_000),
    };
    let request = TransactionRequest {
        from: Some(call.from),
        to: Some(TxKind::Call(call.to)),
        input: TransactionInput::new(call.input.clone()),
        gas: Some(call.gas),
        gas_price: Some(gas_price),
        ..Default::default()
    };

    let result: CreateAccessListProbe = provider
        .client()
        .request("eth_createAccessList", (request, block))
        .await
        .map_err(|e| AccessListError::query("eth_createAccessList", e))?;
    if let Some(error) = result.error {
        return Err(AccessListError::query(
            "eth_createAccessList execution",
            error,
        ));
    }

    let mut access = StorageAccessList::default();
    for item in result.access_list {
        access.accounts.insert(item.address);
        if let Some(storage_keys) = item.storage_keys {
            access.slots.extend(
                storage_keys
                    .into_iter()
                    .map(|key| (item.address, U256::from_be_slice(key.as_slice()))),
            );
        }
    }
    Ok(access)
}

async fn pinned_base_fee<P>(provider: &P, block: BlockId) -> Option<u128>
where
    P: Provider<AnyNetwork>,
{
    let block = provider.get_block(block).await.ok().flatten()?;
    block.header.base_fee_per_gas.map(u128::from)
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

/// An EIP-2930 access list builder with L2 profitability accounting.
///
/// The caller decides which addresses/slots to add (via
/// [`add_address`](Self::add_address) / [`add_storage_key`](Self::add_storage_key));
/// the builder itself applies no per-entry selection. The `into_access_list_*`
/// finalizers decide whether attaching the *whole* list is profitable, comparing
/// its L1 data-posting cost against the L2 warm-access savings.
pub struct SmartAccessList {
    items: Vec<AccessListItem>,
}

impl SmartAccessList {
    /// Create an empty smart access-list builder.
    ///
    /// Populate it with [`SmartAccessList::add_address`] and
    /// [`SmartAccessList::add_storage_key`], then finalize with one of the
    /// `into_access_list_*` methods.
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Create a builder from precomputed EIP-2930 items.
    ///
    /// The items are taken as-is; this constructor does not deduplicate
    /// addresses or storage keys (unlike [`SmartAccessList::add_address`] and
    /// [`SmartAccessList::add_storage_key`]). Pass items that are already
    /// distinct, or rely on downstream encoders to fold duplicates.
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

    /// Evaluate Arbitrum profitability against current L1/L2 gas prices and
    /// return the access list only if it saves money.
    ///
    /// Queries the Arbitrum `ArbGasInfo` precompile for pricing, then compares
    /// the L2 execution savings against the estimated L1 data cost of posting
    /// the serialized list:
    ///
    /// - **L2 savings**: `100 gas * entry_count * perArbGas`, where each address
    ///   and each storage key counts as one entry (the EIP-2929 warm-vs-cold
    ///   access discount).
    /// - **L1 cost**: `l1_data_gas * l1_base_fee`, where `l1_data_gas` is the
    ///   exact per-byte calldata gas ([`l1_data_gas_for_bytes`]) of the EIP-2930
    ///   RLP-encoded access list.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the provider/pricing queries fail.
    ///
    /// Returns `Ok(None)` when:
    /// - the list is empty,
    /// - either the L2 or L1 gas price reads as zero, or
    /// - the estimated L1 cost meets or exceeds the L2 savings (not profitable).
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
        let prices = prices_call
            .call()
            .await
            .map_err(|e| AccessListError::query("ArbGasInfo prices", e))?;
        let l1_fee_call = arb.getL1BaseFeeEstimate();
        let l1_base_fee = l1_fee_call
            .call()
            .await
            .map_err(|e| AccessListError::query("ArbGasInfo L1 base fee", e))?;

        let l2_gas_price = prices.perArbGas;

        if l2_gas_price.is_zero() || l1_base_fee.is_zero() {
            debug!("L1 or L2 gas price is zero, skipping access list");
            return Ok(None);
        }

        let access_list = AccessList(self.items);
        if log_access_list_profitability(
            &access_list,
            l2_gas_price,
            l1_base_fee,
            "Access list profitability check",
        ) {
            Ok(Some(access_list))
        } else {
            Ok(None)
        }
    }
}

/// Evaluate whether an existing access list is profitable on Arbitrum.
///
/// Each access list entry saves L2 execution gas (warm vs cold access) but
/// costs L1 data posting gas for its serialized bytes. This function queries
/// `ArbGasInfo`, computes the exact EIP-2930 RLP data gas, and returns the
/// access list only if profitable.
///
/// This is the free-function counterpart to
/// [`SmartAccessList::into_access_list_if_profitable`] for a pre-built
/// [`AccessList`]; the two share the same cost model and break-even comparison.
///
/// # Errors
///
/// Returns `Err` if the provider/pricing queries fail.
///
/// Returns `Ok(None)` when:
/// - the list is empty,
/// - either the L2 or L1 gas price reads as zero, or
/// - the estimated L1 cost meets or exceeds the L2 savings (not profitable).
pub async fn access_list_if_profitable<P: Provider>(
    access_list: AccessList,
    provider: &P,
) -> Result<Option<AccessList>> {
    if access_list.0.is_empty() {
        return Ok(None);
    }

    // Query ArbGasInfo for current pricing
    let arb = ArbGasInfo::new(ARB_GAS_INFO, provider);
    let prices = arb
        .getPricesInWei()
        .call()
        .await
        .map_err(|e| AccessListError::query("ArbGasInfo prices", e))?;
    let l1_base_fee = arb
        .getL1BaseFeeEstimate()
        .call()
        .await
        .map_err(|e| AccessListError::query("ArbGasInfo L1 base fee", e))?;

    let l2_gas_price = prices.perArbGas;

    if l2_gas_price.is_zero() || l1_base_fee.is_zero() {
        debug!("L1 or L2 gas price is zero, skipping access list");
        return Ok(None);
    }

    if log_access_list_profitability(
        &access_list,
        l2_gas_price,
        l1_base_fee,
        "Simulation access list profitability check",
    ) {
        Ok(Some(access_list))
    } else {
        Ok(None)
    }
}

/// Select the appropriate access list strategy based on pricing inputs.
///
/// - **L1**: Always include the simulation access list (no L1 data cost penalty).
///   Returns `None` only if the list is empty.
/// - **Arbitrum**: Include only when warm-access savings exceed the exact
///   EIP-2930 RLP data cost priced through `ArbGasInfo`.
/// - **OP Stack**: Include only when warm-access savings exceed the incremental
///   `GasPriceOracle.getL1Fee(bytes)` fee between the transaction without and
///   with the access list.
pub async fn resolve_access_list<P: Provider>(
    sim_access_list: AccessList,
    provider: &P,
    pricing: AccessListPricing,
) -> Result<Option<AccessList>> {
    if sim_access_list.0.is_empty() {
        return Ok(None);
    }

    match pricing {
        AccessListPricing::L1 => Ok(Some(sim_access_list)),
        AccessListPricing::Arbitrum => access_list_if_profitable(sim_access_list, provider).await,
        AccessListPricing::OpStack {
            tx_without_access_list,
            tx_with_access_list,
        } => {
            access_list_if_profitable_op_stack(
                sim_access_list,
                provider,
                tx_without_access_list,
                tx_with_access_list,
            )
            .await
        }
    }
}

async fn access_list_if_profitable_op_stack<P: Provider>(
    access_list: AccessList,
    provider: &P,
    tx_without_access_list: Bytes,
    tx_with_access_list: Bytes,
) -> Result<Option<AccessList>> {
    let l2_gas_price = U256::from(
        provider
            .get_gas_price()
            .await
            .map_err(|e| AccessListError::query("OP Stack provider gas price", e))?,
    );

    let l1_fee_without = query_op_l1_fee(provider, tx_without_access_list)
        .await
        .map_err(|e| AccessListError::Query {
            operation: "OP Stack GasPriceOracle L1 fee without access list",
            details: e.to_string(),
        })?;
    let l1_fee_with = query_op_l1_fee(provider, tx_with_access_list)
        .await
        .map_err(|e| AccessListError::Query {
            operation: "OP Stack GasPriceOracle L1 fee with access list",
            details: e.to_string(),
        })?;

    let incremental_l1_fee = l1_fee_with.saturating_sub(l1_fee_without);
    let total_entries = access_list_entry_count(&access_list);
    let l2_savings_wei = U256::from(total_entries) * U256::from(100) * l2_gas_price;
    let profitable = l2_savings_wei > incremental_l1_fee;

    info!(
        entries = total_entries,
        items = access_list.0.len(),
        l2_savings_wei = %l2_savings_wei,
        l1_fee_without_wei = %l1_fee_without,
        l1_fee_with_wei = %l1_fee_with,
        incremental_l1_fee_wei = %incremental_l1_fee,
        l2_gas_price_gwei = %format_gwei(l2_gas_price),
        profitable,
        "OP Stack access list profitability check"
    );

    if profitable {
        Ok(Some(access_list))
    } else {
        Ok(None)
    }
}

async fn query_op_l1_fee<P: Provider>(provider: &P, tx_data: Bytes) -> Result<U256> {
    let calldata = OpGasPriceOracle::getL1FeeCall { _data: tx_data }.abi_encode();
    let tx = TransactionRequest::default()
        .to(OP_GAS_PRICE_ORACLE)
        .input(TransactionInput::from(calldata));

    provider
        .client()
        .request("eth_call", (tx, BlockNumberOrTag::Latest))
        .await
        .map_err(|e| AccessListError::query("OP Stack GasPriceOracle.getL1Fee eth_call", e))
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
///
/// This is the post-EIP-2028 calldata pricing used to approximate the L1 data
/// cost of serialized access-list entries. It counts the raw bytes only and
/// does not add any RLP framing overhead.
///
/// # Examples
///
/// ```
/// use evm_fork_cache::access_list::l1_data_gas_for_bytes;
///
/// // All-zero 32-byte slot: 32 * 4 = 128 gas.
/// assert_eq!(l1_data_gas_for_bytes(&[0u8; 32]), 128);
/// // All-non-zero 20-byte address: 20 * 16 = 320 gas.
/// assert_eq!(l1_data_gas_for_bytes(&[0xFFu8; 20]), 320);
/// // Empty slice costs nothing.
/// assert_eq!(l1_data_gas_for_bytes(&[]), 0);
/// ```
pub fn l1_data_gas_for_bytes(data: &[u8]) -> u64 {
    data.iter()
        .map(|&b| if b == 0 { 4u64 } else { 16u64 })
        .sum()
}

/// Exact L1 calldata gas for the EIP-2930 RLP encoding of an access list.
pub fn access_list_rlp_data_gas(access_list: &AccessList) -> u64 {
    let mut encoded = Vec::with_capacity(access_list.length());
    access_list.encode(&mut encoded);
    l1_data_gas_for_bytes(&encoded)
}

fn access_list_entry_count(access_list: &AccessList) -> u64 {
    access_list
        .0
        .iter()
        .map(|item| 1 + item.storage_keys.len() as u64)
        .sum()
}

fn log_access_list_profitability(
    access_list: &AccessList,
    l2_gas_price: U256,
    l1_base_fee: U256,
    message: &'static str,
) -> bool {
    let total_entries = access_list_entry_count(access_list);
    let total_l1_data_gas = access_list_rlp_data_gas(access_list);
    let l2_savings_wei = U256::from(total_entries) * U256::from(100) * l2_gas_price;
    let l1_cost_wei = U256::from(total_l1_data_gas) * l1_base_fee;
    let profitable = l2_savings_wei > l1_cost_wei;

    info!(
        entries = total_entries,
        items = access_list.0.len(),
        l1_data_gas = total_l1_data_gas,
        l2_savings_wei = %l2_savings_wei,
        l1_cost_wei = %l1_cost_wei,
        l2_gas_price_gwei = %format_gwei(l2_gas_price),
        l1_base_fee_gwei = %format_gwei(l1_base_fee),
        profitable,
        check = message,
        "Access list profitability check"
    );

    profitable
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
    use alloy_primitives::Bytes;

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

    #[test]
    fn access_list_rlp_data_gas_uses_exact_eip2930_encoding() {
        let access_list = AccessList(vec![AccessListItem {
            address: Address::ZERO,
            storage_keys: Vec::new(),
        }]);

        // RLP([[zero_address, []]]) = d7 d6 94 <20 zero bytes> c0.
        // Four non-zero framing bytes cost 64 gas; twenty zero address bytes cost
        // 80 gas. The old fixed-overhead approximation returned 192.
        assert_eq!(access_list_rlp_data_gas(&access_list), 144);
    }

    #[tokio::test]
    async fn access_list_profitability_provider_error_returns_err() {
        use alloy_network::Ethereum;
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let provider = RootProvider::<Ethereum>::new(RpcClient::mocked(Asserter::new()));
        let access_list = AccessList(vec![AccessListItem {
            address: Address::repeat_byte(0xAA),
            storage_keys: Vec::new(),
        }]);

        let err = access_list_if_profitable(access_list, &provider)
            .await
            .expect_err("provider failures must be distinguishable from unprofitable lists");
        assert!(
            err.to_string().contains("ArbGasInfo") || err.to_string().contains("provider"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn access_list_profitability_empty_list_still_returns_none() {
        use alloy_network::Ethereum;
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let provider = RootProvider::<Ethereum>::new(RpcClient::mocked(Asserter::new()));
        let result = access_list_if_profitable(AccessList::default(), &provider)
            .await
            .expect("empty list must not query provider");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_access_list_l1_returns_non_empty_without_provider_calls() {
        use alloy_network::Ethereum;
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let provider = RootProvider::<Ethereum>::new(RpcClient::mocked(Asserter::new()));
        let access_list = AccessList(vec![AccessListItem {
            address: Address::repeat_byte(0xAA),
            storage_keys: Vec::new(),
        }]);

        let result = resolve_access_list(access_list.clone(), &provider, AccessListPricing::L1)
            .await
            .expect("L1 must not query provider");
        assert_eq!(result, Some(access_list));

        let empty = resolve_access_list(AccessList::default(), &provider, AccessListPricing::L1)
            .await
            .expect("empty L1 list must not query provider");
        assert!(empty.is_none());
    }

    #[tokio::test]
    async fn resolve_access_list_op_stack_uses_oracle_incremental_fee() {
        use alloy_network::Ethereum;
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        asserter.push_success(&100u128); // eth_gasPrice
        asserter.push_success(&U256::from(1_000u64)); // getL1Fee(tx_without)
        asserter.push_success(&U256::from(1_010u64)); // getL1Fee(tx_with)
        let provider = RootProvider::<Ethereum>::new(RpcClient::mocked(asserter));
        let access_list = AccessList(vec![AccessListItem {
            address: Address::repeat_byte(0xAA),
            storage_keys: Vec::new(),
        }]);

        let result = resolve_access_list(
            access_list.clone(),
            &provider,
            AccessListPricing::OpStack {
                tx_without_access_list: Bytes::from_static(b"without"),
                tx_with_access_list: Bytes::from_static(b"with"),
            },
        )
        .await
        .expect("OP Stack pricing succeeds");

        assert_eq!(result, Some(access_list));
    }

    #[tokio::test]
    async fn resolve_access_list_op_stack_unprofitable_returns_none() {
        use alloy_network::Ethereum;
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        asserter.push_success(&100u128); // eth_gasPrice
        asserter.push_success(&U256::from(1_000u64)); // getL1Fee(tx_without)
        asserter.push_success(&U256::from(20_000u64)); // getL1Fee(tx_with)
        let provider = RootProvider::<Ethereum>::new(RpcClient::mocked(asserter));
        let access_list = AccessList(vec![AccessListItem {
            address: Address::repeat_byte(0xAA),
            storage_keys: Vec::new(),
        }]);

        let result = resolve_access_list(
            access_list,
            &provider,
            AccessListPricing::OpStack {
                tx_without_access_list: Bytes::from_static(b"without"),
                tx_with_access_list: Bytes::from_static(b"with"),
            },
        )
        .await
        .expect("OP Stack pricing succeeds");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_access_list_op_stack_provider_failure_returns_err() {
        use alloy_network::Ethereum;
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        asserter.push_failure_msg("gas oracle unavailable");
        let provider = RootProvider::<Ethereum>::new(RpcClient::mocked(asserter));
        let access_list = AccessList(vec![AccessListItem {
            address: Address::repeat_byte(0xAA),
            storage_keys: Vec::new(),
        }]);

        let err = resolve_access_list(
            access_list,
            &provider,
            AccessListPricing::OpStack {
                tx_without_access_list: Bytes::from_static(b"without"),
                tx_with_access_list: Bytes::from_static(b"with"),
            },
        )
        .await
        .expect_err("provider failures must be distinguishable from unprofitable lists");

        assert!(
            err.to_string().contains("gas")
                || err.to_string().contains("oracle")
                || err.to_string().contains("provider"),
            "unexpected error: {err:#}"
        );
    }
}
