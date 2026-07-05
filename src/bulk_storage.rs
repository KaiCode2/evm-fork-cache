//! Bulk storage extraction over `eth_call` state overrides.
//!
//! The default [`StorageBatchFetchFn`] issues one `eth_getStorageAt` per slot
//! (JSON-RPC-batched, but still one *billed* request per slot — 20 CU each on
//! Alchemy). This module implements the "bulk storage extraction" technique
//! described by Dedaub, which packs thousands of slot reads into a **single**
//! `eth_call` (26 CU on Alchemy, flat):
//!
//! - blog: <https://dedaub.com/blog/bulk-storage-extraction/>
//! - reference implementation: <https://github.com/Dedaub/storage-extractor>
//!
//! # Mechanism
//!
//! `eth_call` accepts a *state-override set* that can replace the **code** at
//! any address while leaving its **storage** intact. We override the target
//! contract with a 23-byte handwritten extractor ([`STORAGE_EXTRACTOR_CODE`],
//! Dedaub's bytecode, credited above) that treats calldata as a raw array of
//! 32-byte slot keys, `SLOAD`s each one, and returns the packed values —
//! no function selector, no ABI:
//!
//! ```text
//! [00] PUSH0            counter = 0
//! [01] JUMPDEST         loop:
//! [02] DUP1 CALLDATASIZE EQ
//! [05] PUSH1 0x13 JUMPI   -> exit when counter == calldatasize
//! [08] DUP1 CALLDATALOAD  slot key at calldata[counter]
//! [0a] SLOAD
//! [0b] DUP2 MSTORE        mem[counter] = value (counter doubles as mem offset)
//! [0d] PUSH1 0x20 ADD     counter += 32
//! [10] PUSH1 0x01 JUMP
//! [13] JUMPDEST CALLDATASIZE PUSH0 RETURN
//! ```
//!
//! Marginal cost is ~2,664 gas per slot (cold `SLOAD` 2,100 + calldata ~510 +
//! loop ~30 + memory), so a default 50M-gas `eth_call` fits ~18,500 slots.
//! [`BulkCallConfig::max_slots_per_call`] defaults to a conservative 10,000
//! (~27M gas), splitting larger requests across concurrent calls.
//!
//! # Multi-contract batches
//!
//! `SLOAD` reads the storage of the *executing* contract, so each target must
//! run the extractor at its own address. To read many contracts in one round
//! trip we additionally override [`MULTICALL3_ADDRESS`] with the canonical
//! Multicall3 runtime ([`multicall3_runtime_code`]) and dispatch one
//! `aggregate3` call whose subcalls hit each overridden target. Overriding the
//! dispatcher code unconditionally makes the scheme work on chains — and at
//! historical blocks — where Multicall3 is not deployed.
//!
//! # Semantics & caveats
//!
//! - Results are identical to `eth_getStorageAt` at the same block: absent
//!   slots (and slots of code-less accounts) read as zero.
//! - The provider must support the state-override parameter of `eth_call`
//!   (Geth-lineage nodes, Reth, Erigon, and the major hosted providers all
//!   do). Providers that reject it surface a per-slot error; install a
//!   fallback via [`bulk_call_storage_fetcher_with_fallback`] to repair those
//!   with classic point reads.
//! - True precompile addresses (`0x01..=0x11` on mainnet) execute the
//!   precompile regardless of a code override; slots requested there fail the
//!   response-length check and surface as errors (repaired by the fallback
//!   when configured) rather than silently returning garbage.
//! - [`STORAGE_EXTRACTOR_CODE`] uses `PUSH0` (Shanghai). For pre-Shanghai
//!   chains set [`BulkCallConfig::pre_shanghai_extractor`] to use the
//!   equivalent [`STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI`].
//!
//! # Wiring it into a cache
//!
//! **Since 0.2.0 this is every provider-backed cache's default storage
//! fetcher** — no wiring needed. Tune it with
//! [`EvmCacheBuilder::bulk_call_config`](crate::cache::EvmCacheBuilder::bulk_call_config),
//! opt out with
//! [`StorageFetchStrategy::PointRead`](crate::cache::StorageFetchStrategy::PointRead),
//! or compose it manually as below (e.g. over a custom fallback):
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use alloy_provider::{ProviderBuilder, network::AnyNetwork};
//! # use evm_fork_cache::cache::EvmCache;
//! # use evm_fork_cache::bulk_storage::{BulkCallConfig, bulk_call_storage_fetcher_with_fallback};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = Arc::new(
//!     ProviderBuilder::new()
//!         .network::<AnyNetwork>()
//!         .connect_http("https://example-rpc.invalid".parse()?),
//! );
//! let mut cache = EvmCache::builder(provider.clone()).build().await;
//!
//! // Keep the default point-read fetcher as a repair path, then route all
//! // batch storage fetches through call-override bulk extraction.
//! let fallback = cache
//!     .storage_batch_fetcher()
//!     .cloned()
//!     .expect("provider-backed cache has a default fetcher");
//! cache.set_storage_batch_fetcher(bulk_call_storage_fetcher_with_fallback(
//!     provider,
//!     BulkCallConfig::default(),
//!     fallback,
//! ));
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use alloy_eips::BlockId;
use alloy_primitives::{Address, B256, Bytes, U256, hex};
use alloy_provider::Provider;
use alloy_provider::network::AnyNetwork;
use alloy_rpc_types_eth::TransactionRequest;
use alloy_rpc_types_eth::state::{AccountOverride, StateOverride};
use alloy_sol_types::SolCall;
use futures::stream::{self, StreamExt};
use tracing::{debug, warn};

use crate::cache::{StorageBatchFetchFn, block_in_place_handle};
use crate::errors::{StorageFetchError, StorageFetchResult};
use crate::multicall::{IMulticall3, MULTICALL3_ADDRESS};

/// Dedaub's 23-byte storage extractor (see the module docs for the annotated
/// disassembly). Calldata is a contiguous array of 32-byte slot keys; the
/// return data is the corresponding array of 32-byte values. Requires
/// `PUSH0` (Shanghai).
///
/// Source: <https://github.com/Dedaub/storage-extractor> (`extractor.hex`).
pub const STORAGE_EXTRACTOR_CODE: &[u8] = &hex!("5f5b80361460135780355481526020016001565b365ff3");

/// [`STORAGE_EXTRACTOR_CODE`] with both `PUSH0`s replaced by `PUSH1 0x00`
/// (jump targets re-pointed), for chains that have not activated Shanghai.
pub const STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI: &[u8] =
    &hex!("60005b80361460145780355481526020016002565b366000f3");

/// Runtime bytecode of Multicall3 (`0xcA11bde05977b3631167028862bE2a173976CA11`),
/// as deployed on Ethereum mainnet. Injected as a code override at
/// [`MULTICALL3_ADDRESS`] for multi-contract extraction so the dispatcher
/// exists on every chain and at every historical block.
///
/// The fixture was fetched via `eth_getCode` and verified byte-identical
/// across independent providers; `fixtures/README.md` records provenance.
pub fn multicall3_runtime_code() -> &'static Bytes {
    static CODE: OnceLock<Bytes> = OnceLock::new();
    CODE.get_or_init(|| {
        let raw = include_str!("../fixtures/multicall3_runtime.hex");
        Bytes::from(hex::decode(raw.trim()).expect("valid multicall3 runtime hex fixture"))
    })
}

/// How planned extraction chunks are shipped to the provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CallDispatch {
    /// One `eth_call` per planned chunk. Universally supported; chunks run
    /// concurrently up to [`BulkCallConfig::max_concurrent_calls`]. The
    /// default.
    #[default]
    PerCall,
    /// Ship many chunks as the transactions of a single `eth_callMany`
    /// bundle (Erigon-lineage providers, including Alchemy — where it costs
    /// 20 CU per *request* vs 26 per `eth_call`). Requests are bounded by
    /// [`BulkCallConfig::max_slots_per_request`]; a request-level failure
    /// (e.g. the method is unsupported) transparently re-dispatches that
    /// request's chunks per-call. Hash-pinned blocks always dispatch
    /// per-call (`eth_callMany` takes a number/tag block context).
    CallMany,
}

/// Tuning knobs for the call-override bulk storage fetcher.
///
/// The defaults target Geth-default RPC limits (50M gas per `eth_call`):
/// 10,000 slots ≈ 27M gas, comfortably under the cap while leaving headroom
/// for multicall dispatch overhead. Providers with higher caps can raise
/// `max_slots_per_call` substantially (measure before relying on it —
/// Alchemy accepted 30k slots/call in testing, bounded by request body size
/// rather than gas; see `docs/bulk-storage-extraction.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkCallConfig {
    /// Maximum storage slots packed into one `eth_call` (across all targets
    /// in that call). ~2,664 gas per slot; keep the product under the
    /// provider's `eth_call` gas cap.
    pub max_slots_per_call: usize,
    /// Maximum distinct target contracts dispatched through one multicall
    /// (~8k gas of call/ABI overhead per target).
    pub max_targets_per_call: usize,
    /// Maximum `eth_call`s in flight at once when a request spans multiple
    /// calls.
    pub max_concurrent_calls: usize,
    /// Requests with fewer than this many slots are routed to the fallback
    /// fetcher when one is installed (an `eth_call` costs slightly more than
    /// a single `eth_getStorageAt` on CU-metered providers). Ignored when no
    /// fallback is available.
    pub point_read_threshold: usize,
    /// Use the `PUSH0`-free extractor for chains without Shanghai.
    pub pre_shanghai_extractor: bool,
    /// How chunks are shipped: one `eth_call` each, or batched through
    /// `eth_callMany`.
    pub dispatch: CallDispatch,
    /// [`CallDispatch::CallMany`] only: maximum total slots per
    /// `eth_callMany` request. Slot keys are incompressible calldata
    /// (~64 bytes each in the JSON body), and providers cap request bodies —
    /// Alchemy rejects ~2.5 MB with HTTP 413. The default (25,000 ≈ 1.6 MB)
    /// stays inside that.
    pub max_slots_per_request: usize,
}

impl Default for BulkCallConfig {
    fn default() -> Self {
        Self {
            max_slots_per_call: 10_000,
            max_targets_per_call: 250,
            max_concurrent_calls: 4,
            point_read_threshold: 2,
            pre_shanghai_extractor: false,
            dispatch: CallDispatch::PerCall,
            max_slots_per_request: 25_000,
        }
    }
}

impl BulkCallConfig {
    fn normalized(self) -> Self {
        Self {
            max_slots_per_call: self.max_slots_per_call.max(1),
            max_targets_per_call: self.max_targets_per_call.max(1),
            max_concurrent_calls: self.max_concurrent_calls.max(1),
            max_slots_per_request: self.max_slots_per_request.max(1),
            ..self
        }
    }

    fn extractor(&self) -> Bytes {
        if self.pre_shanghai_extractor {
            Bytes::from_static(STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI)
        } else {
            Bytes::from_static(STORAGE_EXTRACTOR_CODE)
        }
    }
}

/// Pack slot keys into extractor calldata: the raw concatenation of each
/// key's 32-byte big-endian representation (no selector, no ABI).
pub fn pack_slots_calldata(slots: &[U256]) -> Bytes {
    let mut out = Vec::with_capacity(slots.len() * 32);
    for slot in slots {
        out.extend_from_slice(&slot.to_be_bytes::<32>());
    }
    out.into()
}

/// Decode extractor return data (packed 32-byte words) into values.
///
/// Returns `None` when the payload is not exactly `expected` words — the
/// signature of a call that did not actually execute the extractor (e.g. a
/// provider that ignored the code override, or a precompile target).
pub fn decode_packed_values(data: &[u8], expected: usize) -> Option<Vec<U256>> {
    if data.len() != expected * 32 {
        return None;
    }
    Some(data.chunks_exact(32).map(U256::from_be_slice).collect())
}

/// ABI-encode one `aggregate3` dispatch whose subcalls run the extractor at
/// each `(target, slots)` pair. Subcalls use `allowFailure = true` so one
/// failing target degrades to per-target errors instead of reverting the
/// whole batch.
pub fn encode_multi_target_calldata(targets: &[(Address, Vec<U256>)]) -> Bytes {
    let calls: Vec<IMulticall3::Call3> = targets
        .iter()
        .map(|(target, slots)| IMulticall3::Call3 {
            target: *target,
            allowFailure: true,
            callData: pack_slots_calldata(slots),
        })
        .collect();
    IMulticall3::aggregate3Call { calls }.abi_encode().into()
}

/// Decode an `aggregate3` response produced by [`encode_multi_target_calldata`]
/// back into one result tuple per requested `(target, slot)` pair.
pub fn decode_multi_target_response(
    targets: &[(Address, Vec<U256>)],
    response: &[u8],
) -> Vec<(Address, U256, StorageFetchResult<U256>)> {
    let decoded = match IMulticall3::aggregate3Call::abi_decode_returns(response) {
        Ok(results) if results.len() == targets.len() => results,
        Ok(results) => {
            return per_target_errors(targets, || {
                StorageFetchError::custom(format!(
                    "aggregate3 returned {} results for {} extraction targets",
                    results.len(),
                    targets.len()
                ))
            });
        }
        Err(e) => {
            return per_target_errors(targets, || {
                StorageFetchError::custom(format!("failed to decode aggregate3 response: {e}"))
            });
        }
    };

    let mut out = Vec::with_capacity(targets.iter().map(|(_, s)| s.len()).sum());
    for ((target, slots), result) in targets.iter().zip(decoded) {
        if !result.success {
            out.extend(slots.iter().map(|slot| {
                (
                    *target,
                    *slot,
                    Err(StorageFetchError::custom(
                        "extractor subcall failed (allowFailure=true); the target may be a precompile",
                    )),
                )
            }));
            continue;
        }
        match decode_packed_values(&result.returnData, slots.len()) {
            Some(values) => out.extend(
                slots
                    .iter()
                    .zip(values)
                    .map(|(slot, value)| (*target, *slot, Ok(value))),
            ),
            None => out.extend(slots.iter().map(|slot| {
                (
                    *target,
                    *slot,
                    Err(StorageFetchError::custom(format!(
                        "extractor at {target} returned {} bytes, expected {}",
                        result.returnData.len(),
                        slots.len() * 32
                    ))),
                )
            })),
        }
    }
    out
}

fn per_target_errors(
    targets: &[(Address, Vec<U256>)],
    make: impl Fn() -> StorageFetchError,
) -> Vec<(Address, U256, StorageFetchResult<U256>)> {
    targets
        .iter()
        .flat_map(|(target, slots)| slots.iter().map(|slot| (*target, *slot, Err(make()))))
        .collect()
}

/// One planned `eth_call`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CallPlan {
    /// Direct extractor call: `to = target`, calldata = packed slots.
    Single { target: Address, slots: Vec<U256> },
    /// Multicall3 dispatch across several targets, each running the extractor.
    Multi { targets: Vec<(Address, Vec<U256>)> },
}

impl CallPlan {
    fn request_slot_count(&self) -> usize {
        match self {
            Self::Single { slots, .. } => slots.len(),
            Self::Multi { targets } => targets.iter().map(|(_, s)| s.len()).sum(),
        }
    }
}

/// Split requests into `eth_call`-sized plans.
///
/// Groups by address in first-seen order; targets with more than
/// `max_slots_per_call` slots are split into dedicated single-target calls,
/// and the remaining small groups are greedily packed into multicall
/// dispatches bounded by both the slot and target budgets. A target equal to
/// [`MULTICALL3_ADDRESS`] always gets a dedicated call so its code override
/// cannot collide with the dispatcher's.
fn plan_calls(requests: &[(Address, U256)], config: &BulkCallConfig) -> Vec<CallPlan> {
    let mut order: Vec<Address> = Vec::new();
    let mut groups: HashMap<Address, Vec<U256>> = HashMap::new();
    for (address, slot) in requests {
        groups
            .entry(*address)
            .or_insert_with(|| {
                order.push(*address);
                Vec::new()
            })
            .push(*slot);
    }

    let mut plans = Vec::new();
    let mut packable: Vec<(Address, Vec<U256>)> = Vec::new();
    for address in order {
        let slots = groups.remove(&address).expect("grouped above");
        for chunk in slots.chunks(config.max_slots_per_call) {
            let full = chunk.len() == config.max_slots_per_call;
            // The dispatcher address must never share a multicall with other
            // targets: its extractor override would clobber the dispatcher
            // code override at the same key. Only the final chunk of a target
            // can be partial, so at most one packable remainder per target.
            if full || address == MULTICALL3_ADDRESS {
                plans.push(CallPlan::Single {
                    target: address,
                    slots: chunk.to_vec(),
                });
            } else {
                packable.push((address, chunk.to_vec()));
            }
        }
    }

    // Greedily pack the small per-target remainders into multicall dispatches.
    let mut current: Vec<(Address, Vec<U256>)> = Vec::new();
    let mut current_slots = 0usize;
    let flush =
        |current: &mut Vec<(Address, Vec<U256>)>, plans: &mut Vec<CallPlan>| match current.len() {
            0 => {}
            1 => {
                let (target, slots) = current.pop().expect("len checked");
                plans.push(CallPlan::Single { target, slots });
            }
            _ => plans.push(CallPlan::Multi {
                targets: std::mem::take(current),
            }),
        };
    for (address, slots) in packable {
        let would_overflow = current_slots + slots.len() > config.max_slots_per_call
            || current.len() >= config.max_targets_per_call;
        if !current.is_empty() && would_overflow {
            flush(&mut current, &mut plans);
            current_slots = 0;
        }
        current_slots += slots.len();
        current.push((address, slots));
    }
    flush(&mut current, &mut plans);

    plans
}

/// Build the state-override set for one plan: the extractor at every target,
/// plus the Multicall3 runtime at the dispatcher for multi-target plans.
fn overrides_for_plan(plan: &CallPlan, extractor: &Bytes) -> StateOverride {
    let mut overrides = StateOverride::default();
    match plan {
        CallPlan::Single { target, .. } => {
            overrides.insert(
                *target,
                AccountOverride::default().with_code(extractor.clone()),
            );
        }
        CallPlan::Multi { targets } => {
            overrides.insert(
                MULTICALL3_ADDRESS,
                AccountOverride::default().with_code(multicall3_runtime_code().clone()),
            );
            for (target, _) in targets {
                overrides.insert(
                    *target,
                    AccountOverride::default().with_code(extractor.clone()),
                );
            }
        }
    }
    overrides
}

/// The `to` address and calldata for one planned call.
fn plan_call_parts(plan: &CallPlan) -> (Address, Bytes) {
    match plan {
        CallPlan::Single { target, slots } => (*target, pack_slots_calldata(slots)),
        CallPlan::Multi { targets } => (MULTICALL3_ADDRESS, encode_multi_target_calldata(targets)),
    }
}

/// Decode one plan's successful call output into per-slot results.
fn decode_plan_response(
    plan: &CallPlan,
    bytes: &[u8],
) -> Vec<(Address, U256, StorageFetchResult<U256>)> {
    match plan {
        CallPlan::Single { target, slots } => match decode_packed_values(bytes, slots.len()) {
            Some(values) => slots
                .iter()
                .zip(values)
                .map(|(slot, value)| (*target, *slot, Ok(value)))
                .collect(),
            None => slots
                .iter()
                .map(|slot| {
                    (
                        *target,
                        *slot,
                        Err(StorageFetchError::custom(format!(
                            "extractor at {target} returned {} bytes, expected {} — the \
                             provider may not support eth_call state overrides, or the \
                             target is a precompile",
                            bytes.len(),
                            slots.len() * 32
                        ))),
                    )
                })
                .collect(),
        },
        CallPlan::Multi { targets } => decode_multi_target_response(targets, bytes),
    }
}

/// Report `err` for every slot the plan covers.
fn plan_error_results(
    plan: &CallPlan,
    err: StorageFetchError,
) -> Vec<(Address, U256, StorageFetchResult<U256>)> {
    match plan {
        CallPlan::Single { target, slots } => slots
            .iter()
            .map(|slot| (*target, *slot, Err(err.clone())))
            .collect(),
        CallPlan::Multi { targets } => targets
            .iter()
            .flat_map(|(target, slots)| {
                slots.iter().map({
                    let err = err.clone();
                    move |slot| (*target, *slot, Err(err.clone()))
                })
            })
            .collect(),
    }
}

async fn execute_plan<P: Provider<AnyNetwork>>(
    provider: &P,
    block: BlockId,
    plan: CallPlan,
    extractor: &Bytes,
) -> Vec<(Address, U256, StorageFetchResult<U256>)> {
    let overrides = overrides_for_plan(&plan, extractor);
    let (to, data) = plan_call_parts(&plan);
    let tx = TransactionRequest::default().to(to).input(data.into());

    let response: Result<Bytes, _> = provider
        .client()
        .request("eth_call", (tx, block, overrides))
        .await;

    match response {
        Ok(bytes) => decode_plan_response(&plan, &bytes),
        Err(e) => plan_error_results(&plan, StorageFetchError::provider("eth_call", &e)),
    }
}

/// One entry of an `eth_callMany` response: `{"value": "0x.."}` on success,
/// `{"error": ..}` on per-transaction failure (Erigon-style).
#[derive(Debug, serde::Deserialize)]
struct CallManyEntry {
    value: Option<Bytes>,
    error: Option<serde_json::Value>,
}

/// Execute several plans as the transactions of one `eth_callMany` bundle.
///
/// Returns `Err` only for request-level failures (method unsupported,
/// transport error, malformed response) so the caller can re-dispatch the
/// same plans per-call; per-transaction failures are mapped to per-slot
/// errors in the `Ok` payload.
async fn execute_plans_call_many<P: Provider<AnyNetwork>>(
    provider: &P,
    number: alloy_eips::BlockNumberOrTag,
    plans: &[CallPlan],
    extractor: &Bytes,
) -> Result<Vec<(Address, U256, StorageFetchResult<U256>)>, StorageFetchError> {
    // One shared override map across every plan in the bundle. Merging is
    // safe: every target maps to the extractor and the dispatcher maps to
    // Multicall3; plans targeting the dispatcher itself are routed per-call
    // by the caller.
    let mut overrides = StateOverride::default();
    let mut transactions = Vec::with_capacity(plans.len());
    for plan in plans {
        for (address, account) in overrides_for_plan(plan, extractor) {
            overrides.insert(address, account);
        }
        let (to, data) = plan_call_parts(plan);
        transactions.push(serde_json::json!({ "to": to, "data": data }));
    }

    let bundles = serde_json::json!([{ "transactions": transactions }]);
    let context = serde_json::json!({ "blockNumber": number, "transactionIndex": -1 });
    let response: Vec<Vec<CallManyEntry>> = provider
        .client()
        .request("eth_callMany", (bundles, context, overrides))
        .await
        .map_err(|e| StorageFetchError::provider("eth_callMany", &e))?;

    let entries: Vec<CallManyEntry> = response.into_iter().flatten().collect();
    if entries.len() != plans.len() {
        return Err(StorageFetchError::custom(format!(
            "eth_callMany returned {} results for {} bundled calls",
            entries.len(),
            plans.len()
        )));
    }

    let mut out = Vec::new();
    for (plan, entry) in plans.iter().zip(entries) {
        match entry.value {
            Some(bytes) => out.extend(decode_plan_response(plan, &bytes)),
            None => {
                let detail = entry
                    .error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "no value returned".to_string());
                out.extend(plan_error_results(
                    plan,
                    StorageFetchError::custom(format!("eth_callMany transaction failed: {detail}")),
                ));
            }
        }
    }
    Ok(out)
}

/// Group plans into `eth_callMany` requests bounded by the per-request slot
/// budget (the request *body* is the binding provider limit, not gas).
fn group_plans_for_call_many(
    plans: Vec<CallPlan>,
    max_slots_per_request: usize,
) -> Vec<Vec<CallPlan>> {
    let mut requests: Vec<Vec<CallPlan>> = Vec::new();
    let mut current: Vec<CallPlan> = Vec::new();
    let mut current_slots = 0usize;
    for plan in plans {
        let slots = plan.request_slot_count();
        if !current.is_empty() && current_slots + slots > max_slots_per_request {
            requests.push(std::mem::take(&mut current));
            current_slots = 0;
        }
        current_slots += slots;
        current.push(plan);
    }
    if !current.is_empty() {
        requests.push(current);
    }
    requests
}

/// Fetch storage slots in bulk via `eth_call` code overrides (async core).
///
/// Returns exactly one result tuple per requested `(address, slot)` pair
/// (order not preserved, duplicates included), matching the
/// [`StorageBatchFetchFn`] contract. Chunk-level failures (transport errors,
/// providers without state-override support) surface as per-slot errors.
///
/// This is the direct entry point for async callers — e.g. loading an entire
/// AMM pool's tick range during cold start — while
/// [`bulk_call_storage_fetcher`] adapts it to the cache's synchronous fetcher
/// seam.
pub async fn fetch_slots_bulk<P: Provider<AnyNetwork>>(
    provider: &P,
    requests: Vec<(Address, U256)>,
    block: BlockId,
    config: BulkCallConfig,
) -> Vec<(Address, U256, StorageFetchResult<U256>)> {
    let config = config.normalized();
    if requests.is_empty() {
        return Vec::new();
    }
    let extractor = config.extractor();
    let plans = plan_calls(&requests, &config);
    debug!(
        slots = requests.len(),
        calls = plans.len(),
        dispatch = ?config.dispatch,
        "bulk storage extraction dispatch"
    );

    let extractor = &extractor;
    // eth_callMany takes a number/tag block context; hash pins dispatch
    // per-call instead.
    let call_many_number = match (config.dispatch, block) {
        (CallDispatch::CallMany, BlockId::Number(number)) => Some(number),
        _ => None,
    };

    let Some(number) = call_many_number else {
        let results: Vec<Vec<_>> = stream::iter(
            plans
                .into_iter()
                .map(|plan| execute_plan(provider, block, plan, extractor)),
        )
        .buffer_unordered(config.max_concurrent_calls)
        .collect()
        .await;
        return results.into_iter().flatten().collect();
    };

    // Plans targeting the dispatcher address itself cannot share a bundle's
    // override map (their extractor override would clobber the dispatcher
    // override); ship those per-call.
    let (conflicting, bundleable): (Vec<_>, Vec<_>) = plans.into_iter().partition(
        |plan| matches!(plan, CallPlan::Single { target, .. } if *target == MULTICALL3_ADDRESS),
    );
    let groups = group_plans_for_call_many(bundleable, config.max_slots_per_request);
    let group_futs = groups.into_iter().map(|group| async move {
        match execute_plans_call_many(provider, number, &group, extractor).await {
            Ok(results) => results,
            Err(e) => {
                // Request-level failure (method unsupported, transport,
                // malformed response): re-dispatch this request's chunks as
                // plain eth_calls so the fetch still succeeds.
                warn!(
                    error = %e,
                    chunks = group.len(),
                    "eth_callMany dispatch failed; re-dispatching per-call"
                );
                let mut results = Vec::new();
                for plan in group {
                    results.extend(execute_plan(provider, block, plan, extractor).await);
                }
                results
            }
        }
    });
    let mut results: Vec<_> = stream::iter(group_futs)
        .buffer_unordered(config.max_concurrent_calls)
        .collect::<Vec<Vec<_>>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    for plan in conflicting {
        results.extend(execute_plan(provider, block, plan, extractor).await);
    }
    results
}

/// Number of `eth_call`s a request set will be split into under `config`.
///
/// Useful for CU budgeting on metered providers: the bulk path costs
/// `planned_call_count(..) × cost(eth_call)` (26 CU each on Alchemy) versus
/// `requests.len() × cost(eth_getStorageAt)` (20 CU each) for point reads.
pub fn planned_call_count(requests: &[(Address, U256)], config: &BulkCallConfig) -> usize {
    plan_calls(requests, &config.normalized()).len()
}

/// Build a [`StorageBatchFetchFn`] backed by call-override bulk extraction.
///
/// Install it with [`EvmCache::set_storage_batch_fetcher`](crate::cache::EvmCache::set_storage_batch_fetcher);
/// every batch consumer (freshness verification, cold-start verify/probe,
/// reactive point-read resyncs, prefetch) then loads storage through bulk
/// `eth_call`s. Requires a multi-thread tokio runtime, like the default
/// fetcher.
///
/// Failed slots are reported as errors; use
/// [`bulk_call_storage_fetcher_with_fallback`] to repair them with classic
/// point reads instead.
pub fn bulk_call_storage_fetcher<P: Provider<AnyNetwork> + 'static>(
    provider: Arc<P>,
    config: BulkCallConfig,
) -> StorageBatchFetchFn {
    make_fetcher(provider, config, None)
}

/// [`bulk_call_storage_fetcher`] with a repair path.
///
/// `fallback` (typically the cache's default point-read fetcher, obtained via
/// [`EvmCache::storage_batch_fetcher`](crate::cache::EvmCache::storage_batch_fetcher)
/// before replacing it) is invoked for:
/// - requests smaller than [`BulkCallConfig::point_read_threshold`], where a
///   point read is cheaper than an `eth_call` on CU-metered providers; and
/// - any pairs the bulk path reported as errors (provider without
///   state-override support, precompile targets, transport failures).
pub fn bulk_call_storage_fetcher_with_fallback<P: Provider<AnyNetwork> + 'static>(
    provider: Arc<P>,
    config: BulkCallConfig,
    fallback: StorageBatchFetchFn,
) -> StorageBatchFetchFn {
    make_fetcher(provider, config, Some(fallback))
}

fn make_fetcher<P: Provider<AnyNetwork> + 'static>(
    provider: Arc<P>,
    config: BulkCallConfig,
    fallback: Option<StorageBatchFetchFn>,
) -> StorageBatchFetchFn {
    let config = config.normalized();
    // After this many *consecutive* batches where every slot failed with a
    // provider-level error (the signature of an endpoint without
    // state-override support), stop attempting bulk extraction and route
    // straight to the fallback. Sticky for the fetcher's lifetime — install a
    // fresh fetcher to retry bulk extraction. Only meaningful when a fallback
    // exists; without one the bulk attempt is the only option anyway.
    const OVERRIDE_FAILURE_LATCH: usize = 2;
    let consecutive_failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Arc::new(move |requests: Vec<(Address, U256)>, block: BlockId| {
        use std::sync::atomic::Ordering;
        if requests.is_empty() {
            return Vec::new();
        }
        if let Some(fallback) = &fallback
            && (requests.len() < config.point_read_threshold
                || consecutive_failures.load(Ordering::Relaxed) >= OVERRIDE_FAILURE_LATCH)
        {
            return fallback(requests, block);
        }

        // Guard against panicking inside `block_in_place` on a current-thread
        // runtime (or when no runtime is present): report an `Err` result for
        // every requested slot instead, mirroring the default fetcher. The
        // guard errors still flow through the fallback repair below — a
        // synchronous fallback can serve them even where the bulk path can't
        // run.
        let bulk_results = match block_in_place_handle() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(fetch_slots_bulk(provider.as_ref(), requests, block, config))
            }),
            Err(e) => requests
                .into_iter()
                .map(|(addr, slot)| (addr, slot, Err(StorageFetchError::Runtime(e.clone()))))
                .collect(),
        };

        let Some(fallback) = &fallback else {
            return bulk_results;
        };

        // Latch bookkeeping: any success resets the streak; a batch where
        // *everything* failed at the provider level counts toward latching.
        if bulk_results.iter().any(|(_, _, r)| r.is_ok()) {
            consecutive_failures.store(0, Ordering::Relaxed);
        } else if bulk_results
            .iter()
            .any(|(_, _, r)| matches!(r, Err(StorageFetchError::Provider { .. })))
        {
            let streak = consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
            if streak == OVERRIDE_FAILURE_LATCH {
                warn!(
                    streak,
                    "bulk storage extraction failed consecutive batches with provider errors; \
                     latching this fetcher to the point-read fallback (install a fresh fetcher \
                     to retry bulk extraction)"
                );
            }
        }

        // Repair failed pairs (with multiplicity) through the fallback,
        // preserving the one-result-per-request contract.
        let mut repaired = Vec::with_capacity(bulk_results.len());
        let mut failed: Vec<(Address, U256)> = Vec::new();
        for (addr, slot, result) in bulk_results {
            match result {
                Ok(value) => repaired.push((addr, slot, Ok(value))),
                Err(_) => failed.push((addr, slot)),
            }
        }
        if !failed.is_empty() {
            warn!(
                failed = failed.len(),
                "bulk storage extraction failed for some slots; repairing via fallback fetcher"
            );
            repaired.extend(fallback(failed, block));
        }
        repaired
    })
}

// ---------------------------------------------------------------------------
// Custom storage programs & companion extractors
// ---------------------------------------------------------------------------

/// A caller-supplied extraction program: arbitrary bytecode injected at
/// `target` through a code override and executed by one `eth_call`.
///
/// The slot-list extractor requires the client to know every slot key up
/// front. A custom program removes that constraint — it can *derive* what to
/// read inside the EVM. Example: a Uniswap V3 loader that walks the
/// `tickBitmap` words on-chain and returns every initialized tick's data in a
/// single round trip, with no calldata at all (the two-phase
/// bitmap-then-ticks pattern collapsed into one call). The program runs at
/// the target's address, so `SLOAD` reads the target's real storage; the
/// output format is whatever the program returns — decoding is the caller's
/// contract with its own bytecode.
///
/// See `examples/bulk_storage_bench.rs` for a worked program (a one-shot
/// Uniswap V3 observation-ring loader that reads the cardinality from
/// `slot0` and returns the whole ring) and the offline revm tests in
/// `tests/bulk_storage.rs` that execute it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageProgram {
    /// Address whose storage the program reads (its code is replaced by
    /// `code` for the duration of the call).
    pub target: Address,
    /// Runtime bytecode to inject at `target`.
    pub code: Bytes,
    /// Calldata passed to the program (may be empty).
    pub calldata: Bytes,
}

/// Execute one [`StorageProgram`] via `eth_call` and return its raw output.
pub async fn run_storage_program<P: Provider<AnyNetwork>>(
    provider: &P,
    block: BlockId,
    program: &StorageProgram,
) -> StorageFetchResult<Bytes> {
    let mut overrides = StateOverride::default();
    overrides.insert(
        program.target,
        AccountOverride::default().with_code(program.code.clone()),
    );
    let tx = TransactionRequest::default()
        .to(program.target)
        .input(program.calldata.clone().into());
    provider
        .client()
        .request("eth_call", (tx, block, overrides))
        .await
        .map_err(|e| StorageFetchError::provider("eth_call", &e))
}

/// Execute several [`StorageProgram`]s, batching programs with distinct
/// targets into a single Multicall3-dispatched `eth_call`.
///
/// Programs that share a target address (each needs its own code override at
/// that key) or that target the dispatcher address run as individual calls.
/// Results are returned in input order, one per program.
pub async fn run_storage_programs<P: Provider<AnyNetwork>>(
    provider: &P,
    block: BlockId,
    programs: &[StorageProgram],
) -> Vec<StorageFetchResult<Bytes>> {
    let mut seen = std::collections::HashSet::new();
    let mut bundle: Vec<usize> = Vec::new();
    let mut individual: Vec<usize> = Vec::new();
    for (index, program) in programs.iter().enumerate() {
        if program.target != MULTICALL3_ADDRESS && seen.insert(program.target) {
            bundle.push(index);
        } else {
            individual.push(index);
        }
    }
    // A bundle of one is just an ordinary call with dispatch overhead.
    if bundle.len() == 1 {
        individual.append(&mut bundle);
    }

    let mut out: Vec<Option<StorageFetchResult<Bytes>>> = vec![None; programs.len()];

    if !bundle.is_empty() {
        let mut overrides = StateOverride::default();
        overrides.insert(
            MULTICALL3_ADDRESS,
            AccountOverride::default().with_code(multicall3_runtime_code().clone()),
        );
        let calls: Vec<IMulticall3::Call3> = bundle
            .iter()
            .map(|&index| {
                let program = &programs[index];
                overrides.insert(
                    program.target,
                    AccountOverride::default().with_code(program.code.clone()),
                );
                IMulticall3::Call3 {
                    target: program.target,
                    allowFailure: true,
                    callData: program.calldata.clone(),
                }
            })
            .collect();
        let data: Bytes = IMulticall3::aggregate3Call { calls }.abi_encode().into();
        let tx = TransactionRequest::default()
            .to(MULTICALL3_ADDRESS)
            .input(data.into());
        let response: Result<Bytes, _> = provider
            .client()
            .request("eth_call", (tx, block, overrides))
            .await;
        match response
            .map_err(|e| StorageFetchError::provider("eth_call", &e))
            .and_then(|bytes| {
                IMulticall3::aggregate3Call::abi_decode_returns(&bytes).map_err(|e| {
                    StorageFetchError::custom(format!("failed to decode aggregate3 response: {e}"))
                })
            }) {
            Ok(results) if results.len() == bundle.len() => {
                for (&index, result) in bundle.iter().zip(results) {
                    out[index] = Some(if result.success {
                        Ok(result.returnData)
                    } else {
                        Err(StorageFetchError::custom(
                            "storage program subcall failed (allowFailure=true)",
                        ))
                    });
                }
            }
            Ok(results) => {
                let err = StorageFetchError::custom(format!(
                    "aggregate3 returned {} results for {} programs",
                    results.len(),
                    bundle.len()
                ));
                for &index in &bundle {
                    out[index] = Some(Err(err.clone()));
                }
            }
            Err(err) => {
                for &index in &bundle {
                    out[index] = Some(Err(err.clone()));
                }
            }
        }
    }

    for index in individual {
        out[index] = Some(run_storage_program(provider, block, &programs[index]).await);
    }

    out.into_iter()
        .map(|entry| entry.expect("every program resolved"))
        .collect()
}

/// Account-fields extractor: calldata is a contiguous array of 32-byte
/// left-padded addresses; the return data is `[balance, extcodehash]` (two
/// words) per address, via the `BALANCE` and `EXTCODEHASH` opcodes.
///
/// ```text
/// [00] PUSH0            counter = 0
/// [01] JUMPDEST         loop: exit when counter == calldatasize
/// [08] DUP1 CALLDATALOAD                addr
/// [0a] DUP1 BALANCE     mem[2*counter]        = balance(addr)
/// [11] EXTCODEHASH      mem[2*counter + 32]   = extcodehash(addr)
/// [1a] counter += 32; loop
/// [20] JUMPDEST RETURN(0, 2*calldatasize)
/// ```
///
/// Requires `PUSH0` (Shanghai). Nonces and storage roots are **not**
/// EVM-visible — use `eth_getProof` (the [`AccountProofFetchFn`] path) when
/// those are needed.
///
/// [`AccountProofFetchFn`]: crate::cache::AccountProofFetchFn
pub const ACCOUNT_FIELDS_EXTRACTOR_CODE: &[u8] =
    &hex!("5f5b803614602057803580318260011b523f8160011b602001526020016001565b3660011b5ff3");

/// Balance + code hash of one account, as sampled in-EVM by
/// [`ACCOUNT_FIELDS_EXTRACTOR_CODE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountFieldsSample {
    /// Native balance (`BALANCE`).
    pub balance: U256,
    /// `EXTCODEHASH` semantics (EIP-1052): zero for a non-existent account,
    /// `keccak256("")` for an existing code-less account (EOA).
    pub code_hash: B256,
}

/// Fetch balance + code hash for many accounts in **one** `eth_call`.
///
/// `BALANCE`/`EXTCODEHASH` read *other* accounts, so only one code override
/// (the extractor host, at [`MULTICALL3_ADDRESS`]) is injected and the
/// queried accounts are untouched. Costs ~5.3k gas per address (two cold
/// account accesses), so thousands of accounts fit in one call. Querying the
/// host address itself reports the extractor's own code hash — give it a
/// dedicated `eth_getProof` instead.
pub async fn fetch_account_fields_bulk<P: Provider<AnyNetwork>>(
    provider: &P,
    addresses: &[Address],
    block: BlockId,
) -> StorageFetchResult<Vec<(Address, AccountFieldsSample)>> {
    if addresses.is_empty() {
        return Ok(Vec::new());
    }
    let mut calldata = Vec::with_capacity(addresses.len() * 32);
    for address in addresses {
        calldata.extend_from_slice(&[0u8; 12]);
        calldata.extend_from_slice(address.as_slice());
    }
    let program = StorageProgram {
        target: MULTICALL3_ADDRESS,
        code: Bytes::from_static(ACCOUNT_FIELDS_EXTRACTOR_CODE),
        calldata: calldata.into(),
    };
    let bytes = run_storage_program(provider, block, &program).await?;
    if bytes.len() != addresses.len() * 64 {
        return Err(StorageFetchError::custom(format!(
            "account-fields extractor returned {} bytes, expected {}",
            bytes.len(),
            addresses.len() * 64
        )));
    }
    Ok(addresses
        .iter()
        .enumerate()
        .map(|(i, address)| {
            (
                *address,
                AccountFieldsSample {
                    balance: U256::from_be_slice(&bytes[i * 64..i * 64 + 32]),
                    code_hash: B256::from_slice(&bytes[i * 64 + 32..i * 64 + 64]),
                },
            )
        })
        .collect())
}

/// Block-context extractor: no calldata; returns seven words —
/// `NUMBER`, `TIMESTAMP`, `BASEFEE`, `COINBASE`, `PREVRANDAO`, `GASLIMIT`,
/// `CHAINID` — straight from the EVM environment of the queried block.
/// Piggybacks block-header context onto the same transport as slot loads
/// without an `eth_getBlockByNumber`. Requires `PUSH0` (Shanghai).
pub const BLOCK_CONTEXT_EXTRACTOR_CODE: &[u8] =
    &hex!("435f52426020524860405241606052446080524560a0524660c05260e05ff3");

/// One block's EVM-visible context, as sampled by
/// [`BLOCK_CONTEXT_EXTRACTOR_CODE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockContextSample {
    /// `NUMBER`.
    pub number: u64,
    /// `TIMESTAMP`.
    pub timestamp: u64,
    /// `BASEFEE` (wei).
    pub basefee: U256,
    /// `COINBASE`.
    pub coinbase: Address,
    /// `PREVRANDAO` (post-merge mix hash).
    pub prevrandao: B256,
    /// `GASLIMIT`.
    pub gas_limit: u64,
    /// `CHAINID`.
    pub chain_id: u64,
}

/// Sample a block's EVM context in one `eth_call` (see
/// [`BLOCK_CONTEXT_EXTRACTOR_CODE`]).
pub async fn fetch_block_context<P: Provider<AnyNetwork>>(
    provider: &P,
    block: BlockId,
) -> StorageFetchResult<BlockContextSample> {
    let program = StorageProgram {
        target: MULTICALL3_ADDRESS,
        code: Bytes::from_static(BLOCK_CONTEXT_EXTRACTOR_CODE),
        calldata: Bytes::new(),
    };
    let bytes = run_storage_program(provider, block, &program).await?;
    if bytes.len() != 7 * 32 {
        return Err(StorageFetchError::custom(format!(
            "block-context extractor returned {} bytes, expected 224",
            bytes.len()
        )));
    }
    let word = |i: usize| U256::from_be_slice(&bytes[i * 32..(i + 1) * 32]);
    let to_u64 = |v: U256| u64::try_from(v).unwrap_or(u64::MAX);
    Ok(BlockContextSample {
        number: to_u64(word(0)),
        timestamp: to_u64(word(1)),
        basefee: word(2),
        coinbase: Address::from_slice(&bytes[3 * 32 + 12..4 * 32]),
        prevrandao: B256::from_slice(&bytes[4 * 32..5 * 32]),
        gas_limit: to_u64(word(5)),
        chain_id: to_u64(word(6)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    fn cfg(max_slots: usize, max_targets: usize) -> BulkCallConfig {
        BulkCallConfig {
            max_slots_per_call: max_slots,
            max_targets_per_call: max_targets,
            ..BulkCallConfig::default()
        }
    }

    #[test]
    fn pack_and_decode_roundtrip() {
        let slots = vec![U256::ZERO, U256::from(1u64), U256::MAX];
        let packed = pack_slots_calldata(&slots);
        assert_eq!(packed.len(), 96);
        assert_eq!(&packed[32..64], &U256::from(1u64).to_be_bytes::<32>());
        let decoded = decode_packed_values(&packed, 3).expect("exact length");
        assert_eq!(decoded, slots);
        assert!(decode_packed_values(&packed, 2).is_none());
        assert!(decode_packed_values(&packed[..95], 3).is_none());
    }

    #[test]
    fn extractor_constants_are_wellformed() {
        // Anchor the exact published bytecode; the EVM-level behavior of both
        // variants is exercised end-to-end in tests/bulk_storage.rs.
        assert_eq!(STORAGE_EXTRACTOR_CODE.len(), 23);
        assert_eq!(STORAGE_EXTRACTOR_CODE[0], 0x5f, "PUSH0 entry");
        assert_eq!(STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI.len(), 25);
        assert!(
            !STORAGE_EXTRACTOR_CODE_PRE_SHANGHAI.contains(&0x5f),
            "pre-Shanghai variant must not use PUSH0"
        );
        assert!(multicall3_runtime_code().len() > 1_000);
    }

    #[test]
    fn planning_single_small_group() {
        let requests = vec![
            (addr(0xaa), U256::from(1u64)),
            (addr(0xaa), U256::from(2u64)),
        ];
        let plans = plan_calls(&requests, &cfg(100, 10));
        assert_eq!(
            plans,
            vec![CallPlan::Single {
                target: addr(0xaa),
                slots: vec![U256::from(1u64), U256::from(2u64)],
            }]
        );
    }

    #[test]
    fn planning_splits_oversized_target_and_packs_remainder() {
        // 7 slots with a 3-slot budget: two full single-target chunks + the
        // 1-slot remainder packed with the other small target.
        let mut requests: Vec<_> = (0..7u64).map(|i| (addr(0x01), U256::from(i))).collect();
        requests.push((addr(0x02), U256::from(99u64)));
        let plans = plan_calls(&requests, &cfg(3, 10));
        assert_eq!(plans.len(), 3);
        assert_eq!(
            plans[0],
            CallPlan::Single {
                target: addr(0x01),
                slots: (0..3u64).map(U256::from).collect(),
            }
        );
        assert_eq!(
            plans[1],
            CallPlan::Single {
                target: addr(0x01),
                slots: (3..6u64).map(U256::from).collect(),
            }
        );
        assert_eq!(
            plans[2],
            CallPlan::Multi {
                targets: vec![
                    (addr(0x01), vec![U256::from(6u64)]),
                    (addr(0x02), vec![U256::from(99u64)]),
                ],
            }
        );
        let planned: usize = plans.iter().map(CallPlan::request_slot_count).sum();
        assert_eq!(planned, requests.len());
    }

    #[test]
    fn planning_respects_target_budget() {
        let requests: Vec<_> = (0..5u8)
            .map(|i| (addr(i + 1), U256::from(i as u64)))
            .collect();
        let plans = plan_calls(&requests, &cfg(100, 2));
        // 5 single-slot targets with a 2-target budget: 2 + 2 + 1.
        assert_eq!(plans.len(), 3);
        assert!(matches!(&plans[0], CallPlan::Multi { targets } if targets.len() == 2));
        assert!(matches!(&plans[1], CallPlan::Multi { targets } if targets.len() == 2));
        assert!(matches!(&plans[2], CallPlan::Single { .. }));
    }

    #[test]
    fn planning_lone_remainder_degrades_to_single_call() {
        let requests: Vec<_> = (0..4u64).map(|i| (addr(0x01), U256::from(i))).collect();
        let plans = plan_calls(&requests, &cfg(3, 10));
        assert_eq!(plans.len(), 2);
        assert!(matches!(&plans[1], CallPlan::Single { slots, .. } if slots.len() == 1));
    }

    #[test]
    fn planning_isolates_dispatcher_address_collision() {
        let requests = vec![
            (MULTICALL3_ADDRESS, U256::from(1u64)),
            (addr(0x02), U256::from(2u64)),
            (addr(0x03), U256::from(3u64)),
        ];
        let plans = plan_calls(&requests, &cfg(100, 10));
        assert_eq!(
            plans[0],
            CallPlan::Single {
                target: MULTICALL3_ADDRESS,
                slots: vec![U256::from(1u64)],
            }
        );
        assert!(matches!(&plans[1], CallPlan::Multi { targets } if targets.len() == 2));
    }

    #[test]
    fn multi_target_overrides_include_dispatcher_and_extractors() {
        let plan = CallPlan::Multi {
            targets: vec![
                (addr(0x02), vec![U256::from(1u64)]),
                (addr(0x03), vec![U256::from(2u64)]),
            ],
        };
        let extractor = Bytes::from_static(STORAGE_EXTRACTOR_CODE);
        let overrides = overrides_for_plan(&plan, &extractor);
        assert_eq!(overrides.len(), 3);
        assert_eq!(
            overrides[&MULTICALL3_ADDRESS].code.as_ref(),
            Some(multicall3_runtime_code())
        );
        assert_eq!(overrides[&addr(0x02)].code.as_ref(), Some(&extractor));
        assert_eq!(overrides[&addr(0x03)].code.as_ref(), Some(&extractor));
    }

    #[test]
    fn call_many_grouping_respects_request_budget() {
        let plans = vec![
            CallPlan::Single {
                target: addr(0x01),
                slots: (0..6u64).map(U256::from).collect(),
            },
            CallPlan::Single {
                target: addr(0x02),
                slots: (0..6u64).map(U256::from).collect(),
            },
            CallPlan::Single {
                target: addr(0x03),
                slots: (0..2u64).map(U256::from).collect(),
            },
        ];
        let groups = group_plans_for_call_many(plans, 10);
        // 6 + 6 > 10 → split; 6 + 2 ≤ 10 → packed together.
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 1);
        assert_eq!(groups[1].len(), 2);
        let total: usize = groups
            .iter()
            .flatten()
            .map(CallPlan::request_slot_count)
            .sum();
        assert_eq!(total, 14);
    }

    #[test]
    fn multi_target_response_decodes_per_target_failures() {
        let targets = vec![
            (addr(0x02), vec![U256::from(1u64), U256::from(2u64)]),
            (addr(0x03), vec![U256::from(3u64)]),
        ];
        let response = IMulticall3::aggregate3Call::abi_encode_returns(&vec![
            IMulticall3::Result {
                success: true,
                returnData: pack_slots_calldata(&[U256::from(11u64), U256::from(22u64)]),
            },
            IMulticall3::Result {
                success: false,
                returnData: Bytes::new(),
            },
        ]);
        let results = decode_multi_target_response(&targets, &response);
        assert_eq!(results.len(), 3);
        assert!(matches!(results[0], (_, _, Ok(v)) if v == U256::from(11u64)));
        assert!(matches!(results[1], (_, _, Ok(v)) if v == U256::from(22u64)));
        assert!(results[2].2.is_err());
    }
}
