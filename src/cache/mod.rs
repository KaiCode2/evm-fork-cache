//! The forked-EVM state cache: lazy RPC loading, a layered write funnel, and
//! cheap copy-on-write snapshots.
//!
//! [`EvmCache`] is the core handle. It fronts a [`foundry_fork_db`]-backed fork
//! database with a hot [`revm`] cache layer, lazily fetching account and storage
//! state from a provider on the first miss and serving it locally thereafter.
//! Targeted writes and purges ([`StateUpdate`], balance/code overrides, verified
//! code seeds) flow through a single write
//! funnel — never the RPC path — so event-driven state maintenance never round-trips.
//! [`EvmCache::snapshot`] produces an immutable, `Arc`-shared, cross-thread
//! [`EvmSnapshot`] (see [`snapshot`]) for parallel fan-out; [`overlay`]
//! layers per-simulation state on top. See the crate-root docs for the full
//! state stack and [`docs/INTERNALS.md`](https://github.com/KaiCode2/evm-fork-cache/blob/main/docs/INTERNALS.md)
//! for the snapshot cost model.

mod binary_state;
mod bytecode;
mod code_seeds;
mod journal_access_list;
mod metadata;
pub mod overlay;
pub mod slot_observations;
pub mod snapshot;
pub(crate) mod versioned;

pub use binary_state::{load_binary_state, save_binary_state};
pub use metadata::{CacheConfig, ImmutableDataCache};
pub use overlay::EvmOverlay;
pub use slot_observations::SlotObservationTracker;
pub use snapshot::EvmSnapshot;

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    fs,
    rc::Rc,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use alloy_consensus::BlockHeader;
use alloy_eips::eip2930::AccessList;
use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_network::BlockResponse;
use alloy_primitives::{Address, B256, Bytes, I256, Log, TxKind, U256, keccak256};
use alloy_provider::{Provider, network::AnyNetwork};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::{SolCall, SolValue, sol};
use foundry_fork_db::{
    BlockchainDb, SharedBackend, backend::BlockingMode, cache::BlockchainDbMeta,
};
use revm::{
    Context, ExecuteCommitEvm, ExecuteEvm, InspectEvm, MainBuilder, MainContext,
    context::{BlockEnv, CfgEnv, Journal, LocalContext, TxEnv, result::ExecutionResult},
    context_interface::JournalTr,
    database::{AccountState, CacheDB},
    primitives::hardfork::SpecId,
    state::{Account, AccountInfo, Bytecode},
};
use tracing::{debug, instrument, trace, warn};

use crate::access_set::StorageAccessList;
use crate::bulk_storage::AccountFieldsSample;
use crate::errors::{
    BlockContextError, CacheError, CacheResult as Result, RpcError, RuntimeError, SimError,
    SimHostError, SimulationError, SimulationResult, StorageFetchError, StorageFetchResult,
};
use crate::freshness::{SlotChange, SlotFetch, SlotOutcome};
use crate::inspector::TransferInspector;
use crate::mapping_probe::{
    HashSlotAccess, HashStorageProbe, SlotLayout, TrackedBalances, TrackedMapping,
};
use crate::state_update::{
    AccountChange, AccountPatch, PurgeRecord, PurgeScope, SkippedAccountPatch, SkippedBalanceDelta,
    SkippedDelta, SkippedMask, SlotDelta, StateDiff, StateUpdate,
};

use bytecode::BytecodeCache;
use code_seeds::CodeSeedCache;
pub use code_seeds::CodeSeedState;
use journal_access_list::{extract_access_list, merge_access_lists};

/// Re-export AnyNetwork for callers that need to construct providers.
pub use alloy_provider::network::AnyNetwork as AnyNetworkType;

/// The database type used by the EVM cache.
/// CacheDB wraps SharedBackend which lazily fetches data from RPC on-demand.
pub type ForkCacheDB = CacheDB<SharedBackend>;

/// Callback for making direct RPC `eth_call` requests, bypassing revm simulation.
/// Used when batch-querying many contracts where revm's lazy storage fetching would
/// be prohibitively slow (e.g. querying 500+ gauge contracts).
pub type RpcCallFn = Arc<dyn Fn(Address, Bytes) -> Result<Bytes, RpcError> + Send + Sync>;

/// Callback for batch-fetching storage slots directly from RPC, bypassing SharedBackend.
///
/// Used by callers that need bulk storage reads without many individual channel
/// round-trips through SharedBackend. Fires concurrent `eth_getStorageAt` calls
/// directly via the provider and returns results for bulk injection into
/// BlockchainDb.
/// Users may replace the provider-backed implementation with their own fetcher via
/// [`EvmCache::set_storage_batch_fetcher`].
///
/// The second argument pins the fetch to a specific block. Callers pass the
/// cache's pinned block at the point they schedule the fetch; deferred callers
/// such as the freshness validator pass the block their snapshot was built from,
/// so a concurrent [`EvmCache::set_block`] cannot make the deferred fetch read a
/// *different* block than the snapshot it is compared against.
///
/// **Contract:** an implementation must return **exactly one** result tuple per
/// requested `(address, slot)` (order does not matter). Callers — `verify_slots`,
/// `reconcile_slots`, and the cold-start verify/probe paths — derive their
/// per-slot outcomes from the returned tuples, so a fetcher that drops, dedups,
/// reorders-and-truncates, or duplicates entries breaks the "one outcome per
/// requested slot" guarantee those APIs document.
pub type StorageBatchFetchFn = Arc<
    dyn Fn(Vec<(Address, U256)>, BlockId) -> Vec<(Address, U256, StorageFetchResult<U256>)>
        + Send
        + Sync,
>;

/// Account header + optional storage-proof slots from `eth_getProof`.
/// `slots` is populated only for requested storage keys; an empty key list is a
/// root-only probe (account fields + `storage_hash`, no slot payload).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountProof {
    /// Merkle root of the account's storage trie (`storageHash`).
    pub storage_hash: B256,
    /// Account balance.
    pub balance: U256,
    /// Account nonce.
    pub nonce: u64,
    /// Hash of the account's runtime code (`codeHash`).
    pub code_hash: B256,
    /// Proven `(slot, value)` pairs for the requested storage keys. Empty for a
    /// root-only probe.
    pub slots: Vec<(U256, U256)>,
}

/// Callback for fetching account headers (and optional storage-proof slots)
/// directly from RPC via `eth_getProof`, mirroring [`StorageBatchFetchFn`].
///
/// Used by callers that need authoritative account fields (balance/nonce/code
/// hash) plus the account's `storageHash`, e.g. account-target resyncs and
/// account-level freshness. Each request is a `(address, keys)` pair; an empty
/// `keys` list is a root-only probe.
///
/// The second argument pins the fetch to a specific block, matching
/// [`StorageBatchFetchFn`]'s block semantics.
///
/// **Contract:** an implementation returns at most one result per requested
/// address. An address present with `Ok(..)` succeeded; present with `Err(..)`
/// failed; omitted entirely means the fetcher produced no result for it. Callers
/// derive their per-address outcome from whether the address appears and, if so,
/// whether it is `Ok`/`Err`.
pub type AccountProofFetchFn = Arc<
    dyn Fn(Vec<(Address, Vec<U256>)>, BlockId) -> Vec<(Address, StorageFetchResult<AccountProof>)>
        + Send
        + Sync,
>;

/// Callback fetching `(balance, EXTCODEHASH)` samples for many addresses at a
/// pinned block — one bulk `eth_call` by default (the
/// [`ACCOUNT_FIELDS_EXTRACTOR_CODE`](crate::bulk_storage::ACCOUNT_FIELDS_EXTRACTOR_CODE)
/// program). Sync and type-erased with the same bridging rules as
/// [`StorageBatchFetchFn`] (multi-thread tokio runtime required for the
/// default provider-backed implementation).
///
/// **Contract:** the call is all-or-nothing — `Ok` carries one sample per
/// requested address (an omitted address is treated by callers as
/// unverifiable), `Err` means the whole fetch failed and nothing can be
/// concluded about any address. Used by
/// [`EvmCache::verify_code_seeds`](EvmCache::verify_code_seeds) and the
/// cold-start `verify_code` phase.
pub type AccountFieldsFetchFn = Arc<
    dyn Fn(Vec<Address>, BlockId) -> StorageFetchResult<Vec<(Address, AccountFieldsSample)>>
        + Send
        + Sync,
>;

/// Final state changes observed from a block-level state-diff trace.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockStateDiff {
    /// Accounts changed by the traced block.
    pub accounts: Vec<BlockStateAccountDiff>,
}

/// Final account/storage values observed for one account in a block trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockStateAccountDiff {
    /// Changed account address.
    pub address: Address,
    /// Final balance when the trace reports a balance change.
    pub balance: Option<U256>,
    /// Final nonce when the trace reports a nonce change.
    pub nonce: Option<u64>,
    /// Final runtime bytecode when the trace reports a code change.
    pub code: Option<Bytes>,
    /// Final storage-slot values reported for this account.
    pub storage: Vec<BlockStateStorageDiff>,
}

/// Final value for one storage slot observed from a block trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockStateStorageDiff {
    /// Storage slot key.
    pub slot: U256,
    /// Final slot value after the block. Cleared slots are represented as zero.
    pub value: U256,
}

/// Callback for fetching one block's state diff through debug/trace RPC.
///
/// The callback returns final post-block values for accounts/storage slots that
/// changed in the block. Callers may resolve matching resync targets from this
/// diff before falling back to point reads.
pub type BlockStateDiffFetchFn =
    Arc<dyn Fn(BlockId) -> StorageFetchResult<BlockStateDiff> + Send + Sync>;

/// Return a tokio runtime [`Handle`] suitable for `block_in_place` + `block_on`,
/// or an error describing why one is unavailable.
///
/// The RPC-backed callbacks ([`RpcCallFn`], [`StorageBatchFetchFn`]) drive async
/// work synchronously via `tokio::task::block_in_place`. That helper panics on a
/// current-thread runtime, and `Handle::current()` panics when no runtime is
/// present. To avoid panicking deep inside a callback, callers use this guard to
/// degrade to a typed error instead.
///
/// Requires a **multi-thread** tokio runtime.
pub(crate) fn block_in_place_handle() -> Result<tokio::runtime::Handle, RuntimeError> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => Err(RuntimeError::CurrentThreadRuntime),
            _ => Ok(handle),
        },
        Err(e) => Err(RuntimeError::MissingRuntime {
            details: e.to_string(),
        }),
    }
}

fn trace_rpc_method_and_params(block: BlockId) -> (&'static str, serde_json::Value) {
    let tracer = serde_json::json!({
        "tracer": "prestateTracer",
        "tracerConfig": {
            "diffMode": true,
        },
    });
    match block {
        BlockId::Hash(hash) => (
            "debug_traceBlockByHash",
            serde_json::json!([hash.block_hash, tracer]),
        ),
        BlockId::Number(number) => (
            "debug_traceBlockByNumber",
            serde_json::json!([block_number_or_tag_param(number), tracer]),
        ),
    }
}

fn block_number_or_tag_param(number: BlockNumberOrTag) -> serde_json::Value {
    match number {
        BlockNumberOrTag::Number(number) => serde_json::json!(format!("{number:#x}")),
        BlockNumberOrTag::Latest => serde_json::json!("latest"),
        BlockNumberOrTag::Finalized => serde_json::json!("finalized"),
        BlockNumberOrTag::Safe => serde_json::json!("safe"),
        BlockNumberOrTag::Earliest => serde_json::json!("earliest"),
        BlockNumberOrTag::Pending => serde_json::json!("pending"),
    }
}

fn parse_block_state_diff_trace(value: &serde_json::Value) -> Result<BlockStateDiff> {
    let mut accounts: HashMap<Address, BlockStateAccountDiff> = HashMap::new();
    match value {
        serde_json::Value::Array(traces) => {
            for trace in traces {
                merge_trace_diff(trace, &mut accounts)?;
            }
        }
        trace => merge_trace_diff(trace, &mut accounts)?,
    }

    let mut accounts: Vec<_> = accounts.into_values().collect();
    accounts.sort_by_key(|account| account.address);
    for account in &mut accounts {
        account.storage.sort_by_key(|slot| slot.slot);
    }
    Ok(BlockStateDiff { accounts })
}

fn merge_trace_diff(
    trace: &serde_json::Value,
    accounts: &mut HashMap<Address, BlockStateAccountDiff>,
) -> Result<()> {
    let diff = trace.get("result").unwrap_or(trace);
    let Some(pre) = diff.get("pre").and_then(serde_json::Value::as_object) else {
        return Ok(());
    };
    let Some(post) = diff.get("post").and_then(serde_json::Value::as_object) else {
        return Ok(());
    };

    for (address, post_account) in post {
        let address = parse_trace_address(address)?;
        let entry = accounts
            .entry(address)
            .or_insert_with(|| empty_block_state_account_diff(address));

        if let Some(balance) = post_account.get("balance") {
            entry.balance = Some(parse_trace_u256(balance)?);
        }
        if let Some(nonce) = post_account.get("nonce") {
            entry.nonce = Some(parse_trace_u64(nonce)?);
        }
        if let Some(code) = post_account.get("code") {
            entry.code = Some(parse_trace_bytes(code)?);
        }
        if let Some(storage) = post_account
            .get("storage")
            .and_then(serde_json::Value::as_object)
        {
            for (slot, value) in storage {
                upsert_block_state_storage_diff(
                    entry,
                    parse_trace_u256_str(slot)?,
                    parse_trace_u256(value)?,
                );
            }
        }
    }

    // In diff mode, state that ends the block *absent* appears in `pre` but is
    // omitted from `post`. Convert those omissions into explicit final values:
    for (address_key, pre_account) in pre {
        let address = parse_trace_address(address_key)?;
        let post_account = post.get(address_key);

        // An account entirely absent from `post` was deleted by the block
        // (SELFDESTRUCT; post-Cancun, a same-tx create+destruct). Synthesize
        // the explicit post-deletion fields so account-target resyncs resolve
        // authoritatively from the trace instead of falling back to point
        // reads. A later transaction in the same block re-creating the account
        // overwrites these in its own merge pass (entries merge in tx order).
        if post_account.is_none() {
            let entry = accounts
                .entry(address)
                .or_insert_with(|| empty_block_state_account_diff(address));
            entry.balance = Some(U256::ZERO);
            entry.nonce = Some(0);
            entry.code = Some(Bytes::new());
        }

        // Storage cleared to zero: present in `pre`, omitted from `post`.
        let Some(pre_storage) = pre_account
            .get("storage")
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };
        let post_storage = post_account
            .and_then(|account| account.get("storage"))
            .and_then(serde_json::Value::as_object);
        for slot in pre_storage.keys() {
            let cleared = post_storage.is_none_or(|storage| !storage.contains_key(slot));
            if cleared {
                let entry = accounts
                    .entry(address)
                    .or_insert_with(|| empty_block_state_account_diff(address));
                upsert_block_state_storage_diff(entry, parse_trace_u256_str(slot)?, U256::ZERO);
            }
        }
    }

    Ok(())
}

fn empty_block_state_account_diff(address: Address) -> BlockStateAccountDiff {
    BlockStateAccountDiff {
        address,
        balance: None,
        nonce: None,
        code: None,
        storage: Vec::new(),
    }
}

fn upsert_block_state_storage_diff(account: &mut BlockStateAccountDiff, slot: U256, value: U256) {
    if let Some(existing) = account.storage.iter_mut().find(|entry| entry.slot == slot) {
        existing.value = value;
    } else {
        account.storage.push(BlockStateStorageDiff { slot, value });
    }
}

fn parse_trace_address(value: &str) -> Result<Address> {
    value.parse().map_err(|err| CacheError::TraceParse {
        details: format!("invalid address `{value}`: {err}"),
    })
}

fn parse_trace_u256(value: &serde_json::Value) -> Result<U256> {
    match value {
        serde_json::Value::String(value) => parse_trace_u256_str(value),
        serde_json::Value::Number(value) => parse_trace_u256_str(&value.to_string()),
        other => Err(CacheError::TraceParse {
            details: format!("expected U256 string/number, got {other:?}"),
        }),
    }
}

fn parse_trace_u256_str(value: &str) -> Result<U256> {
    if let Some(value) = value.strip_prefix("0x") {
        if value.is_empty() {
            return Ok(U256::ZERO);
        }
        return U256::from_str_radix(value, 16).map_err(|err| CacheError::TraceParse {
            details: format!("invalid U256 `0x{value}`: {err}"),
        });
    }
    if value.is_empty() {
        return Ok(U256::ZERO);
    }
    U256::from_str_radix(value, 10).map_err(|err| CacheError::TraceParse {
        details: format!("invalid U256 `{value}`: {err}"),
    })
}

fn parse_trace_u64(value: &serde_json::Value) -> Result<u64> {
    match value {
        serde_json::Value::Number(value) => value.as_u64().ok_or_else(|| CacheError::TraceParse {
            details: format!("invalid u64 number `{value}`"),
        }),
        serde_json::Value::String(value) => {
            if let Some(value) = value.strip_prefix("0x") {
                if value.is_empty() {
                    return Ok(0);
                }
                return u64::from_str_radix(value, 16).map_err(|err| CacheError::TraceParse {
                    details: format!("invalid u64 `0x{value}`: {err}"),
                });
            }
            if value.is_empty() {
                return Ok(0);
            }
            value.parse().map_err(|err| CacheError::TraceParse {
                details: format!("invalid u64 `{value}`: {err}"),
            })
        }
        other => Err(CacheError::TraceParse {
            details: format!("expected u64 string/number, got {other:?}"),
        }),
    }
}

fn parse_trace_bytes(value: &serde_json::Value) -> Result<Bytes> {
    let Some(value) = value.as_str() else {
        return Err(CacheError::TraceParse {
            details: format!("expected bytecode string, got {value:?}"),
        });
    };
    let value = value.strip_prefix("0x").unwrap_or(value);
    let bytes = alloy_primitives::hex::decode(value).map_err(|err| CacheError::TraceParse {
        details: format!("invalid bytecode hex: {err}"),
    })?;
    Ok(Bytes::from(bytes))
}

pub(crate) fn unix_timestamp_secs_saturating(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Read a storage slot from already-borrowed layers (`account_state`-aware),
/// mirroring [`EvmCache::cached_storage_value`] but operating on a held backend
/// storage guard rather than re-locking. Shared by the batched slot-run fast-path
/// ([`EvmCache::apply_slot_run`]) so the same EVM-SLOAD semantics hold inside the
/// held guard: the overlay slot wins; a `StorageCleared`/`NotExisting` overlay
/// account reads a missing slot as ZERO (the backend is **not** consulted);
/// otherwise it falls through to the backend.
fn read_slot_account_state_aware<S1, S2>(
    overlay: &std::collections::HashMap<Address, revm::database::DbAccount, S1>,
    storage: &std::collections::HashMap<Address, foundry_fork_db::cache::StorageInfo, S2>,
    address: Address,
    slot: U256,
) -> Option<U256>
where
    S1: std::hash::BuildHasher,
    S2: std::hash::BuildHasher,
{
    if let Some(db_account) = overlay.get(&address) {
        if let Some(value) = db_account.storage.get(&slot) {
            return Some(*value);
        }
        if matches!(
            db_account.account_state,
            AccountState::StorageCleared | AccountState::NotExisting
        ) {
            return Some(U256::ZERO);
        }
    }
    storage.get(&address).and_then(|s| s.get(&slot).copied())
}

/// Write a storage slot into already-borrowed layers, mirroring
/// [`EvmCache::write_slot_through`] but operating on a held backend storage guard.
/// Backend (layer 2) is always written; the overlay (layer 1) is written only if
/// an overlay account already exists (never materialize a new overlay account).
fn write_slot_into<S1, S2>(
    overlay: &mut std::collections::HashMap<Address, revm::database::DbAccount, S1>,
    storage: &mut std::collections::HashMap<Address, foundry_fork_db::cache::StorageInfo, S2>,
    address: Address,
    slot: U256,
    value: U256,
) where
    S1: std::hash::BuildHasher,
    S2: std::hash::BuildHasher + Default,
{
    storage.entry(address).or_default().insert(slot, value);
    if let Some(db_account) = overlay.get_mut(&address) {
        db_account.storage.insert(slot, value);
    }
}

fn account_patch_is_empty(patch: &AccountPatch) -> bool {
    patch.balance.is_none() && patch.nonce.is_none() && patch.code.is_none()
}

/// Preset runtime tuning profile for cache-side batch storage fetches.
///
/// Converts into [`StorageBatchConfig`]: faster modes send larger batches with
/// more in-flight HTTP requests, slower modes throttle to avoid RPC rate-limiting
/// (e.g. HTTP 429 on Base). Configure a preset per cache with
/// [`EvmCacheBuilder::speed_mode`], or supply exact values with
/// [`EvmCacheBuilder::storage_batch_config`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum CacheSpeedMode {
    /// Largest batches, highest concurrency — fastest, most likely to trip rate limits.
    Fast = 0,
    /// Moderate batch size and concurrency.
    Normal = 1,
    /// Conservative batch size and concurrency. The default.
    #[default]
    Slow = 2,
    /// Smallest batches, single in-flight request — slowest, gentlest on the RPC provider.
    XSlow = 3,
}

/// Concrete tuning knobs for the provider-backed [`StorageBatchFetchFn`].
///
/// `slots_per_batch` controls how many `eth_getStorageAt` calls are packed into
/// each JSON-RPC batch request. `max_concurrent_batches` controls how many of
/// those HTTP batch requests may be in flight at once. Larger values can improve
/// cold-start / verification throughput on tolerant RPC endpoints; smaller
/// values are gentler on rate-limited providers.
///
/// This config only affects the fetcher created by the cache constructors. If a
/// caller installs a custom fetcher with
/// [`set_storage_batch_fetcher`](EvmCache::set_storage_batch_fetcher), that
/// fetcher owns its own batching and throttling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageBatchConfig {
    /// Number of storage slots to include in one JSON-RPC batch request.
    pub slots_per_batch: usize,
    /// Maximum number of JSON-RPC batch requests in flight at once.
    pub max_concurrent_batches: usize,
}

impl StorageBatchConfig {
    /// Construct a config, normalizing zero values to one.
    pub fn new(slots_per_batch: usize, max_concurrent_batches: usize) -> Self {
        Self {
            slots_per_batch,
            max_concurrent_batches,
        }
        .normalized()
    }

    fn normalized(self) -> Self {
        Self {
            slots_per_batch: self.slots_per_batch.max(1),
            max_concurrent_batches: self.max_concurrent_batches.max(1),
        }
    }
}

impl Default for StorageBatchConfig {
    fn default() -> Self {
        CacheSpeedMode::default().into()
    }
}

impl From<CacheSpeedMode> for StorageBatchConfig {
    fn from(mode: CacheSpeedMode) -> Self {
        match mode {
            CacheSpeedMode::Fast => Self::new(150, 8),
            CacheSpeedMode::Normal => Self::new(100, 6),
            CacheSpeedMode::Slow => Self::new(75, 4),
            CacheSpeedMode::XSlow => Self::new(25, 1),
        }
    }
}

/// How a cache's batch storage fetcher loads slots.
///
/// The default is [`BulkCall`](Self::BulkCall): bulk `eth_call` state-override
/// extraction (one call covers thousands of slots) with the classic
/// point-read fetcher as its fallback and repair path. Requests below the
/// bulk config's `point_read_threshold`, providers without state-override
/// support, and precompile targets all degrade gracefully to point reads.
/// See the [`bulk_storage`](crate::bulk_storage) module and
/// `docs/bulk-storage-extraction.md` for mechanism and measured economics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFetchStrategy {
    /// Bulk `eth_call` extraction with point-read fallback (the default).
    BulkCall(crate::bulk_storage::BulkCallConfig),
    /// Classic JSON-RPC-batched `eth_getStorageAt` point reads only
    /// (pre-0.2.0 behavior), tuned by [`StorageBatchConfig`].
    PointRead,
}

impl Default for StorageFetchStrategy {
    fn default() -> Self {
        Self::BulkCall(crate::bulk_storage::BulkCallConfig::default())
    }
}

/// Build the classic point-read [`StorageBatchFetchFn`]: JSON-RPC batches of
/// `eth_getStorageAt`, sized and throttled by [`StorageBatchConfig`].
///
/// This is the default fetcher's *fallback* path (see
/// [`StorageFetchStrategy`]) and the whole fetcher under
/// [`StorageFetchStrategy::PointRead`]. It is public so callers composing
/// their own fetchers — e.g.
/// [`bulk_call_storage_fetcher_with_fallback`](crate::bulk_storage::bulk_call_storage_fetcher_with_fallback)
/// over a differently-tuned repair path — can reuse it.
pub fn point_read_storage_fetcher<P>(
    provider: Arc<P>,
    config: StorageBatchConfig,
) -> StorageBatchFetchFn
where
    P: Provider<AnyNetwork> + 'static,
{
    let config = config.normalized();
    Arc::new(
        move |requests: Vec<(Address, U256)>, current_block: BlockId| {
            use futures::stream::{self, StreamExt};
            // Max items per JSON-RPC batch. RPC providers typically limit batch
            // size to ~1000 items. Kept conservative to avoid 429s on Base.
            let batch_size = config.slots_per_batch;
            // Max concurrent HTTP batch requests. Each batch contains batch_size
            // individual eth_getStorageAt calls. Limiting concurrency prevents
            // thundering herd when prefetching thousands of storage slots.
            let max_concurrent = config.max_concurrent_batches;

            // Guard against panicking inside `block_in_place` on a
            // current-thread runtime (or when no runtime is present): return
            // an `Err` result for every requested slot instead.
            let handle = match block_in_place_handle() {
                Ok(handle) => handle,
                Err(e) => {
                    return requests
                        .into_iter()
                        .map(|(addr, slot)| {
                            (
                                addr,
                                slot,
                                Err(StorageFetchError::Runtime(RuntimeError::MissingRuntime {
                                    details: e.to_string(),
                                })),
                            )
                        })
                        .collect();
                }
            };
            // The caller supplies the exact block this fetch must observe.
            // Capturing it at the call site is what lets the deferred
            // freshness validator fetch at the snapshot's block despite a
            // later `set_block`.
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    let mut results = Vec::with_capacity(requests.len());

                    // Build and send JSON-RPC batches (each batch = one HTTP request)
                    let batch_futs: Vec<_> = requests
                        .chunks(batch_size)
                        .map(|chunk| {
                            let client = provider.client();
                            let mut batch = alloy_rpc_client::BatchRequest::new(client);
                            let mut waiters = Vec::with_capacity(chunk.len());

                            for &(addr, slot) in chunk {
                                let params = (addr, slot, current_block);
                                match batch.add_call::<_, U256>("eth_getStorageAt", &params) {
                                    Ok(waiter) => waiters.push((addr, slot, Ok(waiter))),
                                    Err(e) => {
                                        // Serialization error — rare, treat as failure
                                        tracing::warn!(
                                            ?addr,
                                            ?slot,
                                            "batch request serialization failed: {}",
                                            e
                                        );
                                        waiters.push((
                                            addr,
                                            slot,
                                            Err(StorageFetchError::serialization(e)),
                                        ));
                                    }
                                }
                            }

                            async move {
                                // Send the batch as a single HTTP request
                                let send_result = batch.send().await;
                                let mut chunk_results = Vec::with_capacity(waiters.len());

                                let batch_error =
                                    send_result.as_ref().err().map(|err| err.to_string());
                                for (addr, slot, waiter) in waiters {
                                    match waiter {
                                        Ok(waiter) => {
                                            if let Some(source) = &batch_error {
                                                chunk_results.push((
                                                    addr,
                                                    slot,
                                                    Err(StorageFetchError::batch_send(source)),
                                                ));
                                                continue;
                                            }
                                            match waiter.await {
                                                Ok(value) => {
                                                    chunk_results.push((addr, slot, Ok(value)));
                                                }
                                                Err(e) => {
                                                    chunk_results.push((
                                                        addr,
                                                        slot,
                                                        Err(StorageFetchError::provider(
                                                            "eth_getStorageAt",
                                                            e,
                                                        )),
                                                    ));
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            chunk_results.push((addr, slot, Err(err)));
                                        }
                                    }
                                }
                                chunk_results
                            }
                        })
                        .collect();

                    // Fire batches with bounded concurrency (`max_concurrent`) to avoid
                    // a thundering herd; per-batch size is the configured `batch_size`
                    // chosen above, so throughput scales without overwhelming RPC providers.
                    let all_batch_results: Vec<Vec<_>> = stream::iter(batch_futs)
                        .buffer_unordered(max_concurrent)
                        .collect()
                        .await;
                    for batch_results in all_batch_results {
                        results.extend(batch_results);
                    }
                    results
                })
            })
        },
    )
}

/// Build the same provider-backed storage fetcher selected by an
/// [`EvmCacheBuilder`] without constructing an [`EvmCache`].
///
/// Background cold-start workers use this to perform exact-hash provider work
/// independently of the cache-owner actor. The returned callback remains
/// protocol-neutral and accepts the block pin at invocation time.
pub fn provider_storage_fetcher<P>(
    provider: Arc<P>,
    batch_config: StorageBatchConfig,
    strategy: StorageFetchStrategy,
) -> StorageBatchFetchFn
where
    P: Provider<AnyNetwork> + 'static,
{
    let point_reads = point_read_storage_fetcher(provider.clone(), batch_config);
    match strategy {
        StorageFetchStrategy::BulkCall(config) => {
            crate::bulk_storage::bulk_call_storage_fetcher_with_fallback(
                provider,
                config,
                point_reads,
            )
        }
        StorageFetchStrategy::PointRead => point_reads,
    }
}

/// Build the cache's bounded provider-backed `eth_getProof` callback without
/// constructing an [`EvmCache`].
///
/// The callback accepts its [`BlockId`] per invocation, allowing a background
/// worker to pass an EIP-1898 exact canonical hash. `max_concurrent_proofs` is
/// normalized to at least one and preserves input ordering while overlapping
/// independent single-account proof requests.
pub fn account_proof_fetcher<P>(
    provider: Arc<P>,
    max_concurrent_proofs: usize,
) -> AccountProofFetchFn
where
    P: Provider<AnyNetwork> + 'static,
{
    let max_concurrent_proofs = max_concurrent_proofs.max(1);
    Arc::new(
        move |requests: Vec<(Address, Vec<U256>)>, current_block: BlockId| {
            let handle = match block_in_place_handle() {
                Ok(handle) => handle,
                Err(e) => {
                    return requests
                        .into_iter()
                        .map(|(addr, _keys)| {
                            (
                                addr,
                                Err(StorageFetchError::Runtime(RuntimeError::MissingRuntime {
                                    details: e.to_string(),
                                })),
                            )
                        })
                        .collect();
                }
            };
            let provider = provider.clone();
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    use futures::StreamExt;
                    futures::stream::iter(requests.into_iter().map(|(addr, keys)| {
                        let provider = provider.clone();
                        async move {
                            let proof_keys: Vec<B256> =
                                keys.iter().map(|slot| B256::from(*slot)).collect();
                            let outcome = provider
                                .get_proof(addr, proof_keys)
                                .block_id(current_block)
                                .await;
                            match outcome {
                                Ok(response) => {
                                    let slots = response
                                        .storage_proof
                                        .iter()
                                        .map(|proof| {
                                            (
                                                U256::from_be_bytes(proof.key.as_b256().0),
                                                proof.value,
                                            )
                                        })
                                        .collect();
                                    (
                                        addr,
                                        Ok(AccountProof {
                                            storage_hash: response.storage_hash,
                                            balance: response.balance,
                                            nonce: response.nonce,
                                            code_hash: response.code_hash,
                                            slots,
                                        }),
                                    )
                                }
                                Err(e) => {
                                    (addr, Err(StorageFetchError::provider("eth_getProof", e)))
                                }
                            }
                        }
                    }))
                    .buffered(max_concurrent_proofs)
                    .collect::<Vec<_>>()
                    .await
                })
            })
        },
    )
}

/// Outcome of [`EvmCache::prewarm_slots`].
#[derive(Debug, Default)]
pub struct PrewarmReport {
    /// Slots fetched and injected into the cache.
    pub loaded: usize,
    /// Pairs the fetcher failed to load, with the per-slot error.
    pub failed: Vec<(Address, U256, StorageFetchError)>,
}

/// Outcome of [`EvmCache::verify_code_seeds`]: how each `Pending` canonical
/// code claim resolved against the chain at the pinned block.
///
/// Fail-closed on trust, fail-safe on transport: `mismatched` /
/// `not_deployed` / `codeless` entries were **purged** (both cache layers and
/// the mark — the next touch refetches authoritative chain state), while
/// `unverifiable` entries are **still `Pending`** (a failed read proves
/// nothing, so the seed is neither promoted nor destroyed).
#[derive(Clone, Debug, Default)]
pub struct CodeVerifyReport {
    /// Claims confirmed: marked [`CodeSeedState::Verified`], real balance
    /// injected from the same response.
    pub verified: Vec<Address>,
    /// Claims contradicted by on-chain code — purged. Usually a wrong
    /// template or immutable-patch offset; the mismatching hashes are
    /// included for debugging.
    pub mismatched: Vec<CodeMismatch>,
    /// `EXTCODEHASH == 0`: no account at the pinned block — purged. This is
    /// the live-registration race (the deployment is newer than the pin);
    /// re-pin forward and re-seed rather than debugging the template.
    pub not_deployed: Vec<Address>,
    /// `EXTCODEHASH == keccak256("")`: the address exists but holds no code
    /// (an EOA) — purged.
    pub codeless: Vec<Address>,
    /// The fetch failed (transport error, omitted address, or the
    /// [`MULTICALL3_ADDRESS`](crate::multicall::MULTICALL3_ADDRESS) host
    /// caveat) — each still `Pending`, with the reason.
    pub unverifiable: Vec<(Address, String)>,
}

/// One contradicted code claim from [`EvmCache::verify_code_seeds`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodeMismatch {
    /// The seeded address.
    pub address: Address,
    /// The hash the seed claimed (keccak256 of the seeded bytes).
    pub expected: B256,
    /// The on-chain `EXTCODEHASH` observed at the pinned block.
    pub actual: B256,
}

/// Behavior when overriding code at a target account that is not known to the cache/backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingTargetBehavior {
    /// Return an error if the target account cannot be loaded.
    Error,
    /// Create a default account with the replacement code.
    Create,
}

/// Per-call transaction-environment overrides for a simulation.
///
/// `Default` reproduces the read-only behavior of the plain `call_raw`
/// (zero value, default gas/nonce). Use the `*_with` call variants to supply
/// these — e.g. to simulate a payable function, a native-ETH transfer, or a
/// gas-bounded call. Balance affordability checks are disabled in the
/// simulator, so a non-zero `value` does not require the caller to be funded.
#[derive(Debug, Clone, Default)]
pub struct TxConfig {
    /// Native value (wei) sent with the call. Set this to simulate a payable
    /// function or a native-ETH transfer. Balance checks are disabled in the
    /// simulator, so the caller need not be funded for a non-zero value.
    pub value: U256,
    /// Gas limit for the call. `None` uses revm's default. Set this to model a
    /// gas-bounded call (e.g. to observe out-of-gas behavior).
    pub gas_limit: Option<u64>,
    /// Gas price (wei) for the call. `None` uses revm's default. Rarely needed
    /// because base-fee checks are disabled in the simulator.
    pub gas_price: Option<u128>,
    /// Sender nonce. `None` lets the simulator pick; nonce checks are disabled,
    /// so this is only worth setting when a contract reads the nonce explicitly.
    pub nonce: Option<u64>,
    /// EIP-2930 access list to pre-warm accounts and storage slots for this
    /// call. Pre-warming changes EIP-2929 gas accounting; supply it when
    /// reproducing the gas cost of a transaction that carried an access list.
    pub access_list: Option<AccessList>,
}

/// Which block-context header fields a cache requires to be present.
///
/// Block-env fields (`NUMBER` / `BASEFEE` / `COINBASE` / `PREVRANDAO` /
/// `GASLIMIT`) are populated from a fetched block header. When a field is
/// absent — because a fetch failed or the chain does not carry it — the EVM
/// silently defaults it, which can steer contracts that branch on block context
/// down a different code path and produce quietly-wrong simulations.
///
/// These per-field requirements let a caller opt into failing loudly instead.
/// [`strict()`](Self::strict) requires every field; [`lenient()`](Self::lenient)
/// (the [`Default`]) requires none and reproduces the historical
/// silently-default behavior. A chain without EIP-1559, for example, can start
/// from [`strict()`](Self::strict) and clear [`require_basefee`](Self::require_basefee).
///
/// Requirements are checked by [`validate_header`](Self::validate_header), which
/// [`EvmCache::advance_block`] and [`EvmCacheBuilder::try_build`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockContextRequirements {
    /// Require the header to carry a block number (`NUMBER`).
    pub require_number: bool,
    /// Require the header to carry an EIP-1559 base fee (`BASEFEE`).
    pub require_basefee: bool,
    /// Require the header to carry a beneficiary (`COINBASE`).
    pub require_coinbase: bool,
    /// Require the header to carry a `prevrandao` / mix hash (`PREVRANDAO`).
    pub require_prevrandao: bool,
    /// Require the header to carry a gas limit (`GASLIMIT`).
    pub require_gas_limit: bool,
}

impl Default for BlockContextRequirements {
    fn default() -> Self {
        Self::lenient()
    }
}

impl BlockContextRequirements {
    /// Require every block-context field to be present.
    ///
    /// Under this policy a header missing any required field is rejected rather
    /// than silently defaulted.
    pub const fn strict() -> Self {
        Self {
            require_number: true,
            require_basefee: true,
            require_coinbase: true,
            require_prevrandao: true,
            require_gas_limit: true,
        }
    }

    /// Require no block-context field (the [`Default`]).
    ///
    /// Reproduces the historical behavior: a missing field is silently
    /// defaulted by the EVM.
    pub const fn lenient() -> Self {
        Self {
            require_number: false,
            require_basefee: false,
            require_coinbase: false,
            require_prevrandao: false,
            require_gas_limit: false,
        }
    }

    /// Validate that a header carries every required block-context field.
    ///
    /// Only the two `Option`-typed header fields can actually be absent:
    /// [`require_basefee`](Self::require_basefee) checks
    /// [`base_fee_per_gas`](alloy_consensus::BlockHeader::base_fee_per_gas) and
    /// [`require_prevrandao`](Self::require_prevrandao) checks
    /// [`mix_hash`](alloy_consensus::BlockHeader::mix_hash). Number, beneficiary
    /// and gas limit are non-`Option` on the [`BlockHeader`] trait, so those
    /// requirement flags are satisfied whenever a header is present. Returns
    /// `Ok(())` when all required fields are satisfied.
    pub fn validate_header<H: BlockHeader>(&self, header: &H) -> Result<(), BlockContextError> {
        // `number`, `coinbase` (beneficiary) and `gas_limit` are non-`Option` on
        // the `BlockHeader` trait: they are always present when a header exists,
        // so their requirement flags are trivially satisfied here.
        if self.require_basefee && header.base_fee_per_gas().is_none() {
            return Err(BlockContextError::MissingField { field: "basefee" });
        }
        if self.require_prevrandao && header.mix_hash().is_none() {
            return Err(BlockContextError::MissingField {
                field: "prevrandao",
            });
        }
        Ok(())
    }
}

/// Fluent builder for [`EvmCache`].
///
/// A readable alternative to the positional [`EvmCache::with_cache`]
/// constructor. Defaults: latest block, no disk cache, [`SpecId::CANCUN`].
///
/// ```no_run
/// # use std::sync::Arc;
/// # use alloy_provider::{ProviderBuilder, network::AnyNetwork};
/// # use revm::primitives::hardfork::SpecId;
/// # use evm_fork_cache::cache::EvmCache;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = ProviderBuilder::new()
///     .network::<AnyNetwork>()
///     .connect_http("https://example-rpc.invalid".parse()?);
/// let cache = EvmCache::builder(Arc::new(provider))
///     .latest_block()
///     .spec(SpecId::CANCUN)
///     .build()
///     .await;
/// # let _ = cache;
/// # Ok(())
/// # }
/// ```
pub struct EvmCacheBuilder<P> {
    provider: Arc<P>,
    block: BlockId,
    cache_config: Option<CacheConfig>,
    spec_id: SpecId,
    shared_memory_capacity: SharedMemoryCapacity,
    storage_batch_config: StorageBatchConfig,
    storage_fetch_strategy: StorageFetchStrategy,
    chain_id: Option<u64>,
    block_context_requirements: BlockContextRequirements,
    max_concurrent_proofs: usize,
}

impl<P> EvmCacheBuilder<P>
where
    P: Provider<AnyNetwork> + 'static,
{
    /// Start a builder over the given provider.
    pub fn new(provider: Arc<P>) -> Self {
        Self {
            provider,
            block: BlockId::latest(),
            cache_config: None,
            spec_id: SpecId::CANCUN,
            shared_memory_capacity: SharedMemoryCapacity::default(),
            storage_batch_config: StorageBatchConfig::default(),
            storage_fetch_strategy: StorageFetchStrategy::default(),
            chain_id: None,
            block_context_requirements: BlockContextRequirements::lenient(),
            max_concurrent_proofs: DEFAULT_MAX_CONCURRENT_PROOFS,
        }
    }

    /// Cap the default account-proof fetcher's concurrent `eth_getProof`
    /// fan-out (default 8, name-symmetric with
    /// [`BulkCallConfig::max_concurrent_calls`](crate::bulk_storage::BulkCallConfig)).
    ///
    /// `eth_getProof` is single-address at the RPC level, so when the root
    /// gate or an account resync probes N tracked accounts in one seam call,
    /// concurrency is the only wall-clock lever: `N × RTT` serial becomes
    /// `~ceil(N / cap) × RTT`. Values are clamped to at least 1. Custom
    /// fetchers installed via
    /// [`set_account_proof_fetcher`](EvmCache::set_account_proof_fetcher)
    /// ignore this knob.
    pub fn max_concurrent_proofs(mut self, cap: usize) -> Self {
        self.max_concurrent_proofs = cap.max(1);
        self
    }

    /// Pin simulations and RPC fetches to a specific block.
    ///
    /// Use this to fork at a fixed height for reproducible simulation. Without
    /// a call to [`block`](Self::block) or [`latest_block`](Self::latest_block)
    /// the builder defaults to the latest block at [`build`](Self::build) time.
    pub fn block(mut self, block: BlockId) -> Self {
        self.block = block;
        self
    }

    /// Pin to the latest block.
    ///
    /// The height is resolved when [`build`](Self::build) fetches the block
    /// header, so the cache forks at whatever was latest at construction. Use
    /// [`block`](Self::block) instead to pin a fixed, reproducible height.
    pub fn latest_block(mut self) -> Self {
        self.block = BlockId::latest();
        self
    }

    /// Set the EVM hardfork spec (must match the chain's execution layer).
    pub fn spec(mut self, spec_id: SpecId) -> Self {
        self.spec_id = spec_id;
        self
    }

    /// Set the chain ID reported to simulations via the `CHAINID` opcode.
    ///
    /// **Recommended.** This is the explicit, authoritative way to set the chain
    /// ID. If left unset, [`build`](Self::build) infers it from the provider
    /// (`eth_chainId`), falling back to `1` (Ethereum mainnet) only if that query
    /// fails. A disk [`cache_config`](Self::cache_config) also carries a
    /// `chain_id` (which additionally namespaces the on-disk cache directory);
    /// when both are set, the value passed here wins for the `CHAINID` opcode, so
    /// keep them consistent.
    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    /// Enable disk-backed caching with the given configuration.
    ///
    /// Supplying a [`CacheConfig`] turns on persistence of EVM state, bytecodes,
    /// and immutable data under the configured chain directory; the cache is
    /// loaded on [`build`](Self::build) and flushed on drop. Omit it for a
    /// purely in-memory cache backed solely by RPC.
    pub fn cache_config(mut self, cache_config: CacheConfig) -> Self {
        self.cache_config = Some(cache_config);
        self
    }

    /// Set how much EVM shared memory to pre-allocate per simulation context.
    ///
    /// Defaults to [`SharedMemoryCapacity::Fixed`] with `64 * 1024` bytes
    /// (65,536 bytes).
    /// Use `Fixed(n)` to pin a size, or [`SharedMemoryCapacity::Auto`] to size it
    /// from the chain state loaded at [`build`](Self::build) time (e.g. a bincode
    /// state file supplied via [`cache_config`](Self::cache_config)). See
    /// [`SharedMemoryCapacity`] for the trade-offs.
    pub fn shared_memory_capacity(mut self, capacity: SharedMemoryCapacity) -> Self {
        self.shared_memory_capacity = capacity;
        self
    }

    /// Set the concrete storage batch-fetch configuration for this cache instance.
    ///
    /// The config controls the batch size and concurrency used by the
    /// provider-backed [`StorageBatchFetchFn`]. Defaults to
    /// [`StorageBatchConfig::default`] (the [`CacheSpeedMode::Slow`] preset).
    /// Different cache instances can use different values in the same process.
    /// Zero values are normalized to one.
    pub fn storage_batch_config(mut self, config: impl Into<StorageBatchConfig>) -> Self {
        self.storage_batch_config = config.into().normalized();
        self
    }

    /// Set the storage batch-fetch profile from a preset.
    ///
    /// Shorthand for [`storage_batch_config`](Self::storage_batch_config) with
    /// `mode.into()`.
    pub fn speed_mode(self, mode: CacheSpeedMode) -> Self {
        self.storage_batch_config(mode)
    }

    /// Choose how the cache's batch storage fetcher loads slots.
    ///
    /// Defaults to [`StorageFetchStrategy::BulkCall`] with
    /// [`BulkCallConfig::default`](crate::bulk_storage::BulkCallConfig::default):
    /// bulk `eth_call` state-override extraction, repaired by (and degrading
    /// to) the point-read fetcher that [`storage_batch_config`](Self::storage_batch_config)
    /// tunes. Use [`StorageFetchStrategy::PointRead`] to restore the classic
    /// per-slot behavior.
    pub fn storage_fetch_strategy(mut self, strategy: StorageFetchStrategy) -> Self {
        self.storage_fetch_strategy = strategy;
        self
    }

    /// Tune the bulk `eth_call` extraction path.
    ///
    /// Shorthand for [`storage_fetch_strategy`](Self::storage_fetch_strategy)
    /// with [`StorageFetchStrategy::BulkCall`]`(config)` — e.g. raising
    /// `max_slots_per_call` on a provider with a generous gas cap, or
    /// selecting [`CallDispatch::CallMany`](crate::bulk_storage::CallDispatch::CallMany)
    /// on Erigon-lineage endpoints.
    pub fn bulk_call_config(self, config: crate::bulk_storage::BulkCallConfig) -> Self {
        self.storage_fetch_strategy(StorageFetchStrategy::BulkCall(config))
    }

    /// Set which block-context header fields the cache requires.
    ///
    /// See [`BlockContextRequirements`]. Defaults to
    /// [`lenient`](BlockContextRequirements::lenient). Only [`try_build`](Self::try_build)
    /// enforces non-lenient requirements at construction; the infallible
    /// [`build`](Self::build) always stays lenient.
    pub fn block_context_requirements(mut self, reqs: BlockContextRequirements) -> Self {
        self.block_context_requirements = reqs;
        self
    }

    /// Convenience toggle: require every block-context field (`true`) or none
    /// (`false`).
    ///
    /// Equivalent to
    /// [`block_context_requirements`](Self::block_context_requirements) with
    /// [`strict`](BlockContextRequirements::strict) /
    /// [`lenient`](BlockContextRequirements::lenient). Enforced only by
    /// [`try_build`](Self::try_build).
    pub fn strict_block_context(mut self, strict: bool) -> Self {
        self.block_context_requirements = if strict {
            BlockContextRequirements::strict()
        } else {
            BlockContextRequirements::lenient()
        };
        self
    }

    /// Build the [`EvmCache`], fetching the pinned block's header for context.
    ///
    /// If a chain ID was not set via [`chain_id`](Self::chain_id), it is inferred
    /// from the provider (`eth_chainId`); see [`chain_id`](Self::chain_id) for the
    /// full resolution order.
    ///
    /// This constructor is infallible and always uses
    /// [`lenient`](BlockContextRequirements::lenient) enforcement (a missing
    /// block-context field is silently defaulted). To enforce
    /// [`BlockContextRequirements`] at construction, use
    /// [`try_build`](Self::try_build) instead.
    pub async fn build(self) -> EvmCache {
        let explicit_chain_id = self.chain_id;
        let provider = self.provider.clone();
        let strategy = self.storage_fetch_strategy;
        let storage_batch_config = self.storage_batch_config;
        let mut cache = EvmCache::with_cache_capacity_and_storage_batch_config(
            self.provider,
            self.block,
            self.cache_config,
            self.spec_id,
            self.shared_memory_capacity,
            self.storage_batch_config,
            self.max_concurrent_proofs,
        )
        .await;
        // An explicit builder value is authoritative for the `CHAINID` opcode and
        // overrides both the inferred value and any `cache_config` chain id.
        if let Some(chain_id) = explicit_chain_id {
            cache.set_chain_id(chain_id);
        }
        apply_storage_fetch_strategy(&mut cache, provider, strategy, storage_batch_config);
        cache
    }

    /// Build the [`EvmCache`], enforcing the configured
    /// [`BlockContextRequirements`] against the fetched block header.
    ///
    /// Builds the cache the same way [`build`](Self::build) does, then, if the
    /// requirements are non-lenient, validates the pinned block's header:
    /// - if the header could not be fetched (the provider errored or returned no
    ///   block), returns [`BlockContextError::FetchFailed`];
    /// - otherwise validates the fetched header via
    ///   [`BlockContextRequirements::validate_header`] and propagates any
    ///   [`BlockContextError::MissingField`].
    ///
    /// A [`lenient`](BlockContextRequirements::lenient) build never errors (it
    /// does not fetch a header solely to validate). On success the requirements
    /// are stored on the returned cache so a later
    /// [`advance_block`](EvmCache::advance_block) enforces them too.
    pub async fn try_build(self) -> Result<EvmCache, BlockContextError> {
        let explicit_chain_id = self.chain_id;
        let reqs = self.block_context_requirements;
        let block = self.block;
        let provider = self.provider.clone();
        let strategy = self.storage_fetch_strategy;
        let storage_batch_config = self.storage_batch_config;

        let mut cache = EvmCache::with_cache_capacity_and_storage_batch_config(
            self.provider,
            self.block,
            self.cache_config,
            self.spec_id,
            self.shared_memory_capacity,
            self.storage_batch_config,
            self.max_concurrent_proofs,
        )
        .await;
        if let Some(chain_id) = explicit_chain_id {
            cache.set_chain_id(chain_id);
        }
        cache.set_block_context_requirements(reqs);
        apply_storage_fetch_strategy(&mut cache, provider.clone(), strategy, storage_batch_config);

        // Only a non-lenient policy fetches a header to validate: a lenient
        // build must never error and must not incur an extra RPC round-trip.
        if reqs != BlockContextRequirements::lenient() {
            match provider.get_block(block).await {
                Ok(Some(blk)) => reqs.validate_header(blk.header())?,
                Ok(None) => {
                    return Err(BlockContextError::FetchFailed(format!(
                        "no block header returned for {block:?}"
                    )));
                }
                Err(e) => return Err(BlockContextError::FetchFailed(e.to_string())),
            }
        }

        Ok(cache)
    }
}

/// Install the fetcher a [`StorageFetchStrategy`] describes on a built cache.
///
/// The constructor already installs the default strategy (bulk extraction
/// wrapping the point-read fetcher), so the default case is a no-op rather
/// than a redundant re-wrap.
fn apply_storage_fetch_strategy<P>(
    cache: &mut EvmCache,
    provider: Arc<P>,
    strategy: StorageFetchStrategy,
    batch_config: StorageBatchConfig,
) where
    P: Provider<AnyNetwork> + 'static,
{
    match strategy {
        StorageFetchStrategy::BulkCall(config)
            if config == crate::bulk_storage::BulkCallConfig::default() => {}
        strategy => cache.set_storage_batch_fetcher(provider_storage_fetcher(
            provider,
            batch_config,
            strategy,
        )),
    }
}

type CacheEvm<'a> = revm::MainnetEvm<
    Context<BlockEnv, TxEnv, CfgEnv, &'a mut ForkCacheDB, Journal<&'a mut ForkCacheDB>, ()>,
>;
type InspectorCacheEvm<'a, INSP> = revm::MainnetEvm<
    Context<BlockEnv, TxEnv, CfgEnv, &'a mut ForkCacheDB, Journal<&'a mut ForkCacheDB>, ()>,
    INSP,
>;

/// Default initial capacity for the EVM shared-memory (working-memory) buffer.
/// 64 KiB (65,536 bytes), chosen from profiling a state-heavy workload (16x the
/// revm default of 4 KiB) so simulations rarely reallocate. Exposed for tuning via
/// [`SharedMemoryCapacity`].
const DEFAULT_SHARED_MEMORY_CAPACITY: usize = 64 * 1024;

/// Default cap on the default account-proof fetcher's concurrent
/// `eth_getProof` fan-out (see [`EvmCacheBuilder::max_concurrent_proofs`]).
const DEFAULT_MAX_CONCURRENT_PROOFS: usize = 8;

/// How much EVM shared memory (per-context working memory) to pre-allocate for
/// simulations.
///
/// revm grows its shared memory on demand during execution; pre-allocating just
/// avoids repeated reallocations when simulations touch a lot of memory — the
/// original motivation was a state-heavy workload where resizing was hot. The
/// trade-off cuts both ways: a wide parallel fan-out of *small* simulations pays
/// this much memory per overlay, so general users may want a smaller `Fixed` size,
/// while state-heavy users can raise it or let it auto-size from the loaded state.
///
/// The default is `Fixed(64 * 1024)` (65,536 bytes). Configure it on
/// [`EvmCacheBuilder::shared_memory_capacity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedMemoryCapacity {
    /// Pre-allocate exactly this many bytes. The [`Default`] is
    /// `Fixed(64 * 1024)`.
    Fixed(usize),
    /// Size the buffer from the amount of chain state loaded into the cache at
    /// construction (e.g. from a bincode state file via
    /// [`CacheConfig`]/[`EvmCacheBuilder::cache_config`]), clamped to a sane
    /// floor/ceiling. Falls back to the floor when nothing is loaded.
    ///
    /// This is a heuristic proxy — persisted state size loosely correlates with the
    /// working-set size of simulations over it, not an exact peak-memory model. Use
    /// `Fixed` when you have profiled your workload.
    Auto,
}

impl Default for SharedMemoryCapacity {
    fn default() -> Self {
        Self::Fixed(DEFAULT_SHARED_MEMORY_CAPACITY)
    }
}

impl SharedMemoryCapacity {
    /// Floor for [`Auto`](Self::Auto) (and the default fixed size): 64 KiB
    /// (65,536 bytes).
    pub const MIN_AUTO: usize = DEFAULT_SHARED_MEMORY_CAPACITY;
    /// Ceiling for [`Auto`](Self::Auto): 4 MiB. A simulation that needs more than
    /// this still works — revm grows the buffer past it on demand.
    pub const MAX_AUTO: usize = 4 * 1024 * 1024;
    /// Heuristic proxy: bytes of pre-allocated working memory per loaded storage
    /// slot. Tune if profiling warrants.
    const AUTO_BYTES_PER_SLOT: usize = 16;

    /// Resolve to a concrete byte capacity. `loaded_slots` is the number of layer-2
    /// storage slots present in the cache at construction (0 when nothing is
    /// loaded); it is consulted only for [`Auto`](Self::Auto).
    pub(crate) fn resolve(self, loaded_slots: usize) -> usize {
        match self {
            Self::Fixed(bytes) => bytes,
            Self::Auto => loaded_slots
                .saturating_mul(Self::AUTO_BYTES_PER_SLOT)
                .clamp(Self::MIN_AUTO, Self::MAX_AUTO),
        }
    }
}

/// EVM cache with lazy-loading RPC backend.
///
/// Uses `foundry-fork-db` for intelligent caching and request deduplication.
/// Storage and account data is fetched on-demand when accessed during EVM execution,
/// eliminating the need for expensive access list prefetching.
pub struct EvmCache {
    backend: SharedBackend,
    blockchain_db: BlockchainDb,
    db: ForkCacheDB,
    token_decimals: HashMap<Address, u8>,
    block: BlockId,
    cache_config: Option<CacheConfig>,
    /// Cache for immutable on-chain data (token decimals).
    immutable_cache: ImmutableDataCache,
    /// Optional timestamp override for simulating future blocks.
    /// When set, EVM simulations use this timestamp instead of the current system time.
    timestamp_override: Option<u64>,
    /// Chain ID for EVM simulation (e.g. 42161 for Arbitrum, 1 for Ethereum).
    chain_id: u64,
    /// Block number for EVM simulations (NUMBER opcode).
    /// Fetched from block header during construction. Without this, revm defaults to 0
    /// which causes contracts that read block.number to execute different code paths.
    block_number: Option<u64>,
    /// Base fee per gas for EVM simulations (BASEFEE opcode).
    /// Fetched from block header during construction.
    basefee: Option<u64>,
    /// Block beneficiary for EVM simulations (COINBASE opcode).
    /// Fetched from the block header; commonly read by MEV/builder tip logic.
    coinbase: Option<Address>,
    /// `prevrandao` for EVM simulations (PREVRANDAO opcode), i.e. the header's
    /// mix hash post-merge. Drives on-chain randomness.
    prevrandao: Option<B256>,
    /// Block gas limit for EVM simulations (GASLIMIT opcode).
    block_gas_limit: Option<u64>,
    /// Which block-context header fields this cache requires to be present.
    /// [`lenient`](BlockContextRequirements::lenient) by default; the strict
    /// builder path sets it before returning. Enforced by
    /// [`advance_block`](Self::advance_block).
    block_context_requirements: BlockContextRequirements,
    /// Cache-side batch-fetch configuration for this instance.
    storage_batch_config: StorageBatchConfig,
    /// Shared memory buffer reused across EVM simulations.
    /// This avoids repeated allocations and allows measuring peak memory usage.
    shared_memory_buffer: Rc<RefCell<Vec<u8>>>,
    /// Optional callback for direct RPC `eth_call` (bypasses revm simulation).
    /// Set during construction from the provider. Useful for batch operations
    /// where revm's lazy storage fetching would be too slow.
    rpc_caller: Option<RpcCallFn>,
    /// Optional batch storage fetcher that bypasses SharedBackend.
    /// Captures a provider clone and fires concurrent `eth_getStorageAt` calls directly.
    /// Monotonic snapshot-consistency generation (see
    /// [`snapshot_generation`](Self::snapshot_generation)). Bumped by targeted
    /// state writes (`apply_update` / `apply_updates` / `modify_slot`) and
    /// block re-pins (`set_block` / `advance_block`); cold prefetch
    /// (`inject_storage_batch`) does not bump it.
    snapshot_generation: u64,
    storage_batch_fetcher: Option<StorageBatchFetchFn>,
    /// Optional account/root fetcher that bypasses SharedBackend.
    /// Captures a provider clone and fires `eth_getProof` calls directly to fetch
    /// authoritative account fields (balance/nonce/code hash) and `storageHash`.
    account_proof_fetcher: Option<AccountProofFetchFn>,
    /// Optional block state-diff fetcher backed by debug/trace RPC.
    block_state_diff_fetcher: Option<BlockStateDiffFetchFn>,
    /// Optional bulk account-fields fetcher (balance + `EXTCODEHASH` in one
    /// `eth_call`), the read side of code-seed verification.
    account_fields_fetcher: Option<AccountFieldsFetchFn>,
    /// Provenance + trust marks for bytecode that did not arrive via the lazy
    /// RPC backend (see [`CodeSeedState`]). Absence of a mark = RPC-origin.
    /// Persisted to `code_seeds.bin` (saved before `bytecodes.bin`, full
    /// replace) so a `Pending` claim never masquerades as chain-fetched
    /// across restarts.
    code_seeds: HashMap<Address, CodeSeedState>,
    /// Best-known ERC20 `balanceOf` mapping descriptor per token contract,
    /// carrying both the base slot and the detected [`SlotLayout`] so writes
    /// honor Vyper/Solady byte order — not just Solidity's `keccak(key‖slot)`.
    ///
    /// Populated by discovery (or seeding) and used by
    /// `set_erc20_balance_with_slot_scan` to avoid re-discovering per token.
    erc20_balance_slots: HashMap<Address, TrackedMapping>,
    /// EVM hardfork spec for simulations. Must match the chain's current execution
    /// layer hardfork for accurate gas accounting. Configured per-chain via `evm_spec`
    /// in `chains.toml`.
    spec_id: SpecId,
    /// Memoized, `Arc`-shared flatten of the cold layer-2 index, reused across
    /// successive [`snapshot`](Self::snapshot) calls (Pillar A).
    /// `None` until the first snapshot. Rebuilt copy-on-write by
    /// [`refresh_base`](Self::refresh_base); never mutated in place once shared.
    /// Not part of any public API and not serialized.
    base: Option<Arc<snapshot::BaseState>>,
    /// Layer-2 addresses changed since `base` was built, folded into the next base
    /// rebuild. Populated by the base-invalidation sites (write-through, batch
    /// injects, layer-2 seeding, purges). Not serialized.
    base_dirty: HashSet<Address>,
    /// When set, the next [`refresh_base`](Self::refresh_base) rebuilds the base
    /// from scratch. Set by [`set_block`](Self::set_block) /
    /// [`repin_to_block`](Self::repin_to_block), which replace layer 2 wholesale.
    /// Not serialized.
    base_full_rebuild: bool,
    /// Per-account layer-2 slot count at the last base build, used by
    /// [`refresh_base`](Self::refresh_base)'s `O(accounts)` length-scan to detect
    /// uncontrolled lazy-fetch growth that bypasses the write funnel. Not
    /// serialized.
    base_storage_lens: HashMap<Address, usize>,
    /// Resolved per-context EVM shared-memory pre-allocation (bytes), from the
    /// [`SharedMemoryCapacity`] at construction (resolving `Auto` against the loaded
    /// state). Propagated to each [`EvmSnapshot`] so snapshot-backed overlays
    /// pre-allocate the same amount. See
    /// [`shared_memory_capacity`](Self::shared_memory_capacity).
    shared_memory_capacity: usize,
}

/// Outcome of a balance-delta-tracking simulation.
///
/// Produced by [`EvmCache::simulate_call_with_balance_deltas`] and
/// [`EvmCache::simulate_with_transfer_tracking`]: a successful call together
/// with the per-token balance changes it caused, its emitted logs, the touched
/// access list, and its raw return data.
/// Execution outcome of a simulated call.
///
/// Lets a caller distinguish a successful call — even one that emitted no logs,
/// such as a view call — from a revert or a halt, without guessing from `logs`
/// or `output`. Revert payloads live in [`CallSimulationResult::output`] and can
/// be decoded with [`RevertDecoder`](crate::errors::RevertDecoder); only `Halt`
/// carries extra data here, since its reason has nowhere else to live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimStatus {
    /// The call returned successfully.
    Success,
    /// The call reverted; the revert payload (if any) is in `output`.
    Revert,
    /// The call halted (e.g. out of gas, invalid opcode).
    Halt {
        /// Debug-formatted halt reason.
        reason: String,
    },
}

/// Outcome of a simulated call: status, return data, gas used, and the touched
/// access list. `#[non_exhaustive]` — construct via the simulation APIs and match
/// with a wildcard arm.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CallSimulationResult {
    /// Whether the call succeeded, reverted, or halted.
    pub status: SimStatus,
    /// Gas consumed by the (successful) call.
    pub gas_used: u64,
    /// Net change in `owner`'s balance per tracked token, as a **signed**
    /// [`I256`] (`post - pre`): positive means the call increased the balance,
    /// negative means it decreased it. Tokens not seen by the call may be
    /// absent or zero.
    pub token_deltas: HashMap<Address, I256>,
    /// Logs emitted by the call (in emission order).
    pub logs: Vec<Log>,
    /// EIP-2930 access list of all accounts and storage slots touched during simulation.
    /// Extracted from the EVM journaled state after execution.
    pub access_list: AccessList,
    /// Raw return data of the call.
    ///
    /// `Success` carries the returned bytes, `Revert` the revert payload, and
    /// `Halt` an empty slice. This makes a corrected view-call result observable:
    /// when a re-run reads a changed slot, the new return value differs here even
    /// if both runs succeed.
    pub output: Bytes,
}

sol!(
    #[sol(rpc)]
    contract IERC20 {
        function balanceOf(address target) returns (uint256);
        function decimals() returns (uint8);
        function allowance(address owner, address spender) returns (uint256);
    }
);

/// Parse an EVM hardfork spec name (e.g. from TOML config) into a revm [`SpecId`].
///
/// Accepts revm's canonical names (e.g. `"Cancun"`, `"Shanghai"`, `"Prague"`)
/// case-insensitively. Falls back to [`SpecId::CANCUN`] for unrecognized values.
pub fn parse_evm_spec(spec: &str) -> SpecId {
    // SpecId::from_str expects title-case (e.g. "Cancun"), so normalize the input.
    let mut chars = spec.chars();
    let title_case: String = match chars.next() {
        Some(c) => c.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
        None => String::new(),
    };
    title_case.parse::<SpecId>().unwrap_or_else(|_| {
        warn!(spec, "Unknown EVM spec, defaulting to Cancun");
        SpecId::CANCUN
    })
}

impl EvmCache {
    /// Select how synchronous provider-backed cache misses block the caller.
    ///
    /// `true` blocks the current thread directly, which is required when the
    /// cache is temporarily used from a Tokio `LocalSet` where
    /// `tokio::task::block_in_place` is unavailable. `false` restores the
    /// normal multi-thread-runtime behavior. Callers should keep direct
    /// blocking scopes short and restore the default before handing the cache
    /// to a long-lived actor.
    pub fn set_blocking_provider_reads(&mut self, block_current_thread: bool) {
        let mode = if block_current_thread {
            BlockingMode::Block
        } else {
            BlockingMode::BlockInPlace
        };
        let backend = self.backend.with_blocking_mode(mode);
        self.backend = backend.clone();
        self.db.db = backend;
    }

    /// Start a fluent [`EvmCacheBuilder`] over the given provider.
    ///
    /// Preferred over the positional [`with_cache`](Self::with_cache) /
    /// [`new`](Self::new) constructors for readability.
    pub fn builder<P>(provider: Arc<P>) -> EvmCacheBuilder<P>
    where
        P: Provider<AnyNetwork> + 'static,
    {
        EvmCacheBuilder::new(provider)
    }

    /// Create a new EvmCache with a SharedBackend that lazily fetches from RPC.
    ///
    /// The backend spawns a background handler task that manages RPC requests
    /// and deduplicates concurrent requests for the same data.
    ///
    /// # Runtime requirement
    /// RPC-backed operation requires a **multi-thread** tokio runtime
    /// (`#[tokio::main(flavor = "multi_thread")]` or
    /// `tokio::runtime::Builder::new_multi_thread()`). The direct RPC callbacks
    /// (`eth_call` and batch `eth_getStorageAt`) drive async work synchronously
    /// via `tokio::task::block_in_place`, which is unsupported on a
    /// current-thread runtime. On a current-thread runtime those callbacks
    /// degrade to typed errors rather than panicking.
    pub async fn new<P>(provider: Arc<P>) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        Self::at_block(provider, BlockId::latest()).await
    }

    /// Create a new EvmCache pinned to an explicit block.
    ///
    /// Prefer this over [`new`](Self::new) when reproducibility matters and the
    /// caller has already chosen the fork block.
    pub async fn at_block<P>(provider: Arc<P>, block: BlockId) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        Self::with_cache(provider, block, None, SpecId::CANCUN).await
    }

    /// Create a new EvmCache with disk-based caching.
    ///
    /// This enables several caching features:
    /// 1. Unified EVM state: Accounts + storage loaded from `evm_state.bin` (bincode)
    /// 2. Bytecode caching: Contract bytecodes from `bytecodes.bin`
    /// 3. Immutable data: Token decimals
    ///
    /// # Runtime requirement
    /// RPC-backed operation requires a **multi-thread** tokio runtime
    /// (`#[tokio::main(flavor = "multi_thread")]` or
    /// `tokio::runtime::Builder::new_multi_thread()`). The direct RPC callbacks
    /// (`eth_call` and batch `eth_getStorageAt`) drive async work synchronously
    /// via `tokio::task::block_in_place`, which is unsupported on a
    /// current-thread runtime. On a current-thread runtime those callbacks
    /// degrade to typed errors rather than panicking.
    pub async fn with_cache<P>(
        provider: Arc<P>,
        block: BlockId,
        cache_config: Option<CacheConfig>,
        spec_id: SpecId,
    ) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        Self::with_cache_capacity(
            provider,
            block,
            cache_config,
            spec_id,
            SharedMemoryCapacity::default(),
        )
        .await
    }

    /// Like [`with_cache`](Self::with_cache) but takes an explicit
    /// [`SharedMemoryCapacity`] controlling per-context EVM working-memory
    /// pre-allocation. This is what [`EvmCacheBuilder::build`] calls; prefer the
    /// builder. With [`SharedMemoryCapacity::Auto`] the buffer is sized from the
    /// layer-2 storage loaded at construction (e.g. a bincode state file).
    pub async fn with_cache_capacity<P>(
        provider: Arc<P>,
        block: BlockId,
        cache_config: Option<CacheConfig>,
        spec_id: SpecId,
        shared_memory_capacity: SharedMemoryCapacity,
    ) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        Self::with_cache_capacity_and_storage_batch_config(
            provider,
            block,
            cache_config,
            spec_id,
            shared_memory_capacity,
            StorageBatchConfig::default(),
            DEFAULT_MAX_CONCURRENT_PROOFS,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn with_cache_capacity_and_storage_batch_config<P>(
        provider: Arc<P>,
        block: BlockId,
        cache_config: Option<CacheConfig>,
        spec_id: SpecId,
        shared_memory_capacity: SharedMemoryCapacity,
        storage_batch_config: StorageBatchConfig,
        max_concurrent_proofs: usize,
    ) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        let block_id = block;
        let storage_batch_config = storage_batch_config.normalized();
        let max_concurrent_proofs = max_concurrent_proofs.max(1);

        // Fetch the pinned block header for accurate block context (NUMBER,
        // BASEFEE, COINBASE, PREVRANDAO, GASLIMIT, TIMESTAMP opcodes). Without
        // this, revm defaults to 0/default values, causing contracts that read
        // block context to execute different code paths. Use the concrete
        // BlockId the cache is pinned to so hash pins do not accidentally
        // inherit latest header context.
        let (block_number, basefee, coinbase, prevrandao, block_gas_limit, timestamp) =
            match provider.get_block(block_id).await {
                Ok(Some(blk)) => {
                    let h = blk.header();
                    (
                        Some(h.number()),
                        h.base_fee_per_gas(),
                        Some(h.beneficiary()),
                        h.mix_hash(),
                        Some(h.gas_limit()),
                        Some(h.timestamp()),
                    )
                }
                Ok(None) => {
                    debug!("Block header not found for block context initialization");
                    (None, None, None, None, None, None)
                }
                Err(e) => {
                    debug!(error = %e, "Failed to fetch block header for block context");
                    (None, None, None, None, None, None)
                }
            };

        // Ensure cache directory exists
        if let Some(cfg) = &cache_config {
            let _ = fs::create_dir_all(cfg.chain_dir());
        }

        // Try to load EVM state from binary cache (bincode format)
        let blockchain_db = if let Some(cfg) = &cache_config {
            let binary_path = cfg.binary_state_cache_path();

            if binary_path.exists() {
                let meta = BlockchainDbMeta::default();
                let db = BlockchainDb::new(meta, None);
                if binary_state::load_binary_state(&db, &binary_path) {
                    db
                } else {
                    let meta = BlockchainDbMeta::default();
                    BlockchainDb::new(meta, None)
                }
            } else {
                let meta = BlockchainDbMeta::default();
                BlockchainDb::new(meta, None)
            }
        } else {
            let meta = BlockchainDbMeta::default();
            BlockchainDb::new(meta, None)
        };

        // Filter storage by maintain list (if configured)
        if let Some(cfg) = &cache_config {
            let has_filter = !cfg.maintain_addresses.is_empty() || !cfg.maintain_slots.is_empty();
            if has_filter {
                let mut storage = blockchain_db.storage().write();
                let before_contracts = storage.len();
                let before_slots: usize = storage.values().map(|s| s.len()).sum();

                // Remove addresses not in any maintain list
                let addrs_to_remove: Vec<Address> = storage
                    .keys()
                    .filter(|addr| {
                        !cfg.maintain_addresses.contains(*addr)
                            && !cfg.maintain_slots.contains_key(*addr)
                    })
                    .copied()
                    .collect();
                for addr in &addrs_to_remove {
                    storage.remove(addr);
                }

                // For maintain_slots addresses: keep only the specified slots
                for (addr, allowed_slots) in &cfg.maintain_slots {
                    if let Some(addr_storage) = storage.get_mut(addr) {
                        addr_storage.retain(|slot, _| allowed_slots.contains(slot));
                    }
                }

                let after_contracts = storage.len();
                let after_slots: usize = storage.values().map(|s| s.len()).sum();
                drop(storage);

                debug!(
                    contracts_removed = before_contracts.saturating_sub(after_contracts),
                    slots_removed = before_slots.saturating_sub(after_slots),
                    contracts_kept = after_contracts,
                    slots_kept = after_slots,
                    "Filtered cached storage by maintain list"
                );
            }
        }

        // Seed bytecodes from the bytecodes.bin cache.
        // The binary EVM state cache stores accounts without bytecode,
        // so this is always needed when a cache config is present.
        if let Some(cfg) = &cache_config {
            let bytecode_path = cfg.bytecode_cache_path();
            if let Some(bytecode_cache) = BytecodeCache::load(&bytecode_path) {
                let loaded_count = Self::seed_bytecodes_from_cache(&blockchain_db, &bytecode_cache);
                if loaded_count > 0 {
                    debug!(
                        count = loaded_count,
                        path = ?bytecode_path,
                        "Loaded contract bytecodes from cache"
                    );
                }
            }
        }

        // Restore code-seed marks. Pruning rule: a mark is kept only while the
        // account it describes still holds code with the marked hash — a mark
        // whose code did not survive (evicted, never persisted, or clobbered)
        // is meaningless and must not outlive it. The reverse orphan
        // (code-without-mark) is prevented by `flush()` writing
        // `code_seeds.bin` BEFORE `bytecodes.bin`.
        let code_seeds: HashMap<Address, CodeSeedState> = cache_config
            .as_ref()
            .and_then(|cfg| CodeSeedCache::load(&cfg.code_seeds_cache_path()))
            .map(|cache| {
                let accounts = blockchain_db.accounts().read();
                let before = cache.entries.len();
                let mut entries = cache.entries;
                entries.retain(|addr, state| {
                    accounts.get(addr).is_some_and(|info| {
                        info.code.as_ref().is_some_and(|code| !code.is_empty())
                            && info.code_hash == state.code_hash()
                    })
                });
                if entries.len() < before {
                    debug!(
                        pruned = before - entries.len(),
                        kept = entries.len(),
                        "Pruned code-seed marks whose code did not survive the reload"
                    );
                }
                entries
            })
            .unwrap_or_default();

        // Load immutable data cache (token decimals).
        // This is still needed for validation and metadata lookups
        let immutable_cache = cache_config
            .as_ref()
            .and_then(|cfg| {
                let path = cfg.immutable_cache_path();
                ImmutableDataCache::load(&path).inspect(|cache| {
                    debug!(
                        token_decimals = cache.token_decimals.len(),
                        path = ?path,
                        "Loaded immutable data from cache"
                    );
                })
            })
            .unwrap_or_default();

        // Pre-populate in-memory token decimals from immutable cache
        let token_decimals = immutable_cache.token_decimals.clone();

        // Create an RPC callback for direct eth_call before moving provider into backend.
        // This bypasses revm simulation for batch queries where lazy storage fetching is too slow.
        let provider_for_rpc = provider.clone();
        let rpc_caller: RpcCallFn = Arc::new(move |to: Address, calldata: Bytes| {
            // Guard against panicking inside `block_in_place` on a current-thread
            // runtime (or when no runtime is present): degrade to a typed error.
            let handle = block_in_place_handle()?;
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    let tx = TransactionRequest::default()
                        .to(to)
                        .input(alloy_primitives::Bytes::from(calldata.to_vec()).into());
                    provider_for_rpc
                        .call(tx.into())
                        .await
                        .map_err(|e| RpcError::provider("eth_call", e))
                })
            })
        });

        // Batch storage fetcher: bulk `eth_call` state-override extraction by
        // default, with the classic point-read fetcher as its fallback and
        // repair path (see the `bulk_storage` module and
        // docs/bulk-storage-extraction.md). `StorageBatchConfig` tunes the
        // point-read path; `EvmCacheBuilder::storage_fetch_strategy` swaps or
        // tunes the bulk path.
        let storage_batch_fetcher = provider_storage_fetcher(
            provider.clone(),
            storage_batch_config,
            StorageFetchStrategy::default(),
        );

        // Create an account/root fetcher that bypasses SharedBackend, firing
        // `eth_getProof` calls directly for authoritative account fields plus the
        // account's `storageHash`. `eth_getProof` is single-address at the RPC
        // level, so a multi-account seam call (the reactive root gate, account
        // resyncs, cold-start probe_roots) fans out with bounded, order-
        // preserving concurrency (`buffered`): wall clock drops from N × RTT to
        // ~ceil(N / max_concurrent_proofs) × RTT.
        let account_proof_fetcher = account_proof_fetcher(provider.clone(), max_concurrent_proofs);

        // Create a bulk account-fields fetcher: balance + EXTCODEHASH for many
        // addresses in ONE eth_call via the account-fields extractor program.
        // This is the read side of code-seed verification; the call is
        // all-or-nothing per the `AccountFieldsFetchFn` contract.
        let provider_for_fields = provider.clone();
        let account_fields_fetcher: AccountFieldsFetchFn =
            Arc::new(move |addresses: Vec<Address>, block: BlockId| {
                // Guard against panicking inside `block_in_place` on a
                // current-thread runtime (or with no runtime present): degrade
                // to a typed error, matching the sibling fetchers.
                let handle = block_in_place_handle()?;
                tokio::task::block_in_place(|| {
                    handle.block_on(crate::bulk_storage::fetch_account_fields_bulk(
                        provider_for_fields.as_ref(),
                        &addresses,
                        block,
                    ))
                })
            });

        // Create a block-level state-diff fetcher over debug trace RPC. The
        // reactive runtime uses this as a trace-first accelerator before falling
        // back to point reads for unresolved cold targets.
        let provider_for_trace = provider.clone();
        let block_state_diff_fetcher: BlockStateDiffFetchFn = Arc::new(move |block: BlockId| {
            let handle = block_in_place_handle()?;
            tokio::task::block_in_place(|| {
                handle.block_on(async {
                    let (method, params) = trace_rpc_method_and_params(block);
                    let response = provider_for_trace
                        .client()
                        .request::<_, serde_json::Value>(method, params)
                        .await
                        .map_err(|e| StorageFetchError::provider(method, e))?;
                    parse_block_state_diff_trace(&response)
                        .map_err(|err| StorageFetchError::custom(err.to_string()))
                })
            })
        });

        // Resolve the chain ID reported to simulations (the `CHAINID` opcode). A
        // disk `CacheConfig` is authoritative (its `chain_id` also namespaces the
        // on-disk cache directory); otherwise infer it from the provider via
        // `eth_chainId`, falling back to 1 (Ethereum mainnet) only if that query
        // fails. Resolved before `provider` is moved into the backend below.
        // Prefer setting it explicitly through `EvmCacheBuilder::chain_id`.
        let chain_id = match cache_config.as_ref() {
            Some(cfg) => cfg.chain_id,
            None => match provider.get_chain_id().await {
                Ok(id) => id,
                Err(e) => {
                    debug!(
                        error = %e,
                        "Failed to infer chain ID from provider; defaulting to 1 (Ethereum mainnet). Set it explicitly via EvmCacheBuilder::chain_id."
                    );
                    1
                }
            },
        };

        // Spawn the backend handler on a background task
        let backend =
            SharedBackend::spawn_backend(provider, blockchain_db.clone(), Some(block_id)).await;

        let db = CacheDB::new(backend.clone());

        // Resolve the shared-memory pre-allocation. For `Auto` we size from the
        // amount of layer-2 chain state actually loaded (post-filter), so a large
        // bincode state file yields a larger buffer; `Fixed` ignores the count.
        let loaded_slots = match shared_memory_capacity {
            SharedMemoryCapacity::Auto => blockchain_db
                .storage()
                .read()
                .values()
                .map(|s| s.len())
                .sum(),
            SharedMemoryCapacity::Fixed(_) => 0,
        };
        let shared_memory_capacity = shared_memory_capacity.resolve(loaded_slots);

        Self {
            backend,
            blockchain_db,
            db,
            token_decimals,
            block,
            cache_config,
            immutable_cache,
            timestamp_override: timestamp,
            chain_id,
            block_number,
            basefee,
            coinbase,
            prevrandao,
            block_gas_limit,
            block_context_requirements: BlockContextRequirements::lenient(),
            storage_batch_config,
            shared_memory_buffer: Rc::new(RefCell::new(Vec::with_capacity(shared_memory_capacity))),
            snapshot_generation: 0,
            rpc_caller: Some(rpc_caller),
            storage_batch_fetcher: Some(storage_batch_fetcher),
            account_proof_fetcher: Some(account_proof_fetcher),
            block_state_diff_fetcher: Some(block_state_diff_fetcher),
            account_fields_fetcher: Some(account_fields_fetcher),
            code_seeds,
            erc20_balance_slots: HashMap::new(),
            spec_id,
            base: None,
            base_dirty: HashSet::new(),
            base_full_rebuild: false,
            base_storage_lens: HashMap::new(),
            shared_memory_capacity,
        }
    }

    /// Seed contract bytecodes into the BlockchainDb from a bytecode cache.
    ///
    /// This allows subsequent EVM executions to use cached bytecode instead of
    /// fetching from RPC. Storage slots will still be fetched fresh since they
    /// may have changed between blocks.
    fn seed_bytecodes_from_cache(db: &BlockchainDb, cache: &BytecodeCache) -> usize {
        let mut count = 0;
        for (addr, entry) in &cache.contracts {
            if entry.bytecode.is_empty() {
                continue;
            }

            // Create bytecode and compute hash
            let bytecode = Bytecode::new_raw(Bytes::from(entry.bytecode.clone()));
            let code_hash: B256 = bytecode.hash_slow();

            // Create account info with bytecode but zeroed balance/nonce
            // The balance/nonce will be fetched from RPC if needed during execution
            let info = AccountInfo {
                balance: U256::ZERO,
                nonce: 0,
                code_hash,
                code: Some(bytecode),
                account_id: None,
            };

            db.db().do_insert_account(*addr, info);
            count += 1;
        }
        count
    }

    /// Create a new EvmCache from an existing SharedBackend.
    ///
    /// Useful when you want to share a backend between multiple caches
    /// (e.g. parallel simulation threads).
    ///
    /// **Shared pinned block.** A `SharedBackend` owns a single pinned fork
    /// height. Calling [`set_block`](Self::set_block) / `repin_to_block` on *any*
    /// cache built from the same backend re-pins the RPC fork height for **all**
    /// of them. Sibling caches sharing one backend should agree on a block and not
    /// re-pin independently; build separate backends if they must fork at
    /// different heights.
    pub fn from_backend(
        backend: SharedBackend,
        blockchain_db: BlockchainDb,
        block: BlockId,
        chain_id: u64,
        block_number: Option<u64>,
        basefee: Option<u64>,
        spec_id: SpecId,
    ) -> Self {
        let db = CacheDB::new(backend.clone());
        Self {
            backend,
            blockchain_db,
            db,
            token_decimals: HashMap::new(),
            block,
            cache_config: None,
            immutable_cache: ImmutableDataCache::default(),
            timestamp_override: None,
            chain_id,
            block_number,
            basefee,
            coinbase: None,
            prevrandao: None,
            block_gas_limit: None,
            block_context_requirements: BlockContextRequirements::lenient(),
            storage_batch_config: StorageBatchConfig::default(),
            snapshot_generation: 0,
            shared_memory_buffer: Rc::new(RefCell::new(Vec::with_capacity(
                DEFAULT_SHARED_MEMORY_CAPACITY,
            ))),
            rpc_caller: None,
            storage_batch_fetcher: None,
            account_proof_fetcher: None,
            block_state_diff_fetcher: None,
            account_fields_fetcher: None,
            code_seeds: HashMap::new(),
            erc20_balance_slots: HashMap::new(),
            spec_id,
            base: None,
            base_dirty: HashSet::new(),
            base_full_rebuild: false,
            base_storage_lens: HashMap::new(),
            shared_memory_capacity: DEFAULT_SHARED_MEMORY_CAPACITY,
        }
    }

    /// Flush the cache state to disk.
    ///
    /// This persists:
    /// 1. Unified EVM state (accounts + storage) to `evm_state.bin` (bincode)
    /// 2. Contract bytecodes to `bytecodes.bin`
    /// 3. Immutable data (token decimals) to `immutable_data.bin`
    ///
    /// Call this after loading hot contract state and running simulations to
    /// speed up subsequent runs.
    /// The cache is also automatically flushed when the EvmCache is dropped.
    pub fn flush(&self) -> Result<()> {
        if let Some(cfg) = &self.cache_config {
            // Save EVM state to binary cache (bincode format)
            let binary_path = cfg.binary_state_cache_path();
            binary_state::save_binary_state(&self.blockchain_db, &binary_path)?;

            // Save code-seed marks BEFORE bytecodes (fail-closed ordering: a
            // mark without code is pruned on load and harmless; code without
            // its mark would let a Pending seed masquerade as RPC-origin).
            // Full replace, not merge: marks are mutable trust state, and a
            // merge would resurrect marks purged this session.
            let code_seeds_path = cfg.code_seeds_cache_path();
            CodeSeedCache {
                entries: self.code_seeds.clone(),
            }
            .save(&code_seeds_path)?;
            debug!(
                count = self.code_seeds.len(),
                path = ?code_seeds_path,
                "Updated code-seed mark cache (binary format)"
            );

            // Save bytecode cache
            let bytecode_path = cfg.bytecode_cache_path();
            let mut bytecode_cache = BytecodeCache::load(&bytecode_path).unwrap_or_default();
            bytecode_cache.merge_from_db(&self.blockchain_db);
            bytecode_cache.save(&bytecode_path)?;
            debug!(
                count = bytecode_cache.contracts.len(),
                path = ?bytecode_path,
                "Updated bytecode cache (binary format)"
            );

            // Save the immutable data cache
            let immutable_path = cfg.immutable_cache_path();
            self.immutable_cache.save(&immutable_path)?;
            debug!(
                token_decimals = self.immutable_cache.token_decimals.len(),
                path = ?immutable_path,
                "Updated immutable data cache"
            );
        }
        Ok(())
    }

    /// Get the cache configuration, if any.
    ///
    /// Returns `None` when the cache is purely in-memory (no disk persistence),
    /// i.e. constructed without a [`CacheConfig`] or via
    /// [`from_backend`](Self::from_backend).
    pub fn cache_config(&self) -> Option<&CacheConfig> {
        self.cache_config.as_ref()
    }

    /// Run a synchronous direct mutation against the underlying [`BlockchainDb`]
    /// and invalidate the memoized snapshot base afterwards.
    ///
    /// This is the preferred escape hatch for unavoidable layer-2 map writes such
    /// as `accounts().write().insert(...)` or `storage().write().insert(...)`.
    /// The closure still bypasses the CacheDB overlay and the normal write funnel,
    /// so use higher-level mutators when they can express the change. Unlike
    /// [`unchecked_blockchain_db`](Self::unchecked_blockchain_db), this wrapper
    /// keeps the copy-on-write snapshot base honest automatically after in-place
    /// overwrites whose map cardinality does not change.
    pub fn with_blockchain_db_mut<R>(&mut self, f: impl FnOnce(&BlockchainDb) -> R) -> R {
        let result = f(&self.blockchain_db);
        self.invalidate_base();
        result
    }

    /// Get an unchecked reference to the underlying [`BlockchainDb`] (the layer-2
    /// backend store of accounts, storage, and bytecodes).
    ///
    /// This exposes an internal store and bypasses the cache's two-layer
    /// consistency model: reads here see only the backend layer, not the CacheDB
    /// overlay, and any writes performed through it skip the overlay. Prefer
    /// higher-level accessors or [`with_blockchain_db_mut`](Self::with_blockchain_db_mut)
    /// for direct synchronous writes.
    ///
    /// # Snapshot base
    /// Writing layer 2 directly through this unchecked handle also bypasses the
    /// memoized copy-on-write snapshot base (Pillar A). The next
    /// [`snapshot`](Self::snapshot) only performs a count/absence
    /// growth scan over layer 2, which catches lazy RPC-populated accounts/slots
    /// because that path only appends at a fixed block. It does **not** catch
    /// direct in-place changes where cardinality is unchanged: overwriting an
    /// existing storage slot, or changing an existing account's info/code/balance
    /// without adding a new account, can leave a stale snapshot base. After such a
    /// direct write, call
    /// [`invalidate_snapshot_base`](Self::invalidate_snapshot_base) (or re-pin via
    /// [`set_block`](Self::set_block)) before the next snapshot. Writes via the
    /// crate's own mutators (`inject_storage_batch`, `apply_update`, the `inject_*`
    /// helpers, the purges) keep the base honest automatically.
    pub fn unchecked_blockchain_db(&self) -> &BlockchainDb {
        &self.blockchain_db
    }

    /// Get an unchecked reference to the underlying [`SharedBackend`] (the lazy
    /// RPC-backed fetcher shared across clones).
    ///
    /// This exposes an internal handle and bypasses the cache's two-layer consistency
    /// model: it reads/fetches directly without consulting the CacheDB overlay.
    /// Prefer the higher-level accessors; use with care.
    ///
    /// # Snapshot base
    /// Lazy RPC fetches through this backend only append missing accounts/slots at
    /// the pinned block, so the snapshot growth scan catches them without an
    /// explicit invalidation. Direct `SharedBackend::insert_or_update_storage` /
    /// `insert_or_update_address` calls are different: they enqueue a background
    /// handler request that can rewrite layer-2 entries **in place**, leaving the
    /// memoized copy-on-write base stale at an unchanged slot/account count.
    ///
    /// If you use those helpers directly, first synchronize with the backend
    /// handler by reading back the updated account/slot through `SharedBackend`
    /// (for example via `basic_ref` / `storage_ref`), then call
    /// [`invalidate_snapshot_base`](Self::invalidate_snapshot_base) before the next
    /// [`snapshot`](Self::snapshot). Calling
    /// `invalidate_snapshot_base` immediately after `insert_or_update_*` is not, by
    /// itself, a guarantee that the queued update has been applied before the next
    /// snapshot.
    pub fn unchecked_backend(&self) -> &SharedBackend {
        &self.backend
    }

    /// Get a mutable reference to the underlying [`ForkCacheDB`] (the layer-1
    /// CacheDB overlay).
    ///
    /// This exposes an internal and bypasses the cache's two-layer consistency
    /// model: writes made here land only in the overlay and are not mirrored
    /// into the BlockchainDb backend, so parallel tasks sharing the backend
    /// will not see them. Prefer the higher-level mutators; use with care.
    pub fn db_mut(&mut self) -> &mut ForkCacheDB {
        &mut self.db
    }

    /// Make a direct RPC `eth_call` to the node, bypassing revm simulation.
    ///
    /// This is much faster than `call_raw` for batch operations because the RPC
    /// node has all state in memory and doesn't need lazy storage fetching.
    /// Returns `None` if no RPC caller is available (e.g. `from_backend` constructor).
    ///
    /// # Panics
    /// Must be called from within a **multi-thread** tokio runtime: the callback
    /// drives the async `eth_call` to completion via
    /// `tokio::task::block_in_place`. On a current-thread runtime (or with no
    /// runtime), the callback degrades to an `Err` rather than panicking, but
    /// `block_in_place` itself will panic if invoked from a non-worker thread of
    /// a multi-thread runtime.
    pub fn rpc_call(&self, to: Address, calldata: Bytes) -> Option<Result<Bytes, RpcError>> {
        self.rpc_caller
            .as_ref()
            .map(|caller| (caller)(to, calldata))
    }

    /// Get the batch storage fetcher, if available.
    ///
    /// Returns `None` when constructed via `from_backend` (no provider available).
    ///
    /// # Panics
    /// The returned [`StorageBatchFetchFn`] must be invoked from within a
    /// **multi-thread** tokio runtime: it drives concurrent `eth_getStorageAt`
    /// calls to completion via `tokio::task::block_in_place`. On a
    /// current-thread runtime (or with no runtime) it degrades to an `Err`
    /// result for every requested slot rather than panicking, but
    /// `block_in_place` itself will panic if invoked from a non-worker thread of
    /// a multi-thread runtime.
    pub fn storage_batch_fetcher(&self) -> Option<&StorageBatchFetchFn> {
        self.storage_batch_fetcher.as_ref()
    }

    /// Get the account/root proof fetcher, if available.
    ///
    /// Returns `None` when constructed via `from_backend` (no provider
    /// available) unless a fetcher was injected via
    /// [`set_account_proof_fetcher`](Self::set_account_proof_fetcher).
    ///
    /// # Panics
    /// The returned [`AccountProofFetchFn`] must be invoked from within a
    /// **multi-thread** tokio runtime: it drives `eth_getProof` calls to
    /// completion via `tokio::task::block_in_place`. On a current-thread runtime
    /// (or with no runtime) it degrades to an `Err` result for every requested
    /// address rather than panicking, but `block_in_place` itself will panic if
    /// invoked from a non-worker thread of a multi-thread runtime.
    pub fn account_proof_fetcher(&self) -> Option<&AccountProofFetchFn> {
        self.account_proof_fetcher.as_ref()
    }

    /// Get the block state-diff fetcher, if available.
    ///
    /// Returns `None` when constructed via `from_backend` (no provider
    /// available) unless a fetcher was injected via
    /// [`set_block_state_diff_fetcher`](Self::set_block_state_diff_fetcher).
    pub fn block_state_diff_fetcher(&self) -> Option<&BlockStateDiffFetchFn> {
        self.block_state_diff_fetcher.as_ref()
    }

    /// Inject batch-fetched storage values directly into BlockchainDb (layer 2).
    ///
    /// This bypasses SharedBackend and makes values available for subsequent
    /// `storage_ref()` calls and EVM SLOADs. Used after `StorageBatchFetchFn`
    /// returns results to populate the cache in bulk.
    ///
    /// Takes `&mut self` (as of Pillar A) so it can mark each touched address dirty
    /// for the memoized copy-on-write base; the write itself is still a direct
    /// layer-2 backend write. Overwriting an existing slot at an unchanged slot
    /// count is invalidated here too, since the `refresh_base` growth scan only
    /// catches length changes.
    pub fn inject_storage_batch(&mut self, results: &[(Address, U256, U256)]) {
        {
            let mut storage = self.blockchain_db.storage().write();
            for &(addr, slot, value) in results {
                storage.entry(addr).or_default().insert(slot, value);
            }
        }
        for &(addr, _, _) in results {
            self.mark_base_dirty(addr);
        }
    }

    /// Inject freshly-fetched storage values, healing **both** cache layers.
    ///
    /// Like [`inject_storage_batch`](Self::inject_storage_batch) this writes each
    /// value into the BlockchainDb backend (layer 2). Additionally, for any
    /// address that *already* has a CacheDB overlay entry (layer 1), it writes
    /// the slot into that overlay too.
    ///
    /// This matters because both [`snapshot`](Self::snapshot) and
    /// the synchronous EVM SLOAD path let the overlay win over the backend. A
    /// correction written only to layer 2 would be shadowed by a stale layer-1
    /// slot, so the cache could never converge — the freshness validator would
    /// re-detect the same change and re-correct it every cycle. Writing through
    /// the overlay keeps the layer that wins authoritative.
    ///
    /// It deliberately does **not** create a new overlay account for an address
    /// that has none: such a slot is layer-2-only (e.g. cold prefetch), where
    /// the backend write is already authoritative and materializing an overlay
    /// entry would pollute layer 1 and could shadow later RPC reads.
    pub fn inject_storage_batch_fresh(&mut self, results: &[(Address, U256, U256)]) {
        // Thin wrapper over the unified write primitive (the F1 fix now lives in
        // `apply_slot`). Each tuple becomes a write-through `StateUpdate::Slot`;
        // the returned diff is discarded to preserve this method's `-> ()` API.
        let updates: Vec<StateUpdate> = results
            .iter()
            .map(|&(addr, slot, value)| StateUpdate::slot(addr, slot, value))
            .collect();
        let _ = self.apply_updates(&updates);
    }

    /// Bulk-load the given slots into the cache at its pinned block.
    ///
    /// Fetches through the installed [`StorageBatchFetchFn`] — bulk `eth_call`
    /// extraction by default, so thousands of slots (across many contracts)
    /// arrive in a handful of calls — and injects every successfully fetched
    /// value into layer 2 via
    /// [`inject_storage_batch`](Self::inject_storage_batch), the cold-prefetch
    /// write. Use it to prewarm a declared working set (an AMM pool's tick
    /// range, a protocol's config slots) before entering a simulation or
    /// reactive loop, complementing the *recorded* working sets that
    /// [`prefetch_registry`](crate::prefetch_registry) replays.
    ///
    /// Duplicate pairs are fetched once each and injected idempotently.
    /// Returns how many slots loaded and which pairs failed; failures leave
    /// the cache unchanged (those slots lazily point-read later as usual).
    pub fn prewarm_slots(&mut self, requests: &[(Address, U256)]) -> PrewarmReport {
        let Some(fetcher) = self.storage_batch_fetcher.clone() else {
            return PrewarmReport {
                loaded: 0,
                failed: requests
                    .iter()
                    .map(|&(addr, slot)| {
                        (
                            addr,
                            slot,
                            StorageFetchError::custom("no storage batch fetcher installed"),
                        )
                    })
                    .collect(),
            };
        };
        let results = fetcher(requests.to_vec(), self.block);
        let mut to_inject = Vec::with_capacity(results.len());
        let mut failed = Vec::new();
        for (addr, slot, result) in results {
            match result {
                Ok(value) => to_inject.push((addr, slot, value)),
                Err(e) => failed.push((addr, slot, e)),
            }
        }
        self.inject_storage_batch(&to_inject);
        PrewarmReport {
            loaded: to_inject.len(),
            failed,
        }
    }

    /// Apply a single targeted [`StateUpdate`], returning a [`StateDiff`] of what
    /// actually changed.
    ///
    /// This is the single primitive that writes the state-update vocabulary
    /// across both cache layers with one consistent, documented policy. It is
    /// **synchronous and infallible** — a write, not a fetch, so it never touches
    /// RPC and never errors. See the [`state_update`](crate::state_update) module
    /// for the dual-layer write-through policy and the diff semantics.
    ///
    /// - [`StateUpdate::Slot`] — write `value` into the backend (layer 2) always,
    ///   and into the overlay (layer 1) only if an overlay account already
    ///   exists. Records a [`SlotChange`] only when the value actually changes
    ///   (`old.unwrap_or(ZERO) != value`).
    /// - [`StateUpdate::SlotDelta`] — *relative*, cold-aware. If the slot has a
    ///   cached value, write the saturating delta through the same path and record
    ///   a [`SlotChange`] iff it changed; if the slot is cold (absent from both
    ///   layers), apply nothing and surface a `SkippedDelta` in `diff.skipped`.
    /// - [`StateUpdate::BalanceDelta`] — *relative*, cold-aware native-balance
    ///   update. If the account is present in either layer, apply the saturating
    ///   delta to its balance (nonce/code preserved) write-through and record an
    ///   [`AccountChange`] iff it changed; if the account is cold (absent from both
    ///   layers), apply nothing and surface a [`SkippedBalanceDelta`] in
    ///   `diff.skipped_balances` (no default account is materialized).
    /// - [`StateUpdate::Account`] — load the current `AccountInfo` from the cached
    ///   layers (no RPC), apply each `Some` patch field (recomputing the code hash
    ///   when `code` is set), then write through with the same layer policy.
    ///   Records an [`AccountChange`] with `Some((old, new))` only for fields
    ///   that changed. If the account is cold (absent from both layers), apply
    ///   nothing and surface a [`SkippedAccountPatch`] in
    ///   `diff.skipped_accounts`.
    /// - [`StateUpdate::AccountUpsert`] — same patch semantics, but intentionally
    ///   materializes a cold/default account when absent from both layers.
    /// - [`StateUpdate::Purge`] — dispatch to the matching purge layer logic and
    ///   record a [`PurgeRecord`].
    ///
    /// # Warning — relative updates can be skipped
    ///
    /// A cold-aware update targeting a **cold** address is *dropped, not applied*
    /// unless it is an explicit [`StateUpdate::AccountUpsert`]. Because a skip
    /// produces no change, it is invisible to the changes-only
    /// [`StateDiff::is_empty`] / [`StateDiff::len`] success check, so after
    /// applying cold-aware updates the caller **must** inspect
    /// [`StateDiff::has_skipped`] (or the `skipped_*` fields) and fetch+seed the
    /// cold target.
    ///
    /// ```no_run
    /// # use alloy_primitives::{Address, U256};
    /// # use evm_fork_cache::StateUpdate;
    /// # fn example(cache: &mut evm_fork_cache::cache::EvmCache) {
    /// let contract = Address::repeat_byte(0x01);
    /// let diff = cache.apply_update(&StateUpdate::slot(contract, U256::from(0), U256::from(42)));
    /// assert_eq!(diff.slots.len(), 1);
    /// # }
    /// ```
    pub fn apply_update(&mut self, update: &StateUpdate) -> StateDiff {
        self.bump_snapshot_generation();
        let mut diff = StateDiff::default();
        match update {
            StateUpdate::Slot {
                address,
                slot,
                value,
            } => {
                if let Some(change) = self.apply_slot(*address, *slot, *value) {
                    diff.slots.push(change);
                }
            }
            StateUpdate::SlotDelta {
                address,
                slot,
                delta,
            } => match self.cached_storage_value(*address, *slot) {
                // Hot slot: apply the saturating delta write-through. Build the
                // change from the value we already read (do not route through
                // `apply_slot`, which would re-read the same slot — §16.9.1).
                Some(current) => {
                    let new = delta.apply(current);
                    self.write_slot_through(*address, *slot, new);
                    if current != new {
                        diff.slots.push(SlotChange {
                            address: *address,
                            slot: *slot,
                            old: current,
                            new,
                        });
                    }
                }
                // Cold slot: applying `0 ± amount` would corrupt an unknown value,
                // so write nothing and surface the skip for the caller to seed.
                None => diff.skipped.push(SkippedDelta {
                    address: *address,
                    slot: *slot,
                    delta: *delta,
                }),
            },
            StateUpdate::SlotMasked {
                address,
                slot,
                mask,
                value,
            } => match self.cached_storage_value(*address, *slot) {
                // Hot slot: overwrite only the masked bits, preserving the rest.
                // Build the change from the value we already read (mirroring the
                // `SlotDelta` arm; do not re-read through `apply_slot`).
                Some(old) => {
                    let new = (old & !*mask) | (*value & *mask);
                    self.write_slot_through(*address, *slot, new);
                    if old != new {
                        diff.slots.push(SlotChange {
                            address: *address,
                            slot: *slot,
                            old,
                            new,
                        });
                    }
                }
                // Cold slot: the un-masked bits are unknown, so the result cannot
                // be computed; write nothing and surface the skip for re-seeding.
                None => diff.skipped_masks.push(SkippedMask {
                    address: *address,
                    slot: *slot,
                    mask: *mask,
                    value: *value,
                }),
            },
            StateUpdate::BalanceDelta { address, delta } => {
                match self.apply_balance_delta(*address, *delta) {
                    // Hot account: the saturating delta was applied.
                    Ok(Some(change)) => diff.accounts.push(change),
                    // Hot account but no change (e.g. Sub from 0, Add of 0).
                    Ok(None) => {}
                    // Cold account: surface the skip; nothing was materialized.
                    Err(skipped) => diff.skipped_balances.push(skipped),
                }
            }
            StateUpdate::Account { address, patch } => {
                match self.apply_account_patch(*address, patch, false) {
                    Ok(Some(change)) => diff.accounts.push(change),
                    Ok(None) => {}
                    Err(skipped) => diff.skipped_accounts.push(skipped),
                }
            }
            StateUpdate::AccountUpsert { address, patch } => {
                if let Some(change) = self
                    .apply_account_patch(*address, patch, true)
                    .expect("AccountUpsert never skips cold account patches")
                {
                    diff.accounts.push(change);
                }
            }
            StateUpdate::Purge { address, scope } => {
                diff.purged.push(self.apply_purge(*address, scope));
            }
        }
        diff
    }

    /// Apply a batch of [`StateUpdate`]s left-to-right, merging each per-update
    /// [`StateDiff`].
    ///
    /// Later updates observe the effect of earlier ones: two `Slot` writes to the
    /// same key record `old → a` then `a → b`. Like
    /// [`apply_update`](Self::apply_update) this is synchronous and infallible.
    ///
    /// # Performance — batched single-lock fast-path
    ///
    /// Consecutive `Slot`/`SlotDelta` writes are processed holding the backend
    /// storage write-guard **once** for the run (the overlay map is lock-free), so
    /// a bulk slot seed pays one lock acquisition instead of one read + one write
    /// lock per slot. Apply order is preserved: when an `Account`/`BalanceDelta`/
    /// `Purge` update is reached the guard is dropped first (those take the
    /// `accounts()` / `storage()` locks themselves — holding the storage
    /// write-guard across them would deadlock the non-reentrant `RwLock`), the
    /// update is processed via [`apply_update`](Self::apply_update), then the guard
    /// is lazily re-acquired on the next slot run. The result is byte-identical to
    /// folding [`apply_update`](Self::apply_update) over the batch.
    ///
    /// # Warning — relative updates can be skipped
    ///
    /// See [`apply_update`](Self::apply_update): a cold relative update is dropped,
    /// not applied, and is invisible to [`StateDiff::is_empty`] /
    /// [`StateDiff::len`]. After a batch with relative updates, check
    /// [`StateDiff::has_skipped`].
    pub fn apply_updates(&mut self, updates: &[StateUpdate]) -> StateDiff {
        if !updates.is_empty() {
            self.bump_snapshot_generation();
        }
        let mut diff = StateDiff::default();
        let mut i = 0;
        while i < updates.len() {
            match &updates[i] {
                // A run of consecutive slot writes: process them under a single
                // held storage write-guard, then advance past the run.
                StateUpdate::Slot { .. } | StateUpdate::SlotDelta { .. } => {
                    let run_end = updates[i..]
                        .iter()
                        .position(|u| {
                            !matches!(u, StateUpdate::Slot { .. } | StateUpdate::SlotDelta { .. })
                        })
                        .map(|off| i + off)
                        .unwrap_or(updates.len());
                    self.apply_slot_run(&updates[i..run_end], &mut diff);
                    i = run_end;
                }
                // Account / BalanceDelta / Purge: no held guard (they take their
                // own locks), so route through the single-update primitive.
                _ => {
                    diff.merge(self.apply_update(&updates[i]));
                    i += 1;
                }
            }
        }
        diff
    }

    /// Apply a run of consecutive `Slot`/`SlotDelta` updates under one held backend
    /// storage write-guard (§16.9.2), merging each change into `diff`.
    ///
    /// The backend storage guard is acquired once for the whole run; overlay access
    /// is lock-free (`self.db.cache.accounts`). The old-value read stays
    /// `account_state`-aware (matching [`cached_storage_value`](Self::cached_storage_value)):
    /// for an overlay account whose slot is absent, a `StorageCleared`/`NotExisting`
    /// state reads ZERO and the backend is **not** consulted. Behavior is identical
    /// to applying each update via [`apply_update`](Self::apply_update); the
    /// `apply_updates_batched_equals_sequential` test pins this.
    fn apply_slot_run(&mut self, run: &[StateUpdate], diff: &mut StateDiff) {
        // Borrow the two layers as disjoint fields: the backend storage guard
        // (layer 2) held for the whole run, and the overlay accounts map (layer 1,
        // lock-free). Base invalidation is deferred until after the guard is
        // dropped (it needs `&mut self`): collect the layer-2 addresses written
        // here and mark them dirty below.
        let mut dirtied: Vec<Address> = Vec::new();
        let overlay = &mut self.db.cache.accounts;
        let mut storage = self.blockchain_db.storage().write();

        for update in run {
            // Resolve `(address, slot, old, new)` for the write; a cold SlotDelta
            // is skipped here (write nothing). `old` is the `account_state`-aware
            // read (overlay ▸ cleared-as-ZERO ▸ backend), reused for both the write
            // gate and the change record so each slot is read at most once.
            let (address, slot, old, new) = match update {
                StateUpdate::Slot {
                    address,
                    slot,
                    value,
                } => {
                    let old = read_slot_account_state_aware(overlay, &storage, *address, *slot)
                        .unwrap_or(U256::ZERO);
                    (*address, *slot, old, *value)
                }
                StateUpdate::SlotDelta {
                    address,
                    slot,
                    delta,
                } => match read_slot_account_state_aware(overlay, &storage, *address, *slot) {
                    // Hot: apply the saturating delta to the value already read.
                    Some(current) => (*address, *slot, current, delta.apply(current)),
                    // Cold: skip and surface (write nothing).
                    None => {
                        diff.skipped.push(SkippedDelta {
                            address: *address,
                            slot: *slot,
                            delta: *delta,
                        });
                        continue;
                    }
                },
                // The caller only ever hands this method slot updates.
                _ => unreachable!("apply_slot_run only processes Slot/SlotDelta"),
            };

            write_slot_into(overlay, &mut storage, address, slot, new);
            // Layer 2 was written for this address → it must be re-folded into the
            // memoized base. Mirrors `write_slot_through`'s `mark_base_dirty`.
            dirtied.push(address);
            if old != new {
                diff.slots.push(SlotChange {
                    address,
                    slot,
                    old,
                    new,
                });
            }
        }

        // Drop the storage write-guard before taking `&mut self` for invalidation.
        drop(storage);
        for address in dirtied {
            self.mark_base_dirty(address);
        }
    }

    /// Write-through a single storage slot (§5.1). Returns a [`SlotChange`] iff
    /// the slot's value actually changes.
    fn apply_slot(&mut self, address: Address, slot: U256, value: U256) -> Option<SlotChange> {
        // Old value: overlay ▸ backend ▸ None (treated as ZERO).
        let old = self
            .cached_storage_value(address, slot)
            .unwrap_or(U256::ZERO);

        self.write_slot_through(address, slot, value);

        // Record only an actual change.
        (old != value).then_some(SlotChange {
            address,
            slot,
            old,
            new: value,
        })
    }

    /// The single dual-layer slot write path (§5.1), shared by [`apply_slot`],
    /// the [`StateUpdate::SlotDelta`] handler, and [`modify_slot`](Self::modify_slot).
    ///
    /// Backend (layer 2) is always written; the overlay (layer 1) is written only
    /// if an overlay account already exists. A new overlay account is never
    /// materialized: that preserves the layer-2-only invariant (a fresh
    /// `StorageCleared` overlay account would read missing slots as ZERO and could
    /// shadow later RPC reads), and an absent overlay entry falls through to the
    /// backend on reads so the backend write is authoritative.
    fn write_slot_through(&mut self, address: Address, slot: U256, value: U256) {
        // Backend (layer 2): always write.
        {
            let mut storage = self.blockchain_db.storage().write();
            storage.entry(address).or_default().insert(slot, value);
        }

        // Overlay (layer 1): write only if an overlay account already exists.
        if let Some(db_account) = self.db.cache.accounts.get_mut(&address) {
            db_account.storage.insert(slot, value);
        }

        // Layer 2 changed → invalidate the memoized base for this address (D2:
        // over-invalidation when also shadowed by layer 1 is safe).
        self.mark_base_dirty(address);
    }

    /// Read-modify-write one storage slot through a caller-supplied transform.
    ///
    /// The general closure escape hatch behind [`StateUpdate::SlotDelta`] (the
    /// data-level form flows through [`apply_update`](Self::apply_update); this is
    /// for arbitrary transforms). `f` is called with the current cached value
    /// (overlay ▸ backend ▸ `None` when the slot is cold) and decides the new
    /// value:
    ///
    /// - `Some(new)` writes `new` through both layers (the same write path as
    ///   [`StateUpdate::Slot`]) and returns a [`SlotChange`] iff it changed
    ///   (`old.unwrap_or(ZERO) != new`);
    /// - `None` writes nothing and returns `None`.
    ///
    /// The caller owns the cold/overflow policy. To skip cold slots (the
    /// cold-aware read-modify-write rule), map through the `Option`:
    /// `|cur| cur.map(|v| v.saturating_add(amount))` leaves a cold slot untouched.
    /// To write an absolute value regardless, ignore the argument: `|_| Some(v)`.
    ///
    /// ```no_run
    /// # use alloy_primitives::{Address, U256};
    /// # fn example(cache: &mut evm_fork_cache::cache::EvmCache) {
    /// let token = Address::repeat_byte(0x01);
    /// let slot = U256::from(0);
    /// // Saturating +100, but only if the slot is already hot.
    /// let change = cache.modify_slot(token, slot, |cur| cur.map(|v| v.saturating_add(U256::from(100))));
    /// # let _ = change;
    /// # }
    /// ```
    pub fn modify_slot(
        &mut self,
        address: Address,
        slot: U256,
        f: impl FnOnce(Option<U256>) -> Option<U256>,
    ) -> Option<SlotChange> {
        let current = self.cached_storage_value(address, slot);
        let new = f(current)?;

        self.bump_snapshot_generation();
        self.write_slot_through(address, slot, new);

        let old = current.unwrap_or(U256::ZERO);
        (old != new).then_some(SlotChange {
            address,
            slot,
            old,
            new,
        })
    }

    /// Read-modify-write an account's native balance through a caller-supplied
    /// transform.
    ///
    /// The closure analog of [`StateUpdate::BalanceDelta`] (the data-level form
    /// flows through [`apply_update`](Self::apply_update); this is for arbitrary
    /// transforms). `f` is called with the account's current native balance
    /// (overlay ▸ backend ▸ `None` when the account is absent from **both**
    /// layers) and decides the new balance:
    ///
    /// - `Some(new)` writes `new` through both layers — backend always, overlay
    ///   only if an overlay account already exists — preserving the account's
    ///   nonce and code, and returns an [`AccountChange`] (balance only) iff the
    ///   balance changed;
    /// - `None` writes nothing (no account is materialized) and returns `None`.
    ///
    /// "Cold" for a balance is the account being absent from both layers — or
    /// present in the overlay as revm `NotExisting` (absent to the EVM), which the
    /// internal account read also treats as cold, mirroring `DbAccount::info()`.
    /// To skip cold accounts, map through the `Option`:
    /// `|cur| cur.map(|v| v.saturating_add(amount))`.
    ///
    /// ```no_run
    /// # use alloy_primitives::{Address, U256};
    /// # fn example(cache: &mut evm_fork_cache::cache::EvmCache) {
    /// let acct = Address::repeat_byte(0x01);
    /// // Saturating +100, but only if the account's balance is already known.
    /// let change = cache.modify_account_balance(acct, |cur| cur.map(|v| v.saturating_add(U256::from(100))));
    /// # let _ = change;
    /// # }
    /// ```
    pub fn modify_account_balance(
        &mut self,
        address: Address,
        f: impl FnOnce(Option<U256>) -> Option<U256>,
    ) -> Option<AccountChange> {
        // Load the full info from the cached layers only (overlay ▸ backend); the
        // account is "cold" when absent from both.
        let base = self.loaded_account_info(address);
        let current_balance = base.as_ref().map(|info| info.balance);
        let new_balance = f(current_balance)?;

        // The closure asked to write `new_balance`. Materialize from the loaded
        // base (or a default if the caller chose to write a cold account).
        let mut info = base.unwrap_or_default();
        let old_balance = info.balance;
        info.balance = new_balance;
        self.write_account_info_through(address, info);

        (old_balance != new_balance).then_some(AccountChange {
            address,
            balance: Some((old_balance, new_balance)),
            nonce: None,
            code_hash: None,
        })
    }

    /// Apply a relative (saturating) [`SlotDelta`] to an account's native balance
    /// (§16.5). Cold-aware:
    ///
    /// - `Ok(Some(change))` — present account, balance changed;
    /// - `Ok(None)` — present account, balance unchanged (e.g. `Sub` from 0);
    /// - `Err(skipped)` — cold account (absent from both layers): nothing applied,
    ///   nothing materialized.
    fn apply_balance_delta(
        &mut self,
        address: Address,
        delta: SlotDelta,
    ) -> std::result::Result<Option<AccountChange>, SkippedBalanceDelta> {
        let Some(mut info) = self.loaded_account_info(address) else {
            // Cold: applying a delta against an unknown balance would corrupt it,
            // and materializing a default account would mask the real on-chain one.
            return Err(SkippedBalanceDelta { address, delta });
        };

        let old_balance = info.balance;
        let new_balance = delta.apply(old_balance);
        info.balance = new_balance;
        self.write_account_info_through(address, info);

        Ok((old_balance != new_balance).then_some(AccountChange {
            address,
            balance: Some((old_balance, new_balance)),
            nonce: None,
            code_hash: None,
        }))
    }

    /// Load an account's `AccountInfo` from the cached layers only (overlay ▸
    /// backend), without touching RPC. `None` when the account is absent from
    /// both layers.
    fn loaded_account_info(&self, address: Address) -> Option<AccountInfo> {
        let mut info = if let Some(a) = self.db.cache.accounts.get(&address) {
            // Mirror revm `DbAccount::info()` / `basic_ref`: a NotExisting overlay
            // account is absent to the EVM (returns None) and does NOT fall through
            // to the backend. Without this, a relative balance update / partial
            // patch would compute against a stale `info` the EVM never sees.
            if matches!(a.account_state, AccountState::NotExisting) {
                return None;
            }
            a.info.clone()
        } else {
            self.blockchain_db
                .accounts()
                .read()
                .get(&address)
                .cloned()?
        };
        // Normalize like revm `insert_contract`: a ZERO code_hash denotes empty
        // code -> KECCAK_EMPTY. Done at load time so a patch's `old_code_hash`
        // matches what `write_account_info_through` stores (a self-consistent diff,
        // no phantom/under-reported code_hash change).
        if info.code_hash == B256::ZERO {
            info.code_hash = revm::primitives::KECCAK_EMPTY;
        }
        Some(info)
    }

    /// Write an `AccountInfo` through both layers, mirroring the slot policy:
    /// backend (layer 2) always; overlay (layer 1) only if an overlay account
    /// already exists (never materialize a new overlay account).
    fn write_account_info_through(&mut self, address: Address, mut info: AccountInfo) {
        // Normalize the code hash the way revm's `insert_contract` (applied on the
        // overlay write below) does, so both layers store an identical hash: a ZERO
        // code_hash denotes empty code → KECCAK_EMPTY. Otherwise the overlay would
        // hold KECCAK_EMPTY while the backend kept ZERO for the same account.
        if info.code_hash == B256::ZERO {
            info.code_hash = revm::primitives::KECCAK_EMPTY;
        }
        let overlay_present = self.db.cache.accounts.contains_key(&address);
        {
            let mut accounts = self.blockchain_db.accounts().write();
            accounts.insert(address, info.clone());
        }
        if overlay_present {
            self.db.insert_account_info(address, info);
        }
        // Layer-2 account info changed → invalidate the memoized base for this
        // address (D2: over-invalidation when also in layer 1 is safe).
        self.mark_base_dirty(address);
    }

    /// Apply a partial [`AccountPatch`] write-through (§5.2). Returns an
    /// [`AccountChange`] iff any field actually changes.
    fn apply_account_patch(
        &mut self,
        address: Address,
        patch: &AccountPatch,
        allow_cold_upsert: bool,
    ) -> std::result::Result<Option<AccountChange>, SkippedAccountPatch> {
        // 1. Current info from the cached layers only (overlay ▸ backend). No RPC:
        //    apply is a write, not a fetch. A partial patch on a cold account is
        //    skipped unless the caller explicitly chose AccountUpsert.
        let mut info = match self.loaded_account_info(address) {
            Some(info) => info,
            None if account_patch_is_empty(patch) => return Ok(None),
            None if allow_cold_upsert => AccountInfo::default(),
            None => {
                return Err(SkippedAccountPatch {
                    address,
                    patch: patch.clone(),
                });
            }
        };

        let old_balance = info.balance;
        let old_nonce = info.nonce;
        let old_code_hash = info.code_hash;

        // 2. Apply each `Some` field.
        if let Some(balance) = patch.balance {
            info.balance = balance;
        }
        if let Some(nonce) = patch.nonce {
            info.nonce = nonce;
        }
        if let Some(code) = &patch.code {
            let bytecode = Bytecode::new_raw(code.clone());
            info.code_hash = bytecode.hash_slow();
            info.code = Some(bytecode);
        }

        // 3. Compute the change first. A no-op patch (every field equals the
        //    loaded base) must NOT write either layer — otherwise an all-`None`
        //    patch on an absent address would insert `AccountInfo::default()` into
        //    the shared backend (masking a future RPC fetch) while returning an
        //    empty diff. Only a real field change materializes anything.
        let change = AccountChange {
            address,
            balance: (old_balance != info.balance).then_some((old_balance, info.balance)),
            nonce: (old_nonce != info.nonce).then_some((old_nonce, info.nonce)),
            code_hash: (old_code_hash != info.code_hash).then_some((old_code_hash, info.code_hash)),
        };
        if change.balance.is_none() && change.nonce.is_none() && change.code_hash.is_none() {
            return Ok(None);
        }

        // 4. Write-through, mirroring the slot policy: backend always; overlay
        //    only if an overlay account already exists (do not materialize one).
        self.write_account_info_through(address, info);

        Ok(Some(change))
    }

    /// Dispatch a [`PurgeScope`] to the matching layer logic (§5.3), returning a
    /// [`PurgeRecord`] of what was removed from each layer.
    fn apply_purge(&mut self, address: Address, scope: &PurgeScope) -> PurgeRecord {
        match scope {
            PurgeScope::Account => {
                let (slots_removed, account_removed) = self.purge_account_inner(address);
                PurgeRecord {
                    address,
                    scope: PurgeScope::Account,
                    slots_removed,
                    account_removed,
                }
            }
            PurgeScope::AllStorage => {
                let slots_removed = self.purge_contract_storage_inner(address);
                PurgeRecord {
                    address,
                    scope: PurgeScope::AllStorage,
                    slots_removed,
                    account_removed: false,
                }
            }
            PurgeScope::Slots(slots) => {
                let slots_removed = self.purge_contract_slots_inner(address, slots);
                PurgeRecord {
                    address,
                    scope: PurgeScope::Slots(slots.clone()),
                    slots_removed,
                    account_removed: false,
                }
            }
        }
    }

    /// Set (or replace) the batch storage fetcher.
    ///
    /// This is the seam the freshness controller and tests use to drive
    /// re-verification without a live provider: a stubbed
    /// [`StorageBatchFetchFn`] can be injected over a mocked-provider cache.
    /// Production callers can also inject their own transport, retry, batching,
    /// or rate-limiting strategy here. Once replaced, the cache's
    /// [`StorageBatchConfig`] no longer controls batching; the custom fetcher is
    /// responsible for honoring the [`StorageBatchFetchFn`] contract.
    pub fn set_storage_batch_fetcher(&mut self, f: StorageBatchFetchFn) {
        self.storage_batch_fetcher = Some(f);
    }

    /// Set (or replace) the account/root proof fetcher.
    ///
    /// This is the seam account-target resyncs and account-level freshness use to
    /// drive `eth_getProof` fetches without a live provider: a stubbed
    /// [`AccountProofFetchFn`] can be injected over a mocked-provider cache,
    /// mirroring [`set_storage_batch_fetcher`](Self::set_storage_batch_fetcher).
    pub fn set_account_proof_fetcher(&mut self, f: AccountProofFetchFn) {
        self.account_proof_fetcher = Some(f);
    }

    /// Set (or replace) the block state-diff fetcher.
    ///
    /// This is the seam trace-backed reactive resync uses to resolve matching
    /// targets from one block-level debug trace before falling back to storage or
    /// account proof point reads.
    pub fn set_block_state_diff_fetcher(&mut self, f: BlockStateDiffFetchFn) {
        self.block_state_diff_fetcher = Some(f);
    }

    /// Set (or replace) the bulk account-fields fetcher.
    ///
    /// This is the seam [`verify_code_seeds`](Self::verify_code_seeds) (and
    /// the cold-start `verify_code` phase) reads through: a stubbed
    /// [`AccountFieldsFetchFn`] can be injected over a mocked-provider cache,
    /// mirroring [`set_storage_batch_fetcher`](Self::set_storage_batch_fetcher).
    pub fn set_account_fields_fetcher(&mut self, f: AccountFieldsFetchFn) {
        self.account_fields_fetcher = Some(f);
    }

    /// The installed bulk account-fields fetcher, if any.
    ///
    /// `Some` on provider-backed caches (default-wired to
    /// [`fetch_account_fields_bulk`](crate::bulk_storage::fetch_account_fields_bulk));
    /// `None` on [`from_backend`](Self::from_backend) caches until one is
    /// installed via
    /// [`set_account_fields_fetcher`](Self::set_account_fields_fetcher).
    pub fn account_fields_fetcher(&self) -> Option<&AccountFieldsFetchFn> {
        self.account_fields_fetcher.as_ref()
    }

    /// Return the currently-cached value for a storage slot, if any.
    ///
    /// Mirrors what the EVM would `SLOAD` from the cached layers (it never touches
    /// RPC, unlike [`read_storage_slot`](Self::read_storage_slot)):
    ///
    /// 1. The CacheDB overlay (layer 1) wins: if the overlay account holds the
    ///    slot, return it.
    /// 2. Match revm's `CacheDB::storage_ref`: if the overlay account exists but
    ///    does **not** hold the slot, and its `account_state` is `StorageCleared`
    ///    or `NotExisting`, the live EVM reads the slot as ZERO and never consults
    ///    the backend — so return `Some(U256::ZERO)`, **not** the (shadowed)
    ///    backend value. Returning the backend value here would let a
    ///    `SlotDelta`/`modify_slot` compute a delta against a base the EVM never
    ///    sees (silent corruption) and would mis-record `apply_slot`'s `old`.
    /// 3. Otherwise fall through to the BlockchainDb backend (layer 2); `None` when
    ///    neither layer has seen the slot.
    pub fn cached_storage_value(&self, address: Address, slot: U256) -> Option<U256> {
        if let Some(db_account) = self.db.cache.accounts.get(&address) {
            if let Some(value) = db_account.storage.get(&slot) {
                return Some(*value);
            }
            // A StorageCleared / NotExisting overlay account reads a missing slot
            // as ZERO and never consults the backend (matching the EVM SLOAD).
            if matches!(
                db_account.account_state,
                AccountState::StorageCleared | AccountState::NotExisting
            ) {
                return Some(U256::ZERO);
            }
        }
        let storage = self.blockchain_db.storage().read();
        storage.get(&address).and_then(|s| s.get(&slot).copied())
    }

    /// Re-fetch the given slots via the batch fetcher, compare to the currently
    /// cached values, and inject the ones that changed.
    ///
    /// For each slot whose freshly-fetched value differs from the cached value,
    /// the fresh value is written into the cache via
    /// [`inject_storage_batch_fresh`](Self::inject_storage_batch_fresh) and a
    /// [`SlotChange`] is recorded. Slots that are unchanged, or that the fetcher
    /// fails to return, are left as-is. Returns the set of changed slots.
    ///
    /// Requires a batch fetcher (set at construction or via
    /// [`set_storage_batch_fetcher`](Self::set_storage_batch_fetcher)); errors if
    /// none is available. This is the synchronous main-thread primitive; the
    /// background validator performs the equivalent comparison against a snapshot.
    pub fn verify_slots(&mut self, slots: &[(Address, U256)]) -> Result<Vec<SlotChange>> {
        Ok(self.verify_slots_inner(slots)?.0)
    }

    /// Shared implementation for [`verify_slots`](Self::verify_slots) and the
    /// pipeline's reconcile path. Returns `(changed, fetched_ok)` where
    /// `fetched_ok` is the number of requested slots the fetcher returned a value
    /// for (failed per-slot fetches are skipped, not errors). Errors only when no
    /// batch fetcher is configured.
    fn verify_slots_inner(
        &mut self,
        slots: &[(Address, U256)],
    ) -> Result<(Vec<SlotChange>, usize)> {
        let (changed, outcomes) = self.verify_slots_core(slots)?;
        let fetched_ok = outcomes
            .iter()
            .filter(|o| matches!(o.fetch, SlotFetch::Value(_) | SlotFetch::Zero))
            .count();
        Ok((changed, fetched_ok))
    }

    /// Classify a single fetched slot value into a [`SlotFetch`].
    ///
    /// This is purely the *fetch* classification (`Value` / `Zero` /
    /// `FetchFailed`); it is independent of change detection, which compares the
    /// fetched value to the cached baseline separately. A non-zero `Ok` is
    /// [`SlotFetch::Value`], a genuine `Ok(0)` is [`SlotFetch::Zero`], and an
    /// `Err` is [`SlotFetch::FetchFailed`] carrying the error string.
    ///
    /// Shared with the cold-start probe phase
    /// ([`execute_cold_start_round`](Self::execute_cold_start_round)) so the
    /// single classification is reused rather than duplicated.
    pub(crate) fn classify(fetched: StorageFetchResult<U256>) -> SlotFetch {
        match fetched {
            Ok(v) if v != U256::ZERO => SlotFetch::Value(v),
            Ok(_) => SlotFetch::Zero,
            Err(e) => SlotFetch::FetchFailed {
                reason: e.to_string(),
            },
        }
    }

    /// Core slot-verification loop shared by [`verify_slots_inner`](Self::verify_slots_inner)
    /// and [`verify_slots_with_outcomes`](Self::verify_slots_with_outcomes).
    ///
    /// Fetches every slot via the batch fetcher and, for each slot, performs two
    /// **independent** reads of the same fetched value:
    ///
    /// 1. *Fetch classification* — every slot (including failed ones) produces one
    ///    [`SlotOutcome`] via [`classify`](Self::classify): `Value` / `Zero` /
    ///    `FetchFailed`.
    /// 2. *Change detection* — a successfully-fetched value that differs from the
    ///    cached baseline (`old`, defaulting to `ZERO` for an unseen slot) is
    ///    injected via [`inject_storage_batch_fresh`](Self::inject_storage_batch_fresh)
    ///    and recorded as a [`SlotChange`].
    ///
    /// These two reads are deliberately not collapsed: a genuine `Ok(0)` on a slot
    /// whose cached value was also `0` yields [`SlotFetch::Zero`] **and** no
    /// `SlotChange`. The returned `outcomes` vec has exactly one entry per
    /// requested slot. An empty `slots` input short-circuits to empty results
    /// without requiring a fetcher; otherwise a missing fetcher is an error.
    fn verify_slots_core(
        &mut self,
        slots: &[(Address, U256)],
    ) -> Result<(Vec<SlotChange>, Vec<SlotOutcome>)> {
        if slots.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let fetcher = self
            .storage_batch_fetcher
            .as_ref()
            .ok_or(CacheError::MissingStorageBatchFetcher)?
            .clone();

        // Snapshot the cached values before fetching so we compare against a
        // stable baseline.
        let cached: HashMap<(Address, U256), Option<U256>> = slots
            .iter()
            .map(|&(addr, slot)| ((addr, slot), self.cached_storage_value(addr, slot)))
            .collect();

        let results = (fetcher)(slots.to_vec(), self.block);

        let mut changed = Vec::new();
        let mut outcomes = Vec::with_capacity(results.len());
        let mut to_inject = Vec::new();
        for (addr, slot, fetched) in results {
            // Read 1: classify the fetch outcome for every slot, failed or not.
            let fetch = Self::classify(match &fetched {
                Ok(v) => Ok(*v),
                Err(e) => Err(StorageFetchError::custom(e.to_string())),
            });
            outcomes.push(SlotOutcome {
                address: addr,
                slot,
                fetch,
            });

            // Read 2: change detection, independent of the classification above.
            let fresh = match fetched {
                Ok(value) => value,
                Err(e) => {
                    debug!(%addr, %slot, error = %e, "verify_slots: fetch failed, skipping slot");
                    continue;
                }
            };
            // A slot the cache never saw is treated as old = ZERO (the value a
            // sim would have read), so a non-zero fresh value counts as a change.
            let old = cached
                .get(&(addr, slot))
                .copied()
                .flatten()
                .unwrap_or(U256::ZERO);
            if fresh != old {
                to_inject.push((addr, slot, fresh));
                changed.push(SlotChange {
                    address: addr,
                    slot,
                    old,
                    new: fresh,
                });
            }
        }

        if !to_inject.is_empty() {
            self.inject_storage_batch_fresh(&to_inject);
        }
        Ok((changed, outcomes))
    }

    /// Like [`verify_slots`](Self::verify_slots), but additionally returns one
    /// [`SlotOutcome`] per requested slot (including slots the fetcher failed to
    /// return), classified as `Value` / `Zero` / `FetchFailed`.
    ///
    /// This is the per-slot surface the cold-start driver consumes: it
    /// distinguishes a genuine on-chain zero from a fetch failure for every slot,
    /// closing the archive-miss gap. It is a pure alias of
    /// [`verify_slots_core`](Self::verify_slots_core) and shares its injection
    /// behaviour with [`verify_slots`](Self::verify_slots).
    #[cfg(feature = "reactive")]
    pub(crate) fn verify_slots_with_outcomes(
        &mut self,
        slots: &[(Address, U256)],
    ) -> Result<(Vec<SlotChange>, Vec<SlotOutcome>)> {
        self.verify_slots_core(slots)
    }

    /// Reconciliation re-read used by [`EventPipeline::reconcile`](crate::events::EventPipeline::reconcile).
    ///
    /// Like [`verify_slots`](Self::verify_slots) it fetches the requested slots,
    /// injects the ones that changed, and returns the changed set — but it is
    /// **honest about reachability**: it errors not only when no batch fetcher is
    /// configured, but also when a non-empty request could not fetch **any** slot
    /// (a total fetch failure — e.g. the default RPC fetcher invoked with no usable
    /// runtime, or an unreachable provider). Reconciliation that silently "verified
    /// nothing" would be a false all-clear, so it surfaces as an error for the
    /// caller to retry. A partially-successful fetch returns `Ok` with whatever
    /// changed.
    pub fn reconcile_slots(&mut self, slots: &[(Address, U256)]) -> Result<Vec<SlotChange>> {
        let (changed, fetched_ok) = self.verify_slots_inner(slots)?;
        if !slots.is_empty() && fetched_ok == 0 {
            return Err(CacheError::ReconcileFetchFailed {
                requested: slots.len(),
            });
        }
        Ok(changed)
    }

    /// Purge an account fully from both cache layers: its `AccountInfo`
    /// (balance/nonce/code hash) **and** all of its storage.
    ///
    /// Removes `addr` from the CacheDB overlay accounts map, the BlockchainDb
    /// accounts map, and the BlockchainDb storage map, so the next access
    /// re-fetches a clean account from RPC. This is the account-level
    /// counterpart to the storage-only [`purge_contract_storage`](Self::purge_contract_storage):
    /// use it when an address is fully volatile (no pinned slots) and even its
    /// balance/nonce/code can no longer be trusted.
    pub fn purge_account(&mut self, addr: Address) {
        // Thin wrapper over the unified purge primitive; the layer logic lives in
        // `purge_account_inner` (shared with `apply_update(Purge { Account })`).
        let _ = self.apply_update(&StateUpdate::purge(addr, PurgeScope::Account));
    }

    /// Account-scope purge layer logic. Removes `addr` from the overlay accounts
    /// map, the backend accounts map, and the backend storage map. Returns
    /// `(backend_slots_removed, account_removed)` where `account_removed` is true
    /// if an account entry was removed from either account layer.
    fn purge_account_inner(&mut self, addr: Address) -> (usize, bool) {
        // An account-scope purge also discards any code-seed mark: whatever
        // trust state the code carried, the code itself is gone, and the next
        // touch refetches authoritative chain state (which is unmarked
        // RPC-origin by definition). This is also the documented escape hatch
        // for re-seeding after a believed redeploy.
        self.code_seeds.remove(&addr);

        // Layer 1: CacheDB overlay (accounts + their storage live together).
        let overlay_removed = self.db.cache.accounts.remove(&addr).is_some();

        // Layer 2: BlockchainDb accounts + storage maps.
        let backend_account_removed = self
            .blockchain_db
            .accounts()
            .write()
            .remove(&addr)
            .is_some();
        let backend_storage_removed = self.blockchain_db.storage().write().remove(&addr);
        let slots_removed = backend_storage_removed
            .map(|slots| slots.len())
            .unwrap_or(0);

        let account_removed = overlay_removed || backend_account_removed;
        if account_removed || slots_removed > 0 {
            debug!(
                account = %addr,
                overlay_removed,
                backend_account_removed,
                backend_storage_slots = slots_removed,
                "purged account from both cache layers"
            );
        }
        // Layer 2 (account + storage) changed for this address → invalidate base.
        self.mark_base_dirty(addr);
        (slots_removed, account_removed)
    }

    /// Get the chain ID used for EVM simulations (the `CHAINID` opcode).
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Set the chain ID reported to simulations via the `CHAINID` opcode.
    ///
    /// Prefer setting this at construction through
    /// [`EvmCacheBuilder::chain_id`]. This setter exists for cases where the
    /// chain ID must change after construction. It takes effect on the next
    /// [`snapshot`](Self::snapshot) / `build_evm`; existing
    /// snapshots and overlays keep the chain ID captured when they were created.
    pub fn set_chain_id(&mut self, chain_id: u64) {
        self.chain_id = chain_id;
    }

    /// Take a low-level, same-thread checkpoint of the CacheDB overlay for
    /// in-place restore.
    ///
    /// Clones the inner [`revm::database::Cache`] (the layer-1 overlay's
    /// accounts and storage) only — not the underlying database wrapper or the
    /// BlockchainDb backend. Pair with [`restore`](Self::restore) to roll the
    /// overlay back on the same `EvmCache` after speculative mutations (this is
    /// how the balance-slot scan probes and rewinds).
    ///
    /// For cross-thread fan-out use [`snapshot`](Self::snapshot)
    /// instead: it merges both layers into an `Arc<`[`EvmSnapshot`]`>` that is
    /// `Send + Sync` and can be shared with parallel simulators via
    /// [`EvmOverlay`].
    pub fn checkpoint(&self) -> revm::database::Cache {
        self.db.cache.clone()
    }

    /// Restore the CacheDB overlay from a checkpoint taken with
    /// [`checkpoint`](Self::checkpoint).
    ///
    /// Overwrites the layer-1 overlay wholesale with `checkpoint`, discarding any
    /// overlay mutations made since it was taken. The BlockchainDb backend is
    /// untouched. This is the in-place counterpart to the cross-thread
    /// [`snapshot`](Self::snapshot) / [`EvmOverlay`] path.
    pub fn restore(&mut self, checkpoint: revm::database::Cache) {
        self.db.cache = checkpoint;
    }

    /// Create a new session for executing multiple operations.
    ///
    /// Changes made within the session are only committed to the underlying database
    /// when `session.commit()` is called. Dropping the session without calling commit
    /// discards all changes made during the session.
    pub fn session(&mut self) -> EvmSession<'_> {
        EvmSession {
            evm: self.build_evm(),
        }
    }

    /// Create an immutable, `Send + Sync` snapshot of the current EVM state for
    /// cross-thread fan-out (the copy-on-write two-tier view, Pillar A).
    ///
    /// Rather than deep-copying both layers, this memoizes the cold layer-2
    /// (`BlockchainDb`) index as an `Arc`-shared base — reused as a cheap
    /// `Arc::clone` when layer 2 is unchanged, rebuilt copy-on-write only for the
    /// addresses that changed — and folds the hot layer-1 (`CacheDB` overlay)
    /// delta over it. Layer-1 values shadow the base on reads, reproducing the
    /// live cache's layered semantics; the resulting [`EvmSnapshot`] is shared
    /// across threads via `Arc`. Its cost tracks *changed* state, not *total*
    /// state. (The retained [`snapshot_deep_clone`](Self::snapshot_deep_clone)
    /// is the read-equivalent O(total) reference, kept for benchmarking/testing.)
    ///
    /// Takes `&mut self` because it refreshes and memoizes the base. For cheap
    /// same-thread save/restore of just the overlay, prefer
    /// [`checkpoint`](Self::checkpoint) / [`restore`](Self::restore) instead.
    pub fn snapshot(&mut self) -> Arc<snapshot::EvmSnapshot> {
        // 1. Refresh / memoize the cold layer-2 base, then take a cheap Arc handle
        //    (O(1) when layer 2 is unchanged since the last snapshot).
        self.refresh_base();
        let base = Arc::clone(self.base.as_ref().expect("refresh_base sets base"));

        // 2. Fold layer 1 (the hot CacheDB overlay) into the snapshot's overlay
        //    maps + cleared/not-existing sets, applying the same classification as
        //    the legacy flatten (O(layer-1)).
        let mut overlay_accounts = HashMap::new();
        let mut overlay_storage = HashMap::new();
        let mut overlay_code_by_hash = HashMap::new();
        let mut storage_cleared = std::collections::HashSet::new();
        let mut accounts_not_existing = std::collections::HashSet::new();
        for (addr, db_account) in &self.db.cache.accounts {
            let not_existing = matches!(db_account.account_state, AccountState::NotExisting);
            let cleared =
                not_existing || matches!(db_account.account_state, AccountState::StorageCleared);

            // Account info. Mirror revm `DbAccount::info()` / `loaded_account_info`:
            // a NotExisting overlay account is absent to the EVM (`basic` returns
            // None), so it must NOT contribute info/code to the overlay — and
            // `accounts_not_existing` makes the read short-circuit to None before
            // ever consulting the base.
            if not_existing {
                accounts_not_existing.insert(*addr);
            } else {
                if let Some(code) = &db_account.info.code {
                    overlay_code_by_hash.insert(db_account.info.code_hash, code.clone());
                }
                overlay_accounts.insert(*addr, db_account.info.clone());
            }

            // Storage. A StorageCleared/NotExisting account's storage is locally
            // complete: the overlay holds ONLY its own slots (so a cleared account
            // ALWAYS gets an `overlay_storage` entry, possibly empty), an absent
            // slot reads ZERO via `storage_cleared`, and the base is never consulted
            // for it. A non-cleared overlay account contributes its slots; absent
            // slots fall through to the base on a read.
            if cleared {
                storage_cleared.insert(*addr);
                let account_storage: HashMap<U256, U256> =
                    db_account.storage.iter().map(|(k, v)| (*k, *v)).collect();
                overlay_storage.insert(*addr, account_storage);
            } else if !db_account.storage.is_empty() {
                let account_storage = overlay_storage.entry(*addr).or_default();
                for (slot, value) in &db_account.storage {
                    account_storage.insert(*slot, *value);
                }
            }
        }

        Arc::new(snapshot::EvmSnapshot {
            base,
            overlay_accounts,
            overlay_storage,
            overlay_code_by_hash,
            storage_cleared,
            accounts_not_existing,
            block_hashes: HashMap::new(),
            block_number: self.block_number,
            basefee: self.basefee,
            coinbase: self.coinbase,
            prevrandao: self.prevrandao,
            gas_limit: self.block_gas_limit,
            chain_id: self.chain_id,
            timestamp: self.timestamp_override,
            spec_id: self.spec_id,
            shared_memory_capacity: self.shared_memory_capacity,
        })
    }

    /// Force the next [`snapshot`](Self::snapshot) to rebuild the
    /// memoized copy-on-write base from scratch (Pillar A).
    ///
    /// The crate's own mutators keep the base honest automatically. This is the
    /// **escape-hatch re-honest hook**: call it after writing layer 2 directly
    /// through [`unchecked_blockchain_db`](Self::unchecked_blockchain_db) or
    /// [`unchecked_backend`](Self::unchecked_backend) — those bypass the write
    /// funnel, and in-place changes at unchanged cardinality are invisible to the
    /// snapshot growth scan.
    /// That includes overwriting an existing storage slot and changing an existing
    /// account's info/code/balance without adding a new account. Lazy RPC-populated
    /// data does not need this call because it only appends accounts/slots, which
    /// the growth scan catches.
    ///
    /// When using `SharedBackend::insert_or_update_*` through
    /// [`unchecked_backend`](Self::unchecked_backend), remember those helpers only
    /// enqueue a background update. Synchronize/read back the update through
    /// `SharedBackend` before the next snapshot; `invalidate_snapshot_base` alone
    /// is not a backend-handler synchronization point. Once the direct write is
    /// present, calling this before the next snapshot guarantees it reflects that
    /// write rather than a stale memoized value. Over-invalidation is always safe
    /// (Decision D2); the only cost is one full base rebuild on the next snapshot.
    pub fn invalidate_snapshot_base(&mut self) {
        self.invalidate_base();
    }

    /// Refresh the memoized cold layer-2 [`BaseState`](snapshot::BaseState),
    /// reusing the previous `Arc` wherever layer 2 is unchanged (Pillar A).
    ///
    /// Called at the top of [`snapshot`](Self::snapshot). It never
    /// mutates an `Arc<BaseState>` that may already be shared with a live
    /// snapshot: on any change it builds a *new* `BaseState` that shares the `Arc`
    /// handles of unchanged accounts and rebuilds only the changed ones
    /// (copy-on-write).
    ///
    /// Algorithm (see `docs/phase-5-spec.md` §2.3):
    /// 1. **Full rebuild** when there is no base yet or `base_full_rebuild` is set
    ///    (`set_block` / re-pin replaced layer 2): flatten all of layer 2.
    /// 2. **Detect uncontrolled growth**: a lazy RPC fetch / prefetch can write
    ///    layer 2 from inside `foundry-fork-db`, bypassing our write funnel. An
    ///    `O(accounts)` length-scan over the current layer-2 storage/accounts marks
    ///    any address whose slot count differs from the recorded length, or any
    ///    account absent from the base, as dirty.
    /// 3. **Nothing dirty** → reuse the existing `Arc<BaseState>` unchanged (the
    ///    common hot-loop case; the base side of `snapshot` is then O(1)).
    /// 4. **Some addresses dirty** → build a new `BaseState` sharing the `Arc`s of
    ///    unchanged accounts and rebuilding only the dirty ones.
    fn refresh_base(&mut self) {
        // Case 1: full rebuild.
        if self.base.is_none() || self.base_full_rebuild {
            self.base = Some(Arc::new(self.build_base_full()));
            self.base_dirty.clear();
            self.base_full_rebuild = false;
            return;
        }

        // Case 2: detect uncontrolled layer-2 growth via an O(accounts) length scan
        // (NOT an O(slots) value scan). Any address whose slot count changed, or any
        // account that newly appeared in layer 2, is folded into `base_dirty`.
        //
        // LOAD-BEARING INVARIANT: the count/absence scan is sufficient *only* because
        // the one uncontrolled layer-2 writer — the foundry-fork-db `SharedBackend`
        // lazy fetch — is append-only at a fixed block (its request handler answers an
        // already-cached account/slot from the store and only inserts on a miss; it
        // never overwrites an existing entry in place). So an uncontrolled fetch can
        // only add a new account (caught by the absence check) or a new slot (caught
        // by the count check). An in-place value overwrite at unchanged length is
        // invisible here; the controlled writers therefore call `mark_base_dirty`
        // explicitly, and a direct out-of-band write via `unchecked_blockchain_db()`/`unchecked_backend()`
        // must call `invalidate_snapshot_base`. If a future foundry-fork-db bump makes
        // the lazy path overwrite-in-place, this scan must gain a value/version check.
        {
            let db_storage = self.blockchain_db.storage().read();
            for (addr, slots) in db_storage.iter() {
                if self.base_storage_lens.get(addr).copied() != Some(slots.len()) {
                    self.base_dirty.insert(*addr);
                }
            }
            let db_accounts = self.blockchain_db.accounts().read();
            let base = self.base.as_ref().expect("base present in case 2/3/4");
            for addr in db_accounts.keys() {
                if !base.accounts.contains_key(addr) {
                    self.base_dirty.insert(*addr);
                }
            }
        }

        // Case 3: nothing changed → reuse the existing Arc unchanged.
        if self.base_dirty.is_empty() {
            return;
        }

        // Case 4: rebuild copy-on-write — clone the outer maps (Arc handles +
        // AccountInfo, no per-slot copy) and rebuild only the dirty addresses.
        let prev = self.base.as_ref().expect("base present in case 4");
        let mut accounts = prev.accounts.clone();
        let mut storage = prev.storage.clone();

        let db_accounts = self.blockchain_db.accounts().read();
        let db_storage = self.blockchain_db.storage().read();
        for addr in self.base_dirty.iter().copied() {
            // Account info: refresh from the current layer-2 account, or drop it if
            // the account no longer exists in layer 2 (e.g. after a purge).
            match db_accounts.get(&addr) {
                Some(info) => {
                    accounts.insert(addr, info.clone());
                }
                None => {
                    accounts.remove(&addr);
                }
            }

            // Storage: rebuild this account's Arc<HashMap> from the current layer-2
            // storage, or drop it if the account has no layer-2 storage anymore.
            match db_storage.get(&addr) {
                Some(slots) => {
                    let rebuilt: HashMap<U256, U256> =
                        slots.iter().map(|(k, v)| (*k, *v)).collect();
                    self.base_storage_lens.insert(addr, rebuilt.len());
                    storage.insert(addr, Arc::new(rebuilt));
                }
                None => {
                    storage.remove(&addr);
                    self.base_storage_lens.remove(&addr);
                }
            }
        }
        drop(db_accounts);
        drop(db_storage);

        // Rebuild the code index from the refreshed accounts (NOT cloned from the
        // previous base): a purged or recoded dirty account must not leave a stale
        // `code_by_hash` entry, which would diverge from `snapshot_deep_clone`
        // on a direct `code_by_hash(old_hash)` lookup. Rebuilding from scratch also
        // handles shared code hashes correctly (a hash survives iff some present
        // account still carries it).
        let code_by_hash = Self::code_index(&accounts);

        self.base = Some(Arc::new(snapshot::BaseState {
            accounts,
            storage,
            code_by_hash,
        }));
        self.base_dirty.clear();
    }

    /// Build the bytecode-by-hash index from a set of (layer-2) accounts, matching
    /// the deep-clone reference: a hash is present iff some account carries that
    /// code inline. Rebuilt from scratch on every base (re)build so a purged or
    /// recoded account never leaves a stale entry — preserving read-equivalence
    /// with [`snapshot_deep_clone`](Self::snapshot_deep_clone).
    fn code_index(accounts: &HashMap<Address, AccountInfo>) -> HashMap<B256, Bytecode> {
        accounts
            .values()
            .filter_map(|info| {
                info.code
                    .as_ref()
                    .map(|code| (info.code_hash, code.clone()))
            })
            .collect()
    }

    /// Build a fresh [`BaseState`](snapshot::BaseState) by flattening all of layer
    /// 2, recording `base_storage_lens`. Shared by `refresh_base`'s full-rebuild
    /// path and [`snapshot_deep_clone`](Self::snapshot_deep_clone).
    fn build_base_full(&mut self) -> snapshot::BaseState {
        let mut accounts = HashMap::new();
        {
            let db_accounts = self.blockchain_db.accounts().read();
            for (addr, info) in db_accounts.iter() {
                accounts.insert(*addr, info.clone());
            }
        }
        let code_by_hash = Self::code_index(&accounts);
        let mut storage = HashMap::new();
        self.base_storage_lens.clear();
        {
            let db_storage = self.blockchain_db.storage().read();
            for (addr, slots) in db_storage.iter() {
                let converted: HashMap<U256, U256> = slots.iter().map(|(k, v)| (*k, *v)).collect();
                self.base_storage_lens.insert(*addr, converted.len());
                storage.insert(*addr, Arc::new(converted));
            }
        }
        snapshot::BaseState {
            accounts,
            storage,
            code_by_hash,
        }
    }

    /// The retained deep-clone snapshot — today's full flatten, kept reachable for
    /// A/B benchmarking and as the read-equivalence reference (Decision D3).
    ///
    /// Produces the same two-tier [`EvmSnapshot`](snapshot::EvmSnapshot) shape as
    /// [`snapshot`](Self::snapshot), but with `base` set to the
    /// fully-merged flatten of **both** layers and **empty** overlay maps (the
    /// cleared / not-existing sets still in place). It is read-indistinguishable
    /// from `snapshot` by construction (the `tests/cow_snapshot.rs`
    /// differential gate pins this), at the cost of an O(total state) deep copy
    /// every call — exactly the cost `snapshot` now amortizes away.
    ///
    /// Stays `&self`: it does not touch the memoized base.
    #[doc(hidden)]
    pub fn snapshot_deep_clone(&self) -> Arc<snapshot::EvmSnapshot> {
        let mut accounts = HashMap::new();
        let mut storage: HashMap<Address, HashMap<U256, U256>> = HashMap::new();
        let mut code_by_hash = HashMap::new();

        // 1. Load from BlockchainDb (persistent cache / Layer 2).
        {
            let db_accounts = self.blockchain_db.accounts().read();
            for (addr, info) in db_accounts.iter() {
                if let Some(code) = &info.code {
                    code_by_hash.insert(info.code_hash, code.clone());
                }
                accounts.insert(*addr, info.clone());
            }
        }
        {
            let db_storage = self.blockchain_db.storage().read();
            for (addr, slots) in db_storage.iter() {
                let converted: HashMap<U256, U256> = slots.iter().map(|(k, v)| (*k, *v)).collect();
                storage.insert(*addr, converted);
            }
        }

        // 2. Overlay from CacheDB (Layer 1, takes precedence). Merge into the same
        //    flat maps, dropping shadowed entries, exactly as the original
        //    `snapshot` did. A cleared account's storage is routed into
        //    `overlay_storage` (not the base), because `EvmSnapshot::storage_value`
        //    only applies the cleared-as-ZERO rule for an address with an
        //    `overlay_storage` entry — so the cleared semantics must be expressed
        //    there for both snapshot constructors to read identically.
        let mut overlay_storage: HashMap<Address, HashMap<U256, U256>> = HashMap::new();
        let mut storage_cleared = std::collections::HashSet::new();
        let mut accounts_not_existing = std::collections::HashSet::new();
        for (addr, db_account) in &self.db.cache.accounts {
            let not_existing = matches!(db_account.account_state, AccountState::NotExisting);
            let cleared =
                not_existing || matches!(db_account.account_state, AccountState::StorageCleared);

            if not_existing {
                accounts_not_existing.insert(*addr);
                accounts.remove(addr);
            } else {
                if let Some(code) = &db_account.info.code {
                    code_by_hash.insert(db_account.info.code_hash, code.clone());
                }
                accounts.insert(*addr, db_account.info.clone());
            }

            if cleared {
                // Cleared: storage is locally complete. Drop any shadowed base
                // slots and keep ONLY the overlay slots, in `overlay_storage`.
                storage_cleared.insert(*addr);
                storage.remove(addr);
                let account_storage: HashMap<U256, U256> =
                    db_account.storage.iter().map(|(k, v)| (*k, *v)).collect();
                overlay_storage.insert(*addr, account_storage);
            } else {
                // Non-cleared: overlay slots win over base; fold them into base.
                let account_storage = storage.entry(*addr).or_default();
                for (slot, value) in &db_account.storage {
                    account_storage.insert(*slot, *value);
                }
            }
        }

        let base = snapshot::BaseState {
            accounts,
            storage: storage
                .into_iter()
                .map(|(addr, slots)| (addr, Arc::new(slots)))
                .collect(),
            code_by_hash,
        };

        Arc::new(snapshot::EvmSnapshot {
            base: Arc::new(base),
            overlay_accounts: HashMap::new(),
            overlay_storage,
            overlay_code_by_hash: HashMap::new(),
            storage_cleared,
            accounts_not_existing,
            block_hashes: HashMap::new(),
            block_number: self.block_number,
            basefee: self.basefee,
            coinbase: self.coinbase,
            prevrandao: self.prevrandao,
            gas_limit: self.block_gas_limit,
            chain_id: self.chain_id,
            timestamp: self.timestamp_override,
            spec_id: self.spec_id,
            shared_memory_capacity: self.shared_memory_capacity,
        })
    }

    /// Mark a layer-2 address dirty so the next [`refresh_base`](Self::refresh_base)
    /// re-folds it into the memoized base (Pillar A invalidation; see
    /// `docs/phase-5-spec.md` §3).
    ///
    /// Called from every site that can change a layer-2 value a snapshot read
    /// would surface (write-through, batch injects, layer-2 seeding, purges).
    /// Over-invalidation is safe (Decision D2): marking an address that is also
    /// shadowed by layer 1 just re-folds that one account.
    fn mark_base_dirty(&mut self, address: Address) {
        self.base_dirty.insert(address);
    }

    /// Force a full rebuild of the memoized base on the next
    /// [`refresh_base`](Self::refresh_base) (Pillar A invalidation).
    ///
    /// Used by layer-2 changes too broad to enumerate per-address efficiently
    /// (multi-contract / full-storage purges, block re-pins). Coarser than
    /// [`mark_base_dirty`](Self::mark_base_dirty) but always correct.
    fn invalidate_base(&mut self) {
        self.base_full_rebuild = true;
    }

    /// Update the block that RPC fetches are pinned to.
    ///
    /// This re-pins the SharedBackend and the batch storage fetcher to `block`,
    /// so subsequent RPC fetches read state at the new block.
    ///
    /// # Block-context contract
    /// To prevent the EVM block context from silently diverging from the pinned
    /// block, when `block` is a concrete `BlockId::Number(Number(n))` this also
    /// updates `block_number` (the `NUMBER` opcode) to `n`. For tag-based block
    /// ids (`latest`, `pending`, hashes, etc.), the height is not
    /// statically known, so `block_number` is cleared.
    ///
    /// `basefee` (the `BASEFEE` opcode) is **cleared on every block change** and
    /// on every non-concrete tag/hash pin call because deriving it requires
    /// fetching the block header, which this synchronous method cannot do. Callers
    /// that change blocks should refresh it via
    /// [`set_block_context`](Self::set_block_context) after fetching the new
    /// header. Prefer [`repin_to_block`](Self::repin_to_block) when re-pinning to
    /// a concrete height, since it keeps `block_number` and the pinned block in
    /// lockstep.
    pub fn set_block(&mut self, block: BlockId) {
        let changed = self.block != block;
        let concrete_number = match block {
            BlockId::Number(BlockNumberOrTag::Number(n)) => Some(n),
            _ => None,
        };
        if changed {
            self.block = block;
            self.bump_snapshot_generation();
            // Re-pinning replaces layer 2 wholesale (state at a new block): the
            // memoized base must be rebuilt from scratch on the next snapshot.
            self.invalidate_base();
            let _ = self.backend.set_pinned_block(block);
        }
        if changed || concrete_number.is_none() {
            self.basefee = None;
        }

        // Keep the EVM `NUMBER` opcode aligned with the pin. Only a concrete
        // height is meaningful; tags and hashes clear it so a stale number from
        // an earlier concrete block cannot leak into simulation.
        self.block_number = concrete_number;
    }

    /// Get the block that RPC fetches are currently pinned to.
    pub fn block(&self) -> BlockId {
        self.block
    }

    /// Monotonic generation counter for snapshot consistency (G6).
    ///
    /// Increments on every targeted state write ([`apply_update`](Self::apply_update),
    /// [`apply_updates`](Self::apply_updates), [`modify_slot`](Self::modify_slot)
    /// — and therefore everything built on them: reactive ingestion, freshness
    /// corrections, fresh injections) and on block re-pins
    /// ([`set_block`](Self::set_block), [`advance_block`](Self::advance_block)).
    /// Cold prefetch ([`inject_storage_batch`](Self::inject_storage_batch)) and
    /// lazy backend fetches do **not** increment it: they materialize the pinned
    /// block's existing state rather than changing it.
    ///
    /// The magnitude is opaque — how much one call increments it is
    /// unspecified — so compare values for **equality only**.
    ///
    /// The fan-out pattern: read the generation, take the
    /// [`snapshot`](Self::snapshot), read the generation again. If the two
    /// reads differ, state mutated in between (e.g. your event loop applied
    /// part of a block between the reads) — discard and re-snapshot to avoid
    /// fanning out simulations over a mid-block state.
    ///
    /// ```no_run
    /// # fn demo(cache: &mut evm_fork_cache::cache::EvmCache) {
    /// let snapshot = loop {
    ///     let generation = cache.snapshot_generation();
    ///     let snapshot = cache.snapshot();
    ///     if cache.snapshot_generation() == generation {
    ///         break snapshot;
    ///     }
    ///     // A mutation interleaved: try again at the next stable point.
    /// };
    /// # let _ = snapshot;
    /// # }
    /// ```
    pub fn snapshot_generation(&self) -> u64 {
        self.snapshot_generation
    }

    /// Advance the snapshot-consistency generation (see
    /// [`snapshot_generation`](Self::snapshot_generation)).
    fn bump_snapshot_generation(&mut self) {
        self.snapshot_generation = self.snapshot_generation.wrapping_add(1);
    }

    /// Set a custom timestamp for EVM simulations.
    ///
    /// When set, all EVM executions will use this timestamp instead of the current
    /// system time. This is useful for simulating future blocks to predict when
    /// time-dependent opportunities (like yield farming rewards) become profitable.
    ///
    /// Pass `None` to use the current system time (default behavior).
    pub fn set_timestamp(&mut self, timestamp: Option<u64>) {
        self.timestamp_override = timestamp;
    }

    /// Get the current timestamp override, if any.
    ///
    /// Returns `None` if the cache is using the current system time (default).
    pub fn timestamp(&self) -> Option<u64> {
        self.timestamp_override
    }

    /// Get the block number used for EVM simulations (the `NUMBER` opcode).
    ///
    /// Fetched from the pinned block's header at construction. Concrete-number
    /// pins set it via [`set_block`](Self::set_block) /
    /// [`repin_to_block`](Self::repin_to_block); tag/hash pins clear it
    /// because their height is not statically known. `None` means revm falls back
    /// to `0`, which can steer contracts that branch on `block.number` down a
    /// different code path. Override directly via
    /// [`set_block_context`](Self::set_block_context).
    pub fn block_number(&self) -> Option<u64> {
        self.block_number
    }

    /// Get the base fee per gas used for EVM simulations (the `BASEFEE` opcode).
    ///
    /// Fetched from the pinned block's header at construction. `None` means
    /// revm falls back to `0`. This is cleared by [`set_block`](Self::set_block)
    /// / [`repin_to_block`](Self::repin_to_block) when the pin changes, and by
    /// non-concrete tag/hash pin calls because those can drift without a
    /// concrete number in the API. Refresh it with
    /// [`set_block_context`](Self::set_block_context) after fetching a new header
    /// if `BASEFEE` accuracy matters.
    pub fn basefee(&self) -> Option<u64> {
        self.basefee
    }

    /// Update the block context for EVM simulations.
    ///
    /// Call this when the simulation block changes (e.g. at the start of each
    /// search cycle) to keep NUMBER and BASEFEE opcodes accurate.
    pub fn set_block_context(&mut self, block_number: Option<u64>, basefee: Option<u64>) {
        self.block_number = block_number;
        self.basefee = basefee;
    }

    /// Set the block base fee (the `BASEFEE` opcode) for subsequent simulations,
    /// propagated into the next [`snapshot`](Self::snapshot).
    ///
    /// Offline caches built over a mocked provider have no fetched block header,
    /// so the base fee is unset (and the `BASEFEE` opcode reads `0`). Use this to
    /// install one explicitly — it determines the priority fee
    /// (`gas_price − basefee`) credited to the beneficiary, and thus the
    /// `coinbase_payment` a [`simulate_bundle`](Self::simulate_bundle) reports.
    ///
    /// The cache stores the base fee as a `u64` (matching the block header and the
    /// `EvmSnapshot` field), so a `U256` larger than `u64::MAX` is saturated.
    pub fn set_basefee(&mut self, basefee: U256) {
        self.basefee = Some(basefee.saturating_to::<u64>());
    }

    /// Override the block beneficiary (the `COINBASE` opcode) for subsequent
    /// simulations.
    ///
    /// Set this when simulating logic that reads `block.coinbase` (e.g.
    /// MEV/builder tip accounting). `None` lets revm use its default beneficiary.
    pub fn set_coinbase(&mut self, coinbase: Option<Address>) {
        self.coinbase = coinbase;
    }

    /// Override `prevrandao` (the `PREVRANDAO` opcode, the post-merge header mix
    /// hash) for subsequent simulations.
    ///
    /// Set this when reproducing contracts that source on-chain randomness from
    /// `block.prevrandao`. `None` leaves revm's default in place.
    pub fn set_prevrandao(&mut self, prevrandao: Option<B256>) {
        self.prevrandao = prevrandao;
    }

    /// Override the block gas limit (the `GASLIMIT` opcode) for subsequent
    /// simulations.
    ///
    /// Set this when simulating logic that reads `block.gaslimit`. `None` lets
    /// revm use its default.
    pub fn set_block_gas_limit(&mut self, gas_limit: Option<u64>) {
        self.block_gas_limit = gas_limit;
    }

    /// Get the block beneficiary used for EVM simulations (the `COINBASE`
    /// opcode).
    ///
    /// Fetched from the pinned block's header at construction, refreshed by
    /// [`advance_block`](Self::advance_block), or overridden via
    /// [`set_coinbase`](Self::set_coinbase). `None` means revm uses its default
    /// beneficiary.
    pub fn coinbase(&self) -> Option<Address> {
        self.coinbase
    }

    /// Get `prevrandao` used for EVM simulations (the `PREVRANDAO` opcode, the
    /// post-merge header mix hash).
    ///
    /// Fetched from the pinned block's header at construction, refreshed by
    /// [`advance_block`](Self::advance_block), or overridden via
    /// [`set_prevrandao`](Self::set_prevrandao). `None` leaves revm's default in
    /// place.
    pub fn prevrandao(&self) -> Option<B256> {
        self.prevrandao
    }

    /// Get the block gas limit used for EVM simulations (the `GASLIMIT` opcode).
    ///
    /// Fetched from the pinned block's header at construction, refreshed by
    /// [`advance_block`](Self::advance_block), or overridden via
    /// [`set_block_gas_limit`](Self::set_block_gas_limit). `None` lets revm use
    /// its default.
    pub fn block_gas_limit(&self) -> Option<u64> {
        self.block_gas_limit
    }

    /// Set which block-context header fields subsequent
    /// [`advance_block`](Self::advance_block) calls require to be present.
    ///
    /// See [`BlockContextRequirements`]. Under
    /// [`strict`](BlockContextRequirements::strict) enforcement,
    /// [`advance_block`](Self::advance_block) rejects a header missing a required
    /// field rather than silently defaulting it.
    pub fn set_block_context_requirements(&mut self, reqs: BlockContextRequirements) {
        self.block_context_requirements = reqs;
    }

    /// Engine-driven per-block env refresh from a canonical block header.
    ///
    /// First validates the header against the configured
    /// [`BlockContextRequirements`] (set via
    /// [`set_block_context_requirements`](Self::set_block_context_requirements)
    /// or the strict builder path). Under strict/partial requirements a header
    /// missing a required field is rejected with [`BlockContextError`] instead of
    /// being silently defaulted; under the [`lenient`](BlockContextRequirements::lenient)
    /// default this never errors.
    ///
    /// On success it refreshes the full EVM block env from the header — block
    /// number (`NUMBER`), base fee (`BASEFEE`), beneficiary (`COINBASE`),
    /// `prevrandao` (`PREVRANDAO`), gas limit (`GASLIMIT`) and timestamp — and
    /// re-pins **every** RPC fetch path (the SharedBackend lazy fallback, the
    /// batch storage fetcher, and the account-proof fetcher) to the header's
    /// block number, so a lazy miss after the advance reads state at the
    /// advanced block, in lockstep with the env. Intended to be driven once per
    /// canonical block (e.g. by the reactive runtime as new headers arrive).
    ///
    /// Unlike [`set_block`](Self::set_block), this does **not** invalidate the
    /// memoized COW snapshot base: an advance is a forward roll of the same live
    /// view (canonical mutations flow through the write funnel, which already
    /// marks the base dirty; lazy fetches stay insert-only-on-miss, which the
    /// base's growth scan catches), whereas `set_block` is a wholesale re-fork
    /// that must rebuild layer 2. Re-pinning to an *older* block is a re-fork,
    /// not an advance — use `set_block` for that.
    pub fn advance_block<H: BlockHeader>(&mut self, header: &H) -> Result<(), BlockContextError> {
        self.block_context_requirements.validate_header(header)?;

        self.block_number = Some(header.number());
        self.basefee = header.base_fee_per_gas();
        self.coinbase = Some(header.beneficiary());
        self.prevrandao = header.mix_hash();
        self.block_gas_limit = Some(header.gas_limit());
        self.timestamp_override = Some(header.timestamp());

        // Advance every fetch path to the new height in lockstep with the env:
        // the SharedBackend lazy fallback (a miss must not serve state from the
        // previously pinned block) and the pin accessor. Mirrors `set_block`'s
        // pin updates, minus the base invalidation (see the method docs for why
        // an advance keeps the memoized base).
        let block = BlockId::number(header.number());
        self.block = block;
        let _ = self.backend.set_pinned_block(block);
        // A snapshot spanning the env refresh would pair the new block context
        // with pre-advance state — bump so consumers can detect it (G6).
        self.bump_snapshot_generation();

        Ok(())
    }

    /// Re-pin the cache to a specific block number.
    ///
    /// Updates the SharedBackend pinned block, the batch fetcher block, and the
    /// EVM block context (`NUMBER` opcode) in lockstep. The current `basefee` is
    /// cleared because it cannot be refreshed synchronously; callers should set it
    /// via [`set_block_context`](Self::set_block_context) after fetching the new
    /// block header if `BASEFEE` accuracy matters.
    pub fn repin_to_block(&mut self, block_number: u64) {
        let old_block = self.block;
        self.set_block(BlockId::Number(block_number.into()));

        if let BlockId::Number(BlockNumberOrTag::Number(old_num)) = old_block {
            let drift = block_number.saturating_sub(old_num);
            if drift > 0 {
                debug!(
                    old_block = old_num,
                    new_block = block_number,
                    drift,
                    "Re-pinned cache to current block"
                );
            }
        }
    }

    /// Ensure an account is loaded into the cache.
    ///
    /// With the lazy-loading backend, this is optional - accounts are fetched
    /// automatically when accessed. However, you can use this to pre-warm
    /// the cache for specific accounts.
    #[instrument(level = "trace", skip(self))]
    pub async fn ensure_account(&mut self, address: Address) -> Result<()> {
        if self.db.cache.accounts.contains_key(&address) {
            return Ok(());
        }

        // Load account info via SharedBackend (fetches from RPC if not cached).
        // basic_ref populates BlockchainDb; we also insert into the CacheDB
        // overlay so the account is immediately available for direct reads.
        use revm::database_interface::DatabaseRef;
        let info = self
            .backend
            .basic_ref(address)
            .map_err(|e| CacheError::AccountFetch {
                address,
                details: format!("{e:?}"),
            })?;

        if let Some(info) = info {
            self.db.insert_account_info(address, info);
        }

        Ok(())
    }

    /// Read a single storage slot through the SharedBackend (BlockchainDb -> RPC fallback).
    ///
    /// After `purge_contract_slots` removes a slot from BlockchainDb, this method fetches
    /// fresh data from RPC and caches it in BlockchainDb. Subsequent EVM SLOADs find
    /// the value there without additional RPC calls.
    pub fn read_storage_slot(&mut self, address: Address, slot: U256) -> Result<U256> {
        use revm::database_interface::DatabaseRef;
        self.backend
            .storage_ref(address, slot)
            .map_err(|e| CacheError::StorageRead {
                address,
                slot,
                details: e.to_string(),
            })
    }

    /// Write a raw storage slot value directly into the CacheDB layer.
    ///
    /// Subsequent EVM SLOADs for this (address, slot) will read the injected value
    /// without any RPC call. Used for hot-state injection where we already know the
    /// current on-chain value from WebSocket events.
    pub fn insert_storage_slot(&mut self, address: Address, slot: U256, value: U256) -> Result<()> {
        self.db
            .insert_account_storage(address, slot, value)
            .map_err(|e| CacheError::StorageInsert {
                address,
                slot,
                details: e.to_string(),
            })?;
        Ok(())
    }

    /// Pre-seed known ERC20 `balanceOf` mapping base slots, keyed by token.
    ///
    /// Each `(token, slot)` records the storage slot of the token's
    /// `mapping(address => uint256) balances`, letting
    /// [`set_erc20_balance_with_slot_scan`](Self::set_erc20_balance_with_slot_scan)
    /// skip its discovery pass for that token and write the balance directly.
    /// Seeded slots are assumed to use Solidity's `keccak(key‖slot)` layout
    /// (use [`seed_erc20_balance_layouts`](Self::seed_erc20_balance_layouts) for
    /// Vyper/Solady tokens). Seeding a wrong slot is self-correcting: the write
    /// is verified and a fresh discovery pass runs (evicting the bad seed) if it
    /// fails. Later entries overwrite earlier ones for the same token.
    pub fn seed_erc20_balance_slots(&mut self, slots: impl IntoIterator<Item = (Address, U256)>) {
        for (token, slot) in slots {
            self.erc20_balance_slots.insert(
                token,
                TrackedMapping::new(token, slot, SlotLayout::SolidityMapping),
            );
        }
    }

    /// Pre-seed known ERC20 balance mapping *descriptors* (base slot **and**
    /// layout), keyed by [`TrackedMapping::contract`].
    ///
    /// The layout-aware companion to
    /// [`seed_erc20_balance_slots`](Self::seed_erc20_balance_slots): use this for
    /// Vyper (`keccak(slot‖key)`) or Solady (packed) tokens whose layout is known
    /// up front, so [`set_erc20_balance_with_slot_scan`](Self::set_erc20_balance_with_slot_scan)
    /// writes the correct slot without a discovery pass.
    pub fn seed_erc20_balance_layouts(
        &mut self,
        mappings: impl IntoIterator<Item = TrackedMapping>,
    ) {
        for tracked in mappings {
            self.erc20_balance_slots.insert(tracked.contract, tracked);
        }
    }

    /// Write a value into a Solidity `mapping(address => ...)` entry on
    /// `contract`, at the mapping declared at base slot `slot`.
    ///
    /// Computes the entry's storage key as
    /// `keccak256(abi.encode(slot_address, slot))` — Solidity's layout for an
    /// address-keyed mapping — and writes `value` there in the CacheDB overlay.
    /// Used to forge ERC20 balances and allowances without an on-chain transfer.
    ///
    /// # Errors
    /// Returns an error if the underlying CacheDB storage insert fails (e.g. the
    /// account cannot be loaded from the backend).
    pub fn insert_mapping_storage_slot(
        &mut self,
        contract: Address,
        slot: U256,
        slot_address: Address,
        value: U256,
    ) -> Result<()> {
        let hashed_balance_slot = keccak256((slot_address, slot).abi_encode());
        self.db
            .insert_account_storage(contract, hashed_balance_slot.into(), value)
            .map_err(|e| CacheError::StorageInsert {
                address: contract,
                slot: hashed_balance_slot.into(),
                details: e.to_string(),
            })?;
        Ok(())
    }

    /// Run a call with a composable [`revm::Inspector`] attached, **without
    /// committing** state (the journal is reverted after execution), returning
    /// the execution result and the inspector moved back out so you can read what
    /// it captured.
    ///
    /// This is the cache-level counterpart to
    /// [`EvmOverlay::call_raw_with_inspector`](crate::cache::EvmOverlay::call_raw_with_inspector).
    /// Unlike the overlay form it runs directly against the cache's database, so
    /// any missing state is fetched lazily during the call — discovery works on a
    /// cold fork with no pre-warming.
    pub fn call_raw_with_inspector<I>(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        tx: &TxConfig,
        inspector: I,
    ) -> Result<(ExecutionResult, I)>
    where
        I: for<'a> revm::Inspector<
                Context<
                    BlockEnv,
                    TxEnv,
                    CfgEnv,
                    &'a mut ForkCacheDB,
                    Journal<&'a mut ForkCacheDB>,
                    (),
                >,
            >,
    {
        let tx_env = Self::build_tx_env_with(from, to, calldata, tx)?;
        let mut evm = self.build_evm_with_inspector(inspector);
        let checkpoint = evm.journaled_state.checkpoint();
        let result = evm.inspect_one_tx(tx_env);
        evm.journaled_state.checkpoint_revert(checkpoint);
        let inspector = evm.inspector;
        result.map(|r| (r, inspector)).map_err(CacheError::transact)
    }

    /// Discover every hash-derived storage slot a call reads, factored into
    /// [`HashSlotAccess`]es (mapping keys, base slot, layout, exact slot, value).
    ///
    /// This is the general, ERC-20-agnostic entry point: it works for any mapping
    /// (balances, allowances, protocol positions, …) and any layout
    /// (Solidity / Vyper / Solady / nested), in a single simulation.
    /// `known_keys` are words — typically addresses via [`Address::into_word`] —
    /// used to anchor key/slot disambiguation; pass `&[]` to rely on the
    /// magnitude heuristic.
    ///
    /// # Limitations
    ///
    /// Discovery only sees slots the call actually `SLOAD`s. A getter that
    /// returns a *computed* value without reading a per-key backing slot — a
    /// rebasing balance derived from shares (stETH, Aave aTokens), or a value
    /// served purely from memory/calldata — yields no matching access. Callers
    /// that need a slot regardless (e.g. `set_erc20_balance_with_slot_scan`)
    /// fall through to a brute-force fallback rather than acting on a wrong slot.
    pub fn trace_hashed_slots(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        known_keys: &[B256],
    ) -> Result<Vec<HashSlotAccess>> {
        let (_result, probe) = self.call_raw_with_inspector(
            from,
            to,
            calldata,
            &TxConfig::default(),
            HashStorageProbe::new(),
        )?;
        Ok(probe.accesses(known_keys))
    }

    /// Discover a token's `balanceOf` mapping slot for `owner` from a single
    /// simulated call — layout-agnostic (Solidity / Vyper / Solady), with no
    /// `max_slot` bound and no repeated probing.
    ///
    /// Returns the [`HashSlotAccess`] whose loaded value matches the getter's
    /// return and whose key is `owner`, or `None` if the token exposes no hashed
    /// balance read — a rebasing/computed getter that does not `SLOAD` a
    /// per-owner slot (stETH, aTokens), or unavailable code. Capture the
    /// result with [`HashSlotAccess::as_tracked`] to reuse the layout for other
    /// holders without re-simulating.
    pub fn discover_erc20_balance_slot(
        &mut self,
        token: Address,
        owner: Address,
    ) -> Result<Option<HashSlotAccess>> {
        let calldata = Bytes::from(IERC20::balanceOfCall { target: owner }.abi_encode());
        let (result, probe) = self.call_raw_with_inspector(
            owner,
            token,
            calldata,
            &TxConfig::default(),
            HashStorageProbe::new(),
        )?;
        let ret = match result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            _ => return Ok(None),
        };
        let ret_val = if ret.len() >= 32 {
            U256::from_be_slice(&ret[..32])
        } else {
            U256::from_be_slice(&ret)
        };
        let owner_word = owner.into_word();
        // Prefer the hit whose value equals the return, then higher confidence,
        // then the shallowest derivation.
        let best = probe
            .accesses(&[owner_word])
            .into_iter()
            .filter(|a| a.keyed_by(owner_word))
            .max_by_key(|a| (a.value == ret_val, a.confidence, std::cmp::Reverse(a.depth)));
        Ok(best)
    }

    /// Derive a token's balance mapping layout once, then compute the exact
    /// storage slot for each of `holders` — the "discover, then track these
    /// addresses" primitive.
    ///
    /// Reuses a cached/seeded [`TrackedMapping`] for `token` if present;
    /// otherwise discovers it from a single `balanceOf` simulation (using the
    /// first holder as the probe) and caches it. Returns the reusable descriptor
    /// plus `(holder, slot)` pairs — feed the slots to a
    /// [`FreshnessRegistry`](crate::freshness::FreshnessRegistry)
    /// (`pin_slot`/`mark_volatile_slot`) or a
    /// [`PrefetchRegistry`](crate::prefetch_registry::PrefetchRegistry) to keep
    /// them warm and fresh. Returns `None` if the layout can't be discovered
    /// (e.g. an empty `holders` set with no cached descriptor, or a token with no
    /// hashed balance read).
    pub fn track_erc20_balances(
        &mut self,
        token: Address,
        holders: impl IntoIterator<Item = Address>,
    ) -> Result<Option<TrackedBalances>> {
        let holders: Vec<Address> = holders.into_iter().collect();

        let tracked = if let Some(t) = self.erc20_balance_slots.get(&token).copied() {
            t
        } else {
            let Some(&probe_holder) = holders.first() else {
                return Ok(None);
            };
            let Some(tracked) = self
                .discover_erc20_balance_slot(token, probe_holder)?
                .and_then(|access| access.as_tracked(token))
            else {
                return Ok(None);
            };
            self.erc20_balance_slots.insert(token, tracked);
            tracked
        };

        let pairs = tracked
            .slots_for(holders.iter().map(|h| h.into_word()))
            .into_iter()
            .map(|(key, slot)| (Address::from_word(key), slot))
            .collect();
        Ok(Some((tracked, pairs)))
    }

    /// Forge an ERC-20 allowance: discover the (nested) `allowance` mapping entry
    /// for `(owner, spender)` from a single traced `allowance` call, write
    /// `amount` to the exact slot, and verify.
    ///
    /// This is the approval counterpart to
    /// [`set_erc20_balance_with_slot_scan`](Self::set_erc20_balance_with_slot_scan)
    /// — newly feasible because nested-mapping discovery can locate
    /// `keccak(spender ‖ keccak(owner ‖ base))` (and its Vyper/packed variants)
    /// without a scan. Pass `U256::MAX` for an "unlimited" approval.
    ///
    /// Returns `Ok(true)` if set and verified, `Ok(false)` if the token exposes
    /// no discoverable hashed allowance entry keyed by `(owner, spender)`.
    pub fn set_erc20_allowance(
        &mut self,
        token: Address,
        owner: Address,
        spender: Address,
        amount: U256,
    ) -> Result<bool> {
        let calldata = Bytes::from(IERC20::allowanceCall { owner, spender }.abi_encode());
        let known = [owner.into_word(), spender.into_word()];
        let (owner_word, spender_word) = (owner.into_word(), spender.into_word());

        // The allowance entry is the hashed read keyed by BOTH owner and spender;
        // prefer the deepest/highest-confidence such access.
        let target = self
            .trace_hashed_slots(owner, token, calldata, &known)?
            .into_iter()
            .filter(|a| a.keyed_by(owner_word) && a.keyed_by(spender_word))
            .max_by_key(|a| (a.depth, a.confidence));

        let Some(target) = target else {
            return Ok(false);
        };

        self.insert_storage_slot(token, U256::from_be_slice(target.slot.as_slice()), amount)?;
        Ok(self.erc20_allowance(token, owner, spender)? == amount)
    }

    /// Write `value` into a mapping entry using a **discovered**
    /// [`TrackedMapping`] layout, returning the exact storage slot written.
    ///
    /// Unlike [`insert_mapping_storage_slot`](Self::insert_mapping_storage_slot),
    /// which always assumes Solidity `keccak(key‖slot)` order, this honors the
    /// tracked layout, so it writes the correct slot for Vyper and Solady tokens
    /// too. The `(contract, layout, base slot)` all come from the `tracked`
    /// descriptor.
    pub fn write_mapping_entry(
        &mut self,
        tracked: &TrackedMapping,
        key: B256,
        value: U256,
    ) -> Result<B256> {
        let slot = tracked
            .slot_for(key)
            .ok_or_else(|| CacheError::StorageInsert {
                address: tracked.contract,
                slot: U256::ZERO,
                details: format!(
                    "layout {} does not support single-key slot derivation",
                    tracked.layout
                ),
            })?;
        self.insert_storage_slot(
            tracked.contract,
            U256::from_be_slice(slot.as_slice()),
            value,
        )?;
        Ok(slot)
    }

    /// Create a throwaway [`EvmOverlay`] over the current snapshot, wired to this
    /// cache's backend for lazy fetch.
    ///
    /// This is the entry point for **overlay-scoped mocking**: mock balances,
    /// approvals, and getter returns on the returned overlay
    /// ([`EvmOverlay::mock_balance`], [`EvmOverlay::mock_allowance`],
    /// [`EvmOverlay::mock_call`]) and run your simulations *on that overlay*. The
    /// mocks live only in the overlay's dirty layer and are dropped with it — the
    /// cache is never mutated, so a mocked balance can't leak into a later
    /// simulation. (For a persistent cache-level balance override, use
    /// [`set_erc20_balance_with_slot_scan`](Self::set_erc20_balance_with_slot_scan).)
    ///
    /// ```no_run
    /// # use alloy_primitives::{Address, U256};
    /// # use evm_fork_cache::cache::{EvmCache, TxConfig};
    /// # use alloy_primitives::Bytes;
    /// # fn ex(cache: &mut EvmCache, usdc: Address, alice: Address, router: Address, swap: Bytes)
    /// #     -> Result<(), Box<dyn std::error::Error>> {
    /// let mut sim = cache.mock_overlay();
    /// sim.mock_balance(usdc, alice, U256::from(1_000_000_000_000u64))?; // 1M USDC (6 dp)
    /// sim.mock_allowance(usdc, alice, router, U256::MAX)?;              // unlimited approve
    /// let out = sim.call_raw(alice, router, swap)?;  // simulate against the mocked state
    /// // drop `sim` → mocks discarded; `cache` is untouched.
    /// # let _ = out; Ok(()) }
    /// ```
    pub fn mock_overlay(&mut self) -> EvmOverlay {
        EvmOverlay::new(self.snapshot(), Some(self.backend.clone()))
    }

    /// Set an ERC20 balance, discovering the token's balance mapping slot and
    /// layout if not already known, then writing `amount` there.
    ///
    /// Resolution order:
    /// 1. A cached/seeded [`TrackedMapping`] for the token (fast path).
    /// 2. **Trace-based discovery** — a single simulated `balanceOf(owner)`,
    ///    layout-agnostic (Solidity / Vyper / Solady) and unbounded by `max_slot`
    ///    (see [`discover_erc20_balance_slot`](Self::discover_erc20_balance_slot)).
    /// 3. The legacy brute-force **scan** of `0..=max_slot` (Solidity order only),
    ///    kept as a fallback for the rare token whose `balanceOf` reads no hashed
    ///    slot the trace can attribute.
    ///
    /// Every path verifies the write via `balanceOf` before caching, so a wrong
    /// guess is self-correcting. Returns `Ok(true)` if set and verified,
    /// `Ok(false)` if nothing worked, and `Err` on EVM/cache failures.
    pub fn set_erc20_balance_with_slot_scan(
        &mut self,
        token: Address,
        owner: Address,
        amount: U256,
        max_slot: u16,
    ) -> Result<bool> {
        let owner_word = owner.into_word();

        // 1. Cached/seeded descriptor — write with its (layout-aware) slot.
        if let Some(tracked) = self.erc20_balance_slots.get(&token).copied() {
            self.write_mapping_entry(&tracked, owner_word, amount)?;
            if self.erc20_balance_of(token, owner)? == amount {
                return Ok(true);
            }
            self.erc20_balance_slots.remove(&token);
        }

        // 2. Trace-based discovery: one sim, layout-aware, no max_slot bound.
        if let Some(tracked) = self
            .discover_erc20_balance_slot(token, owner)?
            .and_then(|access| access.as_tracked(token))
        {
            self.write_mapping_entry(&tracked, owner_word, amount)?;
            if self.erc20_balance_of(token, owner)? == amount {
                self.erc20_balance_slots.insert(token, tracked);
                return Ok(true);
            }
            // Discovered slot didn't drive the balance (rebasing/computed getter,
            // or a same-key non-balance mapping): fall through to the scan.
        }

        // 3. Legacy brute-force scan (Solidity order only) as a last resort.
        let Some(discovered_slot) =
            self.discover_erc20_balance_slot_with_scan(token, owner, max_slot)?
        else {
            return Ok(false);
        };

        let tracked = TrackedMapping::new(token, discovered_slot, SlotLayout::SolidityMapping);
        self.write_mapping_entry(&tracked, owner_word, amount)?;
        let verified = self.erc20_balance_of(token, owner)? == amount;
        if verified {
            self.erc20_balance_slots.insert(token, tracked);
        } else {
            self.erc20_balance_slots.remove(&token);
        }
        Ok(verified)
    }

    fn discover_erc20_balance_slot_with_scan(
        &mut self,
        token: Address,
        owner: Address,
        max_slot: u16,
    ) -> Result<Option<U256>> {
        if let Some(tracked) = self.erc20_balance_slots.get(&token) {
            return Ok(Some(tracked.base_slot));
        }

        let baseline_snapshot = self.checkpoint();
        let baseline_balance = self.erc20_balance_of(token, owner)?;

        // Choose a probe value distinct from baseline to avoid false positives.
        let mut probe = U256::from(0xDEAD_BEEF_u64);
        if probe == baseline_balance {
            probe = baseline_balance.saturating_add(U256::from(1u64));
        }
        if probe == baseline_balance {
            probe = U256::MAX;
        }

        for slot_idx in 0..=max_slot {
            self.restore(baseline_snapshot.clone());
            let slot = U256::from(slot_idx);
            self.insert_mapping_storage_slot(token, slot, owner, probe)?;
            if self.erc20_balance_of(token, owner)? == probe {
                self.restore(baseline_snapshot);
                self.erc20_balance_slots.insert(
                    token,
                    TrackedMapping::new(token, slot, SlotLayout::SolidityMapping),
                );
                return Ok(Some(slot));
            }
        }

        self.restore(baseline_snapshot);
        Ok(None)
    }

    /// Execute a call with automatic account/storage fetching.
    ///
    /// Unlike the old implementation, this does NOT prefetch via access lists.
    /// The SharedBackend lazily fetches any missing data during execution.
    #[instrument(level = "debug", skip(self, calldata), fields(calldata_len = calldata.len()))]
    pub fn call(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        commit: bool,
    ) -> Result<ExecutionResult> {
        self.call_raw(from, to, calldata, commit)
    }

    /// Execute a call without any prefetching.
    ///
    /// Data is fetched lazily by the SharedBackend as needed during execution.
    #[instrument(level = "debug", skip(self, calldata), fields(calldata_len = calldata.len()))]
    pub fn call_raw(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        commit: bool,
    ) -> Result<ExecutionResult> {
        self.call_raw_with(from, to, calldata, commit, &TxConfig::default())
    }

    /// Execute a non-committing typed Solidity call from [`Address::ZERO`].
    ///
    /// This is the typed equivalent of encoding a [`SolCall`], passing it to
    /// [`call_raw`](Self::call_raw) with `commit = false`, and decoding the
    /// successful return data with [`SolCall::abi_decode_returns`].
    ///
    /// ```no_run
    /// # use alloy_primitives::Address;
    /// # use alloy_sol_types::sol;
    /// # use evm_fork_cache::cache::EvmCache;
    /// # fn example(cache: &mut EvmCache, token: Address, owner: Address) -> Result<(), Box<dyn std::error::Error>> {
    /// sol! {
    ///     function balanceOf(address account) external view returns (uint256);
    /// }
    ///
    /// let balance = cache.call_sol(token, balanceOfCall { account: owner })?;
    /// # let _ = balance;
    /// # Ok(())
    /// # }
    /// ```
    pub fn call_sol<C>(&mut self, to: Address, call: C) -> Result<C::Return>
    where
        C: SolCall,
    {
        self.call_sol_from(Address::ZERO, to, call)
    }

    /// Execute a non-committing typed Solidity call from an explicit sender.
    ///
    /// Uses the default [`TxConfig`], so native value, gas limit/price, nonce,
    /// and access list are left at the same defaults as [`call_raw`](Self::call_raw).
    pub fn call_sol_from<C>(&mut self, from: Address, to: Address, call: C) -> Result<C::Return>
    where
        C: SolCall,
    {
        self.call_sol_with_commit(from, to, call, &TxConfig::default(), false)
    }

    /// Execute a non-committing typed Solidity call with explicit tx overrides.
    ///
    /// This is the typed equivalent of [`call_raw_with`](Self::call_raw_with)
    /// with `commit = false`.
    pub fn call_sol_with<C>(
        &mut self,
        from: Address,
        to: Address,
        call: C,
        tx: &TxConfig,
    ) -> Result<C::Return>
    where
        C: SolCall,
    {
        self.call_sol_with_commit(from, to, call, tx, false)
    }

    /// Execute a typed Solidity call and commit its state changes.
    ///
    /// This is the typed equivalent of [`call_raw_with`](Self::call_raw_with)
    /// with `commit = true`; the call's state changes are persisted through the
    /// same path as the raw committing API before the return data is decoded.
    pub fn transact_sol<C>(
        &mut self,
        from: Address,
        to: Address,
        call: C,
        tx: &TxConfig,
    ) -> Result<C::Return>
    where
        C: SolCall,
    {
        self.call_sol_with_commit(from, to, call, tx, true)
    }

    /// Execute a call with explicit transaction-environment overrides
    /// ([`TxConfig`]): native `value`, gas limit/price, nonce, and an input
    /// access list. This is the entry point for value-bearing and gas-bounded
    /// simulation; [`call_raw`](Self::call_raw) is the zero-value shorthand.
    #[instrument(level = "debug", skip(self, calldata, tx), fields(calldata_len = calldata.len()))]
    pub fn call_raw_with(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        commit: bool,
        tx: &TxConfig,
    ) -> Result<ExecutionResult> {
        let tx_env = Self::build_tx_env_with(from, to, calldata, tx)?;
        let mut evm = self.build_evm();

        if commit {
            return evm.transact_commit(tx_env).map_err(CacheError::transact);
        }

        let checkpoint = evm.journaled_state.checkpoint();
        let result = evm.transact_one(tx_env);
        evm.journaled_state.checkpoint_revert(checkpoint);
        result.map_err(CacheError::transact)
    }

    /// Execute a non-committing call and extract the access list of touched
    /// accounts and storage slots before reverting.
    ///
    /// Used for EIP-2929 marginal gas estimation in batched simulations.
    pub fn call_raw_with_access_list(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
    ) -> Result<(ExecutionResult, StorageAccessList)> {
        let tx = Self::build_tx_env(from, to, calldata)?;
        let mut evm = self.build_evm();

        let checkpoint = evm.journaled_state.checkpoint();
        match evm.transact_one(tx) {
            Ok(result) => {
                // Extract access list from journaled state before reverting. After
                // transact_one, journaled_state.state holds all touched accounts/slots.
                let mut access_list = StorageAccessList::default();
                for (address, account) in evm.journaled_state.state.iter() {
                    if account.is_touched() {
                        access_list.accounts.insert(*address);
                        for slot_key in account.storage.keys() {
                            access_list.slots.insert((*address, *slot_key));
                        }
                    }
                }
                evm.journaled_state.checkpoint_revert(checkpoint);
                Ok((result, access_list))
            }
            Err(e) => {
                // Revert the checkpoint even on a host/transact error so the EVM
                // journal is not left dirty (mirrors `call_raw`).
                evm.journaled_state.checkpoint_revert(checkpoint);
                Err(CacheError::transact(e))
            }
        }
    }

    /// Execute a call and return its emitted logs and gas used.
    ///
    /// A thin wrapper over [`call`](Self::call) that requires success and
    /// discards the return data. When `commit` is true the call's state changes
    /// are persisted to the CacheDB overlay; otherwise they are reverted.
    ///
    /// # Errors
    /// Returns an error if the underlying transact fails, or if the call did not
    /// `Success` (i.e. it reverted or halted).
    pub fn call_logs(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        commit: bool,
    ) -> Result<(Vec<Log>, u64)> {
        let result = self.call(from, to, calldata, commit)?;
        if let ExecutionResult::Success { logs, gas_used, .. } = result {
            Ok((logs, gas_used))
        } else {
            Err(CacheError::CallNotSuccessful {
                result: format!("{result:?}"),
            })
        }
    }

    /// Read an ERC20 token balance by simulating a `balanceOf(owner)` call.
    ///
    /// Non-committing: the read is reverted, so it never mutates cache state.
    ///
    /// # Errors
    /// Returns an error if the simulated call fails or does not `Success` (e.g.
    /// `token` is not a contract or reverts), or if the returned data cannot be
    /// ABI-decoded as a `uint256`.
    pub fn erc20_balance_of(&mut self, token: Address, owner: Address) -> Result<U256> {
        let call = IERC20::balanceOfCall { target: owner };
        let result = self.call_raw(Address::ZERO, token, Bytes::from(call.abi_encode()), false)?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let balance = IERC20::balanceOfCall::abi_decode_returns(&out).map_err(|e| {
                    CacheError::Decode {
                        what: "ERC20 balanceOf return data",
                        details: format!("{e:?}"),
                    }
                })?;
                Ok(balance)
            }
            _ => Err(CacheError::CallNotSuccessful {
                result: format!("{result:?}"),
            }),
        }
    }

    /// Read an ERC20 allowance by simulating an `allowance(owner, spender)` call.
    ///
    /// Non-committing: the read is reverted, so it never mutates cache state.
    ///
    /// # Errors
    /// Returns an error if the simulated call fails or does not `Success` (e.g.
    /// `token` is not a contract or reverts), or if the returned data cannot be
    /// ABI-decoded as a `uint256`.
    pub fn erc20_allowance(
        &mut self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<U256> {
        let call = IERC20::allowanceCall { owner, spender };
        let result = self.call_raw(Address::ZERO, token, Bytes::from(call.abi_encode()), false)?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let allowance = IERC20::allowanceCall::abi_decode_returns(&out).map_err(|e| {
                    CacheError::Decode {
                        what: "ERC20 allowance return data",
                        details: format!("{e:?}"),
                    }
                })?;
                Ok(allowance)
            }
            _ => Err(CacheError::CallNotSuccessful {
                result: format!("{result:?}"),
            }),
        }
    }

    /// Read an ERC20 token's decimals by simulating a `decimals()` call.
    ///
    /// Memoized: a hit in the in-memory token-decimals map returns immediately
    /// without simulating. On a miss the value is resolved by a non-committing
    /// `decimals()` call.
    ///
    /// # Side effects
    /// On a miss the resolved value is cached in **both** the in-memory
    /// token-decimals map (process lifetime) **and** the immutable data cache
    /// (so it is persisted to disk on the next [`flush`](Self::flush)).
    ///
    /// # Errors
    /// Returns an error if the simulated call fails or does not `Success` (e.g.
    /// `token` is not a contract or reverts), or if the returned data cannot be
    /// ABI-decoded as a `uint8`.
    pub fn erc20_decimals(&mut self, token: Address) -> Result<u8> {
        if let Some(decimals) = self.token_decimals.get(&token) {
            return Ok(*decimals);
        }

        let call = IERC20::decimalsCall {};
        let result = self.call_raw(Address::ZERO, token, Bytes::from(call.abi_encode()), false)?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let decimals = IERC20::decimalsCall::abi_decode_returns(&out).map_err(|e| {
                    CacheError::Decode {
                        what: "ERC20 decimals return data",
                        details: format!("{e:?}"),
                    }
                })?;
                self.token_decimals.insert(token, decimals);
                // Also update immutable cache for persistence
                self.immutable_cache.set_token_decimals(token, decimals);
                Ok(decimals)
            }
            _ => Err(CacheError::CallNotSuccessful {
                result: format!("{result:?}"),
            }),
        }
    }

    /// Get a reference to the immutable data cache (token decimals).
    pub fn immutable_cache(&self) -> &ImmutableDataCache {
        &self.immutable_cache
    }

    /// Get a mutable reference to the immutable data cache.
    ///
    /// Use this to pre-populate token decimals that would otherwise be discovered
    /// lazily. Entries are persisted on the next [`flush`](Self::flush) (and on
    /// drop) when a [`CacheConfig`] is set.
    pub fn immutable_cache_mut(&mut self) -> &mut ImmutableDataCache {
        &mut self.immutable_cache
    }

    /// Check if an address has storage slots pre-loaded in the BlockchainDb.
    ///
    /// This is useful to determine if we loaded the EVM state from the unified
    /// `evm_state.bin` cache and an address already has reusable storage.
    ///
    /// # Arguments
    /// * `address` - The contract address to check
    ///
    /// # Returns
    /// `true` if the address has any storage slots in the underlying BlockchainDb,
    /// `false` otherwise
    pub fn has_contract_storage(&self, address: Address) -> bool {
        let storage = self.blockchain_db.storage().read();
        storage
            .get(&address)
            .map(|slots| !slots.is_empty())
            .unwrap_or(false)
    }

    /// Get the number of storage slots loaded for a contract address.
    ///
    /// Useful for debugging and logging to understand cache state.
    pub fn contract_storage_slot_count(&self, address: Address) -> usize {
        let storage = self.blockchain_db.storage().read();
        storage.get(&address).map(|slots| slots.len()).unwrap_or(0)
    }

    /// Get memory statistics for the shared memory buffer used during EVM simulations.
    ///
    /// Returns a tuple of (current_capacity_bytes, current_length_bytes).
    ///
    /// The capacity represents the high-water mark of memory usage across all
    /// simulations since the buffer grows but doesn't shrink. The length is
    /// typically 0 between simulations (cleared after each use).
    ///
    /// # Use Case
    /// Call this after running a batch of simulations to understand memory usage
    /// and inform the optimal initial capacity for `SharedMemory`.
    ///
    /// # Example
    /// ```ignore
    /// let (capacity, _len) = cache.shared_memory_stats();
    /// println!("Peak memory usage: {} KB", capacity / 1024);
    /// ```
    pub fn shared_memory_stats(&self) -> (usize, usize) {
        let buffer = self.shared_memory_buffer.borrow();
        (buffer.capacity(), buffer.len())
    }

    /// Log the current shared memory buffer statistics.
    ///
    /// Useful for profiling after running a batch of simulations.
    pub fn log_shared_memory_stats(&self) {
        let (capacity, len) = self.shared_memory_stats();
        debug!(
            capacity_bytes = capacity,
            capacity_kb = capacity / 1024,
            current_len = len,
            "Shared memory buffer stats (peak capacity across simulations)"
        );
    }

    /// Pre-allocate the shared memory buffer to a specific capacity.
    ///
    /// Use this after measuring peak usage to avoid reallocation overhead
    /// during simulations. The buffer will grow beyond this if needed,
    /// but pre-sizing to the expected peak eliminates allocations.
    ///
    /// # Arguments
    /// * `capacity` - The capacity in bytes to reserve
    ///
    /// # Example
    /// ```ignore
    /// // After profiling shows peak usage is ~32KB
    /// cache.reserve_shared_memory(32 * 1024);
    /// ```
    pub fn reserve_shared_memory(&mut self, capacity: usize) {
        let mut buffer = self.shared_memory_buffer.borrow_mut();
        let current_capacity = buffer.capacity();
        if current_capacity < capacity {
            buffer.reserve(capacity - current_capacity);
            debug!(
                new_capacity = buffer.capacity(),
                requested = capacity,
                "Reserved shared memory buffer capacity"
            );
        }
        drop(buffer);
        // Record the high-water mark so snapshots taken afterwards propagate it to
        // their overlays (snapshots copy the capacity at creation time).
        self.shared_memory_capacity = self.shared_memory_capacity.max(capacity);
    }

    /// The resolved per-context EVM shared-memory pre-allocation, in bytes.
    ///
    /// This is the [`SharedMemoryCapacity`] configured on the
    /// [`EvmCacheBuilder`] resolved to a concrete size (with
    /// [`SharedMemoryCapacity::Auto`] resolved against the state loaded at
    /// construction), raised by any later [`reserve_shared_memory`](Self::reserve_shared_memory).
    /// Each [`snapshot`](Self::snapshot) copies it onto the snapshot
    /// so snapshot-backed [`EvmOverlay`]s pre-allocate the same amount.
    pub fn shared_memory_capacity(&self) -> usize {
        self.shared_memory_capacity
    }

    /// The cache-side storage batch-fetch configuration for this instance.
    pub fn storage_batch_config(&self) -> StorageBatchConfig {
        self.storage_batch_config
    }

    /// Purge all storage slots for a specific contract from both cache layers.
    ///
    /// This clears:
    /// 1. **CacheDB overlay** (`self.db.cache.accounts[addr].storage`) - the in-memory
    ///    layer that caches storage slots fetched during EVM execution. Without clearing
    ///    this layer, subsequent EVM calls return stale values even after the backend
    ///    is purged.
    /// 2. **BlockchainDb backend** (`self.blockchain_db.storage()`) - the persistent
    ///    layer that caches RPC responses and is loaded from `evm_state.bin`.
    ///
    /// After purging both layers, the next EVM read for this contract's storage will
    /// go all the way to the RPC for fresh data.
    pub fn purge_contract_storage(&mut self, address: Address) -> usize {
        // Thin wrapper over the unified purge primitive; returns the backend slot
        // count the `AllStorage` scope removed.
        self.apply_update(&StateUpdate::purge(address, PurgeScope::AllStorage))
            .purged
            .first()
            .map(|rec| rec.slots_removed)
            .unwrap_or(0)
    }

    /// `AllStorage`-scope purge layer logic. Clears the overlay storage for
    /// `address` and removes its backend storage map. Returns the number of
    /// backend slots removed.
    fn purge_contract_storage_inner(&mut self, address: Address) -> usize {
        // Layer 1: Clear CacheDB overlay
        let cache_db_cleared = if let Some(db_account) = self.db.cache.accounts.get_mut(&address) {
            let count = db_account.storage.len();
            db_account.storage.clear();
            count
        } else {
            0
        };

        // Layer 2: Clear BlockchainDb backend
        let backend_cleared = {
            let mut storage = self.blockchain_db.storage().write();
            if let Some(slots) = storage.remove(&address) {
                slots.len()
            } else {
                0
            }
        };

        if cache_db_cleared > 0 || backend_cleared > 0 {
            debug!(
                contract = %address,
                cache_db_slots = cache_db_cleared,
                backend_slots = backend_cleared,
                "purged contract storage from both cache layers"
            );
        }

        // Layer-2 storage for this address was removed → invalidate base.
        self.mark_base_dirty(address);
        backend_cleared
    }

    /// Purge specific storage slots for a contract from both cache layers.
    ///
    /// Unlike `purge_contract_storage()` which removes ALL storage, this only removes
    /// the specified slots. This is useful when only a narrow subset of hot storage
    /// became stale and the rest of the contract's cached storage should be kept.
    ///
    /// Returns the number of slots removed from the BlockchainDb backend.
    pub fn purge_contract_slots(&mut self, address: Address, slots: &[U256]) -> usize {
        // Thin wrapper over the unified purge primitive; returns the backend slot
        // count the `Slots` scope removed.
        self.apply_update(&StateUpdate::purge(
            address,
            PurgeScope::Slots(slots.to_vec()),
        ))
        .purged
        .first()
        .map(|rec| rec.slots_removed)
        .unwrap_or(0)
    }

    /// `Slots`-scope purge layer logic. Removes the listed slots from the overlay
    /// and the backend storage map. Returns the number of backend slots removed.
    fn purge_contract_slots_inner(&mut self, address: Address, slots: &[U256]) -> usize {
        let mut cache_db_removed = 0usize;
        let mut backend_removed = 0usize;

        // Layer 1: Remove specific slots from CacheDB overlay
        if let Some(db_account) = self.db.cache.accounts.get_mut(&address) {
            for slot in slots {
                if db_account.storage.remove(slot).is_some() {
                    cache_db_removed += 1;
                }
            }
        }

        // Layer 2: Remove specific slots from BlockchainDb backend
        {
            let mut storage = self.blockchain_db.storage().write();
            if let Some(address_storage) = storage.get_mut(&address) {
                for slot in slots {
                    if address_storage.remove(slot).is_some() {
                        backend_removed += 1;
                    }
                }
            }
        }

        if cache_db_removed > 0 || backend_removed > 0 {
            trace!(
                contract = %address,
                requested = slots.len(),
                cache_db_removed,
                backend_removed,
                "selectively purged contract storage slots from both cache layers"
            );
        }

        // Layer-2 storage for this address changed (slots dropped) → invalidate
        // base. The growth scan only catches length changes; mark explicitly.
        self.mark_base_dirty(address);
        backend_removed
    }

    /// Purge storage slots for multiple contracts from both cache layers.
    ///
    /// See `purge_contract_storage()` for details on what each layer contains.
    pub fn purge_contracts_storage(
        &mut self,
        addresses: impl IntoIterator<Item = Address>,
    ) -> usize {
        let mut total_purged = 0usize;

        for address in addresses {
            // Layer 1: Clear CacheDB overlay
            if let Some(db_account) = self.db.cache.accounts.get_mut(&address) {
                db_account.storage.clear();
            }

            // Layer 2: Clear BlockchainDb backend
            let mut storage = self.blockchain_db.storage().write();
            if let Some(slots) = storage.remove(&address) {
                let count = slots.len();
                if count > 0 {
                    debug!(
                        contract = %address,
                        slots_removed = count,
                        "purged contract storage from both cache layers"
                    );
                }
                total_purged += count;
            }
        }

        if total_purged > 0 {
            debug!(
                total_slots_purged = total_purged,
                "purged contract storage from both cache layers"
            );
        }
        // Multiple layer-2 contracts changed → full base rebuild (coarse but
        // correct; cheaper than enumerating each touched address here).
        self.invalidate_base();
        total_purged
    }

    /// Purge ALL storage slots from both cache layers while preserving bytecodes.
    ///
    /// Use this for periodic full cache refresh (e.g., every 48 hours) to ensure
    /// any stale data like strategy swap paths, proxy implementations, reward rates,
    /// etc. are re-fetched from the actual on-chain state.
    ///
    /// This preserves:
    /// - Account info (nonce, balance, code hash)
    /// - Contract bytecodes (immutable)
    ///
    /// This purges:
    /// - All storage slots from CacheDB overlay (layer 1)
    /// - All storage slots from BlockchainDb backend (layer 2)
    ///
    /// # Returns
    /// The total number of storage slots that were removed from the BlockchainDb
    pub fn purge_all_storage(&mut self) -> usize {
        // Layer 1: Clear all storage in CacheDB overlay
        let mut cache_db_cleared = 0usize;
        for db_account in self.db.cache.accounts.values_mut() {
            cache_db_cleared += db_account.storage.len();
            db_account.storage.clear();
        }

        // Layer 2: Clear BlockchainDb backend
        let (total_slots, contract_count) = {
            let mut storage = self.blockchain_db.storage().write();
            let total_slots: usize = storage.values().map(|s| s.len()).sum();
            let contract_count = storage.len();
            storage.clear();
            (total_slots, contract_count)
        };

        if total_slots > 0 || cache_db_cleared > 0 {
            warn!(
                contracts_cleared = contract_count,
                backend_slots_purged = total_slots,
                cache_db_slots_purged = cache_db_cleared,
                "purged ALL storage from both cache layers (full refresh)"
            );
        }
        // All layer-2 storage was cleared → full base rebuild.
        self.invalidate_base();
        total_slots
    }

    /// Enumerate all cached storage slots for a contract address.
    ///
    /// Returns the union of slot keys from both CacheDB overlay (layer 1) and
    /// BlockchainDb backend (layer 2). Used by the slot observation tracker to
    /// selectively purge only slots likely to have changed.
    pub fn enumerate_contract_slots(&self, address: Address) -> Vec<U256> {
        let mut slots: HashSet<U256> = HashSet::new();

        // Layer 1: CacheDB overlay
        if let Some(db_account) = self.db.cache.accounts.get(&address) {
            slots.extend(db_account.storage.keys().copied());
        }

        // Layer 2: BlockchainDb backend
        let storage = self.blockchain_db.storage().read();
        if let Some(backend_slots) = storage.get(&address) {
            slots.extend(backend_slots.keys().copied());
        }

        slots.into_iter().collect()
    }

    /// Return all contract addresses that have cached storage in either layer.
    ///
    /// Used by the observation-aware full purge to enumerate what needs checking.
    pub fn all_cached_contract_addresses(&self) -> Vec<Address> {
        let mut addrs: HashSet<Address> = HashSet::new();

        // Layer 1: CacheDB overlay
        for (addr, account) in &self.db.cache.accounts {
            if !account.storage.is_empty() {
                addrs.insert(*addr);
            }
        }

        // Layer 2: BlockchainDb backend
        let storage = self.blockchain_db.storage().read();
        for addr in storage.keys() {
            addrs.insert(*addr);
        }

        addrs.into_iter().collect()
    }

    /// Get the number of storage slots in the CacheDB overlay for a contract.
    ///
    /// This is useful for diagnostics: if a contract has slots in the CacheDB
    /// overlay, they will be served on EVM reads without going to the backend.
    pub fn cache_db_storage_slot_count(&self, address: Address) -> usize {
        self.db
            .cache
            .accounts
            .get(&address)
            .map(|a| a.storage.len())
            .unwrap_or(0)
    }

    /// Simulate a call and compute `owner`'s net balance change for each token
    /// in `tokens` by reading `balanceOf(owner)` immediately before and after.
    ///
    /// Each delta is the signed `post - pre` difference (see
    /// [`CallSimulationResult::token_deltas`]). When `commit` is true the call's
    /// state changes are persisted to the CacheDB overlay; otherwise they are
    /// reverted. Unlike
    /// [`simulate_with_transfer_tracking`](Self::simulate_with_transfer_tracking),
    /// this measures deltas via pre/post balance reads (not transfer-event
    /// inspection). The returned [`access_list`](CallSimulationResult::access_list)
    /// includes the accounts and slots touched by the pre/post `balanceOf` reads
    /// and the simulated call.
    ///
    /// # Errors
    /// Returns an error if building the tx env fails, if a pre/post
    /// `balanceOf` read fails, or if the call does not `Success` (i.e. it
    /// reverted or halted). On error the simulation is reverted.
    pub fn simulate_call_with_balance_deltas(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        owner: Address,
        tokens: impl IntoIterator<Item = Address>,
        commit: bool,
    ) -> Result<CallSimulationResult> {
        let token_list: Vec<Address> = tokens.into_iter().collect();

        let mut pre_balances = HashMap::with_capacity(token_list.len());
        let mut access_lists = Vec::with_capacity(token_list.len().saturating_mul(2) + 1);
        for token in &token_list {
            let mut evm = self.build_evm();
            let synthetic_beneficiary = Self::seed_synthetic_beneficiary(&mut evm);
            let (balance, access_list) =
                Self::erc20_balance_of_in_evm_isolated(&mut evm, from, *token, owner)?;
            Self::remove_synthetic_beneficiary(&mut evm, synthetic_beneficiary);
            pre_balances.insert(*token, balance);
            access_lists.push(access_list);
        }

        let tx = Self::build_tx_env(from, to, calldata)?;
        let mut evm = self.build_evm();
        let synthetic_beneficiary = Self::seed_synthetic_beneficiary(&mut evm);
        let target_checkpoint = evm.journaled_state.checkpoint();
        let result = evm.transact_one(tx).map_err(CacheError::transact)?;
        let (logs, gas_used, output) = match result {
            ExecutionResult::Success {
                logs,
                gas_used,
                output,
                ..
            } => (logs, gas_used, output.into_data()),
            _ => {
                evm.journaled_state.checkpoint_revert(target_checkpoint);
                Self::remove_synthetic_beneficiary(&mut evm, synthetic_beneficiary);
                return Err(CacheError::CallNotSuccessful {
                    result: format!("{result:?}"),
                });
            }
        };
        access_lists.push(extract_access_list(&evm.journaled_state.state));

        let mut token_deltas = HashMap::with_capacity(token_list.len());
        for token in &token_list {
            let (post, access_list) =
                match Self::erc20_balance_of_in_evm_isolated(&mut evm, from, *token, owner) {
                    Ok(result) => result,
                    Err(err) => {
                        evm.journaled_state.checkpoint_revert(target_checkpoint);
                        Self::remove_synthetic_beneficiary(&mut evm, synthetic_beneficiary);
                        return Err(err);
                    }
                };
            let pre = pre_balances.get(token).copied().unwrap_or_default();
            token_deltas.insert(*token, I256::from_raw(post) - I256::from_raw(pre));
            access_lists.push(access_list);
        }

        let access_list = merge_access_lists(access_lists);
        if commit {
            Self::remove_synthetic_beneficiary(&mut evm, synthetic_beneficiary);
            evm.commit_inner();
        } else {
            evm.journaled_state.checkpoint_revert(target_checkpoint);
            Self::remove_synthetic_beneficiary(&mut evm, synthetic_beneficiary);
        }

        Ok(CallSimulationResult {
            status: SimStatus::Success,
            gas_used,
            token_deltas,
            logs,
            access_list,
            output,
        })
    }

    /// Simulate a call and track token balance changes using a TransferInspector.
    ///
    /// This method uses EVM inspection to capture ERC20 Transfer events during execution,
    /// eliminating the need for manual balance reads before/after the transaction.
    ///
    /// Returns:
    /// - `Ok(CallSimulationResult)` on successful execution
    /// - `Err(SimError::Revert(_))` when the transaction reverts (graceful failure)
    /// - `Err(SimError::Other(_))` for unexpected errors (should be propagated)
    #[instrument(level = "debug", skip(self, calldata, tokens), fields(calldata_len = calldata.len()))]
    pub fn simulate_with_transfer_tracking(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        owner: Address,
        tokens: Option<impl IntoIterator<Item = Address>>,
        commit: bool,
    ) -> SimulationResult<CallSimulationResult> {
        let tx = Self::build_tx_env(from, to, calldata).map_err(SimError::from)?;
        let inspector = TransferInspector::new();
        let mut evm = self.build_evm_with_inspector(inspector);
        let checkpoint = evm.journaled_state.checkpoint();

        let result = evm
            .inspect_one_tx(tx)
            .map_err(|e| SimError::Other(SimHostError::transact(e)));

        match result {
            Ok(ExecutionResult::Success {
                logs,
                gas_used,
                output,
                ..
            }) => {
                // Compute balance deltas from captured transfers
                let token_deltas = if let Some(token_list) = tokens {
                    evm.inspector.balance_deltas_for_tokens(owner, token_list)
                } else {
                    evm.inspector.balance_deltas(owner)
                };

                // Log shared memory buffer capacity for profiling
                let memory_capacity = evm.ctx.local.shared_memory_buffer.borrow().capacity();
                trace!(
                    memory_capacity_bytes = memory_capacity,
                    memory_capacity_kb = memory_capacity / 1024,
                    "EVM shared memory buffer capacity after simulation"
                );

                // Extract EIP-2930 access list from journaled state before commit/revert.
                // After inspect_one_tx, state contains all touched accounts and storage slots.
                let access_list = extract_access_list(&evm.journaled_state.state);

                if commit {
                    evm.commit_inner();
                } else {
                    evm.journaled_state.checkpoint_revert(checkpoint);
                }

                Ok(CallSimulationResult {
                    status: SimStatus::Success,
                    gas_used,
                    token_deltas,
                    logs,
                    access_list,
                    output: output.into_data(),
                })
            }
            Ok(ExecutionResult::Revert { gas_used, output }) => {
                evm.journaled_state.checkpoint_revert(checkpoint);
                Err(SimulationError::from_revert(gas_used, output).into())
            }
            Ok(ExecutionResult::Halt { reason, gas_used }) => {
                evm.journaled_state.checkpoint_revert(checkpoint);
                Err(SimError::Halt {
                    reason: format!("{reason:?}"),
                    gas_used,
                })
            }
            Err(err) => {
                evm.journaled_state.checkpoint_revert(checkpoint);
                Err(err)
            }
        }
    }

    /// Simulate a call with transfer tracking without any prefetching.
    ///
    /// This is identical to `simulate_with_transfer_tracking` since we no longer
    /// do access list prefetching. Kept for API compatibility.
    pub fn simulate_with_transfer_tracking_raw(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        owner: Address,
        tokens: Option<impl IntoIterator<Item = Address>>,
        commit: bool,
    ) -> SimulationResult<CallSimulationResult> {
        self.simulate_with_transfer_tracking(from, to, calldata, owner, tokens, commit)
    }

    /// Simulate an ordered transaction **bundle** over cumulative block state,
    /// with a revert policy and coinbase/miner-payment accounting (Phase 6
    /// Track A+B).
    ///
    /// This is a convenience wrapper: it snapshots the cache and runs the bundle
    /// on a fresh transient [`EvmOverlay`] via
    /// [`EvmOverlay::simulate_bundle`](crate::cache::EvmOverlay::simulate_bundle),
    /// which carries the full semantics (ordered cumulative state, the
    /// [`RevertPolicy`](crate::bundle::RevertPolicy), and coinbase accounting).
    ///
    /// The cache itself is **never** mutated — even when `opts.commit` is `true`.
    /// `commit` controls only whether the bundle's cumulative state is folded
    /// into the transient overlay (and is therefore moot here, since that overlay
    /// is dropped when this call returns). Snapshot the cache yourself and drive
    /// [`EvmOverlay::simulate_bundle`] directly when you need the committed
    /// overlay state to outlive the call (e.g. to chain a follow-up read).
    ///
    /// # Errors
    ///
    /// Returns [`SimError`] if a transaction environment cannot be built or revm
    /// fails to transact. A transaction reverting is reported through the
    /// per-transaction outcome and the revert policy, not as an error.
    pub fn simulate_bundle(
        &mut self,
        txs: &[crate::bundle::BundleTx],
        opts: &crate::bundle::BundleOptions,
    ) -> SimulationResult<crate::bundle::BundleResult> {
        let snapshot = self.snapshot();
        let mut overlay = EvmOverlay::new(snapshot, None);
        overlay.simulate_bundle(txs, opts)
    }

    /// Deploy a contract via CREATE transaction and return the deployed address.
    ///
    /// The `creation_code` should include the init code with ABI-encoded constructor
    /// arguments appended. Nonce checks are disabled, so any `from` address works.
    ///
    /// Note: This commits the deployment to the CacheDB. Use a throw-away deployer
    /// address (e.g., `Address::ZERO`) to avoid side effects on real accounts.
    ///
    /// # Errors
    /// Returns an error if the CREATE tx env cannot be built, if the deployment
    /// reverts or halts, or if it succeeds but the EVM returns no contract
    /// address.
    pub fn deploy_contract(&mut self, from: Address, creation_code: Bytes) -> Result<Address> {
        let tx = TxEnv::builder()
            .caller(from)
            .kind(TxKind::Create)
            .data(creation_code)
            .value(U256::ZERO)
            .build()
            .map_err(CacheError::tx_env)?;

        // Use a relaxed contract size limit for deployment. Arbitrum supports
        // larger contracts than the EIP-170 24576-byte limit via ArbOS.
        let mut evm = self.build_evm();
        evm.cfg.limit_contract_code_size = Some(usize::MAX);
        let result = evm.transact_commit(tx).map_err(CacheError::transact)?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let address = output
                    .address()
                    .copied()
                    .ok_or(CacheError::DeploymentMissingAddress)?;
                // A locally-deployed contract is divergence by construction:
                // record it so `etched_accounts` reports every non-chain code
                // site. The committed create left the runtime code in the
                // overlay; hash from there.
                let code_hash = self
                    .db
                    .cache
                    .accounts
                    .get(&address)
                    .map(|account| account.info.code_hash)
                    .unwrap_or(revm::primitives::KECCAK_EMPTY);
                self.code_seeds
                    .insert(address, CodeSeedState::Etched { code_hash });
                Ok(address)
            }
            ExecutionResult::Revert { output, .. } => Err(CacheError::DeploymentReverted {
                output_hex: alloy_primitives::hex::encode(&output),
            }),
            ExecutionResult::Halt { reason, .. } => Err(CacheError::DeploymentHalted {
                reason: format!("{reason:?}"),
            }),
        }
    }

    /// Override the bytecode at `target` address with bytecode from `source` address.
    ///
    /// Copies only non-empty runtime code and code_hash; storage, balance, and nonce
    /// at `target` remain unchanged. `target` must already have non-empty runtime
    /// bytecode. Both the CacheDB overlay and BlockchainDb backend are updated,
    /// ensuring the override is visible to parallel EVM tasks sharing the same backend.
    ///
    /// # Errors
    /// Returns an error if `source` has no cached bytecode or its code is empty,
    /// if `target` cannot be loaded (it must already exist on the backend), or
    /// if `target` has no existing runtime bytecode to override. For synthetic
    /// `target` addresses that may not exist, use
    /// [`override_or_create_account_code`](Self::override_or_create_account_code).
    pub fn override_account_code(&mut self, source: Address, target: Address) -> Result<()> {
        self.override_account_code_with_missing_target(source, target, MissingTargetBehavior::Error)
    }

    /// Override the bytecode at `target`, creating a default target account when absent.
    ///
    /// Use this for synthetic addresses in local simulations. For live forked
    /// accounts where storage/balance/nonce must be preserved, prefer
    /// [`Self::override_account_code`].
    pub fn override_or_create_account_code(
        &mut self,
        source: Address,
        target: Address,
    ) -> Result<()> {
        self.override_account_code_with_missing_target(
            source,
            target,
            MissingTargetBehavior::Create,
        )
    }

    /// Override code at `target`, with explicit behavior for missing target accounts.
    ///
    /// This is intentionally **not** folded onto
    /// [`apply_update`](Self::apply_update)'s `Account` code patch: it copies code
    /// from a `source` account, preserves the target's existing balance/nonce/
    /// storage, and **unconditionally materializes** the target in the CacheDB
    /// overlay (the primary read path for EVM execution, required for the
    /// `Create` synthetic-target case). The generic primitive writes the overlay
    /// only when an account is already present, so the two are not
    /// behavior-equivalent. For a plain code overwrite that follows the
    /// dual-layer write-through policy, use
    /// `apply_update(StateUpdate::Account { patch: AccountPatch::default().code(..) })`.
    pub fn override_account_code_with_missing_target(
        &mut self,
        source: Address,
        target: Address,
        missing_target: MissingTargetBehavior,
    ) -> Result<()> {
        // Read deployed bytecode from source (in CacheDB overlay after deploy_contract)
        let source_code = self
            .db
            .cache
            .accounts
            .get(&source)
            .and_then(|a| a.info.code.clone())
            .ok_or(CacheError::MissingSourceBytecode {
                source_address: source,
            })?;
        Self::ensure_runtime_code(source, Some(&source_code), "source")?;

        let code_hash = source_code.hash_slow();
        debug!(
            source = %source,
            target = %target,
            code_size = source_code.len(),
            "Overriding account bytecode"
        );

        let mut target_info = self.target_account_info(target, missing_target)?;

        if matches!(missing_target, MissingTargetBehavior::Error) {
            Self::ensure_runtime_code(target, target_info.code.as_ref(), "target")?;
        }

        target_info.code = Some(source_code);
        target_info.code_hash = code_hash;

        // Update CacheDB overlay (primary read path for EVM execution).
        self.db.insert_account_info(target, target_info.clone());

        // Update BlockchainDb backend (shared with parallel tasks)
        {
            let mut accounts = self.blockchain_db.accounts().write();
            accounts.insert(target, target_info);
        }

        // Layer 2 changed → invalidate the memoized base for `target`. The layer-1
        // `insert_account_info` above currently shadows it on every snapshot read,
        // but we dirty unconditionally for uniformity with every other layer-2 write
        // site (D2), so base correctness never relies on that shadowing invariant.
        self.mark_base_dirty(target);

        // Every locally-divergent code write is visible in one place: the
        // override target joins the etched set (see `etched_accounts`).
        self.code_seeds
            .insert(target, CodeSeedState::Etched { code_hash });

        Ok(())
    }

    /// Verify every [`CodeSeedState::Pending`] canonical code claim against
    /// the chain at the pinned block — one bulk `eth_call` for the whole set.
    ///
    /// Per-address outcomes (see [`CodeVerifyReport`]):
    /// - **match** ⇒ marked [`CodeSeedState::Verified`] (never re-checked;
    ///   post-EIP-6780 code is immutable) and the account's real balance is
    ///   patched in from the same response — pure materialization of
    ///   pinned-block truth, so it does **not** bump the
    ///   [snapshot generation](Self::snapshot_generation);
    /// - **mismatch / not-deployed / code-less** ⇒
    ///   [`purge_account`](Self::purge_account) (both layers **and** the
    ///   mark; the purge path bumps the generation) — the next touch
    ///   refetches authoritative chain state;
    /// - **transport failure** (the whole call, an omitted address, or the
    ///   `MULTICALL3_ADDRESS` extractor-host caveat) ⇒ still `Pending`,
    ///   reported `unverifiable` — a failed read proves nothing, so it never
    ///   promotes and never destroys a seed.
    ///
    /// With no pending seeds this is a no-op that needs no fetcher. Verified
    /// seeds are skipped forever, so calling this repeatedly (or from every
    /// cold-start round) costs nothing once the set is settled.
    ///
    /// # Errors
    /// [`CacheError::MissingAccountFieldsFetcher`] when pending seeds exist
    /// but no [`AccountFieldsFetchFn`] is installed (a
    /// [`from_backend`](Self::from_backend) cache without
    /// [`set_account_fields_fetcher`](Self::set_account_fields_fetcher)).
    pub fn verify_code_seeds(&mut self) -> Result<CodeVerifyReport> {
        let pending = self.pending_code_seeds();
        if pending.is_empty() {
            return Ok(CodeVerifyReport::default());
        }
        let fetcher = self
            .account_fields_fetcher
            .clone()
            .ok_or(CacheError::MissingAccountFieldsFetcher)?;

        let mut report = CodeVerifyReport::default();

        // The extractor is hosted at MULTICALL3_ADDRESS under the eth_call
        // override, so querying that address would report the extractor's own
        // hash — a seed there is unverifiable by this path (use eth_getProof).
        let (host, query): (Vec<Address>, Vec<Address>) = pending
            .into_iter()
            .partition(|address| *address == crate::multicall::MULTICALL3_ADDRESS);
        for address in host {
            report.unverifiable.push((
                address,
                "the account-fields extractor is hosted at this address under the eth_call \
                 override; verify it via the eth_getProof path instead"
                    .to_string(),
            ));
        }
        if query.is_empty() {
            return Ok(report);
        }

        let samples = match (fetcher)(query.clone(), self.block) {
            Ok(samples) => samples,
            Err(error) => {
                // Fail-safe on transport: every seed stays Pending.
                let reason = error.to_string();
                report
                    .unverifiable
                    .extend(query.into_iter().map(|address| (address, reason.clone())));
                return Ok(report);
            }
        };
        let by_address: HashMap<Address, AccountFieldsSample> = samples.into_iter().collect();

        let verified_at_block = self.block_number.unwrap_or_default();
        for address in query {
            let Some(CodeSeedState::Pending {
                code_hash: expected,
            }) = self.code_seeds.get(&address).cloned()
            else {
                // Unreachable in practice (the set was snapshotted above);
                // skip rather than misclassify.
                continue;
            };
            let Some(sample) = by_address.get(&address) else {
                report.unverifiable.push((
                    address,
                    "account-fields fetcher returned no sample for this address".to_string(),
                ));
                continue;
            };

            if sample.code_hash == expected {
                self.code_seeds.insert(
                    address,
                    CodeSeedState::Verified {
                        code_hash: expected,
                        verified_at_block,
                    },
                );
                self.materialize_verified_balance(address, sample.balance);
                report.verified.push(address);
            } else if sample.code_hash == B256::ZERO {
                self.purge_account(address);
                report.not_deployed.push(address);
            } else if sample.code_hash == revm::primitives::KECCAK_EMPTY {
                self.purge_account(address);
                report.codeless.push(address);
            } else {
                self.purge_account(address);
                report.mismatched.push(CodeMismatch {
                    address,
                    expected,
                    actual: sample.code_hash,
                });
            }
        }
        Ok(report)
    }

    /// Validate exact-hash account fields and verified runtime code without
    /// mutating either cache layer or the code-seed marks.
    ///
    /// The complete patch is validated before the first write: the cache hash
    /// must still match, account identities must be unique, proofs must be
    /// root-only, and each runtime byte string must hash to its proof's
    /// `codeHash`. Existing deliberate etches and conflicting seed generations
    /// are rejected. The same internal preparation routine is reused by
    /// [`apply_prepared_account_patch`](Self::apply_prepared_account_patch).
    #[cfg(feature = "reactive")]
    pub fn validate_prepared_account_patch(
        &self,
        patch: &crate::cold_start::PreparedAccountPatch,
    ) -> std::result::Result<(), crate::cold_start::PreparedAccountPatchError> {
        self.prepare_prepared_account_patch(patch).map(|_| ())
    }

    /// Atomically install a patch previously accepted by
    /// [`validate_prepared_account_patch`](Self::validate_prepared_account_patch).
    ///
    /// This repeats the same pure validation at the final exclusive-owner
    /// boundary, then performs only infallible in-memory writes. With no
    /// intervening account/seed or baseline mutation, validation followed by
    /// apply cannot diverge. Account info/code is written through both cache
    /// layers and [`CodeSeedState::Verified`] is recorded directly, so canonical
    /// state is never published with an intermediate `Pending` mark.
    #[cfg(feature = "reactive")]
    pub fn apply_prepared_account_patch(
        &mut self,
        patch: &crate::cold_start::PreparedAccountPatch,
    ) -> std::result::Result<usize, crate::cold_start::PreparedAccountPatchError> {
        let prepared = self.prepare_prepared_account_patch(patch)?;

        for (address, info, _) in &prepared {
            self.db.insert_account_info(*address, info.clone());
        }
        {
            let mut accounts = self.blockchain_db.accounts().write();
            for (address, info, _) in &prepared {
                accounts.insert(*address, info.clone());
            }
        }
        for (address, _, code_hash) in &prepared {
            self.code_seeds.insert(
                *address,
                CodeSeedState::Verified {
                    code_hash: *code_hash,
                    verified_at_block: patch.verified_at_block(),
                },
            );
            self.mark_base_dirty(*address);
        }
        if !prepared.is_empty() {
            self.bump_snapshot_generation();
        }
        Ok(prepared.len())
    }

    #[cfg(feature = "reactive")]
    fn prepare_prepared_account_patch(
        &self,
        patch: &crate::cold_start::PreparedAccountPatch,
    ) -> std::result::Result<
        Vec<(Address, AccountInfo, B256)>,
        crate::cold_start::PreparedAccountPatchError,
    > {
        use crate::cold_start::PreparedAccountPatchError;

        let cache_hash = match self.block() {
            BlockId::Hash(hash) => Some(hash.block_hash),
            BlockId::Number(_) => None,
        };
        if cache_hash != Some(patch.block_hash()) {
            return Err(PreparedAccountPatchError::BaselineMismatch {
                prepared: patch.block_hash(),
                cache: cache_hash,
            });
        }

        let mut identities = HashSet::with_capacity(patch.values().len());
        let mut prepared = Vec::with_capacity(patch.values().len());
        for value in patch.values() {
            let address = value.address();
            let proof = value.proof();
            let code = value.code();
            if !identities.insert(address) {
                return Err(PreparedAccountPatchError::DuplicateAccount { address });
            }
            if code.is_empty() {
                return Err(PreparedAccountPatchError::EmptyCode { address });
            }
            if !proof.slots.is_empty() {
                return Err(PreparedAccountPatchError::UnexpectedProofSlots {
                    address,
                    slots: proof.slots.len(),
                });
            }
            let actual = keccak256(code);
            if actual != proof.code_hash {
                return Err(PreparedAccountPatchError::CodeHashMismatch {
                    address,
                    expected: proof.code_hash,
                    actual,
                });
            }
            if let Some(existing) = self.code_seeds.get(&address) {
                if matches!(existing, CodeSeedState::Etched { .. }) {
                    return Err(PreparedAccountPatchError::EtchedAccount { address });
                }
                if existing.code_hash() != actual {
                    return Err(PreparedAccountPatchError::SeedConflict {
                        address,
                        existing: existing.code_hash(),
                        prepared: actual,
                    });
                }
            }

            let mut info = self.local_account_info(address).unwrap_or_default();
            info.balance = proof.balance;
            info.nonce = proof.nonce;
            info.code_hash = actual;
            info.code = Some(Bytecode::new_raw(code.clone()));
            prepared.push((address, info, actual));
        }
        Ok(prepared)
    }

    /// Patch a just-verified seed's balance to the on-chain value from the
    /// verification sample — in both layers, **without** a
    /// snapshot-generation bump: confirming a claim and materializing
    /// pinned-block truth is the prefetch class of write, not a mutation.
    /// The overlay is only patched when the account already has an entry
    /// there (it always does for a seeded account), mirroring the layer
    /// policy of [`inject_storage_batch_fresh`](Self::inject_storage_batch_fresh).
    fn materialize_verified_balance(&mut self, address: Address, balance: U256) {
        if let Some(account) = self.db.cache.accounts.get_mut(&address) {
            account.info.balance = balance;
        }
        {
            let mut accounts = self.blockchain_db.accounts().write();
            if let Some(info) = accounts.get_mut(&address) {
                info.balance = balance;
            }
        }
        self.mark_base_dirty(address);
    }

    /// Local (already-materialized) account info for `address` — CacheDB
    /// overlay first, then the BlockchainDb backend. Never fetches: code-seed
    /// decisions are made strictly against what the cache already holds.
    fn local_account_info(&self, address: Address) -> Option<AccountInfo> {
        if let Some(account) = self.db.cache.accounts.get(&address) {
            return Some(account.info.clone());
        }
        self.blockchain_db.accounts().read().get(&address).cloned()
    }

    /// Dual-layer account write shared by [`seed_account_code_with`](Self::seed_account_code_with)
    /// and [`etch_account_code`](Self::etch_account_code): CacheDB overlay
    /// (the primary EVM read path) plus the BlockchainDb backend (shared with
    /// parallel tasks), base invalidation, and a snapshot-generation bump —
    /// a code write changes executable state (see
    /// [`snapshot_generation`](Self::snapshot_generation)).
    fn write_marked_code(&mut self, address: Address, info: AccountInfo) {
        self.db.insert_account_info(address, info.clone());
        {
            let mut accounts = self.blockchain_db.accounts().write();
            accounts.insert(address, info);
        }
        self.mark_base_dirty(address);
        self.bump_snapshot_generation();
    }

    /// Seed canonical runtime code for `address` without fetching it.
    ///
    /// The claim is marked [`CodeSeedState::Pending`] until
    /// [`verify_code_seeds`](Self::verify_code_seeds) confirms it against the
    /// on-chain `EXTCODEHASH` (or the cold-start driver's `verify_code` phase
    /// does). Because the account is materialized in both cache layers, the
    /// lazy backend never fires its balance/nonce/code RPC triple for it.
    ///
    /// Defaults: nonce 1 (the EIP-161 contract minimum — exact for any
    /// contract that never `CREATE`s) and balance `ZERO` until verification
    /// patches the real value from the same response. Use
    /// [`seed_account_code_with`](Self::seed_account_code_with) to supply
    /// both explicitly.
    ///
    /// Conflict rules (chain-fetched state is authoritative over templates):
    /// seeding an **unmarked** address that already holds RPC-origin code
    /// with the same hash marks it `Verified` immediately (zero RPC — the
    /// warm-cache fast path); a differing hash (including a code-less EOA) is
    /// [`CacheError::CodeSeedConflict`] and leaves the cached code untouched.
    /// Re-seeding a marked address overwrites and restarts the claim as
    /// `Pending`.
    ///
    /// Returns the keccak256 hash recorded for the claim.
    ///
    /// # Errors
    /// [`CacheError::CodeSeedEmpty`] for empty `code`;
    /// [`CacheError::CodeSeedConflict`] as above.
    pub fn seed_account_code(&mut self, address: Address, code: Bytes) -> Result<B256> {
        self.seed_account_code_with(address, code, 1, U256::ZERO)
    }

    /// [`seed_account_code`](Self::seed_account_code) with explicit `nonce`
    /// and provisional `balance` for the materialized account. Verification
    /// still overwrites the balance with the on-chain value on a match; the
    /// nonce keeps the supplied value (an exact nonce needs the
    /// `eth_getProof` path and only matters for contracts that `CREATE`).
    pub fn seed_account_code_with(
        &mut self,
        address: Address,
        code: Bytes,
        nonce: u64,
        balance: U256,
    ) -> Result<B256> {
        if code.is_empty() {
            return Err(CacheError::CodeSeedEmpty { address });
        }
        let bytecode = Bytecode::new_raw(code);
        let code_hash = bytecode.hash_slow();

        // Unmarked + locally present ⇒ RPC-origin, which is authoritative.
        if !self.code_seeds.contains_key(&address)
            && let Some(existing) = self.local_account_info(address)
        {
            if existing.code_hash == code_hash {
                // Hash equality proves byte equality: the claim is already
                // confirmed by chain-fetched state, zero RPC. If the restored
                // account is missing its code *bytes* (binary state without a
                // bytecodes.bin entry), the seed supplies exactly the bytes
                // the recorded hash committed to — a free repair.
                if existing
                    .code
                    .as_ref()
                    .is_none_or(|existing_code| existing_code.is_empty())
                {
                    let mut info = existing;
                    info.code = Some(bytecode);
                    info.code_hash = code_hash;
                    self.write_marked_code(address, info);
                }
                self.code_seeds.insert(
                    address,
                    CodeSeedState::Verified {
                        code_hash,
                        verified_at_block: self.block_number.unwrap_or_default(),
                    },
                );
                return Ok(code_hash);
            }
            return Err(CacheError::CodeSeedConflict {
                address,
                cached: existing.code_hash,
                seeded: code_hash,
            });
        }

        // Absent, or an existing mark being re-seeded: write the claim.
        // A marked account keeps its current balance/nonce; a fresh one gets
        // the caller's provisional values.
        let mut info = self.local_account_info(address).unwrap_or(AccountInfo {
            balance,
            nonce,
            code_hash: revm::primitives::KECCAK_EMPTY,
            code: None,
            account_id: None,
        });
        info.code = Some(bytecode);
        info.code_hash = code_hash;
        self.write_marked_code(address, info);
        self.code_seeds
            .insert(address, CodeSeedState::Pending { code_hash });
        Ok(code_hash)
    }

    /// Etch deliberately-local runtime code at `address` — the raw-bytes
    /// sibling of [`override_or_create_account_code`](Self::override_or_create_account_code),
    /// with no source account needed.
    ///
    /// Marks [`CodeSeedState::Etched`]: never verified, excluded from all
    /// canonical machinery, and reported via
    /// [`etched_accounts`](Self::etched_accounts) so local divergence stays
    /// visible. Preserves the existing balance/nonce/storage when the account
    /// is already present; creates a default account otherwise. Overwrites
    /// any prior code or mark — divergence is the caller's explicit intent.
    ///
    /// Returns the keccak256 hash of the etched code.
    ///
    /// # Errors
    /// [`CacheError::CodeSeedEmpty`] for empty `code`.
    pub fn etch_account_code(&mut self, address: Address, code: Bytes) -> Result<B256> {
        if code.is_empty() {
            return Err(CacheError::CodeSeedEmpty { address });
        }
        let bytecode = Bytecode::new_raw(code);
        let code_hash = bytecode.hash_slow();
        let mut info = self.local_account_info(address).unwrap_or_default();
        info.code = Some(bytecode);
        info.code_hash = code_hash;
        self.write_marked_code(address, info);
        self.code_seeds
            .insert(address, CodeSeedState::Etched { code_hash });
        Ok(code_hash)
    }

    /// The code-seed mark for `address`, if any. `None` means RPC-origin:
    /// the code (if present) was fetched from the provider and is trusted as
    /// chain state.
    pub fn code_seed_state(&self, address: &Address) -> Option<&CodeSeedState> {
        self.code_seeds.get(address)
    }

    /// Addresses whose canonical code claims still await verification
    /// ([`CodeSeedState::Pending`]), sorted for deterministic iteration.
    /// This is the implicit work set of
    /// [`verify_code_seeds`](Self::verify_code_seeds) and the cold-start
    /// `verify_code` phase.
    pub fn pending_code_seeds(&self) -> Vec<Address> {
        let mut pending: Vec<Address> = self
            .code_seeds
            .iter()
            .filter_map(|(addr, state)| {
                matches!(state, CodeSeedState::Pending { .. }).then_some(*addr)
            })
            .collect();
        pending.sort();
        pending
    }

    /// Addresses whose code deliberately diverges from the chain
    /// ([`CodeSeedState::Etched`]), sorted for deterministic iteration. This
    /// is the health surface for local divergence: everything written through
    /// [`etch_account_code`](Self::etch_account_code),
    /// [`override_account_code`](Self::override_account_code) and friends, or
    /// [`deploy_contract`](Self::deploy_contract) appears here.
    pub fn etched_accounts(&self) -> Vec<Address> {
        let mut etched: Vec<Address> = self
            .code_seeds
            .iter()
            .filter_map(|(addr, state)| {
                matches!(state, CodeSeedState::Etched { .. }).then_some(*addr)
            })
            .collect();
        etched.sort();
        etched
    }

    pub(crate) fn require_contract_target(&self, target: Address) -> Result<()> {
        let target_info = self.target_account_info(target, MissingTargetBehavior::Error)?;
        Self::ensure_runtime_code(target, target_info.code.as_ref(), "target")
    }

    fn target_account_info(
        &self,
        target: Address,
        missing_target: MissingTargetBehavior,
    ) -> Result<AccountInfo> {
        if let Some(account) = self.db.cache.accounts.get(&target) {
            // A NotExisting overlay account is absent to the EVM (revm
            // `DbAccount::info()` returns None); treat it as a missing target
            // rather than returning its stale/default info.
            if !matches!(account.account_state, AccountState::NotExisting) {
                return Ok(account.info.clone());
            }
        }

        match missing_target {
            MissingTargetBehavior::Create => Ok(AccountInfo::default()),
            MissingTargetBehavior::Error => {
                use revm::database_interface::DatabaseRef;
                self.backend
                    .basic_ref(target)
                    .map_err(|e| CacheError::TargetAccountFetch {
                        target,
                        details: format!("{e:?}"),
                    })?
                    .ok_or(CacheError::MissingTargetAccount { target })
            }
        }
    }

    fn ensure_runtime_code(address: Address, code: Option<&Bytecode>, role: &str) -> Result<()> {
        if code.is_some_and(|code| !code.is_empty()) {
            return Ok(());
        }

        Err(CacheError::MissingRuntimeCode {
            role: match role {
                "source" => "source",
                "target" => "target",
                _ => "account",
            },
            address,
        })
    }
}

/// Read-only state view for the event pipeline (Pillar B.2): a decoder reads the
/// current cached value of a slot through [`cached_storage_value`](EvmCache::cached_storage_value),
/// which never touches RPC and is `account_state`-aware (a cold slot reads
/// `None`).
impl crate::events::StateView for EvmCache {
    fn storage(&self, address: Address, slot: U256) -> Option<U256> {
        self.cached_storage_value(address, slot)
    }
}

impl EvmCache {
    /// Create a LocalContext that reuses the shared memory buffer.
    ///
    /// The buffer is cleared (length set to 0) but capacity is preserved,
    /// avoiding repeated allocations across simulations.
    fn make_local_context(&self) -> LocalContext {
        // Clear the buffer but preserve capacity. `Vec::clear` sets the length
        // to 0 without releasing the allocation, so the buffer is reused across
        // simulations.
        self.shared_memory_buffer.borrow_mut().clear();

        LocalContext {
            shared_memory_buffer: self.shared_memory_buffer.clone(),
            precompile_error_message: None,
        }
    }

    fn build_evm(&mut self) -> CacheEvm<'_> {
        let local = self.make_local_context();
        let chain_id = self.chain_id;
        let mut evm = Context::mainnet()
            .with_db(&mut self.db)
            .with_local(local)
            .modify_cfg_chained(|cfg| {
                cfg.disable_nonce_check = true;
                cfg.disable_eip3607 = true;
                cfg.disable_base_fee = true;
                cfg.disable_balance_check = true;
                cfg.chain_id = chain_id;
                cfg.limit_contract_code_size = None;
                cfg.tx_chain_id_check = false;
                cfg.spec = self.spec_id;
            })
            .build_mainnet();

        let timestamp = self
            .timestamp_override
            .unwrap_or_else(|| unix_timestamp_secs_saturating(SystemTime::now()));
        evm.block.timestamp = U256::from(timestamp);
        if let Some(number) = self.block_number {
            evm.block.number = U256::from(number);
        }
        if let Some(basefee) = self.basefee {
            evm.block.basefee = basefee;
        }
        if let Some(coinbase) = self.coinbase {
            evm.block.beneficiary = coinbase;
        }
        if let Some(prevrandao) = self.prevrandao {
            evm.block.prevrandao = Some(prevrandao);
        }
        if let Some(gas_limit) = self.block_gas_limit {
            evm.block.gas_limit = gas_limit;
        }
        evm
    }

    fn build_evm_with_inspector<INSP>(&mut self, inspector: INSP) -> InspectorCacheEvm<'_, INSP> {
        let local = self.make_local_context();
        let chain_id = self.chain_id;
        let mut evm = Context::mainnet()
            .with_db(&mut self.db)
            .with_local(local)
            .modify_cfg_chained(|cfg| {
                cfg.disable_nonce_check = true;
                cfg.disable_eip3607 = true;
                cfg.disable_base_fee = true;
                cfg.disable_balance_check = true;
                cfg.chain_id = chain_id;
                cfg.limit_contract_code_size = None;
                cfg.tx_chain_id_check = false;
                cfg.spec = self.spec_id;
            })
            .build_mainnet_with_inspector(inspector);

        let timestamp = self
            .timestamp_override
            .unwrap_or_else(|| unix_timestamp_secs_saturating(SystemTime::now()));
        evm.block.timestamp = U256::from(timestamp);
        if let Some(number) = self.block_number {
            evm.block.number = U256::from(number);
        }
        if let Some(basefee) = self.basefee {
            evm.block.basefee = basefee;
        }
        if let Some(coinbase) = self.coinbase {
            evm.block.beneficiary = coinbase;
        }
        if let Some(prevrandao) = self.prevrandao {
            evm.block.prevrandao = Some(prevrandao);
        }
        if let Some(gas_limit) = self.block_gas_limit {
            evm.block.gas_limit = gas_limit;
        }
        evm
    }

    fn build_tx_env(from: Address, to: Address, calldata: Bytes) -> Result<TxEnv> {
        Self::build_tx_env_with(from, to, calldata, &TxConfig::default())
    }

    fn build_tx_env_with(
        from: Address,
        to: Address,
        calldata: Bytes,
        tx: &TxConfig,
    ) -> Result<TxEnv> {
        let mut builder = TxEnv::builder()
            .caller(from)
            .kind(TxKind::Call(to))
            .data(calldata)
            .value(tx.value);
        if let Some(gas_limit) = tx.gas_limit {
            builder = builder.gas_limit(gas_limit);
        }
        if let Some(gas_price) = tx.gas_price {
            builder = builder.gas_price(gas_price);
        }
        if let Some(nonce) = tx.nonce {
            builder = builder.nonce(nonce);
        }
        if let Some(access_list) = &tx.access_list {
            builder = builder.access_list(access_list.clone());
        }
        builder.build().map_err(CacheError::tx_env)
    }

    fn call_sol_with_commit<C>(
        &mut self,
        from: Address,
        to: Address,
        call: C,
        tx: &TxConfig,
        commit: bool,
    ) -> Result<C::Return>
    where
        C: SolCall,
    {
        let calldata = Bytes::from(call.abi_encode());
        let result = self.call_raw_with(from, to, calldata, commit, tx)?;
        Self::decode_sol_call_result::<C>(from, to, result)
    }

    fn decode_sol_call_result<C>(
        from: Address,
        to: Address,
        result: ExecutionResult,
    ) -> Result<C::Return>
    where
        C: SolCall,
    {
        match result {
            ExecutionResult::Success { output, .. } => {
                let output = output.into_data();
                C::abi_decode_returns(&output).map_err(|error| CacheError::SolCallDecode {
                    signature: C::SIGNATURE,
                    from,
                    to,
                    output_len: output.len(),
                    details: format!("{error:?}"),
                })
            }
            other => Err(CacheError::SolCallFailed {
                signature: C::SIGNATURE,
                from,
                to,
                result: format!("{other:?}"),
            }),
        }
    }

    fn erc20_balance_of_in_evm(
        evm: &mut CacheEvm<'_>,
        caller: Address,
        token: Address,
        owner: Address,
    ) -> Result<U256> {
        let call = IERC20::balanceOfCall { target: owner };
        let tx = Self::build_tx_env(caller, token, Bytes::from(call.abi_encode()))?;
        let result = evm.transact_one(tx).map_err(CacheError::transact)?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let balance = IERC20::balanceOfCall::abi_decode_returns(&out).map_err(|e| {
                    CacheError::Decode {
                        what: "ERC20 balanceOf return data",
                        details: format!("{e:?}"),
                    }
                })?;
                Ok(balance)
            }
            _ => Err(CacheError::CallNotSuccessful {
                result: format!("{result:?}"),
            }),
        }
    }

    fn erc20_balance_of_in_evm_isolated(
        evm: &mut CacheEvm<'_>,
        caller: Address,
        token: Address,
        owner: Address,
    ) -> Result<(U256, AccessList)> {
        let state_before = evm.journaled_state.state.clone();
        let checkpoint = evm.journaled_state.checkpoint();
        let result = Self::erc20_balance_of_in_evm(evm, caller, token, owner);
        let access_list = extract_access_list(&evm.journaled_state.state);
        evm.journaled_state.checkpoint_revert(checkpoint);
        evm.journaled_state.state = state_before;
        result.map(|balance| (balance, access_list))
    }

    fn seed_synthetic_beneficiary(evm: &mut CacheEvm<'_>) -> Option<Address> {
        let beneficiary = evm.block.beneficiary;
        if evm.journaled_state.state.contains_key(&beneficiary) {
            return None;
        }
        evm.journaled_state
            .state
            .insert(beneficiary, Account::from(AccountInfo::default()));
        Some(beneficiary)
    }

    fn remove_synthetic_beneficiary(evm: &mut CacheEvm<'_>, beneficiary: Option<Address>) {
        if let Some(beneficiary) = beneficiary {
            evm.journaled_state.state.remove(&beneficiary);
        }
    }
}

/// A session for executing multiple EVM operations without committing to the underlying DB.
///
/// Changes made within a session are tracked in the EVM's journaled state. Call `commit()` to
/// persist changes to the underlying database, or simply drop the session to discard
/// all changes.
///
/// Note: For checkpoint/restore functionality across multiple transactions, use
/// `EvmCache::checkpoint()` and `EvmCache::restore()` instead, as the EVM journal
/// is cleared after each transaction.
pub struct EvmSession<'a> {
    evm: CacheEvm<'a>,
}

impl<'a> EvmSession<'a> {
    /// Execute a call within the session.
    ///
    /// If `commit` is true, changes are persisted to the session's journaled state.
    /// If `commit` is false, the call is executed but its effects are immediately reverted.
    ///
    /// Note: Changes are not persisted to the underlying CacheDB until `commit()` is called
    /// on the session itself.
    pub fn call_raw(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        commit: bool,
    ) -> Result<ExecutionResult> {
        let tx = EvmCache::build_tx_env(from, to, calldata)?;

        if commit {
            self.evm.transact_one(tx).map_err(CacheError::transact)
        } else {
            let checkpoint = self.evm.journaled_state.checkpoint();
            let result = self.evm.transact_one(tx);
            self.evm.journaled_state.checkpoint_revert(checkpoint);
            result.map_err(CacheError::transact)
        }
    }

    /// Commit all session changes to the underlying database.
    ///
    /// This persists all changes made during the session to the CacheDB.
    pub fn commit(mut self) {
        self.evm.commit_inner();
    }

    /// Get access to the underlying EVM for advanced operations.
    ///
    /// This exposes revm internals and bypasses the cache's two-layer
    /// consistency model: state mutated directly through the journaled EVM
    /// lands in the session's journal, not the BlockchainDb backend, and is
    /// only flushed to the CacheDB overlay on [`commit`](Self::commit). Use
    /// with care.
    pub fn evm(&mut self) -> &mut CacheEvm<'a> {
        &mut self.evm
    }
}

/// Automatically flush the cache to disk when the EvmCache is dropped.
impl Drop for EvmCache {
    fn drop(&mut self) {
        if self.cache_config.is_some() {
            debug!("Flushing EVM cache on drop");
            if let Err(e) = self.flush() {
                warn!(error = %e, "Failed to flush EVM cache on drop");
            }
        }
    }
}

#[cfg(test)]
mod shared_memory_capacity_tests {
    use super::SharedMemoryCapacity as Cap;

    #[test]
    fn default_is_fixed_64k() {
        assert_eq!(Cap::default(), Cap::Fixed(64 * 1024));
    }

    #[test]
    fn fixed_ignores_loaded_slots() {
        assert_eq!(Cap::Fixed(8_192).resolve(10_000_000), 8_192);
        assert_eq!(Cap::Fixed(0).resolve(123), 0);
    }

    #[test]
    fn auto_floors_clamps_and_scales() {
        // Nothing / little loaded → floor.
        assert_eq!(Cap::Auto.resolve(0), Cap::MIN_AUTO);
        assert_eq!(Cap::Auto.resolve(1_000), Cap::MIN_AUTO); // 16 KiB < 64 KiB floor
        // Linear region (16 bytes/slot).
        assert_eq!(Cap::Auto.resolve(10_000), 160_000);
        assert_eq!(Cap::Auto.resolve(100_000), 1_600_000);
        // Ceiling.
        assert_eq!(Cap::Auto.resolve(usize::MAX), Cap::MAX_AUTO);
        assert_eq!(Cap::Auto.resolve(262_144), Cap::MAX_AUTO); // 262_144 * 16 == 4 MiB
    }
}

/// Tests that exercise the generic cache engine.
#[cfg(test)]
mod core_tests {
    use super::*;

    #[test]
    fn parses_prestate_diff_trace_values_and_cleared_slots() {
        let trace = serde_json::json!([
            {
                "result": {
                    "pre": {
                        "0x4242424242424242424242424242424242424242": {
                            "storage": {
                                "0x01": "0x05",
                                "0x02": "0x06"
                            }
                        }
                    },
                    "post": {
                        "0x4242424242424242424242424242424242424242": {
                            "balance": 10,
                            "nonce": "0x0a",
                            "code": "0x6001",
                            "storage": {
                                "0x01": "0x0b"
                            }
                        }
                    }
                }
            }
        ]);

        let diff = parse_block_state_diff_trace(&trace).unwrap();

        assert_eq!(diff.accounts.len(), 1);
        let account = &diff.accounts[0];
        assert_eq!(account.address, Address::repeat_byte(0x42));
        assert_eq!(account.balance, Some(U256::from(10)));
        assert_eq!(account.nonce, Some(10));
        assert_eq!(account.code, Some(Bytes::from(vec![0x60, 0x01])));
        assert_eq!(
            account.storage,
            vec![
                BlockStateStorageDiff {
                    slot: U256::from(1),
                    value: U256::from(11),
                },
                BlockStateStorageDiff {
                    slot: U256::from(2),
                    value: U256::ZERO,
                },
            ]
        );
    }

    #[test]
    fn parses_prestate_diff_trace_account_deletion() {
        // A SELFDESTRUCTed account appears in `pre` but is entirely absent
        // from `post`. The merged diff must carry its explicit post-deletion
        // fields (zero balance/nonce, empty code) — and, when the account had
        // storage, zeroed slots — so account-target resyncs resolve from the
        // trace instead of falling back to point reads.
        let trace = serde_json::json!([
            {
                "result": {
                    "pre": {
                        // Deleted WITH storage history in the trace.
                        "0x4242424242424242424242424242424242424242": {
                            "balance": "0x64",
                            "nonce": "0x01",
                            "code": "0x6001",
                            "storage": { "0x01": "0x05" }
                        },
                        // Deleted WITHOUT any storage entry (the previously
                        // missed case).
                        "0x1111111111111111111111111111111111111111": {
                            "balance": "0x0a"
                        }
                    },
                    "post": {}
                }
            }
        ]);

        let diff = parse_block_state_diff_trace(&trace).unwrap();
        assert_eq!(diff.accounts.len(), 2);

        let bare = &diff.accounts[0]; // 0x11.. sorts first
        assert_eq!(bare.address, Address::repeat_byte(0x11));
        assert_eq!(bare.balance, Some(U256::ZERO));
        assert_eq!(bare.nonce, Some(0));
        assert_eq!(bare.code, Some(Bytes::new()));
        assert!(bare.storage.is_empty());

        let stored = &diff.accounts[1];
        assert_eq!(stored.address, Address::repeat_byte(0x42));
        assert_eq!(stored.balance, Some(U256::ZERO));
        assert_eq!(stored.nonce, Some(0));
        assert_eq!(stored.code, Some(Bytes::new()));
        assert_eq!(
            stored.storage,
            vec![BlockStateStorageDiff {
                slot: U256::from(1),
                value: U256::ZERO,
            }]
        );
    }

    #[test]
    fn parses_prestate_diff_trace_deletion_then_recreation_keeps_final_state() {
        // tx1 deletes the account; tx2 re-creates it. Entries merge in tx
        // order, so the final post-block values must win over the synthesized
        // deletion zeros.
        let trace = serde_json::json!([
            {
                "result": {
                    "pre": {
                        "0x4242424242424242424242424242424242424242": { "balance": "0x64" }
                    },
                    "post": {}
                }
            },
            {
                "result": {
                    "pre": {},
                    "post": {
                        "0x4242424242424242424242424242424242424242": {
                            "balance": "0x07",
                            "nonce": "0x01",
                            "code": "0x6002"
                        }
                    }
                }
            }
        ]);

        let diff = parse_block_state_diff_trace(&trace).unwrap();
        assert_eq!(diff.accounts.len(), 1);
        let account = &diff.accounts[0];
        assert_eq!(account.balance, Some(U256::from(7)));
        assert_eq!(account.nonce, Some(1));
        assert_eq!(account.code, Some(Bytes::from(vec![0x60, 0x02])));
    }

    #[test]
    fn snapshot_generation_bumps_on_writes_and_repins_not_prefetch() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));

        let addr = Address::repeat_byte(0x77);
        let g0 = cache.snapshot_generation();

        // Targeted writes bump (magnitude is opaque; assert monotonic change).
        cache.apply_updates(&[StateUpdate::slot(addr, U256::from(1), U256::from(10))]);
        let g1 = cache.snapshot_generation();
        assert!(g1 > g0, "apply_updates must bump the generation");

        cache.apply_update(&StateUpdate::slot(addr, U256::from(2), U256::from(20)));
        let g2 = cache.snapshot_generation();
        assert!(g2 > g1, "apply_update must bump the generation");

        // An empty batch is a no-op, not a mutation.
        cache.apply_updates(&[]);
        assert_eq!(cache.snapshot_generation(), g2);

        // modify_slot on a warm slot bumps.
        let change = cache.modify_slot(addr, U256::from(1), |v| {
            Some(v.unwrap_or_default() + U256::from(1))
        });
        assert!(change.is_some());
        let g3 = cache.snapshot_generation();
        assert!(g3 > g2, "modify_slot must bump the generation");

        // Cold prefetch materializes existing chain state — no bump.
        cache.inject_storage_batch(&[(addr, U256::from(9), U256::from(90))]);
        assert_eq!(
            cache.snapshot_generation(),
            g3,
            "inject_storage_batch is prefetch, not mutation"
        );

        // Block re-pins bump; a same-block set_block is a no-op.
        cache.set_block(BlockId::Number(BlockNumberOrTag::Number(5)));
        let g4 = cache.snapshot_generation();
        assert!(g4 > g3, "set_block to a new pin must bump the generation");
        cache.set_block(BlockId::Number(BlockNumberOrTag::Number(5)));
        assert_eq!(
            cache.snapshot_generation(),
            g4,
            "re-pinning to the same block is not a mutation"
        );

        // advance_block refreshes the env — a spanning snapshot would be
        // inconsistent, so it bumps too.
        let header = alloy_consensus::Header::default();
        cache.advance_block(&header).expect("lenient advance");
        assert!(cache.snapshot_generation() > g4);
    }

    #[test]
    fn test_address_to_u256_conversion() {
        // Test that address conversion preserves the address bytes correctly
        let addr = Address::repeat_byte(0xAB);
        let value = U256::from_be_slice(addr.as_slice());

        // Address is 20 bytes, should be right-aligned in U256 (32 bytes)
        let bytes = value.to_be_bytes::<32>();

        // First 12 bytes should be zero (padding)
        assert_eq!(&bytes[..12], &[0u8; 12]);

        // Last 20 bytes should be the address
        assert_eq!(&bytes[12..], addr.as_slice());
    }

    // ==================== block context tests ====================

    #[test]
    fn new_defaults_to_latest_block_pin() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let cache = rt.block_on(EvmCache::new(Arc::new(provider)));

        assert_eq!(
            cache.block(),
            BlockId::latest(),
            "a default cache must carry an explicit latest block pin, not None"
        );
    }

    #[test]
    fn test_set_block_context_stores_values() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));

        // Initially None
        assert_eq!(cache.block_number(), None);
        assert_eq!(cache.basefee(), None);

        // Set values
        cache.set_block_context(Some(148_252_680), Some(50));
        assert_eq!(cache.block_number(), Some(148_252_680));
        assert_eq!(cache.basefee(), Some(50));

        // Clear values
        cache.set_block_context(None, None);
        assert_eq!(cache.block_number(), None);
        assert_eq!(cache.basefee(), None);
    }

    #[test]
    fn set_block_latest_clears_stale_block_context() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
        cache.set_block_context(Some(148_252_680), Some(50));

        cache.set_block(BlockId::latest());

        assert_eq!(
            cache.block_number(),
            None,
            "tag pins must not retain a stale NUMBER context"
        );
        assert_eq!(
            cache.basefee(),
            None,
            "set_block cannot refresh BASEFEE synchronously, so it must clear stale values"
        );
    }

    #[test]
    fn set_block_latest_clears_stale_context_even_when_pin_unchanged() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
        cache.set_block_context(Some(148_252_680), Some(50));

        cache.set_block(BlockId::latest());

        assert_eq!(
            cache.block_number(),
            None,
            "latest pins must not retain a stale NUMBER context"
        );
        assert_eq!(
            cache.basefee(),
            None,
            "latest pins can drift like tags, so stale BASEFEE must be cleared"
        );
    }

    #[test]
    fn set_block_number_sets_number_and_clears_stale_basefee() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
        cache.set_block_context(Some(100), Some(50));

        cache.set_block(BlockId::Number(BlockNumberOrTag::Number(200)));

        assert_eq!(cache.block_number(), Some(200));
        assert_eq!(
            cache.basefee(),
            None,
            "set_block cannot refresh BASEFEE synchronously, so it must clear stale values"
        );
    }

    #[test]
    fn repin_to_block_clears_stale_basefee() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));
        cache.set_block_context(Some(100), Some(50));

        cache.repin_to_block(200);

        assert_eq!(cache.block_number(), Some(200));
        assert_eq!(
            cache.basefee(),
            None,
            "repin_to_block must not carry stale BASEFEE across blocks"
        );
    }

    #[test]
    fn test_build_evm_applies_block_context() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider)));

        let block_num = 148_252_680u64;
        let basefee_val = 50u64;
        let coinbase = Address::repeat_byte(0xC0);
        let prevrandao = B256::repeat_byte(0x77);
        let gas_limit = 30_000_000u64;
        cache.set_block_context(Some(block_num), Some(basefee_val));
        cache.set_coinbase(Some(coinbase));
        cache.set_prevrandao(Some(prevrandao));
        cache.set_block_gas_limit(Some(gas_limit));

        let evm = cache.build_evm();
        assert_eq!(evm.block.number, U256::from(block_num));
        assert_eq!(evm.block.basefee, basefee_val);
        assert_eq!(evm.block.beneficiary, coinbase);
        assert_eq!(evm.block.prevrandao, Some(prevrandao));
        assert_eq!(evm.block.gas_limit, gas_limit);
    }

    #[test]
    fn test_from_backend_propagates_block_context() {
        use alloy_provider::RootProvider;
        use alloy_rpc_client::RpcClient;
        use alloy_transport::mock::Asserter;

        let asserter = Asserter::new();
        let client = RpcClient::mocked(asserter);
        let provider = RootProvider::<AnyNetwork>::new(client);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let parent = rt.block_on(EvmCache::new(Arc::new(provider)));

        let block_num = Some(148_252_680u64);
        let basefee_val = Some(50u64);
        let child = EvmCache::from_backend(
            parent.unchecked_backend().clone(),
            parent.unchecked_blockchain_db().clone(),
            parent.block(),
            42161,
            block_num,
            basefee_val,
            SpecId::CANCUN,
        );

        assert_eq!(child.block_number(), block_num);
        assert_eq!(child.basefee(), basefee_val);
    }

    #[test]
    fn unix_timestamp_secs_saturating_handles_pre_epoch() {
        let before_epoch = std::time::UNIX_EPOCH - std::time::Duration::from_secs(5);
        assert_eq!(
            unix_timestamp_secs_saturating(before_epoch),
            0,
            "pre-epoch system times must saturate instead of panicking"
        );
    }
}
