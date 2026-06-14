mod binary_state;
mod bytecode;
mod metadata;
pub mod overlay;
pub mod slot_observations;
pub mod snapshot;
#[cfg(feature = "protocols")]
mod storage_keys;
#[cfg(feature = "protocols")]
mod tick_snapshot;

pub use binary_state::{load_binary_state, save_binary_state};
pub use metadata::{
    BalancerPoolMetadata, CacheConfig, ImmutableDataCache, V2PoolMetadata, V3PoolMetadata,
};
pub use overlay::EvmOverlay;
pub use slot_observations::SlotObservationTracker;
pub use snapshot::EvmSnapshot;
#[cfg(feature = "protocols")]
pub use storage_keys::{
    PANCAKE_V3_LIQUIDITY_SLOT, PANCAKE_V3_TICK_BITMAP_BASE_SLOT, PANCAKE_V3_TICKS_BASE_SLOT,
    SLIPSTREAM_LIQUIDITY_SLOT, SLIPSTREAM_SLOT0_SLOT, SLIPSTREAM_TICK_BITMAP_BASE_SLOT,
    SLIPSTREAM_TICKS_BASE_SLOT, V2_RESERVES_SLOT, V3_LIQUIDITY_SLOT, V3_SLOT0_SLOT,
    V3_TICK_BITMAP_BASE_SLOT, V3_TICKS_BASE_SLOT, v3_tick_bitmap_storage_key,
    v3_tick_bitmap_storage_key_with_base, v3_tick_info_storage_keys,
    v3_tick_info_storage_keys_with_base,
};
#[cfg(feature = "protocols")]
pub use tick_snapshot::{SerializableTickInfo, TickInfo, V3PoolTickSnapshot, V3TickSnapshotCache};

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    fs,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::SystemTime,
};

use alloy_consensus::BlockHeader;
use alloy_eips::eip2930::{AccessList, AccessListItem};
use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_network::BlockResponse;
use alloy_primitives::{Address, B256, Bytes, I256, Log, TxKind, U256, keccak256};
use alloy_provider::{Provider, network::AnyNetwork};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Result, anyhow};
use foundry_fork_db::{BlockchainDb, SharedBackend, cache::BlockchainDbMeta};
use revm::{
    Context, ExecuteCommitEvm, ExecuteEvm, InspectEvm, MainBuilder, MainContext,
    context::{BlockEnv, CfgEnv, Journal, LocalContext, TxEnv, result::ExecutionResult},
    context_interface::JournalTr,
    database::CacheDB,
    primitives::hardfork::SpecId,
    state::{AccountInfo, Bytecode},
};
use tracing::{debug, instrument, trace, warn};

use crate::access_set::StorageAccessList;
use crate::errors::{SimError, SimulationError, SimulationResult};
use crate::freshness::SlotChange;
use crate::inspector::TransferInspector;

use bytecode::BytecodeCache;
#[cfg(feature = "protocols")]
use storage_keys::{i128_to_u256, i256_from_i16, i256_from_i24};

/// Re-export AnyNetwork for callers that need to construct providers.
pub use alloy_provider::network::AnyNetwork as AnyNetworkType;

/// The database type used by the EVM cache.
/// CacheDB wraps SharedBackend which lazily fetches data from RPC on-demand.
pub type ForkCacheDB = CacheDB<SharedBackend>;

/// Callback for making direct RPC `eth_call` requests, bypassing revm simulation.
/// Used when batch-querying many contracts where revm's lazy storage fetching would
/// be prohibitively slow (e.g. querying 500+ gauge contracts).
pub type RpcCallFn = Arc<dyn Fn(Address, Bytes) -> Result<Bytes> + Send + Sync>;

/// Callback for batch-fetching storage slots directly from RPC, bypassing SharedBackend.
///
/// Used by V3 tick prefetch to avoid 16K+ individual channel round-trips through
/// SharedBackend. Fires concurrent `eth_getStorageAt` calls directly via the provider
/// and returns results for bulk injection into BlockchainDb.
pub type StorageBatchFetchFn =
    Arc<dyn Fn(Vec<(Address, U256)>) -> Vec<(Address, U256, Result<U256>)> + Send + Sync>;

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
fn block_in_place_handle() -> Result<tokio::runtime::Handle> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => Err(anyhow!(
                "evm-fork-cache RPC operations require a multi-thread tokio runtime; \
                 found a current-thread runtime (block_in_place is not supported there). \
                 Build the runtime with `tokio::runtime::Builder::new_multi_thread()` \
                 or annotate with `#[tokio::main(flavor = \"multi_thread\")]`"
            )),
            _ => Ok(handle),
        },
        Err(e) => Err(anyhow!(
            "evm-fork-cache RPC operations require a running multi-thread tokio runtime: {e}"
        )),
    }
}

static CACHE_SPEED_MODE: AtomicU8 = AtomicU8::new(CacheSpeedMode::Slow as u8);

/// Runtime tuning profile for cache-side batch storage fetches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CacheSpeedMode {
    Fast = 0,
    Normal = 1,
    Slow = 2,
    XSlow = 3,
}

impl CacheSpeedMode {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Fast,
            1 => Self::Normal,
            2 => Self::Slow,
            3 => Self::XSlow,
            _ => Self::Slow,
        }
    }
}

/// Set the global cache batch-fetch speed profile.
pub fn set_cache_speed_mode(mode: CacheSpeedMode) {
    CACHE_SPEED_MODE.store(mode as u8, Ordering::Relaxed);
}

/// Return the current global cache batch-fetch speed profile.
pub fn cache_speed_mode() -> CacheSpeedMode {
    CacheSpeedMode::from_u8(CACHE_SPEED_MODE.load(Ordering::Relaxed))
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
    /// Native value (wei) sent with the call.
    pub value: U256,
    /// Gas limit; `None` uses revm's default.
    pub gas_limit: Option<u64>,
    /// Gas price (wei); `None` uses revm's default.
    pub gas_price: Option<u128>,
    /// Sender nonce; `None` lets the simulator pick (nonce checks are disabled).
    pub nonce: Option<u64>,
    /// EIP-2930 access list to pre-warm slots for this call.
    pub access_list: Option<AccessList>,
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
/// # async fn example() -> anyhow::Result<()> {
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
    block: Option<BlockId>,
    cache_config: Option<CacheConfig>,
    spec_id: SpecId,
}

impl<P> EvmCacheBuilder<P>
where
    P: Provider<AnyNetwork> + 'static,
{
    /// Start a builder over the given provider.
    pub fn new(provider: Arc<P>) -> Self {
        Self {
            provider,
            block: None,
            cache_config: None,
            spec_id: SpecId::CANCUN,
        }
    }

    /// Pin simulations and RPC fetches to a specific block.
    pub fn block(mut self, block: BlockId) -> Self {
        self.block = Some(block);
        self
    }

    /// Pin to the latest block.
    pub fn latest_block(mut self) -> Self {
        self.block = Some(BlockId::latest());
        self
    }

    /// Set the EVM hardfork spec (must match the chain's execution layer).
    pub fn spec(mut self, spec_id: SpecId) -> Self {
        self.spec_id = spec_id;
        self
    }

    /// Enable disk-backed caching with the given configuration.
    pub fn cache_config(mut self, cache_config: CacheConfig) -> Self {
        self.cache_config = Some(cache_config);
        self
    }

    /// Build the [`EvmCache`], fetching the pinned block's header for context.
    pub async fn build(self) -> EvmCache {
        EvmCache::with_cache(self.provider, self.block, self.cache_config, self.spec_id).await
    }
}

type CacheEvm<'a> = revm::MainnetEvm<
    Context<BlockEnv, TxEnv, CfgEnv, &'a mut ForkCacheDB, Journal<&'a mut ForkCacheDB>, ()>,
>;
type InspectorCacheEvm<'a, INSP> = revm::MainnetEvm<
    Context<BlockEnv, TxEnv, CfgEnv, &'a mut ForkCacheDB, Journal<&'a mut ForkCacheDB>, ()>,
    INSP,
>;

/// Default initial capacity for shared memory buffer.
/// Set to 64KB based on profiling (16x the REVM default of 4KB).
/// This eliminates reallocation during typical simulations with headroom.
const DEFAULT_SHARED_MEMORY_CAPACITY: usize = 64 * 1024;

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
    block: Option<BlockId>,
    cache_config: Option<CacheConfig>,
    /// Cache for immutable on-chain data (token decimals, pool metadata).
    immutable_cache: ImmutableDataCache,
    /// Cache for V3 pool tick snapshots (tick_bitmap, ticks, liquidity).
    #[cfg(feature = "protocols")]
    tick_snapshot_cache: V3TickSnapshotCache,
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
    /// Shared memory buffer reused across EVM simulations.
    /// This avoids repeated allocations and allows measuring peak memory usage.
    shared_memory_buffer: Rc<RefCell<Vec<u8>>>,
    /// Optional callback for direct RPC `eth_call` (bypasses revm simulation).
    /// Set during construction from the provider. Useful for batch operations
    /// where revm's lazy storage fetching would be too slow.
    rpc_caller: Option<RpcCallFn>,
    /// Optional batch storage fetcher that bypasses SharedBackend.
    /// Captures a provider clone and fires concurrent `eth_getStorageAt` calls directly.
    storage_batch_fetcher: Option<StorageBatchFetchFn>,
    /// Shared block ID for the batch storage fetcher closure.
    /// Updated by `set_block()` so batch fetches always use the current block.
    batch_block_id: Arc<Mutex<BlockId>>,
    /// Best-known ERC20 `balanceOf` mapping slot per token contract.
    ///
    /// Used by `set_erc20_balance_with_slot_scan` to avoid re-scanning slots
    /// repeatedly for the same token.
    erc20_balance_slots: HashMap<Address, U256>,
    /// EVM hardfork spec for simulations. Must match the chain's current execution
    /// layer hardfork for accurate gas accounting. Configured per-chain via `evm_spec`
    /// in `chains.toml`.
    spec_id: SpecId,
}

#[derive(Clone, Debug)]
pub struct CallSimulationResult {
    pub gas_used: u64,
    pub token_deltas: HashMap<Address, I256>,
    pub logs: Vec<Log>,
    /// EIP-2930 access list of all accounts and storage slots touched during simulation.
    /// Extracted from the EVM journaled state after execution.
    pub access_list: AccessList,
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
    pub async fn new<P>(provider: Arc<P>, block: Option<BlockId>) -> Self
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
    /// 3. Tick snapshots: V3 pool tick data for validation
    /// 4. Immutable data: Token decimals, pool metadata
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
        block: Option<BlockId>,
        cache_config: Option<CacheConfig>,
        spec_id: SpecId,
    ) -> Self
    where
        P: Provider<AnyNetwork> + 'static,
    {
        let block_id = block.unwrap_or_default();

        // Fetch block header for accurate block context (NUMBER, BASEFEE opcodes).
        // Without this, revm defaults to 0 for both, causing contracts that read
        // block.number or block.basefee to execute different code paths.
        let (block_number, basefee, coinbase, prevrandao, block_gas_limit) = match provider
            .get_block_by_number(match block_id {
                BlockId::Number(n) => n,
                _ => BlockNumberOrTag::Latest,
            })
            .await
        {
            Ok(Some(blk)) => {
                let h = blk.header();
                (
                    Some(h.number()),
                    h.base_fee_per_gas(),
                    Some(h.beneficiary()),
                    h.mix_hash(),
                    Some(h.gas_limit()),
                )
            }
            Ok(None) => {
                debug!("Block header not found for block context initialization");
                (None, None, None, None, None)
            }
            Err(e) => {
                debug!(error = %e, "Failed to fetch block header for block context");
                (None, None, None, None, None)
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

        // Load immutable data cache (token decimals, pool metadata)
        // This is still needed for validation and metadata lookups
        let immutable_cache = cache_config
            .as_ref()
            .and_then(|cfg| {
                let path = cfg.immutable_cache_path();
                ImmutableDataCache::load(&path).inspect(|cache| {
                    debug!(
                        token_decimals = cache.token_decimals.len(),
                        v2_pools = cache.v2_pools.len(),
                        v3_pools = cache.v3_pools.len(),
                        balancer_pools = cache.balancer_pools.len(),
                        path = ?path,
                        "Loaded immutable data from cache"
                    );
                })
            })
            .unwrap_or_default();

        // Pre-populate in-memory token decimals from immutable cache
        let token_decimals = immutable_cache.token_decimals.clone();

        // Load V3 tick snapshot cache (for liquidity validation)
        #[cfg(feature = "protocols")]
        let tick_snapshot_cache = cache_config
            .as_ref()
            .and_then(|cfg| {
                let path = cfg.tick_snapshot_cache_path();
                V3TickSnapshotCache::load(&path).inspect(|cache| {
                    debug!(
                        snapshots = cache.len(),
                        path = ?path,
                        "Loaded V3 tick snapshots from cache"
                    );
                })
            })
            .unwrap_or_default();

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
                        .map_err(|e| anyhow!("{}", e))
                })
            })
        });

        // Create a batch storage fetcher that bypasses SharedBackend for bulk prefetch.
        // Uses JSON-RPC batch requests to send multiple eth_getStorageAt calls in a
        // single HTTP request, dramatically reducing round-trip overhead.
        let provider_for_batch = provider.clone();
        let batch_block_id = Arc::new(Mutex::new(block_id));
        let batch_block_ref = batch_block_id.clone();
        let storage_batch_fetcher: StorageBatchFetchFn =
            Arc::new(move |requests: Vec<(Address, U256)>| {
                use futures::stream::{self, StreamExt};
                // Max items per JSON-RPC batch. RPC providers typically limit batch
                // size to ~1000 items. Reduced from 200 to avoid 429s on Base.
                let batch_size: usize = match cache_speed_mode() {
                    CacheSpeedMode::Fast => 150,
                    CacheSpeedMode::Normal => 100,
                    CacheSpeedMode::Slow => 75,
                    CacheSpeedMode::XSlow => 25,
                };
                // Max concurrent HTTP batch requests. Each batch contains batch_size
                // individual eth_getStorageAt calls. Limiting concurrency prevents
                // thundering herd when prefetching thousands of storage slots.
                let max_concurrent: usize = match cache_speed_mode() {
                    CacheSpeedMode::Fast => 8,
                    CacheSpeedMode::Normal => 6,
                    CacheSpeedMode::Slow => 4,
                    CacheSpeedMode::XSlow => 1,
                };

                // Guard against panicking inside `block_in_place` on a
                // current-thread runtime (or when no runtime is present): return
                // an `Err` result for every requested slot instead.
                let handle = match block_in_place_handle() {
                    Ok(handle) => handle,
                    Err(e) => {
                        let msg = e.to_string();
                        return requests
                            .into_iter()
                            .map(|(addr, slot)| (addr, slot, Err(anyhow!("{}", msg))))
                            .collect();
                    }
                };
                let current_block = *batch_block_ref.lock().unwrap();
                tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        let mut results = Vec::with_capacity(requests.len());

                        // Build and send JSON-RPC batches (each batch = one HTTP request)
                        let batch_futs: Vec<_> = requests
                            .chunks(batch_size)
                            .map(|chunk| {
                                let client = provider_for_batch.client();
                                let mut batch = alloy_rpc_client::BatchRequest::new(client);
                                let mut waiters = Vec::with_capacity(chunk.len());

                                for &(addr, slot) in chunk {
                                    let params = (addr, slot, current_block);
                                    match batch.add_call::<_, U256>("eth_getStorageAt", &params) {
                                        Ok(waiter) => waiters.push((addr, slot, Some(waiter))),
                                        Err(e) => {
                                            // Serialization error — rare, treat as failure
                                            tracing::warn!(
                                                ?addr,
                                                ?slot,
                                                "batch request serialization failed: {}",
                                                e
                                            );
                                            waiters.push((addr, slot, None));
                                        }
                                    }
                                }

                                async move {
                                    // Send the batch as a single HTTP request
                                    let send_result = batch.send().await;
                                    let mut chunk_results = Vec::with_capacity(waiters.len());

                                    for (addr, slot, waiter) in waiters {
                                        if let Some(waiter) = waiter {
                                            if send_result.is_ok() {
                                                match waiter.await {
                                                    Ok(value) => {
                                                        chunk_results.push((addr, slot, Ok(value)));
                                                    }
                                                    Err(e) => {
                                                        chunk_results.push((
                                                            addr,
                                                            slot,
                                                            Err(anyhow!("{}", e)),
                                                        ));
                                                    }
                                                }
                                            } else {
                                                chunk_results.push((
                                                    addr,
                                                    slot,
                                                    Err(anyhow!("batch send failed")),
                                                ));
                                            }
                                        } else {
                                            chunk_results.push((
                                                addr,
                                                slot,
                                                Err(anyhow!("serialization failed")),
                                            ));
                                        }
                                    }
                                    chunk_results
                                }
                            })
                            .collect();

                        // Fire batches with bounded concurrency (`max_concurrent`) to avoid
                        // a thundering herd; per-batch size is the speed-mode `batch_size`
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
            });

        // Spawn the backend handler on a background task
        let backend =
            SharedBackend::spawn_backend(provider, blockchain_db.clone(), Some(block_id)).await;

        let db = CacheDB::new(backend.clone());

        // Extract chain_id from cache config if available, default to Arbitrum
        let chain_id = cache_config.as_ref().map(|c| c.chain_id).unwrap_or(42161);

        Self {
            backend,
            blockchain_db,
            db,
            token_decimals,
            block,
            cache_config,
            immutable_cache,
            #[cfg(feature = "protocols")]
            tick_snapshot_cache,
            timestamp_override: None,
            chain_id,
            block_number,
            basefee,
            coinbase,
            prevrandao,
            block_gas_limit,
            shared_memory_buffer: Rc::new(RefCell::new(Vec::with_capacity(
                DEFAULT_SHARED_MEMORY_CAPACITY,
            ))),
            rpc_caller: Some(rpc_caller),
            storage_batch_fetcher: Some(storage_batch_fetcher),
            batch_block_id,
            erc20_balance_slots: HashMap::new(),
            spec_id,
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
    pub fn from_backend(
        backend: SharedBackend,
        blockchain_db: BlockchainDb,
        block: Option<BlockId>,
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
            #[cfg(feature = "protocols")]
            tick_snapshot_cache: V3TickSnapshotCache::default(),
            timestamp_override: None,
            chain_id,
            block_number,
            basefee,
            coinbase: None,
            prevrandao: None,
            block_gas_limit: None,
            shared_memory_buffer: Rc::new(RefCell::new(Vec::with_capacity(
                DEFAULT_SHARED_MEMORY_CAPACITY,
            ))),
            rpc_caller: None,
            storage_batch_fetcher: None,
            batch_block_id: Arc::new(Mutex::new(block.unwrap_or_default())),
            erc20_balance_slots: HashMap::new(),
            spec_id,
        }
    }

    /// Flush the cache state to disk.
    ///
    /// This persists:
    /// 1. Unified EVM state (accounts + storage) to `evm_state.bin` (bincode)
    /// 2. Contract bytecodes to `bytecodes.bin`
    /// 3. Immutable data (token decimals, pool metadata) to `immutable_data.bin`
    /// 4. V3 tick snapshots to `v3_tick_snapshots.bin`
    ///
    /// Call this after loading AMMs and running simulations to speed up subsequent runs.
    /// The cache is also automatically flushed when the EvmCache is dropped.
    pub fn flush(&self) {
        if let Some(cfg) = &self.cache_config {
            // Save EVM state to binary cache (bincode format)
            let binary_path = cfg.binary_state_cache_path();
            binary_state::save_binary_state(&self.blockchain_db, &binary_path);

            // Save bytecode cache
            let bytecode_path = cfg.bytecode_cache_path();
            let mut bytecode_cache = BytecodeCache::load(&bytecode_path).unwrap_or_default();
            bytecode_cache.merge_from_db(&self.blockchain_db);
            if let Err(e) = bytecode_cache.save(&bytecode_path) {
                warn!(error = %e, "Failed to save bytecode cache");
            } else {
                debug!(
                    count = bytecode_cache.contracts.len(),
                    path = ?bytecode_path,
                    "Updated bytecode cache (binary format)"
                );
            }

            // Save the immutable data cache
            let immutable_path = cfg.immutable_cache_path();
            if let Err(e) = self.immutable_cache.save(&immutable_path) {
                warn!(error = %e, "Failed to save immutable data cache");
            } else {
                debug!(
                    token_decimals = self.immutable_cache.token_decimals.len(),
                    v2_pools = self.immutable_cache.v2_pools.len(),
                    v3_pools = self.immutable_cache.v3_pools.len(),
                    balancer_pools = self.immutable_cache.balancer_pools.len(),
                    path = ?immutable_path,
                    "Updated immutable data cache"
                );
            }

            // Save the V3 tick snapshot cache (needed for liquidity validation)
            #[cfg(feature = "protocols")]
            {
                let tick_snapshot_path = cfg.tick_snapshot_cache_path();
                if let Err(e) = self.tick_snapshot_cache.save(&tick_snapshot_path) {
                    warn!(error = %e, "Failed to save V3 tick snapshot cache");
                } else {
                    debug!(
                        snapshots = self.tick_snapshot_cache.len(),
                        path = ?tick_snapshot_path,
                        "Updated V3 tick snapshot cache"
                    );
                }
            }
        }
    }

    /// Get the cache configuration, if any.
    pub fn cache_config(&self) -> Option<&CacheConfig> {
        self.cache_config.as_ref()
    }

    /// Get a reference to the underlying BlockchainDb.
    pub fn blockchain_db(&self) -> &BlockchainDb {
        &self.blockchain_db
    }

    /// Get a reference to the underlying SharedBackend.
    pub fn backend(&self) -> &SharedBackend {
        &self.backend
    }

    /// Get a mutable reference to the database.
    pub fn db_mut(&mut self) -> &mut ForkCacheDB {
        &mut self.db
    }

    /// Make a direct RPC `eth_call` to the node, bypassing revm simulation.
    ///
    /// This is much faster than `call_raw` for batch operations because the RPC
    /// node has all state in memory and doesn't need lazy storage fetching.
    /// Returns `None` if no RPC caller is available (e.g. `from_backend` constructor).
    pub fn rpc_call(&self, to: Address, calldata: Bytes) -> Option<Result<Bytes>> {
        self.rpc_caller
            .as_ref()
            .map(|caller| (caller)(to, calldata))
    }

    /// Get the batch storage fetcher, if available.
    ///
    /// Returns `None` when constructed via `from_backend` (no provider available).
    pub fn storage_batch_fetcher(&self) -> Option<&StorageBatchFetchFn> {
        self.storage_batch_fetcher.as_ref()
    }

    /// Inject batch-fetched storage values directly into BlockchainDb.
    ///
    /// This bypasses SharedBackend and makes values available for subsequent
    /// `storage_ref()` calls and EVM SLOADs. Used after `StorageBatchFetchFn`
    /// returns results to populate the cache in bulk.
    pub fn inject_storage_batch(&self, results: &[(Address, U256, U256)]) {
        let mut storage = self.blockchain_db.storage().write();
        for &(addr, slot, value) in results {
            storage.entry(addr).or_default().insert(slot, value);
        }
    }

    /// Set (or replace) the batch storage fetcher.
    ///
    /// This is the seam the freshness controller and tests use to drive
    /// re-verification without a live provider: a stubbed
    /// [`StorageBatchFetchFn`] can be injected over a mocked-provider cache.
    pub fn set_storage_batch_fetcher(&mut self, f: StorageBatchFetchFn) {
        self.storage_batch_fetcher = Some(f);
    }

    /// Return the currently-cached value for a storage slot, if any.
    ///
    /// Checks the CacheDB overlay (layer 1) first, then the BlockchainDb backend
    /// (layer 2). Returns `None` when neither layer has seen the slot. Unlike
    /// [`read_storage_slot`](Self::read_storage_slot) this never touches RPC.
    pub fn cached_storage_value(&self, address: Address, slot: U256) -> Option<U256> {
        if let Some(db_account) = self.db.cache.accounts.get(&address)
            && let Some(value) = db_account.storage.get(&slot)
        {
            return Some(*value);
        }
        let storage = self.blockchain_db.storage().read();
        storage.get(&address).and_then(|s| s.get(&slot).copied())
    }

    /// Re-fetch the given slots via the batch fetcher, compare to the currently
    /// cached values, and inject the ones that changed.
    ///
    /// For each slot whose freshly-fetched value differs from the cached value,
    /// the fresh value is written into the cache via
    /// [`inject_storage_batch`](Self::inject_storage_batch) and a [`SlotChange`]
    /// is recorded. Slots that are unchanged, or that the fetcher fails to
    /// return, are left as-is. Returns the set of changed slots.
    ///
    /// Requires a batch fetcher (set at construction or via
    /// [`set_storage_batch_fetcher`](Self::set_storage_batch_fetcher)); errors if
    /// none is available. This is the synchronous main-thread primitive; the
    /// background validator performs the equivalent comparison against a snapshot.
    pub fn verify_slots(&mut self, slots: &[(Address, U256)]) -> Result<Vec<SlotChange>> {
        if slots.is_empty() {
            return Ok(Vec::new());
        }
        let fetcher = self
            .storage_batch_fetcher
            .as_ref()
            .ok_or_else(|| anyhow!("verify_slots requires a storage batch fetcher"))?
            .clone();

        // Snapshot the cached values before fetching so we compare against a
        // stable baseline.
        let cached: HashMap<(Address, U256), Option<U256>> = slots
            .iter()
            .map(|&(addr, slot)| ((addr, slot), self.cached_storage_value(addr, slot)))
            .collect();

        let results = (fetcher)(slots.to_vec());

        let mut changed = Vec::new();
        let mut to_inject = Vec::new();
        for (addr, slot, fetched) in results {
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
            self.inject_storage_batch(&to_inject);
        }
        Ok(changed)
    }

    /// Purge an account fully from both cache layers: its `AccountInfo`
    /// (balance/nonce/code hash) **and** all of its storage.
    ///
    /// Removes `addr` from the CacheDB overlay accounts map, the BlockchainDb
    /// accounts map, and the BlockchainDb storage map, so the next access
    /// re-fetches a clean account from RPC. This is the account-level
    /// counterpart to the storage-only [`purge_pool_storage`](Self::purge_pool_storage):
    /// use it when an address is fully volatile (no pinned slots) and even its
    /// balance/nonce/code can no longer be trusted.
    pub fn purge_account(&mut self, addr: Address) {
        // Layer 1: CacheDB overlay (accounts + their storage live together).
        let overlay_removed = self.db.cache.accounts.remove(&addr).is_some();

        // Layer 2: BlockchainDb accounts + storage maps.
        let backend_account_removed = self
            .blockchain_db
            .accounts()
            .write()
            .remove(&addr)
            .is_some();
        let backend_storage_removed = self.blockchain_db.storage().write().remove(&addr).is_some();

        if overlay_removed || backend_account_removed || backend_storage_removed {
            debug!(
                account = %addr,
                overlay_removed,
                backend_account_removed,
                backend_storage_removed,
                "purged account from both cache layers"
            );
        }
    }

    /// Get the chain ID used for EVM simulations.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Create a snapshot of the current cache state for later restoration.
    ///
    /// Note: This creates a copy of the inner cache only (accounts and storage),
    /// not the underlying database wrapper.
    pub fn snapshot(&self) -> revm::database::Cache {
        self.db.cache.clone()
    }

    /// Restore the cache state from a previous snapshot.
    pub fn restore(&mut self, snapshot: revm::database::Cache) {
        self.db.cache = snapshot;
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

    /// Create an immutable snapshot of the current EVM state.
    ///
    /// Merges both layers (CacheDB overlay + BlockchainDb backend) into a
    /// single flat HashMap. The snapshot is `Send + Sync` and can be shared
    /// across threads via `Arc`.
    ///
    /// CacheDB overlay values take precedence over BlockchainDb values.
    /// Use with [`EvmOverlay`] for parallel simulation.
    pub fn create_snapshot(&self) -> Arc<snapshot::EvmSnapshot> {
        let mut accounts = HashMap::new();
        let mut storage = HashMap::new();
        let mut code_by_hash = HashMap::new();

        // 1. Load from BlockchainDb (persistent cache / Layer 2)
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
                // Convert from DefaultHashBuilder to RandomState HashMap
                let converted: HashMap<U256, U256> = slots.iter().map(|(k, v)| (*k, *v)).collect();
                storage.insert(*addr, converted);
            }
        }

        // 2. Overlay from CacheDB (Layer 1, takes precedence)
        for (addr, db_account) in &self.db.cache.accounts {
            if let Some(code) = &db_account.info.code {
                code_by_hash.insert(db_account.info.code_hash, code.clone());
            }
            accounts.insert(*addr, db_account.info.clone());
            let account_storage = storage.entry(*addr).or_default();
            for (slot, value) in &db_account.storage {
                account_storage.insert(*slot, *value);
            }
        }

        Arc::new(snapshot::EvmSnapshot {
            accounts,
            storage,
            block_hashes: HashMap::new(),
            code_by_hash,
            block_number: self.block_number,
            basefee: self.basefee,
            coinbase: self.coinbase,
            prevrandao: self.prevrandao,
            gas_limit: self.block_gas_limit,
            chain_id: self.chain_id,
            timestamp: self.timestamp_override,
            spec_id: self.spec_id,
        })
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
    /// ids (`latest`, `pending`, hashes, etc.) the height is not statically known,
    /// so `block_number` is left unchanged.
    ///
    /// `basefee` (the `BASEFEE` opcode) is **not** refreshed here because deriving
    /// it requires fetching the block header, which this synchronous method cannot
    /// do. Callers that change blocks should refresh it via
    /// [`set_block_context`](Self::set_block_context) (e.g. after fetching the new
    /// header). Prefer [`repin_to_block`](Self::repin_to_block) when re-pinning to
    /// a concrete height, since it keeps `block_number` and the pinned block in
    /// lockstep.
    pub fn set_block(&mut self, block: Option<BlockId>) {
        if self.block != block {
            self.block = block;
            if let Some(block_id) = block {
                let _ = self.backend.set_pinned_block(block_id);
                *self.batch_block_id.lock().unwrap() = block_id;
                // Keep the EVM `NUMBER` opcode aligned with the pinned block so the
                // two cannot silently diverge. Only a concrete height is meaningful;
                // tags (latest/pending/hash) leave `block_number` untouched.
                if let BlockId::Number(BlockNumberOrTag::Number(n)) = block_id {
                    self.block_number = Some(n);
                }
            }
        }
    }

    /// Get the current block.
    pub fn block(&self) -> Option<BlockId> {
        self.block
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

    /// Get the block number used for EVM simulations (NUMBER opcode).
    pub fn block_number(&self) -> Option<u64> {
        self.block_number
    }

    /// Get the base fee used for EVM simulations (BASEFEE opcode).
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

    /// Override the block beneficiary (COINBASE opcode) for subsequent simulations.
    pub fn set_coinbase(&mut self, coinbase: Option<Address>) {
        self.coinbase = coinbase;
    }

    /// Override `prevrandao` (PREVRANDAO opcode) for subsequent simulations.
    pub fn set_prevrandao(&mut self, prevrandao: Option<B256>) {
        self.prevrandao = prevrandao;
    }

    /// Override the block gas limit (GASLIMIT opcode) for subsequent simulations.
    pub fn set_block_gas_limit(&mut self, gas_limit: Option<u64>) {
        self.block_gas_limit = gas_limit;
    }

    /// Re-pin the cache to a specific block number.
    ///
    /// Updates the SharedBackend pinned block, the batch fetcher block, and the
    /// EVM block context (`NUMBER` opcode) in lockstep. The current `basefee` is
    /// preserved; callers should refresh it via
    /// [`set_block_context`](Self::set_block_context) after fetching the new
    /// block header if `BASEFEE` accuracy matters.
    pub fn repin_to_block(&mut self, block_number: u64) {
        let old_block = self.block;
        // `set_block` already updates `block_number` for a concrete height; the
        // explicit `set_block_context` below preserves `basefee` and keeps the
        // re-pin atomic and self-documenting.
        self.set_block(Some(BlockId::Number(block_number.into())));
        self.set_block_context(Some(block_number), self.basefee);

        if let Some(BlockId::Number(BlockNumberOrTag::Number(old_num))) = old_block {
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
            .map_err(|e| anyhow!("Failed to fetch account: {:?}", e))?;

        if let Some(info) = info {
            self.db.insert_account_info(address, info);
        }

        Ok(())
    }

    /// Read a single storage slot through the SharedBackend (BlockchainDb -> RPC fallback).
    ///
    /// After `purge_pool_slots` removes a slot from BlockchainDb, this method fetches
    /// fresh data from RPC and caches it in BlockchainDb. Subsequent EVM SLOADs find
    /// the value there without additional RPC calls.
    pub fn read_storage_slot(&mut self, address: Address, slot: U256) -> Result<U256> {
        use revm::database_interface::DatabaseRef;
        self.backend
            .storage_ref(address, slot)
            .map_err(|e| anyhow!("storage read failed for {address} slot {slot}: {e}"))
    }

    /// Write a raw storage slot value directly into the CacheDB layer.
    ///
    /// Subsequent EVM SLOADs for this (address, slot) will read the injected value
    /// without any RPC call. Used for hot-state injection where we already know the
    /// current on-chain value from WebSocket events.
    pub fn insert_storage_slot(&mut self, address: Address, slot: U256, value: U256) -> Result<()> {
        self.db.insert_account_storage(address, slot, value)?;
        Ok(())
    }

    /// Pre-seed known ERC20 balance mapping slots so that `set_erc20_balance_with_slot_scan`
    /// can skip the scanning step for these tokens.
    pub fn seed_erc20_balance_slots(&mut self, slots: impl IntoIterator<Item = (Address, U256)>) {
        for (token, slot) in slots {
            self.erc20_balance_slots.insert(token, slot);
        }
    }

    pub fn insert_mapping_storage_slot(
        &mut self,
        contract: Address,
        slot: U256,
        slot_address: Address,
        value: U256,
    ) -> Result<()> {
        let hashed_balance_slot = keccak256((slot_address, slot).abi_encode());
        self.db
            .insert_account_storage(contract, hashed_balance_slot.into(), value)?;
        Ok(())
    }

    /// Set an ERC20 balance by probing storage mapping slots until `balanceOf(owner)` reflects
    /// a probe value, then writing `amount` to the discovered slot.
    ///
    /// Returns `Ok(true)` if the balance was set and verified, `Ok(false)` if no slot in
    /// `0..=max_slot` matched, and `Err` on EVM/cache failures.
    pub fn set_erc20_balance_with_slot_scan(
        &mut self,
        token: Address,
        owner: Address,
        amount: U256,
        max_slot: u16,
    ) -> Result<bool> {
        if let Some(slot) = self.erc20_balance_slots.get(&token).copied() {
            self.insert_mapping_storage_slot(token, slot, owner, amount)?;
            if self.erc20_balance_of(token, owner)? == amount {
                return Ok(true);
            }
            self.erc20_balance_slots.remove(&token);
        }

        let Some(discovered_slot) =
            self.discover_erc20_balance_slot_with_scan(token, owner, max_slot)?
        else {
            return Ok(false);
        };

        self.insert_mapping_storage_slot(token, discovered_slot, owner, amount)?;
        let verified = self.erc20_balance_of(token, owner)? == amount;
        if verified {
            self.erc20_balance_slots.insert(token, discovered_slot);
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
        if let Some(slot) = self.erc20_balance_slots.get(&token).copied() {
            return Ok(Some(slot));
        }

        let baseline_snapshot = self.snapshot();
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
                self.erc20_balance_slots.insert(token, slot);
                return Ok(Some(slot));
            }
        }

        self.restore(baseline_snapshot);
        Ok(None)
    }

    /// Inject UniswapV2 pool metadata (token0, token1) directly into the EVM storage cache.
    ///
    /// This allows subsequent EVM calls to `token0()` and `token1()` to hit the
    /// local cache instead of fetching from RPC.
    ///
    /// # Storage Layout
    /// In UniswapV2Pair:
    /// - Slot 6: token0 (address)
    /// - Slot 7: token1 (address)
    ///
    /// # Arguments
    /// * `pool_address` - The UniswapV2 pair contract address
    /// * `metadata` - The cached pool metadata containing token0 and token1
    #[cfg(feature = "protocols")]
    pub fn inject_v2_pool_metadata(
        &mut self,
        pool_address: Address,
        metadata: &V2PoolMetadata,
    ) -> Result<()> {
        const TOKEN0_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);
        const TOKEN1_SLOT: U256 = U256::from_limbs([7, 0, 0, 0]);

        // Addresses are stored as 20 bytes right-aligned in a 32-byte slot
        let token0_value = U256::from_be_slice(metadata.token0.as_slice());
        let token1_value = U256::from_be_slice(metadata.token1.as_slice());

        self.db
            .insert_account_storage(pool_address, TOKEN0_SLOT, token0_value)?;
        self.db
            .insert_account_storage(pool_address, TOKEN1_SLOT, token1_value)?;

        Ok(())
    }

    /// Inject UniswapV3 tickBitmap data directly into the EVM storage cache.
    ///
    /// This allows subsequent EVM calls to `tickBitmap(wordPosition)` to hit the
    /// local cache instead of fetching from RPC.
    ///
    /// # Storage Layout
    /// In UniswapV3Pool, `tickBitmap` is a `mapping(int16 => uint256)` at storage slot 6.
    /// For a mapping at slot `p`, the value for key `k` is stored at `keccak256(abi.encode(k, p))`.
    ///
    /// # Arguments
    /// * `pool_address` - The UniswapV3 pool contract address
    /// * `tick_bitmap` - Map of word position (int16) to bitmap value (uint256)
    #[cfg(feature = "protocols")]
    pub fn inject_v3_tick_bitmap(
        &mut self,
        pool_address: Address,
        tick_bitmap: &std::collections::HashMap<i16, U256>,
    ) -> Result<usize> {
        self.inject_v3_tick_bitmap_with_base(pool_address, tick_bitmap, V3_TICK_BITMAP_BASE_SLOT)
    }

    /// Inject V3-style tick bitmap data with a custom base slot.
    ///
    /// PancakeSwap V3 uses base slot 7 instead of Uniswap V3's slot 6.
    #[cfg(feature = "protocols")]
    pub fn inject_v3_tick_bitmap_with_base(
        &mut self,
        pool_address: Address,
        tick_bitmap: &std::collections::HashMap<i16, U256>,
        base_slot: U256,
    ) -> Result<usize> {
        let mut injected = 0;
        for (&word_position, &bitmap_value) in tick_bitmap {
            let word_position_i256 = i256_from_i16(word_position);
            let mut slot_preimage = [0u8; 64];
            slot_preimage[..32].copy_from_slice(&word_position_i256);
            slot_preimage[32..64].copy_from_slice(&base_slot.to_be_bytes::<32>());
            let storage_slot: U256 = keccak256(slot_preimage).into();
            self.db
                .insert_account_storage(pool_address, storage_slot, bitmap_value)?;
            injected += 1;
        }
        Ok(injected)
    }

    /// Inject UniswapV3 tick info data directly into the EVM storage cache.
    ///
    /// This allows subsequent EVM calls to `ticks(tick)` to partially hit the
    /// local cache instead of fetching all data from RPC.
    ///
    /// # Storage Layout
    /// In UniswapV3Pool, `ticks` is a `mapping(int24 => Tick.Info)` at storage slot 5.
    /// `Tick.Info` is a struct that spans 4 storage slots:
    ///
    /// - Slot +0: `liquidityGross` (u128, bits 0-127) | `liquidityNet` (i128, bits 128-255)
    /// - Slot +1: `feeGrowthOutside0X128` (u256)
    /// - Slot +2: `feeGrowthOutside1X128` (u256)
    /// - Slot +3: packed (`tickCumulativeOutside`, `secondsPerLiquidityOutsideX128`,
    ///   `secondsOutside`, `initialized`)
    ///
    /// We inject slot 0 (liquidityGross + liquidityNet) and slot 3 (initialized flag).
    /// Slot 0 covers the most critical data used in swap simulation.
    /// Slot 3 contains the `initialized` flag which determines whether a tick
    /// is processed during swap execution -- stale values from Layer 2 can cause
    /// ticks to be erroneously skipped or processed.
    ///
    /// # Arguments
    /// * `pool_address` - The UniswapV3 pool contract address
    /// * `ticks` - Map of tick index (int24) to tick info
    #[cfg(feature = "protocols")]
    pub fn inject_v3_ticks(
        &mut self,
        pool_address: Address,
        ticks: &std::collections::HashMap<i32, TickInfo>,
    ) -> Result<usize> {
        self.inject_v3_ticks_with_base(pool_address, ticks, V3_TICKS_BASE_SLOT)
    }

    /// Inject V3-style tick info data with a custom ticks mapping slot.
    ///
    /// PancakeSwap V3 uses ticks at slot 6 instead of Uniswap V3's slot 5.
    #[cfg(feature = "protocols")]
    pub fn inject_v3_ticks_with_base(
        &mut self,
        pool_address: Address,
        ticks: &std::collections::HashMap<i32, TickInfo>,
        ticks_slot: U256,
    ) -> Result<usize> {
        let mut injected = 0;
        for (&tick, info) in ticks {
            let tick_i256 = i256_from_i24(tick);
            let mut slot_preimage = [0u8; 64];
            slot_preimage[..32].copy_from_slice(&tick_i256);
            slot_preimage[32..64].copy_from_slice(&ticks_slot.to_be_bytes::<32>());

            let base_slot: U256 = keccak256(slot_preimage).into();

            // Pack liquidityGross and liquidityNet into slot 0
            // Solidity packing: liquidityGross in lower 128 bits, liquidityNet in upper 128 bits
            let liquidity_gross_u256 = U256::from(info.liquidity_gross);
            let liquidity_net_u256 = i128_to_u256(info.liquidity_net);
            let packed_slot0 = liquidity_gross_u256 | (liquidity_net_u256 << 128);

            self.db
                .insert_account_storage(pool_address, base_slot, packed_slot0)?;

            // Also inject slot 3 with the `initialized` flag.
            // Slot 3 layout: packed (tickCumulativeOutside, secondsPerLiquidityOutsideX128,
            //                        secondsOutside, initialized)
            // The `initialized` flag is in the highest byte (bit 248+).
            // We only set the initialized flag; the other fields in slot 3 are
            // not used by swap simulation, but without this injection the EVM
            // would read stale values from Layer 2 (evm_state.bin).
            let slot3 = base_slot + U256::from(3);
            let initialized_value = if info.initialized {
                // initialized is a bool packed at byte offset 31 (rightmost byte of the
                // last field in the packed struct). In the actual Solidity layout it's at
                // a higher bit position, but the key thing is we need the bit set.
                // Actual layout: initialized is at byte 31 of the packed word.
                U256::from(1u64) << 248
            } else {
                U256::ZERO
            };
            self.db
                .insert_account_storage(pool_address, slot3, initialized_value)?;

            injected += 1;
        }

        Ok(injected)
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
            return evm
                .transact_commit(tx_env)
                .map_err(|e| anyhow!("Failed to transact: {:?}", e));
        }

        let checkpoint = evm.journaled_state.checkpoint();
        let result = evm.transact_one(tx_env);
        evm.journaled_state.checkpoint_revert(checkpoint);
        result.map_err(|e| anyhow!("Failed to transact: {:?}", e))
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
        let result = evm
            .transact_one(tx)
            .map_err(|e| anyhow!("Failed to transact: {:?}", e))?;

        // Extract access list from journaled state before reverting.
        // After transact_one, journaled_state.state contains all touched accounts/slots.
        let mut access_list = StorageAccessList::default();
        for (address, account) in evm.journaled_state.state.iter() {
            if account.is_touched() {
                access_list.accounts.insert(*address);
                for (slot_key, _) in account.storage.iter() {
                    access_list.slots.insert((*address, *slot_key));
                }
            }
        }

        evm.journaled_state.checkpoint_revert(checkpoint);
        Ok((result, access_list))
    }

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
            Err(anyhow!("Failed to call: {:?}", result))
        }
    }

    pub fn erc20_balance_of(&mut self, token: Address, owner: Address) -> Result<U256> {
        let call = IERC20::balanceOfCall { target: owner };
        let result = self.call_raw(Address::ZERO, token, Bytes::from(call.abi_encode()), false)?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let balance = IERC20::balanceOfCall::abi_decode_returns(&out)
                    .map_err(|e| anyhow!("Failed to decode balanceOf: {:?}", e))?;
                Ok(balance)
            }
            _ => Err(anyhow!("balanceOf call failed: {:?}", result)),
        }
    }

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
                let allowance = IERC20::allowanceCall::abi_decode_returns(&out)
                    .map_err(|e| anyhow!("Failed to decode allowance: {:?}", e))?;
                Ok(allowance)
            }
            _ => Err(anyhow!("allowance call failed: {:?}", result)),
        }
    }

    pub fn erc20_decimals(&mut self, token: Address) -> Result<u8> {
        if let Some(decimals) = self.token_decimals.get(&token) {
            return Ok(*decimals);
        }

        let call = IERC20::decimalsCall {};
        let result = self.call_raw(Address::ZERO, token, Bytes::from(call.abi_encode()), false)?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let decimals = IERC20::decimalsCall::abi_decode_returns(&out)
                    .map_err(|e| anyhow!("Failed to decode decimals: {:?}", e))?;
                self.token_decimals.insert(token, decimals);
                // Also update immutable cache for persistence
                self.immutable_cache.set_token_decimals(token, decimals);
                Ok(decimals)
            }
            _ => Err(anyhow!("decimals call failed: {:?}", result)),
        }
    }

    /// Get a reference to the immutable data cache.
    pub fn immutable_cache(&self) -> &ImmutableDataCache {
        &self.immutable_cache
    }

    /// Get a mutable reference to the immutable data cache.
    pub fn immutable_cache_mut(&mut self) -> &mut ImmutableDataCache {
        &mut self.immutable_cache
    }

    /// Get a reference to the V3 tick snapshot cache.
    #[cfg(feature = "protocols")]
    pub fn tick_snapshot_cache(&self) -> &V3TickSnapshotCache {
        &self.tick_snapshot_cache
    }

    /// Get a mutable reference to the V3 tick snapshot cache.
    #[cfg(feature = "protocols")]
    pub fn tick_snapshot_cache_mut(&mut self) -> &mut V3TickSnapshotCache {
        &mut self.tick_snapshot_cache
    }

    /// Check if a pool has storage slots pre-loaded in the BlockchainDb.
    ///
    /// This is useful to determine if we loaded the EVM state from the unified
    /// `evm_state.bin` cache and the pool's tick data is already in storage.
    /// If true, we can skip expensive tick injection when liquidity hasn't changed.
    ///
    /// # Arguments
    /// * `address` - The pool contract address to check
    ///
    /// # Returns
    /// `true` if the pool has any storage slots in the underlying BlockchainDb,
    /// `false` otherwise
    pub fn has_pool_storage(&self, address: Address) -> bool {
        let storage = self.blockchain_db.storage().read();
        storage
            .get(&address)
            .map(|slots| !slots.is_empty())
            .unwrap_or(false)
    }

    /// Get the number of storage slots loaded for a pool.
    ///
    /// Useful for debugging and logging to understand cache state.
    pub fn pool_storage_slot_count(&self, address: Address) -> usize {
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
    }

    /// Purge all storage slots for a specific pool from both cache layers.
    ///
    /// This clears:
    /// 1. **CacheDB overlay** (`self.db.cache.accounts[addr].storage`) - the in-memory
    ///    layer that caches storage slots fetched during EVM execution. Without clearing
    ///    this layer, subsequent EVM calls return stale values even after the backend
    ///    is purged.
    /// 2. **BlockchainDb backend** (`self.blockchain_db.storage()`) - the persistent
    ///    layer that caches RPC responses and is loaded from `evm_state.bin`.
    ///
    /// After purging both layers, the next EVM read for this pool's storage will
    /// go all the way to the RPC for fresh data.
    pub fn purge_pool_storage(&mut self, address: Address) -> usize {
        // Layer 1: Clear CacheDB overlay
        let cache_db_cleared = if let Some(db_account) = self.db.cache.accounts.get_mut(&address) {
            let count = db_account.storage.len();
            db_account.storage.clear();
            count
        } else {
            0
        };

        // Layer 2: Clear BlockchainDb backend
        let mut storage = self.blockchain_db.storage().write();
        let backend_cleared = if let Some(slots) = storage.remove(&address) {
            slots.len()
        } else {
            0
        };

        if cache_db_cleared > 0 || backend_cleared > 0 {
            debug!(
                pool = %address,
                cache_db_slots = cache_db_cleared,
                backend_slots = backend_cleared,
                "purged pool storage from both cache layers"
            );
        }

        backend_cleared
    }

    /// Purge specific storage slots for a pool from both cache layers.
    ///
    /// Unlike `purge_pool_storage()` which removes ALL storage, this only removes
    /// the specified slots. This is critical for performance: V3 pools may have
    /// hundreds of tick data slots that are expensive to re-fetch. When we only
    /// need fresh slot0/liquidity values, we can purge just those 2 slots and
    /// preserve the tick data.
    ///
    /// Returns the number of slots removed from the BlockchainDb backend.
    pub fn purge_pool_slots(&mut self, address: Address, slots: &[U256]) -> usize {
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
        let mut storage = self.blockchain_db.storage().write();
        if let Some(address_storage) = storage.get_mut(&address) {
            for slot in slots {
                if address_storage.remove(slot).is_some() {
                    backend_removed += 1;
                }
            }
        }

        if cache_db_removed > 0 || backend_removed > 0 {
            trace!(
                pool = %address,
                requested = slots.len(),
                cache_db_removed,
                backend_removed,
                "selectively purged pool storage slots from both cache layers"
            );
        }

        backend_removed
    }

    /// Purge storage slots for multiple contracts from both cache layers.
    ///
    /// See `purge_pool_storage()` for details on what each layer contains.
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
        let mut storage = self.blockchain_db.storage().write();
        let total_slots: usize = storage.values().map(|s| s.len()).sum();
        let contract_count = storage.len();
        storage.clear();

        if total_slots > 0 || cache_db_cleared > 0 {
            warn!(
                contracts_cleared = contract_count,
                backend_slots_purged = total_slots,
                cache_db_slots_purged = cache_db_cleared,
                "purged ALL storage from both cache layers (full refresh)"
            );
        }
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

    /// Get the number of storage slots in the CacheDB overlay for a pool.
    ///
    /// This is useful for diagnostics - if a pool has slots in the CacheDB overlay,
    /// they will be served on EVM reads without going to the backend.
    pub fn cache_db_storage_slot_count(&self, address: Address) -> usize {
        self.db
            .cache
            .accounts
            .get(&address)
            .map(|a| a.storage.len())
            .unwrap_or(0)
    }

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

        let tx = Self::build_tx_env(from, to, calldata)?;
        let mut evm = self.build_evm();
        let checkpoint = evm.journaled_state.checkpoint();

        let result = (|| {
            let mut pre_balances = HashMap::with_capacity(token_list.len());
            for token in &token_list {
                let balance = Self::erc20_balance_of_in_evm(&mut evm, *token, owner)?;
                pre_balances.insert(*token, balance);
            }

            let result = evm
                .transact_one(tx)
                .map_err(|e| anyhow!("Failed to transact: {:?}", e))?;
            let (logs, gas_used) = match result {
                ExecutionResult::Success { logs, gas_used, .. } => (logs, gas_used),
                _ => return Err(anyhow!("Failed to call: {:?}", result)),
            };

            let mut token_deltas = HashMap::with_capacity(token_list.len());
            for token in &token_list {
                let post = Self::erc20_balance_of_in_evm(&mut evm, *token, owner)?;
                let pre = pre_balances.get(token).copied().unwrap_or_default();
                token_deltas.insert(*token, I256::from_raw(post) - I256::from_raw(pre));
            }

            Ok((gas_used, token_deltas, logs))
        })();

        match result {
            Ok((gas_used, token_deltas, logs)) => {
                if commit {
                    evm.commit_inner();
                } else {
                    evm.journaled_state.checkpoint_revert(checkpoint);
                }
                Ok(CallSimulationResult {
                    gas_used,
                    token_deltas,
                    logs,
                    access_list: AccessList::default(),
                })
            }
            Err(err) => {
                evm.journaled_state.checkpoint_revert(checkpoint);
                Err(err)
            }
        }
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
        let tx = Self::build_tx_env(from, to, calldata).map_err(SimError::Other)?;
        let inspector = TransferInspector::new();
        let mut evm = self.build_evm_with_inspector(inspector);
        let checkpoint = evm.journaled_state.checkpoint();

        let result = evm
            .inspect_one_tx(tx)
            .map_err(|e| SimError::Other(anyhow!("Failed to transact: {:?}", e)));

        match result {
            Ok(ExecutionResult::Success { logs, gas_used, .. }) => {
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
                    gas_used,
                    token_deltas,
                    logs,
                    access_list,
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

    /// Deploy a contract via CREATE transaction and return the deployed address.
    ///
    /// The `creation_code` should include the init code with ABI-encoded constructor
    /// arguments appended. Nonce checks are disabled, so any `from` address works.
    ///
    /// Note: This commits the deployment to the CacheDB. Use a throw-away deployer
    /// address (e.g., `Address::ZERO`) to avoid side effects on real accounts.
    pub fn deploy_contract(&mut self, from: Address, creation_code: Bytes) -> Result<Address> {
        let tx = TxEnv::builder()
            .caller(from)
            .kind(TxKind::Create)
            .data(creation_code)
            .value(U256::ZERO)
            .build()
            .map_err(|e| anyhow!("Failed to build CREATE tx: {:?}", e))?;

        // Use a relaxed contract size limit for deployment. Arbitrum supports
        // larger contracts than the EIP-170 24576-byte limit via ArbOS.
        let mut evm = self.build_evm();
        evm.cfg.limit_contract_code_size = Some(usize::MAX);
        let result = evm
            .transact_commit(tx)
            .map_err(|e| anyhow!("Contract deployment failed: {:?}", e))?;

        match result {
            ExecutionResult::Success { output, .. } => output
                .address()
                .copied()
                .ok_or_else(|| anyhow!("Contract deployment succeeded but no address returned")),
            ExecutionResult::Revert { output, .. } => Err(anyhow!(
                "Contract deployment reverted: 0x{}",
                alloy_primitives::hex::encode(&output)
            )),
            ExecutionResult::Halt { reason, .. } => {
                Err(anyhow!("Contract deployment halted: {:?}", reason))
            }
        }
    }

    /// Override the bytecode at `target` address with bytecode from `source` address.
    ///
    /// Copies only non-empty runtime code and code_hash; storage, balance, and nonce
    /// at `target` remain unchanged. `target` must already have non-empty runtime
    /// bytecode. Both the CacheDB overlay and BlockchainDb backend are updated,
    /// ensuring the override is visible to parallel EVM tasks sharing the same backend.
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
            .ok_or_else(|| anyhow!("No bytecode found at source address {}", source))?;
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

        Ok(())
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
            return Ok(account.info.clone());
        }

        match missing_target {
            MissingTargetBehavior::Create => Ok(AccountInfo::default()),
            MissingTargetBehavior::Error => {
                use revm::database_interface::DatabaseRef;
                self.backend
                    .basic_ref(target)
                    .map_err(|e| anyhow!("Failed to fetch target account {}: {:?}", target, e))?
                    .ok_or_else(|| {
                        anyhow!(
                            "Target account {} not found; use override_or_create_account_code for synthetic targets",
                            target
                        )
                    })
            }
        }
    }

    fn ensure_runtime_code(address: Address, code: Option<&Bytecode>, role: &str) -> Result<()> {
        if code.is_some_and(|code| !code.is_empty()) {
            return Ok(());
        }

        Err(anyhow!(
            "{} account {} has no runtime bytecode",
            role,
            address
        ))
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

        let timestamp = self.timestamp_override.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
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

        let timestamp = self.timestamp_override.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
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
        builder
            .build()
            .map_err(|e| anyhow!("Failed to build tx env: {:?}", e))
    }

    fn erc20_balance_of_in_evm(
        evm: &mut CacheEvm<'_>,
        token: Address,
        owner: Address,
    ) -> Result<U256> {
        let call = IERC20::balanceOfCall { target: owner };
        let tx = Self::build_tx_env(Address::ZERO, token, Bytes::from(call.abi_encode()))?;
        let result = evm
            .transact_one(tx)
            .map_err(|e| anyhow!("Failed to transact: {:?}", e))?;

        match result {
            ExecutionResult::Success { output, .. } => {
                let out = output.into_data();
                let balance = IERC20::balanceOfCall::abi_decode_returns(&out)
                    .map_err(|e| anyhow!("Failed to decode balanceOf: {:?}", e))?;
                Ok(balance)
            }
            _ => Err(anyhow!("balanceOf call failed: {:?}", result)),
        }
    }
}

/// A session for executing multiple EVM operations without committing to the underlying DB.
///
/// Changes made within a session are tracked in the EVM's journaled state. Call `commit()` to
/// persist changes to the underlying database, or simply drop the session to discard
/// all changes.
///
/// Note: For snapshot/restore functionality across multiple transactions, use `EvmCache::snapshot()`
/// and `EvmCache::restore()` instead, as the EVM journal is cleared after each transaction.
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
            self.evm
                .transact_one(tx)
                .map_err(|e| anyhow!("Failed to transact: {:?}", e))
        } else {
            let checkpoint = self.evm.journaled_state.checkpoint();
            let result = self.evm.transact_one(tx);
            self.evm.journaled_state.checkpoint_revert(checkpoint);
            result.map_err(|e| anyhow!("Failed to transact: {:?}", e))
        }
    }

    /// Commit all session changes to the underlying database.
    ///
    /// This persists all changes made during the session to the CacheDB.
    pub fn commit(mut self) {
        self.evm.commit_inner();
    }

    /// Get access to the underlying EVM for advanced operations.
    pub fn evm(&mut self) -> &mut CacheEvm<'a> {
        &mut self.evm
    }
}

/// Automatically flush the cache to disk when the EvmCache is dropped.
impl Drop for EvmCache {
    fn drop(&mut self) {
        if self.cache_config.is_some() {
            debug!("Flushing EVM cache on drop");
            self.flush();
        }
    }
}

/// Extract an EIP-2930 access list from the EVM journaled state.
///
/// After a transaction executes, `journaled_state.state` contains all accounts
/// and storage slots that were touched. This converts them into an `AccessList`
/// suitable for inclusion in a transaction, ensuring all accessed storage is warm.
fn extract_access_list(state: &revm::state::EvmState) -> AccessList {
    let items: Vec<AccessListItem> = state
        .iter()
        .filter(|(_, account)| account.is_touched())
        .map(|(address, account)| AccessListItem {
            address: *address,
            storage_keys: account
                .storage
                .keys()
                .map(|slot| B256::from(*slot))
                .collect(),
        })
        .collect();
    AccessList(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_keys::{i128_to_u256, i256_from_i16, i256_from_i24};

    #[test]
    fn test_i256_from_i16_positive() {
        let result = i256_from_i16(1);
        // Should be 31 zero bytes followed by 0x0001
        assert_eq!(result[0..30], [0u8; 30]);
        assert_eq!(result[30], 0x00);
        assert_eq!(result[31], 0x01);
    }

    #[test]
    fn test_i256_from_i16_negative() {
        let result = i256_from_i16(-1);
        // Should be 30 0xFF bytes followed by 0xFFFF
        assert_eq!(result[0..30], [0xFF; 30]);
        assert_eq!(result[30], 0xFF);
        assert_eq!(result[31], 0xFF);
    }

    #[test]
    fn test_i256_from_i16_zero() {
        let result = i256_from_i16(0);
        assert_eq!(result, [0u8; 32]);
    }

    #[test]
    fn test_i256_from_i16_max() {
        let result = i256_from_i16(i16::MAX); // 32767 = 0x7FFF
        assert_eq!(result[0..30], [0u8; 30]);
        assert_eq!(result[30], 0x7F);
        assert_eq!(result[31], 0xFF);
    }

    #[test]
    fn test_i256_from_i16_min() {
        let result = i256_from_i16(i16::MIN); // -32768 = 0x8000
        assert_eq!(result[0..30], [0xFF; 30]);
        assert_eq!(result[30], 0x80);
        assert_eq!(result[31], 0x00);
    }

    #[test]
    fn test_tick_bitmap_storage_slot_calculation() {
        // This test verifies our storage slot calculation matches Solidity's behavior.
        // In Solidity: mapping(int16 => uint256) tickBitmap at slot 6
        // Storage slot = keccak256(abi.encode(wordPosition, 6))
        //
        // For wordPosition = 0:
        // abi.encode(int256(0), uint256(6)) =
        //   0x0000...0000 (32 bytes for 0) ++ 0x0000...0006 (32 bytes for 6)
        // keccak256 of that gives the storage slot

        const TICK_BITMAP_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);

        // Test word position 0
        let word_pos: i16 = 0;
        let word_position_i256 = i256_from_i16(word_pos);

        let mut slot_preimage = [0u8; 64];
        slot_preimage[..32].copy_from_slice(&word_position_i256);
        slot_preimage[32..64].copy_from_slice(&TICK_BITMAP_SLOT.to_be_bytes::<32>());

        let storage_slot: U256 = keccak256(slot_preimage).into();

        // The slot should be a valid keccak256 hash (non-zero, 256 bits)
        assert!(storage_slot != U256::ZERO);

        // Verify the preimage is correctly formed
        assert_eq!(&slot_preimage[..32], &[0u8; 32]); // word position 0
        assert_eq!(slot_preimage[63], 6); // slot 6 in last byte
        assert_eq!(&slot_preimage[32..63], &[0u8; 31]); // rest of slot is zeros
    }

    #[test]
    fn test_tick_bitmap_storage_slot_negative_word() {
        // Test with negative word position to ensure sign extension works
        const TICK_BITMAP_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);

        let word_pos: i16 = -1;
        let word_position_i256 = i256_from_i16(word_pos);

        let mut slot_preimage = [0u8; 64];
        slot_preimage[..32].copy_from_slice(&word_position_i256);
        slot_preimage[32..64].copy_from_slice(&TICK_BITMAP_SLOT.to_be_bytes::<32>());

        let storage_slot: U256 = keccak256(slot_preimage).into();

        // Should produce a valid slot
        assert!(storage_slot != U256::ZERO);

        // Verify the preimage has sign-extended -1
        assert_eq!(&slot_preimage[..32], &[0xFF; 32]); // -1 sign-extended
    }

    #[test]
    fn test_different_word_positions_give_different_slots() {
        const TICK_BITMAP_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);

        let calc_slot = |word_pos: i16| -> U256 {
            let word_position_i256 = i256_from_i16(word_pos);
            let mut slot_preimage = [0u8; 64];
            slot_preimage[..32].copy_from_slice(&word_position_i256);
            slot_preimage[32..64].copy_from_slice(&TICK_BITMAP_SLOT.to_be_bytes::<32>());
            keccak256(slot_preimage).into()
        };

        let slot_0 = calc_slot(0);
        let slot_1 = calc_slot(1);
        let slot_neg1 = calc_slot(-1);
        let slot_100 = calc_slot(100);

        // All should be different
        assert_ne!(slot_0, slot_1);
        assert_ne!(slot_0, slot_neg1);
        assert_ne!(slot_0, slot_100);
        assert_ne!(slot_1, slot_neg1);
        assert_ne!(slot_1, slot_100);
        assert_ne!(slot_neg1, slot_100);
    }

    // ==================== i256_from_i24 tests ====================

    #[test]
    fn test_i256_from_i24_zero() {
        let result = i256_from_i24(0);
        assert_eq!(result, [0u8; 32]);
    }

    #[test]
    fn test_i256_from_i24_positive() {
        let result = i256_from_i24(1);
        assert_eq!(result[0..31], [0u8; 31]);
        assert_eq!(result[31], 0x01);
    }

    #[test]
    fn test_i256_from_i24_negative_one() {
        // -1 in 24-bit two's complement is 0xFFFFFF
        let result = i256_from_i24(-1);
        // Should be sign-extended: 29 0xFF bytes followed by 0xFFFFFF
        assert_eq!(result[0..29], [0xFF; 29]);
        assert_eq!(result[29], 0xFF);
        assert_eq!(result[30], 0xFF);
        assert_eq!(result[31], 0xFF);
    }

    #[test]
    fn test_i256_from_i24_max_positive() {
        // Max int24 is 8388607 = 0x7FFFFF
        let max_i24: i32 = 0x7FFFFF;
        let result = i256_from_i24(max_i24);
        assert_eq!(result[0..29], [0u8; 29]);
        assert_eq!(result[29], 0x7F);
        assert_eq!(result[30], 0xFF);
        assert_eq!(result[31], 0xFF);
    }

    #[test]
    fn test_i256_from_i24_min_negative() {
        // Min int24 is -8388608 = 0x800000 (as 24-bit signed)
        let min_i24: i32 = -8388608;
        let result = i256_from_i24(min_i24);
        // Should be sign-extended with 0xFF
        assert_eq!(result[0..29], [0xFF; 29]);
        assert_eq!(result[29], 0x80);
        assert_eq!(result[30], 0x00);
        assert_eq!(result[31], 0x00);
    }

    #[test]
    fn test_i256_from_i24_typical_tick_positive() {
        // Test a typical positive tick value (e.g., 1000)
        let tick: i32 = 1000; // 0x0003E8
        let result = i256_from_i24(tick);
        assert_eq!(result[0..30], [0u8; 30]);
        assert_eq!(result[30], 0x03);
        assert_eq!(result[31], 0xE8);
    }

    #[test]
    fn test_i256_from_i24_typical_tick_negative() {
        // Test a typical negative tick value (e.g., -1000)
        // -1000 in 24-bit two's complement: 0xFFFC18
        let tick: i32 = -1000;
        let result = i256_from_i24(tick);
        assert_eq!(result[0..29], [0xFF; 29]);
        assert_eq!(result[29], 0xFF);
        assert_eq!(result[30], 0xFC);
        assert_eq!(result[31], 0x18);
    }

    // ==================== i128_to_u256 tests ====================

    #[test]
    fn test_i128_to_u256_positive() {
        let result = i128_to_u256(12345);
        assert_eq!(result, U256::from(12345u128));
    }

    #[test]
    fn test_i128_to_u256_zero() {
        let result = i128_to_u256(0);
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn test_i128_to_u256_negative_one() {
        // -1 in two's complement u128 is all 1s
        let result = i128_to_u256(-1);
        // Lower 128 bits should be all 1s
        let expected = U256::from(u128::MAX);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_i128_to_u256_negative() {
        // -100 should give us the two's complement representation
        let result = i128_to_u256(-100);
        let expected = U256::from((-100i128) as u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_i128_to_u256_max() {
        let result = i128_to_u256(i128::MAX);
        assert_eq!(result, U256::from(i128::MAX as u128));
    }

    #[test]
    fn test_i128_to_u256_min() {
        let result = i128_to_u256(i128::MIN);
        // i128::MIN as u128 = 0x8000...0000
        assert_eq!(result, U256::from(i128::MIN as u128));
    }

    // ==================== Tick storage slot tests ====================

    #[test]
    fn test_tick_info_packing() {
        // Test that liquidityGross and liquidityNet are packed correctly
        let liquidity_gross: u128 = 1_000_000_000;
        let liquidity_net: i128 = -500_000_000;

        let liquidity_gross_u256 = U256::from(liquidity_gross);
        let liquidity_net_u256 = i128_to_u256(liquidity_net);
        let packed = liquidity_gross_u256 | (liquidity_net_u256 << 128);

        // Extract and verify
        let extracted_gross = packed & U256::from(u128::MAX);
        let extracted_net_u256 = packed >> 128;

        assert_eq!(extracted_gross, U256::from(liquidity_gross));
        assert_eq!(extracted_net_u256, liquidity_net_u256);
    }

    #[test]
    fn test_tick_storage_slot_calculation() {
        const TICKS_SLOT: U256 = U256::from_limbs([5, 0, 0, 0]);

        // Test tick 0
        let tick: i32 = 0;
        let tick_i256 = i256_from_i24(tick);

        let mut slot_preimage = [0u8; 64];
        slot_preimage[..32].copy_from_slice(&tick_i256);
        slot_preimage[32..64].copy_from_slice(&TICKS_SLOT.to_be_bytes::<32>());

        let storage_slot: U256 = keccak256(slot_preimage).into();

        // Should produce a valid slot
        assert!(storage_slot != U256::ZERO);

        // Verify preimage
        assert_eq!(&slot_preimage[..32], &[0u8; 32]); // tick 0
        assert_eq!(slot_preimage[63], 5); // slot 5
    }

    #[test]
    fn test_different_ticks_give_different_slots() {
        const TICKS_SLOT: U256 = U256::from_limbs([5, 0, 0, 0]);

        let calc_slot = |tick: i32| -> U256 {
            let tick_i256 = i256_from_i24(tick);
            let mut slot_preimage = [0u8; 64];
            slot_preimage[..32].copy_from_slice(&tick_i256);
            slot_preimage[32..64].copy_from_slice(&TICKS_SLOT.to_be_bytes::<32>());
            keccak256(slot_preimage).into()
        };

        let slot_0 = calc_slot(0);
        let slot_60 = calc_slot(60);
        let slot_neg60 = calc_slot(-60);
        let slot_887272 = calc_slot(887272); // MAX_TICK

        // All should be different
        assert_ne!(slot_0, slot_60);
        assert_ne!(slot_0, slot_neg60);
        assert_ne!(slot_0, slot_887272);
        assert_ne!(slot_60, slot_neg60);
    }

    // ==================== V2 pool metadata injection tests ====================

    #[test]
    fn test_v2_pool_metadata_storage_slots() {
        // Verify the storage slot constants match UniswapV2Pair layout
        const TOKEN0_SLOT: U256 = U256::from_limbs([6, 0, 0, 0]);
        const TOKEN1_SLOT: U256 = U256::from_limbs([7, 0, 0, 0]);

        // Slots should be sequential starting at 6
        assert_eq!(TOKEN0_SLOT, U256::from(6));
        assert_eq!(TOKEN1_SLOT, U256::from(7));
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

    #[test]
    fn test_v2_metadata_address_values() {
        // Test specific address encoding
        let token0 = Address::repeat_byte(0x11);
        let token1 = Address::repeat_byte(0x22);

        let metadata = V2PoolMetadata {
            token0,
            token1,
            last_block_timestamp: 0,
        };

        let token0_value = U256::from_be_slice(metadata.token0.as_slice());
        let token1_value = U256::from_be_slice(metadata.token1.as_slice());

        // Values should be different
        assert_ne!(token0_value, token1_value);

        // Each should be non-zero
        assert_ne!(token0_value, U256::ZERO);
        assert_ne!(token1_value, U256::ZERO);

        // Verify round-trip: extract address bytes back
        let token0_bytes = token0_value.to_be_bytes::<32>();
        let token1_bytes = token1_value.to_be_bytes::<32>();

        assert_eq!(&token0_bytes[12..], token0.as_slice());
        assert_eq!(&token1_bytes[12..], token1.as_slice());
    }

    // -- PancakeSwap V3 storage slot tests --

    #[test]
    fn test_pancake_v3_constants_correct_values() {
        // PancakeSwap V3 slots are shifted +1 from Uniswap V3
        assert_eq!(V3_LIQUIDITY_SLOT, U256::from(4));
        assert_eq!(PANCAKE_V3_LIQUIDITY_SLOT, U256::from(5));

        assert_eq!(V3_TICKS_BASE_SLOT, U256::from(5));
        assert_eq!(PANCAKE_V3_TICKS_BASE_SLOT, U256::from(6));

        assert_eq!(V3_TICK_BITMAP_BASE_SLOT, U256::from(6));
        assert_eq!(PANCAKE_V3_TICK_BITMAP_BASE_SLOT, U256::from(7));
    }

    #[test]
    fn test_tick_bitmap_with_base_matches_original_for_uniswap() {
        // v3_tick_bitmap_storage_key_with_base using Uniswap base slot should
        // produce identical results to v3_tick_bitmap_storage_key
        for word_pos in [-100i16, -1, 0, 1, 42, 100] {
            let original = v3_tick_bitmap_storage_key(word_pos);
            let with_base =
                v3_tick_bitmap_storage_key_with_base(word_pos, V3_TICK_BITMAP_BASE_SLOT);
            assert_eq!(
                original, with_base,
                "with_base should match original for word_pos={word_pos}"
            );
        }
    }

    #[test]
    fn test_tick_bitmap_pancake_differs_from_uniswap() {
        // PancakeSwap base slot 7 must produce different keys than Uniswap slot 6
        for word_pos in [-1i16, 0, 1, 42] {
            let uniswap = v3_tick_bitmap_storage_key(word_pos);
            let pancake =
                v3_tick_bitmap_storage_key_with_base(word_pos, PANCAKE_V3_TICK_BITMAP_BASE_SLOT);
            assert_ne!(
                uniswap, pancake,
                "PancakeSwap bitmap key should differ from Uniswap for word_pos={word_pos}"
            );
        }
    }

    #[test]
    fn test_tick_info_with_base_matches_original_for_uniswap() {
        // v3_tick_info_storage_keys_with_base using Uniswap base slot should
        // produce identical results to v3_tick_info_storage_keys
        for tick in [-887_272i32, -1000, 0, 1000, 887_272] {
            let original = v3_tick_info_storage_keys(tick);
            let with_base = v3_tick_info_storage_keys_with_base(tick, V3_TICKS_BASE_SLOT);
            assert_eq!(
                original, with_base,
                "with_base should match original for tick={tick}"
            );
        }
    }

    #[test]
    fn test_tick_info_pancake_differs_from_uniswap() {
        // PancakeSwap ticks base slot 6 must produce different keys than Uniswap slot 5
        for tick in [-1000i32, 0, 1000] {
            let uniswap = v3_tick_info_storage_keys(tick);
            let pancake = v3_tick_info_storage_keys_with_base(tick, PANCAKE_V3_TICKS_BASE_SLOT);
            for i in 0..4 {
                assert_ne!(
                    uniswap[i], pancake[i],
                    "PancakeSwap tick info key[{i}] should differ from Uniswap for tick={tick}"
                );
            }
        }
    }

    #[test]
    fn test_tick_info_with_base_keys_are_sequential() {
        // The 4 keys returned should be consecutive (base, base+1, base+2, base+3)
        let keys = v3_tick_info_storage_keys_with_base(500, PANCAKE_V3_TICKS_BASE_SLOT);
        assert_eq!(keys[1], keys[0] + U256::from(1));
        assert_eq!(keys[2], keys[0] + U256::from(2));
        assert_eq!(keys[3], keys[0] + U256::from(3));
    }

    // ==================== block context tests ====================

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

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider), None));

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

        let mut cache = rt.block_on(EvmCache::new(Arc::new(provider), None));

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

        let parent = rt.block_on(EvmCache::new(Arc::new(provider), None));

        let block_num = Some(148_252_680u64);
        let basefee_val = Some(50u64);
        let child = EvmCache::from_backend(
            parent.backend().clone(),
            parent.blockchain_db().clone(),
            parent.block(),
            42161,
            block_num,
            basefee_val,
            SpecId::CANCUN,
        );

        assert_eq!(child.block_number(), block_num);
        assert_eq!(child.basefee(), basefee_val);
    }
}
