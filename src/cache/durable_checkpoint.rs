//! Atomic, versioned checkpoints for event-maintained EVM state.
//!
//! The ordinary cache files are startup accelerators and may be flushed
//! independently. A durable checkpoint has a stricter contract: cache state,
//! canonical chain position, consumer identity, handler schema, and the last
//! ingested subscriber token are serialized into one file and atomically
//! replaced before the token may be acknowledged upstream. Files carry a
//! Keccak integrity checksum and a configurable resource bound. The checksum
//! detects accidental corruption; it is not authentication for an
//! attacker-writable checkpoint path.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use alloy_eips::{BlockId, BlockNumberOrTag, RpcBlockHash};
use alloy_primitives::{Address, B256, U256, keccak256};
use foundry_fork_db::BlockchainDb;
use revm::{database::Cache, primitives::hardfork::SpecId, state::AccountInfo};
use serde::{Deserialize, Serialize};

use super::{
    BlockEnvSource, CodeSeedState, EvmCache, ImmutableDataCache, TrackedMapping, versioned,
};

const CHECKPOINT_MAGIC: &[u8; 8] = b"EFCCKPT\0";
// 7: the reactive runtime blob gained `log_coverage_head`, and `ChainControl`
// gained `LogCoverage`. A version 6 file is rejected as `InvalidFormat`
// rather than decoded against the newer shape.
const CHECKPOINT_VERSION: u32 = 7;
const CHECKPOINT_LABEL: &str = "durable reactive checkpoint";
const CHECKPOINT_CHECKSUM_BYTES: usize = 32;
const CHECKPOINT_HEADER_BYTES: u64 =
    CHECKPOINT_MAGIC.len() as u64 + std::mem::size_of::<u32>() as u64;
const MAX_TEMP_CREATE_ATTEMPTS: usize = 128;
/// Default upper bound for a single durable checkpoint file (512 MiB).
pub const DEFAULT_MAX_DURABLE_CHECKPOINT_BYTES: u64 = 512 * 1024 * 1024;
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
static CHECKPOINT_COORDINATORS: OnceLock<
    Mutex<HashMap<PathBuf, Weak<CheckpointWriteCoordinator>>>,
> = OnceLock::new();

/// Stable identity of one durable cache consumer.
///
/// `subscriber_id` distinguishes independently acknowledged event sessions.
/// `handler_set_id` is an application-owned schema/version fingerprint; change
/// it whenever handler decoding or state semantics become incompatible with an
/// older checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DurableCheckpointIdentity {
    /// Chain whose state is represented by the checkpoint.
    pub chain_id: u64,
    /// Stable event-subscriber or remote-session identity.
    pub subscriber_id: String,
    /// Stable application handler-set/schema identity.
    pub handler_set_id: String,
}

impl DurableCheckpointIdentity {
    /// Construct a durable consumer identity.
    pub fn new(
        chain_id: u64,
        subscriber_id: impl Into<String>,
        handler_set_id: impl Into<String>,
    ) -> Self {
        Self {
            chain_id,
            subscriber_id: subscriber_id.into(),
            handler_set_id: handler_set_id.into(),
        }
    }
}

/// Canonical block committed by a durable checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DurableCheckpointBlock {
    /// Block number.
    pub number: u64,
    /// Canonical block hash. Applications must validate this against their RPC
    /// source before restoring a non-finalized checkpoint.
    pub hash: B256,
    /// Parent hash, when the source supplied it.
    pub parent_hash: Option<B256>,
    /// Block timestamp, when the source supplied it.
    pub timestamp: Option<u64>,
}

impl DurableCheckpointBlock {
    /// Construct the minimum exact canonical identity required for restore.
    pub const fn new(number: u64, hash: B256) -> Self {
        Self {
            number,
            hash,
            parent_hash: None,
            timestamp: None,
        }
    }

    /// Attach the canonical parent hash supplied by the source.
    pub const fn with_parent_hash(mut self, parent_hash: B256) -> Self {
        self.parent_hash = Some(parent_hash);
        self
    }

    /// Attach the block timestamp supplied by the source.
    pub const fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// Public commit metadata stored alongside the cache snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DurableCheckpointMetadata {
    /// Durable consumer identity guarded on restore.
    pub identity: DurableCheckpointIdentity,
    /// Last canonical block whose event effects are included.
    pub block: DurableCheckpointBlock,
    /// Last subscriber delivery token included in the snapshot.
    ///
    /// If the subscriber replays this token after an acknowledgement was lost,
    /// the consumer can acknowledge it without applying the batch again.
    pub delivery_token: Option<Vec<u8>>,
    /// Core-computed witness for the exact delivery associated with
    /// [`delivery_token`](Self::delivery_token).
    ///
    /// The witness is intentionally retained with its token when a later
    /// tokenless barrier advances the overall checkpoint. On replay, the engine
    /// requires the incoming delivery to reproduce this witness before it can
    /// acknowledge the token without reapplying the batch. A token supplied by
    /// low-level callers without a witness cannot use that replay shortcut.
    pub delivery_witness: Option<B256>,
    /// Opaque provider-specific resume state committed with this snapshot.
    ///
    /// The cache never interprets this value. A subscriber extension may use it
    /// to resume from a native cursor after process restart.
    pub subscriber_checkpoint: Option<Vec<u8>>,
    /// Opaque core-runtime recovery state committed with the cache snapshot.
    ///
    /// The cache layer stores these bytes but does not interpret them. The
    /// reactive engine uses them to restore finality and its bounded rollback
    /// journal after restart.
    pub runtime_checkpoint: Option<Vec<u8>>,
}

impl DurableCheckpointMetadata {
    /// Construct checkpoint metadata.
    pub fn new(identity: DurableCheckpointIdentity, block: DurableCheckpointBlock) -> Self {
        Self {
            identity,
            block,
            delivery_token: None,
            delivery_witness: None,
            subscriber_checkpoint: None,
            runtime_checkpoint: None,
        }
    }

    /// Attach the subscriber token whose effects are represented by this state.
    pub fn with_delivery_token(mut self, delivery_token: impl Into<Vec<u8>>) -> Self {
        self.delivery_token = Some(delivery_token.into());
        self
    }

    /// Attach the core delivery witness associated with the delivery token.
    ///
    /// Most applications should let the reactive engine compute this value.
    /// This builder exists for checkpoint migration and other
    /// low-level integrations that reproduce the core witness contract exactly.
    pub fn with_delivery_witness(mut self, delivery_witness: B256) -> Self {
        self.delivery_witness = Some(delivery_witness);
        self
    }

    /// Attach opaque provider-specific resume state represented by this state.
    pub fn with_subscriber_checkpoint(mut self, checkpoint: impl Into<Vec<u8>>) -> Self {
        self.subscriber_checkpoint = Some(checkpoint.into());
        self
    }

    /// Attach opaque core-runtime recovery state represented by this snapshot.
    pub fn with_runtime_checkpoint(mut self, checkpoint: impl Into<Vec<u8>>) -> Self {
        self.runtime_checkpoint = Some(checkpoint.into());
        self
    }
}

/// Filesystem-backed durable checkpoint store.
///
/// Every store constructed for the same normalized path in one process shares
/// writer generations, so a cancelled older async save cannot replace a newer
/// request even when the callers did not clone the same store value. Deployments
/// must still enforce one writer process per checkpoint path; filesystem rename
/// atomicity does not establish ordering between independent processes. Atomic
/// saves currently require Unix; unsupported platforms return a typed error
/// rather than falling back to a remove-then-rename durability gap.
#[derive(Clone, Debug)]
pub struct DurableCheckpointStore {
    path: PathBuf,
    coordinator: Arc<CheckpointWriteCoordinator>,
    max_checkpoint_bytes: u64,
}

#[derive(Debug, Default)]
struct CheckpointWriteCoordinator {
    latest_generation: AtomicU64,
    writer: Mutex<()>,
}

impl PartialEq for DurableCheckpointStore {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for DurableCheckpointStore {}

impl DurableCheckpointStore {
    /// Use `path` as the single atomic checkpoint file.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = normalized_checkpoint_path(&path.into());
        Self {
            coordinator: checkpoint_coordinator(&path),
            path,
            max_checkpoint_bytes: DEFAULT_MAX_DURABLE_CHECKPOINT_BYTES,
        }
    }

    /// Override the maximum encoded checkpoint size accepted for reads and
    /// writes. The default is [`DEFAULT_MAX_DURABLE_CHECKPOINT_BYTES`].
    ///
    /// This bounds file reads and the encoded write allocation; it is not a
    /// retention target. Snapshot capture still owns a clone of the cache state
    /// before measuring its serialized size, so services must budget capture
    /// memory separately. Lower the bound for tightly constrained services or
    /// raise it deliberately for unusually large caches.
    pub fn with_max_checkpoint_bytes(mut self, max_checkpoint_bytes: u64) -> Self {
        self.max_checkpoint_bytes = max_checkpoint_bytes;
        self
    }

    /// Maximum encoded checkpoint size accepted by this store.
    pub fn max_checkpoint_bytes(&self) -> u64 {
        self.max_checkpoint_bytes
    }

    /// Path of the checkpoint file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persist a complete cache snapshot and its commit metadata atomically.
    ///
    /// The new file is written and synced in the same directory, renamed over
    /// the previous checkpoint, and then the parent directory is synced. A
    /// failure before the rename leaves the previous committed file intact.
    /// The final rename replaces the destination directory entry itself; if the
    /// destination is a symlink, the symlink is replaced rather than followed.
    ///
    /// This low-level API can verify the cache's chain id, but cannot prove that
    /// its event-maintained state includes every effect through
    /// `metadata.block`. It trusts that caller assertion and normalizes the
    /// persisted exact pin/context to it. Production reactive consumers should
    /// normally use `ReactiveEngine::*_checkpointed`, which derives metadata
    /// from the batch committed with the runtime state.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCheckpointError`] when cache and metadata chain/context
    /// identity disagree, the generation counter is exhausted, encoding exceeds
    /// the configured size bound, or atomic write/sync/replace fails.
    pub fn save(
        &self,
        cache: &EvmCache,
        metadata: DurableCheckpointMetadata,
    ) -> Result<(), DurableCheckpointError> {
        validate_capture_identity(cache, &metadata)?;
        // Request order is assigned before the potentially expensive snapshot
        // capture. Otherwise an older large capture can finish after a newer
        // small capture, reserve the later generation, and overwrite it.
        let generation = self.reserve_generation()?;
        let snapshot = DurableCheckpointSnapshot::capture(cache, metadata);
        persist_snapshot(
            &self.path,
            snapshot,
            &self.coordinator,
            generation,
            self.max_checkpoint_bytes,
        )
    }

    /// Persist a complete cache snapshot without serializing or syncing the
    /// checkpoint file on the async runtime worker.
    ///
    /// Capturing the owned cache snapshot is synchronous, but the potentially
    /// long bincode encode, file write, fsync, rename, and directory fsync run
    /// on Tokio's blocking pool. This keeps checkpoint durability out of event
    /// transport and heartbeat scheduling paths.
    /// The same low-level metadata trust contract as [`save`](Self::save)
    /// applies.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCheckpointError`] for the same identity, generation,
    /// size, encoding, and filesystem failures as [`save`](Self::save), or when
    /// Tokio's blocking task cannot be joined.
    pub async fn save_async(
        &self,
        cache: &EvmCache,
        metadata: DurableCheckpointMetadata,
    ) -> Result<(), DurableCheckpointError> {
        validate_capture_identity(cache, &metadata)?;
        // See `save`: generation order is request-entry order, not
        // capture-completion order. Capture is infallible after validation.
        let generation = self.reserve_generation()?;
        let snapshot = DurableCheckpointSnapshot::capture(cache, metadata);
        let path = self.path.clone();
        let coordinator = Arc::clone(&self.coordinator);
        let max_checkpoint_bytes = self.max_checkpoint_bytes;
        tokio::task::spawn_blocking(move || {
            persist_snapshot(
                &path,
                snapshot,
                &coordinator,
                generation,
                max_checkpoint_bytes,
            )
        })
        .await
        .map_err(DurableCheckpointError::TaskJoin)?
    }

    fn reserve_generation(&self) -> Result<u64, DurableCheckpointError> {
        self.coordinator
            .latest_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| DurableCheckpointError::GenerationExhausted)
    }

    /// Load a checkpoint without mutating a cache.
    ///
    /// This split lets callers inspect and RPC-validate the canonical hash in
    /// [`LoadedDurableCheckpoint::metadata`] before choosing to restore it.
    /// The configured size ceiling and integrity checksum are verified before
    /// any checkpoint payload is decoded.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCheckpointError`] for read/metadata failures, oversized
    /// files, invalid magic/version/encoding, checksum mismatch, or malformed
    /// checkpoint content. A missing file returns `Ok(None)`.
    pub fn load(&self) -> Result<Option<LoadedDurableCheckpoint>, DurableCheckpointError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(DurableCheckpointError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        let reported_bytes = file
            .metadata()
            .map_err(|source| DurableCheckpointError::Read {
                path: self.path.clone(),
                source,
            })?
            .len();
        if reported_bytes > self.max_checkpoint_bytes {
            return Err(DurableCheckpointError::CheckpointTooLarge {
                path: self.path.clone(),
                bytes: reported_bytes,
                max_bytes: self.max_checkpoint_bytes,
            });
        }
        let mut data = Vec::new();
        file.take(self.max_checkpoint_bytes.saturating_add(1))
            .read_to_end(&mut data)
            .map_err(|source| DurableCheckpointError::Read {
                path: self.path.clone(),
                source,
            })?;
        if data.len() as u64 > self.max_checkpoint_bytes {
            return Err(DurableCheckpointError::CheckpointTooLarge {
                path: self.path.clone(),
                bytes: data.len() as u64,
                max_bytes: self.max_checkpoint_bytes,
            });
        }
        let Some(checksum_start) = data.len().checked_sub(CHECKPOINT_CHECKSUM_BYTES) else {
            return Err(DurableCheckpointError::InvalidFormat {
                path: self.path.clone(),
            });
        };
        let encoded = &data[..checksum_start];
        let expected = B256::from_slice(&data[checksum_start..]);
        let actual = keccak256(encoded);
        if actual != expected {
            return Err(DurableCheckpointError::ChecksumMismatch {
                path: self.path.clone(),
            });
        }
        let snapshot = versioned::decode(
            encoded,
            CHECKPOINT_MAGIC,
            CHECKPOINT_VERSION,
            CHECKPOINT_LABEL,
        )
        .ok_or_else(|| DurableCheckpointError::InvalidFormat {
            path: self.path.clone(),
        })?;
        Ok(Some(LoadedDurableCheckpoint { snapshot }))
    }
}

fn checkpoint_coordinator(path: &Path) -> Arc<CheckpointWriteCoordinator> {
    let key = normalized_checkpoint_path(path);
    let coordinators = CHECKPOINT_COORDINATORS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut coordinators = coordinators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(coordinator) = coordinators.get(&key).and_then(Weak::upgrade) {
        return coordinator;
    }
    coordinators.retain(|_, coordinator| coordinator.strong_count() > 0);
    let coordinator = Arc::new(CheckpointWriteCoordinator::default());
    coordinators.insert(key, Arc::downgrade(&coordinator));
    coordinator
}

fn normalized_checkpoint_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };

    // Canonicalize the directory identity while deliberately preserving the
    // destination entry itself. This makes aliases through symlinked parents
    // share a writer coordinator, but an existing destination symlink is
    // replaced by the atomic rename rather than followed to another file.
    let Some(file_name) = absolute.file_name() else {
        return normalize_existing_path_prefix(&absolute);
    };
    let parent = absolute.parent().unwrap_or_else(|| Path::new("."));
    normalize_existing_path_prefix(parent).join(file_name)
}

fn normalize_existing_path_prefix(absolute: &Path) -> PathBuf {
    // Resolve every existing prefix through the OS before normalizing a
    // missing suffix. This preserves `symlink/..` semantics (which differ from
    // blindly popping path components) while producing the same key before and
    // after this store creates an ordinary missing directory suffix.
    let components: Vec<_> = absolute.components().collect();
    for split in (1..=components.len()).rev() {
        let prefix: PathBuf = components[..split]
            .iter()
            .map(|component| component.as_os_str())
            .collect();
        let Ok(mut resolved) = prefix.canonicalize() else {
            continue;
        };
        for component in &components[split..] {
            match component {
                Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
                Component::RootDir => resolved.push(component.as_os_str()),
                Component::CurDir => {}
                Component::ParentDir => {
                    let _ = resolved.pop();
                }
                Component::Normal(part) => resolved.push(part),
            }
        }
        return resolved;
    }

    // Absolute roots normally make the loop succeed. Retain a deterministic
    // fallback for unusual platforms/current-directory failures.
    absolute.to_path_buf()
}

fn validate_capture_identity(
    cache: &EvmCache,
    metadata: &DurableCheckpointMetadata,
) -> Result<(), DurableCheckpointError> {
    if metadata.identity.chain_id != cache.chain_id {
        return Err(DurableCheckpointError::CacheChainMismatch {
            cache_chain_id: cache.chain_id,
            checkpoint_chain_id: metadata.identity.chain_id,
        });
    }
    Ok(())
}

fn persist_snapshot(
    path: &Path,
    snapshot: DurableCheckpointSnapshot,
    coordinator: &CheckpointWriteCoordinator,
    generation: u64,
    max_checkpoint_bytes: u64,
) -> Result<(), DurableCheckpointError> {
    let _writer = coordinator
        .writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let latest = coordinator.latest_generation.load(Ordering::Acquire);
    if generation != latest {
        return Err(DurableCheckpointError::WriteSuperseded { generation, latest });
    }
    // Measure through bincode's size counter before allocating the encoded
    // payload. The configured ceiling is a memory-safety boundary as well as a
    // file-size boundary; checking only after `encode` would allocate the full
    // checkpoint first.
    let payload_bytes = bincode::serialized_size(&snapshot).map_err(|source| {
        DurableCheckpointError::Encode(crate::errors::PersistenceError::serialize(
            CHECKPOINT_LABEL,
            source,
        ))
    })?;
    let encoded_bytes = payload_bytes
        .checked_add(CHECKPOINT_HEADER_BYTES)
        .and_then(|bytes| bytes.checked_add(CHECKPOINT_CHECKSUM_BYTES as u64))
        .ok_or(DurableCheckpointError::CheckpointSizeOverflow {
            path: path.to_path_buf(),
        })?;
    if encoded_bytes > max_checkpoint_bytes {
        return Err(DurableCheckpointError::CheckpointTooLarge {
            path: path.to_path_buf(),
            bytes: encoded_bytes,
            max_bytes: max_checkpoint_bytes,
        });
    }
    let mut data = versioned::encode(
        CHECKPOINT_MAGIC,
        CHECKPOINT_VERSION,
        &snapshot,
        CHECKPOINT_LABEL,
    )
    .map_err(DurableCheckpointError::Encode)?;
    let checksum = keccak256(&data);
    data.extend_from_slice(checksum.as_slice());
    debug_assert_eq!(data.len() as u64, encoded_bytes);
    atomic_replace(path, &data)
}

/// A decoded checkpoint awaiting identity/hash validation and restore.
pub struct LoadedDurableCheckpoint {
    snapshot: DurableCheckpointSnapshot,
}

impl LoadedDurableCheckpoint {
    /// Inspect commit metadata before mutating the cache.
    pub fn metadata(&self) -> &DurableCheckpointMetadata {
        &self.snapshot.metadata
    }

    /// Restore the checkpoint into an already configured cache.
    ///
    /// The identity must match exactly and the cache must target the same chain.
    /// Callers should validate `metadata().block.hash` against an authoritative
    /// RPC source first when the checkpoint block is not finalized.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCheckpointError::IdentityMismatch`] when `expected`
    /// differs from stored metadata, or
    /// [`DurableCheckpointError::CacheChainMismatch`] when the configured cache
    /// targets another chain. The cache is not mutated on either error.
    pub fn restore_into(
        self,
        cache: &mut EvmCache,
        expected: &DurableCheckpointIdentity,
    ) -> Result<DurableCheckpointMetadata, DurableCheckpointError> {
        if &self.snapshot.metadata.identity != expected {
            return Err(DurableCheckpointError::IdentityMismatch {
                expected: expected.clone(),
                actual: self.snapshot.metadata.identity.clone(),
            });
        }
        if cache.chain_id != expected.chain_id {
            return Err(DurableCheckpointError::CacheChainMismatch {
                cache_chain_id: cache.chain_id,
                checkpoint_chain_id: expected.chain_id,
            });
        }
        Ok(self.snapshot.restore(cache))
    }
}

#[derive(Serialize, Deserialize)]
struct DurableCheckpointSnapshot {
    metadata: DurableCheckpointMetadata,
    state: EvmCacheStateSnapshot,
}

/// Complete mutable cache state used both by the on-disk checkpoint and by the
/// checkpointed engine's in-process rollback guard.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct EvmCacheStateSnapshot {
    backend_accounts: Vec<(Address, AccountInfo)>,
    backend_storage: Vec<(Address, Vec<(U256, U256)>)>,
    backend_block_hashes: Vec<(U256, B256)>,
    overlay: Cache,
    token_decimals: HashMap<Address, u8>,
    immutable_cache: ImmutableDataCache,
    code_seeds: HashMap<Address, CodeSeedState>,
    erc20_balance_slots: HashMap<Address, TrackedMapping>,
    block: PersistedBlockId,
    block_number: Option<u64>,
    basefee: Option<u64>,
    coinbase: Option<Address>,
    prevrandao: Option<B256>,
    block_gas_limit: Option<u64>,
    timestamp_override: Option<u64>,
    block_env_source: Option<BlockEnvSource>,
    spec_id: SpecId,
    snapshot_generation: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum PersistedBlockId {
    Hash {
        hash: B256,
        require_canonical: Option<bool>,
    },
    Latest,
    Finalized,
    Safe,
    Earliest,
    Pending,
    Number(u64),
}

impl From<BlockId> for PersistedBlockId {
    fn from(block: BlockId) -> Self {
        match block {
            BlockId::Hash(hash) => Self::Hash {
                hash: hash.block_hash,
                require_canonical: hash.require_canonical,
            },
            BlockId::Number(BlockNumberOrTag::Latest) => Self::Latest,
            BlockId::Number(BlockNumberOrTag::Finalized) => Self::Finalized,
            BlockId::Number(BlockNumberOrTag::Safe) => Self::Safe,
            BlockId::Number(BlockNumberOrTag::Earliest) => Self::Earliest,
            BlockId::Number(BlockNumberOrTag::Pending) => Self::Pending,
            BlockId::Number(BlockNumberOrTag::Number(number)) => Self::Number(number),
        }
    }
}

impl From<PersistedBlockId> for BlockId {
    fn from(block: PersistedBlockId) -> Self {
        match block {
            PersistedBlockId::Hash {
                hash,
                require_canonical,
            } => BlockId::Hash(RpcBlockHash::from_hash(hash, require_canonical)),
            PersistedBlockId::Latest => BlockId::latest(),
            PersistedBlockId::Finalized => BlockId::finalized(),
            PersistedBlockId::Safe => BlockId::safe(),
            PersistedBlockId::Earliest => BlockId::earliest(),
            PersistedBlockId::Pending => BlockId::pending(),
            PersistedBlockId::Number(number) => BlockId::number(number),
        }
    }
}

impl DurableCheckpointSnapshot {
    fn capture(cache: &EvmCache, metadata: DurableCheckpointMetadata) -> Self {
        let mut state = EvmCacheStateSnapshot::capture(cache);
        state.align_to_checkpoint_block(&metadata.block);
        Self { metadata, state }
    }

    fn restore(self, cache: &mut EvmCache) -> DurableCheckpointMetadata {
        let block_hash = self.metadata.block.hash;
        self.state.restore(cache);

        // Keep lazy RPC misses pinned to exactly the validated canonical block.
        // A number-only pin could silently mix this snapshot with a replacement
        // branch after a shallow reorg. Setting
        // the backend pin directly avoids `set_block` clearing the restored EVM
        // block context and incrementing the restored generation.
        let block = alloy_eips::BlockId::from((block_hash, Some(true)));
        cache.block = block;
        let _ = cache.backend.set_pinned_block(block);

        self.metadata
    }
}

impl EvmCacheStateSnapshot {
    pub(crate) fn capture(cache: &EvmCache) -> Self {
        let (backend_accounts, backend_storage, backend_block_hashes) =
            capture_backend_maps(&cache.blockchain_db);

        Self {
            backend_accounts,
            backend_storage,
            backend_block_hashes,
            overlay: cache.db.cache.clone(),
            token_decimals: cache.token_decimals.clone(),
            immutable_cache: cache.immutable_cache.clone(),
            code_seeds: cache.code_seeds.clone(),
            erc20_balance_slots: cache.erc20_balance_slots.clone(),
            block: cache.block.into(),
            block_number: cache.block_number,
            basefee: cache.basefee,
            coinbase: cache.coinbase,
            prevrandao: cache.prevrandao,
            block_gas_limit: cache.block_gas_limit,
            timestamp_override: cache.timestamp_override,
            block_env_source: cache.block_env_source,
            spec_id: cache.spec_id,
            snapshot_generation: cache.snapshot_generation,
        }
    }

    /// Make the persisted execution context internally agree with the
    /// checkpoint's authoritative canonical coverage.
    ///
    /// Compact indexer progress can advance the checkpoint without delivering
    /// a full header. In that case carrying an older cache `NUMBER`, timestamp,
    /// or fee environment alongside the newer exact pin would create a split
    /// state on restore. A full environment is retained only when cache
    /// provenance proves it came from this exact number/hash and any supplied
    /// checkpoint timestamp agrees. Otherwise number and timestamp are aligned
    /// from metadata and unproven header-only fields are cleared.
    fn align_to_checkpoint_block(&mut self, block: &DurableCheckpointBlock) {
        let preserve_full_env = matches!(
            self.block_env_source,
            Some(BlockEnvSource::VerifiedHash { number, hash })
                if number == block.number
                    && hash == block.hash
                    && block
                        .timestamp
                        .zip(self.timestamp_override)
                        .is_none_or(|(expected, actual)| expected == actual)
        );
        self.block = PersistedBlockId::Hash {
            hash: block.hash,
            require_canonical: Some(true),
        };
        self.block_number = Some(block.number);
        if !preserve_full_env {
            self.timestamp_override = block.timestamp;
            self.basefee = None;
            self.coinbase = None;
            self.prevrandao = None;
            self.block_gas_limit = None;
            self.block_env_source = None;
        }
    }

    pub(crate) fn restore(self, cache: &mut EvmCache) {
        {
            let mut accounts = cache.blockchain_db.accounts().write();
            accounts.clear();
            accounts.extend(self.backend_accounts);
        }
        {
            let mut storage = cache.blockchain_db.storage().write();
            storage.clear();
            storage.extend(
                self.backend_storage
                    .into_iter()
                    .map(|(address, slots)| (address, slots.into_iter().collect())),
            );
        }
        {
            let mut hashes = cache.blockchain_db.block_hashes().write();
            hashes.clear();
            hashes.extend(self.backend_block_hashes);
        }

        cache.db.cache = self.overlay;
        cache.token_decimals = self.token_decimals;
        cache.immutable_cache = self.immutable_cache;
        cache.code_seeds = self.code_seeds;
        cache.erc20_balance_slots = self.erc20_balance_slots;
        let block = BlockId::from(self.block);
        cache.block = block;
        let _ = cache.backend.set_pinned_block(block);
        cache.block_number = self.block_number;
        cache.basefee = self.basefee;
        cache.coinbase = self.coinbase;
        cache.prevrandao = self.prevrandao;
        cache.block_gas_limit = self.block_gas_limit;
        cache.timestamp_override = self.timestamp_override;
        cache.block_env_source = self.block_env_source;
        cache.spec_id = self.spec_id;
        cache.snapshot_generation = self.snapshot_generation;
        cache.base = None;
        cache.base_dirty.clear();
        cache.base_full_rebuild = true;
        cache.base_storage_lens.clear();
    }
}

type BackendMapsSnapshot = (
    Vec<(Address, AccountInfo)>,
    Vec<(Address, Vec<(U256, U256)>)>,
    Vec<(U256, B256)>,
);

fn capture_backend_maps(blockchain_db: &BlockchainDb) -> BackendMapsSnapshot {
    // Hold all three backend read guards together. `SharedBackend` applies
    // queued account/storage/block-hash mutations through these same locks;
    // retaining the earlier guards while later maps are cloned therefore gives
    // the snapshot one coherent point-in-time prefix instead of an impossible
    // old-account/new-storage mixture. The backend handler never holds more
    // than one of these locks, so this stable order cannot form a cycle with
    // normal lazy RPC population.
    let accounts = blockchain_db.accounts().read();
    let storage = blockchain_db.storage().read();
    let block_hashes = blockchain_db.block_hashes().read();
    let backend_accounts = accounts
        .iter()
        .map(|(address, info)| (*address, info.clone()))
        .collect();
    let backend_storage = storage
        .iter()
        .map(|(address, slots)| {
            (
                *address,
                slots.iter().map(|(key, value)| (*key, *value)).collect(),
            )
        })
        .collect();
    let backend_block_hashes = block_hashes
        .iter()
        .map(|(number, hash)| (*number, *hash))
        .collect();
    (backend_accounts, backend_storage, backend_block_hashes)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Barrier},
        thread,
        time::{Duration, Instant},
    };

    use foundry_fork_db::{BlockchainDb, cache::BlockchainDbMeta};

    use super::capture_backend_maps;
    #[cfg(unix)]
    use super::create_unique_temp_file;

    #[test]
    fn backend_capture_retains_earlier_guards_while_waiting_for_later_maps() {
        let blockchain_db = Arc::new(BlockchainDb::new(BlockchainDbMeta::default(), None));
        let storage_guard = blockchain_db.storage().write();
        let start = Arc::new(Barrier::new(2));
        let worker_db = Arc::clone(&blockchain_db);
        let worker_start = Arc::clone(&start);
        let capture = thread::spawn(move || {
            worker_start.wait();
            capture_backend_maps(&worker_db)
        });
        start.wait();

        // The worker can acquire the accounts read guard, but must block on the
        // storage write guard held above. A per-map clone that dropped the first
        // guard would allow this write; coherent capture must keep rejecting it.
        let deadline = Instant::now() + Duration::from_secs(2);
        let retained_accounts_guard = loop {
            if blockchain_db.accounts().try_write().is_none() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            thread::yield_now();
        };
        assert!(
            retained_accounts_guard,
            "capture must retain the accounts guard while awaiting storage"
        );

        drop(storage_guard);
        capture.join().expect("capture thread");
    }

    #[cfg(unix)]
    #[test]
    fn stale_temp_candidate_is_skipped_without_blocking_checkpoint_progress() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "evm-fork-cache-stale-temp-{}-{}",
            std::process::id(),
            super::NEXT_TEMP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).expect("create test directory");
        let destination = root.join("checkpoint.bin");
        let stale = root.join(".checkpoint.bin.first");
        let fresh = root.join(".checkpoint.bin.second");
        fs::write(&stale, b"stale crash residue").expect("precreate first candidate");
        let mut candidates = [stale.clone(), fresh.clone()].into_iter();

        let (selected, file) = create_unique_temp_file(&destination, || {
            candidates.next().expect("bounded test candidates")
        })
        .expect("collision must retry with the next candidate");
        drop(file);

        assert_eq!(selected, fresh);
        assert_eq!(
            fs::metadata(&selected)
                .expect("fresh temp metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "checkpoint temp files contain provider cursors and must be owner-only"
        );
        assert_eq!(
            fs::read(&stale).expect("stale file remains"),
            b"stale crash residue"
        );
        fs::remove_dir_all(root).expect("remove test directory");
    }
}

#[cfg(unix)]
fn atomic_replace(path: &Path, data: &[u8]) -> Result<(), DurableCheckpointError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| DurableCheckpointError::CreateDir {
        path: parent.to_path_buf(),
        source,
    })?;

    let (temp_path, mut file) = create_unique_temp_file(path, || next_temp_path(path))?;
    let result = (|| {
        file.write_all(data)
            .and_then(|()| file.sync_all())
            .map_err(|source| DurableCheckpointError::Write {
                path: temp_path.clone(),
                source,
            })?;
        fs::rename(&temp_path, path).map_err(|source| DurableCheckpointError::Rename {
            from: temp_path.clone(),
            to: path.to_path_buf(),
            source,
        })?;
        sync_parent_directory(parent)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

#[cfg(not(unix))]
fn atomic_replace(path: &Path, _data: &[u8]) -> Result<(), DurableCheckpointError> {
    Err(DurableCheckpointError::AtomicReplaceUnsupported {
        path: path.to_path_buf(),
    })
}

#[cfg(unix)]
fn create_unique_temp_file(
    destination: &Path,
    mut next_candidate: impl FnMut() -> PathBuf,
) -> Result<(PathBuf, File), DurableCheckpointError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    for _ in 0..MAX_TEMP_CREATE_ATTEMPTS {
        let candidate = next_candidate();
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate)
        {
            Ok(file) => {
                // `mode` prevents a permissive creation window; explicitly
                // resetting permissions also makes the contract independent
                // of an unusually restrictive process umask.
                if let Err(source) = file.set_permissions(fs::Permissions::from_mode(0o600)) {
                    drop(file);
                    let _ = fs::remove_file(&candidate);
                    return Err(DurableCheckpointError::Write {
                        path: candidate,
                        source,
                    });
                }
                return Ok((candidate, file));
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(DurableCheckpointError::Write {
                    path: candidate,
                    source,
                });
            }
        }
    }
    Err(DurableCheckpointError::TemporaryPathExhausted {
        path: destination.to_path_buf(),
        attempts: MAX_TEMP_CREATE_ATTEMPTS,
    })
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> Result<(), DurableCheckpointError> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| DurableCheckpointError::SyncDirectory {
            path: parent.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> Result<(), DurableCheckpointError> {
    // Rust has no portable directory-fsync primitive. The file itself has
    // already been synced before the atomic replace on these targets.
    Ok(())
}

#[cfg(unix)]
fn next_temp_path(path: &Path) -> PathBuf {
    let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkpoint");
    path.with_file_name(format!(".{name}.tmp-{}-{id}", std::process::id()))
}

/// Durable checkpoint persistence or compatibility failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DurableCheckpointError {
    /// This platform cannot provide the crate's required atomic replacement.
    #[error("atomic durable checkpoint replacement for {path:?} is unsupported on this platform")]
    AtomicReplaceUnsupported {
        /// Destination checkpoint path.
        path: PathBuf,
    },
    /// The checkpoint payload could not be serialized.
    #[error(transparent)]
    Encode(#[from] crate::errors::PersistenceError),
    /// The blocking checkpoint writer task was cancelled or panicked.
    #[error("durable checkpoint writer task failed: {0}")]
    TaskJoin(#[source] tokio::task::JoinError),
    /// The in-process writer generation counter cannot advance safely.
    #[error("durable checkpoint writer generation exhausted")]
    GenerationExhausted,
    /// A newer write was requested before this writer could install its snapshot.
    #[error(
        "durable checkpoint write generation {generation} was superseded by generation {latest}"
    )]
    WriteSuperseded {
        /// Generation assigned to this write.
        generation: u64,
        /// Latest generation requested from this store.
        latest: u64,
    },
    /// The checkpoint file could not be read.
    #[error("failed to read durable checkpoint {path:?}: {source}")]
    Read {
        /// Checkpoint path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The file does not carry the supported magic, version, or payload.
    #[error("durable checkpoint {path:?} has an invalid or unsupported format")]
    InvalidFormat {
        /// Checkpoint path.
        path: PathBuf,
    },
    /// The checkpoint checksum does not match its encoded contents.
    #[error("durable checkpoint {path:?} failed its integrity checksum")]
    ChecksumMismatch {
        /// Checkpoint path.
        path: PathBuf,
    },
    /// The checkpoint exceeds this store's configured resource bound.
    #[error(
        "durable checkpoint {path:?} is {bytes} bytes, exceeding the configured {max_bytes}-byte limit"
    )]
    CheckpointTooLarge {
        /// Checkpoint path.
        path: PathBuf,
        /// Encoded file size observed or produced.
        bytes: u64,
        /// Maximum encoded size accepted by the store.
        max_bytes: u64,
    },
    /// Encoded checkpoint size overflowed the supported `u64` accounting.
    #[error("durable checkpoint {path:?} size exceeds supported accounting")]
    CheckpointSizeOverflow {
        /// Checkpoint path.
        path: PathBuf,
    },
    /// The checkpoint directory could not be created.
    #[error("failed to create durable checkpoint directory {path:?}: {source}")]
    CreateDir {
        /// Directory path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The temporary checkpoint file could not be written or synced.
    #[error("failed to write durable checkpoint {path:?}: {source}")]
    Write {
        /// Temporary checkpoint path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: io::Error,
    },
    /// Every bounded unique temporary-file candidate already existed.
    #[error(
        "failed to allocate a unique temporary file for durable checkpoint {path:?} after {attempts} attempts"
    )]
    TemporaryPathExhausted {
        /// Destination checkpoint path.
        path: PathBuf,
        /// Number of collision retries attempted.
        attempts: usize,
    },
    /// The synced temporary file could not be atomically installed.
    #[error("failed to replace durable checkpoint {to:?} from {from:?}: {source}")]
    Rename {
        /// Temporary checkpoint path.
        from: PathBuf,
        /// Destination checkpoint path.
        to: PathBuf,
        /// Filesystem failure.
        #[source]
        source: io::Error,
    },
    /// The checkpoint rename committed but its containing directory could not
    /// be synced. The file may exist, but the caller must not acknowledge the
    /// delivery because crash durability was not established.
    #[error("failed to sync durable checkpoint directory {path:?}: {source}")]
    SyncDirectory {
        /// Directory path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: io::Error,
    },
    /// A checkpoint was opened under a different subscriber or handler schema.
    #[error("durable checkpoint identity mismatch: expected {expected:?}, found {actual:?}")]
    IdentityMismatch {
        /// Requested consumer identity.
        expected: DurableCheckpointIdentity,
        /// Stored consumer identity.
        actual: DurableCheckpointIdentity,
    },
    /// The configured cache and checkpoint identity target different chains.
    #[error(
        "durable checkpoint chain {checkpoint_chain_id} does not match cache chain {cache_chain_id}"
    )]
    CacheChainMismatch {
        /// Current cache chain id.
        cache_chain_id: u64,
        /// Stored/requested checkpoint chain id.
        checkpoint_chain_id: u64,
    },
}
