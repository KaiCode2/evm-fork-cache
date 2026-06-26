//! Protocol-neutral reactive runtime for cache state effects.
//!
//! The reactive runtime generalizes the log-only [`events`](crate::events)
//! pipeline into a handler pipeline that can ingest logs, block notifications,
//! and pending transaction signals. Handlers remain pure synchronous functions:
//! they read through [`StateView`], return structured
//! [`ReactiveEffect`] values, and let the runtime validate and commit cache
//! mutations through [`StateUpdate`].
//!
//! This module intentionally contains no protocol, AMM, strategy, signing, or
//! transaction-submission concepts. Downstream crates can layer those domains on
//! top by implementing [`ReactiveHandler`] and [`ReactiveHook`].

use std::{
    any::Any,
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use alloy_consensus::{BlockHeader as _, Transaction as _};
use alloy_eips::BlockId;
use alloy_network::{
    Ethereum, Network,
    primitives::{
        BlockResponse as _, HeaderResponse as HeaderResponseTrait,
        TransactionResponse as TransactionResponseTrait,
    },
};
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, FilterSet, Log};
use futures::{
    StreamExt,
    stream::{self, BoxStream, SelectAll},
};

use crate::{
    cache::EvmCache,
    events::{EventDecoder, StateView},
    state_update::{AccountPatch, PurgeScope, StateDiff, StateUpdate},
};

/// Input accepted by the reactive runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReactiveInput<N: Network = Ethereum> {
    /// A canonical or removed EVM log, using Alloy's RPC log type.
    Log(Log),
    /// A block header response for header-oriented handlers.
    BlockHeader(N::HeaderResponse),
    /// A full block response for block handlers that need transaction bodies.
    FullBlock(N::BlockResponse),
    /// A pending transaction hash.
    PendingTxHash(B256),
    /// A full pending transaction body.
    PendingTx(N::TransactionResponse),
}

/// Context supplied with each [`ReactiveInput`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReactiveContext {
    /// Chain id, when known.
    pub chain_id: Option<u64>,
    /// Where the input came from.
    pub source: InputSource,
    /// Lifecycle status of the input.
    pub chain_status: ChainStatus,
    /// Block metadata associated with the input, when known.
    pub block: Option<BlockRef>,
    /// Transaction index for log or transaction inputs.
    pub transaction_index: Option<u64>,
    /// Log index for log inputs.
    pub log_index: Option<u64>,
}

/// Minimal block identity carried through reports.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockRef {
    /// Block number.
    pub number: u64,
    /// Block hash.
    pub hash: B256,
    /// Parent hash, when known.
    pub parent_hash: Option<B256>,
    /// Block timestamp, when known.
    pub timestamp: Option<u64>,
}

/// Lifecycle status for an input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainStatus {
    /// The input is mempool-only and must not mutate canonical cache state.
    Pending,
    /// The input is included in a block with a confirmation count.
    Included {
        /// Included block.
        block: BlockRef,
        /// Confirmation count.
        confirmations: u64,
    },
    /// The input is in the chain's safe head.
    Safe {
        /// Safe block.
        block: BlockRef,
    },
    /// The input is in the finalized head.
    Finalized {
        /// Finalized block.
        block: BlockRef,
    },
    /// The input was dropped by a reorg.
    Reorged {
        /// Block the input was dropped from.
        dropped_from: BlockRef,
    },
}

/// Source of an input batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InputSource {
    /// Caller-supplied batch.
    Batch,
    /// Live subscription stream.
    Subscription,
    /// Polling subscriber.
    Poll,
    /// Historical backfill.
    Backfill,
    /// Test or synthetic input.
    Synthetic,
}

/// Stable identity used for input deduplication and reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InputRef {
    /// Stable log identity.
    Log {
        /// Chain id, when known.
        chain_id: Option<u64>,
        /// Block hash containing the log.
        block_hash: B256,
        /// Transaction hash that emitted the log.
        transaction_hash: B256,
        /// Log index within the block.
        log_index: u64,
    },
    /// Stable pending transaction identity.
    PendingTx {
        /// Chain id, when known.
        chain_id: Option<u64>,
        /// Transaction hash.
        hash: B256,
    },
    /// Stable block identity.
    Block {
        /// Chain id, when known.
        chain_id: Option<u64>,
        /// Block hash.
        hash: B256,
        /// Block number.
        number: u64,
    },
}

/// Reliability of state effects emitted by a handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StateEffectQuality {
    /// Effects are exact from the input alone.
    ExactFromInput,
    /// Effects were applied, but follow-up resync is pending.
    AppliedWithPendingResync,
    /// Effects came from authoritative resync.
    ResyncedAuthoritatively,
    /// State requires repair before it should be trusted.
    RequiresRepair,
    /// No canonical state effect was emitted.
    NoStateEffect,
}

/// Identifier for a reactive handler.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HandlerId(String);

impl HandlerId {
    /// Create a handler id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Return the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HandlerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Lightweight report label.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReportTag {
    /// Label key.
    pub key: String,
    /// Label value.
    pub value: String,
}

impl ReportTag {
    /// Create a report tag.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Domain-neutral hook signal emitted by a handler.
#[derive(Clone)]
pub struct HookSignal {
    /// Signal namespace owned by the caller.
    pub namespace: Cow<'static, str>,
    /// Signal kind within the namespace.
    pub kind: Cow<'static, str>,
    /// Additional labels for routing or observability.
    pub labels: Vec<ReportTag>,
    /// Optional in-process typed payload.
    pub payload: Option<Arc<dyn Any + Send + Sync>>,
}

impl fmt::Debug for HookSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HookSignal")
            .field("namespace", &self.namespace)
            .field("kind", &self.kind)
            .field("labels", &self.labels)
            .field("payload", &self.payload.as_ref().map(|_| "<payload>"))
            .finish()
    }
}

/// Effect emitted by a [`ReactiveHandler`].
#[derive(Clone, Debug)]
pub enum ReactiveEffect {
    /// Canonical cache mutation applied through [`EvmCache::apply_updates`].
    StateUpdate(StateUpdate),
    /// Request for authoritative state repair.
    Resync(ResyncRequest),
    /// Rich invalidation request lowered to [`StateUpdate::Purge`].
    Invalidate(InvalidationRequest),
    /// Hook signal dispatched after committed mutation phases.
    Hook(HookSignal),
    /// Speculative signal for mempool or downstream work.
    Speculative(SpeculativeRequest),
}

/// Handler output for a single input.
#[derive(Clone, Debug)]
pub struct HandlerOutcome {
    /// Effects emitted by the handler.
    pub effects: Vec<ReactiveEffect>,
    /// Reliability of emitted state effects.
    pub quality: StateEffectQuality,
    /// Labels copied into reports.
    pub tags: Vec<ReportTag>,
}

impl HandlerOutcome {
    /// Construct an empty outcome with the supplied quality.
    pub fn empty(quality: StateEffectQuality) -> Self {
        Self {
            effects: Vec::new(),
            quality,
            tags: Vec::new(),
        }
    }
}

/// One input and its execution context.
#[derive(Clone, Debug)]
pub struct ReactiveInputRecord<N: Network = Ethereum> {
    /// Input value.
    pub input: ReactiveInput<N>,
    /// Input context.
    pub context: ReactiveContext,
}

impl<N: Network> ReactiveInputRecord<N> {
    /// Create an input record.
    pub fn new(input: ReactiveInput<N>, context: ReactiveContext) -> Self {
        Self { input, context }
    }

    /// Compute the stable input reference used for deduplication.
    pub fn input_ref(&self) -> InputRef {
        input_ref(&self.input, &self.context)
    }
}

/// Batch of reactive input records.
#[derive(Clone, Debug)]
pub struct ReactiveInputBatch<N: Network = Ethereum> {
    records: Vec<ReactiveInputRecord<N>>,
}

impl<N: Network> ReactiveInputBatch<N> {
    /// Create a batch from records.
    pub fn new(records: Vec<ReactiveInputRecord<N>>) -> Self {
        Self { records }
    }

    /// Borrow the records in this batch.
    pub fn records(&self) -> &[ReactiveInputRecord<N>] {
        &self.records
    }

    /// Consume the batch into its records.
    pub fn into_records(self) -> Vec<ReactiveInputRecord<N>> {
        self.records
    }
}

/// Pure synchronous handler for reactive inputs.
pub trait ReactiveHandler<N: Network = Ethereum>: Send + Sync {
    /// Stable handler id.
    fn id(&self) -> HandlerId;

    /// Interests used by subscribers and the local router.
    fn interests(&self) -> Vec<ReactiveInterest<N>>;

    /// Handle one input against a read-only cache view.
    fn handle(
        &self,
        ctx: &ReactiveContext,
        input: &ReactiveInput<N>,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError>;
}

/// Hook invoked after reports are built and cache mutation phases have ended.
pub trait ReactiveHook<N: Network = Ethereum>: Send + Sync {
    /// Observe a runtime report.
    fn on_report(&self, report: Arc<ReactiveReport<N>>);
}

/// Reactive subscription interest.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum ReactiveInterest<N: Network = Ethereum> {
    /// Log interest.
    Logs(LogInterest),
    /// Block interest.
    Blocks(BlockInterest),
    /// Pending transaction interest.
    PendingTransactions(PendingTxInterest<N>),
}

impl<N: Network> fmt::Debug for ReactiveInterest<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Logs(interest) => f.debug_tuple("Logs").field(interest).finish(),
            Self::Blocks(interest) => f.debug_tuple("Blocks").field(interest).finish(),
            Self::PendingTransactions(interest) => f
                .debug_tuple("PendingTransactions")
                .field(interest)
                .finish(),
        }
    }
}

/// Interest in logs.
#[derive(Clone)]
pub struct LogInterest {
    /// Provider-side filter.
    pub provider_filter: Filter,
    /// Optional local matcher for predicates providers cannot express.
    pub local_matcher: Option<Arc<dyn LogMatcher>>,
    /// Optional route-key extraction strategy.
    pub route_key: Option<RouteKeySpec>,
}

impl LogInterest {
    /// Return true if the log matches both the provider filter and local matcher.
    pub fn matches(&self, log: &Log) -> bool {
        self.provider_filter.rpc_matches(log)
            && self
                .local_matcher
                .as_ref()
                .is_none_or(|matcher| matcher.matches(log))
    }

    /// Extract the route key for a matching log, if configured.
    pub fn route_key(&self, log: &Log) -> Option<RouteKey> {
        self.route_key.as_ref().and_then(|spec| spec.extract(log))
    }
}

impl fmt::Debug for LogInterest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogInterest")
            .field("provider_filter", &self.provider_filter)
            .field(
                "local_matcher",
                &self.local_matcher.as_ref().map(|_| "<matcher>"),
            )
            .field("route_key", &self.route_key)
            .finish()
    }
}

/// Local log predicate.
pub trait LogMatcher: Send + Sync {
    /// Return true when the log should be routed to the handler.
    fn matches(&self, log: &Log) -> bool;
}

/// Route-key extraction strategy for logs.
#[derive(Clone)]
pub enum RouteKeySpec {
    /// Route by emitting address.
    EmitterAddress,
    /// Route by indexed topic.
    Topic {
        /// Topic index.
        index: usize,
    },
    /// Route by a byte slice in log data.
    DataSlice {
        /// Byte offset in the data payload.
        offset: usize,
        /// Number of bytes to copy.
        len: usize,
    },
    /// Custom extractor.
    Custom(Arc<dyn RouteKeyExtractor>),
}

impl RouteKeySpec {
    /// Extract a route key from a log.
    pub fn extract(&self, log: &Log) -> Option<RouteKey> {
        match self {
            Self::EmitterAddress => Some(RouteKey::Address(log.address())),
            Self::Topic { index } => log.topics().get(*index).copied().map(RouteKey::Bytes32),
            Self::DataSlice { offset, len } => {
                let data = log.inner.data.data.as_ref();
                let end = offset.checked_add(*len)?;
                data.get(*offset..end)
                    .map(|bytes| RouteKey::Bytes(bytes.to_vec()))
            }
            Self::Custom(extractor) => extractor.extract(log),
        }
    }
}

impl fmt::Debug for RouteKeySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmitterAddress => f.write_str("EmitterAddress"),
            Self::Topic { index } => f.debug_struct("Topic").field("index", index).finish(),
            Self::DataSlice { offset, len } => f
                .debug_struct("DataSlice")
                .field("offset", offset)
                .field("len", len)
                .finish(),
            Self::Custom(_) => f.write_str("Custom(<extractor>)"),
        }
    }
}

/// Extracts custom route keys from logs.
pub trait RouteKeyExtractor: Send + Sync {
    /// Extract a route key.
    fn extract(&self, log: &Log) -> Option<RouteKey>;
}

/// Extracted route key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouteKey {
    /// Address key.
    Address(Address),
    /// 32-byte key.
    Bytes32(B256),
    /// Arbitrary bytes key.
    Bytes(Vec<u8>),
}

/// Exact log route selected by [`ReactiveRegistry::route_log`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReactiveLogRoute {
    /// Handler whose log interest matched.
    pub handler_id: HandlerId,
    /// Optional route key extracted from the matching log interest.
    pub route_key: Option<RouteKey>,
}

/// Interest in block inputs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockInterest {
    /// Block input mode.
    pub mode: BlockInterestMode,
}

impl Default for BlockInterest {
    fn default() -> Self {
        Self {
            mode: BlockInterestMode::Header,
        }
    }
}

/// Block subscription mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockInterestMode {
    /// Header-only block input.
    Header,
    /// Full block input.
    FullBlock,
}

/// Interest in pending transaction inputs.
#[derive(Clone)]
pub struct PendingTxInterest<N: Network = Ethereum> {
    /// Whether the handler requires full transaction bodies.
    pub full_transactions: bool,
    /// Sender matcher.
    pub from: AddressMatcher,
    /// Recipient matcher.
    pub to: AddressMatcher,
    /// Calldata selector matcher.
    pub selectors: SelectorMatcher,
    /// Optional local transaction matcher.
    pub local_matcher: Option<Arc<dyn PendingTxMatcher<N>>>,
}

impl<N: Network> Default for PendingTxInterest<N> {
    fn default() -> Self {
        Self {
            full_transactions: false,
            from: AddressMatcher::Any,
            to: AddressMatcher::Any,
            selectors: SelectorMatcher::Any,
            local_matcher: None,
        }
    }
}

impl<N: Network> PendingTxInterest<N> {
    fn matches_hash_only(&self) -> bool {
        !self.full_transactions
            && self.from.is_any()
            && self.to.is_any()
            && self.selectors.is_any()
            && self.local_matcher.is_none()
    }

    fn matches_tx(&self, tx: &N::TransactionResponse) -> bool {
        self.from.matches(tx.from())
            && self.to.matches_option(tx.to())
            && self.selectors.matches(tx.input())
            && self
                .local_matcher
                .as_ref()
                .is_none_or(|matcher| matcher.matches(tx))
    }
}

impl<N: Network> fmt::Debug for PendingTxInterest<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingTxInterest")
            .field("full_transactions", &self.full_transactions)
            .field("from", &self.from)
            .field("to", &self.to)
            .field("selectors", &self.selectors)
            .field(
                "local_matcher",
                &self.local_matcher.as_ref().map(|_| "<matcher>"),
            )
            .finish()
    }
}

/// Address matching helper for pending transaction interests.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AddressMatcher {
    /// Match every address.
    Any,
    /// Match one address.
    Exact(Address),
    /// Match any address in the list.
    AnyOf(Vec<Address>),
}

impl AddressMatcher {
    /// Return true when the matcher is unconstrained.
    pub fn is_any(&self) -> bool {
        matches!(self, Self::Any)
    }

    /// Match a present address.
    pub fn matches(&self, address: Address) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => *expected == address,
            Self::AnyOf(addresses) => addresses.contains(&address),
        }
    }

    /// Match an optional address.
    pub fn matches_option(&self, address: Option<Address>) -> bool {
        match (self, address) {
            (Self::Any, _) => true,
            (_, Some(address)) => self.matches(address),
            _ => false,
        }
    }
}

/// Calldata selector matching helper.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SelectorMatcher {
    /// Match every selector.
    Any,
    /// Match any selector in the list.
    AnyOf(Vec<[u8; 4]>),
}

impl SelectorMatcher {
    /// Return true when the matcher is unconstrained.
    pub fn is_any(&self) -> bool {
        matches!(self, Self::Any)
    }

    /// Match calldata bytes.
    pub fn matches(&self, input: &Bytes) -> bool {
        match self {
            Self::Any => true,
            Self::AnyOf(selectors) => input
                .get(..4)
                .and_then(|bytes| bytes.try_into().ok())
                .is_some_and(|selector| selectors.contains(&selector)),
        }
    }
}

/// Local predicate over a full pending transaction.
pub trait PendingTxMatcher<N: Network = Ethereum>: Send + Sync {
    /// Return true when the transaction should be routed to the handler.
    fn matches(&self, tx: &N::TransactionResponse) -> bool;
}

/// Request for authoritative state repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResyncRequest {
    /// Resync id.
    pub id: ResyncId,
    /// Reason for the request.
    pub reason: ResyncReason,
    /// Block selection for the read.
    pub block: ResyncBlock,
    /// Targets to resync.
    pub targets: Vec<ResyncTarget>,
    /// Scheduling priority.
    pub priority: ResyncPriority,
}

/// Resync id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResyncId(String);

impl ResyncId {
    /// Create a resync id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Reason for a resync request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ResyncReason {
    /// Handler requested repair.
    HandlerRequested,
    /// State effect could not be applied completely.
    SkippedStateEffect,
    /// Caller-defined reason.
    Custom(String),
}

/// Block target for a resync.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ResyncBlock {
    /// Latest block.
    Latest,
    /// Safe head.
    Safe,
    /// Finalized head.
    Finalized,
    /// Block number.
    Number(u64),
    /// Block hash and number.
    Hash {
        /// Block number.
        number: u64,
        /// Block hash.
        hash: B256,
        /// Require the hash to still be canonical.
        require_canonical: bool,
    },
}

/// State target for a resync.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ResyncTarget {
    /// One storage slot.
    StorageSlot {
        /// Contract address.
        address: Address,
        /// Storage slot.
        slot: U256,
    },
    /// Multiple storage slots on one contract.
    StorageSlots {
        /// Contract address.
        address: Address,
        /// Storage slots.
        slots: Vec<U256>,
    },
    /// Account fields.
    Account {
        /// Account address.
        address: Address,
        /// Fields to resync.
        fields: AccountFieldMask,
    },
}

/// Account fields requested by a resync.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct AccountFieldMask {
    /// Balance field.
    pub balance: bool,
    /// Nonce field.
    pub nonce: bool,
    /// Code field.
    pub code: bool,
}

/// Resync priority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResyncPriority {
    /// Low priority.
    Low,
    /// Normal priority.
    #[default]
    Normal,
    /// High priority.
    High,
}

/// Rich invalidation request lowered to [`StateUpdate::Purge`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidationRequest {
    /// Purge scope.
    pub scope: PurgeScope,
    /// Address to purge.
    pub address: Address,
    /// Reason for reporting.
    pub reason: InvalidationReason,
}

/// Invalidation reason.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum InvalidationReason {
    /// Handler requested invalidation.
    HandlerRequested,
    /// Reorg invalidation.
    Reorg,
    /// Caller-defined reason.
    Custom(String),
}

/// Speculative signal emitted by handlers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeculativeRequest {
    /// Speculative request id.
    pub id: SpeculativeId,
    /// Input that triggered the request.
    pub input_ref: InputRef,
    /// Labels for downstream routing.
    pub labels: Vec<ReportTag>,
}

/// Speculative request id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SpeculativeId(String);

impl SpeculativeId {
    /// Create a speculative id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Configuration for [`ReactiveRuntime`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReactiveConfig {
    /// Hook backpressure policy. **Reserved — currently has no effect.** Hook
    /// dispatch is synchronous today (every report is delivered to every hook in
    /// order), so this field is a no-op placeholder for a future async dispatcher.
    /// Setting it to anything other than the default does not change behavior.
    pub hook_backpressure: HookBackpressure,
    /// Reorg journal depth: the number of recent canonical blocks whose effects
    /// are journaled for rollback. This is **load-bearing** for reorg recovery —
    /// a reorg deeper than `journal_depth` cannot roll back reversible writes for
    /// the aged-out blocks and degrades to a targeted purge. `0` disables
    /// journaling entirely (every reorg falls back to purge).
    pub journal_depth: usize,
}

impl Default for ReactiveConfig {
    fn default() -> Self {
        Self {
            hook_backpressure: HookBackpressure::Block,
            journal_depth: 64,
        }
    }
}

/// Hook backpressure policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HookBackpressure {
    /// Block the producer until hooks are accepted.
    Block,
    /// Drop the newest report under pressure.
    DropNewest,
    /// Drop the oldest report under pressure.
    DropOldest,
    /// Return an error under pressure.
    Error,
}

/// Runtime report.
#[derive(Clone, Debug)]
pub enum ReactiveReport<N: Network = Ethereum> {
    /// Input was accepted after deduplication.
    Input(InputReport<N>),
    /// Handlers produced outcomes.
    Decoded(DecodedReport<N>),
    /// Direct state effects were applied.
    Applied(AppliedReport<N>),
    /// Resync request was scheduled or completed.
    Resynced(ResyncReport),
    /// Block-level processing completed.
    BlockCommitted(BlockReport<N>),
    /// Reorg processing report.
    Reorg(ReorgReport<N>),
    /// Runtime or handler error.
    Error(ReactiveErrorReport<N>),
}

/// Input acceptance report.
#[derive(Clone, Debug)]
pub struct InputReport<N: Network = Ethereum> {
    /// Input reference.
    pub input_ref: InputRef,
    /// Input context.
    pub context: ReactiveContext,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Decoding report.
#[derive(Clone, Debug)]
pub struct DecodedReport<N: Network = Ethereum> {
    /// Input reference.
    pub input_ref: InputRef,
    /// Handler ids that matched the input.
    pub handler_ids: Vec<HandlerId>,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Applied state report.
#[derive(Clone, Debug)]
pub struct AppliedReport<N: Network = Ethereum> {
    /// Input reference.
    pub input_ref: InputRef,
    /// Handler that produced the applied effects.
    pub handler_id: HandlerId,
    /// State effect quality.
    pub quality: StateEffectQuality,
    /// Labels emitted by the handler.
    pub tags: Vec<ReportTag>,
    /// Merged state diff from applied updates and invalidations.
    pub diff: StateDiff,
    /// State updates applied through the cache.
    pub state_updates: Vec<StateUpdate>,
    /// Invalidation requests lowered to purge updates.
    pub invalidations: Vec<InvalidationRequest>,
    /// Resync requests surfaced for a scheduler.
    pub resyncs: Vec<ResyncRequest>,
    /// Speculative requests surfaced for downstream users.
    pub speculative: Vec<SpeculativeRequest>,
    /// Hook signals emitted by the handler.
    pub hook_signals: Vec<HookSignal>,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Report of the storage resync requests executed during an ingest cycle: the
/// requests considered, the authoritative updates built from successful fetches
/// (and their applied diff), and any targets that could not be resynced.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResyncReport {
    /// Requests considered by the resync execution pass.
    pub requested: Vec<ResyncRequest>,
    /// Authoritative state updates built from successful resync fetches.
    pub state_updates: Vec<StateUpdate>,
    /// Diff returned by applying [`state_updates`](Self::state_updates).
    pub diff: StateDiff,
    /// Targets that could not be resynced.
    pub failed: Vec<ResyncFailure>,
}

/// One resync target that could not be fetched or applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResyncFailure {
    /// Request that produced the failed target.
    pub request_id: ResyncId,
    /// Block selection used for the failed target.
    pub block: ResyncBlock,
    /// Target that could not be resynced.
    pub target: ResyncTarget,
    /// Stable failure classification for retry policy and metrics.
    pub kind: ResyncFailureKind,
    /// Human-readable failure reason.
    pub message: String,
}

/// Stable classification for a failed resync target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResyncFailureKind {
    /// A storage target could not be fetched because no storage batch fetcher is configured.
    MissingStorageFetcher,
    /// The storage batch fetcher returned an error for the requested slot.
    StorageFetchFailed,
    /// The storage batch fetcher did not return a result for the requested slot.
    StorageFetchOmitted,
    /// Account-field resync is not supported by the current provider-neutral cache seam.
    UnsupportedAccountTarget,
}

/// Block processing report.
#[derive(Clone, Debug)]
pub struct BlockReport<N: Network = Ethereum> {
    /// Block reference, when known.
    pub block: Option<BlockRef>,
    /// Input references committed for the block.
    pub inputs: Vec<InputRef>,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Report of a detected reorg and the recovery it performed: the dropped
/// block(s) and inputs, the exact rollback updates applied for reversible dropped
/// effects, the conservative purge updates for irreversible ones, the canceled
/// hash-pinned resyncs, and why recovery ran.
#[derive(Clone, Debug)]
pub struct ReorgReport<N: Network = Ethereum> {
    /// First dropped block, when known.
    pub dropped: Option<BlockRef>,
    /// Blocks dropped from the journal, in ascending journal order.
    pub dropped_blocks: Vec<BlockRef>,
    /// Input references that belonged to dropped blocks.
    pub dropped_inputs: Vec<InputRef>,
    /// Exact rollback updates applied for reversible dropped effects.
    pub rollback_updates: Vec<StateUpdate>,
    /// Diff returned by applying [`rollback_updates`](Self::rollback_updates).
    pub rollback_diff: StateDiff,
    /// Conservative purge updates applied for irreversible dropped effects.
    pub purge_updates: Vec<StateUpdate>,
    /// Diff returned by applying [`purge_updates`](Self::purge_updates).
    pub purge_diff: StateDiff,
    /// Hash-pinned pending resync requests canceled because their block was dropped.
    pub canceled_resyncs: Vec<ResyncRequest>,
    /// Reorg trigger.
    pub reason: ReorgReason,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Reason reorg recovery ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReorgReason {
    /// A provider emitted an Alloy removed log.
    RemovedLog,
    /// The input context explicitly marked an input as reorged.
    ReorgedInput,
    /// A canonical block did not connect to the journaled head.
    ParentMismatch,
}

/// Report of a non-fatal error surfaced during an ingest cycle, with the
/// associated input (when known) and a human-readable message.
#[derive(Clone, Debug)]
pub struct ReactiveErrorReport<N: Network = Ethereum> {
    /// Input associated with the error, when known.
    pub input_ref: Option<InputRef>,
    /// Error message.
    pub message: String,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Batch report returned by [`ReactiveRuntime::ingest_batch`] and
/// [`ReactiveRuntime::ingest_batch_with_resync`].
#[derive(Clone, Debug)]
pub struct ReactiveBatchReport<N: Network = Ethereum> {
    /// Applied reports in commit order.
    pub applied: Vec<AppliedReport<N>>,
    /// Resync requests surfaced during the batch.
    pub resyncs: Vec<ResyncRequest>,
    /// Speculative requests surfaced during the batch.
    pub speculative: Vec<SpeculativeRequest>,
    /// Hook reports dispatched after mutation phases.
    pub reports: Vec<Arc<ReactiveReport<N>>>,
}

impl<N: Network> Default for ReactiveBatchReport<N> {
    fn default() -> Self {
        Self {
            applied: Vec::new(),
            resyncs: Vec::new(),
            speculative: Vec::new(),
            reports: Vec::new(),
        }
    }
}

/// Error returned by a handler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandlerError {
    message: String,
}

impl HandlerError {
    /// Create a handler error from a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for HandlerError {}

impl From<String> for HandlerError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for HandlerError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

/// Runtime error.
#[derive(Debug, thiserror::Error)]
pub enum ReactiveError {
    /// Handler returned an error.
    #[error("handler `{handler_id}` failed: {source}")]
    HandlerFailed {
        /// Handler id.
        handler_id: HandlerId,
        /// Handler error.
        source: HandlerError,
    },
    /// Multiple handlers emitted incompatible absolute writes for one input.
    #[error(
        "conflicting effects for input {input_ref:?} on target {target:?}: `{first}` vs `{second}`"
    )]
    ConflictingEffects {
        /// Input reference.
        input_ref: Box<InputRef>,
        /// Conflicting target.
        target: Box<EffectTarget>,
        /// First handler id.
        first: HandlerId,
        /// Second handler id.
        second: HandlerId,
    },
    /// Pending inputs attempted to mutate canonical cache state.
    #[error(
        "pending input {input_ref:?} emitted invalid canonical effect `{effect_kind}` from `{handler_id}`"
    )]
    InvalidPendingEffect {
        /// Input reference.
        input_ref: Box<InputRef>,
        /// Handler id.
        handler_id: HandlerId,
        /// Effect kind.
        effect_kind: &'static str,
    },
    /// Registration error.
    #[error(transparent)]
    Register(#[from] RegisterError),
}

/// Handler registration error.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// Duplicate handler id.
    #[error("handler id `{0}` is already registered")]
    DuplicateHandler(HandlerId),
}

/// Absolute write target used for conflict reports.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EffectTarget {
    /// Storage slot target.
    StorageSlot {
        /// Contract address.
        address: Address,
        /// Storage slot.
        slot: U256,
    },
    /// Account balance target.
    AccountBalance {
        /// Account address.
        address: Address,
    },
    /// Account nonce target.
    AccountNonce {
        /// Account address.
        address: Address,
    },
    /// Account code target.
    AccountCode {
        /// Account address.
        address: Address,
    },
    /// Masked storage slot target.
    MaskedStorageSlot {
        /// Contract address.
        address: Address,
        /// Storage slot.
        slot: U256,
        /// Bit mask.
        mask: U256,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AbsoluteValue {
    U256(U256),
    U64(u64),
    Bytes(Bytes),
}

/// Reactive runtime.
pub struct ReactiveRuntime<N: Network = Ethereum> {
    registry: ReactiveRegistry<N>,
    hooks: Vec<Arc<dyn ReactiveHook<N>>>,
    config: ReactiveConfig,
    journal: VecDeque<BlockJournal<N>>,
    pending_resyncs: Vec<ResyncRequest>,
}

#[derive(Clone, Debug)]
struct BlockJournal<N: Network = Ethereum> {
    block: BlockRef,
    inputs: Vec<InputRef>,
    applied: Vec<AppliedReport<N>>,
    resynced: Vec<ResyncReport>,
}

/// Registry and router for provider-neutral reactive handlers.
///
/// The registry stores pure [`ReactiveHandler`]s in registration order, exposes
/// consolidated provider-side log filters for subscription setup, and routes
/// provider logs back to the exact matching log interests. Consolidated filters
/// may be safe supersets; [`Self::route_log`] always re-checks the original
/// [`LogInterest`] and its local matcher before returning a route.
pub struct ReactiveRegistry<N: Network = Ethereum> {
    handlers: Vec<RegisteredHandler<N>>,
}

struct RegisteredHandler<N: Network = Ethereum> {
    id: HandlerId,
    handler: Arc<dyn ReactiveHandler<N>>,
    interests: Vec<ReactiveInterest<N>>,
}

impl<N: Network> Default for ReactiveRegistry<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Network> ReactiveRegistry<N> {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    /// Register a handler, preserving registration order.
    ///
    /// Duplicate handler ids are rejected with
    /// [`RegisterError::DuplicateHandler`].
    pub fn register_handler(
        &mut self,
        handler: Arc<dyn ReactiveHandler<N>>,
    ) -> Result<(), RegisterError> {
        let id = handler.id();
        if self.handlers.iter().any(|registered| registered.id == id) {
            return Err(RegisterError::DuplicateHandler(id));
        }
        let interests = handler.interests();
        self.handlers.push(RegisteredHandler {
            id,
            handler,
            interests,
        });
        Ok(())
    }

    /// Return all registered interests in handler registration order.
    pub fn interests(&self) -> Vec<ReactiveInterest<N>> {
        self.handlers
            .iter()
            .flat_map(|handler| handler.interests.clone())
            .collect()
    }

    /// Return consolidated provider-side log filters.
    ///
    /// Filters are emitted in deterministic first-registration order by
    /// compatible block option. Within each returned filter, address and topic
    /// sets are unioned independently, which can intentionally overfetch. Use
    /// [`Self::route_log`] to enforce the exact original [`LogInterest`]s.
    pub fn log_subscription_filters(&self) -> Vec<Filter> {
        let mut filters = Vec::new();
        for interest in self.log_interests() {
            merge_log_subscription_filter(&mut filters, &interest.provider_filter);
        }
        filters
    }

    /// Route a log to exact matching handler interests.
    ///
    /// Routes are returned in handler registration order. Each handler appears
    /// at most once for a log, using the first matching log interest declared by
    /// that handler.
    pub fn route_log(&self, log: &Log) -> Vec<ReactiveLogRoute> {
        self.handlers
            .iter()
            .filter_map(|handler| handler.route_log(log))
            .collect()
    }

    fn handlers(&self) -> &[RegisteredHandler<N>] {
        &self.handlers
    }

    fn log_interests(&self) -> impl Iterator<Item = &LogInterest> {
        self.handlers.iter().flat_map(|handler| {
            handler
                .interests
                .iter()
                .filter_map(|interest| match interest {
                    ReactiveInterest::Logs(interest) => Some(interest),
                    ReactiveInterest::Blocks(_) | ReactiveInterest::PendingTransactions(_) => None,
                })
        })
    }
}

impl<N: Network> ReactiveRuntime<N> {
    /// Create an empty runtime.
    pub fn new(config: ReactiveConfig) -> Self {
        Self {
            registry: ReactiveRegistry::new(),
            hooks: Vec::new(),
            config,
            journal: VecDeque::new(),
            pending_resyncs: Vec::new(),
        }
    }

    /// Register a handler.
    pub fn register_handler(
        &mut self,
        handler: Arc<dyn ReactiveHandler<N>>,
    ) -> Result<(), RegisterError> {
        self.registry.register_handler(handler)
    }

    /// Register a hook.
    pub fn register_hook(&mut self, hook: Arc<dyn ReactiveHook<N>>) -> Result<(), RegisterError> {
        self.hooks.push(hook);
        Ok(())
    }

    /// Return all registered interests in handler registration order.
    pub fn interests(&self) -> Vec<ReactiveInterest<N>> {
        self.registry.interests()
    }

    /// Ingest a batch, apply valid direct state effects, and dispatch reports.
    pub fn ingest_batch(
        &mut self,
        cache: &mut EvmCache,
        batch: ReactiveInputBatch<N>,
    ) -> Result<ReactiveBatchReport<N>, ReactiveError> {
        let batch_report = self.ingest_batch_direct(cache, batch)?;
        self.dispatch_reports(&batch_report.reports);
        let _ = &self.config;
        Ok(batch_report)
    }

    /// Ingest a batch, then execute surfaced storage resync requests.
    ///
    /// This entrypoint preserves [`ingest_batch`](Self::ingest_batch) behavior for
    /// direct handler effects, then runs a synchronous resync phase over the
    /// collected [`ResyncRequest`]s. Storage targets are fetched through
    /// [`EvmCache::storage_batch_fetcher`] grouped by [`ResyncBlock`], successful
    /// values are applied as [`StateUpdate::slot`] updates through
    /// [`EvmCache::apply_updates`], and unsupported or failed targets are reported
    /// in [`ResyncReport::failed`]. It does not start subscribers, background
    /// workers, or network transport.
    pub fn ingest_batch_with_resync(
        &mut self,
        cache: &mut EvmCache,
        batch: ReactiveInputBatch<N>,
    ) -> Result<ReactiveBatchReport<N>, ReactiveError> {
        let mut batch_report = self.ingest_batch_direct(cache, batch)?;

        if !batch_report.resyncs.is_empty() {
            let resync_report = execute_resync_requests(cache, &batch_report.resyncs);
            self.remove_pending_resyncs(batch_report.resyncs.iter().map(|request| &request.id));
            self.record_journal_resync(&resync_report);
            batch_report
                .reports
                .push(Arc::new(ReactiveReport::Resynced(resync_report)));
        }

        self.dispatch_reports(&batch_report.reports);
        let _ = &self.config;
        Ok(batch_report)
    }

    fn ingest_batch_direct(
        &mut self,
        cache: &mut EvmCache,
        batch: ReactiveInputBatch<N>,
    ) -> Result<ReactiveBatchReport<N>, ReactiveError> {
        let records = sort_records(dedupe_records(batch.into_records()));

        let mut batch_report = ReactiveBatchReport::default();
        let mut reports_to_dispatch = Vec::new();

        for record in records {
            let input_ref = record.input_ref();
            reports_to_dispatch.push(Arc::new(ReactiveReport::Input(InputReport {
                input_ref,
                context: record.context.clone(),
                _network: PhantomData,
            })));

            if let Some(reorg_report) = self.recover_for_canonical_input(cache, &record) {
                remove_canceled_resyncs_from_batch(
                    &mut batch_report.resyncs,
                    &reorg_report.canceled_resyncs,
                );
                reports_to_dispatch.push(Arc::new(ReactiveReport::Reorg(reorg_report)));
            }

            if let Some(reorg_report) = self.recover_for_reorged_input(cache, &record) {
                remove_canceled_resyncs_from_batch(
                    &mut batch_report.resyncs,
                    &reorg_report.canceled_resyncs,
                );
                reports_to_dispatch.push(Arc::new(ReactiveReport::Reorg(reorg_report)));
                continue;
            }

            if let Some(block) = canonical_record_block(&record) {
                self.record_journal_input(block, input_ref);
            }

            let executions = self.execute_handlers(cache, &record, input_ref)?;
            if executions.is_empty() {
                continue;
            }

            reports_to_dispatch.push(Arc::new(ReactiveReport::Decoded(DecodedReport {
                input_ref,
                handler_ids: executions
                    .iter()
                    .map(|execution| execution.handler_id.clone())
                    .collect(),
                _network: PhantomData,
            })));

            detect_conflicts(input_ref, &executions)?;

            for execution in executions {
                let diff = if execution.state_updates.is_empty() {
                    StateDiff::default()
                } else {
                    cache.apply_updates(&execution.state_updates)
                };

                batch_report
                    .resyncs
                    .extend(execution.resyncs.iter().cloned());
                self.pending_resyncs
                    .extend(execution.resyncs.iter().cloned());
                batch_report
                    .speculative
                    .extend(execution.speculative.iter().cloned());

                let applied = AppliedReport {
                    input_ref,
                    handler_id: execution.handler_id,
                    quality: execution.quality,
                    tags: execution.tags,
                    diff,
                    state_updates: execution.state_updates,
                    invalidations: execution.invalidations,
                    resyncs: execution.resyncs,
                    speculative: execution.speculative,
                    hook_signals: execution.hook_signals,
                    _network: PhantomData,
                };
                let report = Arc::new(ReactiveReport::Applied(applied.clone()));
                reports_to_dispatch.push(report);
                if let Some(block) = canonical_record_block(&record) {
                    self.record_journal_applied(block, applied.clone());
                }
                batch_report.applied.push(applied);
            }
        }

        batch_report.reports = reports_to_dispatch;
        Ok(batch_report)
    }

    fn execute_handlers(
        &self,
        cache: &EvmCache,
        record: &ReactiveInputRecord<N>,
        input_ref: InputRef,
    ) -> Result<Vec<HandlerExecution>, ReactiveError> {
        let mut executions = Vec::new();
        for registered in self.registry.handlers() {
            if !registered.matches(&record.input) {
                continue;
            }

            let outcome = registered
                .handler
                .handle(&record.context, &record.input, cache)
                .map_err(|source| ReactiveError::HandlerFailed {
                    handler_id: registered.id.clone(),
                    source,
                })?;

            validate_effects(input_ref, &record.context, &registered.id, &outcome.effects)?;
            executions.push(HandlerExecution::from_outcome(
                registered.id.clone(),
                input_ref,
                outcome,
            ));
        }
        Ok(executions)
    }

    fn dispatch_reports(&self, reports: &[Arc<ReactiveReport<N>>]) {
        for report in reports {
            for hook in &self.hooks {
                hook.on_report(report.clone());
            }
        }
    }

    fn recover_for_canonical_input(
        &mut self,
        cache: &mut EvmCache,
        record: &ReactiveInputRecord<N>,
    ) -> Option<ReorgReport<N>> {
        let block = canonical_record_block(record)?;
        let latest = self.journal.back()?.block.clone();

        if self
            .journal
            .iter()
            .any(|entry| entry.block.hash == block.hash && entry.block.number == block.number)
        {
            return None;
        }

        if block.number == latest.number.saturating_add(1) && block.parent_hash == Some(latest.hash)
        {
            return None;
        }

        if block.number > latest.number.saturating_add(1) {
            return None;
        }

        let dropped = if let Some(parent_hash) = block.parent_hash {
            if let Some(parent_index) = self
                .journal
                .iter()
                .rposition(|entry| entry.block.hash == parent_hash)
            {
                self.drain_journal_after(parent_index)
            } else {
                self.drain_journal_from_number(block.number)
            }
        } else {
            self.drain_journal_from_number(block.number)
        };

        self.recover_dropped_journals(cache, dropped, ReorgReason::ParentMismatch)
    }

    fn recover_for_reorged_input(
        &mut self,
        cache: &mut EvmCache,
        record: &ReactiveInputRecord<N>,
    ) -> Option<ReorgReport<N>> {
        let (dropped_block, reason) = reorg_signal_block(record)?;
        let dropped = if let Some(index) = self
            .journal
            .iter()
            .position(|entry| entry.block.hash == dropped_block.hash)
        {
            self.drain_journal_from(index)
        } else {
            self.drain_journal_from_number(dropped_block.number)
        };

        if dropped.is_empty() {
            let canceled_resyncs =
                self.cancel_resyncs_for_dropped_blocks(std::slice::from_ref(&dropped_block));
            if canceled_resyncs.is_empty() {
                return None;
            }
            return Some(ReorgReport {
                dropped: Some(dropped_block.clone()),
                dropped_blocks: vec![dropped_block],
                dropped_inputs: Vec::new(),
                rollback_updates: Vec::new(),
                rollback_diff: StateDiff::default(),
                purge_updates: Vec::new(),
                purge_diff: StateDiff::default(),
                canceled_resyncs,
                reason,
                _network: PhantomData,
            });
        }

        self.recover_dropped_journals(cache, dropped, reason)
    }

    fn record_journal_input(&mut self, block: &BlockRef, input_ref: InputRef) {
        let entry = self.journal_entry_mut(block);
        if !entry.inputs.contains(&input_ref) {
            entry.inputs.push(input_ref);
        }
        self.trim_journal();
    }

    fn record_journal_applied(&mut self, block: &BlockRef, applied: AppliedReport<N>) {
        self.journal_entry_mut(block).applied.push(applied);
        self.trim_journal();
    }

    fn record_journal_resync(&mut self, report: &ResyncReport) {
        if report.diff.is_empty() {
            return;
        }
        let Some(block) = single_hash_pinned_resync_block(report) else {
            return;
        };
        self.journal_entry_mut(&block).resynced.push(report.clone());
        self.trim_journal();
    }

    fn journal_entry_mut(&mut self, block: &BlockRef) -> &mut BlockJournal<N> {
        if let Some(index) = self
            .journal
            .iter()
            .position(|entry| entry.block.hash == block.hash && entry.block.number == block.number)
        {
            return &mut self.journal[index];
        }

        self.journal.push_back(BlockJournal {
            block: block.clone(),
            inputs: Vec::new(),
            applied: Vec::new(),
            resynced: Vec::new(),
        });
        let index = self.journal.len() - 1;
        &mut self.journal[index]
    }

    fn trim_journal(&mut self) {
        if self.config.journal_depth == 0 {
            self.journal.clear();
            return;
        }
        while self.journal.len() > self.config.journal_depth {
            self.journal.pop_front();
        }
    }

    fn drain_journal_after(&mut self, index: usize) -> Vec<BlockJournal<N>> {
        self.journal.drain((index + 1)..).collect()
    }

    fn drain_journal_from(&mut self, index: usize) -> Vec<BlockJournal<N>> {
        self.journal.drain(index..).collect()
    }

    fn drain_journal_from_number(&mut self, number: u64) -> Vec<BlockJournal<N>> {
        let Some(index) = self
            .journal
            .iter()
            .position(|entry| entry.block.number >= number)
        else {
            return Vec::new();
        };
        self.drain_journal_from(index)
    }

    fn recover_dropped_journals(
        &mut self,
        cache: &mut EvmCache,
        dropped: Vec<BlockJournal<N>>,
        reason: ReorgReason,
    ) -> Option<ReorgReport<N>> {
        if dropped.is_empty() {
            return None;
        }

        let dropped_blocks: Vec<_> = dropped.iter().map(|entry| entry.block.clone()).collect();
        let dropped_inputs: Vec<_> = dropped
            .iter()
            .flat_map(|entry| entry.inputs.iter().copied())
            .collect();
        let canceled_resyncs = self.cancel_resyncs_for_dropped_blocks(&dropped_blocks);
        let purge_scopes = purge_scopes_for_dropped_journals(&dropped);
        let rollback_updates = rollback_updates_for_dropped_journals(&dropped, &purge_scopes);
        let purge_updates: Vec<_> = purge_scopes
            .iter()
            .map(|(address, scope)| StateUpdate::purge(*address, scope.clone()))
            .collect();

        let rollback_diff = if rollback_updates.is_empty() {
            StateDiff::default()
        } else {
            cache.apply_updates(&rollback_updates)
        };
        let purge_diff = if purge_updates.is_empty() {
            StateDiff::default()
        } else {
            cache.apply_updates(&purge_updates)
        };

        Some(ReorgReport {
            dropped: dropped_blocks.first().cloned(),
            dropped_blocks,
            dropped_inputs,
            rollback_updates,
            rollback_diff,
            purge_updates,
            purge_diff,
            canceled_resyncs,
            reason,
            _network: PhantomData,
        })
    }

    fn cancel_resyncs_for_dropped_blocks(
        &mut self,
        dropped_blocks: &[BlockRef],
    ) -> Vec<ResyncRequest> {
        let mut canceled = Vec::new();
        self.pending_resyncs.retain(|request| {
            let should_cancel = resync_request_targets_dropped_block(request, dropped_blocks);
            if should_cancel {
                canceled.push(request.clone());
            }
            !should_cancel
        });
        canceled
    }

    fn remove_pending_resyncs<'a>(&mut self, ids: impl IntoIterator<Item = &'a ResyncId>) {
        let ids: HashSet<_> = ids.into_iter().cloned().collect();
        self.pending_resyncs
            .retain(|request| !ids.contains(&request.id));
    }
}

fn canonical_record_block<N: Network>(record: &ReactiveInputRecord<N>) -> Option<&BlockRef> {
    if matches!(&record.input, ReactiveInput::Log(log) if log.removed) {
        return None;
    }
    if is_canonical_status(&record.context.chain_status) {
        return context_block_ref(&record.context);
    }
    None
}

fn context_block_ref(ctx: &ReactiveContext) -> Option<&BlockRef> {
    match &ctx.chain_status {
        ChainStatus::Included { block, .. }
        | ChainStatus::Safe { block }
        | ChainStatus::Finalized { block } => Some(block),
        ChainStatus::Reorged { dropped_from } => Some(dropped_from),
        ChainStatus::Pending => ctx.block.as_ref(),
    }
}

fn reorg_signal_block<N: Network>(
    record: &ReactiveInputRecord<N>,
) -> Option<(BlockRef, ReorgReason)> {
    if matches!(&record.input, ReactiveInput::Log(log) if log.removed) {
        return block_ref_from_record(record).map(|block| (block, ReorgReason::RemovedLog));
    }

    if let ChainStatus::Reorged { dropped_from } = &record.context.chain_status {
        return Some((dropped_from.clone(), ReorgReason::ReorgedInput));
    }

    None
}

fn block_ref_from_record<N: Network>(record: &ReactiveInputRecord<N>) -> Option<BlockRef> {
    context_block_ref(&record.context)
        .cloned()
        .or_else(|| match &record.input {
            ReactiveInput::Log(log) => Some(BlockRef {
                number: log.block_number?,
                hash: log.block_hash?,
                parent_hash: None,
                timestamp: log.block_timestamp,
            }),
            ReactiveInput::BlockHeader(header) => Some(BlockRef {
                number: header.number(),
                hash: header.hash(),
                parent_hash: Some(header.parent_hash()),
                timestamp: Some(header.timestamp()),
            }),
            ReactiveInput::FullBlock(block) => {
                let header = block.header();
                Some(BlockRef {
                    number: header.number(),
                    hash: header.hash(),
                    parent_hash: Some(header.parent_hash()),
                    timestamp: Some(header.timestamp()),
                })
            }
            ReactiveInput::PendingTxHash(_) | ReactiveInput::PendingTx(_) => None,
        })
}

fn remove_canceled_resyncs_from_batch(
    resyncs: &mut Vec<ResyncRequest>,
    canceled: &[ResyncRequest],
) {
    if canceled.is_empty() {
        return;
    }
    let canceled_ids: HashSet<_> = canceled.iter().map(|request| request.id.clone()).collect();
    resyncs.retain(|request| !canceled_ids.contains(&request.id));
}

fn resync_request_targets_dropped_block(
    request: &ResyncRequest,
    dropped_blocks: &[BlockRef],
) -> bool {
    let ResyncBlock::Hash { number, hash, .. } = &request.block else {
        return false;
    };
    dropped_blocks
        .iter()
        .any(|block| block.hash == *hash && block.number == *number)
}

fn single_hash_pinned_resync_block(report: &ResyncReport) -> Option<BlockRef> {
    let first = report.requested.first()?.block.clone();
    if !report
        .requested
        .iter()
        .all(|request| request.block == first)
    {
        return None;
    }

    let ResyncBlock::Hash { number, hash, .. } = first else {
        return None;
    };

    Some(BlockRef {
        number,
        hash,
        parent_hash: None,
        timestamp: None,
    })
}

fn purge_scopes_for_dropped_journals<N: Network>(
    dropped: &[BlockJournal<N>],
) -> Vec<(Address, PurgeScope)> {
    let mut scopes: Vec<(Address, PurgeScope)> = Vec::new();
    for entry in dropped.iter().rev() {
        for resynced in entry.resynced.iter().rev() {
            merge_purge_scopes_for_diff(&mut scopes, &resynced.diff);
        }
        for applied in entry.applied.iter().rev() {
            merge_purge_scopes_for_diff(&mut scopes, &applied.diff);
        }
    }
    scopes
}

fn rollback_updates_for_dropped_journals<N: Network>(
    dropped: &[BlockJournal<N>],
    purge_scopes: &[(Address, PurgeScope)],
) -> Vec<StateUpdate> {
    let purge_addresses: HashSet<_> = purge_scopes
        .iter()
        .map(|(address, _scope)| *address)
        .collect();
    let mut updates = Vec::new();
    for entry in dropped.iter().rev() {
        for resynced in entry.resynced.iter().rev() {
            push_rollback_updates_for_diff(&mut updates, &resynced.diff, &purge_addresses);
        }
        for applied in entry.applied.iter().rev() {
            push_rollback_updates_for_diff(&mut updates, &applied.diff, &purge_addresses);
        }
    }
    updates
}

fn merge_purge_scopes_for_diff(scopes: &mut Vec<(Address, PurgeScope)>, diff: &StateDiff) {
    for change in &diff.accounts {
        merge_purge_scope(scopes, change.address, PurgeScope::Account);
    }
    for record in &diff.purged {
        merge_purge_scope(scopes, record.address, record.scope.clone());
    }
}

fn push_rollback_updates_for_diff(
    updates: &mut Vec<StateUpdate>,
    diff: &StateDiff,
    purge_addresses: &HashSet<Address>,
) {
    for change in diff.slots.iter().rev() {
        if purge_addresses.contains(&change.address) {
            continue;
        }
        updates.push(StateUpdate::slot(change.address, change.slot, change.old));
    }
}

fn merge_purge_scope(scopes: &mut Vec<(Address, PurgeScope)>, address: Address, scope: PurgeScope) {
    if let Some((_existing_address, existing_scope)) = scopes
        .iter_mut()
        .find(|(existing_address, _scope)| *existing_address == address)
    {
        *existing_scope = merged_purge_scope(existing_scope.clone(), scope);
    } else {
        scopes.push((address, scope));
    }
}

fn merged_purge_scope(left: PurgeScope, right: PurgeScope) -> PurgeScope {
    match (left, right) {
        (PurgeScope::Account, _) | (_, PurgeScope::Account) => PurgeScope::Account,
        (PurgeScope::AllStorage, _) | (_, PurgeScope::AllStorage) => PurgeScope::AllStorage,
        (PurgeScope::Slots(mut left), PurgeScope::Slots(right)) => {
            for slot in right {
                if !left.contains(&slot) {
                    left.push(slot);
                }
            }
            PurgeScope::Slots(left)
        }
    }
}

#[derive(Clone, Debug)]
struct StorageFetchSlot {
    address: Address,
    slot: U256,
    origins: Vec<StorageFetchOrigin>,
}

#[derive(Clone, Debug)]
struct StorageFetchOrigin {
    request_id: ResyncId,
    target: ResyncTarget,
}

#[derive(Clone, Debug)]
struct StorageFetchGroup {
    block: ResyncBlock,
    slots: Vec<StorageFetchSlot>,
    seen: HashSet<(Address, U256)>,
}

fn execute_resync_requests(cache: &mut EvmCache, requests: &[ResyncRequest]) -> ResyncReport {
    let mut failed = Vec::new();
    let mut storage_groups: Vec<StorageFetchGroup> = Vec::new();

    for request in requests {
        for target in &request.targets {
            match target {
                ResyncTarget::StorageSlot { address, slot } => {
                    push_storage_resync_slot(
                        &mut storage_groups,
                        &request.id,
                        &request.block,
                        *address,
                        *slot,
                    );
                }
                ResyncTarget::StorageSlots { address, slots } => {
                    for slot in slots {
                        push_storage_resync_slot(
                            &mut storage_groups,
                            &request.id,
                            &request.block,
                            *address,
                            *slot,
                        );
                    }
                }
                ResyncTarget::Account { .. } => failed.push(ResyncFailure {
                    request_id: request.id.clone(),
                    block: request.block.clone(),
                    target: target.clone(),
                    kind: ResyncFailureKind::UnsupportedAccountTarget,
                    message:
                        "account resync is unsupported until a provider-neutral account fetcher exists"
                            .to_string(),
                }),
            }
        }
    }

    let mut state_updates = Vec::new();
    if !storage_groups.is_empty() {
        if let Some(fetcher) = cache.storage_batch_fetcher().cloned() {
            for group in storage_groups {
                let block = group.block.clone();
                let fetches: Vec<(Address, U256)> = group
                    .slots
                    .iter()
                    .map(|slot| (slot.address, slot.slot))
                    .collect();
                let results = (fetcher)(fetches, Some(resync_block_to_block_id(&block)));
                let mut pending: HashMap<(Address, U256), StorageFetchSlot> = group
                    .slots
                    .iter()
                    .cloned()
                    .map(|slot| ((slot.address, slot.slot), slot))
                    .collect();

                for (address, slot, fetched) in results {
                    let Some(requested_slot) = pending.remove(&(address, slot)) else {
                        continue;
                    };
                    match fetched {
                        Ok(value) => state_updates.push(StateUpdate::slot(address, slot, value)),
                        Err(error) => {
                            let message = error.to_string();
                            push_resync_failures(
                                &mut failed,
                                &block,
                                requested_slot.origins,
                                ResyncFailureKind::StorageFetchFailed,
                                message,
                            );
                        }
                    }
                }

                for requested_slot in group.slots {
                    if pending
                        .remove(&(requested_slot.address, requested_slot.slot))
                        .is_some()
                    {
                        push_resync_failures(
                            &mut failed,
                            &block,
                            requested_slot.origins,
                            ResyncFailureKind::StorageFetchOmitted,
                            "storage batch fetcher did not return a value for slot".to_string(),
                        );
                    }
                }
            }
        } else {
            for group in storage_groups {
                let block = group.block.clone();
                for slot in group.slots {
                    push_resync_failures(
                        &mut failed,
                        &block,
                        slot.origins,
                        ResyncFailureKind::MissingStorageFetcher,
                        "storage resync requires a storage batch fetcher".to_string(),
                    );
                }
            }
        }
    }

    let diff = if state_updates.is_empty() {
        StateDiff::default()
    } else {
        cache.apply_updates(&state_updates)
    };

    ResyncReport {
        requested: requests.to_vec(),
        state_updates,
        diff,
        failed,
    }
}

fn push_resync_failures(
    failed: &mut Vec<ResyncFailure>,
    block: &ResyncBlock,
    origins: Vec<StorageFetchOrigin>,
    kind: ResyncFailureKind,
    message: String,
) {
    for origin in origins {
        failed.push(ResyncFailure {
            request_id: origin.request_id,
            block: block.clone(),
            target: origin.target,
            kind,
            message: message.clone(),
        });
    }
}

fn push_storage_resync_slot(
    groups: &mut Vec<StorageFetchGroup>,
    request_id: &ResyncId,
    block: &ResyncBlock,
    address: Address,
    slot: U256,
) {
    let group_index = if let Some(index) = groups.iter().position(|group| group.block == *block) {
        index
    } else {
        groups.push(StorageFetchGroup {
            block: block.clone(),
            slots: Vec::new(),
            seen: HashSet::new(),
        });
        groups.len() - 1
    };

    let group = &mut groups[group_index];
    let origin = StorageFetchOrigin {
        request_id: request_id.clone(),
        target: ResyncTarget::StorageSlot { address, slot },
    };
    if group.seen.insert((address, slot)) {
        group.slots.push(StorageFetchSlot {
            address,
            slot,
            origins: vec![origin],
        });
    } else if let Some(existing) = group
        .slots
        .iter_mut()
        .find(|existing| existing.address == address && existing.slot == slot)
    {
        existing.origins.push(origin);
    }
}

fn resync_block_to_block_id(block: &ResyncBlock) -> BlockId {
    match block {
        ResyncBlock::Latest => BlockId::latest(),
        ResyncBlock::Safe => BlockId::safe(),
        ResyncBlock::Finalized => BlockId::finalized(),
        ResyncBlock::Number(number) => BlockId::number(*number),
        ResyncBlock::Hash {
            number: _,
            hash,
            require_canonical,
        } => BlockId::from((*hash, Some(*require_canonical))),
    }
}

impl<N: Network> RegisteredHandler<N> {
    fn matches(&self, input: &ReactiveInput<N>) -> bool {
        self.interests
            .iter()
            .any(|interest| interest_matches(interest, input))
    }

    fn route_log(&self, log: &Log) -> Option<ReactiveLogRoute> {
        self.interests.iter().find_map(|interest| match interest {
            ReactiveInterest::Logs(interest) if interest.matches(log) => Some(ReactiveLogRoute {
                handler_id: self.id.clone(),
                route_key: interest.route_key(log),
            }),
            ReactiveInterest::Logs(_)
            | ReactiveInterest::Blocks(_)
            | ReactiveInterest::PendingTransactions(_) => None,
        })
    }
}

fn merge_log_subscription_filter(filters: &mut Vec<Filter>, next: &Filter) {
    if let Some(existing) = filters
        .iter_mut()
        .find(|existing| existing.block_option == next.block_option)
    {
        merge_filter_set(&mut existing.address, &next.address);
        for (existing_topic, next_topic) in existing.topics.iter_mut().zip(next.topics.iter()) {
            merge_filter_set(existing_topic, next_topic);
        }
    } else {
        filters.push(next.clone());
    }
}

fn merge_filter_set<T: Clone + Eq + Hash>(target: &mut FilterSet<T>, source: &FilterSet<T>) {
    if target.is_empty() {
        return;
    }
    if source.is_empty() {
        *target = FilterSet::default();
        return;
    }
    for value in source.iter() {
        target.insert(value.clone());
    }
}

#[derive(Clone, Debug)]
struct HandlerExecution {
    handler_id: HandlerId,
    quality: StateEffectQuality,
    tags: Vec<ReportTag>,
    state_updates: Vec<StateUpdate>,
    invalidations: Vec<InvalidationRequest>,
    resyncs: Vec<ResyncRequest>,
    speculative: Vec<SpeculativeRequest>,
    hook_signals: Vec<HookSignal>,
}

impl HandlerExecution {
    fn from_outcome(handler_id: HandlerId, input_ref: InputRef, outcome: HandlerOutcome) -> Self {
        let mut state_updates = Vec::new();
        let mut invalidations = Vec::new();
        let mut resyncs = Vec::new();
        let mut speculative = Vec::new();
        let mut hook_signals = Vec::new();

        for effect in outcome.effects {
            match effect {
                ReactiveEffect::StateUpdate(update) => state_updates.push(update),
                ReactiveEffect::Invalidate(invalidation) => {
                    state_updates.push(StateUpdate::purge(
                        invalidation.address,
                        invalidation.scope.clone(),
                    ));
                    invalidations.push(invalidation);
                }
                ReactiveEffect::Resync(request) => resyncs.push(request),
                ReactiveEffect::Hook(signal) => hook_signals.push(signal),
                ReactiveEffect::Speculative(mut request) => {
                    request.input_ref = input_ref;
                    speculative.push(request);
                }
            }
        }

        Self {
            handler_id,
            quality: outcome.quality,
            tags: outcome.tags,
            state_updates,
            invalidations,
            resyncs,
            speculative,
            hook_signals,
        }
    }
}

fn dedupe_records<N: Network>(records: Vec<ReactiveInputRecord<N>>) -> Vec<ReactiveInputRecord<N>> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(records.len());
    for record in records {
        if seen.insert(record.input_ref()) {
            deduped.push(record);
        }
    }
    deduped
}

fn sort_records<N: Network>(records: Vec<ReactiveInputRecord<N>>) -> Vec<ReactiveInputRecord<N>> {
    let mut indexed: Vec<(usize, ReactiveInputRecord<N>)> =
        records.into_iter().enumerate().collect();
    indexed.sort_by_key(|(index, record)| record_sort_key(*index, record));
    indexed.into_iter().map(|(_, record)| record).collect()
}

fn record_sort_key<N: Network>(index: usize, record: &ReactiveInputRecord<N>) -> RecordSortKey {
    if let ReactiveInput::Log(log) = &record.input
        && is_canonical_status(&record.context.chain_status)
        && !log.removed
    {
        return RecordSortKey {
            class: 0,
            block_number: log
                .block_number
                .or(record.context.block.as_ref().map(|block| block.number))
                .unwrap_or(u64::MAX),
            transaction_index: log
                .transaction_index
                .or(record.context.transaction_index)
                .unwrap_or(u64::MAX),
            log_index: log
                .log_index
                .or(record.context.log_index)
                .unwrap_or(u64::MAX),
            original_index: index,
        };
    }

    RecordSortKey {
        class: 1,
        block_number: 0,
        transaction_index: 0,
        log_index: 0,
        original_index: index,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RecordSortKey {
    class: u8,
    block_number: u64,
    transaction_index: u64,
    log_index: u64,
    original_index: usize,
}

fn interest_matches<N: Network>(interest: &ReactiveInterest<N>, input: &ReactiveInput<N>) -> bool {
    match (interest, input) {
        (ReactiveInterest::Logs(interest), ReactiveInput::Log(log)) => interest.matches(log),
        (
            ReactiveInterest::Blocks(BlockInterest {
                mode: BlockInterestMode::Header,
            }),
            ReactiveInput::BlockHeader(_),
        ) => true,
        (
            ReactiveInterest::Blocks(BlockInterest {
                mode: BlockInterestMode::FullBlock,
            }),
            ReactiveInput::FullBlock(_),
        ) => true,
        (ReactiveInterest::PendingTransactions(interest), ReactiveInput::PendingTxHash(_)) => {
            interest.matches_hash_only()
        }
        (ReactiveInterest::PendingTransactions(interest), ReactiveInput::PendingTx(tx)) => {
            interest.matches_tx(tx)
        }
        _ => false,
    }
}

fn validate_effects(
    input_ref: InputRef,
    ctx: &ReactiveContext,
    handler_id: &HandlerId,
    effects: &[ReactiveEffect],
) -> Result<(), ReactiveError> {
    let pending = matches!(ctx.chain_status, ChainStatus::Pending)
        || matches!(input_ref, InputRef::PendingTx { .. });
    if !pending {
        return Ok(());
    }

    for effect in effects {
        let effect_kind = match effect {
            ReactiveEffect::StateUpdate(_) => Some("state_update"),
            ReactiveEffect::Invalidate(_) => Some("invalidate"),
            ReactiveEffect::Resync(_) => Some("resync"),
            ReactiveEffect::Hook(_) | ReactiveEffect::Speculative(_) => None,
        };
        if let Some(effect_kind) = effect_kind {
            return Err(ReactiveError::InvalidPendingEffect {
                input_ref: Box::new(input_ref),
                handler_id: handler_id.clone(),
                effect_kind,
            });
        }
    }
    Ok(())
}

fn detect_conflicts(
    input_ref: InputRef,
    executions: &[HandlerExecution],
) -> Result<(), ReactiveError> {
    let mut writes: HashMap<EffectTarget, (AbsoluteValue, HandlerId)> = HashMap::new();
    for execution in executions {
        for update in &execution.state_updates {
            for (target, value) in absolute_writes(update) {
                if let Some((previous_value, previous_handler)) = writes.get(&target) {
                    if previous_value != &value {
                        return Err(ReactiveError::ConflictingEffects {
                            input_ref: Box::new(input_ref),
                            target: Box::new(target),
                            first: previous_handler.clone(),
                            second: execution.handler_id.clone(),
                        });
                    }
                } else {
                    writes.insert(target, (value, execution.handler_id.clone()));
                }
            }
        }
    }
    Ok(())
}

fn absolute_writes(update: &StateUpdate) -> Vec<(EffectTarget, AbsoluteValue)> {
    match update {
        StateUpdate::Slot {
            address,
            slot,
            value,
        } => vec![(
            EffectTarget::StorageSlot {
                address: *address,
                slot: *slot,
            },
            AbsoluteValue::U256(*value),
        )],
        StateUpdate::SlotMasked {
            address,
            slot,
            mask,
            value,
        } => vec![(
            EffectTarget::MaskedStorageSlot {
                address: *address,
                slot: *slot,
                mask: *mask,
            },
            AbsoluteValue::U256(*value),
        )],
        StateUpdate::Account { address, patch } | StateUpdate::AccountUpsert { address, patch } => {
            account_patch_writes(*address, patch)
        }
        StateUpdate::SlotDelta { .. }
        | StateUpdate::BalanceDelta { .. }
        | StateUpdate::Purge { .. } => Vec::new(),
    }
}

fn account_patch_writes(
    address: Address,
    patch: &AccountPatch,
) -> Vec<(EffectTarget, AbsoluteValue)> {
    let mut writes = Vec::new();
    if let Some(balance) = patch.balance {
        writes.push((
            EffectTarget::AccountBalance { address },
            AbsoluteValue::U256(balance),
        ));
    }
    if let Some(nonce) = patch.nonce {
        writes.push((
            EffectTarget::AccountNonce { address },
            AbsoluteValue::U64(nonce),
        ));
    }
    if let Some(code) = &patch.code {
        writes.push((
            EffectTarget::AccountCode { address },
            AbsoluteValue::Bytes(code.clone()),
        ));
    }
    writes
}

fn input_ref<N: Network>(input: &ReactiveInput<N>, ctx: &ReactiveContext) -> InputRef {
    match input {
        ReactiveInput::Log(log) => InputRef::Log {
            chain_id: ctx.chain_id,
            block_hash: log
                .block_hash
                .or(ctx.block.as_ref().map(|block| block.hash))
                .unwrap_or_default(),
            transaction_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.or(ctx.log_index).unwrap_or_default(),
        },
        ReactiveInput::PendingTxHash(hash) => InputRef::PendingTx {
            chain_id: ctx.chain_id,
            hash: *hash,
        },
        ReactiveInput::PendingTx(tx) => InputRef::PendingTx {
            chain_id: ctx.chain_id,
            hash: tx.tx_hash(),
        },
        ReactiveInput::BlockHeader(header) => InputRef::Block {
            chain_id: ctx.chain_id,
            hash: header.hash(),
            number: header.number(),
        },
        ReactiveInput::FullBlock(block) => {
            let header = block.header();
            InputRef::Block {
                chain_id: ctx.chain_id,
                hash: header.hash(),
                number: header.number(),
            }
        }
    }
}

fn is_canonical_status(status: &ChainStatus) -> bool {
    matches!(
        status,
        ChainStatus::Included { .. } | ChainStatus::Safe { .. } | ChainStatus::Finalized { .. }
    )
}

/// Adapter that wraps a legacy [`EventDecoder`] as a log-only reactive handler.
pub struct EventDecoderHandler {
    id: HandlerId,
    decoder: Arc<dyn EventDecoder>,
    interest: LogInterest,
}

impl EventDecoderHandler {
    /// Create an adapter from a decoder and log interest.
    pub fn new(id: HandlerId, decoder: Arc<dyn EventDecoder>, interest: LogInterest) -> Self {
        Self {
            id,
            decoder,
            interest,
        }
    }
}

impl<N: Network> ReactiveHandler<N> for EventDecoderHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest<N>> {
        vec![ReactiveInterest::Logs(self.interest.clone())]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        input: &ReactiveInput<N>,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };

        Ok(HandlerOutcome {
            effects: self
                .decoder
                .decode(&log.inner, state)
                .into_iter()
                .map(ReactiveEffect::StateUpdate)
                .collect(),
            quality: StateEffectQuality::ExactFromInput,
            tags: Vec::new(),
        })
    }
}

/// Provider-agnostic subscriber interface.
pub trait EventSubscriber<N: Network = Ethereum>: Send {
    /// Register interests with the subscriber.
    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError>;

    /// Return the next input batch, or `Ok(None)` when the stream is exhausted.
    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N>;
}

/// Boxed future returned by [`EventSubscriber::next_batch`].
pub type SubscriberNextBatch<'a, N> = Pin<
    Box<dyn Future<Output = Result<Option<ReactiveInputBatch<N>>, SubscriberError>> + Send + 'a>,
>;

/// Subscriber mode requested for the Alloy subscriber.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SubscriberMode {
    /// Prefer the default compiled transport.
    ///
    /// With the default `reactive-ws` feature this resolves to pubsub/WebSocket
    /// subscriptions. Without `reactive-ws`, it resolves to polling only when
    /// the opt-in `reactive-polling` feature is enabled.
    #[default]
    Auto,
    /// Use provider pubsub streams.
    PubSub,
    /// Use polling/watch APIs. Requires the `reactive-polling` feature.
    Polling,
}

/// Subscriber configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriberConfig {
    /// Hydrate pending transaction hashes into full bodies when possible.
    pub hydrate_pending_transactions: bool,
    /// Maximum records to emit per batch.
    pub max_batch_size: usize,
    /// Reconnect policy for WebSocket/pubsub streams.
    pub reconnect: SubscriberReconnectConfig,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            hydrate_pending_transactions: false,
            max_batch_size: 1024,
            reconnect: SubscriberReconnectConfig::default(),
        }
    }
}

/// WebSocket/pubsub reconnect policy.
///
/// Reconnects are applied after an established subscription stream terminates.
/// Initial subscription failures are still returned immediately so deployment
/// mistakes, unsupported transports, and bad endpoints fail fast.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriberReconnectConfig {
    /// Whether pubsub streams should be recreated after termination.
    pub enabled: bool,
    /// Delay before the first reconnect attempt.
    pub initial_delay: Duration,
    /// Delay before the second reconnect attempt. Later retries double this
    /// delay up to [`Self::max_delay`].
    pub retry_delay: Duration,
    /// Maximum delay between reconnect attempts.
    pub max_delay: Duration,
    /// Maximum reconnect attempts per terminated stream. `None` retries forever.
    pub max_attempts: Option<usize>,
    /// Number of recently emitted canonical input refs remembered to suppress
    /// duplicates across reconnect backfill and subscription replay.
    pub dedupe_window: usize,
}

impl Default for SubscriberReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: Duration::ZERO,
            retry_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            max_attempts: Some(3),
            dedupe_window: 4096,
        }
    }
}

/// Alloy-backed event subscriber.
///
/// The default transport slice drives Alloy pubsub subscriptions for logs,
/// block headers, and pending transaction hashes. The HTTP polling `watch_*`
/// transport remains available behind the opt-in `reactive-polling` feature.
/// Pubsub streams reconnect automatically after termination, and log
/// subscriptions are backfilled from the last seen block. Full pending
/// transaction hydration, full block bodies, and arbitrary historical backfill
/// remain explicit follow-up work.
/// With no registered interests, [`EventSubscriber::next_batch`] returns
/// `Ok(None)`.
pub struct AlloySubscriber<P, N: Network = Ethereum> {
    provider: P,
    mode: SubscriberMode,
    config: SubscriberConfig,
    interests: Vec<ReactiveInterest<N>>,
    state: AlloySubscriberState<N>,
    pending_records: VecDeque<ReactiveInputRecord<N>>,
    last_seen_log_blocks: HashMap<usize, u64>,
    recent_input_refs: VecDeque<InputRef>,
    recent_input_ref_set: HashSet<InputRef>,
    _network: PhantomData<N>,
}

/// Best-effort installation of rustls' `ring` crypto provider as the process
/// default, so an `wss://` TLS handshake under `reactive-ws` does not panic with
/// "no process-level CryptoProvider available". Runs at most once and ignores the
/// error if a default provider is already installed (the host app may have set
/// its own).
#[cfg(feature = "reactive-ws")]
fn ensure_ring_crypto_provider() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

impl<P, N: Network> AlloySubscriber<P, N> {
    /// Create a new Alloy subscriber.
    pub fn new(provider: P, mode: SubscriberMode, config: SubscriberConfig) -> Self {
        #[cfg(feature = "reactive-ws")]
        ensure_ring_crypto_provider();
        Self {
            provider,
            mode,
            config,
            interests: Vec::new(),
            state: AlloySubscriberState::Uninitialized,
            pending_records: VecDeque::new(),
            last_seen_log_blocks: HashMap::new(),
            recent_input_refs: VecDeque::new(),
            recent_input_ref_set: HashSet::new(),
            _network: PhantomData,
        }
    }

    /// Borrow the provider.
    pub fn provider(&self) -> &P {
        &self.provider
    }

    /// Subscriber mode.
    pub fn mode(&self) -> SubscriberMode {
        self.mode
    }

    /// Subscriber config.
    pub fn config(&self) -> &SubscriberConfig {
        &self.config
    }

    /// Registered interests.
    pub fn registered_interests(&self) -> &[ReactiveInterest<N>] {
        &self.interests
    }

    fn drain_next_batch(&mut self) -> Option<ReactiveInputBatch<N>> {
        if self.pending_records.is_empty() {
            return None;
        }

        let len = self.config.max_batch_size.min(self.pending_records.len());
        let records = self.pending_records.drain(..len).collect();
        Some(ReactiveInputBatch::new(records))
    }

    fn reset_delivery_state(&mut self) {
        self.pending_records.clear();
        self.last_seen_log_blocks.clear();
        self.recent_input_refs.clear();
        self.recent_input_ref_set.clear();
    }
}

enum AlloySubscriberState<N: Network> {
    Uninitialized,
    Active(SubscriberStreams<N>),
    Empty,
}

struct SubscriberStreams<N: Network> {
    streams: SelectAll<BoxStream<'static, SubscriberEvent<N>>>,
}

impl<N: Network> SubscriberStreams<N> {
    fn new() -> Self {
        Self {
            streams: SelectAll::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    fn push(&mut self, stream: BoxStream<'static, SubscriberEvent<N>>) {
        self.streams.push(stream);
    }

    async fn next(&mut self) -> Option<SubscriberEvent<N>> {
        self.streams.next().await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum SubscriberTransport {
    PubSub,
    Polling,
}

#[derive(Clone, Debug)]
enum SubscriberStreamSource {
    PubSubLog { id: usize, filter: Filter },
    PubSubPendingHashes,
    PubSubBlockHeaders,
    PollingLog { filter: Filter },
    PollingPendingHashes,
}

impl SubscriberStreamSource {
    fn label(&self) -> &'static str {
        match self {
            Self::PubSubLog { .. } => "pubsub log",
            Self::PubSubPendingHashes => "pubsub pending transaction hash",
            Self::PubSubBlockHeaders => "pubsub block header",
            Self::PollingLog { .. } => "polling log",
            Self::PollingPendingHashes => "polling pending transaction hash",
        }
    }

    fn is_pubsub(&self) -> bool {
        matches!(
            self,
            Self::PubSubLog { .. } | Self::PubSubPendingHashes | Self::PubSubBlockHeaders
        )
    }
}

#[allow(dead_code)]
enum SubscriberEvent<N: Network> {
    Log { source_id: usize, log: Log },
    BackfilledLogs { source_id: usize, logs: Vec<Log> },
    Logs(Vec<Log>),
    BlockHeader(N::HeaderResponse),
    PendingHash(B256),
    PendingHashes(Vec<B256>),
    StreamTerminated(SubscriberStreamSource),
}

impl<P, N> EventSubscriber<N> for AlloySubscriber<P, N>
where
    P: Provider<N> + Send + Sync,
    N: Network + 'static,
    N::HeaderResponse: Send + 'static,
{
    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError> {
        validate_subscriber_config(&self.config)?;
        validate_supported_interests(self.mode, &self.config, interests)?;

        self.interests = interests.to_vec();
        self.reset_delivery_state();
        self.state = AlloySubscriberState::Uninitialized;
        Ok(())
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        Box::pin(async {
            if let Some(batch) = self.drain_next_batch() {
                return Ok(Some(batch));
            }

            if self.interests.is_empty() {
                return Ok(None);
            }

            if matches!(self.state, AlloySubscriberState::Uninitialized) {
                let streams = self.init_streams().await?;
                self.state = if streams.is_empty() {
                    AlloySubscriberState::Empty
                } else {
                    AlloySubscriberState::Active(streams)
                };
            }

            loop {
                let Some(event) = self.next_event().await? else {
                    return Ok(None);
                };

                self.enqueue_event(event);
                if let Some(batch) = self.drain_next_batch() {
                    return Ok(Some(batch));
                }
            }
        })
    }
}

impl<P, N> AlloySubscriber<P, N>
where
    P: Provider<N> + Send + Sync,
    N: Network + 'static,
    N::HeaderResponse: Send + 'static,
{
    async fn init_streams(&mut self) -> Result<SubscriberStreams<N>, SubscriberError> {
        let mut streams = SubscriberStreams::new();
        for source in self.stream_sources()? {
            streams.push(self.connect_source_stream(source).await?);
        }
        Ok(streams)
    }

    fn stream_sources(&self) -> Result<Vec<SubscriberStreamSource>, SubscriberError> {
        match resolve_subscriber_transport(self.mode)? {
            SubscriberTransport::PubSub => Ok(pubsub_stream_sources(&self.interests)),
            SubscriberTransport::Polling => Ok(polling_stream_sources(&self.interests)),
        }
    }

    async fn connect_source_stream(
        &mut self,
        source: SubscriberStreamSource,
    ) -> Result<BoxStream<'static, SubscriberEvent<N>>, SubscriberError> {
        match source {
            SubscriberStreamSource::PubSubLog { id, filter } => {
                self.connect_pubsub_log_stream(id, filter).await
            }
            SubscriberStreamSource::PubSubPendingHashes => {
                self.connect_pubsub_pending_hash_stream().await
            }
            SubscriberStreamSource::PubSubBlockHeaders => {
                self.connect_pubsub_block_header_stream().await
            }
            SubscriberStreamSource::PollingLog { filter } => {
                self.connect_polling_log_stream(filter).await
            }
            SubscriberStreamSource::PollingPendingHashes => {
                self.connect_polling_pending_hash_stream().await
            }
        }
    }

    async fn connect_pubsub_log_stream(
        &mut self,
        id: usize,
        filter: Filter,
    ) -> Result<BoxStream<'static, SubscriberEvent<N>>, SubscriberError> {
        #[cfg(feature = "reactive-ws")]
        {
            let source = SubscriberStreamSource::PubSubLog {
                id,
                filter: filter.clone(),
            };
            let stream = self
                .provider
                .subscribe_logs(&filter)
                .channel_size(self.config.max_batch_size.max(1))
                .await
                .map_err(provider_error)?
                .into_stream()
                .map(move |log| SubscriberEvent::Log { source_id: id, log });
            Ok(stream_with_termination(stream, source))
        }

        #[cfg(not(feature = "reactive-ws"))]
        {
            let _ = (id, filter);
            Err(SubscriberError::Unsupported(
                "AlloySubscriber pubsub mode requires the reactive-ws feature",
            ))
        }
    }

    async fn connect_pubsub_pending_hash_stream(
        &mut self,
    ) -> Result<BoxStream<'static, SubscriberEvent<N>>, SubscriberError> {
        #[cfg(feature = "reactive-ws")]
        {
            let stream = self
                .provider
                .subscribe_pending_transactions()
                .channel_size(self.config.max_batch_size.max(1))
                .await
                .map_err(provider_error)?
                .into_stream()
                .map(SubscriberEvent::PendingHash);
            Ok(stream_with_termination(
                stream,
                SubscriberStreamSource::PubSubPendingHashes,
            ))
        }

        #[cfg(not(feature = "reactive-ws"))]
        {
            Err(SubscriberError::Unsupported(
                "AlloySubscriber pubsub mode requires the reactive-ws feature",
            ))
        }
    }

    async fn connect_pubsub_block_header_stream(
        &mut self,
    ) -> Result<BoxStream<'static, SubscriberEvent<N>>, SubscriberError> {
        #[cfg(feature = "reactive-ws")]
        {
            let stream = self
                .provider
                .subscribe_blocks()
                .channel_size(self.config.max_batch_size.max(1))
                .await
                .map_err(provider_error)?
                .into_stream()
                .map(SubscriberEvent::BlockHeader);
            Ok(stream_with_termination(
                stream,
                SubscriberStreamSource::PubSubBlockHeaders,
            ))
        }

        #[cfg(not(feature = "reactive-ws"))]
        {
            Err(SubscriberError::Unsupported(
                "AlloySubscriber pubsub mode requires the reactive-ws feature",
            ))
        }
    }

    async fn connect_polling_log_stream(
        &mut self,
        filter: Filter,
    ) -> Result<BoxStream<'static, SubscriberEvent<N>>, SubscriberError> {
        #[cfg(feature = "reactive-polling")]
        {
            let source = SubscriberStreamSource::PollingLog {
                filter: filter.clone(),
            };
            let stream = self
                .provider
                .watch_logs(&filter)
                .await
                .map_err(provider_error)?
                .with_channel_size(self.config.max_batch_size.max(1))
                .into_stream()
                .map(SubscriberEvent::Logs);
            Ok(stream_with_termination(stream, source))
        }

        #[cfg(not(feature = "reactive-polling"))]
        {
            let _ = filter;
            Err(SubscriberError::Unsupported(
                "AlloySubscriber polling mode requires the reactive-polling feature",
            ))
        }
    }

    async fn connect_polling_pending_hash_stream(
        &mut self,
    ) -> Result<BoxStream<'static, SubscriberEvent<N>>, SubscriberError> {
        #[cfg(feature = "reactive-polling")]
        {
            let stream = self
                .provider
                .watch_pending_transactions()
                .await
                .map_err(provider_error)?
                .with_channel_size(self.config.max_batch_size.max(1))
                .into_stream()
                .map(SubscriberEvent::PendingHashes);
            Ok(stream_with_termination(
                stream,
                SubscriberStreamSource::PollingPendingHashes,
            ))
        }

        #[cfg(not(feature = "reactive-polling"))]
        {
            Err(SubscriberError::Unsupported(
                "AlloySubscriber polling mode requires the reactive-polling feature",
            ))
        }
    }

    async fn next_event(&mut self) -> Result<Option<SubscriberEvent<N>>, SubscriberError> {
        loop {
            let event = match &mut self.state {
                AlloySubscriberState::Active(streams) => streams.next().await,
                AlloySubscriberState::Uninitialized | AlloySubscriberState::Empty => {
                    return Ok(None);
                }
            };

            let Some(event) = event else {
                return Err(SubscriberError::Provider(
                    "Alloy subscriber streams terminated before the subscriber was stopped"
                        .to_owned(),
                ));
            };

            match event {
                SubscriberEvent::StreamTerminated(source) => {
                    if let Some(backfill_event) = self.reconnect_source_stream(source).await? {
                        return Ok(Some(backfill_event));
                    }
                }
                event => return Ok(Some(event)),
            }
        }
    }

    fn enqueue_event(&mut self, event: SubscriberEvent<N>) {
        match event {
            SubscriberEvent::Log { source_id, log } => {
                if log_matches_any_interest(&log, &self.interests) {
                    let record = log_input_record(log, InputSource::Subscription);
                    self.note_log_block(source_id, &record);
                    self.enqueue_record(record);
                }
            }
            SubscriberEvent::BackfilledLogs { source_id, logs } => {
                for log in logs {
                    if log_matches_any_interest(&log, &self.interests) {
                        let record = log_input_record(log, InputSource::Backfill);
                        self.note_log_block(source_id, &record);
                        self.enqueue_record(record);
                    }
                }
            }
            SubscriberEvent::Logs(logs) => self.pending_records.extend(
                logs.into_iter()
                    .filter(|log| log_matches_any_interest(log, &self.interests))
                    .map(|log| log_input_record(log, InputSource::Poll)),
            ),
            SubscriberEvent::BlockHeader(header) => {
                if needs_header_block_stream(&self.interests) {
                    let record = block_header_input_record::<N>(header);
                    self.enqueue_record(record);
                }
            }
            SubscriberEvent::PendingHash(hash) => {
                let record = pending_hash_input_record::<N>(hash, InputSource::Subscription);
                self.enqueue_record(record);
            }
            SubscriberEvent::PendingHashes(hashes) => self.pending_records.extend(
                hashes
                    .into_iter()
                    .map(|hash| pending_hash_input_record::<N>(hash, InputSource::Poll)),
            ),
            SubscriberEvent::StreamTerminated(_) => {}
        }
    }

    async fn reconnect_source_stream(
        &mut self,
        source: SubscriberStreamSource,
    ) -> Result<Option<SubscriberEvent<N>>, SubscriberError> {
        if !source.is_pubsub() {
            return Err(stream_terminated_error(&source));
        }

        if !self.config.reconnect.enabled {
            return Err(SubscriberError::Provider(format!(
                "Alloy subscriber {} stream terminated and reconnect is disabled",
                source.label()
            )));
        }

        let mut attempts = 0usize;
        let mut delay = self.config.reconnect.initial_delay;
        let mut retry_delay = self.config.reconnect.retry_delay;

        loop {
            attempts = attempts.saturating_add(1);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            match self.reconnect_source_once(source.clone()).await {
                Ok(backfill_event) => return Ok(backfill_event),
                Err(error) if reconnect_attempts_exhausted(attempts, &self.config.reconnect) => {
                    return Err(SubscriberError::Provider(format!(
                        "Alloy subscriber {} stream terminated and reconnect failed after {attempts} attempt(s): {error}",
                        source.label()
                    )));
                }
                Err(error) => {
                    tracing::warn!(
                        stream = source.label(),
                        attempts,
                        error = %error,
                        "Alloy subscriber reconnect attempt failed"
                    );
                    delay = retry_delay;
                    retry_delay =
                        next_reconnect_delay(retry_delay, self.config.reconnect.max_delay);
                }
            }
        }
    }

    async fn reconnect_source_once(
        &mut self,
        source: SubscriberStreamSource,
    ) -> Result<Option<SubscriberEvent<N>>, SubscriberError> {
        let stream = self.connect_source_stream(source.clone()).await?;
        let backfill_event = self.backfill_reconnected_source(&source).await?;

        match &mut self.state {
            AlloySubscriberState::Active(streams) => streams.push(stream),
            AlloySubscriberState::Uninitialized | AlloySubscriberState::Empty => {
                return Err(SubscriberError::Provider(
                    "Alloy subscriber state changed before reconnect completed".to_owned(),
                ));
            }
        }

        Ok(backfill_event)
    }

    async fn backfill_reconnected_source(
        &mut self,
        source: &SubscriberStreamSource,
    ) -> Result<Option<SubscriberEvent<N>>, SubscriberError> {
        let SubscriberStreamSource::PubSubLog { id, filter } = source else {
            return Ok(None);
        };
        let Some(from_block) = self.last_seen_log_blocks.get(id).copied() else {
            return Ok(None);
        };

        let latest = self
            .provider
            .get_block_number()
            .await
            .map_err(provider_error)?;
        if latest < from_block {
            return Ok(None);
        }

        let logs = self
            .provider
            .get_logs(&filter.clone().from_block(from_block).to_block(latest))
            .await
            .map_err(provider_error)?;
        Ok(Some(SubscriberEvent::BackfilledLogs {
            source_id: *id,
            logs,
        }))
    }

    fn note_log_block(&mut self, source_id: usize, record: &ReactiveInputRecord<N>) {
        if let Some(block) = record.context.block.as_ref() {
            self.last_seen_log_blocks.insert(source_id, block.number);
        }
    }

    fn enqueue_record(&mut self, record: ReactiveInputRecord<N>) {
        if self.should_skip_recent_duplicate(&record) {
            return;
        }
        self.remember_record(&record);
        self.pending_records.push_back(record);
    }

    fn should_skip_recent_duplicate(&self, record: &ReactiveInputRecord<N>) -> bool {
        if !should_dedupe_record(record) {
            return false;
        }
        self.recent_input_ref_set.contains(&record.input_ref())
    }

    fn remember_record(&mut self, record: &ReactiveInputRecord<N>) {
        if !should_dedupe_record(record) || self.config.reconnect.dedupe_window == 0 {
            return;
        }

        let input_ref = record.input_ref();
        if !self.recent_input_ref_set.insert(input_ref) {
            return;
        }
        self.recent_input_refs.push_back(input_ref);

        while self.recent_input_refs.len() > self.config.reconnect.dedupe_window {
            if let Some(evicted) = self.recent_input_refs.pop_front() {
                self.recent_input_ref_set.remove(&evicted);
            }
        }
    }
}

fn stream_with_termination<N, S>(
    stream: S,
    source: SubscriberStreamSource,
) -> BoxStream<'static, SubscriberEvent<N>>
where
    N: Network + 'static,
    S: futures::Stream<Item = SubscriberEvent<N>> + Send + 'static,
{
    stream
        .chain(stream::once(async move {
            SubscriberEvent::StreamTerminated(source)
        }))
        .boxed()
}

fn pubsub_stream_sources<N: Network>(
    interests: &[ReactiveInterest<N>],
) -> Vec<SubscriberStreamSource> {
    let mut sources = Vec::new();

    for (id, filter) in log_filters(interests).into_iter().enumerate() {
        sources.push(SubscriberStreamSource::PubSubLog { id, filter });
    }

    if needs_pending_hash_stream(interests) {
        sources.push(SubscriberStreamSource::PubSubPendingHashes);
    }

    if needs_header_block_stream(interests) {
        sources.push(SubscriberStreamSource::PubSubBlockHeaders);
    }

    sources
}

fn polling_stream_sources<N: Network>(
    interests: &[ReactiveInterest<N>],
) -> Vec<SubscriberStreamSource> {
    let mut sources = Vec::new();

    for filter in log_filters(interests) {
        sources.push(SubscriberStreamSource::PollingLog { filter });
    }

    if needs_pending_hash_stream(interests) {
        sources.push(SubscriberStreamSource::PollingPendingHashes);
    }

    sources
}

fn stream_terminated_error(source: &SubscriberStreamSource) -> SubscriberError {
    SubscriberError::Provider(format!(
        "Alloy subscriber {} stream terminated before the subscriber was stopped",
        source.label()
    ))
}

fn reconnect_attempts_exhausted(attempts: usize, config: &SubscriberReconnectConfig) -> bool {
    config
        .max_attempts
        .is_some_and(|max_attempts| attempts >= max_attempts)
}

fn next_reconnect_delay(current: Duration, max: Duration) -> Duration {
    if current.is_zero() {
        return current;
    }
    current.checked_mul(2).unwrap_or(max).min(max)
}

fn should_dedupe_record<N: Network>(record: &ReactiveInputRecord<N>) -> bool {
    match &record.input {
        ReactiveInput::Log(log) => {
            is_canonical_status(&record.context.chain_status) && !log.removed
        }
        ReactiveInput::BlockHeader(_) | ReactiveInput::PendingTxHash(_) => true,
        ReactiveInput::FullBlock(_) | ReactiveInput::PendingTx(_) => false,
    }
}

#[cfg(test)]
mod subscriber_helper_tests {
    use super::*;
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;

    fn rpc_log(removed: bool) -> Log {
        Log {
            inner: alloy_primitives::Log::new_unchecked(
                Address::repeat_byte(0x42),
                vec![B256::repeat_byte(0x01)],
                Bytes::new(),
            ),
            block_hash: Some(B256::repeat_byte(0x02)),
            block_number: Some(7),
            block_timestamp: Some(1_700_000_000),
            transaction_hash: Some(B256::repeat_byte(0x03)),
            transaction_index: Some(4),
            log_index: Some(5),
            removed,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_with_termination_yields_terminal_source_marker() {
        let mut stream = stream_with_termination::<Ethereum, _>(
            stream::iter([SubscriberEvent::<Ethereum>::PendingHash(B256::repeat_byte(
                0xaa,
            ))]),
            SubscriberStreamSource::PubSubPendingHashes,
        );

        assert!(matches!(
            stream.next().await,
            Some(SubscriberEvent::PendingHash(hash)) if hash == B256::repeat_byte(0xaa)
        ));
        assert!(matches!(
            stream.next().await,
            Some(SubscriberEvent::StreamTerminated(source)) if source.is_pubsub()
        ));
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn reconnect_delay_doubles_until_capped() {
        assert_eq!(
            next_reconnect_delay(Duration::from_millis(250), Duration::from_secs(1)),
            Duration::from_millis(500)
        );
        assert_eq!(
            next_reconnect_delay(Duration::from_millis(750), Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_reconnect_delay(Duration::ZERO, Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn canonical_logs_are_deduped_but_removed_logs_are_not() {
        let included = log_input_record::<Ethereum>(rpc_log(false), InputSource::Subscription);
        let removed = log_input_record::<Ethereum>(rpc_log(true), InputSource::Subscription);

        assert!(should_dedupe_record(&included));
        assert!(!should_dedupe_record(&removed));
    }

    #[test]
    fn pubsub_sources_assign_stable_log_ids_before_shared_streams() {
        let sources = pubsub_stream_sources::<Ethereum>(&[
            ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(Address::repeat_byte(0x01)),
                local_matcher: None,
                route_key: None,
            }),
            ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(Address::repeat_byte(0x02)),
                local_matcher: None,
                route_key: None,
            }),
            ReactiveInterest::PendingTransactions(PendingTxInterest::default()),
        ]);

        assert!(matches!(
            &sources[0],
            SubscriberStreamSource::PubSubLog { id: 0, .. }
        ));
        assert!(matches!(
            sources[1],
            SubscriberStreamSource::PubSubPendingHashes
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(feature = "reactive-ws")]
    async fn pubsub_stream_termination_attempts_reconnect_before_error() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig {
                reconnect: SubscriberReconnectConfig {
                    initial_delay: Duration::ZERO,
                    retry_delay: Duration::ZERO,
                    max_delay: Duration::ZERO,
                    max_attempts: Some(1),
                    ..SubscriberReconnectConfig::default()
                },
                ..SubscriberConfig::default()
            },
        );
        subscriber.interests = vec![ReactiveInterest::PendingTransactions(
            PendingTxInterest::default(),
        )];

        let mut streams = SubscriberStreams::new();
        streams.push(
            stream::once(async {
                SubscriberEvent::<Ethereum>::StreamTerminated(
                    SubscriberStreamSource::PubSubPendingHashes,
                )
            })
            .boxed(),
        );
        subscriber.state = AlloySubscriberState::Active(streams);

        let result = subscriber.next_batch().await;
        assert!(
            matches!(result, Err(SubscriberError::Provider(ref message)) if message.contains("reconnect failed after 1 attempt")),
            "terminated pubsub streams should attempt reconnect before surfacing failure: {result:?}"
        );
    }

    #[test]
    fn backfilled_logs_skip_recent_subscription_duplicates() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber.interests = vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(Address::repeat_byte(0x42))
                .event_signature(B256::repeat_byte(0x01)),
            local_matcher: None,
            route_key: None,
        })];

        let log = rpc_log(false);
        subscriber.enqueue_event(SubscriberEvent::Log {
            source_id: 0,
            log: log.clone(),
        });
        subscriber.enqueue_event(SubscriberEvent::BackfilledLogs {
            source_id: 0,
            logs: vec![log],
        });

        assert_eq!(subscriber.pending_records.len(), 1);
        assert_eq!(subscriber.last_seen_log_blocks.get(&0), Some(&7));
        assert_eq!(
            subscriber.pending_records[0].context.source,
            InputSource::Subscription
        );
    }

    #[test]
    fn backfilled_logs_surface_with_backfill_source() {
        // A backfilled log with no prior subscription duplicate is delivered as
        // an `InputSource::Backfill` record (the positive side of the dedup test,
        // pinning the README's "marking recovered records as InputSource::Backfill"
        // claim — the only place that source is produced).
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber.interests = vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(Address::repeat_byte(0x42))
                .event_signature(B256::repeat_byte(0x01)),
            local_matcher: None,
            route_key: None,
        })];

        subscriber.enqueue_event(SubscriberEvent::BackfilledLogs {
            source_id: 0,
            logs: vec![rpc_log(false)],
        });

        assert_eq!(subscriber.pending_records.len(), 1);
        assert_eq!(
            subscriber.pending_records[0].context.source,
            InputSource::Backfill
        );
        assert_eq!(subscriber.last_seen_log_blocks.get(&0), Some(&7));
    }
}

fn resolve_subscriber_transport(
    mode: SubscriberMode,
) -> Result<SubscriberTransport, SubscriberError> {
    match mode {
        SubscriberMode::PubSub => {
            #[cfg(feature = "reactive-ws")]
            {
                Ok(SubscriberTransport::PubSub)
            }
            #[cfg(not(feature = "reactive-ws"))]
            {
                Err(SubscriberError::Unsupported(
                    "AlloySubscriber pubsub mode requires the reactive-ws feature",
                ))
            }
        }
        SubscriberMode::Polling => {
            #[cfg(feature = "reactive-polling")]
            {
                Ok(SubscriberTransport::Polling)
            }
            #[cfg(not(feature = "reactive-polling"))]
            {
                Err(SubscriberError::Unsupported(
                    "AlloySubscriber polling mode requires the reactive-polling feature",
                ))
            }
        }
        SubscriberMode::Auto => resolve_auto_subscriber_transport(),
    }
}

fn resolve_auto_subscriber_transport() -> Result<SubscriberTransport, SubscriberError> {
    #[cfg(feature = "reactive-ws")]
    {
        Ok(SubscriberTransport::PubSub)
    }

    #[cfg(all(not(feature = "reactive-ws"), feature = "reactive-polling"))]
    {
        Ok(SubscriberTransport::Polling)
    }

    #[cfg(not(any(feature = "reactive-ws", feature = "reactive-polling")))]
    {
        Err(SubscriberError::Unsupported(
            "AlloySubscriber requires either reactive-ws or reactive-polling",
        ))
    }
}

fn validate_subscriber_config(config: &SubscriberConfig) -> Result<(), SubscriberError> {
    if config.max_batch_size == 0 {
        return Err(SubscriberError::InvalidConfig(
            "SubscriberConfig::max_batch_size must be greater than zero",
        ));
    }
    if config.reconnect.enabled {
        if config.reconnect.retry_delay > config.reconnect.max_delay {
            return Err(SubscriberError::InvalidConfig(
                "SubscriberReconnectConfig::retry_delay must be less than or equal to max_delay",
            ));
        }
        if matches!(config.reconnect.max_attempts, Some(0)) {
            return Err(SubscriberError::InvalidConfig(
                "SubscriberReconnectConfig::max_attempts must be greater than zero when set",
            ));
        }
    }
    Ok(())
}

fn validate_supported_interests<N: Network>(
    mode: SubscriberMode,
    config: &SubscriberConfig,
    interests: &[ReactiveInterest<N>],
) -> Result<(), SubscriberError> {
    let transport = resolve_subscriber_transport(mode)?;

    for interest in interests {
        match interest {
            ReactiveInterest::Logs(_) => {}
            ReactiveInterest::PendingTransactions(interest)
                if !config.hydrate_pending_transactions && interest.matches_hash_only() => {}
            ReactiveInterest::PendingTransactions(_) => {
                return Err(SubscriberError::Unsupported(
                    "AlloySubscriber currently supports pending transaction hash interests only (full pending-tx hydration is unimplemented)",
                ));
            }
            ReactiveInterest::Blocks(interest) => match (transport, interest.mode) {
                (SubscriberTransport::PubSub, BlockInterestMode::Header) => {}
                (_, BlockInterestMode::FullBlock) => {
                    return Err(SubscriberError::Unsupported(
                        "AlloySubscriber full block streams are not implemented in this transport slice",
                    ));
                }
                (SubscriberTransport::Polling, BlockInterestMode::Header) => {
                    return Err(SubscriberError::Unsupported(
                        "AlloySubscriber polling block streams are not implemented in this transport slice",
                    ));
                }
            },
        }
    }

    Ok(())
}

fn log_filters<N: Network>(interests: &[ReactiveInterest<N>]) -> Vec<Filter> {
    let mut filters = Vec::new();
    for interest in interests {
        if let ReactiveInterest::Logs(interest) = interest {
            merge_log_subscription_filter(&mut filters, &interest.provider_filter);
        }
    }
    filters
}

fn needs_header_block_stream<N: Network>(interests: &[ReactiveInterest<N>]) -> bool {
    interests.iter().any(|interest| {
        matches!(
            interest,
            ReactiveInterest::Blocks(BlockInterest {
                mode: BlockInterestMode::Header,
            })
        )
    })
}

fn needs_pending_hash_stream<N: Network>(interests: &[ReactiveInterest<N>]) -> bool {
    interests.iter().any(|interest| {
        matches!(
            interest,
            ReactiveInterest::PendingTransactions(interest) if interest.matches_hash_only()
        )
    })
}

fn log_matches_any_interest<N: Network>(log: &Log, interests: &[ReactiveInterest<N>]) -> bool {
    interests.iter().any(|interest| {
        matches!(
            interest,
            ReactiveInterest::Logs(interest) if interest.matches(log)
        )
    })
}

fn log_input_record<N: Network>(log: Log, source: InputSource) -> ReactiveInputRecord<N> {
    let context = log_reactive_context(&log);
    ReactiveInputRecord::new(
        ReactiveInput::Log(log),
        ReactiveContext { source, ..context },
    )
}

fn log_reactive_context(log: &Log) -> ReactiveContext {
    let block = match (log.block_hash, log.block_number) {
        (Some(hash), Some(number)) => Some(BlockRef {
            number,
            hash,
            parent_hash: None,
            timestamp: log.block_timestamp,
        }),
        _ => None,
    };

    let chain_status = match (&block, log.removed) {
        (Some(block), true) => ChainStatus::Reorged {
            dropped_from: block.clone(),
        },
        (Some(block), false) => ChainStatus::Included {
            block: block.clone(),
            confirmations: 0,
        },
        (None, _) => ChainStatus::Pending,
    };

    ReactiveContext {
        chain_id: None,
        source: InputSource::Poll,
        chain_status,
        block,
        transaction_index: log.transaction_index,
        log_index: log.log_index,
    }
}

fn block_header_input_record<N>(header: N::HeaderResponse) -> ReactiveInputRecord<N>
where
    N: Network,
{
    let block = BlockRef {
        number: header.number(),
        hash: HeaderResponseTrait::hash(&header),
        parent_hash: Some(header.parent_hash()),
        timestamp: Some(header.timestamp()),
    };
    ReactiveInputRecord::new(
        ReactiveInput::BlockHeader(header),
        ReactiveContext {
            chain_id: None,
            source: InputSource::Subscription,
            chain_status: ChainStatus::Included {
                block: block.clone(),
                confirmations: 0,
            },
            block: Some(block),
            transaction_index: None,
            log_index: None,
        },
    )
}

fn pending_hash_input_record<N: Network>(
    hash: B256,
    source: InputSource,
) -> ReactiveInputRecord<N> {
    ReactiveInputRecord::new(
        ReactiveInput::PendingTxHash(hash),
        ReactiveContext {
            chain_id: None,
            source,
            chain_status: ChainStatus::Pending,
            block: None,
            transaction_index: None,
            log_index: None,
        },
    )
}

fn provider_error(error: impl fmt::Display) -> SubscriberError {
    SubscriberError::Provider(error.to_string())
}

/// Subscriber error.
#[derive(Debug, thiserror::Error)]
pub enum SubscriberError {
    /// Invalid subscriber configuration.
    #[error("{0}")]
    InvalidConfig(&'static str),
    /// Requested subscriber behavior is not implemented.
    #[error("{0}")]
    Unsupported(&'static str),
    /// Provider or transport error.
    #[error("provider error: {0}")]
    Provider(String),
}
