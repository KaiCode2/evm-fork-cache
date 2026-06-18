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
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
};

use alloy_consensus::{BlockHeader as _, Transaction as _};
use alloy_network::{
    Ethereum, Network,
    primitives::{
        BlockResponse as _, HeaderResponse as _, TransactionResponse as TransactionResponseTrait,
    },
};
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_eth::{Filter, FilterSet, Log};

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
    /// Hook backpressure policy for future async dispatchers.
    pub hook_backpressure: HookBackpressure,
    /// Reorg journal depth reserved for future rollback support.
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

/// Resync reporting scaffold.
#[derive(Clone, Debug, Default)]
pub struct ResyncReport {
    /// Requests surfaced by handlers.
    pub requested: Vec<ResyncRequest>,
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

/// Reorg reporting scaffold.
#[derive(Clone, Debug)]
pub struct ReorgReport<N: Network = Ethereum> {
    /// Dropped block, when known.
    pub dropped: Option<BlockRef>,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Error report scaffold.
#[derive(Clone, Debug)]
pub struct ReactiveErrorReport<N: Network = Ethereum> {
    /// Input associated with the error, when known.
    pub input_ref: Option<InputRef>,
    /// Error message.
    pub message: String,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Batch report returned by [`ReactiveRuntime::ingest_batch`].
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
                batch_report.applied.push(applied);
            }
        }

        self.dispatch_reports(&reports_to_dispatch);
        batch_report.reports = reports_to_dispatch;
        let _ = &self.config;
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

/// Subscriber mode requested for the Alloy scaffold.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SubscriberMode {
    /// Use provider pubsub streams when available.
    #[default]
    PubSub,
    /// Use polling/watch APIs.
    Polling,
}

/// Subscriber configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriberConfig {
    /// Hydrate pending transaction hashes into full bodies when possible.
    pub hydrate_pending_transactions: bool,
    /// Maximum records to emit per batch.
    pub max_batch_size: usize,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            hydrate_pending_transactions: false,
            max_batch_size: 1024,
        }
    }
}

/// Documented Alloy subscriber scaffold.
///
/// This type records interests and configuration but does not yet drive live
/// provider streams. Use it as the integration point for a future
/// `reactive-alloy` transport implementation.
pub struct AlloySubscriber<P, N: Network = Ethereum> {
    provider: P,
    mode: SubscriberMode,
    config: SubscriberConfig,
    interests: Vec<ReactiveInterest<N>>,
    _network: PhantomData<N>,
}

impl<P, N: Network> AlloySubscriber<P, N> {
    /// Create a new Alloy subscriber scaffold.
    pub fn new(provider: P, mode: SubscriberMode, config: SubscriberConfig) -> Self {
        Self {
            provider,
            mode,
            config,
            interests: Vec::new(),
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
}

impl<P: Send, N: Network> EventSubscriber<N> for AlloySubscriber<P, N> {
    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError> {
        self.interests = interests.to_vec();
        Ok(())
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        Box::pin(async {
            Err(SubscriberError::Unsupported(
                "AlloySubscriber is a scaffold; live stream driving is not implemented in this feature slice",
            ))
        })
    }
}

/// Subscriber error.
#[derive(Debug, thiserror::Error)]
pub enum SubscriberError {
    /// Requested subscriber behavior is not implemented.
    #[error("{0}")]
    Unsupported(&'static str),
    /// Provider or transport error.
    #[error("provider error: {0}")]
    Provider(String),
}
