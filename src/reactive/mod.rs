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
    num::NonZeroU64,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
#[cfg(any(feature = "reactive-ws", feature = "reactive-polling", test))]
use futures::{StreamExt, stream};
use futures::{future::poll_fn, stream::BoxStream};

use crate::{
    cache::{AccountProof, BlockStateDiff, EvmCache},
    errors::{BlockContextError, StorageFetchResult},
    events::{EventDecoder, StateView},
    freshness::FreshnessRegistry,
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

/// How a tracked account is kept live by the per-block root gate (Phase-8 step 4).
///
/// The `storageHash` root gate behaves *oppositely* for two contract shapes, so
/// liveness strategy is per-contract:
///
/// - A sparse-interest contract (a few balance slots, e.g. WETH) has its root
///   churn on nearly every block, so the root is a noisy gate — [`Slots`] opts
///   out. Its enumerated slots stay fresh via decoders + cadence reconcile.
/// - A whole-economic-state contract (e.g. a Uniswap-V2 pool) has
///   `root_moved ≈ my_state_changed`, so [`WholeAccount`] opts in: probe the root
///   each canonical block; a move a decoder did not cover is a coverage gap.
///
/// A false-positive resync is never *incorrect* — it costs one batched read — so
/// the policy is a **pure cost knob**, not a correctness lever.
///
/// [`Slots`]: TrackingPolicy::Slots
/// [`WholeAccount`]: TrackingPolicy::WholeAccount
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TrackingPolicy {
    /// Sparse interest (e.g. WETH: a few balance slots). The root churns on
    /// nearly every block, so it is a noisy gate — this policy is **never**
    /// root-gated (spec Decision 3). Keep the enumerated slots fresh via decoders
    /// and cadence reconcile.
    Slots {
        /// The enumerated storage slots of interest.
        slots: Vec<U256>,
    },
    /// Whole economic state (e.g. a V2 pool). `root_moved ≈ my_state_changed`, so
    /// the root is a tight, cheap gate: probe each canonical block; on a move no
    /// decoder covered, emit a [`ReactiveReport::CoverageGap`] and schedule a
    /// [`ResyncReason::RootMoved`] repair.
    WholeAccount,
    /// Balance / nonce / code-hash only — resolved from the same `get_proof`
    /// response's account fields; no storage interest. Native balance/nonce
    /// changes do **not** move the storage root, so this policy compares the
    /// account fields directly across blocks rather than root-gating.
    Scalars,
}

/// How often the reactive root gate probes tracked accounts
/// ([`TrackingPolicy::WholeAccount`] / [`TrackingPolicy::Scalars`]; the
/// `Scalars` account-fields comparison rides the same firing).
///
/// `eth_getProof` is the slowest read this crate issues, so per-block probing
/// is never the default. Skipping blocks is safe by construction: the gate
/// diffs `root_now` against its **persisted baseline**, never
/// block-over-block, so a move in any skipped block is still visible at the
/// next firing — cadence trades detection lag (at most `n − 1` blocks) for
/// cost, never eventual detection. The decoder-touched set accumulates across
/// skipped blocks and drains per firing, so a covered write in a skipped
/// block never false-positives as a [`ReactiveReport::CoverageGap`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootGateCadence {
    /// Probe at most once every `n` canonical blocks (the first canonical
    /// block ever seen always fires, so baseline adoption does not wait a
    /// full window). `EveryNBlocks(1)` is per-block probing.
    EveryNBlocks(NonZeroU64),
    /// Root gate off: coverage gaps surface only via decoders + freshness.
    Disabled,
}

impl RootGateCadence {
    /// Probe at most once every `n` canonical blocks, clamping `0` to `1`.
    pub fn every_n_blocks(n: u64) -> Self {
        Self::EveryNBlocks(NonZeroU64::new(n.max(1)).expect("clamped to at least 1"))
    }
}

impl Default for RootGateCadence {
    /// Every 16 canonical blocks — ~3.2 min worst-case detection lag on
    /// mainnet for a 16× probe-cost cut. Fast-block chains should *raise*
    /// `n`, not lower it.
    fn default() -> Self {
        Self::every_n_blocks(16)
    }
}

/// Per-account baseline held by the root gate: the last observed on-chain root
/// and account fields, plus the block they were observed at.
///
/// The gate diffs the on-chain root **across time** (never local-vs-chain, per
/// spec §6): it persists the *observed* root as a baseline and compares
/// `root_now` to it. This is a currency gate, not a completeness gate.
#[derive(Clone, Debug)]
struct TrackedRoot {
    last_root: B256,
    last_block: u64,
    balance: U256,
    nonce: u64,
    code_hash: B256,
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
#[non_exhaustive]
pub enum ResyncReason {
    /// Handler requested repair.
    HandlerRequested,
    /// State effect could not be applied completely.
    SkippedStateEffect,
    /// A missed block range was detected; caller-scheduled repair.
    ///
    /// The runtime does not fabricate a targetless [`ResyncRequest`] for a missed
    /// range (there are no known targets to resync). This reason is provided so a
    /// caller building its own repair in response to a
    /// [`ReactiveReport::MissedBlockRange`] can attribute it.
    MissedBlockRange,
    /// A tracked account's storage root moved with no covering decoder.
    ///
    /// Emitted by the per-block root gate (Phase-8 step 4). A
    /// [`WholeAccount`](TrackingPolicy::WholeAccount)-tracked account's
    /// `storageHash` moved between the adopted baseline and the current canonical
    /// block, yet no decoder wrote that account during the block — a coverage gap.
    /// The gate schedules a resync with this reason to re-read the account
    /// authoritatively and self-heal the blind spot. Also used for the
    /// [`Scalars`](TrackingPolicy::Scalars) account-field freshness path.
    RootMoved,
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
    /// are journaled for rollback. This is **load-bearing** for reorg recovery:
    /// only blocks still resident in the journal can be recovered. A reorg deeper
    /// than `journal_depth` recovers the blocks still in the journal and leaves
    /// the aged-out blocks' effects in place — they are **neither rolled back nor
    /// purged**, so the freshness/validation loop is the only backstop for that
    /// span. `0` disables journaling entirely: no reorg is rolled back or purged.
    ///
    /// Set `journal_depth` to exceed the deepest reorg you intend to recover
    /// precisely. When a reorg references a block that is no longer in the journal,
    /// the runtime emits a `tracing::warn!` so the under-recovery is observable
    /// rather than silent.
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

/// Queryable coarse health of the reactive cache.
///
/// The runtime starts [`Healthy`](CacheHealth::Healthy) and transitions to a
/// degraded or unhealthy state when it detects that its recovery guarantees no
/// longer hold (for example a reorg that runs deeper than the journal, so some
/// dropped effects are neither rolled back nor purged). Later waves report
/// missed-range and coverage-gap conditions into the same state machine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheHealth {
    /// All recovery guarantees hold; the cache is fully self-consistent.
    #[default]
    Healthy,
    /// A recoverable inconsistency was detected (for example under-recovered
    /// reorg effects); `since_block` records the block that triggered the
    /// transition.
    Degraded {
        /// Block number at which the degradation was first observed.
        since_block: u64,
    },
    /// A more serious inconsistency was detected; `since_block` records the
    /// block that triggered the transition.
    Unhealthy {
        /// Block number at which the unhealthy condition was first observed.
        since_block: u64,
    },
}

/// Point-in-time copy of the reactive runtime's observability counters.
///
/// Returned by [`ReactiveRuntime::metrics`]. Each field is a monotonically
/// increasing count over the lifetime of the runtime. Counters wired by later
/// waves (missed-range detection, storage-hash coverage gaps, stale-verdict
/// tracking) remain zero until those waves land.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CacheMetricsSnapshot {
    /// Reorgs that ran deeper than the journal, so aged-out effects could not be
    /// rolled back or purged.
    pub deep_reorgs: u64,
    /// Reorgs for which a [`ReorgReport`] recovery ran (including deep reorgs).
    pub reorgs_recovered: u64,
    /// Storage resync targets considered by the resync execution pass.
    pub resync_requests: u64,
    /// Storage resync targets that could not be fetched or applied.
    pub resync_failures: u64,
    /// Ranges of blocks the runtime detected it did not observe (reserved).
    pub missed_ranges: u64,
    /// Storage-hash coverage gaps detected (reserved).
    pub coverage_gaps: u64,
    /// Pending-source inputs that attempted a canonical cache effect.
    pub pending_contamination: u64,
    /// Verdicts served past their freshness horizon (reserved).
    pub stale_verdicts: u64,
}

/// Internal atomic-backed counters mirrored by [`CacheMetricsSnapshot`].
///
/// Fields are [`AtomicU64`] so counters can be incremented behind a shared
/// reference; [`ReactiveRuntime::metrics`] loads each with [`Ordering::Relaxed`]
/// into a plain [`CacheMetricsSnapshot`].
#[derive(Debug, Default)]
struct CacheMetrics {
    deep_reorgs: AtomicU64,
    reorgs_recovered: AtomicU64,
    resync_requests: AtomicU64,
    resync_failures: AtomicU64,
    missed_ranges: AtomicU64,
    coverage_gaps: AtomicU64,
    pending_contamination: AtomicU64,
    stale_verdicts: AtomicU64,
}

impl CacheMetrics {
    fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            deep_reorgs: self.deep_reorgs.load(Ordering::Relaxed),
            reorgs_recovered: self.reorgs_recovered.load(Ordering::Relaxed),
            resync_requests: self.resync_requests.load(Ordering::Relaxed),
            resync_failures: self.resync_failures.load(Ordering::Relaxed),
            missed_ranges: self.missed_ranges.load(Ordering::Relaxed),
            coverage_gaps: self.coverage_gaps.load(Ordering::Relaxed),
            pending_contamination: self.pending_contamination.load(Ordering::Relaxed),
            stale_verdicts: self.stale_verdicts.load(Ordering::Relaxed),
        }
    }
}

/// Runtime report.
#[derive(Clone, Debug)]
#[non_exhaustive]
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
    /// A forward gap in the canonical block sequence was detected: blocks between
    /// the last-seen head and an arriving block were never observed.
    MissedBlockRange(MissedRangeReport<N>),
    /// Cache health transitioned between states.
    Health(HealthReport<N>),
    /// A tracked account's storage root moved with no covering decoder — a
    /// coverage gap the per-block root gate detected (Phase-8 step 4).
    CoverageGap(CoverageGapReport<N>),
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
#[non_exhaustive]
pub enum ResyncFailureKind {
    /// A storage target could not be fetched because no storage batch fetcher is configured.
    MissingStorageFetcher,
    /// The storage batch fetcher returned an error for the requested slot.
    StorageFetchFailed,
    /// The storage batch fetcher did not return a result for the requested slot.
    StorageFetchOmitted,
    /// An account target could not be fetched because no account proof fetcher is configured.
    MissingAccountFetcher,
    /// The account proof fetcher returned an error for the requested address.
    AccountFetchFailed,
    /// The account proof fetcher did not return a result for the requested address.
    AccountFetchOmitted,
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
///
/// Recovery only covers blocks still resident in the journal. If a reorg runs
/// deeper than [`ReactiveConfig::journal_depth`], the aged-out blocks do not
/// appear here and their effects are neither rolled back nor purged (the runtime
/// logs a `tracing::warn!` in that case); the freshness/validation loop is the
/// backstop for that span.
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

/// Report of a forward gap in the canonical block sequence: an arriving block
/// whose number is more than one past the last-seen head, so the blocks in
/// between were never observed (for example during a subscription disconnect).
///
/// The arriving block is still accepted and applied — the chain extends — so this
/// report only makes the skipped span observable; it does not drop the block. The
/// span `from..=to` is inclusive of both endpoints.
#[derive(Clone, Debug)]
pub struct MissedRangeReport<N: Network = Ethereum> {
    /// First skipped block (`last-seen block number + 1`).
    pub from: u64,
    /// Last skipped block (`arriving block number - 1`).
    pub to: u64,
    /// The arriving block's number.
    pub block: u64,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Report of a [`CacheHealth`] transition, emitted into the ingest cycle that
/// caused it and delivered to hooks through the normal dispatch path.
#[derive(Clone, Debug)]
pub struct HealthReport<N: Network = Ethereum> {
    /// Health state before the transition.
    pub from: CacheHealth,
    /// Health state after the transition.
    pub to: CacheHealth,
    /// Block number associated with the transition, when known.
    pub block: Option<u64>,
    /// Network marker.
    pub _network: PhantomData<N>,
}

/// Report that a tracked account's storage root moved on a canonical block that
/// no decoder covered — a coverage gap surfaced by the per-block root gate
/// (Phase-8 step 4).
///
/// An account's `storageHash` is a collision-resistant commitment over all of its
/// storage, so a moved root proves *something* under the account changed. When
/// that account is [`WholeAccount`](TrackingPolicy::WholeAccount)-tracked and the
/// batch's touched-address set does not include it, the change arrived through a
/// path no decoder observed. The runtime emits this report (delivered through the
/// normal dispatch path so [`ReactiveHook::on_report`] observers see it),
/// increments [`CacheMetricsSnapshot::coverage_gaps`], and schedules a
/// [`ResyncReason::RootMoved`] repair to re-read the account authoritatively.
#[derive(Clone, Debug)]
pub struct CoverageGapReport<N: Network = Ethereum> {
    /// The tracked account whose root moved with no covering decoder.
    pub address: Address,
    /// The canonical block number at which the gap was observed.
    pub block: u64,
    /// Network marker.
    pub _network: PhantomData<N>,
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

/// Error returned when [`ReactiveEngine`] cannot register a handler on both the
/// runtime and subscriber sides.
#[derive(Debug, thiserror::Error)]
pub enum ReactiveEngineRegisterError {
    /// Runtime registry rejected the handler.
    #[error(transparent)]
    Register(#[from] RegisterError),
    /// Subscriber rejected the handler's interests.
    #[error(transparent)]
    Subscriber(#[from] SubscriberError),
}

/// Error returned by [`ReactiveEngine`] helpers that combine subscriber polling
/// and runtime ingestion.
#[derive(Debug, thiserror::Error)]
pub enum ReactiveEngineError {
    /// Subscriber polling failed.
    #[error(transparent)]
    Subscriber(#[from] SubscriberError),
    /// Runtime ingestion failed.
    #[error(transparent)]
    Runtime(#[from] ReactiveError),
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
    health: CacheHealth,
    metrics: CacheMetrics,
    /// Opt-in freshness registry the runtime stamps for canonical event writes.
    ///
    /// `None` by default (behavior unchanged); populated by
    /// [`enable_freshness_stamping`](Self::enable_freshness_stamping). When
    /// present, applying a canonical handler storage-slot effect stamps the
    /// touched `(address, slot)` as [`Validity::ValidThrough`](crate::freshness::Validity::ValidThrough)`(N)`
    /// so event-maintained slots stop being needlessly re-verified while aging to
    /// volatile once the clock passes `N`.
    freshness: Option<FreshnessRegistry>,
    /// Per-account tracking registry consulted by the per-block root gate
    /// (Phase-8 step 4). Empty by default; populated by
    /// [`track_account`](Self::track_account). When empty the gate is a no-op.
    tracking: HashMap<Address, TrackingPolicy>,
    /// Per-account root/field baselines the gate diffs against across blocks.
    /// Adopted on first probe and re-adopted on every observed move.
    tracked_roots: HashMap<Address, TrackedRoot>,
    /// How often the root gate fires (§6.2); see [`RootGateCadence`].
    root_gate_cadence: RootGateCadence,
    /// Canonical block of the last root-gate firing. `None` until the first
    /// firing (which happens at the first canonical block ever seen, so
    /// baseline adoption never waits a full cadence window).
    last_gate_block: Option<u64>,
    /// Union of decoder-touched addresses since the last root-gate firing,
    /// drained when it fires. Under cadence the gap rule "root moved ∧ addr ∉
    /// touched" must judge against every covered write in the window, or a
    /// decoder-covered write in a skipped block would false-positive as a
    /// [`ReactiveReport::CoverageGap`].
    touched_since_gate: HashSet<Address>,
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

    /// Remove one handler by id, leaving all other handlers and interests intact.
    ///
    /// Returns the removed handler when the id was registered. Cache eviction is
    /// intentionally outside this API: unregistering stops future routing and
    /// decode for the handler only.
    pub fn unregister_handler(&mut self, id: &HandlerId) -> Option<Arc<dyn ReactiveHandler<N>>> {
        let index = self
            .handlers
            .iter()
            .position(|registered| &registered.id == id)?;
        Some(self.handlers.remove(index).handler)
    }

    /// Return true when `id` is currently registered.
    pub fn contains_handler(&self, id: &HandlerId) -> bool {
        self.handlers.iter().any(|registered| &registered.id == id)
    }

    /// Ids of all registered handlers, in registration (= routing) order.
    pub fn handler_ids(&self) -> Vec<HandlerId> {
        self.handlers
            .iter()
            .map(|registered| registered.id.clone())
            .collect()
    }

    /// Borrow the interests owned by one handler.
    pub fn handler_interests(&self, id: &HandlerId) -> Option<&[ReactiveInterest<N>]> {
        self.handlers
            .iter()
            .find(|registered| &registered.id == id)
            .map(|registered| registered.interests.as_slice())
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
            health: CacheHealth::Healthy,
            metrics: CacheMetrics::default(),
            freshness: None,
            tracking: HashMap::new(),
            tracked_roots: HashMap::new(),
            root_gate_cadence: RootGateCadence::default(),
            last_gate_block: None,
            touched_since_gate: HashSet::new(),
        }
    }

    /// Track `address` under `policy` for the per-block root gate (Phase-8 step 4).
    ///
    /// Tracking is strictly opt-in: a runtime with no tracked accounts runs the
    /// gate as a no-op. Registering an account clears any baseline it held (a
    /// policy change re-adopts on the next probe rather than diffing against a
    /// baseline captured under the old policy). Each [`RootGateCadence`]
    /// firing, the gate
    /// probes tracked [`WholeAccount`](TrackingPolicy::WholeAccount) and
    /// [`Scalars`](TrackingPolicy::Scalars) accounts' roots/fields via the
    /// account-proof seam and, on a move no decoder covered, emits a
    /// [`ReactiveReport::CoverageGap`] and schedules a
    /// [`ResyncReason::RootMoved`] repair. [`Slots`](TrackingPolicy::Slots)
    /// accounts are never root-gated (spec Decision 3).
    pub fn track_account(&mut self, address: Address, policy: TrackingPolicy) {
        self.tracking.insert(address, policy);
        self.tracked_roots.remove(&address);
    }

    /// Stop tracking `address`, dropping its policy and any adopted baseline.
    ///
    /// Returns `true` if the account was tracked.
    pub fn untrack_account(&mut self, address: Address) -> bool {
        self.tracked_roots.remove(&address);
        self.tracking.remove(&address).is_some()
    }

    /// Set how often the root gate probes tracked accounts (default:
    /// [`RootGateCadence::default`] — every 16 canonical blocks; see the
    /// [`RootGateCadence`] docs for why skipping blocks loses no detection).
    ///
    /// Reconfiguring resets the gate's window bookkeeping (the touched-address
    /// accumulator and the last-fired block), so a stale window never leaks
    /// into the new cadence: the next canonical block fires the gate.
    pub fn set_root_gate_cadence(&mut self, cadence: RootGateCadence) {
        self.root_gate_cadence = cadence;
        self.last_gate_block = None;
        self.touched_since_gate.clear();
    }

    /// The configured [`RootGateCadence`].
    pub fn root_gate_cadence(&self) -> RootGateCadence {
        self.root_gate_cadence
    }

    /// Enable freshness stamping of canonical event-derived writes (opt-in).
    ///
    /// Installs a [`FreshnessRegistry`] the runtime owns; while it is present,
    /// applying a canonical handler storage-slot effect for a block `N` stamps the
    /// touched `(address, slot)` as
    /// [`Validity::ValidThrough`](crate::freshness::Validity::ValidThrough)`(N)`.
    /// The slot is therefore not volatile *at* `N` (event-maintained, no need to
    /// re-verify) but ages to volatile once the clock passes `N`.
    ///
    /// Idempotent: if a registry is already installed it is left untouched, so an
    /// existing registry (and any stamps it holds) is never clobbered.
    pub fn enable_freshness_stamping(&mut self) {
        if self.freshness.is_none() {
            self.freshness = Some(FreshnessRegistry::new());
        }
    }

    /// Borrow the runtime's freshness registry, if stamping was enabled.
    ///
    /// Returns `None` unless
    /// [`enable_freshness_stamping`](Self::enable_freshness_stamping) was called.
    pub fn freshness(&self) -> Option<&FreshnessRegistry> {
        self.freshness.as_ref()
    }

    /// Mutably borrow the runtime's freshness registry, if stamping was enabled.
    ///
    /// Returns `None` unless
    /// [`enable_freshness_stamping`](Self::enable_freshness_stamping) was called.
    pub fn freshness_mut(&mut self) -> Option<&mut FreshnessRegistry> {
        self.freshness.as_mut()
    }

    /// Return the current queryable [`CacheHealth`] of the runtime.
    pub fn health(&self) -> CacheHealth {
        self.health
    }

    /// Return a point-in-time snapshot of the runtime's observability counters.
    pub fn metrics(&self) -> CacheMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Complete the caller-driven self-heal by returning health to
    /// [`CacheHealth::Healthy`].
    ///
    /// A trust-loss event (a reorg deeper than the journal, or a detected missed
    /// block range) escalates health toward [`CacheHealth::Unhealthy`] as a
    /// "stop until rebuilt" signal that the caller must act on. Once the caller
    /// has resynced or rebuilt the affected state, it invokes this to clear the
    /// signal. It does not emit a [`ReactiveReport::Health`] report, since it is
    /// called outside an ingest cycle.
    pub fn reset_health(&mut self) {
        self.health = CacheHealth::Healthy;
    }

    /// Escalate health one rung up the trust-loss ladder for a trust-loss event
    /// observed at `block`, returning a [`ReactiveReport::Health`] report when the
    /// state actually changes.
    ///
    /// The ladder is:
    /// - [`Healthy`](CacheHealth::Healthy) -> [`Degraded`](CacheHealth::Degraded)
    /// - [`Degraded`](CacheHealth::Degraded) -> [`Unhealthy`](CacheHealth::Unhealthy)
    /// - [`Unhealthy`](CacheHealth::Unhealthy) -> no change (`None`)
    ///
    /// A first event degrades; a second escalates to the terminal
    /// [`Unhealthy`](CacheHealth::Unhealthy) stop signal. This is shared by both
    /// trust-loss paths (deep reorg beyond the journal and missed-range
    /// detection) so mixed event types climb the same ladder.
    fn escalate_trust(&mut self, block: u64) -> Option<Arc<ReactiveReport<N>>> {
        let to = match self.health {
            CacheHealth::Healthy => CacheHealth::Degraded { since_block: block },
            CacheHealth::Degraded { .. } => CacheHealth::Unhealthy { since_block: block },
            CacheHealth::Unhealthy { .. } => return None,
        };
        self.transition_health(to, Some(block))
    }

    /// Transition health to `to`, returning a [`ReactiveReport::Health`] report
    /// when the state actually changes.
    ///
    /// The returned report must be threaded into the ingest cycle's dispatched
    /// reports so it reaches hooks and appears in
    /// [`ReactiveBatchReport::reports`]. Returns `None` when `to` equals the
    /// current state (no transition, no report).
    fn transition_health(
        &mut self,
        to: CacheHealth,
        block: Option<u64>,
    ) -> Option<Arc<ReactiveReport<N>>> {
        if to == self.health {
            return None;
        }
        let from = self.health;
        self.health = to;
        Some(Arc::new(ReactiveReport::Health(HealthReport {
            from,
            to,
            block,
            _network: PhantomData,
        })))
    }

    /// Register a handler.
    pub fn register_handler(
        &mut self,
        handler: Arc<dyn ReactiveHandler<N>>,
    ) -> Result<(), RegisterError> {
        self.registry.register_handler(handler)
    }

    /// Remove one handler from the runtime registry without resetting runtime state.
    ///
    /// This delegates to [`ReactiveRegistry::unregister_handler`] only. It does
    /// not clear the reorg journal, health, metrics, hooks, pending resyncs,
    /// tracking policy, freshness registry, or root-gate baselines, and it does
    /// not purge [`EvmCache`] state. Callers that want cache eviction must issue
    /// explicit `StateUpdate::purge` updates or use cache purge APIs separately.
    pub fn unregister_handler(&mut self, id: &HandlerId) -> Option<Arc<dyn ReactiveHandler<N>>> {
        self.registry.unregister_handler(id)
    }

    /// Return true when the runtime has a registered handler with `id`.
    pub fn contains_handler(&self, id: &HandlerId) -> bool {
        self.registry.contains_handler(id)
    }

    /// Ids of all registered handlers, in registration (= routing) order.
    pub fn handler_ids(&self) -> Vec<HandlerId> {
        self.registry.handler_ids()
    }

    /// Borrow the interests owned by one registered handler.
    pub fn handler_interests(&self, id: &HandlerId) -> Option<&[ReactiveInterest<N>]> {
        self.registry.handler_interests(id)
    }

    /// The most recently journaled canonical block, if any.
    ///
    /// This is the runtime's current chain position: the canonical block most
    /// recently recorded by ingestion. Reorged blocks are dropped from the
    /// journal during recovery, so a rolled-back head does not linger here.
    /// [`ReactiveEngine::register_handler`] uses it as the default backfill
    /// anchor for handlers registered mid-lifecycle. `None` until the first
    /// canonical input is journaled, and always `None` when
    /// [`ReactiveConfig::journal_depth`] is 0 (journaling disabled).
    pub fn last_canonical_block(&self) -> Option<BlockRef> {
        self.journal.back().map(|entry| entry.block.clone())
    }

    /// Queued resync requests: surfaced by handlers but not yet executed by an
    /// [`ingest_batch_with_resync`](Self::ingest_batch_with_resync) pass.
    ///
    /// Callers driving resync execution themselves (plain
    /// [`ingest_batch`](Self::ingest_batch) loops) can read the ledger here;
    /// reorg recovery cancels entries whose pinned blocks were dropped, and
    /// [`cancel_pending_resyncs`](Self::cancel_pending_resyncs) drops entries
    /// for torn-down accounts.
    pub fn pending_resyncs(&self) -> &[ResyncRequest] {
        &self.pending_resyncs
    }

    /// Cancel queued resync work that targets `address`, returning the
    /// cancelled portions.
    ///
    /// Every pending [`ResyncRequest`] target referencing `address` is removed;
    /// a request reduced to zero targets is dropped entirely, while
    /// mixed-target requests keep their other accounts queued. Each returned
    /// request mirrors the original id/reason/block/priority and carries only
    /// the targets that were cancelled.
    ///
    /// This is part of the adapter-teardown recipe (see
    /// [`ReactiveEngine::unregister_handler`]): it clears the pending ledger so
    /// a dropped pool's queued repairs stop occupying memory and stop
    /// surfacing as cancellations in reorg reports. It cannot recall requests
    /// already returned to the caller in earlier batch reports.
    pub fn cancel_pending_resyncs(&mut self, address: Address) -> Vec<ResyncRequest> {
        let mut cancelled = Vec::new();
        self.pending_resyncs.retain_mut(|request| {
            let (matching, remaining): (Vec<_>, Vec<_>) = request
                .targets
                .drain(..)
                .partition(|target| resync_target_address(target) == address);
            request.targets = remaining;
            if !matching.is_empty() {
                cancelled.push(ResyncRequest {
                    id: request.id.clone(),
                    reason: request.reason.clone(),
                    block: request.block.clone(),
                    targets: matching,
                    priority: request.priority,
                });
            }
            !request.targets.is_empty()
        });
        cancelled
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
            // Count unique logical requests: several handlers may emit the same
            // ResyncId in one batch, and duplicates fan out per-origin in the
            // report but are one unit of resync work for the metric.
            let unique_requests = resync_report
                .requested
                .iter()
                .map(|request| &request.id)
                .collect::<HashSet<_>>()
                .len();
            self.metrics
                .resync_requests
                .fetch_add(unique_requests as u64, Ordering::Relaxed);
            self.metrics
                .resync_failures
                .fetch_add(resync_report.failed.len() as u64, Ordering::Relaxed);
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
        // Phase-8 step 4: accumulate the addresses a decoder actually wrote this
        // batch (union of applied `StateDiff` addresses) and the batch's canonical
        // block number, so the per-block root gate can run once after the record
        // loop with the full touched set.
        let mut touched_addrs: HashSet<Address> = HashSet::new();
        let mut canonical_batch_block: Option<u64> = None;

        for record in records {
            let input_ref = record.input_ref();
            reports_to_dispatch.push(Arc::new(ReactiveReport::Input(InputReport {
                input_ref,
                context: record.context.clone(),
                _network: PhantomData,
            })));

            if let Some(reorg_report) =
                self.recover_for_canonical_input(cache, &record, &mut reports_to_dispatch)
            {
                self.metrics
                    .reorgs_recovered
                    .fetch_add(1, Ordering::Relaxed);
                remove_canceled_resyncs_from_batch(
                    &mut batch_report.resyncs,
                    &reorg_report.canceled_resyncs,
                );
                reports_to_dispatch.push(Arc::new(ReactiveReport::Reorg(reorg_report)));
            }

            if let Some(reorg_report) =
                self.recover_for_reorged_input(cache, &record, &mut reports_to_dispatch)
            {
                self.metrics
                    .reorgs_recovered
                    .fetch_add(1, Ordering::Relaxed);
                remove_canceled_resyncs_from_batch(
                    &mut batch_report.resyncs,
                    &reorg_report.canceled_resyncs,
                );
                reports_to_dispatch.push(Arc::new(ReactiveReport::Reorg(reorg_report)));
                continue;
            }

            if let Some(block) = canonical_record_block(&record) {
                // Phase-8 step 4: remember the batch's canonical block (the last
                // canonical record wins) so the root gate probes at that height.
                canonical_batch_block = Some(block.number);
                self.record_journal_input(block, input_ref);
            }

            // Phase-8 step 2: drive a per-block env refresh from canonical
            // headers. Best-effort — a strict validation failure is surfaced as
            // a non-fatal error report and does not abort the batch.
            if let Some(Err(err)) = advance_block_for_canonical_record(cache, &record) {
                reports_to_dispatch.push(Arc::new(ReactiveReport::Error(ReactiveErrorReport {
                    input_ref: Some(input_ref),
                    message: err.to_string(),
                    _network: PhantomData,
                })));
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

            // Phase-8 step 3: canonical block number for freshness stamping.
            // Copied out as a plain `u64` (dropping the borrow of `record`) so it
            // can be used while `self.freshness_mut()` mutably borrows `self`
            // inside the execution loop. `None` for pending/removed/reorged
            // records — those never stamp canonical freshness.
            let canonical_block_number = canonical_record_block(&record).map(|block| block.number);

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
                // Phase-8 step 3 (opt-in): stamp every touched `(address, slot)`
                // from this canonical handler write as `ValidThrough(N)`, so an
                // event-maintained slot stops being re-verified until the clock
                // passes its write block. Read the changed slots straight off
                // `applied.diff` (which borrows the local, not `self`) and stamp
                // via `self.freshness`, done before `applied` is moved into the
                // journal/batch below. Only genuinely-changed slots appear here,
                // since a no-op re-write records no `SlotChange`.
                if let (Some(number), Some(registry)) =
                    (canonical_block_number, self.freshness.as_mut())
                {
                    for change in &applied.diff.slots {
                        registry.valid_through_slot(change.address, change.slot, number);
                    }
                }

                // Phase-8 step 4: record every address this decoder actually wrote
                // (or attempted to write) so the root gate can tell a
                // decoder-covered root move from an uncovered coverage gap. Fold in
                // the full `StateDiff` address footprint — real changes
                // (`slots`/`accounts`/`purged`) and cold-skipped attempts alike, so
                // a decoder that tried to write a cold slot still counts as
                // covering the account.
                collect_diff_addresses(&applied.diff, &mut touched_addrs);

                let report = Arc::new(ReactiveReport::Applied(applied.clone()));
                reports_to_dispatch.push(report);
                if let Some(block) = canonical_record_block(&record) {
                    self.record_journal_applied(block, applied.clone());
                }
                batch_report.applied.push(applied);
            }
        }

        // Phase-8 step 4 + §6.2 cadence: accumulate this batch's touched
        // addresses (after all handler effects, so the set is complete), then
        // fire the root gate only on cadence boundaries. The gate diffs
        // against persisted baselines, so skipped blocks lose no detection —
        // but the touched set must be the union since the last firing, or a
        // decoder-covered write in a skipped block would false-positive as a
        // CoverageGap. Fired resyncs surface in `batch_report.resyncs` (so
        // callers see them and `ingest_batch_with_resync` executes them) and
        // coverage reports go into the dispatched reports.
        if self.root_gate_runnable(cache) {
            self.touched_since_gate
                .extend(touched_addrs.iter().copied());
            if self.root_gate_due(canonical_batch_block) {
                let accumulated = std::mem::take(&mut self.touched_since_gate);
                self.run_root_gate(
                    cache,
                    canonical_batch_block,
                    &accumulated,
                    &mut batch_report.resyncs,
                    &mut reports_to_dispatch,
                );
                self.last_gate_block = canonical_batch_block;
            }
        } else {
            // A gate that cannot run (disabled, nothing root-gated, or no
            // proof fetcher) must not grow the accumulator unboundedly.
            // Dropping it is safe: without a runnable gate no baselines exist
            // (a fetcher cannot be uninstalled, and untracking drops the
            // baseline), so there is nothing a lost touched set could falsely
            // gap against later.
            self.touched_since_gate.clear();
        }

        batch_report.reports = reports_to_dispatch;
        Ok(batch_report)
    }

    /// Whether the root gate could produce any signal at all: some tracked
    /// account is root-gated (`Slots` never is) and a proof fetcher exists.
    /// When this is false the touched accumulator is dropped rather than
    /// grown (see the ingest call site for why that is safe).
    fn root_gate_runnable(&self, cache: &EvmCache) -> bool {
        if matches!(self.root_gate_cadence, RootGateCadence::Disabled) {
            return false;
        }
        let has_gated_targets = self
            .tracking
            .values()
            .any(|policy| !matches!(policy, TrackingPolicy::Slots { .. }));
        has_gated_targets && cache.account_proof_fetcher().is_some()
    }

    /// Whether the root gate is due at this batch's canonical block (§6.2):
    /// the first canonical block ever seen always fires (baseline adoption
    /// must not wait a full window), then at most once every `n` blocks.
    fn root_gate_due(&self, canonical_block: Option<u64>) -> bool {
        let Some(block) = canonical_block else {
            return false;
        };
        match self.root_gate_cadence {
            RootGateCadence::Disabled => false,
            RootGateCadence::EveryNBlocks(n) => match self.last_gate_block {
                None => true,
                Some(last) => block >= last.saturating_add(n.get()),
            },
        }
    }

    /// The `storageHash` root gate (Phase-8 step 4), fired per
    /// [`RootGateCadence`] window (§6.2).
    ///
    /// Runs at the firing batch's canonical block, with `touched` carrying the
    /// union of decoder-touched addresses since the previous firing. For each tracked
    /// [`WholeAccount`](TrackingPolicy::WholeAccount) / [`Scalars`](TrackingPolicy::Scalars)
    /// account, probe the root (and account fields) via the account-proof seam and
    /// apply the spec §4 table:
    ///
    /// - No baseline yet ⇒ **adopt** (no gap, no resync — adoption is not a gap).
    /// - [`WholeAccount`](TrackingPolicy::WholeAccount) root unchanged ⇒ nothing.
    /// - [`WholeAccount`](TrackingPolicy::WholeAccount) root moved, `addr ∈ touched`
    ///   ⇒ a decoder covered it; re-adopt, no gap.
    /// - [`WholeAccount`](TrackingPolicy::WholeAccount) root moved, `addr ∉ touched`
    ///   ⇒ emit [`ReactiveReport::CoverageGap`], count it, schedule a
    ///   [`ResyncReason::RootMoved`] account resync, re-adopt.
    /// - [`Scalars`](TrackingPolicy::Scalars) ⇒ compare balance/nonce/code-hash to
    ///   the baseline (native field changes never move the storage root); on a move
    ///   with `addr ∉ touched`, schedule a [`ResyncReason::RootMoved`] account
    ///   resync for the changed fields and re-adopt.
    ///
    /// No-op when the tracking registry is empty, when the batch has no canonical
    /// block, or when the cache has no account-proof fetcher installed.
    /// [`Slots`](TrackingPolicy::Slots) accounts are never root-gated (spec
    /// Decision 3).
    fn run_root_gate(
        &mut self,
        cache: &EvmCache,
        canonical_block: Option<u64>,
        touched: &HashSet<Address>,
        resyncs: &mut Vec<ResyncRequest>,
        reports: &mut Vec<Arc<ReactiveReport<N>>>,
    ) {
        if self.tracking.is_empty() {
            return;
        }
        let Some(block) = canonical_block else {
            return;
        };
        let Some(fetcher) = cache.account_proof_fetcher().cloned() else {
            return;
        };

        // Collect the root-gated targets (Slots opts out) in a stable order so a
        // single-block sequence of resyncs/reports is deterministic.
        let mut targets: Vec<(Address, bool)> = self
            .tracking
            .iter()
            .filter_map(|(address, policy)| match policy {
                TrackingPolicy::Slots { .. } => None,
                TrackingPolicy::WholeAccount => Some((*address, true)),
                TrackingPolicy::Scalars => Some((*address, false)),
            })
            .collect();
        if targets.is_empty() {
            return;
        }
        targets.sort_by_key(|(address, _)| *address);

        let block_id = BlockId::number(block);
        // ONE seam invocation carries every root-gated target (root-only
        // probes: no storage keys needed). eth_getProof is single-address at
        // the RPC level, so batching here lets the fetcher fan the requests
        // out concurrently instead of paying N sequential round trips.
        let mut probes: HashMap<Address, StorageFetchResult<AccountProof>> = (fetcher)(
            targets
                .iter()
                .map(|&(address, _)| (address, vec![]))
                .collect(),
            block_id,
        )
        .into_iter()
        .collect();
        for (address, whole_account) in targets {
            let Some(Ok(proof)) = probes.remove(&address) else {
                // A failed/omitted probe carries no signal; leave the baseline
                // untouched and try again next block.
                continue;
            };

            let baseline = self.tracked_roots.get(&address).cloned();
            let Some(baseline) = baseline else {
                // First observation: adopt the baseline. Not a coverage gap.
                self.adopt_root(address, block, &proof);
                continue;
            };

            // A stale probe (a batch whose canonical block is not newer than the
            // last one we baselined this account against) carries no forward
            // signal: skip it rather than diff against — or clobber — a newer
            // baseline.
            if block <= baseline.last_block {
                continue;
            }

            if whole_account {
                if proof.storage_hash == baseline.last_root {
                    // Tight steady-state path: unchanged root ⇒ nothing.
                    continue;
                }
                // Root moved.
                if !touched.contains(&address) {
                    // Moved with no covering decoder — the coverage gap.
                    reports.push(Arc::new(ReactiveReport::CoverageGap(CoverageGapReport {
                        address,
                        block,
                        _network: PhantomData,
                    })));
                    self.metrics.coverage_gaps.fetch_add(1, Ordering::Relaxed);
                    resyncs.push(root_moved_account_resync(
                        address,
                        block,
                        AccountFieldMask {
                            balance: true,
                            nonce: true,
                            code: true,
                        },
                    ));
                }
                // Adopt the new root whether or not a decoder covered it.
                self.adopt_root(address, block, &proof);
            } else {
                // Scalars: compare the account fields directly (native changes do
                // not move the storage root).
                let balance_moved = proof.balance != baseline.balance;
                let nonce_moved = proof.nonce != baseline.nonce;
                let code_moved = proof.code_hash != baseline.code_hash;
                if (balance_moved || nonce_moved || code_moved) && !touched.contains(&address) {
                    resyncs.push(root_moved_account_resync(
                        address,
                        block,
                        AccountFieldMask {
                            balance: balance_moved,
                            nonce: nonce_moved,
                            code: code_moved,
                        },
                    ));
                }
                self.adopt_root(address, block, &proof);
            }
        }
    }

    /// Adopt (or re-adopt) `proof` as the baseline for `address` at `block`.
    fn adopt_root(&mut self, address: Address, block: u64, proof: &AccountProof) {
        self.tracked_roots.insert(
            address,
            TrackedRoot {
                last_root: proof.storage_hash,
                last_block: block,
                balance: proof.balance,
                nonce: proof.nonce,
                code_hash: proof.code_hash,
            },
        );
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

            if let Err(error) =
                validate_effects(input_ref, &record.context, &registered.id, &outcome.effects)
            {
                if matches!(error, ReactiveError::InvalidPendingEffect { .. }) {
                    self.metrics
                        .pending_contamination
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(error);
            }
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
        health_reports: &mut Vec<Arc<ReactiveReport<N>>>,
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
            // A forward gap: blocks between the journaled head and the arriving
            // block were never observed (e.g. a disconnect). Make it observable
            // and escalate health, but still accept the arriving block so it
            // journals/applies normally (the chain extends).
            self.metrics.missed_ranges.fetch_add(1, Ordering::Relaxed);
            health_reports.extend(self.escalate_trust(block.number));
            health_reports.push(Arc::new(ReactiveReport::MissedBlockRange(
                MissedRangeReport {
                    from: latest.number + 1,
                    to: block.number - 1,
                    block: block.number,
                    _network: PhantomData,
                },
            )));
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
                health_reports.extend(self.warn_under_recovery(block.number));
                self.drain_journal_from_number(block.number)
            }
        } else {
            health_reports.extend(self.warn_under_recovery(block.number));
            self.drain_journal_from_number(block.number)
        };

        self.recover_dropped_journals(cache, dropped, ReorgReason::ParentMismatch)
    }

    fn recover_for_reorged_input(
        &mut self,
        cache: &mut EvmCache,
        record: &ReactiveInputRecord<N>,
        health_reports: &mut Vec<Arc<ReactiveReport<N>>>,
    ) -> Option<ReorgReport<N>> {
        let (dropped_block, reason) = reorg_signal_block(record)?;
        let dropped = if let Some(index) = self
            .journal
            .iter()
            .position(|entry| entry.block.hash == dropped_block.hash)
        {
            self.drain_journal_from(index)
        } else {
            health_reports.extend(self.warn_under_recovery(dropped_block.number));
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

    /// Warn that a reorg references a block no longer resident in the journal, so
    /// recovery is limited to the blocks still journaled — effects from aged-out
    /// blocks are neither rolled back nor purged (the freshness/validation loop is
    /// the backstop). Makes the under-recovery observable instead of silent.
    ///
    /// This is a deep reorg: it increments the `deep_reorgs` counter and escalates
    /// health along the trust-loss ladder via [`escalate_trust`](Self::escalate_trust)
    /// (a first event degrades to [`CacheHealth::Degraded`], a second escalates to
    /// [`CacheHealth::Unhealthy`]). Any resulting [`ReactiveReport::Health`]
    /// transition is returned so the caller can thread it into the ingest cycle's
    /// dispatched reports.
    fn warn_under_recovery(&mut self, reorg_number: u64) -> Option<Arc<ReactiveReport<N>>> {
        let oldest_journaled = self.journal.front().map(|entry| entry.block.number);
        tracing::warn!(
            reorg_block = reorg_number,
            oldest_journaled = ?oldest_journaled,
            journal_depth = self.config.journal_depth,
            "reactive reorg recovery is incomplete: the reorged block is no longer \
             in the journal, so effects from blocks aged out of the journal are \
             neither rolled back nor purged (the freshness/validation loop is the \
             backstop). Increase ReactiveConfig::journal_depth to recover deeper \
             reorgs precisely."
        );

        self.metrics.deep_reorgs.fetch_add(1, Ordering::Relaxed);

        self.escalate_trust(reorg_number)
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

/// Fold every address a [`StateDiff`] references — genuine changes
/// (`slots`/`accounts`/`purged`) and cold-skipped attempts (`skipped*`) alike —
/// into `into`. Used by the per-block root gate to accumulate the batch's
/// decoder-touched address set: an account a decoder wrote (or tried to write) is
/// "covered," so a subsequent root move for it is not a coverage gap.
fn collect_diff_addresses(diff: &StateDiff, into: &mut HashSet<Address>) {
    into.extend(diff.slots.iter().map(|change| change.address));
    into.extend(diff.accounts.iter().map(|change| change.address));
    into.extend(diff.purged.iter().map(|purge| purge.address));
    into.extend(diff.skipped.iter().map(|skipped| skipped.address));
    into.extend(diff.skipped_balances.iter().map(|skipped| skipped.address));
    into.extend(diff.skipped_masks.iter().map(|skipped| skipped.address));
    into.extend(diff.skipped_accounts.iter().map(|skipped| skipped.address));
}

/// Build the [`ResyncReason::RootMoved`] account resync the root gate schedules
/// for an uncovered move. Re-reads `address`'s `fields` at `block` through the
/// existing account-resync path (Wave 2). The id is derived from the address and
/// block so a repeated move on the same account/block coalesces deterministically.
fn root_moved_account_resync(
    address: Address,
    block: u64,
    fields: AccountFieldMask,
) -> ResyncRequest {
    ResyncRequest {
        id: ResyncId::new(format!("root-moved:{address:#x}:{block}")),
        reason: ResyncReason::RootMoved,
        block: ResyncBlock::Number(block),
        targets: vec![ResyncTarget::Account { address, fields }],
        priority: ResyncPriority::Normal,
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

/// Best-effort per-block env refresh (Phase-8 step 2).
///
/// For a canonical record carrying a full header — a
/// [`ReactiveInput::BlockHeader`] or [`ReactiveInput::FullBlock`] — refresh the
/// cache's block env from that header via [`EvmCache::advance_block`]. Returns
/// `Some(result)` when a header was present (so the caller can surface a strict
/// validation error), and `None` for pending/reorged records or non-header
/// inputs, which must never drive a canonical env refresh.
fn advance_block_for_canonical_record<N: Network>(
    cache: &mut EvmCache,
    record: &ReactiveInputRecord<N>,
) -> Option<Result<(), BlockContextError>> {
    if !is_canonical_status(&record.context.chain_status) {
        return None;
    }
    match &record.input {
        ReactiveInput::BlockHeader(header) => Some(cache.advance_block(header)),
        ReactiveInput::FullBlock(block) => Some(cache.advance_block(block.header())),
        _ => None,
    }
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

fn resync_target_address(target: &ResyncTarget) -> Address {
    match target {
        ResyncTarget::StorageSlot { address, .. }
        | ResyncTarget::StorageSlots { address, .. }
        | ResyncTarget::Account { address, .. } => *address,
    }
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

/// One account-target resync collected during request scanning, resolved through
/// the account proof fetcher after storage groups are processed.
#[derive(Clone, Debug)]
struct AccountResyncTarget {
    request_id: ResyncId,
    block: ResyncBlock,
    address: Address,
    fields: AccountFieldMask,
}

fn resolve_trace_resyncs(
    cache: &EvmCache,
    storage_groups: &mut Vec<StorageFetchGroup>,
    account_targets: &mut Vec<AccountResyncTarget>,
    state_updates: &mut Vec<StateUpdate>,
) {
    let Some(fetcher) = cache.block_state_diff_fetcher().cloned() else {
        return;
    };

    let mut blocks = Vec::new();
    let mut seen = HashSet::new();
    for block in storage_groups
        .iter()
        .map(|group| group.block.clone())
        .chain(account_targets.iter().map(|target| target.block.clone()))
    {
        if seen.insert(block.clone()) {
            blocks.push(block);
        }
    }

    let mut traces = HashMap::new();
    for block in blocks {
        match (fetcher)(resync_block_to_block_id(&block)) {
            Ok(diff) => {
                traces.insert(block, diff);
            }
            Err(error) => {
                tracing::debug!(
                    block = ?block,
                    error = %error,
                    "block trace resync source failed; falling back to point resync"
                );
            }
        }
    }

    for group in storage_groups.iter_mut() {
        let Some(trace) = traces.get(&group.block) else {
            continue;
        };
        group.slots.retain(|slot| {
            if let Some(value) = trace_storage_value(trace, slot.address, slot.slot) {
                state_updates.push(StateUpdate::slot(slot.address, slot.slot, value));
                return false;
            }
            cache
                .cached_storage_value(slot.address, slot.slot)
                .is_none()
        });
        group.seen = group
            .slots
            .iter()
            .map(|slot| (slot.address, slot.slot))
            .collect();
    }
    storage_groups.retain(|group| !group.slots.is_empty());

    let mut unresolved_accounts = Vec::new();
    for mut account in account_targets.drain(..) {
        let Some(trace) = traces.get(&account.block) else {
            unresolved_accounts.push(account);
            continue;
        };
        let Some(trace_account) = trace
            .accounts
            .iter()
            .find(|diff| diff.address == account.address)
        else {
            unresolved_accounts.push(account);
            continue;
        };

        let mut patch = AccountPatch::default();
        let mut unresolved = AccountFieldMask::default();
        if account.fields.balance {
            if let Some(balance) = trace_account.balance {
                patch = patch.balance(balance);
            } else {
                unresolved.balance = true;
            }
        }
        if account.fields.nonce {
            if let Some(nonce) = trace_account.nonce {
                patch = patch.nonce(nonce);
            } else {
                unresolved.nonce = true;
            }
        }
        if account.fields.code {
            if let Some(code) = &trace_account.code {
                patch = patch.code(code.clone());
            } else {
                unresolved.code = true;
            }
        }

        if patch.balance.is_some() || patch.nonce.is_some() || patch.code.is_some() {
            state_updates.push(StateUpdate::account_upsert(account.address, patch));
        }
        if !account_field_mask_empty(unresolved) {
            account.fields = unresolved;
            unresolved_accounts.push(account);
        }
    }
    *account_targets = unresolved_accounts;
}

fn trace_storage_value(trace: &BlockStateDiff, address: Address, slot: U256) -> Option<U256> {
    trace
        .accounts
        .iter()
        .find(|account| account.address == address)
        .and_then(|account| {
            account
                .storage
                .iter()
                .find(|entry| entry.slot == slot)
                .map(|entry| entry.value)
        })
}

fn account_field_mask_empty(mask: AccountFieldMask) -> bool {
    !mask.balance && !mask.nonce && !mask.code
}

fn execute_resync_requests(cache: &mut EvmCache, requests: &[ResyncRequest]) -> ResyncReport {
    let mut failed = Vec::new();
    let mut storage_groups: Vec<StorageFetchGroup> = Vec::new();
    let mut account_targets: Vec<AccountResyncTarget> = Vec::new();

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
                ResyncTarget::Account { address, fields } => {
                    account_targets.push(AccountResyncTarget {
                        request_id: request.id.clone(),
                        block: request.block.clone(),
                        address: *address,
                        fields: *fields,
                    });
                }
            }
        }
    }

    let mut state_updates = Vec::new();
    resolve_trace_resyncs(
        cache,
        &mut storage_groups,
        &mut account_targets,
        &mut state_updates,
    );

    if !storage_groups.is_empty() {
        if let Some(fetcher) = cache.storage_batch_fetcher().cloned() {
            for group in storage_groups {
                let block = group.block.clone();
                let fetches: Vec<(Address, U256)> = group
                    .slots
                    .iter()
                    .map(|slot| (slot.address, slot.slot))
                    .collect();
                let results = (fetcher)(fetches, resync_block_to_block_id(&block));
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

    if !account_targets.is_empty() {
        if let Some(fetcher) = cache.account_proof_fetcher().cloned() {
            // ONE seam invocation per distinct resync block (targets may pin
            // different blocks): eth_getProof is single-address at the RPC
            // level, so batching the addresses lets the fetcher fan the
            // requests out concurrently instead of paying one round trip per
            // account. Root-only probes: account fields need no storage keys.
            let mut groups: Vec<(BlockId, Vec<_>)> = Vec::new();
            for account in account_targets {
                let block_id = resync_block_to_block_id(&account.block);
                match groups
                    .iter_mut()
                    .find(|(group_block, _)| *group_block == block_id)
                {
                    Some((_, group)) => group.push(account),
                    None => groups.push((block_id, vec![account])),
                }
            }
            for (block_id, group) in groups {
                let probes: HashMap<Address, StorageFetchResult<AccountProof>> = (fetcher)(
                    group
                        .iter()
                        .map(|account| (account.address, vec![]))
                        .collect(),
                    block_id,
                )
                .into_iter()
                .collect();
                for account in group {
                    // `get` + clone rather than `remove`: two targets for the
                    // same address in one group must both resolve from the
                    // single probe.
                    match probes.get(&account.address).cloned() {
                        Some(Ok(proof)) => {
                            // Build an authoritative account update from the requested
                            // field mask. Use the MATERIALIZING `account_upsert` so a
                            // resync applies even to a cold account (a partial `Account`
                            // patch on a cold address is silently skipped).
                            let mut patch = AccountPatch::default();
                            if account.fields.balance {
                                patch = patch.balance(proof.balance);
                            }
                            if account.fields.nonce {
                                patch = patch.nonce(proof.nonce);
                            }
                            // Note: `AccountProof` carries `code_hash`, not code bytes;
                            // the `eth_getProof` seam cannot supply runtime code, so a
                            // code-field resync is a no-op here (code freshness is
                            // handled by a later wave). We still materialize the account
                            // so requested balance/nonce fields take effect.
                            state_updates.push(StateUpdate::account_upsert(account.address, patch));
                        }
                        Some(Err(error)) => {
                            failed.push(ResyncFailure {
                                request_id: account.request_id,
                                block: account.block,
                                target: ResyncTarget::Account {
                                    address: account.address,
                                    fields: account.fields,
                                },
                                kind: ResyncFailureKind::AccountFetchFailed,
                                message: error.to_string(),
                            });
                        }
                        None => {
                            failed.push(ResyncFailure {
                                request_id: account.request_id,
                                block: account.block,
                                target: ResyncTarget::Account {
                                    address: account.address,
                                    fields: account.fields,
                                },
                                kind: ResyncFailureKind::AccountFetchOmitted,
                                message:
                                    "account proof fetcher did not return a result for address"
                                        .to_string(),
                            });
                        }
                    }
                }
            }
        } else {
            for account in account_targets {
                failed.push(ResyncFailure {
                    request_id: account.request_id,
                    block: account.block,
                    target: ResyncTarget::Account {
                        address: account.address,
                        fields: account.fields,
                    },
                    kind: ResyncFailureKind::MissingAccountFetcher,
                    message: "account resync requires an account proof fetcher".to_string(),
                });
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
    /// Replace all interests registered with the subscriber.
    ///
    /// Implementations may use this as a full setup/reset operation. The
    /// in-crate [`AlloySubscriber`] clears owner-scoped interest state and
    /// delivery/dedupe bookkeeping when this method is called.
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

/// Historical log backfill requested when adding subscriber interests.
///
/// Backfill applies only to [`ReactiveInterest::Logs`] entries. Block and
/// pending-transaction interests are live-only. `AlloySubscriber` emits records
/// fetched through this policy as [`InputSource::Backfill`] before attempting
/// live stream initialization, and a drained backfill seeds the filter's
/// delivery anchor at its resolved upper bound (even when the window held no
/// logs), so the newly added filter gets the same reconnect/catch-up protection
/// an established one has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriberBackfill {
    from_block: u64,
    to_block: Option<u64>,
}

impl SubscriberBackfill {
    /// Backfill an inclusive block range.
    pub fn range(from_block: u64, to_block: u64) -> Self {
        Self {
            from_block,
            to_block: Some(to_block),
        }
    }

    /// Backfill from `from_block` through the provider's latest block.
    pub fn from_block(from_block: u64) -> Self {
        Self {
            from_block,
            to_block: None,
        }
    }

    /// First block included in the backfill.
    pub fn start_block(&self) -> u64 {
        self.from_block
    }

    /// Last block included in the backfill, or `None` for provider latest.
    pub fn end_block(&self) -> Option<u64> {
        self.to_block
    }
}

/// Extension trait for subscribers that can add and remove handler-owned
/// interests incrementally.
///
/// [`EventSubscriber::register_interests`] remains the full-replacement setup
/// API. Implement this trait when a subscriber can preserve unrelated live
/// sources and delivery state while one handler's interests are added or
/// removed. Implementations should make owner *replacement* continuity-safe:
/// updating an owner's interests must not silently discard delivery progress
/// the previous interests had already established (the in-crate
/// [`AlloySubscriber`] carries the owner's prior delivery anchor over to
/// changed filter shapes and automatically backfills the gap).
pub trait InterestOwnerSubscriber<N: Network = Ethereum>: EventSubscriber<N> {
    /// Add or replace the interests owned by `owner`.
    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError>;

    /// Add or replace owner interests and schedule log backfill for that owner.
    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        backfill: SubscriberBackfill,
    ) -> Result<(), SubscriberError>;

    /// Remove one owner's interests, preserving unrelated interests.
    fn remove_interest_owner(&mut self, owner: &HandlerId) -> Option<Vec<ReactiveInterest<N>>>;

    /// Borrow the interests currently owned by `owner`.
    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<N>]>;
}

/// Binds a [`ReactiveRuntime`] to an [`EventSubscriber`] for the common
/// subscribe-ingest lifecycle.
///
/// The engine treats the runtime registry as the single source of truth for
/// handler lifecycle: [`register_handler`](Self::register_handler) and
/// [`unregister_handler`](Self::unregister_handler) update runtime routing and
/// subscriber interests as one operation, keyed by the handler's stable
/// [`HandlerId`]. Registration is continuity-safe by default — once the runtime
/// has journaled a canonical block, a newly registered handler is backfilled
/// from that block, so a pool discovered in block *N* (say via a factory
/// `PoolCreated` event) misses none of its own logs from *N* onward even though
/// its live subscription starts later. Overlap between backfill and live
/// delivery is absorbed by subscriber and runtime dedup.
///
/// Registration methods by intent:
///
/// | Method | Backfill |
/// |---|---|
/// | [`register_handler`](Self::register_handler) | from the runtime's last canonical block (live-only on a fresh runtime) |
/// | [`register_handler_with_backfill`](Self::register_handler_with_backfill) | explicit range or anchor (deep history) |
/// | [`register_handler_live_only`](Self::register_handler_live_only) | none — future logs only |
///
/// Unregistering a handler stops future subscription routing and runtime
/// decode for that handler; it deliberately does not evict [`EvmCache`] state
/// or undo runtime side effects. See
/// [`unregister_handler`](Self::unregister_handler) for the complete teardown
/// recipe.
///
/// The runtime and subscriber stay independently accessible through
/// [`runtime_mut`](Self::runtime_mut) / [`subscriber_mut`](Self::subscriber_mut)
/// for advanced use. One caution: avoid calling
/// [`EventSubscriber::register_interests`] (the full-replacement setup API) on
/// an engine-managed subscriber — implementations may clear owner-scoped
/// bookkeeping, after which per-handler unregistration no longer releases the
/// handler's transport subscriptions. To bootstrap the subscriber from a
/// runtime that already has handlers, use
/// [`sync_handler_interests`](Self::sync_handler_interests), which registers
/// one owner per handler instead of one unowned blob.
pub struct ReactiveEngine<S, N: Network = Ethereum> {
    runtime: ReactiveRuntime<N>,
    subscriber: S,
}

impl<S, N> ReactiveEngine<S, N>
where
    N: Network,
    S: EventSubscriber<N>,
{
    /// Bind a runtime and subscriber.
    pub fn new(runtime: ReactiveRuntime<N>, subscriber: S) -> Self {
        Self {
            runtime,
            subscriber,
        }
    }

    /// Split the engine into its runtime and subscriber parts.
    pub fn into_parts(self) -> (ReactiveRuntime<N>, S) {
        (self.runtime, self.subscriber)
    }

    /// Borrow the runtime.
    pub fn runtime(&self) -> &ReactiveRuntime<N> {
        &self.runtime
    }

    /// Mutably borrow the runtime.
    pub fn runtime_mut(&mut self) -> &mut ReactiveRuntime<N> {
        &mut self.runtime
    }

    /// Borrow the subscriber.
    pub fn subscriber(&self) -> &S {
        &self.subscriber
    }

    /// Mutably borrow the subscriber.
    pub fn subscriber_mut(&mut self) -> &mut S {
        &mut self.subscriber
    }

    /// Poll the subscriber for the next batch.
    pub fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        self.subscriber.next_batch()
    }

    /// Ingest one already-polled batch through the runtime (direct effects
    /// only; surfaced resync requests are reported, not executed).
    pub fn ingest_batch(
        &mut self,
        cache: &mut EvmCache,
        batch: ReactiveInputBatch<N>,
    ) -> Result<ReactiveBatchReport<N>, ReactiveError> {
        self.runtime.ingest_batch(cache, batch)
    }

    /// Ingest one already-polled batch and execute the storage/account resyncs
    /// it surfaces, exactly like
    /// [`ReactiveRuntime::ingest_batch_with_resync`].
    pub fn ingest_batch_with_resync(
        &mut self,
        cache: &mut EvmCache,
        batch: ReactiveInputBatch<N>,
    ) -> Result<ReactiveBatchReport<N>, ReactiveError> {
        self.runtime.ingest_batch_with_resync(cache, batch)
    }

    /// Poll the subscriber once and ingest the returned batch when present
    /// (direct effects only).
    pub async fn next_ingest(
        &mut self,
        cache: &mut EvmCache,
    ) -> Result<Option<ReactiveBatchReport<N>>, ReactiveEngineError> {
        let Some(batch) = self.subscriber.next_batch().await? else {
            return Ok(None);
        };
        Ok(Some(self.runtime.ingest_batch(cache, batch)?))
    }

    /// Poll the subscriber once and ingest the returned batch with resync
    /// execution — the loop shape for consumers that rely on coverage-gap
    /// repair (root-gate resyncs, handler-requested re-reads).
    pub async fn next_ingest_with_resync(
        &mut self,
        cache: &mut EvmCache,
    ) -> Result<Option<ReactiveBatchReport<N>>, ReactiveEngineError> {
        let Some(batch) = self.subscriber.next_batch().await? else {
            return Ok(None);
        };
        Ok(Some(self.runtime.ingest_batch_with_resync(cache, batch)?))
    }
}

impl<S, N> ReactiveEngine<S, N>
where
    N: Network,
    S: InterestOwnerSubscriber<N>,
{
    /// Register a handler with both the runtime and subscriber, backfilling its
    /// log interests from the runtime's last canonical block.
    ///
    /// This is the continuity-safe default for mid-lifecycle registration: the
    /// runtime already knows how far it has processed the chain, so the new
    /// handler's logs are fetched from that block forward and no discovery gap
    /// opens between "we decided to track this pool" and "its live subscription
    /// started". On a runtime that has not journaled any canonical block yet
    /// (fresh start, or `journal_depth` 0) registration is live-only, matching
    /// pre-ingestion bootstrap. Use
    /// [`register_handler_with_backfill`](Self::register_handler_with_backfill)
    /// for deeper history or
    /// [`register_handler_live_only`](Self::register_handler_live_only) to opt
    /// out of backfill entirely.
    ///
    /// If subscriber registration fails, the runtime registration is rolled back
    /// before the error is returned.
    pub fn register_handler(
        &mut self,
        handler: Arc<dyn ReactiveHandler<N>>,
    ) -> Result<(), ReactiveEngineRegisterError> {
        let backfill = self
            .runtime
            .last_canonical_block()
            .map(|block| SubscriberBackfill::from_block(block.number));
        self.register_handler_inner(handler, backfill)
    }

    /// Register a handler and request an explicit owner-scoped log backfill for
    /// its interests (deep history / custom anchors).
    ///
    /// If subscriber registration fails, the runtime registration is rolled back
    /// before the error is returned.
    pub fn register_handler_with_backfill(
        &mut self,
        handler: Arc<dyn ReactiveHandler<N>>,
        backfill: SubscriberBackfill,
    ) -> Result<(), ReactiveEngineRegisterError> {
        self.register_handler_inner(handler, Some(backfill))
    }

    /// Register a handler without any log backfill — only logs delivered after
    /// its live subscription starts are routed to it.
    ///
    /// If subscriber registration fails, the runtime registration is rolled back
    /// before the error is returned.
    pub fn register_handler_live_only(
        &mut self,
        handler: Arc<dyn ReactiveHandler<N>>,
    ) -> Result<(), ReactiveEngineRegisterError> {
        self.register_handler_inner(handler, None)
    }

    fn register_handler_inner(
        &mut self,
        handler: Arc<dyn ReactiveHandler<N>>,
        backfill: Option<SubscriberBackfill>,
    ) -> Result<(), ReactiveEngineRegisterError> {
        let id = handler.id();
        self.runtime.register_handler(handler)?;
        let interests = self
            .runtime
            .handler_interests(&id)
            .expect("handler was just registered")
            .to_vec();

        let subscribed = match backfill {
            Some(backfill) => {
                self.subscriber
                    .add_interest_owner_with_backfill(id.clone(), &interests, backfill)
            }
            None => self.subscriber.add_interest_owner(id.clone(), &interests),
        };
        if let Err(error) = subscribed {
            self.runtime.unregister_handler(&id);
            return Err(error.into());
        }

        Ok(())
    }

    /// Register every handler currently in the runtime registry as a subscriber
    /// interest owner.
    ///
    /// This is the bootstrap path for an engine built around a pre-populated
    /// runtime: each handler becomes its own owner (upsert semantics, so
    /// rerunning is safe and already-registered owners are refreshed in place).
    /// No backfill is requested — bootstrap happens before ingestion starts, so
    /// there is no processed position to be continuous with; use
    /// [`register_handler_with_backfill`](Self::register_handler_with_backfill)
    /// for handlers that need history. Owners are not removed by this call: use
    /// [`unregister_handler`](Self::unregister_handler) for lifecycle removal
    /// rather than mutating the runtime registry directly.
    ///
    /// On error, owners already synced stay registered (upserts are
    /// independent); the call can simply be retried.
    pub fn sync_handler_interests(&mut self) -> Result<(), SubscriberError> {
        for id in self.runtime.handler_ids() {
            let interests = self
                .runtime
                .handler_interests(&id)
                .map(<[ReactiveInterest<N>]>::to_vec)
                .unwrap_or_default();
            self.subscriber.add_interest_owner(id, &interests)?;
        }
        Ok(())
    }

    /// Unregister a handler from both the subscriber and runtime.
    ///
    /// Subscriber interests are removed first so no new live records are routed
    /// to a handler after it has left the runtime registry. Returns the removed
    /// handler when the id was registered.
    ///
    /// This is the routing/transport half of dropping an adapter. State the
    /// handler accumulated is deliberately left in place; the complete teardown
    /// for a pool or adapter that will not return is:
    ///
    /// ```text
    /// engine.unregister_handler(&id);
    /// for address in handler_addresses {
    ///     // stop root-gate eth_getProof probes for the account
    ///     engine.runtime_mut().untrack_account(address);
    ///     // drop its queued (unexecuted) repair work from the pending ledger
    ///     engine.runtime_mut().cancel_pending_resyncs(address);
    /// }
    /// // optional: evict cached state via StateUpdate::purge / cache purge APIs
    /// ```
    ///
    /// Health, metrics, the reorg journal, hooks, and freshness stamps are
    /// runtime-global and are never touched by handler removal.
    pub fn unregister_handler(&mut self, id: &HandlerId) -> Option<Arc<dyn ReactiveHandler<N>>> {
        self.subscriber.remove_interest_owner(id);
        self.runtime.unregister_handler(id)
    }
}

/// Alloy-backed event subscriber.
///
/// The default transport slice drives Alloy pubsub subscriptions for logs,
/// block headers, and pending transaction hashes. The HTTP polling `watch_*`
/// transport remains available behind the opt-in `reactive-polling` feature.
/// Pubsub streams reconnect automatically after termination, and log
/// subscriptions are backfilled from the last seen block. Owner-scoped log
/// additions can request backfill from an explicit block anchor. Full pending
/// transaction hydration and full block bodies remain explicit follow-up work.
/// With no registered interests, [`EventSubscriber::next_batch`] returns
/// `Ok(None)`.
pub struct AlloySubscriber<P, N: Network = Ethereum> {
    provider: P,
    mode: SubscriberMode,
    config: SubscriberConfig,
    base_interests: Vec<ReactiveInterest<N>>,
    owned_interests: Vec<OwnedSubscriberInterests<N>>,
    interests: Vec<ReactiveInterest<N>>,
    /// Stable source id per distinct live log filter. Ids key delivery anchors
    /// and live `SubscriberEvent`s; entries are retired (and their anchors
    /// pruned) when no base or owner interest references the filter anymore, so
    /// long-lived owner churn cannot grow this map unboundedly.
    log_source_ids: HashMap<Filter, usize>,
    next_log_source_id: usize,
    pending_backfills: VecDeque<QueuedSubscriberBackfill>,
    /// Set when interest bookkeeping changed since the last successful stream
    /// reconcile, so steady-state polling skips the desired-vs-live diff.
    sources_dirty: bool,
    state: AlloySubscriberState<N>,
    pending_records: VecDeque<ReactiveInputRecord<N>>,
    last_seen_log_blocks: HashMap<usize, u64>,
    recent_input_refs: VecDeque<InputRef>,
    recent_input_ref_set: HashSet<InputRef>,
    _network: PhantomData<N>,
}

struct OwnedSubscriberInterests<N: Network = Ethereum> {
    owner: HandlerId,
    interests: Vec<ReactiveInterest<N>>,
}

struct QueuedSubscriberBackfill {
    owner: HandlerId,
    filter: Filter,
    backfill: SubscriberBackfill,
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
            base_interests: Vec::new(),
            owned_interests: Vec::new(),
            interests: Vec::new(),
            log_source_ids: HashMap::new(),
            next_log_source_id: 0,
            pending_backfills: VecDeque::new(),
            sources_dirty: true,
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

    /// Registered interests across base and owner-scoped registrations.
    pub fn registered_interests(&self) -> &[ReactiveInterest<N>] {
        &self.interests
    }

    /// Add or replace the interests owned by `owner`.
    ///
    /// This preserves unrelated owners, queued/pending records, recent dedupe
    /// state, and last-seen log anchors. The live transport is reconciled on the
    /// next [`EventSubscriber::next_batch`] call so newly added log filters can
    /// be subscribed without rebuilding the whole subscriber object.
    ///
    /// Replacing an existing owner is continuity-safe: filters the owner
    /// already had keep their delivery anchors, and any changed or new filter
    /// shape is automatically backfilled from the owner's oldest prior anchor —
    /// growing a pool set on an established owner does not open a delivery gap
    /// for what the old subscription had already covered. A brand-new owner has
    /// no anchor to inherit; pass an explicit
    /// [`add_interest_owner_with_backfill`](Self::add_interest_owner_with_backfill)
    /// anchor (or register through [`ReactiveEngine::register_handler`], which
    /// anchors to the runtime's last canonical block).
    pub fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError> {
        self.set_interest_owner(owner, interests, None)
    }

    /// Add or replace owner interests and schedule log backfill for that owner.
    ///
    /// Backfill is queued only for log interests; block and pending transaction
    /// interests are live-only. Queued backfill is drained before live stream
    /// initialization, its resolved upper bound seeds the filter's delivery
    /// anchor, and the anchored filter is caught up again right after its live
    /// stream connects — so the discovery boundary is closed end to end as long
    /// as `backfill` starts at (or before) the block the interest was
    /// discovered in. Continuity backfill for a replaced owner (see
    /// [`add_interest_owner`](Self::add_interest_owner)) is queued in addition,
    /// unless this explicit backfill is open-ended and already starts at or
    /// below the owner's prior anchor.
    pub fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        backfill: SubscriberBackfill,
    ) -> Result<(), SubscriberError> {
        self.set_interest_owner(owner, interests, Some(backfill))
    }

    /// Remove one owner's interests, preserving unrelated owner/base interests.
    ///
    /// The owner's queued backfills are dropped, and source-id/anchor
    /// bookkeeping for filters no other owner references is retired. Live
    /// streams for retired filters are torn down on the next
    /// [`EventSubscriber::next_batch`] call (dropping an Alloy subscription
    /// unsubscribes provider-side); events already in flight from them stop
    /// matching the merged interest set and are discarded.
    pub fn remove_interest_owner(&mut self, owner: &HandlerId) -> Option<Vec<ReactiveInterest<N>>> {
        let index = self
            .owned_interests
            .iter()
            .position(|entry| &entry.owner == owner)?;
        let removed = self.owned_interests.remove(index).interests;
        self.pending_backfills
            .retain(|backfill| &backfill.owner != owner);
        self.rebuild_registered_interests();
        self.retire_unreferenced_filters();
        self.sources_dirty = true;
        Some(removed)
    }

    /// Borrow the interests currently owned by `owner`.
    pub fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<N>]> {
        self.owned_interests
            .iter()
            .find(|entry| &entry.owner == owner)
            .map(|entry| entry.interests.as_slice())
    }

    fn set_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        backfill: Option<SubscriberBackfill>,
    ) -> Result<(), SubscriberError> {
        validate_subscriber_config(&self.config)?;

        let mut next_owned = self.clone_owned_interests();
        match next_owned.iter_mut().find(|entry| entry.owner == owner) {
            Some(entry) => entry.interests = interests.to_vec(),
            None => next_owned.push(OwnedSubscriberInterests {
                owner: owner.clone(),
                interests: interests.to_vec(),
            }),
        }
        let next_registered = aggregate_interests(&self.base_interests, &next_owned);
        validate_supported_interests(self.mode, &self.config, &next_registered)?;

        // Continuity capture, before the mutation lands: the owner's previous
        // filter shapes and the oldest delivery anchor among them. A changed
        // filter gets a fresh source id with no anchor, so without this
        // hand-off, replacing an owner's interests (the normal way to grow a
        // pool set) would silently discard the delivery watermark and open a
        // gap until some later explicit backfill.
        let previous_filters: Vec<Filter> = self
            .owner_interests(&owner)
            .map(log_filters)
            .unwrap_or_default();
        let continuity_anchor: Option<u64> = previous_filters
            .iter()
            .filter_map(|filter| self.log_anchor(filter))
            .min();

        self.owned_interests = next_owned;
        self.interests = next_registered;
        self.retire_unreferenced_filters();
        self.sources_dirty = true;

        // Re-queue this owner's backfills from scratch: previously queued
        // entries may reference filter shapes that no longer exist.
        self.pending_backfills
            .retain(|queued| queued.owner != owner);
        for filter in log_filters(interests) {
            if let Some(backfill) = backfill {
                self.pending_backfills.push_back(QueuedSubscriberBackfill {
                    owner: owner.clone(),
                    filter: filter.clone(),
                    backfill,
                });
            }

            // Continuity backfill for changed/new shapes only: an unchanged
            // filter kept its anchor and its live stream, and an open-ended
            // explicit backfill starting at or below the anchor already covers
            // the window.
            let unchanged = previous_filters.contains(&filter);
            let explicit_covers = backfill.is_some_and(|explicit| {
                explicit.end_block().is_none()
                    && continuity_anchor.is_some_and(|anchor| explicit.start_block() <= anchor)
            });
            if let Some(anchor) = continuity_anchor
                && !unchanged
                && !explicit_covers
            {
                self.pending_backfills.push_back(QueuedSubscriberBackfill {
                    owner: owner.clone(),
                    filter,
                    backfill: SubscriberBackfill::from_block(anchor),
                });
            }
        }
        Ok(())
    }

    fn clone_owned_interests(&self) -> Vec<OwnedSubscriberInterests<N>> {
        self.owned_interests
            .iter()
            .map(|entry| OwnedSubscriberInterests {
                owner: entry.owner.clone(),
                interests: entry.interests.clone(),
            })
            .collect()
    }

    fn rebuild_registered_interests(&mut self) {
        self.interests = aggregate_interests(&self.base_interests, &self.owned_interests);
    }

    /// Delivery anchor (last block known fully delivered) for `filter`, if the
    /// filter has a source id and has seen delivery.
    fn log_anchor(&self, filter: &Filter) -> Option<u64> {
        let id = self.log_source_ids.get(filter)?;
        self.last_seen_log_blocks.get(id).copied()
    }

    /// Every live log filter across base and owner interests — merged within
    /// each origin (owner boundaries are preserved so one owner's churn cannot
    /// rewrite another's subscription), then deduplicated across origins so an
    /// identical shape shared by several owners maps to exactly one stream and
    /// one delivery anchor.
    // `Filter` derives `Hash`/`Eq` and has no interior mutability; the
    // `mutable_key_type` lint is a known false positive for it.
    #[allow(clippy::mutable_key_type)]
    fn log_stream_filters(&self) -> Vec<Filter> {
        let mut filters = log_filters(&self.base_interests);
        for entry in &self.owned_interests {
            filters.extend(log_filters(&entry.interests));
        }
        let mut seen = HashSet::new();
        filters.retain(|filter| seen.insert(filter.clone()));
        filters
    }

    /// Drop source-id and anchor bookkeeping for filters no longer referenced
    /// by any base or owner interest, so long-lived owner churn cannot grow the
    /// maps unboundedly. Live streams for retired filters are pruned by the
    /// next reconcile.
    // `Filter` derives `Hash`/`Eq` and has no interior mutability; the
    // `mutable_key_type` lint is a known false positive for it.
    #[allow(clippy::mutable_key_type)]
    fn retire_unreferenced_filters(&mut self) {
        let live: HashSet<Filter> = self.log_stream_filters().into_iter().collect();
        self.log_source_ids
            .retain(|filter, _| live.contains(filter));
        let live_ids: HashSet<usize> = self.log_source_ids.values().copied().collect();
        self.last_seen_log_blocks
            .retain(|id, _| live_ids.contains(id));
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
        self.pending_backfills.clear();
        self.log_source_ids.clear();
        self.next_log_source_id = 0;
        self.sources_dirty = true;
    }
}

impl<P, N> InterestOwnerSubscriber<N> for AlloySubscriber<P, N>
where
    P: Provider<N> + Send + Sync,
    N: Network + 'static,
    N::HeaderResponse: Send + 'static,
{
    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
    ) -> Result<(), SubscriberError> {
        AlloySubscriber::add_interest_owner(self, owner, interests)
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        backfill: SubscriberBackfill,
    ) -> Result<(), SubscriberError> {
        AlloySubscriber::add_interest_owner_with_backfill(self, owner, interests, backfill)
    }

    fn remove_interest_owner(&mut self, owner: &HandlerId) -> Option<Vec<ReactiveInterest<N>>> {
        AlloySubscriber::remove_interest_owner(self, owner)
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<N>]> {
        AlloySubscriber::owner_interests(self, owner)
    }
}

enum AlloySubscriberState<N: Network> {
    Uninitialized,
    Active(SubscriberStreams<N>),
    Empty,
}

struct SubscriberStreams<N: Network> {
    entries: Vec<SubscriberStreamEntry<N>>,
    next_index: usize,
}

struct SubscriberStreamEntry<N: Network> {
    source: SubscriberStreamSource,
    stream: BoxStream<'static, SubscriberEvent<N>>,
}

impl<N: Network> SubscriberStreams<N> {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_index: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn push(
        &mut self,
        source: SubscriberStreamSource,
        stream: BoxStream<'static, SubscriberEvent<N>>,
    ) {
        self.entries.push(SubscriberStreamEntry { source, stream });
    }

    #[cfg(all(test, feature = "reactive-ws"))]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn contains_source(&self, source: &SubscriberStreamSource) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.source.same_key(source))
    }

    fn retain_sources(&mut self, sources: &[SubscriberStreamSource]) {
        self.entries
            .retain(|entry| sources.iter().any(|source| entry.source.same_key(source)));
        self.normalize_next_index();
    }

    fn normalize_next_index(&mut self) {
        if self.entries.is_empty() {
            self.next_index = 0;
        } else if self.next_index >= self.entries.len() {
            self.next_index %= self.entries.len();
        }
    }

    async fn next(&mut self) -> Option<SubscriberEvent<N>> {
        poll_fn(|cx| {
            self.normalize_next_index();
            if self.entries.is_empty() {
                return std::task::Poll::Ready(None);
            }

            let mut index = self.next_index;
            let mut checked = 0usize;
            while checked < self.entries.len() {
                if index >= self.entries.len() {
                    index = 0;
                }
                match self.entries[index].stream.as_mut().poll_next(cx) {
                    std::task::Poll::Ready(Some(event)) => {
                        if matches!(event, SubscriberEvent::StreamTerminated(_)) {
                            self.entries.remove(index);
                            self.next_index = if self.entries.is_empty() {
                                0
                            } else {
                                index % self.entries.len()
                            };
                        } else {
                            self.next_index = (index + 1) % self.entries.len();
                        }
                        return std::task::Poll::Ready(Some(event));
                    }
                    std::task::Poll::Ready(None) => {
                        self.entries.remove(index);
                        if self.entries.is_empty() {
                            self.next_index = 0;
                            return std::task::Poll::Ready(None);
                        }
                    }
                    std::task::Poll::Pending => {
                        checked += 1;
                        index += 1;
                    }
                }
            }

            if self.entries.is_empty() {
                std::task::Poll::Ready(None)
            } else {
                self.next_index = index % self.entries.len();
                std::task::Poll::Pending
            }
        })
        .await
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

    fn same_key(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PubSubLog { filter: left, .. }, Self::PubSubLog { filter: right, .. })
            | (Self::PollingLog { filter: left }, Self::PollingLog { filter: right }) => {
                left == right
            }
            (Self::PubSubPendingHashes, Self::PubSubPendingHashes)
            | (Self::PubSubBlockHeaders, Self::PubSubBlockHeaders)
            | (Self::PollingPendingHashes, Self::PollingPendingHashes) => true,
            _ => false,
        }
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

        self.base_interests = interests.to_vec();
        self.owned_interests.clear();
        self.rebuild_registered_interests();
        self.reset_delivery_state();
        self.state = AlloySubscriberState::Uninitialized;
        Ok(())
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        Box::pin(async {
            if let Some(batch) = self.drain_next_batch() {
                return Ok(Some(batch));
            }

            self.drain_pending_backfills().await?;
            if let Some(batch) = self.drain_next_batch() {
                return Ok(Some(batch));
            }

            self.ensure_streams().await?;
            if let Some(batch) = self.drain_next_batch() {
                return Ok(Some(batch));
            }

            if self.interests.is_empty() {
                return Ok(None);
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
    /// Bring live streams in line with the current interest set.
    ///
    /// Runs incrementally: the desired-vs-live diff only happens when interest
    /// bookkeeping changed since the last successful pass (`sources_dirty`), so
    /// steady-state polling costs nothing here. Missing sources are connected,
    /// sources for retired filters are dropped (dropping an Alloy subscription
    /// unsubscribes provider-side), and unrelated live streams — with their
    /// delivery and anchor state — are left untouched.
    ///
    /// A newly connected log source whose filter already has a delivery anchor
    /// is caught up from that anchor immediately after subscribing (the same
    /// subscribe-then-backfill order the reconnect path uses). Together with
    /// anchor seeding in [`Self::drain_pending_backfills`], that closes the
    /// window between an adoption backfill and live stream start.
    async fn ensure_streams(&mut self) -> Result<(), SubscriberError> {
        if !self.sources_dirty {
            return Ok(());
        }
        // An interest-less subscriber stays Uninitialized and never touches the
        // provider ([`EventSubscriber::next_batch`] returns `Ok(None)`),
        // matching setup-before-interests behavior.
        if matches!(self.state, AlloySubscriberState::Uninitialized) && self.interests.is_empty() {
            return Ok(());
        }

        let desired = self.stream_sources()?;
        let missing: Vec<SubscriberStreamSource> = match &self.state {
            AlloySubscriberState::Active(streams) => desired
                .iter()
                .filter(|source| !streams.contains_source(source))
                .cloned()
                .collect(),
            AlloySubscriberState::Uninitialized | AlloySubscriberState::Empty => desired.clone(),
        };

        let mut connected = Vec::new();
        for source in missing {
            let stream = self.connect_source_stream(source.clone()).await?;
            // Anchored catch-up for a source with a known delivery watermark
            // (seeded by a drained adoption backfill, or inherited from a
            // filter shape that was live before): subscribe first, then fetch
            // the gap, so nothing lands between the two.
            if let Some(event) = self.backfill_reconnected_source(&source).await? {
                self.enqueue_event(event);
            }
            connected.push((source, stream));
        }

        match &mut self.state {
            AlloySubscriberState::Active(streams) => {
                streams.retain_sources(&desired);
                for (source, stream) in connected {
                    streams.push(source, stream);
                }
                if streams.is_empty() {
                    self.state = AlloySubscriberState::Empty;
                }
            }
            AlloySubscriberState::Uninitialized | AlloySubscriberState::Empty => {
                let mut streams = SubscriberStreams::new();
                for (source, stream) in connected {
                    streams.push(source, stream);
                }
                self.state = if streams.is_empty() {
                    AlloySubscriberState::Empty
                } else {
                    AlloySubscriberState::Active(streams)
                };
            }
        }

        self.sources_dirty = false;
        Ok(())
    }

    /// Fetch queued adoption/continuity backfills, oldest first.
    ///
    /// An entry is consumed only after its `get_logs` fetch succeeds — a
    /// transient RPC failure surfaces the error and leaves the entry queued for
    /// the next poll, so a flaky request cannot silently discard the missed
    /// window the backfill exists to close. Open-ended backfills resolve their
    /// upper bound to the provider's current head before fetching, and every
    /// drained backfill advances the filter's delivery anchor to that bound —
    /// even a zero-log window — so the filter is reconnect-protected from then
    /// on. Draining pauses as soon as records are ready for delivery; remaining
    /// entries stay queued.
    async fn drain_pending_backfills(&mut self) -> Result<(), SubscriberError> {
        while let Some(queued) = self.pending_backfills.front() {
            // Owner was removed while its backfill was queued.
            if self.owner_interests(&queued.owner).is_none() {
                self.pending_backfills.pop_front();
                continue;
            }
            let filter = queued.filter.clone();
            let backfill = queued.backfill;

            let to_block = match backfill.end_block() {
                Some(to_block) => to_block,
                None => self
                    .provider
                    .get_block_number()
                    .await
                    .map_err(provider_error)?,
            };
            if to_block < backfill.start_block() {
                // Anchor already at (or past) the provider head: nothing to
                // fetch, and the anchor keeps its current value.
                self.pending_backfills.pop_front();
                continue;
            }

            let range = filter
                .clone()
                .from_block(backfill.start_block())
                .to_block(to_block);
            let logs = self
                .provider
                .get_logs(&range)
                .await
                .map_err(provider_error)?;

            // Fetch succeeded: consume the entry, deliver, and advance the
            // anchor through the fetched bound.
            self.pending_backfills.pop_front();
            let source_id = self.log_source_id(&filter);
            self.enqueue_backfilled_logs(logs, Some(source_id));
            let anchor = self
                .last_seen_log_blocks
                .entry(source_id)
                .or_insert(to_block);
            *anchor = (*anchor).max(to_block);

            if !self.pending_records.is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn stream_sources(&mut self) -> Result<Vec<SubscriberStreamSource>, SubscriberError> {
        match resolve_subscriber_transport(self.mode)? {
            SubscriberTransport::PubSub => Ok(self.pubsub_stream_sources()),
            SubscriberTransport::Polling => Ok(self.polling_stream_sources()),
        }
    }

    fn pubsub_stream_sources(&mut self) -> Vec<SubscriberStreamSource> {
        let mut sources = Vec::new();

        for filter in self.log_stream_filters() {
            let id = self.log_source_id(&filter);
            sources.push(SubscriberStreamSource::PubSubLog { id, filter });
        }

        if needs_pending_hash_stream(&self.interests) {
            sources.push(SubscriberStreamSource::PubSubPendingHashes);
        }

        if needs_header_block_stream(&self.interests) {
            sources.push(SubscriberStreamSource::PubSubBlockHeaders);
        }

        sources
    }

    fn polling_stream_sources(&self) -> Vec<SubscriberStreamSource> {
        let mut sources = Vec::new();

        for filter in self.log_stream_filters() {
            sources.push(SubscriberStreamSource::PollingLog { filter });
        }

        if needs_pending_hash_stream(&self.interests) {
            sources.push(SubscriberStreamSource::PollingPendingHashes);
        }

        sources
    }

    fn log_source_id(&mut self, filter: &Filter) -> usize {
        if let Some(id) = self.log_source_ids.get(filter) {
            return *id;
        }

        let id = self.next_log_source_id;
        self.next_log_source_id = self.next_log_source_id.saturating_add(1);
        self.log_source_ids.insert(filter.clone(), id);
        id
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
                self.enqueue_backfilled_logs(logs, Some(source_id));
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

    fn enqueue_backfilled_logs(&mut self, logs: Vec<Log>, source_id: Option<usize>) {
        for log in logs {
            if log_matches_any_interest(&log, &self.interests) {
                let record = log_input_record(log, InputSource::Backfill);
                if let Some(source_id) = source_id {
                    self.note_log_block(source_id, &record);
                }
                self.enqueue_record(record);
            }
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
            AlloySubscriberState::Active(streams) => streams.push(source, stream),
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

#[cfg(any(feature = "reactive-ws", feature = "reactive-polling", test))]
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

fn aggregate_interests<N: Network>(
    base: &[ReactiveInterest<N>],
    owned: &[OwnedSubscriberInterests<N>],
) -> Vec<ReactiveInterest<N>> {
    base.iter()
        .cloned()
        .chain(
            owned
                .iter()
                .flat_map(|entry| entry.interests.iter().cloned()),
        )
        .collect()
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
    #[cfg(feature = "reactive-ws")]
    fn pubsub_sources_assign_stable_log_ids_before_shared_streams() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .register_interests(&[
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
            ])
            .expect("register base interests");

        // The two default-block-option log filters merge into one address
        // superset (existing consolidation behavior), so there is one log source
        // — assigned id 0, before the pending-hash source.
        let sources = subscriber.stream_sources().expect("stream sources");
        assert_eq!(sources.len(), 2);
        assert!(matches!(
            &sources[0],
            SubscriberStreamSource::PubSubLog { id: 0, .. }
        ));
        assert!(matches!(
            sources[1],
            SubscriberStreamSource::PubSubPendingHashes
        ));

        // Ids are stable across repeated source construction.
        let again = subscriber.stream_sources().expect("stream sources again");
        assert!(again[0].same_key(&sources[0]));
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
        let source = SubscriberStreamSource::PubSubPendingHashes;
        streams.push(
            source,
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

    #[test]
    #[cfg(feature = "reactive-ws")]
    fn owner_removal_preserves_delivery_and_dedupe_state() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner(
                HandlerId::new("pool-a"),
                &[ReactiveInterest::Logs(LogInterest {
                    provider_filter: Filter::new()
                        .address(Address::repeat_byte(0x42))
                        .event_signature(B256::repeat_byte(0x01)),
                    local_matcher: None,
                    route_key: None,
                })],
            )
            .expect("register pool-a owner");
        subscriber
            .add_interest_owner(
                HandlerId::new("pool-b"),
                &[ReactiveInterest::Logs(LogInterest {
                    provider_filter: Filter::new()
                        .address(Address::repeat_byte(0x24))
                        .event_signature(B256::repeat_byte(0x02)),
                    local_matcher: None,
                    route_key: None,
                })],
            )
            .expect("register pool-b owner");

        // Allocate source ids the way live stream setup would (pool-a -> id 0),
        // so the injected delivery anchor hangs off a referenced filter.
        let _ = subscriber.stream_sources().expect("stream sources");
        subscriber.enqueue_event(SubscriberEvent::Log {
            source_id: 0,
            log: rpc_log(false),
        });
        assert_eq!(subscriber.pending_records.len(), 1);
        assert_eq!(subscriber.recent_input_refs.len(), 1);
        assert_eq!(subscriber.last_seen_log_blocks.get(&0), Some(&7));

        let removed = subscriber
            .remove_interest_owner(&HandlerId::new("pool-b"))
            .expect("pool-b should be removed");

        assert_eq!(removed.len(), 1);
        assert_eq!(subscriber.pending_records.len(), 1);
        assert_eq!(subscriber.recent_input_refs.len(), 1);
        assert_eq!(subscriber.last_seen_log_blocks.get(&0), Some(&7));
        assert!(
            subscriber
                .owner_interests(&HandlerId::new("pool-a"))
                .is_some()
        );
        assert!(
            subscriber
                .owner_interests(&HandlerId::new("pool-b"))
                .is_none()
        );
        assert_eq!(subscriber.registered_interests().len(), 1);
    }

    #[test]
    #[cfg(feature = "reactive-ws")]
    fn owner_log_sources_do_not_merge_across_owners() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner(
                HandlerId::new("pool-a"),
                &[ReactiveInterest::Logs(LogInterest {
                    provider_filter: Filter::new().address(Address::repeat_byte(0xa1)),
                    local_matcher: None,
                    route_key: None,
                })],
            )
            .expect("register pool-a owner");

        let initial_sources = subscriber.stream_sources().expect("initial sources");
        assert_eq!(initial_sources.len(), 1);
        let pool_a_source = initial_sources[0].clone();
        assert!(matches!(
            &pool_a_source,
            SubscriberStreamSource::PubSubLog { id: 0, .. }
        ));

        subscriber
            .add_interest_owner(
                HandlerId::new("pool-b"),
                &[ReactiveInterest::Logs(LogInterest {
                    provider_filter: Filter::new().address(Address::repeat_byte(0xb2)),
                    local_matcher: None,
                    route_key: None,
                })],
            )
            .expect("register pool-b owner");

        let expanded_sources = subscriber.stream_sources().expect("expanded sources");
        assert_eq!(expanded_sources.len(), 2);
        assert!(
            expanded_sources
                .iter()
                .any(|source| source.same_key(&pool_a_source)),
            "adding pool-b should not rewrite pool-a's stream source"
        );

        subscriber
            .remove_interest_owner(&HandlerId::new("pool-b"))
            .expect("pool-b should be removed");
        let trimmed_sources = subscriber.stream_sources().expect("trimmed sources");
        assert_eq!(trimmed_sources.len(), 1);
        assert!(trimmed_sources[0].same_key(&pool_a_source));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(feature = "reactive-ws")]
    async fn owner_backfill_seeds_reconnect_anchor_before_live_log() {
        let asserter = Asserter::new();
        asserter.push_success(&vec![rpc_log(false)]);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner_with_backfill(
                HandlerId::new("pool-a"),
                &[ReactiveInterest::Logs(LogInterest {
                    provider_filter: Filter::new()
                        .address(Address::repeat_byte(0x42))
                        .event_signature(B256::repeat_byte(0x01)),
                    local_matcher: None,
                    route_key: None,
                })],
                SubscriberBackfill::range(1, 7),
            )
            .expect("register pool-a with backfill");

        subscriber
            .drain_pending_backfills()
            .await
            .expect("owner backfill should drain");

        assert_eq!(subscriber.pending_records.len(), 1);
        assert_eq!(subscriber.last_seen_log_blocks.get(&0), Some(&7));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn subscriber_streams_poll_ready_sources_round_robin() {
        let first_hash = B256::repeat_byte(0x01);
        let second_hash = B256::repeat_byte(0x02);
        let mut streams = SubscriberStreams::new();
        streams.push(
            SubscriberStreamSource::PubSubPendingHashes,
            stream::iter([
                SubscriberEvent::<Ethereum>::PendingHash(first_hash),
                SubscriberEvent::<Ethereum>::PendingHash(first_hash),
            ])
            .boxed(),
        );
        streams.push(
            SubscriberStreamSource::PubSubBlockHeaders,
            stream::once(async move { SubscriberEvent::<Ethereum>::PendingHash(second_hash) })
                .boxed(),
        );

        assert!(matches!(
            streams.next().await,
            Some(SubscriberEvent::PendingHash(hash)) if hash == first_hash
        ));
        assert!(matches!(
            streams.next().await,
            Some(SubscriberEvent::PendingHash(hash)) if hash == second_hash
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(feature = "reactive-ws")]
    async fn owner_updates_ensure_streams_without_full_reset() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .register_interests(&[ReactiveInterest::PendingTransactions(
                PendingTxInterest::default(),
            )])
            .expect("register base pending interest");
        subscriber
            .add_interest_owner(
                HandlerId::new("headers"),
                &[ReactiveInterest::Blocks(BlockInterest::default())],
            )
            .expect("register header owner");

        let mut streams = SubscriberStreams::new();
        streams.push(
            SubscriberStreamSource::PubSubPendingHashes,
            stream::pending::<SubscriberEvent<Ethereum>>().boxed(),
        );
        streams.push(
            SubscriberStreamSource::PubSubBlockHeaders,
            stream::pending::<SubscriberEvent<Ethereum>>().boxed(),
        );
        subscriber.state = AlloySubscriberState::Active(streams);

        subscriber
            .remove_interest_owner(&HandlerId::new("headers"))
            .expect("header owner should be removed");
        assert!(matches!(
            &subscriber.state,
            AlloySubscriberState::Active(streams) if streams.len() == 2
        ));

        subscriber
            .ensure_streams()
            .await
            .expect("pure removal reconciliation should not touch provider");

        assert!(matches!(
            &subscriber.state,
            AlloySubscriberState::Active(streams)
                if streams.len() == 1
                    && streams.contains_source(&SubscriberStreamSource::PubSubPendingHashes)
                    && !streams.contains_source(&SubscriberStreamSource::PubSubBlockHeaders)
        ));

        subscriber
            .add_interest_owner(
                HandlerId::new("headers"),
                &[ReactiveInterest::Blocks(BlockInterest::default())],
            )
            .expect("re-add header owner");
        assert!(matches!(
            &subscriber.state,
            AlloySubscriberState::Active(streams) if streams.len() == 1
        ));
    }

    // A log interest matching `rpc_log` (address 0x42, topic0 0x01).
    #[cfg(feature = "reactive-ws")]
    fn log_interest_matching_rpc_log() -> ReactiveInterest<Ethereum> {
        ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(Address::repeat_byte(0x42))
                .event_signature(B256::repeat_byte(0x01)),
            local_matcher: None,
            route_key: None,
        })
    }

    #[cfg(feature = "reactive-ws")]
    fn log_interest_for(address: u8) -> ReactiveInterest<Ethereum> {
        ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(address)),
            local_matcher: None,
            route_key: None,
        })
    }

    // B1: a transient provider error must not consume the queued backfill — the
    // missed window has to survive for the next poll to retry.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg(feature = "reactive-ws")]
    async fn drain_backfill_retains_queue_entry_on_provider_error() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("rate limited");
        asserter.push_success(&vec![rpc_log(false)]);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let mut subscriber = AlloySubscriber::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner_with_backfill(
                HandlerId::new("pool"),
                &[log_interest_matching_rpc_log()],
                SubscriberBackfill::range(1, 7),
            )
            .expect("register owner with backfill");
        assert_eq!(subscriber.pending_backfills.len(), 1);

        let first = subscriber.drain_pending_backfills().await;
        assert!(first.is_err(), "provider failure should surface");
        assert_eq!(
            subscriber.pending_backfills.len(),
            1,
            "failed fetch must leave the backfill queued for retry"
        );
        assert!(subscriber.pending_records.is_empty());

        subscriber
            .drain_pending_backfills()
            .await
            .expect("retry should succeed");
        assert!(subscriber.pending_backfills.is_empty());
        assert_eq!(subscriber.pending_records.len(), 1);
    }

    // B3: a zero-log backfill window still advances the delivery anchor to its
    // upper bound, so a later reconnect catches up from the right block.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg(feature = "reactive-ws")]
    async fn drain_backfill_seeds_anchor_on_empty_window() {
        let asserter = Asserter::new();
        asserter.push_success(&Vec::<Log>::new());
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let mut subscriber = AlloySubscriber::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner_with_backfill(
                HandlerId::new("pool"),
                &[log_interest_matching_rpc_log()],
                SubscriberBackfill::range(1, 42),
            )
            .expect("register owner with backfill");

        subscriber
            .drain_pending_backfills()
            .await
            .expect("empty backfill should drain");

        assert!(subscriber.pending_records.is_empty());
        let filter = log_filters(subscriber.owner_interests(&HandlerId::new("pool")).unwrap())
            .pop()
            .unwrap();
        assert_eq!(
            subscriber.log_anchor(&filter),
            Some(42),
            "empty window must still seed the anchor at its upper bound"
        );
    }

    // B3 (open-ended): a `from_block`-only backfill resolves its upper bound to
    // the provider head and seeds the anchor there.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg(feature = "reactive-ws")]
    async fn drain_backfill_open_ended_resolves_head_and_seeds_anchor() {
        let asserter = Asserter::new();
        asserter.push_success(&100u64); // get_block_number
        asserter.push_success(&Vec::<Log>::new()); // get_logs
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let mut subscriber = AlloySubscriber::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner_with_backfill(
                HandlerId::new("pool"),
                &[log_interest_matching_rpc_log()],
                SubscriberBackfill::from_block(10),
            )
            .expect("register owner with open-ended backfill");

        subscriber
            .drain_pending_backfills()
            .await
            .expect("open-ended backfill should drain");

        let filter = log_filters(subscriber.owner_interests(&HandlerId::new("pool")).unwrap())
            .pop()
            .unwrap();
        assert_eq!(subscriber.log_anchor(&filter), Some(100));
    }

    // B2: two owners requesting the same filter shape share exactly one live
    // source (and thus one anchor), rather than double-subscribing.
    #[test]
    #[cfg(feature = "reactive-ws")]
    fn duplicate_filters_across_owners_map_to_single_source() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner(HandlerId::new("pool-a"), &[log_interest_for(0xaa)])
            .expect("register pool-a");
        subscriber
            .add_interest_owner(HandlerId::new("pool-b"), &[log_interest_for(0xaa)])
            .expect("register pool-b with identical filter");

        assert_eq!(
            subscriber.log_stream_filters().len(),
            1,
            "identical filters across owners must collapse to one"
        );
        let sources = subscriber.stream_sources().expect("stream sources");
        assert_eq!(sources.len(), 1);
    }

    // B4: removing an owner retires the source-id and anchor bookkeeping for
    // filters no other owner references, so long-lived churn cannot leak.
    #[test]
    #[cfg(feature = "reactive-ws")]
    fn owner_removal_prunes_source_ids_and_anchors() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner(HandlerId::new("pool-a"), &[log_interest_for(0xaa)])
            .expect("register pool-a");
        subscriber
            .add_interest_owner(HandlerId::new("pool-b"), &[log_interest_for(0xbb)])
            .expect("register pool-b");

        // Allocate ids and simulate delivery anchors on both.
        let _ = subscriber.stream_sources().expect("stream sources");
        let filter_a = log_filters(&[log_interest_for(0xaa)]).pop().unwrap();
        let filter_b = log_filters(&[log_interest_for(0xbb)]).pop().unwrap();
        let id_a = subscriber.log_source_id(&filter_a);
        let id_b = subscriber.log_source_id(&filter_b);
        subscriber.last_seen_log_blocks.insert(id_a, 10);
        subscriber.last_seen_log_blocks.insert(id_b, 20);
        assert_eq!(subscriber.log_source_ids.len(), 2);

        subscriber
            .remove_interest_owner(&HandlerId::new("pool-b"))
            .expect("remove pool-b");

        assert_eq!(
            subscriber.log_source_ids.len(),
            1,
            "pool-b's filter id should be retired"
        );
        assert!(subscriber.log_source_ids.contains_key(&filter_a));
        assert_eq!(subscriber.last_seen_log_blocks.get(&id_a), Some(&10));
        assert_eq!(
            subscriber.last_seen_log_blocks.get(&id_b),
            None,
            "pool-b's anchor should be pruned"
        );
    }

    // D1: growing an owner's filter set (a new pool on an existing adapter)
    // changes the merged filter shape; the new shape must inherit the old
    // anchor via an automatic continuity backfill, or logs between the last
    // delivery and the new subscription are silently lost.
    #[test]
    #[cfg(feature = "reactive-ws")]
    fn owner_filter_growth_queues_continuity_backfill_from_prior_anchor() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner(HandlerId::new("amm"), &[log_interest_for(0xaa)])
            .expect("register amm with pool A");

        // Simulate the owner's single merged filter having delivered up to
        // block 50.
        let filter_a = log_filters(&[log_interest_for(0xaa)]).pop().unwrap();
        let id_a = subscriber.log_source_id(&filter_a);
        subscriber.last_seen_log_blocks.insert(id_a, 50);

        // Grow the owner to also watch pool B (same block option -> merges into
        // one {A,B} filter, a new shape).
        subscriber
            .add_interest_owner(
                HandlerId::new("amm"),
                &[log_interest_for(0xaa), log_interest_for(0xbb)],
            )
            .expect("grow amm to pools A+B");

        assert_eq!(
            subscriber.pending_backfills.len(),
            1,
            "the changed merged filter should queue exactly one continuity backfill"
        );
        let queued = &subscriber.pending_backfills[0];
        assert_eq!(queued.owner, HandlerId::new("amm"));
        assert_eq!(queued.backfill.start_block(), 50);
        assert_eq!(
            queued.backfill.end_block(),
            None,
            "continuity backfill runs open-ended to the current head"
        );
    }

    // D1 negative: replacing an owner's interests with the identical shape must
    // NOT re-fetch — the filter kept its anchor and its live stream.
    #[test]
    #[cfg(feature = "reactive-ws")]
    fn unchanged_owner_filter_does_not_queue_continuity_backfill() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner(HandlerId::new("amm"), &[log_interest_for(0xaa)])
            .expect("register amm");
        let filter_a = log_filters(&[log_interest_for(0xaa)]).pop().unwrap();
        let id_a = subscriber.log_source_id(&filter_a);
        subscriber.last_seen_log_blocks.insert(id_a, 50);

        subscriber
            .add_interest_owner(HandlerId::new("amm"), &[log_interest_for(0xaa)])
            .expect("re-register identical interests");

        assert!(
            subscriber.pending_backfills.is_empty(),
            "an unchanged filter shape must not queue continuity backfill"
        );
    }

    // D5 interaction: an explicit open-ended backfill starting at or below the
    // owner's prior anchor already covers the continuity window, so no extra
    // continuity backfill is queued (no redundant double fetch).
    #[test]
    #[cfg(feature = "reactive-ws")]
    fn explicit_open_ended_backfill_below_anchor_suppresses_continuity() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        subscriber
            .add_interest_owner(HandlerId::new("amm"), &[log_interest_for(0xaa)])
            .expect("register amm");
        let filter_a = log_filters(&[log_interest_for(0xaa)]).pop().unwrap();
        let id_a = subscriber.log_source_id(&filter_a);
        subscriber.last_seen_log_blocks.insert(id_a, 50);

        // Grow with an explicit deep backfill from block 10 (< anchor 50).
        subscriber
            .add_interest_owner_with_backfill(
                HandlerId::new("amm"),
                &[log_interest_for(0xaa), log_interest_for(0xbb)],
                SubscriberBackfill::from_block(10),
            )
            .expect("grow amm with explicit deep backfill");

        assert_eq!(
            subscriber.pending_backfills.len(),
            1,
            "only the explicit backfill should be queued; continuity is subsumed"
        );
        assert_eq!(subscriber.pending_backfills[0].backfill.start_block(), 10);
    }

    // The dirty flag gates reconciliation: when nothing changed since the last
    // reconcile, `ensure_streams` must not touch the provider or the state.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg(feature = "reactive-ws")]
    async fn ensure_streams_is_noop_when_not_dirty() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        );
        // An interest that WOULD require a new block-header source...
        subscriber
            .add_interest_owner(
                HandlerId::new("headers"),
                &[ReactiveInterest::Blocks(BlockInterest::default())],
            )
            .expect("register header owner");
        // ...but we mark bookkeeping clean and start from Empty.
        subscriber.state = AlloySubscriberState::Empty;
        subscriber.sources_dirty = false;

        subscriber
            .ensure_streams()
            .await
            .expect("clean reconcile must be a no-op");

        assert!(
            matches!(subscriber.state, AlloySubscriberState::Empty),
            "not-dirty ensure_streams must not connect new sources"
        );
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
