use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use alloy_primitives::{Address, B256, Bytes, FixedBytes, Keccak256, Log as PrimitiveLog};
use alloy_rpc_types_eth::Log;
use tokio::sync::{mpsc, oneshot};

use super::{
    BaseFlashblockBase, FlashblockContentCommitment, FlashblockIngressTiming, FlashblockRef,
    ProviderRef, deserialize_optional_rpc_u64, flashblock_content_hash,
    flashblock_transaction_hashes, non_placeholder_hash,
};

/// Resource bounds applied while converting receipt-enriched JSON Flashblocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RawJsonFlashblocksLimits {
    /// Largest accepted JSON frame.
    pub max_frame_bytes: usize,
    /// Largest accepted index for one payload generation.
    pub max_flashblocks_per_payload: usize,
    /// Largest cumulative transaction membership retained for one generation.
    pub max_transactions_per_payload: usize,
    /// Largest cumulative receipt-log count retained for one generation.
    pub max_logs_per_payload: usize,
}

impl Default for RawJsonFlashblocksLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_flashblocks_per_payload: 64,
            max_transactions_per_payload: 50_000,
            max_logs_per_payload: 200_000,
        }
    }
}

impl RawJsonFlashblocksLimits {
    fn validate(self) -> Result<(), RawJsonFlashblocksError> {
        if self.max_frame_bytes == 0 {
            return Err(RawJsonFlashblocksError::InvalidLimits(
                "max_frame_bytes must be greater than zero",
            ));
        }
        if self.max_flashblocks_per_payload == 0 {
            return Err(RawJsonFlashblocksError::InvalidLimits(
                "max_flashblocks_per_payload must be greater than zero",
            ));
        }
        if self.max_transactions_per_payload == 0 {
            return Err(RawJsonFlashblocksError::InvalidLimits(
                "max_transactions_per_payload must be greater than zero",
            ));
        }
        if self.max_logs_per_payload == 0 {
            return Err(RawJsonFlashblocksError::InvalidLimits(
                "max_logs_per_payload must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// One provider-provenanced cumulative preview plus the logs added by its
/// latest indexed delta.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct FlashblockSnapshot {
    /// Identity and cumulative transaction membership for the preview.
    pub flashblock: FlashblockRef,
    /// Structured logs added by this exact indexed delta.
    pub logs: Vec<Log>,
}

/// Why a speculative Flashblock payload generation was revoked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FlashblockInvalidationReason {
    /// The source skipped at least one index within a payload generation.
    IndexGap,
    /// A repeated index carried different content.
    ConflictingDuplicate,
    /// A generation began after index zero, so its base header was unavailable.
    MissingInitialIndex,
    /// The caller replaced or disconnected the externally managed source.
    SourceReset,
}

/// Observable fail-closed invalidation for one provider generation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct FlashblockInvalidation {
    /// Provider generation whose speculative payload was revoked.
    pub provider: ProviderRef,
    /// Payload identity that was active or could not be trusted.
    pub payload_id: FixedBytes<8>,
    /// Continuity or caller lifecycle transition that caused the revocation.
    pub reason: FlashblockInvalidationReason,
}

/// Standardized update accepted by the existing preconfirmation pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlashblockUpdate {
    /// A cumulative preview and its newly added structured logs.
    Snapshot(Box<FlashblockSnapshot>),
    /// The active speculative provider generation must be discarded.
    Invalidated(FlashblockInvalidation),
}

/// One normalized raw update paired with its caller-clock arrival.
///
/// The millisecond value belongs to the monotonic clock supplied to
/// [`BufferedRawJsonFlashblocksAdapter::ingest_json_timed_at`]. Applications
/// convert it back to their `Instant` domain before subscriber handoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimedFlashblockUpdate {
    update: FlashblockUpdate,
    source_ingress_millis: u64,
}

impl TimedFlashblockUpdate {
    const fn new(update: FlashblockUpdate, source_ingress_millis: u64) -> Self {
        Self {
            update,
            source_ingress_millis,
        }
    }

    /// Borrow the normalized update.
    pub const fn update(&self) -> &FlashblockUpdate {
        &self.update
    }

    /// Arrival in the caller-owned monotonic millisecond domain.
    pub const fn source_ingress_millis(&self) -> u64 {
        self.source_ingress_millis
    }

    /// Consume the timed value into its normalized update.
    pub fn into_update(self) -> FlashblockUpdate {
        self.update
    }
}

impl FlashblockUpdate {
    /// Provider generation carried by this standardized update.
    pub const fn provider(&self) -> &ProviderRef {
        match self {
            Self::Snapshot(snapshot) => &snapshot.flashblock.provider,
            Self::Invalidated(invalidation) => &invalidation.provider,
        }
    }
}

/// Failure to enqueue a standardized update into an attached subscriber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FlashblockUpdateChannelError {
    /// The update names a different configured endpoint.
    #[error("Flashblock update came from an unexpected provider endpoint")]
    UnexpectedEndpoint,
    /// A non-blocking send found the bounded channel at capacity.
    #[error("Flashblock update channel is full")]
    Full,
    /// The subscriber no longer owns the receiving half of the channel.
    #[error("Flashblock update channel is closed")]
    Closed,
    /// The subscriber consumed the update but rejected its integrity or local
    /// resource requirements. The application must revoke the source
    /// generation before continuing.
    #[error("Flashblock update was rejected by the subscriber")]
    Rejected,
}

/// Subscriber verdict for one non-blocking standardized update admission.
///
/// Awaiting this receipt distinguishes bounded-queue admission from actual
/// subscriber validation. Dropping it does not cancel the queued update.
#[derive(Debug)]
#[must_use = "await wait() to observe the subscriber validation verdict"]
pub struct FlashblockUpdateAcknowledgement {
    receiver: oneshot::Receiver<Result<(), FlashblockUpdateChannelError>>,
}

impl FlashblockUpdateAcknowledgement {
    /// Wait until the subscriber accepts or rejects the queued update.
    ///
    /// # Errors
    ///
    /// Returns [`FlashblockUpdateChannelError::Rejected`] after subscriber
    /// validation failure, or [`FlashblockUpdateChannelError::Closed`] when the
    /// subscriber shuts down before producing a verdict.
    pub async fn wait(self) -> Result<(), FlashblockUpdateChannelError> {
        self.receiver
            .await
            .unwrap_or(Err(FlashblockUpdateChannelError::Closed))
    }
}

pub(crate) struct QueuedFlashblockUpdate {
    pub(crate) update: FlashblockUpdate,
    pub(crate) timing: FlashblockIngressTiming,
    pub(crate) acknowledgement: oneshot::Sender<Result<(), FlashblockUpdateChannelError>>,
}

impl QueuedFlashblockUpdate {
    fn new(
        update: FlashblockUpdate,
        timing: FlashblockIngressTiming,
    ) -> (Self, FlashblockUpdateAcknowledgement) {
        let (acknowledgement, receiver) = oneshot::channel();
        (
            Self {
                update,
                timing,
                acknowledgement,
            },
            FlashblockUpdateAcknowledgement { receiver },
        )
    }
}

/// Cloneable application handle for a subscriber-owned, bounded update queue.
///
/// This handle performs no network I/O and implements no retry policy. The
/// application keeps it while the [`super::AlloySubscriber`] may be moved into
/// another runtime owner, and sends only updates produced by a compatible
/// adapter. Backpressure, reconnects, and source rotation remain application
/// responsibilities.
#[derive(Clone, Debug)]
pub struct FlashblockUpdateSender {
    provider: ProviderRef,
    sender: mpsc::Sender<QueuedFlashblockUpdate>,
}

impl FlashblockUpdateSender {
    pub(crate) const fn new(
        provider: ProviderRef,
        sender: mpsc::Sender<QueuedFlashblockUpdate>,
    ) -> Self {
        Self { provider, sender }
    }

    /// Configured endpoint and initial generation for this queue.
    pub const fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    /// Await bounded queue capacity, enqueue one standardized update, and wait
    /// for the subscriber's validation verdict.
    ///
    /// # Errors
    ///
    /// Returns [`FlashblockUpdateChannelError::UnexpectedEndpoint`] before
    /// enqueueing an update from another endpoint, or
    /// [`FlashblockUpdateChannelError::Closed`] after subscriber shutdown.
    /// [`FlashblockUpdateChannelError::Rejected`] means the subscriber consumed
    /// the update but rejected its integrity or local resource requirements;
    /// revoke and replace that source generation before continuing.
    pub async fn send(&self, update: FlashblockUpdate) -> Result<(), FlashblockUpdateChannelError> {
        self.send_with_ingress(update, FlashblockIngressTiming::new(Instant::now()))
            .await
    }

    /// Enqueue an update with its original process-local typed source arrival.
    ///
    /// # Errors
    ///
    /// Returns the same endpoint, closure, and subscriber-rejection errors as
    /// [`Self::send`].
    pub async fn send_with_ingress(
        &self,
        update: FlashblockUpdate,
        timing: FlashblockIngressTiming,
    ) -> Result<(), FlashblockUpdateChannelError> {
        self.validate_endpoint(&update)?;
        let (queued, acknowledgement) = QueuedFlashblockUpdate::new(update, timing);
        self.sender
            .send(queued)
            .await
            .map_err(|_| FlashblockUpdateChannelError::Closed)?;
        acknowledgement.wait().await
    }

    /// Enqueue one standardized update without waiting for capacity and return
    /// a receipt for the subscriber's eventual validation verdict.
    ///
    /// # Errors
    ///
    /// In addition to endpoint and closure errors, returns
    /// [`FlashblockUpdateChannelError::Full`] when the bounded queue has no
    /// immediate capacity. A successful return proves only queue admission;
    /// await [`FlashblockUpdateAcknowledgement::wait`] before treating the
    /// update as accepted. The caller decides whether to wait, invalidate its
    /// current generation, or reconnect the external source.
    pub fn try_send(
        &self,
        update: FlashblockUpdate,
    ) -> Result<FlashblockUpdateAcknowledgement, FlashblockUpdateChannelError> {
        self.try_send_with_ingress(update, FlashblockIngressTiming::new(Instant::now()))
    }

    /// Non-blockingly enqueue an update with its original typed source arrival.
    ///
    /// # Errors
    ///
    /// Returns the same endpoint, capacity, and closure errors as
    /// [`Self::try_send`].
    pub fn try_send_with_ingress(
        &self,
        update: FlashblockUpdate,
        timing: FlashblockIngressTiming,
    ) -> Result<FlashblockUpdateAcknowledgement, FlashblockUpdateChannelError> {
        self.validate_endpoint(&update)?;
        let (queued, acknowledgement) = QueuedFlashblockUpdate::new(update, timing);
        self.sender.try_send(queued).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => FlashblockUpdateChannelError::Full,
            mpsc::error::TrySendError::Closed(_) => FlashblockUpdateChannelError::Closed,
        })?;
        Ok(acknowledgement)
    }

    fn validate_endpoint(
        &self,
        update: &FlashblockUpdate,
    ) -> Result<(), FlashblockUpdateChannelError> {
        if update.provider().endpoint != self.provider.endpoint {
            return Err(FlashblockUpdateChannelError::UnexpectedEndpoint);
        }
        Ok(())
    }
}

pub(crate) fn flashblock_update_channel(
    provider: ProviderRef,
    capacity: usize,
) -> (
    FlashblockUpdateSender,
    mpsc::Receiver<QueuedFlashblockUpdate>,
) {
    let (sender, receiver) = mpsc::channel(capacity);
    (FlashblockUpdateSender::new(provider, sender), receiver)
}

/// A malformed, unsupported, or resource-exhausting raw JSON Flashblocks frame.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RawJsonFlashblocksError {
    /// Adapter bounds are unusable.
    #[error("invalid raw JSON Flashblocks limits: {0}")]
    InvalidLimits(&'static str),
    /// A caller attempted an ambiguous or regressing source transition.
    #[error("invalid raw JSON Flashblocks source transition: {0}")]
    InvalidSourceTransition(&'static str),
    /// The frame exceeded the configured byte bound.
    #[error("raw JSON Flashblocks frame exceeds the configured byte limit")]
    FrameTooLarge,
    /// The JSON document did not match the supported receipt-enriched profile.
    #[error("invalid raw JSON Flashblocks payload: {0}")]
    InvalidPayload(String),
    /// A configured per-payload resource bound was exceeded.
    #[error("raw JSON Flashblocks payload exceeds the configured {0} limit")]
    ResourceExhausted(&'static str),
}

/// Stateful converter for receipt-enriched, indexed JSON Flashblock payloads.
///
/// Compatibility is defined by the accepted wire profile, not by chain id:
/// `payload_id`, a monotonically increasing `index`, an index-zero `base` (or
/// `static`) header, transaction deltas in `diff.transactions`, and an exact
/// receipt map in `metadata.receipts`. Provider JSON-RPC subscription envelopes,
/// receipt-less payloads, and binary SSZ frames are separate wire profiles and
/// are not accepted by this adapter.
///
/// The adapter performs no I/O. The caller owns WebSocket control frames,
/// authentication, timeouts, retry, backoff, and provider rotation. On source
/// replacement or disconnect, call [`Self::reset`] and forward the returned
/// invalidation before accepting updates from the new provider generation.
#[derive(Clone, Debug)]
pub struct RawJsonFlashblocksAdapter {
    provider: ProviderRef,
    limits: RawJsonFlashblocksLimits,
    active: Option<RawPayloadState>,
    ignored_payload: Option<FixedBytes<8>>,
}

impl RawJsonFlashblocksAdapter {
    /// Construct an adapter with bounded production defaults.
    pub fn new(provider: ProviderRef) -> Self {
        Self {
            provider,
            limits: RawJsonFlashblocksLimits::default(),
            active: None,
            ignored_payload: None,
        }
    }

    /// Construct an adapter with explicit resource bounds.
    pub fn with_limits(
        provider: ProviderRef,
        limits: RawJsonFlashblocksLimits,
    ) -> Result<Self, RawJsonFlashblocksError> {
        limits.validate()?;
        Ok(Self {
            provider,
            limits,
            active: None,
            ignored_payload: None,
        })
    }

    /// Provider generation attached to newly normalized snapshots.
    pub const fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    /// Resource limits applied by this adapter.
    pub const fn limits(&self) -> RawJsonFlashblocksLimits {
        self.limits
    }

    /// Revoke the active payload and begin a caller-managed provider generation.
    ///
    /// This method never reconnects, sleeps, or performs I/O. `None` means the
    /// prior source had no active payload to revoke.
    ///
    /// # Errors
    ///
    /// Returns [`RawJsonFlashblocksError::InvalidSourceTransition`] when the
    /// endpoint identity changes or the generation does not increase. Rebuild
    /// the adapter and subscriber binding to select a different endpoint.
    pub fn reset(
        &mut self,
        provider: ProviderRef,
    ) -> Result<Option<FlashblockUpdate>, RawJsonFlashblocksError> {
        if provider.endpoint != self.provider.endpoint {
            return Err(RawJsonFlashblocksError::InvalidSourceTransition(
                "reset cannot change the configured endpoint identity",
            ));
        }
        if provider.generation <= self.provider.generation {
            return Err(RawJsonFlashblocksError::InvalidSourceTransition(
                "reset requires a strictly newer provider generation",
            ));
        }
        let invalidation = self.active.take().map(|active| {
            FlashblockUpdate::Invalidated(FlashblockInvalidation {
                provider: self.provider.clone(),
                payload_id: active.payload_id,
                reason: FlashblockInvalidationReason::SourceReset,
            })
        });
        self.provider = provider;
        self.ignored_payload = None;
        Ok(invalidation)
    }

    /// Decode and normalize one raw JSON application-data frame.
    ///
    /// `Ok(None)` denotes an identical duplicate or a later delta from a
    /// generation already invalidated for continuity loss. `Err` leaves the
    /// last successfully published adapter state unchanged. A live caller that
    /// cannot prove an application-data error irrelevant must call
    /// [`Self::reset`] with a newer generation and forward the returned
    /// invalidation before accepting more frames.
    pub fn ingest_json(
        &mut self,
        frame: &[u8],
    ) -> Result<Option<FlashblockUpdate>, RawJsonFlashblocksError> {
        let payload = self.decode_json(frame)?;
        self.ingest(payload)
    }

    fn decode_json(&self, frame: &[u8]) -> Result<RawFlashblockPayload, RawJsonFlashblocksError> {
        if frame.len() > self.limits.max_frame_bytes {
            return Err(RawJsonFlashblocksError::FrameTooLarge);
        }
        let payload: RawFlashblockPayload = serde_json::from_slice(frame)
            .map_err(|error| RawJsonFlashblocksError::InvalidPayload(error.to_string()))?;
        self.validate_index(payload.index)?;
        Ok(payload)
    }

    fn validate_index(&self, index: u64) -> Result<(), RawJsonFlashblocksError> {
        if usize::try_from(index)
            .ok()
            .is_none_or(|index| index >= self.limits.max_flashblocks_per_payload)
        {
            return Err(RawJsonFlashblocksError::ResourceExhausted(
                "Flashblock index",
            ));
        }
        Ok(())
    }

    fn ingest(
        &mut self,
        payload: RawFlashblockPayload,
    ) -> Result<Option<FlashblockUpdate>, RawJsonFlashblocksError> {
        self.validate_index(payload.index)?;
        if self.ignored_payload == Some(payload.payload_id) {
            return Ok(None);
        }

        let begins_new_payload = self
            .active
            .as_ref()
            .is_none_or(|active| active.payload_id != payload.payload_id);
        if begins_new_payload {
            if payload.index != 0 {
                let invalidated_payload = self
                    .active
                    .take()
                    .map_or(payload.payload_id, |active| active.payload_id);
                self.ignored_payload = Some(payload.payload_id);
                return Ok(Some(self.invalidation(
                    invalidated_payload,
                    FlashblockInvalidationReason::MissingInitialIndex,
                )));
            }
            let base = payload.base.clone().ok_or_else(|| {
                RawJsonFlashblocksError::InvalidPayload("index zero omitted its base header".into())
            })?;
            if let Some(metadata_number) = payload.metadata.block_number
                && metadata_number != base.block_number
            {
                return Err(RawJsonFlashblocksError::InvalidPayload(
                    "base and metadata block numbers disagree".into(),
                ));
            }
            let previous_active = self.active.take();
            let previous_ignored_payload = self.ignored_payload.take();
            self.active = Some(RawPayloadState {
                payload_id: payload.payload_id,
                base,
                last_index: None,
                cumulative_transactions: Vec::new(),
                transaction_set: HashSet::new(),
                next_log_index: 0,
                cumulative_logs: 0,
                index_commitments: HashMap::new(),
            });
            let result = self.ingest_active(payload);
            if result.is_err() {
                self.active = previous_active;
                self.ignored_payload = previous_ignored_payload;
            }
            return result;
        }

        self.ingest_active(payload)
    }

    fn ingest_active(
        &mut self,
        payload: RawFlashblockPayload,
    ) -> Result<Option<FlashblockUpdate>, RawJsonFlashblocksError> {
        let active = self.active.as_mut().expect("new payload initialized above");
        if payload
            .metadata
            .block_number
            .is_some_and(|number| number != active.base.block_number)
            || payload
                .base
                .as_ref()
                .is_some_and(|base| base != &active.base)
        {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "base header or metadata block numbers disagree with the active payload".into(),
            ));
        }
        let delta_transactions = flashblock_transaction_hashes(&payload.diff.transactions)
            .map_err(|error| RawJsonFlashblocksError::InvalidPayload(error.to_string()))?;
        let receipt_hashes = payload
            .metadata
            .receipts
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        let transaction_hashes = delta_transactions.iter().copied().collect::<HashSet<_>>();
        if transaction_hashes.len() != delta_transactions.len() {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "the transaction delta contains a duplicate hash".into(),
            ));
        }
        if receipt_hashes != transaction_hashes {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "receipt-map membership disagrees with the transaction delta".into(),
            ));
        }
        let commitment = raw_payload_commitment(&payload, &delta_transactions);
        if let Some(previous) = active.index_commitments.get(&payload.index) {
            if *previous == commitment {
                return Ok(None);
            }
            let payload_id = active.payload_id;
            self.active = None;
            self.ignored_payload = Some(payload_id);
            return Ok(Some(self.invalidation(
                payload_id,
                FlashblockInvalidationReason::ConflictingDuplicate,
            )));
        }
        let expected_index = active.last_index.map_or(0, |index| index.saturating_add(1));
        if payload.index != expected_index {
            let payload_id = active.payload_id;
            self.active = None;
            self.ignored_payload = Some(payload_id);
            return Ok(Some(
                self.invalidation(payload_id, FlashblockInvalidationReason::IndexGap),
            ));
        }

        if active
            .cumulative_transactions
            .len()
            .saturating_add(delta_transactions.len())
            > self.limits.max_transactions_per_payload
        {
            return Err(RawJsonFlashblocksError::ResourceExhausted(
                "transaction count",
            ));
        }
        if delta_transactions
            .iter()
            .any(|hash| active.transaction_set.contains(hash))
        {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "a transaction appeared in more than one indexed delta".into(),
            ));
        }

        let transaction_offset = active.cumulative_transactions.len();
        let mut logs = Vec::new();
        for (delta_index, transaction_hash) in delta_transactions.iter().enumerate() {
            let receipt = payload
                .metadata
                .receipts
                .get(transaction_hash)
                .expect("receipt membership checked above");
            let transaction_index =
                u64::try_from(transaction_offset.saturating_add(delta_index))
                    .map_err(|_| RawJsonFlashblocksError::ResourceExhausted("transaction index"))?;
            for raw_log in &receipt.logs {
                if active.cumulative_logs.saturating_add(logs.len())
                    >= self.limits.max_logs_per_payload
                {
                    return Err(RawJsonFlashblocksError::ResourceExhausted("log count"));
                }
                let inner = PrimitiveLog::new(
                    raw_log.address,
                    raw_log.topics.clone(),
                    raw_log.data.clone(),
                )
                .ok_or_else(|| {
                    RawJsonFlashblocksError::InvalidPayload(
                        "receipt log contains more than four topics".into(),
                    )
                })?;
                let log_index = active
                    .next_log_index
                    .checked_add(
                        u64::try_from(logs.len())
                            .map_err(|_| RawJsonFlashblocksError::ResourceExhausted("log index"))?,
                    )
                    .ok_or(RawJsonFlashblocksError::ResourceExhausted("log index"))?;
                logs.push(Log {
                    inner,
                    block_hash: None,
                    block_number: Some(active.base.block_number),
                    block_timestamp: Some(active.base.timestamp),
                    transaction_hash: Some(*transaction_hash),
                    transaction_index: Some(transaction_index),
                    log_index: Some(log_index),
                    removed: false,
                });
            }
        }

        let mut cumulative_transactions = active.cumulative_transactions.clone();
        cumulative_transactions.extend(delta_transactions.iter().copied());
        let partial_block_hash = non_placeholder_hash(payload.diff.block_hash);
        let state_root = non_placeholder_hash(payload.diff.state_root);
        let transactions_root = payload
            .diff
            .transactions_root
            .and_then(non_placeholder_hash);
        let parent_hash = non_placeholder_hash(active.base.parent_hash);
        let prevrandao = active.base.prevrandao.and_then(non_placeholder_hash);
        let content_hash = flashblock_content_hash(FlashblockContentCommitment {
            provider: &self.provider,
            payload_id: Some(payload.payload_id),
            index: Some(payload.index),
            block_number: active.base.block_number,
            partial_block_hash,
            parent_hash,
            state_root,
            transactions_root,
            transaction_hashes: &cumulative_transactions,
            timestamp: Some(active.base.timestamp),
            base_fee_per_gas: active.base.base_fee_per_gas,
            beneficiary: active.base.beneficiary,
            prevrandao,
            gas_limit: active.base.gas_limit,
        });
        for log in &mut logs {
            log.block_hash = Some(content_hash);
        }
        let flashblock = FlashblockRef {
            provider: self.provider.clone(),
            payload_id: Some(payload.payload_id),
            index: Some(payload.index),
            block_number: active.base.block_number,
            content_hash,
            partial_block_hash,
            parent_hash,
            state_root,
            transactions_root,
            transaction_hashes: cumulative_transactions.clone(),
            timestamp: Some(active.base.timestamp),
            base_fee_per_gas: active.base.base_fee_per_gas,
            beneficiary: active.base.beneficiary,
            prevrandao,
            gas_limit: active.base.gas_limit,
        };
        let next_log_index = active
            .next_log_index
            .checked_add(
                u64::try_from(logs.len())
                    .map_err(|_| RawJsonFlashblocksError::ResourceExhausted("log index"))?,
            )
            .ok_or(RawJsonFlashblocksError::ResourceExhausted("log index"))?;
        active.transaction_set.extend(delta_transactions);
        active.cumulative_transactions = cumulative_transactions;
        active.last_index = Some(payload.index);
        active.index_commitments.insert(payload.index, commitment);
        active.next_log_index = next_log_index;
        active.cumulative_logs = active.cumulative_logs.saturating_add(logs.len());

        Ok(Some(FlashblockUpdate::Snapshot(Box::new(
            FlashblockSnapshot { flashblock, logs },
        ))))
    }

    fn invalidation(
        &self,
        payload_id: FixedBytes<8>,
        reason: FlashblockInvalidationReason,
    ) -> FlashblockUpdate {
        FlashblockUpdate::Invalidated(FlashblockInvalidation {
            provider: self.provider.clone(),
            payload_id,
            reason,
        })
    }

    fn invalidate_active(
        &mut self,
        payload_id: FixedBytes<8>,
        reason: FlashblockInvalidationReason,
    ) -> FlashblockUpdate {
        self.active = None;
        self.ignored_payload = Some(payload_id);
        self.invalidation(payload_id, reason)
    }
}

const MIN_BUFFERED_GAP_MILLIS: u64 = 300;
const MAX_BUFFERED_GAP_MILLIS: u64 = 500;

/// Provider-free adapter that tolerates one briefly reordered JSON Flashblock.
///
/// This wrapper preserves [`RawJsonFlashblocksAdapter`]'s immediate behavior
/// except for one narrow case: when an active payload receives exactly
/// `expected_index + 1`, it retains that one parsed frame until the missing
/// index arrives or the caller-owned deadline expires. It performs no I/O,
/// starts no timer, mutates no canonical state, and grants no execution or
/// trigger authority. Applications must schedule their own timer from
/// [`Self::buffered_gap`] and call [`Self::expire_gap_at`].
#[derive(Clone, Debug)]
pub struct BufferedRawJsonFlashblocksAdapter {
    inner: RawJsonFlashblocksAdapter,
    gap_timeout_millis: u64,
    buffered: Option<BufferedRawFlashblock>,
}

#[derive(Clone, Debug)]
struct BufferedRawFlashblock {
    payload: RawFlashblockPayload,
    commitment: B256,
    expected_index: u64,
    source_ingress_millis: u64,
    expires_at_millis: u64,
}

impl BufferedRawJsonFlashblocksAdapter {
    /// Construct a bounded one-frame reorder adapter.
    ///
    /// `gap_timeout_millis` must be in the reviewed inclusive range
    /// `300..=500`. The caller supplies timestamps from one monotonic clock.
    ///
    /// # Errors
    ///
    /// Returns [`RawJsonFlashblocksError::InvalidLimits`] for an unreviewed gap
    /// timeout or invalid raw-frame resource limits.
    pub fn new(
        provider: ProviderRef,
        limits: RawJsonFlashblocksLimits,
        gap_timeout_millis: u64,
    ) -> Result<Self, RawJsonFlashblocksError> {
        if !(MIN_BUFFERED_GAP_MILLIS..=MAX_BUFFERED_GAP_MILLIS).contains(&gap_timeout_millis) {
            return Err(RawJsonFlashblocksError::InvalidLimits(
                "buffered gap timeout must be between 300 and 500 milliseconds",
            ));
        }
        Ok(Self {
            inner: RawJsonFlashblocksAdapter::with_limits(provider, limits)?,
            gap_timeout_millis,
            buffered: None,
        })
    }

    /// Provider generation attached to normalized snapshots and invalidations.
    pub const fn provider(&self) -> &ProviderRef {
        self.inner.provider()
    }

    /// Resource limits applied to immediate and buffered frames.
    pub const fn limits(&self) -> RawJsonFlashblocksLimits {
        self.inner.limits()
    }

    /// Return `(missing_index, buffered_index, expires_at_millis)` when a
    /// caller-owned gap timer is required.
    pub fn buffered_gap(&self) -> Option<(u64, u64, u64)> {
        self.buffered.as_ref().map(|buffered| {
            (
                buffered.expected_index,
                buffered.payload.index,
                buffered.expires_at_millis,
            )
        })
    }

    /// Decode one application-data frame at a caller-supplied monotonic time.
    ///
    /// The returned vector has at most two entries. Two snapshots are returned
    /// only when the missing index and the retained next index are validated
    /// atomically and drained in order. Errors preserve the last successfully
    /// published state and any pending gap for explicit caller reset.
    ///
    /// # Errors
    ///
    /// Returns [`RawJsonFlashblocksError`] immediately for malformed,
    /// unsupported, or resource-exhausting input.
    pub fn ingest_json_at(
        &mut self,
        frame: &[u8],
        now_millis: u64,
    ) -> Result<Vec<FlashblockUpdate>, RawJsonFlashblocksError> {
        self.ingest_json_timed_at(frame, now_millis).map(|updates| {
            updates
                .into_iter()
                .map(TimedFlashblockUpdate::into_update)
                .collect()
        })
    }

    /// Decode one application-data frame while retaining the original
    /// caller-clock arrival for every emitted update.
    ///
    /// When a future frame is buffered across a one-index gap, its eventual
    /// output retains the timestamp from the call that first supplied that
    /// frame, not the later gap-closing call. This method is provider-free and
    /// starts no timer.
    ///
    /// # Errors
    ///
    /// Returns [`RawJsonFlashblocksError`] under the same conditions as
    /// [`Self::ingest_json_at`].
    pub fn ingest_json_timed_at(
        &mut self,
        frame: &[u8],
        now_millis: u64,
    ) -> Result<Vec<TimedFlashblockUpdate>, RawJsonFlashblocksError> {
        let payload = self.inner.decode_json(frame)?;
        let mut updates = Vec::with_capacity(2);

        if self
            .buffered
            .as_ref()
            .is_some_and(|buffered| now_millis >= buffered.expires_at_millis)
        {
            let expired = self.buffered.as_ref().expect("checked above");
            if payload.payload_id == expired.payload.payload_id {
                let payload_id = expired.payload.payload_id;
                self.buffered = None;
                updates.push(TimedFlashblockUpdate::new(
                    self.inner
                        .invalidate_active(payload_id, FlashblockInvalidationReason::IndexGap),
                    now_millis,
                ));
                return Ok(updates);
            }

            // Validate the replacement on a clone before publishing the expiry.
            // An application-data error therefore preserves the observable
            // invalidation and the pending timer for an explicit retry/reset.
            let payload_id = expired.payload.payload_id;
            let invalidation = self
                .inner
                .invalidation(payload_id, FlashblockInvalidationReason::IndexGap);
            let mut staged = self.inner.clone();
            let replacement = staged.ingest(payload)?;
            self.inner = staged;
            self.buffered = None;
            updates.push(TimedFlashblockUpdate::new(invalidation, now_millis));
            if let Some(update) = replacement {
                updates.push(TimedFlashblockUpdate::new(update, now_millis));
            }
            return Ok(updates);
        }

        if let Some(buffered) = self.buffered.as_ref() {
            if payload.payload_id == buffered.payload.payload_id {
                if payload.index == buffered.expected_index {
                    let buffered = self.buffered.as_ref().expect("checked above").clone();
                    let mut staged = self.inner.clone();
                    if let Some(update) = staged.ingest(payload)? {
                        updates.push(TimedFlashblockUpdate::new(update, now_millis));
                    }
                    if let Some(update) = staged.ingest(buffered.payload)? {
                        updates.push(TimedFlashblockUpdate::new(
                            update,
                            buffered.source_ingress_millis,
                        ));
                    }
                    self.inner = staged;
                    self.buffered = None;
                    return Ok(updates);
                }

                if payload.index == buffered.payload.index {
                    let commitment = self.validate_bufferable_payload(&payload)?;
                    if commitment == buffered.commitment {
                        return Ok(updates);
                    }
                    let payload_id = payload.payload_id;
                    self.buffered = None;
                    updates.push(TimedFlashblockUpdate::new(
                        self.inner.invalidate_active(
                            payload_id,
                            FlashblockInvalidationReason::ConflictingDuplicate,
                        ),
                        now_millis,
                    ));
                    return Ok(updates);
                }

                if payload.index > buffered.payload.index {
                    self.validate_bufferable_payload(&payload)?;
                    let payload_id = payload.payload_id;
                    self.buffered = None;
                    updates.push(TimedFlashblockUpdate::new(
                        self.inner
                            .invalidate_active(payload_id, FlashblockInvalidationReason::IndexGap),
                        now_millis,
                    ));
                    return Ok(updates);
                }
            } else {
                // A rejected replacement must preserve the unresolved buffer.
                let mut staged = self.inner.clone();
                let replacement = staged.ingest(payload)?;
                self.inner = staged;
                self.buffered = None;
                if let Some(update) = replacement {
                    updates.push(TimedFlashblockUpdate::new(update, now_millis));
                }
                return Ok(updates);
            }
        }

        if self.can_buffer_one_gap(&payload) {
            let commitment = self.validate_bufferable_payload(&payload)?;
            let active = self
                .inner
                .active
                .as_ref()
                .expect("buffering requires active state");
            let expected_index = active.last_index.map_or(0, |index| index.saturating_add(1));
            self.buffered = Some(BufferedRawFlashblock {
                payload,
                commitment,
                expected_index,
                source_ingress_millis: now_millis,
                expires_at_millis: now_millis.saturating_add(self.gap_timeout_millis),
            });
            return Ok(updates);
        }

        if self.is_more_than_one_index_ahead(&payload) {
            self.validate_bufferable_payload(&payload)?;
        }

        if let Some(update) = self.inner.ingest(payload)? {
            if matches!(update, FlashblockUpdate::Invalidated(_)) {
                self.buffered = None;
            }
            updates.push(TimedFlashblockUpdate::new(update, now_millis));
        }
        Ok(updates)
    }

    /// Expire a buffered gap using the same caller-owned monotonic clock.
    ///
    /// At or after the deadline this emits one typed `IndexGap` invalidation,
    /// clears the retained frame, and ignores the late remainder of that
    /// payload until a new payload begins.
    pub fn expire_gap_at(&mut self, now_millis: u64) -> Option<FlashblockUpdate> {
        let expired = self
            .buffered
            .as_ref()
            .is_some_and(|buffered| now_millis >= buffered.expires_at_millis);
        if !expired {
            return None;
        }
        let buffered = self.buffered.take().expect("checked above");
        Some(self.inner.invalidate_active(
            buffered.payload.payload_id,
            FlashblockInvalidationReason::IndexGap,
        ))
    }

    /// Revoke the active payload and clear any retained reorder frame.
    ///
    /// A rejected source transition preserves both the active state and buffer.
    ///
    /// # Errors
    ///
    /// Returns [`RawJsonFlashblocksError::InvalidSourceTransition`] under the
    /// same conditions as [`RawJsonFlashblocksAdapter::reset`].
    pub fn reset(
        &mut self,
        provider: ProviderRef,
    ) -> Result<Option<FlashblockUpdate>, RawJsonFlashblocksError> {
        let invalidation = self.inner.reset(provider)?;
        self.buffered = None;
        Ok(invalidation)
    }

    fn can_buffer_one_gap(&self, payload: &RawFlashblockPayload) -> bool {
        let Some(active) = self.inner.active.as_ref() else {
            return false;
        };
        if active.payload_id != payload.payload_id {
            return false;
        }
        let expected = active.last_index.map_or(0, |index| index.saturating_add(1));
        payload.index == expected.saturating_add(1)
    }

    fn is_more_than_one_index_ahead(&self, payload: &RawFlashblockPayload) -> bool {
        let Some(active) = self.inner.active.as_ref() else {
            return false;
        };
        if active.payload_id != payload.payload_id {
            return false;
        }
        let expected = active.last_index.map_or(0, |index| index.saturating_add(1));
        payload.index > expected.saturating_add(1)
    }

    fn validate_bufferable_payload(
        &self,
        payload: &RawFlashblockPayload,
    ) -> Result<B256, RawJsonFlashblocksError> {
        let active = self
            .inner
            .active
            .as_ref()
            .expect("buffer validation requires active state");
        if payload
            .metadata
            .block_number
            .is_some_and(|number| number != active.base.block_number)
            || payload
                .base
                .as_ref()
                .is_some_and(|base| base != &active.base)
        {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "base header or metadata block numbers disagree with the active payload".into(),
            ));
        }
        let transaction_hashes = flashblock_transaction_hashes(&payload.diff.transactions)
            .map_err(|error| RawJsonFlashblocksError::InvalidPayload(error.to_string()))?;
        let unique_transactions = transaction_hashes.iter().copied().collect::<HashSet<_>>();
        if unique_transactions.len() != transaction_hashes.len() {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "the transaction delta contains a duplicate hash".into(),
            ));
        }
        let receipt_hashes = payload
            .metadata
            .receipts
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        if receipt_hashes != unique_transactions {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "receipt-map membership disagrees with the transaction delta".into(),
            ));
        }
        if active
            .cumulative_transactions
            .len()
            .saturating_add(transaction_hashes.len())
            > self.inner.limits.max_transactions_per_payload
        {
            return Err(RawJsonFlashblocksError::ResourceExhausted(
                "transaction count",
            ));
        }
        if transaction_hashes
            .iter()
            .any(|hash| active.transaction_set.contains(hash))
        {
            return Err(RawJsonFlashblocksError::InvalidPayload(
                "a transaction appeared in more than one indexed delta".into(),
            ));
        }
        let log_count =
            payload
                .metadata
                .receipts
                .values()
                .try_fold(0_usize, |count, receipt| {
                    for log in &receipt.logs {
                        if log.topics.len() > 4 {
                            return Err(RawJsonFlashblocksError::InvalidPayload(
                                "receipt log contains more than four topics".into(),
                            ));
                        }
                    }
                    count
                        .checked_add(receipt.logs.len())
                        .ok_or(RawJsonFlashblocksError::ResourceExhausted("log count"))
                })?;
        if active.cumulative_logs.saturating_add(log_count) > self.inner.limits.max_logs_per_payload
        {
            return Err(RawJsonFlashblocksError::ResourceExhausted("log count"));
        }
        u64::try_from(
            active
                .cumulative_transactions
                .len()
                .saturating_add(transaction_hashes.len()),
        )
        .map_err(|_| RawJsonFlashblocksError::ResourceExhausted("transaction index"))?;
        active
            .next_log_index
            .checked_add(
                u64::try_from(log_count)
                    .map_err(|_| RawJsonFlashblocksError::ResourceExhausted("log index"))?,
            )
            .ok_or(RawJsonFlashblocksError::ResourceExhausted("log index"))?;
        Ok(raw_payload_commitment(payload, &transaction_hashes))
    }
}

fn raw_payload_commitment(payload: &RawFlashblockPayload, transaction_hashes: &[B256]) -> B256 {
    let mut commitment = Keccak256::new();
    commitment.update(b"evm-fork-cache/raw-json-flashblock/v1");
    commitment.update(payload.payload_id.as_slice());
    commitment.update(payload.index.to_be_bytes());
    match payload.base.as_ref() {
        Some(base) => {
            commitment.update([1]);
            commitment.update(base.parent_hash.as_slice());
            commitment.update(base.block_number.to_be_bytes());
            commitment.update(base.timestamp.to_be_bytes());
            commit_optional_raw_u64(&mut commitment, base.gas_limit);
            commit_optional_raw_u64(&mut commitment, base.base_fee_per_gas);
            commit_optional_raw_bytes(
                &mut commitment,
                base.beneficiary.as_ref().map(|address| address.as_slice()),
            );
            commit_optional_raw_bytes(
                &mut commitment,
                base.prevrandao.as_ref().map(B256::as_slice),
            );
        }
        None => commitment.update([0]),
    }
    commitment.update(payload.diff.state_root.as_slice());
    commitment.update(payload.diff.block_hash.as_slice());
    commit_optional_raw_bytes(
        &mut commitment,
        payload.diff.transactions_root.as_ref().map(B256::as_slice),
    );
    commit_optional_raw_u64(&mut commitment, payload.metadata.block_number);
    commitment.update((transaction_hashes.len() as u64).to_be_bytes());
    for transaction_hash in transaction_hashes {
        commitment.update(transaction_hash.as_slice());
        let receipt = payload
            .metadata
            .receipts
            .get(transaction_hash)
            .expect("receipt membership validated before commitment");
        commitment.update((receipt.logs.len() as u64).to_be_bytes());
        for log in &receipt.logs {
            commitment.update(log.address.as_slice());
            commitment.update((log.topics.len() as u64).to_be_bytes());
            for topic in &log.topics {
                commitment.update(topic.as_slice());
            }
            commitment.update((log.data.len() as u64).to_be_bytes());
            commitment.update(log.data.as_ref());
        }
    }
    commitment.finalize()
}

fn commit_optional_raw_u64(commitment: &mut Keccak256, value: Option<u64>) {
    match value {
        Some(value) => {
            commitment.update([1]);
            commitment.update(value.to_be_bytes());
        }
        None => commitment.update([0]),
    }
}

fn commit_optional_raw_bytes(commitment: &mut Keccak256, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            commitment.update([1]);
            commitment.update((value.len() as u64).to_be_bytes());
            commitment.update(value);
        }
        None => commitment.update([0]),
    }
}

#[derive(Clone, Debug)]
struct RawPayloadState {
    payload_id: FixedBytes<8>,
    base: BaseFlashblockBase,
    last_index: Option<u64>,
    cumulative_transactions: Vec<B256>,
    transaction_set: HashSet<B256>,
    next_log_index: u64,
    cumulative_logs: usize,
    index_commitments: HashMap<u64, B256>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct RawFlashblockPayload {
    payload_id: FixedBytes<8>,
    index: u64,
    #[serde(default, alias = "static")]
    base: Option<BaseFlashblockBase>,
    diff: RawFlashblockDiff,
    metadata: RawFlashblockMetadata,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct RawFlashblockDiff {
    state_root: B256,
    block_hash: B256,
    #[serde(default)]
    transactions: Vec<serde_json::Value>,
    #[serde(default)]
    transactions_root: Option<B256>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct RawFlashblockMetadata {
    #[serde(default, deserialize_with = "deserialize_optional_rpc_u64")]
    block_number: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_receipts")]
    receipts: HashMap<B256, RawTransactionReceipt>,
}

fn deserialize_receipts<'de, D>(
    deserializer: D,
) -> Result<HashMap<B256, RawTransactionReceipt>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct ReceiptsVisitor;

    impl<'de> serde::de::Visitor<'de> for ReceiptsVisitor {
        type Value = HashMap<B256, RawTransactionReceipt>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a receipt map with unique transaction-hash keys")
        }

        fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut receipts = HashMap::with_capacity(entries.size_hint().unwrap_or_default());
            while let Some((transaction_hash, receipt)) = entries.next_entry()? {
                if receipts.insert(transaction_hash, receipt).is_some() {
                    return Err(serde::de::Error::custom("duplicate receipt key"));
                }
            }
            Ok(receipts)
        }
    }

    deserializer.deserialize_map(ReceiptsVisitor)
}

#[derive(Clone, Debug, serde::Deserialize)]
struct RawTransactionReceipt {
    #[serde(default)]
    logs: Vec<RawReceiptLog>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct RawReceiptLog {
    address: Address,
    #[serde(default)]
    topics: Vec<B256>,
    data: Bytes,
}
