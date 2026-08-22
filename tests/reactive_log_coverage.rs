//! Provider-neutral validation of the log-coverage attestation.
//!
//! [`ChainControl::LogCoverage`] is a *negative* guarantee — no log-notification
//! loss went unhealed at or below the named block — and its value depends
//! entirely on being unforgeable. A consumer that trusts it stops re-fetching
//! logs, so every way the watermark could overstate what a source proved has to
//! be rejected: it must not regress, must not outrun canonical coverage, must
//! name a block whose identity agrees with retained history, and must never be
//! inferred from silence.
#![cfg(feature = "reactive")]

mod common;

use alloy_network::Ethereum;
use alloy_primitives::B256;
use anyhow::Result;
use common::setup_cache;
use evm_fork_cache::reactive::{
    BlockRef, CanonicalSequenceMutation, CanonicalSequenceState, ChainControl, ChainStatus,
    InputSource, ReactiveConfig, ReactiveContext, ReactiveError, ReactiveInput, ReactiveInputBatch,
    ReactiveInputRecord, ReactiveRuntime, validate_canonical_sequence,
    validate_canonical_sequence_diagnostic,
};

fn block(number: u64, hash: B256, parent_hash: B256) -> BlockRef {
    BlockRef {
        number,
        hash,
        parent_hash: Some(parent_hash),
        timestamp: Some(1_700_000_000 + number),
    }
}

/// Canonical history through block 3, with no attestation yet.
fn state() -> (CanonicalSequenceState, BlockRef, BlockRef) {
    let parent = block(2, B256::repeat_byte(0x02), B256::repeat_byte(0x01));
    let head = block(3, B256::repeat_byte(0x03), B256::repeat_byte(0x02));
    (
        CanonicalSequenceState::new(vec![parent, head], Some(head), None, None),
        parent,
        head,
    )
}

fn controls(controls: impl IntoIterator<Item = ChainControl>) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls(controls)
}

/// An absent watermark is *unknown*, never "complete at genesis". A consumer
/// that read `None` as an attestation would stop re-fetching on the strength of
/// a promise nobody made.
#[test]
fn coverage_defaults_to_unknown_and_is_only_ever_set_explicitly() {
    let (state, _, head) = state();
    assert_eq!(state.log_coverage_head(), None);

    let seeded = state.clone().with_log_coverage_head(Some(head));
    assert_eq!(seeded.log_coverage_head(), Some(&head));

    // Seeding is total: an explicit `None` clears rather than preserves.
    assert_eq!(
        seeded.with_log_coverage_head(None).log_coverage_head(),
        None
    );
}

/// The attestation advances its own watermark and stages a replayable mutation,
/// while making no claim about chain progress: canonical coverage is untouched.
#[test]
fn attestation_advances_only_the_log_watermark() {
    let (state, _, head) = state();

    let validation =
        validate_canonical_sequence(&state, &controls([ChainControl::LogCoverage(head)]))
            .expect("attesting the canonical head is valid");

    assert_eq!(validation.next_state().log_coverage_head(), Some(&head));
    assert_eq!(
        validation.next_state().coverage_head(),
        Some(&head),
        "canonical coverage must be unchanged by an attestation"
    );
    assert!(
        validation.mutations().iter().any(
            |mutation| matches!(mutation, CanonicalSequenceMutation::LogCoverage(b) if *b == head)
        ),
        "the watermark must be replayable by an external checkpoint owner"
    );
    assert!(
        !validation
            .mutations()
            .iter()
            .any(|mutation| matches!(mutation, CanonicalSequenceMutation::Canonical(_))),
        "an attestation must not stage canonical progress"
    );
}

/// Re-sending the same watermark is harmless; retreating is not. A consumer that
/// had already narrowed a window must never see it widen.
#[test]
fn attestation_may_repeat_but_never_retreat() {
    let (state, parent, head) = state();
    let attested = state.with_log_coverage_head(Some(head));

    validate_canonical_sequence(&attested, &controls([ChainControl::LogCoverage(head)]))
        .expect("re-attesting the same block is idempotent");

    let error = validate_canonical_sequence_diagnostic(
        &attested,
        &controls([ChainControl::LogCoverage(parent)]),
    )
    .expect_err("a regressing watermark must be rejected");
    assert!(
        format!("{error:?}").contains("log coverage"),
        "the error must name the rejected watermark: {error:?}"
    );
}

/// Logs cannot be attested complete for a block whose canonical identity the
/// source has not established. Allowing it would let a source vouch for blocks
/// it has never seen.
#[test]
fn attestation_cannot_outrun_canonical_coverage() {
    let (state, _, head) = state();
    let ahead = block(
        head.number + 1,
        B256::repeat_byte(0x04),
        B256::repeat_byte(0x03),
    );

    assert!(matches!(
        validate_canonical_sequence(&state, &controls([ChainControl::LogCoverage(ahead)])),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

/// A watermark naming a block at a retained height but with a different hash is
/// attesting a branch this state never had.
#[test]
fn attestation_identity_must_agree_with_retained_history() {
    let (state, _, head) = state();
    let impostor = block(
        head.number,
        B256::repeat_byte(0xee),
        B256::repeat_byte(0x02),
    );

    assert!(matches!(
        validate_canonical_sequence(&state, &controls([ChainControl::LogCoverage(impostor)])),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

/// Attesting an older retained block is legitimate — a source may be behind —
/// and must round-trip without disturbing canonical coverage.
#[test]
fn attesting_a_retained_ancestor_is_valid_and_lags_behind_coverage() {
    let (state, parent, head) = state();

    let validation =
        validate_canonical_sequence(&state, &controls([ChainControl::LogCoverage(parent)]))
            .expect("a lagging attestation is valid");

    assert_eq!(validation.next_state().log_coverage_head(), Some(&parent));
    assert_eq!(validation.next_state().coverage_head(), Some(&head));
}

/// The watermark is carried on the validation snapshot, so an external
/// checkpoint owner can persist and restore it alongside the canonical heads.
#[test]
fn watermark_survives_a_state_snapshot_round_trip() {
    let (state, _, head) = state();
    let attested =
        validate_canonical_sequence(&state, &controls([ChainControl::LogCoverage(head)]))
            .expect("valid attestation")
            .next_state()
            .clone();

    let restored = CanonicalSequenceState::new(
        attested.retained_canonical_history().to_vec(),
        attested.coverage_head().copied(),
        attested.safe_head().copied(),
        attested.finalized_head().copied(),
    )
    .with_log_coverage_head(attested.log_coverage_head().copied());

    assert_eq!(restored.log_coverage_head(), Some(&head));
    restored.validate().expect("restored state is coherent");
}

/// A canonical header record so the runtime establishes coverage before the
/// attestation that references it.
fn header_record(point: BlockRef) -> ReactiveInputRecord<Ethereum> {
    ReactiveInputRecord::new(
        ReactiveInput::BlockHeader(alloy_rpc_types_eth::Header {
            hash: point.hash,
            inner: alloy_consensus::Header {
                number: point.number,
                parent_hash: point.parent_hash.unwrap_or_default(),
                timestamp: point.timestamp.unwrap_or_default(),
                ..alloy_consensus::Header::default()
            },
            total_difficulty: None,
            size: None,
        }),
        ReactiveContext {
            chain_id: None,
            source: InputSource::Subscription,
            chain_status: ChainStatus::Included {
                block: point,
                confirmations: 0,
            },
            block: Some(point),
            transaction_index: None,
            log_index: None,
        },
    )
}

/// End-to-end through the runtime: this is the value a consumer reads to decide
/// whether it may treat its buffered log set as authoritative, so it must be
/// absent until attested and must not be confused with canonical progress.
#[tokio::test]
async fn runtime_exposes_the_attested_watermark_only_once_attested() -> Result<()> {
    let head = block(41, B256::repeat_byte(0x41), B256::repeat_byte(0x40));
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    // Canonical progress alone attests nothing about log completeness.
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![header_record(head)]),
    )?;
    assert_eq!(
        runtime.log_coverage_head(),
        None,
        "coverage progress must never imply log completeness"
    );

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::<Ethereum>::new(Vec::new())
            // A control-only batch must name its chain authoritatively.
            .with_chain_id(1)
            .with_chain_controls([ChainControl::LogCoverage(head)]),
    )?;

    assert_eq!(runtime.log_coverage_head(), Some(&head));
    Ok(())
}
