//! Acceptance tests for reactive block journaling and reorg recovery.
//!
//! These tests cover the runtime-owned machinery that downstream crates should not
//! need to rebuild: journaling canonical block effects, handling removed/reorged
//! inputs, rolling back reversible storage writes, falling back to targeted purges
//! for irreversible effects, and canceling stale hash-pinned resyncs.
#![cfg(feature = "reactive")]

mod common;

use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use common::{install_mock_erc20, setup_cache};
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    BlockRef, CanonicalRollbackKind, CanonicalSequenceError, CanonicalSequenceMutation,
    CanonicalSequenceState, ChainControl, ChainStatus, DeliveryAudience, DeliveryScope,
    HandlerError, HandlerId, HandlerOutcome, InputRef, InputSource, InvalidationReason,
    InvalidationRequest, LogInterest, ReactiveConfig, ReactiveContext, ReactiveEffect,
    ReactiveError, ReactiveHandler, ReactiveInput, ReactiveInputBatch, ReactiveInputDelivery,
    ReactiveInputIdentity, ReactiveInputKind, ReactiveInputRecord, ReactiveInterest,
    ReactiveReport, ReactiveRuntime, ResyncBlock, ResyncId, ResyncPriority, ResyncReason,
    ResyncRequest, ResyncTarget, RouteKeySpec, StateEffectQuality,
    normalize_and_validate_canonical_sequence, validate_canonical_sequence,
    validate_canonical_sequence_diagnostic,
};
use evm_fork_cache::{PurgeScope, StateUpdate};

fn block(number: u64, hash: B256, parent_hash: B256) -> BlockRef {
    BlockRef {
        number,
        hash,
        parent_hash: Some(parent_hash),
        timestamp: Some(1_700_000_000 + number),
    }
}

fn rpc_log(
    address: Address,
    topics: Vec<B256>,
    block: &BlockRef,
    tx_index: u64,
    log_index: u64,
    removed: bool,
) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::new()),
        block_hash: Some(block.hash),
        block_number: Some(block.number),
        block_timestamp: block.timestamp,
        transaction_hash: Some(B256::repeat_byte((tx_index + 1) as u8)),
        transaction_index: Some(tx_index),
        log_index: Some(log_index),
        removed,
    }
}

fn included_context(block: BlockRef, log_index: u64) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Batch,
        chain_status: ChainStatus::Included {
            block,
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

fn reorged_context(dropped_from: BlockRef, log_index: u64) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Batch,
        chain_status: ChainStatus::Reorged { dropped_from },
        block: Some(dropped_from),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

fn batch(input: ReactiveInput<Ethereum>, ctx: ReactiveContext) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, ctx)])
}

fn sequence_log_record(
    block: BlockRef,
    log_index: u64,
    removed: bool,
) -> ReactiveInputRecord<Ethereum> {
    ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            Address::repeat_byte(0xce),
            vec![keccak256(b"CanonicalSequence()")],
            &block,
            0,
            log_index,
            removed,
        )),
        if removed {
            reorged_context(block, log_index)
        } else {
            included_context(block, log_index)
        },
    )
}

fn sequence_log_record_with_context_block(
    payload_block: BlockRef,
    context_block: BlockRef,
    log_index: u64,
    removed: bool,
) -> ReactiveInputRecord<Ethereum> {
    ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            Address::repeat_byte(0xce),
            vec![keccak256(b"CanonicalSequence()")],
            &payload_block,
            0,
            log_index,
            removed,
        )),
        if removed {
            reorged_context(context_block, log_index)
        } else {
            included_context(context_block, log_index)
        },
    )
}

fn replay_sequence_mutations(
    initial: &CanonicalSequenceState,
    mutations: &[CanonicalSequenceMutation],
) -> CanonicalSequenceState {
    let mut history = initial.retained_canonical_history().to_vec();
    let mut coverage = initial.coverage_head().copied();
    let mut safe = initial.safe_head().copied();
    let mut finalized = initial.finalized_head().copied();
    let enrich = |current: &mut BlockRef, incoming: &BlockRef| {
        current.parent_hash = current.parent_hash.or(incoming.parent_hash);
        current.timestamp = current.timestamp.or(incoming.timestamp);
    };
    let clear_above = |head: &mut Option<BlockRef>, ancestor: Option<BlockRef>| {
        if head.is_some_and(|head| {
            ancestor.is_none_or(|ancestor| {
                head.number > ancestor.number
                    || (head.number == ancestor.number && head.hash != ancestor.hash)
            })
        }) {
            *head = None;
        }
    };

    for mutation in mutations {
        match mutation {
            CanonicalSequenceMutation::Rewind {
                common_ancestor,
                dropped,
            } => {
                history.retain(|block| {
                    !dropped
                        .iter()
                        .any(|dropped| block.number == dropped.number && block.hash == dropped.hash)
                });
                coverage = *common_ancestor;
                clear_above(&mut safe, *common_ancestor);
                clear_above(&mut finalized, *common_ancestor);
            }
            CanonicalSequenceMutation::Canonical(block) => {
                if let Some(existing) = history
                    .iter_mut()
                    .find(|entry| entry.number == block.number && entry.hash == block.hash)
                {
                    enrich(existing, block);
                } else {
                    history.push(*block);
                    history.sort_by_key(|entry| entry.number);
                }
                match coverage.as_mut() {
                    Some(current)
                        if current.number == block.number && current.hash == block.hash =>
                    {
                        enrich(current, block);
                    }
                    Some(current) if current.number >= block.number => {}
                    _ => coverage = Some(*block),
                }
            }
            CanonicalSequenceMutation::Safe(block) => safe = Some(*block),
            CanonicalSequenceMutation::Finalized(block) => finalized = Some(*block),
            _ => {}
        }
    }
    CanonicalSequenceState::new(history, coverage, safe, finalized)
}

#[test]
fn provider_neutral_sequence_validator_is_sparse_checkpointable_and_fail_closed() -> Result<()> {
    let retained = block(100, B256::repeat_byte(0x64), B256::repeat_byte(0x63));
    let old_tip = block(105, B256::repeat_byte(0x69), B256::repeat_byte(0x68));
    let ancestor = block(103, B256::repeat_byte(0x67), B256::repeat_byte(0x66));
    let new_tip = block(105, B256::repeat_byte(0xf5), B256::repeat_byte(0xf4));
    let state = CanonicalSequenceState::new(vec![retained, old_tip], Some(old_tip), None, None);
    let encoded = serde_json::to_vec(&state)?;
    let restored: CanonicalSequenceState = serde_json::from_slice(&encoded)?;
    assert_eq!(
        restored, state,
        "callers can checkpoint the validation state"
    );
    restored.validate()?;
    let mut bounded = restored.clone();
    bounded.retain_recent_history(1);
    assert_eq!(bounded.retained_canonical_history(), &[old_tip]);
    assert_eq!(bounded.coverage_head(), Some(&old_tip));
    bounded.validate()?;
    let invalid_checkpoint = CanonicalSequenceState::new(
        vec![
            retained,
            BlockRef {
                hash: B256::repeat_byte(0xff),
                ..retained
            },
        ],
        Some(retained),
        None,
        None,
    );
    assert!(matches!(
        invalid_checkpoint.validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let replacement = block(104, B256::repeat_byte(0xe8), ancestor.hash);
    let valid = ReactiveInputBatch::<Ethereum>::new(Vec::new())
        .with_chain_id(1)
        .with_chain_controls([
            ChainControl::Reorg {
                common_ancestor: ancestor,
                old_tip,
                new_tip,
            },
            ChainControl::CanonicalProgress(replacement),
        ]);
    let validated = validate_canonical_sequence(&state, &valid)?;
    assert_eq!(validated.next_state().coverage_head(), Some(&replacement));
    assert_eq!(
        validated.next_state().retained_canonical_history(),
        &[retained, ancestor, replacement],
        "an unlogged common ancestor is accepted inside sparse retained history"
    );
    assert!(matches!(
        validated.mutations().first(),
        Some(CanonicalSequenceMutation::Rewind { common_ancestor: Some(block), dropped })
            if *block == ancestor && dropped == &[old_tip]
    ));

    let outside_horizon = CanonicalSequenceState::new(vec![old_tip], Some(old_tip), None, None);
    assert!(matches!(
        validate_canonical_sequence(&outside_horizon, &valid),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    let diagnostic = validate_canonical_sequence_diagnostic(&outside_horizon, &valid)
        .expect_err("durable callers need a stable incomplete-history diagnostic");
    assert!(diagnostic.requires_history());
    assert!(matches!(
        diagnostic,
        CanonicalSequenceError::IncompleteRollback {
            common_ancestor: 103,
            oldest_retained: Some(105),
            kind: CanonicalRollbackKind::Explicit,
        }
    ));

    let invalid_snapshot = CanonicalSequenceState::new(vec![old_tip], None, None, None);
    let invalid = validate_canonical_sequence_diagnostic(&invalid_snapshot, &valid)
        .expect_err("intrinsically malformed state is not history exhaustion");
    assert!(!invalid.requires_history());
    assert!(matches!(invalid, CanonicalSequenceError::Invalid(_)));

    let conflicting = ReactiveInputBatch::<Ethereum>::new(Vec::new())
        .with_chain_id(1)
        .with_chain_controls([
            ChainControl::CanonicalProgress(replacement),
            ChainControl::Safe(BlockRef {
                hash: B256::repeat_byte(0xff),
                ..replacement
            }),
        ]);
    assert!(matches!(
        validate_canonical_sequence(validated.next_state(), &conflicting),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let overlap = ReactiveInputBatch::<Ethereum>::new(Vec::new())
        .with_chain_id(1)
        .with_chain_controls([
            ChainControl::CanonicalProgress(ancestor),
            ChainControl::Barrier {
                id: b"overlap-cutover".to_vec(),
                block: Some(replacement),
            },
        ]);
    let normalized = normalize_and_validate_canonical_sequence(validated.next_state(), &overlap)?;
    assert_eq!(
        normalized.normalized_chain_controls(),
        &[ChainControl::Barrier {
            id: b"overlap-cutover".to_vec(),
            block: None,
        }]
    );
    Ok(())
}

#[test]
fn sequence_validator_emits_replayable_rewinds_for_implicit_replacements() -> Result<()> {
    let parent = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x09));
    let old_11 = block(11, B256::repeat_byte(0x11), parent.hash);
    let old_12 = block(12, B256::repeat_byte(0x12), old_11.hash);
    let replacement_11 = block(11, B256::repeat_byte(0xa1), parent.hash);
    let same_height_12 = block(12, B256::repeat_byte(0xa2), old_11.hash);

    for (initial, replacement, expected_anchor, expected_dropped) in [
        (
            CanonicalSequenceState::new(vec![parent, old_11, old_12], Some(old_12), None, None),
            same_height_12,
            old_11,
            vec![old_12],
        ),
        (
            CanonicalSequenceState::new(vec![parent, old_11, old_12], Some(old_12), None, None),
            replacement_11,
            parent,
            vec![old_11, old_12],
        ),
    ] {
        let batch = ReactiveInputBatch::new(vec![sequence_log_record(replacement, 0, false)]);
        let validation = validate_canonical_sequence(&initial, &batch)?;
        assert_eq!(
            validation.mutations(),
            &[
                CanonicalSequenceMutation::Rewind {
                    common_ancestor: Some(expected_anchor),
                    dropped: expected_dropped,
                },
                CanonicalSequenceMutation::Canonical(replacement),
            ]
        );
        assert_eq!(
            replay_sequence_mutations(&initial, validation.mutations()),
            *validation.next_state(),
            "the public mutations reproduce the validated next state"
        );
    }

    let finalized_parent = block(20, B256::repeat_byte(0x20), B256::repeat_byte(0x19));
    let old_child = block(21, B256::repeat_byte(0x21), finalized_parent.hash);
    let replacement_child = block(21, B256::repeat_byte(0xb1), finalized_parent.hash);
    let finalized_state = CanonicalSequenceState::new(
        vec![old_child],
        Some(old_child),
        None,
        Some(finalized_parent),
    );
    let validation = validate_canonical_sequence(
        &finalized_state,
        &ReactiveInputBatch::new(vec![sequence_log_record(replacement_child, 1, false)]),
    )?;
    assert_eq!(
        validation.mutations(),
        &[
            CanonicalSequenceMutation::Rewind {
                common_ancestor: Some(finalized_parent),
                dropped: vec![old_child],
            },
            CanonicalSequenceMutation::Canonical(replacement_child),
        ]
    );
    assert_eq!(
        replay_sequence_mutations(&finalized_state, validation.mutations()),
        *validation.next_state()
    );
    Ok(())
}

#[test]
fn implicit_replacement_diagnostics_separate_invalid_input_from_missing_history() {
    let parent = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x09));
    let old_tip = block(11, B256::repeat_byte(0x11), parent.hash);
    let parentless_replacement = BlockRef {
        number: old_tip.number,
        hash: B256::repeat_byte(0xa1),
        parent_hash: None,
        timestamp: old_tip.timestamp,
    };
    let sparse = CanonicalSequenceState::new(vec![old_tip], Some(old_tip), None, None);
    let parentless = validate_canonical_sequence_diagnostic(
        &sparse,
        &ReactiveInputBatch::new(vec![sequence_log_record(parentless_replacement, 0, false)]),
    )
    .expect_err("an implicit replacement must identify its parent");
    assert!(!parentless.requires_history());
    assert!(matches!(
        parentless,
        CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
    ));

    let conflicting_replacement = block(
        old_tip.number,
        B256::repeat_byte(0xa2),
        B256::repeat_byte(0xfe),
    );
    let retained = CanonicalSequenceState::new(vec![parent, old_tip], Some(old_tip), None, None);
    let conflicting = validate_canonical_sequence_diagnostic(
        &retained,
        &ReactiveInputBatch::new(vec![sequence_log_record(conflicting_replacement, 1, false)]),
    )
    .expect_err("a supplied parent cannot contradict the exact retained predecessor");
    assert!(!conflicting.requires_history());
    assert!(matches!(
        conflicting,
        CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
    ));

    let missing_history_replacement = block(old_tip.number, B256::repeat_byte(0xa3), parent.hash);
    let missing_history = validate_canonical_sequence_diagnostic(
        &sparse,
        &ReactiveInputBatch::new(vec![sequence_log_record(
            missing_history_replacement,
            2,
            false,
        )]),
    )
    .expect_err("a supplied but unretained parent may require deeper history");
    assert!(missing_history.requires_history());
    assert!(matches!(
        missing_history,
        CanonicalSequenceError::IncompleteRollback {
            common_ancestor: 10,
            oldest_retained: Some(11),
            kind: CanonicalRollbackKind::ImplicitParent,
        }
    ));
}

#[test]
fn sequence_validator_requires_the_parent_at_the_exact_adjacent_height() {
    let reused_hash = B256::repeat_byte(0x42);
    let non_parent = block(2, reused_hash, B256::repeat_byte(0x01));
    let old_tip = block(4, B256::repeat_byte(0x44), B256::repeat_byte(0x43));
    let replacement = block(4, B256::repeat_byte(0xf4), reused_hash);
    let state = CanonicalSequenceState::new(vec![non_parent, old_tip], Some(old_tip), None, None);
    let batch = ReactiveInputBatch::new(vec![sequence_log_record(replacement, 0, false)]);
    assert!(matches!(
        validate_canonical_sequence(&state, &batch),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let sparse_state = CanonicalSequenceState::new(vec![non_parent], Some(non_parent), None, None);
    let impossible_gap_child = block(4, B256::repeat_byte(0xf4), reused_hash);
    assert!(matches!(
        validate_canonical_sequence(
            &sparse_state,
            &ReactiveInputBatch::new(vec![sequence_log_record(impossible_gap_child, 0, false,)]),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    assert!(matches!(
        validate_canonical_sequence(
            &sparse_state,
            &ReactiveInputBatch::<Ethereum>::new(Vec::new())
                .with_chain_controls([ChainControl::CanonicalProgress(impossible_gap_child),]),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    assert!(matches!(
        CanonicalSequenceState::new(
            vec![non_parent, impossible_gap_child],
            Some(impossible_gap_child),
            None,
            None,
        )
        .validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

#[test]
fn parentless_removed_tip_can_use_an_exact_retained_predecessor() -> Result<()> {
    let parent = block(30, B256::repeat_byte(0x30), B256::repeat_byte(0x29));
    let old_tip = BlockRef {
        number: 31,
        hash: B256::repeat_byte(0x31),
        parent_hash: None,
        timestamp: Some(1_700_000_031),
    };
    let replacement = BlockRef {
        hash: B256::repeat_byte(0xb1),
        ..old_tip
    };
    let resolved_replacement = BlockRef {
        parent_hash: Some(parent.hash),
        ..replacement
    };
    let state =
        CanonicalSequenceState::new(vec![parent, old_tip], Some(old_tip), None, Some(parent));
    let batch = ReactiveInputBatch::new(vec![
        sequence_log_record(replacement, 1, false),
        sequence_log_record(old_tip, 0, true),
    ]);
    let validation = validate_canonical_sequence(&state, &batch)?;
    assert_eq!(
        validation.mutations(),
        &[
            CanonicalSequenceMutation::Rewind {
                common_ancestor: Some(parent),
                dropped: vec![old_tip],
            },
            CanonicalSequenceMutation::Canonical(resolved_replacement),
        ]
    );
    assert_eq!(
        replay_sequence_mutations(&state, validation.mutations()),
        *validation.next_state()
    );
    Ok(())
}

#[tokio::test]
async fn runtime_parentless_replacement_uses_the_same_batch_removed_proof() -> Result<()> {
    let parent = block(30, B256::repeat_byte(0x30), B256::repeat_byte(0x29));
    let old_tip = BlockRef {
        number: 31,
        hash: B256::repeat_byte(0x31),
        parent_hash: None,
        timestamp: Some(1_700_000_031),
    };
    let replacement = BlockRef {
        hash: B256::repeat_byte(0xb1),
        ..old_tip
    };
    let resolved_replacement = BlockRef {
        parent_hash: Some(parent.hash),
        ..replacement
    };
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![sequence_log_record(parent, 0, false)]),
    )?;
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(old_tip)]),
    )?;

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![
            sequence_log_record(replacement, 1, false),
            sequence_log_record(old_tip, 0, true),
        ]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(resolved_replacement));
    assert_eq!(runtime.metrics().deep_reorgs, 0);
    assert_eq!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Healthy
    );
    Ok(())
}

#[test]
fn normalized_equal_coverage_retains_metadata_enrichment_but_older_enrichment_is_non_forwarding()
-> Result<()> {
    let parent = B256::repeat_byte(0x40);
    let sparse = BlockRef {
        number: 41,
        hash: B256::repeat_byte(0x41),
        parent_hash: None,
        timestamp: None,
    };
    let enriched = block(41, sparse.hash, parent);
    let sparse_state = CanonicalSequenceState::new(vec![sparse], Some(sparse), None, None);

    for control in [
        ChainControl::CanonicalProgress(enriched),
        ChainControl::Barrier {
            id: b"equal-enrichment".to_vec(),
            block: Some(enriched),
        },
    ] {
        let validation = normalize_and_validate_canonical_sequence(
            &sparse_state,
            &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([control.clone()]),
        )?;
        assert_eq!(validation.normalized_chain_controls(), &[control]);
        assert_eq!(validation.next_state().coverage_head(), Some(&enriched));
        assert_eq!(
            validation.next_state().retained_canonical_history(),
            &[enriched]
        );
    }

    let head = block(42, B256::repeat_byte(0x42), sparse.hash);
    let advanced = CanonicalSequenceState::new(vec![sparse, head], Some(head), None, None);
    let older = ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
        ChainControl::CanonicalProgress(enriched),
        ChainControl::Barrier {
            id: b"older-enrichment".to_vec(),
            block: Some(enriched),
        },
    ]);
    let normalized = normalize_and_validate_canonical_sequence(&advanced, &older)?;
    assert_eq!(normalized.next_state(), &advanced);
    assert_eq!(
        normalized.normalized_chain_controls(),
        &[ChainControl::Barrier {
            id: b"older-enrichment".to_vec(),
            block: None,
        }]
    );
    Ok(())
}

#[test]
fn sequence_finality_never_advances_beyond_coverage() -> Result<()> {
    let covered = block(50, B256::repeat_byte(0x50), B256::repeat_byte(0x49));
    let next = block(51, B256::repeat_byte(0x51), covered.hash);
    assert!(matches!(
        CanonicalSequenceState::new(vec![covered], Some(covered), Some(next), None).validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    let state = CanonicalSequenceState::new(vec![covered], Some(covered), None, None);
    let ahead = ReactiveInputBatch::<Ethereum>::new(Vec::new())
        .with_chain_controls([ChainControl::Safe(next)]);
    assert!(matches!(
        validate_canonical_sequence(&state, &ahead),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let certified = ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
        ChainControl::CanonicalProgress(next),
        ChainControl::Safe(next),
        ChainControl::Finalized(next),
    ]);
    let validation = validate_canonical_sequence(&state, &certified)?;
    assert_eq!(validation.next_state().coverage_head(), Some(&next));
    assert_eq!(validation.next_state().safe_head(), Some(&next));
    assert_eq!(validation.next_state().finalized_head(), Some(&next));
    Ok(())
}

#[test]
fn sequence_snapshots_reject_broken_adjacent_links() {
    let parent = block(60, B256::repeat_byte(0x60), B256::repeat_byte(0x59));
    let wrong_child = block(61, B256::repeat_byte(0x61), B256::repeat_byte(0xff));
    assert!(matches!(
        CanonicalSequenceState::new(vec![parent], None, None, None).validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    assert!(matches!(
        CanonicalSequenceState::new(vec![parent, wrong_child], Some(wrong_child), None, None,)
            .validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    assert!(matches!(
        CanonicalSequenceState::new(vec![parent], Some(wrong_child), None, None).validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    let safe_with_wrong_finalized_parent = BlockRef {
        number: 61,
        hash: B256::repeat_byte(0x61),
        parent_hash: Some(B256::repeat_byte(0xfe)),
        timestamp: Some(1_700_000_061),
    };
    assert!(matches!(
        CanonicalSequenceState::new(
            vec![safe_with_wrong_finalized_parent],
            Some(safe_with_wrong_finalized_parent),
            Some(safe_with_wrong_finalized_parent),
            Some(parent),
        )
        .validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let finality = block(70, B256::repeat_byte(0x70), B256::repeat_byte(0x69));
    let wrong_coverage = block(71, B256::repeat_byte(0x71), B256::repeat_byte(0xff));
    for (safe, finalized) in [
        (Some(finality), None),
        (None, Some(finality)),
        (Some(finality), Some(finality)),
    ] {
        assert!(matches!(
            CanonicalSequenceState::new(Vec::new(), Some(wrong_coverage), safe, finalized,)
                .validate(),
            Err(ReactiveError::InvalidChainControl { .. })
        ));
    }
    let linked_coverage = BlockRef {
        parent_hash: Some(finality.hash),
        ..wrong_coverage
    };
    CanonicalSequenceState::new(
        Vec::new(),
        Some(linked_coverage),
        Some(finality),
        Some(finality),
    )
    .validate()
    .expect("adjacent coverage may descend from the exact safe/finalized head");

    let retained = block(80, B256::repeat_byte(0x80), B256::repeat_byte(0x79));
    let sparse_coverage = BlockRef {
        number: 81,
        hash: B256::repeat_byte(0x81),
        parent_hash: None,
        timestamp: None,
    };
    let conflicting_alias = BlockRef {
        parent_hash: Some(B256::repeat_byte(0xfe)),
        ..sparse_coverage
    };
    assert!(matches!(
        CanonicalSequenceState::new(
            vec![retained],
            Some(sparse_coverage),
            Some(conflicting_alias),
            None,
        )
        .validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    CanonicalSequenceState::new(
        vec![retained],
        Some(sparse_coverage),
        Some(BlockRef {
            parent_hash: Some(retained.hash),
            ..sparse_coverage
        }),
        None,
    )
    .validate()
    .expect("same-height finality metadata may enrich sparse coverage compatibly");

    let reused = B256::repeat_byte(0xaa);
    let first_use = block(90, reused, B256::repeat_byte(0x89));
    let second_use = BlockRef {
        number: 92,
        hash: reused,
        parent_hash: None,
        timestamp: Some(1_700_000_092),
    };
    assert!(matches!(
        CanonicalSequenceState::new(vec![first_use, second_use], Some(second_use), None, None,)
            .validate(),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    let first_use_state = CanonicalSequenceState::new(vec![first_use], Some(first_use), None, None);
    assert!(matches!(
        validate_canonical_sequence(
            &first_use_state,
            &ReactiveInputBatch::<Ethereum>::new(Vec::new())
                .with_chain_controls([ChainControl::CanonicalProgress(second_use),]),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

#[test]
fn explicit_reorg_span_cannot_suppress_removal_of_its_new_tip() {
    let ancestor = block(70, B256::repeat_byte(0x70), B256::repeat_byte(0x69));
    let old_tip = block(71, B256::repeat_byte(0x71), ancestor.hash);
    let new_tip = block(71, B256::repeat_byte(0xf1), ancestor.hash);
    let state = CanonicalSequenceState::new(vec![ancestor, old_tip], Some(old_tip), None, None);
    let batch = ReactiveInputBatch::new(vec![sequence_log_record(new_tip, 0, true)])
        .with_chain_controls([ChainControl::Reorg {
            common_ancestor: ancestor,
            old_tip,
            new_tip,
        }]);
    assert!(matches!(
        validate_canonical_sequence(&state, &batch),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let removed_and_canonical = ReactiveInputBatch::new(vec![
        sequence_log_record(old_tip, 0, true),
        sequence_log_record(old_tip, 0, false),
    ]);
    assert!(matches!(
        validate_canonical_sequence(&state, &removed_and_canonical),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

#[test]
fn removed_identity_cannot_be_reasserted_by_any_post_record_control() {
    let parent = block(72, B256::repeat_byte(0x72), B256::repeat_byte(0x71));
    let removed = block(73, B256::repeat_byte(0x73), parent.hash);
    let state = CanonicalSequenceState::new(vec![parent, removed], Some(removed), None, None);

    for control in [
        ChainControl::CanonicalProgress(removed),
        ChainControl::Barrier {
            id: b"removed-tip".to_vec(),
            block: Some(removed),
        },
        ChainControl::Safe(removed),
        ChainControl::Finalized(removed),
    ] {
        let error = validate_canonical_sequence(
            &state,
            &ReactiveInputBatch::new(vec![sequence_log_record(removed, 0, true)])
                .with_chain_controls([control.clone()]),
        )
        .expect_err("a post-record control cannot reassert a removed identity");
        assert!(
            matches!(error, ReactiveError::InvalidChainControl { ref message }
                if message.contains("also removed")),
            "{control:?} returned the wrong contradiction: {error}"
        );
    }

    let conflicting_removed = BlockRef {
        timestamp: removed.timestamp.map(|timestamp| timestamp + 1),
        ..removed
    };
    assert!(matches!(
        validate_canonical_sequence(
            &state,
            &ReactiveInputBatch::new(vec![
                sequence_log_record(removed, 0, true),
                sequence_log_record(conflicting_removed, 1, true),
            ]),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let reused_removed_hash = BlockRef {
        number: removed.number + 1,
        hash: removed.hash,
        parent_hash: Some(B256::repeat_byte(0xfe)),
        timestamp: removed.timestamp.map(|timestamp| timestamp + 1),
    };
    let duplicate_height_error = validate_canonical_sequence_diagnostic(
        &state,
        &ReactiveInputBatch::new(vec![
            sequence_log_record(removed, 2, true),
            sequence_log_record(reused_removed_hash, 3, true),
        ]),
    )
    .expect_err("one removed hash cannot identify two heights");
    assert!(matches!(
        duplicate_height_error,
        CanonicalSequenceError::Invalid(_)
    ));
    let removed_then_reused_control = validate_canonical_sequence_diagnostic(
        &state,
        &ReactiveInputBatch::new(vec![sequence_log_record(removed, 4, true)])
            .with_chain_controls([ChainControl::CanonicalProgress(reused_removed_hash)]),
    )
    .expect_err("a removed hash cannot be canonically asserted at another height");
    assert!(matches!(
        removed_then_reused_control,
        CanonicalSequenceError::Invalid(_)
    ));
}

#[test]
fn explicit_reorg_only_suppresses_exact_old_branch_removals() {
    let ancestor = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x09));
    let old_11 = block(11, B256::repeat_byte(0x11), ancestor.hash);
    let old_tip = block(12, B256::repeat_byte(0x12), old_11.hash);
    let new_tip = block(12, B256::repeat_byte(0xb2), B256::repeat_byte(0xb1));
    let foreign_11 = block(11, B256::repeat_byte(0xf1), ancestor.hash);
    let state =
        CanonicalSequenceState::new(vec![ancestor, old_11, old_tip], Some(old_tip), None, None);
    let batch = ReactiveInputBatch::new(vec![sequence_log_record(foreign_11, 0, true)])
        .with_chain_controls([ChainControl::Reorg {
            common_ancestor: ancestor,
            old_tip,
            new_tip,
        }]);

    assert!(matches!(
        validate_canonical_sequence(&state, &batch),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

#[tokio::test]
async fn removed_genesis_is_rejected_without_mutating_runtime_state() -> Result<()> {
    let address = Address::repeat_byte(0xb0);
    let genesis = block(0, B256::repeat_byte(0x01), B256::ZERO);
    let state = CanonicalSequenceState::new(vec![genesis], Some(genesis), None, None);
    let removed = ReactiveInputBatch::new(vec![sequence_log_record(genesis, 0, true)]);
    assert!(matches!(
        validate_canonical_sequence(&state, &removed),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Genesis()")],
                &genesis,
                0,
                0,
                false,
            )),
            included_context(genesis, 0),
        ),
    )?;
    let error = runtime
        .ingest_batch(&mut cache, removed)
        .expect_err("genesis removal must fail atomically");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), Some(genesis));
    Ok(())
}

#[test]
fn removed_tip_uses_its_authenticated_parent_as_the_rewind_anchor() -> Result<()> {
    let parent = block(20, B256::repeat_byte(0x20), B256::repeat_byte(0x19));
    let old_tip = block(21, B256::repeat_byte(0x21), parent.hash);
    let replacement = block(21, B256::repeat_byte(0xb1), parent.hash);
    let batch = ReactiveInputBatch::new(vec![
        sequence_log_record(old_tip, 0, true),
        sequence_log_record(replacement, 1, false),
    ]);

    for (safe, finalized) in [
        (Some(parent), None),
        (None, Some(parent)),
        (Some(parent), Some(parent)),
    ] {
        let initial = CanonicalSequenceState::new(vec![old_tip], Some(old_tip), safe, finalized);
        let validation = validate_canonical_sequence(&initial, &batch)?;
        assert_eq!(
            validation.mutations(),
            &[
                CanonicalSequenceMutation::Rewind {
                    common_ancestor: Some(parent),
                    dropped: vec![old_tip],
                },
                CanonicalSequenceMutation::Canonical(replacement),
            ]
        );
        assert_eq!(validation.next_state().coverage_head(), Some(&replacement));
        assert_eq!(validation.next_state().safe_head(), safe.as_ref());
        assert_eq!(validation.next_state().finalized_head(), finalized.as_ref());
        assert_eq!(
            replay_sequence_mutations(&initial, validation.mutations()),
            *validation.next_state()
        );
    }

    let partial_removed = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..old_tip
    };
    let partial = validate_canonical_sequence(
        &CanonicalSequenceState::new(vec![old_tip], Some(old_tip), None, Some(parent)),
        &ReactiveInputBatch::new(vec![
            sequence_log_record(partial_removed, 2, true),
            sequence_log_record(replacement, 3, false),
        ]),
    )?;
    assert!(matches!(
        partial.mutations().first(),
        Some(CanonicalSequenceMutation::Rewind {
            common_ancestor: Some(anchor),
            dropped,
        }) if *anchor == parent && dropped == &[old_tip]
    ));

    let conflicting_removed = BlockRef {
        timestamp: old_tip.timestamp.map(|timestamp| timestamp + 1),
        ..old_tip
    };
    assert!(matches!(
        validate_canonical_sequence(
            &CanonicalSequenceState::new(vec![old_tip], Some(old_tip), None, None),
            &ReactiveInputBatch::new(vec![sequence_log_record(conflicting_removed, 4, true,)]),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
    Ok(())
}

#[test]
fn known_removed_anchor_mismatch_is_invalid_without_more_history() {
    let parent = block(20, B256::repeat_byte(0x20), B256::repeat_byte(0x19));
    let old_tip = block(21, B256::repeat_byte(0x21), parent.hash);
    let replacement = block(21, B256::repeat_byte(0xb1), B256::repeat_byte(0xfe));
    let initial =
        CanonicalSequenceState::new(vec![old_tip], Some(old_tip), Some(parent), Some(parent));
    let batches = [
        ReactiveInputBatch::new(vec![
            sequence_log_record(old_tip, 0, true),
            sequence_log_record(replacement, 1, false),
        ]),
        ReactiveInputBatch::new(vec![sequence_log_record(old_tip, 0, true)])
            .with_chain_controls([ChainControl::CanonicalProgress(replacement)]),
    ];

    for batch in batches {
        let error = validate_canonical_sequence_diagnostic(&initial, &batch)
            .expect_err("a replacement that contradicts a known parent must fail");
        assert!(
            !error.requires_history(),
            "older history cannot repair a known parent contradiction: {error:?}"
        );
        assert!(matches!(
            error,
            CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
        ));
    }
}

#[test]
fn removed_metadata_cannot_contradict_the_exact_retained_predecessor() {
    let parent = block(20, B256::repeat_byte(0x20), B256::repeat_byte(0x19));
    let partial_old_tip = BlockRef {
        number: 21,
        hash: B256::repeat_byte(0x21),
        parent_hash: None,
        timestamp: Some(1_700_000_021),
    };
    let conflicting_removed = BlockRef {
        parent_hash: Some(B256::repeat_byte(0xfe)),
        ..partial_old_tip
    };
    let initial = CanonicalSequenceState::new(
        vec![parent, partial_old_tip],
        Some(partial_old_tip),
        None,
        None,
    );

    let error = validate_canonical_sequence_diagnostic(
        &initial,
        &ReactiveInputBatch::new(vec![sequence_log_record(conflicting_removed, 0, true)]),
    )
    .expect_err("removed metadata cannot contradict an exact retained parent");
    assert!(!error.requires_history());
    assert!(matches!(
        error,
        CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
    ));
}

#[test]
fn removed_identity_cannot_reuse_a_hash_from_another_retained_height() {
    let parent = block(5, B256::repeat_byte(0x55), B256::repeat_byte(0x44));
    let current = block(6, B256::repeat_byte(0x66), parent.hash);
    let impossible_removed = BlockRef {
        number: current.number,
        hash: parent.hash,
        parent_hash: None,
        timestamp: current.timestamp,
    };
    let initial = CanonicalSequenceState::new(vec![parent, current], Some(current), None, None);

    let error = validate_canonical_sequence_diagnostic(
        &initial,
        &ReactiveInputBatch::new(vec![sequence_log_record(impossible_removed, 0, true)]),
    )
    .expect_err("one block hash cannot identify two canonical heights");
    assert!(!error.requires_history());
    assert!(matches!(
        error,
        CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
    ));
}

#[test]
fn explicit_reorg_assertion_authenticates_a_partial_new_tip_record() -> Result<()> {
    let ancestor = block(40, B256::repeat_byte(0x40), B256::repeat_byte(0x39));
    let old_tip = block(41, B256::repeat_byte(0x41), ancestor.hash);
    let new_tip = block(41, B256::repeat_byte(0xf1), ancestor.hash);
    let partial_new_tip = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..new_tip
    };
    let initial = CanonicalSequenceState::new(
        vec![ancestor, old_tip],
        Some(old_tip),
        Some(ancestor),
        Some(ancestor),
    );
    let envelope = ReactiveInputBatch::new(vec![sequence_log_record(partial_new_tip, 0, false)])
        .with_chain_controls([ChainControl::Reorg {
            common_ancestor: ancestor,
            old_tip,
            new_tip,
        }]);

    let validation = validate_canonical_sequence_diagnostic(&initial, &envelope)?;
    assert_eq!(validation.next_state().coverage_head(), Some(&new_tip));
    assert_eq!(validation.next_state().safe_head(), Some(&ancestor));
    assert_eq!(validation.next_state().finalized_head(), Some(&ancestor));
    assert!(matches!(
        validation.mutations().last(),
        Some(CanonicalSequenceMutation::Canonical(block)) if *block == new_tip
    ));
    Ok(())
}

#[tokio::test]
async fn runtime_retains_metadata_resolved_for_a_partial_explicit_new_tip() -> Result<()> {
    let ancestor = block(50, B256::repeat_byte(0x50), B256::repeat_byte(0x49));
    let old_tip = block(51, B256::repeat_byte(0x51), ancestor.hash);
    let new_tip = block(51, B256::repeat_byte(0xf1), ancestor.hash);
    let partial_new_tip = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..new_tip
    };
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for (block, log_index) in [(ancestor, 0), (old_tip, 1)] {
        runtime.ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![sequence_log_record(block, log_index, false)]),
        )?;
    }
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![sequence_log_record(partial_new_tip, 2, false)])
            .with_chain_controls([ChainControl::Reorg {
                common_ancestor: ancestor,
                old_tip,
                new_tip,
            }]),
    )?;
    assert_eq!(runtime.last_canonical_block(), Some(new_tip));

    let conflicting = BlockRef {
        timestamp: new_tip.timestamp.map(|timestamp| timestamp + 1),
        ..new_tip
    };
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::<Ethereum>::new(Vec::new())
                .with_chain_controls([ChainControl::CanonicalProgress(conflicting)]),
        )
        .expect_err("resolved metadata must remain authoritative across batches");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), Some(new_tip));
    Ok(())
}

#[tokio::test]
async fn exact_post_controls_authenticate_compact_records_and_survive_normalization() -> Result<()>
{
    let parent = block(60, B256::repeat_byte(0x60), B256::repeat_byte(0x59));
    let child = block(61, B256::repeat_byte(0x61), parent.hash);
    let partial_child = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..child
    };
    let initial = CanonicalSequenceState::new(vec![parent], Some(parent), None, None);
    let controls = [
        ChainControl::CanonicalProgress(child),
        ChainControl::Barrier {
            id: b"compact-proof".to_vec(),
            block: Some(child),
        },
        ChainControl::Safe(child),
        ChainControl::Finalized(child),
    ];

    for control in controls {
        let envelope = ReactiveInputBatch::new(vec![sequence_log_record(partial_child, 0, false)])
            .with_chain_controls([control.clone()]);
        let validation = normalize_and_validate_canonical_sequence(&initial, &envelope)?;
        assert_eq!(validation.next_state().coverage_head(), Some(&child));
        assert_eq!(
            validation.normalized_chain_controls(),
            std::slice::from_ref(&control),
            "proof-bearing controls must survive normalization"
        );

        let mut cache = setup_cache().await?;
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![sequence_log_record(parent, 0, false)]),
        )?;
        runtime.ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![sequence_log_record(partial_child, 1, false)])
                .with_chain_controls(validation.normalized_chain_controls().iter().cloned()),
        )?;
        assert_eq!(runtime.last_canonical_block(), Some(child));
    }

    let conflicting_child = BlockRef {
        timestamp: child.timestamp.map(|timestamp| timestamp + 1),
        ..child
    };
    let timestamped_partial = BlockRef {
        parent_hash: None,
        ..child
    };
    let conflict = validate_canonical_sequence_diagnostic(
        &initial,
        &ReactiveInputBatch::new(vec![sequence_log_record(timestamped_partial, 2, false)])
            .with_chain_controls([ChainControl::Safe(conflicting_child)]),
    )
    .expect_err("a post-control proof cannot contradict record metadata");
    assert!(!conflict.requires_history());
    assert!(matches!(
        conflict,
        CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn log_payload_timestamp_participates_in_canonical_metadata_resolution() -> Result<()> {
    let parent = block(70, B256::repeat_byte(0x70), B256::repeat_byte(0x69));
    let child = block(71, B256::repeat_byte(0x71), parent.hash);
    let partial_context = BlockRef {
        timestamp: None,
        ..child
    };
    let initial = CanonicalSequenceState::new(vec![parent], Some(parent), None, None);

    let compatible = validate_canonical_sequence_diagnostic(
        &initial,
        &ReactiveInputBatch::new(vec![sequence_log_record_with_context_block(
            child,
            partial_context,
            0,
            false,
        )]),
    )?;
    assert_eq!(compatible.next_state().coverage_head(), Some(&child));

    let conflicting = BlockRef {
        timestamp: child.timestamp.map(|timestamp| timestamp + 1),
        ..child
    };
    for (state, controls) in [
        (
            initial.clone(),
            vec![ChainControl::CanonicalProgress(conflicting)],
        ),
        (
            CanonicalSequenceState::new(vec![conflicting], Some(conflicting), None, None),
            Vec::new(),
        ),
    ] {
        let error = validate_canonical_sequence_diagnostic(
            &state,
            &ReactiveInputBatch::new(vec![sequence_log_record_with_context_block(
                child,
                partial_context,
                1,
                false,
            )])
            .with_chain_controls(controls),
        )
        .expect_err("payload timestamps cannot conflict with canonical state or controls");
        assert!(!error.requires_history());
        assert!(matches!(
            error,
            CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
        ));
    }

    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![sequence_log_record(parent, 0, false)]),
    )?;
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![sequence_log_record_with_context_block(
                child,
                partial_context,
                1,
                false,
            )])
            .with_chain_controls([ChainControl::CanonicalProgress(conflicting)]),
        )
        .expect_err("payload/control timestamp conflict must fail atomically");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), Some(parent));

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![sequence_log_record_with_context_block(
            child,
            partial_context,
            2,
            false,
        )]),
    )?;
    assert_eq!(runtime.last_canonical_block(), Some(child));
    Ok(())
}

#[test]
fn sparse_removed_tip_advances_to_its_authenticated_parent_and_can_continue() -> Result<()> {
    let retained = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89));
    let finalized = block(100, B256::repeat_byte(0x64), B256::repeat_byte(0x63));
    let parent = BlockRef {
        number: 109,
        hash: B256::repeat_byte(0x6d),
        parent_hash: None,
        timestamp: None,
    };
    let removed = block(110, B256::repeat_byte(0x6e), parent.hash);
    let replacement = block(110, B256::repeat_byte(0xee), parent.hash);
    let initial = CanonicalSequenceState::new(
        vec![retained, removed],
        Some(removed),
        Some(finalized),
        Some(finalized),
    );

    let removed_only = validate_canonical_sequence(
        &initial,
        &ReactiveInputBatch::new(vec![sequence_log_record(removed, 0, true)]),
    )?;
    assert_eq!(
        removed_only.mutations(),
        &[CanonicalSequenceMutation::Rewind {
            common_ancestor: Some(parent),
            dropped: vec![removed],
        }]
    );
    assert_eq!(
        removed_only.next_state(),
        &CanonicalSequenceState::new(
            vec![retained],
            Some(parent),
            Some(finalized),
            Some(finalized),
        )
    );
    assert_eq!(
        replay_sequence_mutations(&initial, removed_only.mutations()),
        *removed_only.next_state()
    );

    let continued = validate_canonical_sequence(
        removed_only.next_state(),
        &ReactiveInputBatch::new(vec![sequence_log_record(replacement, 1, false)]),
    )?;
    assert_eq!(continued.next_state().coverage_head(), Some(&replacement));
    assert_eq!(continued.next_state().safe_head(), Some(&finalized));
    assert_eq!(continued.next_state().finalized_head(), Some(&finalized));
    Ok(())
}

#[tokio::test]
async fn runtime_removed_sole_tip_preserves_authenticated_safe_and_finalized_parent() -> Result<()>
{
    let address = Address::repeat_byte(0xb2);
    let parent = block(20, B256::repeat_byte(0x20), B256::repeat_byte(0x19));
    let old_tip = block(21, B256::repeat_byte(0x21), parent.hash);
    let replacement = block(21, B256::repeat_byte(0xb1), parent.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
        journal_depth: 1,
        ..ReactiveConfig::default()
    });
    for (canonical, log_index) in [(parent, 0), (old_tip, 1)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"RemovedFinalityParity()")],
                    &canonical,
                    0,
                    log_index,
                    false,
                )),
                included_context(canonical, log_index),
            ),
        )?;
    }
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Safe(parent), ChainControl::Finalized(parent)]),
    )?;

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![
            sequence_log_record(
                BlockRef {
                    parent_hash: None,
                    timestamp: None,
                    ..old_tip
                },
                0,
                true,
            ),
            sequence_log_record(replacement, 1, false),
        ]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(replacement));
    assert_eq!(runtime.safe_head(), Some(&parent));
    assert_eq!(runtime.finalized_head(), Some(&parent));
    Ok(())
}

#[tokio::test]
async fn runtime_sparse_removal_installs_parent_coverage_before_continuation() -> Result<()> {
    let address = Address::repeat_byte(0xb3);
    let retained = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89));
    let finalized = block(100, B256::repeat_byte(0x64), B256::repeat_byte(0x63));
    let parent = BlockRef {
        number: 109,
        hash: B256::repeat_byte(0x6d),
        parent_hash: None,
        timestamp: None,
    };
    let removed = block(110, B256::repeat_byte(0x6e), parent.hash);
    let replacement = block(110, B256::repeat_byte(0xee), parent.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for (canonical, log_index) in [(retained, 0), (removed, 1)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"SparseRemovedContinuation()")],
                    &canonical,
                    0,
                    log_index,
                    false,
                )),
                included_context(canonical, log_index),
            ),
        )?;
    }
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([
                ChainControl::Safe(finalized),
                ChainControl::Finalized(finalized),
            ]),
    )?;

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![sequence_log_record(removed, 0, true)]),
    )?;
    assert_eq!(runtime.last_canonical_block(), Some(parent));
    assert_eq!(runtime.safe_head(), Some(&finalized));
    assert_eq!(runtime.finalized_head(), Some(&finalized));

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![sequence_log_record(replacement, 1, false)]),
    )?;
    assert_eq!(runtime.last_canonical_block(), Some(replacement));
    assert_eq!(runtime.safe_head(), Some(&finalized));
    assert_eq!(runtime.finalized_head(), Some(&finalized));
    Ok(())
}

#[test]
fn removed_anchor_survives_older_and_ancestor_records_until_replacement() -> Result<()> {
    let older = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89));
    let parent = block(109, B256::repeat_byte(0x6d), B256::repeat_byte(0x6c));
    let removed = BlockRef {
        number: 110,
        hash: B256::repeat_byte(0x6e),
        parent_hash: None,
        timestamp: Some(1_700_000_110),
    };
    let replacement = BlockRef {
        hash: B256::repeat_byte(0xee),
        ..removed
    };
    let resolved_replacement = BlockRef {
        parent_hash: Some(parent.hash),
        ..replacement
    };
    let initial =
        CanonicalSequenceState::new(vec![older, parent, removed], Some(removed), None, None);
    let batch = ReactiveInputBatch::new(vec![
        sequence_log_record(removed, 0, true),
        sequence_log_record(older, 1, false),
        sequence_log_record(parent, 2, false),
        sequence_log_record(replacement, 3, false),
    ]);

    let validation = validate_canonical_sequence(&initial, &batch)?;
    assert_eq!(
        validation.next_state().coverage_head(),
        Some(&resolved_replacement)
    );
    assert_eq!(
        validation.next_state().retained_canonical_history(),
        &[older, parent, resolved_replacement]
    );
    assert!(matches!(
        validation.mutations().first(),
        Some(CanonicalSequenceMutation::Rewind {
            common_ancestor: Some(anchor),
            dropped,
        }) if *anchor == parent && dropped == &[removed]
    ));
    assert_eq!(
        replay_sequence_mutations(&initial, validation.mutations()),
        *validation.next_state()
    );
    Ok(())
}

#[test]
fn explicit_rewind_and_finality_mutations_carry_resolved_metadata() -> Result<()> {
    let ancestor = block(30, B256::repeat_byte(0x30), B256::repeat_byte(0x29));
    let old_tip = block(31, B256::repeat_byte(0x31), ancestor.hash);
    let new_tip = block(31, B256::repeat_byte(0xb1), ancestor.hash);
    let partial_ancestor = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..ancestor
    };
    let initial = CanonicalSequenceState::new(vec![ancestor, old_tip], Some(old_tip), None, None);
    let rewind = validate_canonical_sequence(
        &initial,
        &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
            ChainControl::Reorg {
                common_ancestor: partial_ancestor,
                old_tip,
                new_tip,
            },
        ]),
    )?;
    assert_eq!(rewind.next_state().coverage_head(), Some(&ancestor));
    assert_eq!(
        rewind.mutations(),
        &[CanonicalSequenceMutation::Rewind {
            common_ancestor: Some(ancestor),
            dropped: vec![old_tip],
        }]
    );
    assert_eq!(
        replay_sequence_mutations(&initial, rewind.mutations()),
        *rewind.next_state()
    );

    let coverage = block(41, B256::repeat_byte(0x41), B256::repeat_byte(0x40));
    let resolved = block(40, coverage.parent_hash.unwrap(), B256::repeat_byte(0x3f));
    let partial = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..resolved
    };
    let finality_state = CanonicalSequenceState::new(
        vec![resolved, coverage],
        Some(coverage),
        Some(resolved),
        Some(resolved),
    );
    let finality = validate_canonical_sequence(
        &finality_state,
        &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
            ChainControl::Safe(partial),
            ChainControl::Finalized(partial),
        ]),
    )?;
    assert_eq!(
        finality.mutations(),
        &[
            CanonicalSequenceMutation::Safe(resolved),
            CanonicalSequenceMutation::Finalized(resolved),
        ]
    );
    assert_eq!(
        replay_sequence_mutations(&finality_state, finality.mutations()),
        *finality.next_state()
    );

    let conflicting = BlockRef {
        timestamp: resolved.timestamp.map(|timestamp| timestamp + 1),
        ..resolved
    };
    let sparse_finality_state = CanonicalSequenceState::new(
        vec![coverage],
        Some(coverage),
        Some(resolved),
        Some(resolved),
    );
    for control in [
        ChainControl::Safe(conflicting),
        ChainControl::Finalized(conflicting),
    ] {
        assert!(matches!(
            validate_canonical_sequence(
                &sparse_finality_state,
                &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([control]),
            ),
            Err(ReactiveError::InvalidChainControl { .. })
        ));
    }
    Ok(())
}

#[test]
fn explicit_reorg_old_tip_metadata_must_match_current_coverage() {
    let ancestor = block(50, B256::repeat_byte(0x50), B256::repeat_byte(0x49));
    let old_tip = block(51, B256::repeat_byte(0x51), ancestor.hash);
    let conflicting_old_tip = BlockRef {
        timestamp: old_tip.timestamp.map(|timestamp| timestamp + 1),
        ..old_tip
    };
    let new_tip = block(51, B256::repeat_byte(0xd1), ancestor.hash);
    let state = CanonicalSequenceState::new(vec![ancestor, old_tip], Some(old_tip), None, None);

    assert!(matches!(
        validate_canonical_sequence(
            &state,
            &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
                ChainControl::Reorg {
                    common_ancestor: ancestor,
                    old_tip: conflicting_old_tip,
                    new_tip,
                },
            ]),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let sparse_ancestor = block(60, B256::repeat_byte(0x60), B256::repeat_byte(0x59));
    let known_wrong_height = block(61, B256::repeat_byte(0x61), sparse_ancestor.hash);
    let sparse_old_tip = block(63, B256::repeat_byte(0x63), B256::repeat_byte(0x62));
    let sparse_new_tip = block(63, B256::repeat_byte(0xe3), known_wrong_height.hash);
    let sparse_state = CanonicalSequenceState::new(
        vec![sparse_ancestor, known_wrong_height, sparse_old_tip],
        Some(sparse_old_tip),
        None,
        None,
    );
    assert!(matches!(
        validate_canonical_sequence(
            &sparse_state,
            &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
                ChainControl::Reorg {
                    common_ancestor: sparse_ancestor,
                    old_tip: sparse_old_tip,
                    new_tip: sparse_new_tip,
                },
            ]),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

#[test]
fn explicit_reorg_rejects_tip_identities_that_disappear_after_rewind() {
    let common_ancestor = block(18, B256::repeat_byte(0x18), B256::repeat_byte(0x17));
    let exact_parent = block(19, B256::repeat_byte(0x19), common_ancestor.hash);
    let partial_old_tip = BlockRef {
        number: 20,
        hash: B256::repeat_byte(0x20),
        parent_hash: None,
        timestamp: Some(1_700_000_020),
    };
    let state = CanonicalSequenceState::new(
        vec![common_ancestor, exact_parent, partial_old_tip],
        Some(partial_old_tip),
        None,
        None,
    );
    let conflicting_old_tip = BlockRef {
        parent_hash: Some(B256::repeat_byte(0xfe)),
        ..partial_old_tip
    };
    let invalid_old_tip = validate_canonical_sequence_diagnostic(
        &state,
        &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
            ChainControl::Reorg {
                common_ancestor,
                old_tip: conflicting_old_tip,
                new_tip: block(20, B256::repeat_byte(0xf0), B256::repeat_byte(0xef)),
            },
        ]),
    )
    .expect_err("a dropped old tip cannot contradict its retained predecessor");
    assert!(!invalid_old_tip.requires_history());
    assert!(matches!(
        invalid_old_tip,
        CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
    ));

    let reused_hash = block(17, B256::repeat_byte(0x77), B256::repeat_byte(0x16));
    let ancestor = block(18, B256::repeat_byte(0x78), reused_hash.hash);
    let old_tip = block(19, B256::repeat_byte(0x79), ancestor.hash);
    let reused_state = CanonicalSequenceState::new(
        vec![reused_hash, ancestor, old_tip],
        Some(old_tip),
        None,
        None,
    );
    let invalid_new_tip = validate_canonical_sequence_diagnostic(
        &reused_state,
        &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
            ChainControl::Reorg {
                common_ancestor: ancestor,
                old_tip,
                new_tip: block(19, reused_hash.hash, ancestor.hash),
            },
        ]),
    )
    .expect_err("a new tip cannot reuse a retained hash from another height");
    assert!(!invalid_new_tip.requires_history());
    assert!(matches!(
        invalid_new_tip,
        CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
    ));
}

#[test]
fn sparse_explicit_reorg_ancestor_must_match_the_retained_old_branch() {
    let oldest = block(100, B256::repeat_byte(0x64), B256::repeat_byte(0x63));
    let ancestor = block(103, B256::repeat_byte(0x67), B256::repeat_byte(0x66));
    let retained_child = block(104, B256::repeat_byte(0x68), B256::repeat_byte(0xba));
    let old_tip = block(105, B256::repeat_byte(0x69), retained_child.hash);
    let new_tip = block(105, B256::repeat_byte(0xf5), B256::repeat_byte(0xf4));
    let conflicting_child_state = CanonicalSequenceState::new(
        vec![oldest, retained_child, old_tip],
        Some(old_tip),
        None,
        None,
    );
    let non_adjacent_old_tip = BlockRef {
        parent_hash: Some(ancestor.hash),
        ..old_tip
    };
    let non_adjacent_state = CanonicalSequenceState::new(
        vec![oldest, non_adjacent_old_tip],
        Some(non_adjacent_old_tip),
        None,
        None,
    );

    for (state, exact_old_tip) in [
        (conflicting_child_state, old_tip),
        (non_adjacent_state, non_adjacent_old_tip),
    ] {
        let error = validate_canonical_sequence_diagnostic(
            &state,
            &ReactiveInputBatch::<Ethereum>::new(Vec::new()).with_chain_controls([
                ChainControl::Reorg {
                    common_ancestor: ancestor,
                    old_tip: exact_old_tip,
                    new_tip,
                },
            ]),
        )
        .expect_err("the declared ancestor must agree with the retained old branch");
        assert!(!error.requires_history());
        assert!(matches!(
            error,
            CanonicalSequenceError::Invalid(ReactiveError::InvalidChainControl { .. })
        ));
    }
}

#[test]
fn explicit_input_identity_parts_enforce_object_representation_pairs() {
    let log_ref = InputRef::Log {
        chain_id: Some(1),
        block_hash: B256::repeat_byte(1),
        transaction_hash: B256::repeat_byte(2),
        log_index: 3,
    };
    let identity =
        ReactiveInputIdentity::try_from_parts(log_ref, ReactiveInputKind::ReorgSignalLog)
            .expect("a reorg signal is a valid log representation");
    assert_eq!(identity.input_ref(), log_ref);
    assert_eq!(identity.kind(), ReactiveInputKind::ReorgSignalLog);

    let error = ReactiveInputIdentity::try_from_parts(log_ref, ReactiveInputKind::FullBlock)
        .expect_err("a log reference cannot identify a full block representation");
    assert_eq!(error.input_ref(), log_ref);
    assert_eq!(error.kind(), ReactiveInputKind::FullBlock);
}

#[tokio::test]
async fn every_reorg_signal_is_control_only_even_when_nothing_can_be_rolled_back() -> Result<()> {
    let address = Address::repeat_byte(0x34);
    let slot = U256::from(44);
    let dropped = block(44, B256::repeat_byte(0x44), B256::repeat_byte(0x43));
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(7))?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: HandlerId::new("must-not-run-on-reorg"),
        address,
        slot,
        value: U256::from(99),
    }))?;

    let removed = || {
        ReactiveInputRecord::new(
            ReactiveInput::<Ethereum>::Log(rpc_log(
                address,
                vec![keccak256(b"Removed()")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped, 0),
        )
    };

    // Unknown/deep and repeated removals have no resident journal entry. They
    // are still lifecycle controls, never ordinary handler inputs.
    for record in [removed(), removed()] {
        runtime.ingest_batch(&mut cache, ReactiveInputBatch::new(vec![record]))?;
        assert_eq!(
            cache.cached_storage_value(address, slot),
            Some(U256::from(7))
        );
    }

    // Routing scope cannot turn a removal back into data. In particular, an
    // owner catch-up source must not replay a removed log into its handler.
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![removed()])
            .with_audience(DeliveryAudience::Owners(vec![HandlerId::new(
                "must-not-run-on-reorg",
            )]))
            .with_delivery_scope(DeliveryScope::OwnerCatchup),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(7))
    );
    Ok(())
}

#[tokio::test]
async fn same_batch_removed_old_branch_is_applied_before_its_replacement() -> Result<()> {
    let address = Address::repeat_byte(0x36);
    let slot = U256::from(36);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let old = block(80, B256::repeat_byte(0x80), parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), parent.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;

    for (canonical, value) in [(&parent, 10), (&old, 20)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Branch(uint256)")],
                    canonical,
                    0,
                    value,
                    false,
                )),
                included_context(*canonical, value),
            ),
        )?;
    }

    let replacement_record = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            address,
            vec![keccak256(b"Branch(uint256)")],
            &replacement,
            0,
            30,
            false,
        )),
        included_context(replacement, 30),
    );
    let removed_record = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            address,
            vec![keccak256(b"Branch(uint256)")],
            &old,
            0,
            20,
            true,
        )),
        reorged_context(old, 20),
    );
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![replacement_record, removed_record]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(replacement));
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(30)),
        "the old-branch lifecycle signal must run before replacement data"
    );
    Ok(())
}

#[tokio::test]
async fn distinct_same_batch_removals_share_one_rollback_without_losing_lifecycle_inputs()
-> Result<()> {
    let address = Address::repeat_byte(0x3a);
    let slot = U256::from(38);
    let block_10 = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x09));
    let block_11 = block(11, B256::repeat_byte(0x11), block_10.hash);
    let block_12 = block(12, B256::repeat_byte(0x12), block_11.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;

    for (canonical, value) in [(&block_10, 10), (&block_11, 11), (&block_12, 12)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Canonical(uint256)")],
                    canonical,
                    0,
                    value,
                    false,
                )),
                included_context(*canonical, value),
            ),
        )?;
    }

    let removed = |dropped: BlockRef, log_index: u64| {
        ReactiveInputRecord::new(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Removed(uint256)")],
                &dropped,
                0,
                log_index,
                true,
            )),
            reorged_context(dropped, log_index),
        )
    };
    let report = runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![
            removed(block_11, 0),
            removed(block_11, 1),
            removed(block_12, 2),
        ]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(block_10));
    assert_eq!(runtime.metrics().deep_reorgs, 0);
    assert_eq!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Healthy
    );
    assert_eq!(
        report
            .reports
            .iter()
            .filter(|report| matches!(report.as_ref(), ReactiveReport::Input(_)))
            .count(),
        3,
        "every distinct removed lifecycle record remains observable"
    );
    assert_eq!(
        report
            .reports
            .iter()
            .filter(|report| matches!(report.as_ref(), ReactiveReport::Reorg(_)))
            .count(),
        1,
        "one rollback must cover all later removals from the drained span"
    );
    Ok(())
}

#[tokio::test]
async fn explicit_reorg_coalesces_redundant_removed_records_with_and_without_replacement()
-> Result<()> {
    for include_replacement in [false, true] {
        let address = Address::repeat_byte(if include_replacement { 0x3c } else { 0x3b });
        let slot = U256::from(39);
        let ancestor = block(20, B256::repeat_byte(0x20), B256::repeat_byte(0x19));
        let old_tip = block(21, B256::repeat_byte(0x21), ancestor.hash);
        let replacement = block(21, B256::repeat_byte(0xa1), ancestor.hash);
        let mut cache = setup_cache().await?;
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;
        for (canonical, value) in [(&ancestor, 20), (&old_tip, 21)] {
            runtime.ingest_batch(
                &mut cache,
                batch(
                    ReactiveInput::Log(rpc_log(
                        address,
                        vec![keccak256(b"Explicit(uint256)")],
                        canonical,
                        0,
                        value,
                        false,
                    )),
                    included_context(*canonical, value),
                ),
            )?;
        }

        let mut records = vec![ReactiveInputRecord::new(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"RemovedAfterExplicit()")],
                &old_tip,
                0,
                7,
                true,
            )),
            reorged_context(old_tip, 7),
        )];
        if include_replacement {
            records.push(ReactiveInputRecord::new(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Explicit(uint256)")],
                    &replacement,
                    0,
                    31,
                    false,
                )),
                included_context(replacement, 31),
            ));
        }
        let report = runtime.ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(records)
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Reorg {
                    common_ancestor: ancestor,
                    old_tip,
                    new_tip: replacement,
                }]),
        )?;

        assert_eq!(
            runtime.last_canonical_block(),
            Some(if include_replacement {
                replacement
            } else {
                ancestor
            })
        );
        assert_eq!(runtime.metrics().deep_reorgs, 0);
        assert_eq!(
            runtime.health(),
            evm_fork_cache::reactive::CacheHealth::Healthy
        );
        assert_eq!(
            report
                .reports
                .iter()
                .filter(|report| matches!(report.as_ref(), ReactiveReport::Input(_)))
                .count(),
            records_len(include_replacement),
        );
    }
    Ok(())
}

const fn records_len(include_replacement: bool) -> usize {
    if include_replacement { 2 } else { 1 }
}

#[tokio::test]
async fn explicit_multiblock_reorg_coalesces_exact_intermediate_removed_logs() -> Result<()> {
    let address = Address::repeat_byte(0xb5);
    let ancestor = block(10, B256::repeat_byte(0x10), B256::repeat_byte(0x09));
    let old_11 = block(11, B256::repeat_byte(0x11), ancestor.hash);
    let old_tip = block(12, B256::repeat_byte(0x12), old_11.hash);
    let new_tip = block(12, B256::repeat_byte(0xb2), B256::repeat_byte(0xb1));
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for (canonical, log_index) in [(ancestor, 0), (old_11, 1), (old_tip, 2)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"ExplicitIntermediateRemoval()")],
                    &canonical,
                    0,
                    log_index,
                    false,
                )),
                included_context(canonical, log_index),
            ),
        )?;
    }

    let report = runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![sequence_log_record(old_11, 3, true)])
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Reorg {
                common_ancestor: ancestor,
                old_tip,
                new_tip,
            }]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(ancestor));
    assert_eq!(runtime.metrics().deep_reorgs, 0);
    assert_eq!(runtime.metrics().reorgs_recovered, 1);
    assert_eq!(
        report
            .reports
            .iter()
            .filter(|report| matches!(report.as_ref(), ReactiveReport::Reorg(_)))
            .count(),
        1,
        "the intermediate removed log is observable as input but does not rerun recovery"
    );
    Ok(())
}

#[tokio::test]
async fn removed_sole_tip_can_be_replaced_by_zero_event_progress_or_barrier() -> Result<()> {
    for use_barrier in [false, true] {
        let address = Address::repeat_byte(if use_barrier { 0x3e } else { 0x3d });
        let dropped = block(1, B256::repeat_byte(0x01), B256::ZERO);
        let replacement = block(1, B256::repeat_byte(0xf1), B256::ZERO);
        let mut cache = setup_cache().await?;
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
            journal_depth: 1,
            ..ReactiveConfig::default()
        });
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Tip()")],
                    &dropped,
                    0,
                    0,
                    false,
                )),
                included_context(dropped, 0),
            ),
        )?;
        let removed = ReactiveInputRecord::new(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Tip()")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped, 0),
        );
        let progress = if use_barrier {
            ChainControl::Barrier {
                id: b"zero-event-replacement".to_vec(),
                block: Some(replacement),
            }
        } else {
            ChainControl::CanonicalProgress(replacement)
        };

        runtime.ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![removed])
                .with_chain_id(1)
                .with_chain_controls([progress]),
        )?;
        assert_eq!(runtime.last_canonical_block(), Some(replacement));
        assert_eq!(runtime.metrics().deep_reorgs, 0);
    }
    Ok(())
}

#[test]
fn zero_event_replacement_rejects_unverifiable_outside_window_conflicting_and_finalized_paths() {
    let address = Address::repeat_byte(0x3f);
    let dropped = block(1, B256::repeat_byte(0x01), B256::ZERO);
    let replacement = block(1, B256::repeat_byte(0xf1), B256::ZERO);
    let removed = |block: BlockRef| {
        ReactiveInputRecord::new(
            ReactiveInput::<Ethereum>::Log(rpc_log(
                address,
                vec![keccak256(b"Tip()")],
                &block,
                0,
                0,
                true,
            )),
            reorged_context(block, 0),
        )
    };
    let replacement_batch = |record, controls| {
        ReactiveInputBatch::new(vec![record])
            .with_chain_id(1)
            .with_chain_controls(controls)
    };

    let unverifiable = BlockRef {
        parent_hash: None,
        ..dropped
    };
    let state = CanonicalSequenceState::new(vec![unverifiable], Some(unverifiable), None, None);
    assert!(matches!(
        validate_canonical_sequence(
            &state,
            &replacement_batch(
                removed(unverifiable),
                vec![ChainControl::CanonicalProgress(replacement)],
            ),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let retained_tip = block(2, B256::repeat_byte(0x02), dropped.hash);
    let outside_window =
        CanonicalSequenceState::new(vec![retained_tip], Some(retained_tip), None, None);
    assert!(matches!(
        validate_canonical_sequence(
            &outside_window,
            &replacement_batch(
                removed(dropped),
                vec![ChainControl::CanonicalProgress(replacement)],
            ),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let retained = CanonicalSequenceState::new(vec![dropped], Some(dropped), None, None);
    let removed_only = ReactiveInputBatch::new(vec![removed(dropped)]);
    let removed_only = validate_canonical_sequence(&retained, &removed_only)
        .expect("the removed block authenticates its exact parent as new coverage");
    assert_eq!(
        removed_only.next_state().coverage_head(),
        Some(&BlockRef {
            number: 0,
            hash: dropped.parent_hash.unwrap(),
            parent_hash: None,
            timestamp: None,
        })
    );
    let conflicting = BlockRef {
        hash: B256::repeat_byte(0xf2),
        ..replacement
    };
    assert!(matches!(
        validate_canonical_sequence(
            &retained,
            &replacement_batch(
                removed(dropped),
                vec![
                    ChainControl::CanonicalProgress(replacement),
                    ChainControl::CanonicalProgress(conflicting),
                ],
            ),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));

    let finalized =
        CanonicalSequenceState::new(vec![dropped], Some(dropped), Some(dropped), Some(dropped));
    assert!(matches!(
        validate_canonical_sequence(
            &finalized,
            &replacement_batch(
                removed(dropped),
                vec![ChainControl::CanonicalProgress(replacement)],
            ),
        ),
        Err(ReactiveError::InvalidChainControl { .. })
    ));
}

#[tokio::test]
async fn delayed_duplicate_removed_log_cannot_drain_a_different_canonical_hash() -> Result<()> {
    let address = Address::repeat_byte(0x37);
    let slot = U256::from(37);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let old = block(80, B256::repeat_byte(0x80), parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), parent.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;

    for (canonical, value) in [(&parent, 10), (&old, 20), (&replacement, 30)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Delayed(uint256)")],
                    canonical,
                    0,
                    value,
                    false,
                )),
                included_context(*canonical, value),
            ),
        )?;
    }

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Delayed(uint256)")],
                &old,
                0,
                20,
                true,
            )),
            reorged_context(old, 20),
        ),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(replacement));
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(30))
    );
    Ok(())
}

#[tokio::test]
async fn owner_catchup_revalidates_its_journal_entry_at_the_mutation_boundary() -> Result<()> {
    let address = Address::repeat_byte(0x38);
    let slot = U256::from(38);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let old = block(80, B256::repeat_byte(0x80), parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), parent.hash);
    let owner = HandlerId::new("log-index-slot-writer");
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;

    for (canonical, value) in [(&parent, 10), (&old, 20)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"OwnerRace(uint256)")],
                    canonical,
                    0,
                    value,
                    false,
                )),
                included_context(*canonical, value),
            ),
        )?;
    }

    let replacement_record = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            address,
            vec![keccak256(b"OwnerRace(uint256)")],
            &replacement,
            0,
            30,
            false,
        )),
        included_context(replacement, 30),
    );
    let owner_record = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            address,
            vec![keccak256(b"OwnerRace(uint256)")],
            &old,
            0,
            40,
            false,
        )),
        included_context(old, 40),
    );
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::from_deliveries([
                ReactiveInputDelivery::new(
                    replacement_record,
                    DeliveryAudience::All,
                    DeliveryScope::Canonical,
                ),
                ReactiveInputDelivery::new(
                    owner_record,
                    DeliveryAudience::Owners(vec![owner]),
                    DeliveryScope::OwnerCatchup,
                ),
            ]),
        )
        .expect_err("replacement must not strand an irreversible owner mutation");

    assert!(matches!(
        error,
        ReactiveError::OwnerCatchupOutsideJournal {
            number: 80,
            hash,
        } if hash == old.hash
    ));
    assert_eq!(runtime.last_canonical_block(), Some(old));
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20)),
        "the complete mixed batch must roll back transactionally"
    );
    Ok(())
}

#[tokio::test]
async fn canonical_delivery_continues_from_barrier_coverage_without_false_gap() -> Result<()> {
    let address = Address::repeat_byte(0x35);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: HandlerId::new("barrier-continuity"),
        address,
        slot,
        value: U256::from(1),
    }))?;

    let block_90 = block(90, B256::repeat_byte(90), B256::repeat_byte(89));
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Event()")],
                &block_90,
                0,
                0,
                false,
            )),
            included_context(block_90, 0),
        ),
    )?;
    let block_100 = block(100, B256::repeat_byte(100), B256::repeat_byte(99));
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Barrier {
                id: b"covered-through-100".to_vec(),
                block: Some(block_100),
            }]),
    )?;

    let block_101 = block(101, B256::repeat_byte(101), block_100.hash);
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Event()")],
                &block_101,
                0,
                0,
                false,
            )),
            included_context(block_101, 0),
        ),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(block_101));
    assert_eq!(runtime.metrics().missed_ranges, 0);
    assert_eq!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Healthy
    );
    Ok(())
}

#[tokio::test]
async fn compact_canonical_progress_advances_coverage_without_a_fabricated_header() -> Result<()> {
    let mut cache = setup_cache().await?;
    cache.set_block_context(Some(199), Some(7));
    cache.set_coinbase(Some(Address::repeat_byte(0xcc)));
    cache.set_prevrandao(Some(B256::repeat_byte(0xdd)));
    cache.set_block_gas_limit(Some(30_000_000));
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let progress = block(200, B256::repeat_byte(200), B256::repeat_byte(199));

    let report = runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(progress)]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(progress));
    assert_eq!(cache.block(), BlockId::from((progress.hash, Some(true))));
    assert_eq!(cache.block_number(), Some(progress.number));
    assert_eq!(cache.timestamp(), progress.timestamp);
    assert_eq!(cache.basefee(), None);
    assert_eq!(cache.coinbase(), None);
    assert_eq!(cache.prevrandao(), None);
    assert_eq!(cache.block_gas_limit(), None);
    assert!(report.applied.is_empty());
    assert!(report.reports.iter().any(|report| matches!(
        report.as_ref(),
        ReactiveReport::ChainControl(control)
            if control.control == ChainControl::CanonicalProgress(progress)
    )));
    Ok(())
}

#[tokio::test]
async fn certified_sparse_backfill_keeps_zero_event_tail_and_does_not_report_live_gap() -> Result<()>
{
    let address = Address::repeat_byte(0x36);
    let slot = U256::from(1);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let owner = HandlerId::new("sparse-backfill");
    runtime.register_handler(Arc::new(SlotWriter {
        id: owner.clone(),
        address,
        slot,
        value: U256::from(9),
    }))?;

    let baseline = block(100, B256::repeat_byte(100), B256::repeat_byte(99));
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(baseline)]),
    )?;
    let event_block = block(105, B256::repeat_byte(105), B256::repeat_byte(104));
    let certified_tail = block(110, B256::repeat_byte(110), B256::repeat_byte(109));
    let report = runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Event()")],
                &event_block,
                0,
                0,
                false,
            )),
            included_context(event_block, 0),
        )])
        .with_delivery_scope(DeliveryScope::CanonicalProgress)
        .with_chain_controls([ChainControl::Barrier {
            id: b"certified-through-110".to_vec(),
            block: Some(certified_tail),
        }]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(certified_tail));
    assert_eq!(
        cache.block(),
        BlockId::from((certified_tail.hash, Some(true)))
    );
    assert_eq!(cache.block_number(), Some(certified_tail.number));
    assert_eq!(cache.timestamp(), certified_tail.timestamp);
    assert_eq!(runtime.metrics().missed_ranges, 0);
    assert!(
        !report
            .reports
            .iter()
            .any(|report| matches!(report.as_ref(), ReactiveReport::Reorg(_)))
    );

    // The zero-log tail is a real retained anchor, so a newly registered
    // owner's inclusive catch-up at the certified head remains rollbackable.
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Event()")],
                &certified_tail,
                0,
                1,
                false,
            )),
            included_context(certified_tail, 1),
        )])
        .with_audience(DeliveryAudience::Owners(vec![owner]))
        .with_delivery_scope(DeliveryScope::OwnerCatchup),
    )?;
    Ok(())
}

#[tokio::test]
async fn same_head_partial_log_cannot_downgrade_canonical_metadata() -> Result<()> {
    let address = Address::repeat_byte(0x37);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let full = block(120, B256::repeat_byte(120), B256::repeat_byte(119));
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(full)]),
    )?;

    let partial = BlockRef {
        parent_hash: None,
        timestamp: None,
        ..full
    };
    let mut log = rpc_log(
        address,
        vec![keccak256(b"Metadata()")],
        &partial,
        0,
        0,
        false,
    );
    log.block_timestamp = None;
    runtime.ingest_batch(
        &mut cache,
        batch(ReactiveInput::Log(log), included_context(partial, 0)),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(full));
    assert_eq!(cache.timestamp(), full.timestamp);
    Ok(())
}

#[tokio::test]
async fn control_only_batches_require_the_cache_chain_identity() -> Result<()> {
    let mut cache = setup_cache().await?;
    let progress = block(200, B256::repeat_byte(200), B256::repeat_byte(199));

    for batch in [
        ReactiveInputBatch::new(Vec::new())
            .with_chain_controls([ChainControl::CanonicalProgress(progress)]),
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(2)
            .with_chain_controls([ChainControl::CanonicalProgress(progress)]),
    ] {
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        let error = runtime
            .ingest_batch(&mut cache, batch)
            .expect_err("unbound or cross-chain controls must fail closed");
        assert!(matches!(
            error,
            ReactiveError::InvalidChainControl { .. } | ReactiveError::InvalidInputRecord { .. }
        ));
        assert!(runtime.last_canonical_block().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn explicit_reorg_rejects_noop_and_same_height_non_descendant_triples() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let current = block(30, B256::repeat_byte(30), B256::repeat_byte(29));
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(current)]),
    )?;

    for new_tip in [
        current,
        block(30, B256::repeat_byte(31), B256::repeat_byte(29)),
    ] {
        let error = runtime
            .ingest_batch(
                &mut cache,
                ReactiveInputBatch::new(Vec::new())
                    .with_chain_id(1)
                    .with_chain_controls([ChainControl::Reorg {
                        common_ancestor: current,
                        old_tip: current,
                        new_tip,
                    }]),
            )
            .expect_err("a reorg must replace a non-empty branch above its ancestor");
        assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
        assert_eq!(runtime.last_canonical_block(), Some(current));
    }
    Ok(())
}

struct SlotWriter {
    id: HandlerId,
    address: Address,
    slot: U256,
    value: U256,
}

impl ReactiveHandler<Ethereum> for SlotWriter {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                self.value,
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

struct LogIndexSlotWriter {
    address: Address,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for LogIndexSlotWriter {
    fn id(&self) -> HandlerId {
        HandlerId::new("log-index-slot-writer")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot(
                self.address,
                self.slot,
                U256::from(ctx.log_index.expect("test log context carries index")),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: vec![],
        })
    }
}

struct PurgeHandler {
    address: Address,
}

impl ReactiveHandler<Ethereum> for PurgeHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("purge-handler")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::Invalidate(InvalidationRequest {
                scope: PurgeScope::AllStorage,
                address: self.address,
                reason: InvalidationReason::HandlerRequested,
            })],
            quality: StateEffectQuality::RequiresRepair,
            tags: vec![],
        })
    }
}

struct ResyncOnlyHandler {
    address: Address,
    slot: U256,
    block: ResyncBlock,
}

impl ReactiveHandler<Ethereum> for ResyncOnlyHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("resync-only")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.address),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::Resync(ResyncRequest {
                id: ResyncId::new("hash-pinned-repair"),
                reason: ResyncReason::HandlerRequested,
                block: self.block.clone(),
                targets: vec![ResyncTarget::StorageSlot {
                    address: self.address,
                    slot: self.slot,
                }],
                priority: ResyncPriority::High,
            })],
            quality: StateEffectQuality::AppliedWithPendingResync,
            tags: vec![],
        })
    }
}

#[tokio::test]
async fn reactive_runtime_rolls_back_hash_pinned_resync_effects_with_dropped_block() -> Result<()> {
    let address = Address::repeat_byte(0xa5);
    let slot = U256::from(10);
    let dropped = block(110, B256::repeat_byte(0xbb), B256::repeat_byte(0xab));
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(10))?;
    cache.set_storage_batch_fetcher(Arc::new(move |requests, _block| {
        requests
            .into_iter()
            .map(|(address, slot)| (address, slot, Ok(U256::from(42))))
            .collect()
    }));

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: HandlerId::new("provisional-writer"),
        address,
        slot,
        value: U256::from(20),
    }))?;
    runtime.register_handler(Arc::new(ResyncOnlyHandler {
        address,
        slot,
        block: ResyncBlock::Hash {
            number: dropped.number,
            hash: dropped.hash,
            require_canonical: true,
        },
    }))?;

    runtime.ingest_batch_with_resync(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"WriteThenRepair()")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped, 0),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(42))
    );

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"WriteThenRepair()")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped, 0),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(10)),
        "reorg rollback must unwind authoritative resync writes as well as direct effects"
    );
    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("removed log emits a reorg report");
    assert_eq!(reorg.rollback_updates.len(), 2);
    assert_eq!(reorg.rollback_diff.slots[0].old, U256::from(42));
    assert_eq!(reorg.rollback_diff.slots[0].new, U256::from(20));
    assert_eq!(reorg.rollback_diff.slots[1].old, U256::from(20));
    assert_eq!(reorg.rollback_diff.slots[1].new, U256::from(10));

    Ok(())
}

#[tokio::test]
async fn reactive_runtime_rolls_back_removed_log_storage_effects() -> Result<()> {
    let address = Address::repeat_byte(0xa1);
    let slot = U256::from(7);
    let dropped = block(70, B256::repeat_byte(0x70), B256::repeat_byte(0x6f));
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(10))?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: HandlerId::new("slot-writer"),
        address,
        slot,
        value: U256::from(20),
    }))?;

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Write(uint256)")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped, 0),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20))
    );

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Write(uint256)")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped, 0),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(10)),
        "removed logs should roll back reversible storage writes"
    );
    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("removed log emits a reorg report");
    assert_eq!(reorg.dropped_blocks, vec![dropped]);
    assert_eq!(reorg.rollback_updates.len(), 1);
    assert!(reorg.purge_updates.is_empty());
    assert_eq!(reorg.rollback_diff.slots[0].old, U256::from(20));
    assert_eq!(reorg.rollback_diff.slots[0].new, U256::from(10));

    Ok(())
}

#[tokio::test]
async fn explicit_chain_controls_are_ordered_with_delivery_and_rollback_state() -> Result<()> {
    let address = Address::repeat_byte(0xa9);
    let slot = U256::from(19);
    let ancestor = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89));
    let old_tip = block(91, B256::repeat_byte(0x91), ancestor.hash);
    let new_tip = block(91, B256::repeat_byte(0xa1), ancestor.hash);
    let safe = block(89, B256::repeat_byte(0x89), B256::repeat_byte(0x88));
    let finalized = block(88, B256::repeat_byte(0x88), B256::repeat_byte(0x87));
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;
    for (block, value) in [(&ancestor, 20), (&old_tip, 30)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Controlled(uint256)")],
                    block,
                    0,
                    value,
                    false,
                )),
                included_context(*block, value),
            ),
        )?;
    }
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(30))
    );

    let report = runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([
                ChainControl::Reorg {
                    common_ancestor: ancestor,
                    old_tip,
                    new_tip,
                },
                ChainControl::Safe(safe),
                ChainControl::Finalized(finalized),
                ChainControl::Barrier {
                    id: b"catchup-complete".to_vec(),
                    block: Some(ancestor),
                },
            ]),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20))
    );
    assert_eq!(runtime.last_canonical_block(), Some(ancestor));
    assert_eq!(runtime.safe_head(), Some(&safe));
    assert_eq!(runtime.finalized_head(), Some(&finalized));
    assert!(report.reports.iter().any(|report| matches!(
        report.as_ref(),
        ReactiveReport::Reorg(reorg)
            if reorg.reason == evm_fork_cache::reactive::ReorgReason::Explicit
                && reorg.dropped_blocks == vec![old_tip]
    )));
    let controls: Vec<_> = report
        .reports
        .iter()
        .filter_map(|report| match report.as_ref() {
            ReactiveReport::ChainControl(report) => Some(report.control.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        controls,
        vec![
            ChainControl::Reorg {
                common_ancestor: block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89),),
                old_tip,
                new_tip,
            },
            ChainControl::Safe(safe),
            ChainControl::Finalized(finalized),
            ChainControl::Barrier {
                id: b"catchup-complete".to_vec(),
                block: Some(block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89),)),
            },
        ]
    );

    Ok(())
}

#[tokio::test]
async fn reorg_ancestor_in_zero_event_gap_rolls_back_without_false_deep_reorg() -> Result<()> {
    let address = Address::repeat_byte(0xb1);
    let slot = U256::from(23);
    let retained = block(100, B256::repeat_byte(0x64), B256::repeat_byte(0x63));
    let old_tip = block(105, B256::repeat_byte(0x69), B256::repeat_byte(0x68));
    let ancestor = block(103, B256::repeat_byte(0x67), B256::repeat_byte(0x66));
    let new_tip = block(105, B256::repeat_byte(0xf5), B256::repeat_byte(0xf4));
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Gap(uint256)")],
                &retained,
                0,
                20,
                false,
            )),
            included_context(retained, 20),
        ),
    )?;
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Gap(uint256)")],
                &old_tip,
                0,
                30,
                false,
            )),
            included_context(old_tip, 30),
        )])
        .with_delivery_scope(DeliveryScope::CanonicalProgress)
        .with_chain_controls([ChainControl::Barrier {
            id: b"sparse-old-tip".to_vec(),
            block: Some(old_tip),
        }]),
    )?;
    cache.with_blockchain_db_mut(|database| {
        database
            .block_hashes()
            .write()
            .insert(U256::from(old_tip.number), old_tip.hash);
    });

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Reorg {
                common_ancestor: ancestor,
                old_tip,
                new_tip,
            }]),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20))
    );
    assert_eq!(runtime.last_canonical_block(), Some(ancestor));
    assert_eq!(runtime.metrics().deep_reorgs, 0);
    assert_eq!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Healthy
    );
    assert!(
        cache
            .unchecked_blockchain_db()
            .block_hashes()
            .read()
            .get(&U256::from(old_tip.number))
            .is_none(),
        "a re-pin must not retain BLOCKHASH values from the displaced branch"
    );
    Ok(())
}

#[tokio::test]
async fn reorg_below_oldest_retained_entry_remains_observable_as_deep() -> Result<()> {
    let address = Address::repeat_byte(0xb2);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
        journal_depth: 2,
        ..ReactiveConfig::default()
    });
    let block_104 = block(104, B256::repeat_byte(0x68), B256::repeat_byte(0x67));
    let old_tip = block(105, B256::repeat_byte(0x69), block_104.hash);
    for current in [block_104, old_tip] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Deep()")],
                    &current,
                    0,
                    0,
                    false,
                )),
                included_context(current, 0),
            ),
        )?;
    }
    let ancestor = block(103, B256::repeat_byte(0x67), B256::repeat_byte(0x66));
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Reorg {
                common_ancestor: ancestor,
                old_tip,
                new_tip: block(105, B256::repeat_byte(0xf5), B256::repeat_byte(0xf4)),
            }]),
    )?;

    assert_eq!(runtime.metrics().deep_reorgs, 1);
    assert!(matches!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Degraded { since_block: 104 }
    ));
    Ok(())
}

#[tokio::test]
async fn contradictory_chain_controls_fail_before_mutating_runtime_state() -> Result<()> {
    let address = Address::repeat_byte(0xaa);
    let slot = U256::from(21);
    let handler_id = HandlerId::new("atomic-control-writer");
    let tip = block(95, B256::repeat_byte(0x95), B256::repeat_byte(0x94));
    let finalized = block(94, B256::repeat_byte(0x94), B256::repeat_byte(0x93));
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(10))?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: handler_id.clone(),
        address,
        slot,
        value: U256::from(20),
    }))?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Tip()")],
                &tip,
                0,
                0,
                false,
            )),
            included_context(tip, 0),
        ),
    )?;
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Finalized(finalized)]),
    )?;

    let cache_before = cache.cached_storage_value(address, slot);
    let coverage_before = runtime.last_canonical_block();
    let safe_before = runtime.safe_head().cloned();
    let finalized_before = runtime.finalized_head().cloned();
    let health_before = runtime.health();
    let metrics_before = runtime.metrics();
    let resyncs_before = runtime.pending_resyncs().to_vec();
    let journaled_before = runtime.has_journaled_handler_effects(&handler_id);

    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([
                    ChainControl::Reorg {
                        common_ancestor: finalized,
                        old_tip: tip,
                        new_tip: block(95, B256::repeat_byte(0xa5), finalized.hash),
                    },
                    ChainControl::Finalized(block(
                        93,
                        B256::repeat_byte(0x93),
                        B256::repeat_byte(0x92),
                    )),
                ]),
        )
        .expect_err("a later invalid control must reject the whole batch");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(cache.cached_storage_value(address, slot), cache_before);
    assert_eq!(runtime.last_canonical_block(), coverage_before);
    assert_eq!(runtime.safe_head(), safe_before.as_ref());
    assert_eq!(runtime.finalized_head(), finalized_before.as_ref());
    assert_eq!(runtime.health(), health_before);
    assert_eq!(runtime.metrics(), metrics_before);
    assert_eq!(runtime.pending_resyncs(), resyncs_before);
    assert_eq!(
        runtime.has_journaled_handler_effects(&handler_id),
        journaled_before
    );

    let barrier = block(100, B256::repeat_byte(0x10), B256::repeat_byte(0x99));
    let conflicting_finality = block(100, B256::repeat_byte(0x20), B256::repeat_byte(0x99));
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([
                    ChainControl::Barrier {
                        id: b"coverage-100".to_vec(),
                        block: Some(barrier),
                    },
                    ChainControl::Finalized(conflicting_finality),
                ]),
        )
        .expect_err("finality must agree with coverage advanced earlier in the batch");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), coverage_before);
    assert_eq!(runtime.finalized_head(), finalized_before.as_ref());

    let mismatched_old_tip = block(95, B256::repeat_byte(0xe5), finalized.hash);
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Reorg {
                    common_ancestor: finalized,
                    old_tip: mismatched_old_tip,
                    new_tip: block(95, B256::repeat_byte(0xf5), finalized.hash),
                }]),
        )
        .expect_err("mismatched old tip must fail closed");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), Some(tip));
    assert_eq!(runtime.finalized_head(), Some(&finalized));

    let regression = block(93, B256::repeat_byte(0x93), B256::repeat_byte(0x92));
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Finalized(regression)]),
        )
        .expect_err("finality regression must fail closed");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.finalized_head(), Some(&finalized));

    let conflicting_tip = block(95, B256::repeat_byte(0xc5), finalized.hash);
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Finalized(conflicting_tip)]),
        )
        .expect_err("finality must agree with a known canonical block hash");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));

    let bad_parent = block(95, tip.hash, B256::repeat_byte(0xee));
    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Safe(bad_parent)]),
        )
        .expect_err("an adjacent safe head must descend from finalized");
    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), Some(tip));
    assert_eq!(runtime.finalized_head(), Some(&finalized));

    Ok(())
}

#[tokio::test]
async fn same_hash_control_metadata_conflict_fails_before_mutation() -> Result<()> {
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    let first = block(96, B256::repeat_byte(0x96), B256::repeat_byte(0x95));
    let conflicting = BlockRef {
        timestamp: first.timestamp.map(|timestamp| timestamp + 1),
        ..first
    };
    let cache_generation = cache.snapshot_generation();

    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([
                    ChainControl::CanonicalProgress(first),
                    ChainControl::Barrier {
                        id: b"conflicting-metadata".to_vec(),
                        block: Some(conflicting),
                    },
                ]),
        )
        .expect_err("same hash with conflicting timestamp must fail closed");

    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), None);
    assert_eq!(cache.snapshot_generation(), cache_generation);
    Ok(())
}

#[tokio::test]
async fn chain_controls_and_records_are_validated_as_one_atomic_sequence() -> Result<()> {
    let address = Address::repeat_byte(0xac);
    let slot = U256::from(23);
    let certified = block(100, B256::repeat_byte(0x10), B256::repeat_byte(0x09));
    let conflicting = block(100, B256::repeat_byte(0x20), B256::repeat_byte(0x09));
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(10))?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(SlotWriter {
        id: HandlerId::new("joint-preflight-writer"),
        address,
        slot,
        value: U256::from(20),
    }))?;
    let record = ReactiveInputRecord::new(
        ReactiveInput::Log(rpc_log(
            address,
            vec![keccak256(b"Conflict()")],
            &conflicting,
            0,
            0,
            false,
        )),
        included_context(conflicting, 0),
    );

    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![record]).with_chain_controls([ChainControl::Barrier {
                id: b"certified-100".to_vec(),
                block: Some(certified),
            }]),
        )
        .expect_err("a record cannot contradict an earlier control in the same batch");

    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), None);
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(10))
    );
    assert!(!runtime.has_journaled_handler_effects(&HandlerId::new("joint-preflight-writer")));
    Ok(())
}

#[tokio::test]
async fn reorg_progress_then_safe_accepts_the_replacement_branch_in_one_control_batch() -> Result<()>
{
    let address = Address::repeat_byte(0xab);
    let ancestor = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89));
    let old_tip = block(91, B256::repeat_byte(0x91), ancestor.hash);
    let new_tip = block(91, B256::repeat_byte(0xa1), ancestor.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    for canonical in [&ancestor, &old_tip] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"ReplacementBranch()")],
                    canonical,
                    0,
                    0,
                    false,
                )),
                included_context(*canonical, 0),
            ),
        )?;
    }
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Finalized(ancestor)]),
    )?;

    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([
                ChainControl::Reorg {
                    common_ancestor: ancestor,
                    old_tip,
                    new_tip,
                },
                ChainControl::CanonicalProgress(new_tip),
                ChainControl::Safe(new_tip),
            ]),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(new_tip));
    assert_eq!(runtime.finalized_head(), Some(&ancestor));
    assert_eq!(runtime.safe_head(), Some(&new_tip));

    Ok(())
}

#[tokio::test]
async fn explicit_reorg_rejects_a_fabricated_common_ancestor_hash() -> Result<()> {
    let address = Address::repeat_byte(0xad);
    let ancestor = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x89));
    let middle = block(91, B256::repeat_byte(0x91), ancestor.hash);
    let old_tip = block(92, B256::repeat_byte(0x92), middle.hash);
    let fabricated = block(90, B256::repeat_byte(0xf0), ancestor.parent_hash.unwrap());
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for canonical in [&ancestor, &middle, &old_tip] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Canonical()")],
                    canonical,
                    0,
                    0,
                    false,
                )),
                included_context(*canonical, 0),
            ),
        )?;
    }

    let error = runtime
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Reorg {
                    common_ancestor: fabricated,
                    old_tip,
                    new_tip: block(92, B256::repeat_byte(0xa2), B256::repeat_byte(0xa1)),
                }]),
        )
        .expect_err("a retained canonical ancestor has one authoritative hash");

    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), Some(old_tip));
    assert_eq!(runtime.metrics().deep_reorgs, 0);
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_reorgs_parent_mismatch_before_replacement_block() -> Result<()> {
    let address = Address::repeat_byte(0xa2);
    let slot = U256::from(8);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let dropped = block(80, B256::repeat_byte(0x80), parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), parent.hash);
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Base(uint256)")],
                &parent,
                0,
                10,
                false,
            )),
            included_context(parent, 10),
        ),
    )?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Dropped(uint256)")],
                &dropped,
                0,
                20,
                false,
            )),
            included_context(dropped, 20),
        ),
    )?;
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20))
    );

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Replacement(uint256)")],
                &replacement,
                0,
                30,
                false,
            )),
            included_context(replacement, 30),
        ),
    )?;

    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(30)),
        "replacement block should apply after the dropped block is rolled back"
    );
    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("replacement block emits a parent-mismatch reorg report");
    assert_eq!(reorg.dropped_blocks, vec![dropped]);
    assert_eq!(reorg.rollback_updates.len(), 1);
    assert!(reorg
        .dropped_inputs
        .iter()
        .any(|input| matches!(input, evm_fork_cache::reactive::InputRef::Log { block_hash, .. } if *block_hash == B256::repeat_byte(0x80))));

    Ok(())
}

#[tokio::test]
async fn implicit_parent_mismatch_cannot_replace_finalized_state() -> Result<()> {
    let address = Address::repeat_byte(0xa6);
    let slot = U256::from(28);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let finalized = block(80, B256::repeat_byte(0x80), parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), parent.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(LogIndexSlotWriter { address, slot }))?;
    for (canonical, value) in [(&parent, 10), (&finalized, 20)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"FinalizedBranch(uint256)")],
                    canonical,
                    0,
                    value,
                    false,
                )),
                included_context(*canonical, value),
            ),
        )?;
    }
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([
                ChainControl::Safe(finalized),
                ChainControl::Finalized(finalized),
            ]),
    )?;

    let error = runtime
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"ConflictingBranch(uint256)")],
                    &replacement,
                    0,
                    30,
                    false,
                )),
                included_context(replacement, 30),
            ),
        )
        .expect_err("implicit rollback must not cross finalized state");

    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(
        cache.cached_storage_value(address, slot),
        Some(U256::from(20))
    );
    assert_eq!(runtime.last_canonical_block(), Some(finalized));
    assert_eq!(runtime.safe_head(), Some(&finalized));
    assert_eq!(runtime.finalized_head(), Some(&finalized));
    Ok(())
}

#[tokio::test]
async fn unknown_parent_replacement_overwrites_the_stale_parent_blockhash() -> Result<()> {
    let address = Address::repeat_byte(0xa8);
    let old_parent_hash = B256::repeat_byte(0x79);
    let stale_grandparent_hash = B256::repeat_byte(0x78);
    let replacement_parent_hash = B256::repeat_byte(0xe9);
    let old = block(80, B256::repeat_byte(0x80), old_parent_hash);
    let replacement = block(80, B256::repeat_byte(0x81), replacement_parent_hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"UnknownParent()")],
                &old,
                0,
                0,
                false,
            )),
            included_context(old, 0),
        ),
    )?;
    cache.with_blockchain_db_mut(|database| {
        let mut hashes = database.block_hashes().write();
        hashes.insert(U256::from(79), old_parent_hash);
        hashes.insert(U256::from(78), stale_grandparent_hash);
    });
    cache
        .db_mut()
        .cache
        .block_hashes
        .insert(U256::from(78), stale_grandparent_hash);
    let displaced_snapshot = cache.snapshot();
    assert_eq!(displaced_snapshot.block_hash(79), Some(old_parent_hash));
    assert_eq!(
        displaced_snapshot.block_hash(78),
        Some(stale_grandparent_hash)
    );

    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"UnknownParent()")],
                &replacement,
                0,
                1,
                false,
            )),
            included_context(replacement, 1),
        ),
    )?;

    assert_eq!(runtime.last_canonical_block(), Some(replacement));
    let replacement_snapshot = cache.snapshot();
    assert_eq!(
        replacement_snapshot.block_hash(79),
        Some(replacement_parent_hash)
    );
    assert_eq!(replacement_snapshot.block_hash(78), None);
    assert_eq!(
        displaced_snapshot.block_hash(79),
        Some(old_parent_hash),
        "reorg recovery must not rewrite an already issued snapshot"
    );
    assert_eq!(
        displaced_snapshot.block_hash(78),
        Some(stale_grandparent_hash),
        "reorg invalidation must remain point-in-time for prior snapshots"
    );
    assert_eq!(
        cache
            .unchecked_blockchain_db()
            .block_hashes()
            .read()
            .get(&U256::from(79))
            .copied(),
        Some(replacement_parent_hash),
        "the arriving exact parent identity must replace stale BLOCKHASH(N-1)"
    );
    assert!(
        cache
            .unchecked_blockchain_db()
            .block_hashes()
            .read()
            .get(&U256::from(78))
            .is_none(),
        "an unknown parent does not authenticate stale BLOCKHASH(N-2) in the backend layer"
    );
    assert!(
        !cache
            .db_mut()
            .cache
            .block_hashes
            .contains_key(&U256::from(78)),
        "an unknown parent does not authenticate stale BLOCKHASH(N-2) in the revm layer"
    );
    Ok(())
}

#[tokio::test]
async fn direct_runtime_keeps_an_unproven_parent_replacement_observable() -> Result<()> {
    let address = Address::repeat_byte(0xaf);
    let parent = block(89, B256::repeat_byte(0x89), B256::repeat_byte(0x88));
    let old_tip = block(90, B256::repeat_byte(0x90), parent.hash);
    let replacement = block(90, B256::repeat_byte(0xf0), B256::repeat_byte(0xee));
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    for (canonical, log_index) in [(parent, 0), (old_tip, 1), (replacement, 2)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"ObservableDeepReorg()")],
                    &canonical,
                    0,
                    log_index,
                    false,
                )),
                included_context(canonical, log_index),
            ),
        )?;
    }

    assert_eq!(runtime.last_canonical_block(), Some(replacement));
    assert_eq!(runtime.metrics().deep_reorgs, 1);
    assert!(matches!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Degraded { since_block: 90 }
    ));

    let child = block(91, B256::repeat_byte(0xf1), replacement.hash);
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"ObservableDeepReorg()")],
                &child,
                0,
                3,
                false,
            )),
            included_context(child, 3),
        ),
    )?;
    assert_eq!(runtime.last_canonical_block(), Some(child));
    assert_eq!(runtime.metrics().deep_reorgs, 1);
    Ok(())
}

#[tokio::test]
async fn direct_runtime_keeps_a_parentless_implicit_replacement_observable() -> Result<()> {
    let address = Address::repeat_byte(0xb5);
    let parent = block(99, B256::repeat_byte(0x99), B256::repeat_byte(0x98));
    let old_tip = block(100, B256::repeat_byte(0x64), parent.hash);
    let replacement = BlockRef {
        number: old_tip.number,
        hash: B256::repeat_byte(0xf4),
        parent_hash: None,
        timestamp: old_tip.timestamp,
    };
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());

    for (canonical, log_index) in [(parent, 0), (old_tip, 1), (replacement, 2)] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"ObservableParentlessDeepReorg()")],
                    &canonical,
                    0,
                    log_index,
                    false,
                )),
                included_context(canonical, log_index),
            ),
        )?;
    }

    assert_eq!(runtime.last_canonical_block(), Some(replacement));
    assert_eq!(runtime.metrics().deep_reorgs, 1);
    assert!(matches!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Degraded { since_block: 100 }
    ));
    Ok(())
}

#[tokio::test]
async fn empty_journal_discontinuities_still_emit_typed_reorg_reports() -> Result<()> {
    let address = Address::repeat_byte(0xb4);
    let base = block(80, B256::repeat_byte(0x80), B256::repeat_byte(0x79));
    let first_replacement = block(80, B256::repeat_byte(0xe0), B256::repeat_byte(0xdf));
    let second_replacement = block(80, B256::repeat_byte(0xf0), B256::repeat_byte(0xef));
    let unknown_removed = block(77, B256::repeat_byte(0x77), B256::repeat_byte(0x76));
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
        journal_depth: 0,
        ..ReactiveConfig::default()
    });
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"EmptyJournalReorg()")],
                &base,
                0,
                0,
                false,
            )),
            included_context(base, 0),
        ),
    )?;

    for (replacement, log_index) in [(first_replacement, 1), (second_replacement, 2)] {
        let report = runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"EmptyJournalReorg()")],
                    &replacement,
                    0,
                    log_index,
                    false,
                )),
                included_context(replacement, log_index),
            ),
        )?;
        assert!(report.reports.iter().any(|report| {
            matches!(
                report.as_ref(),
                ReactiveReport::Reorg(reorg)
                    if reorg.reason == evm_fork_cache::reactive::ReorgReason::ParentMismatch
                        && reorg.dropped_blocks.is_empty()
            )
        }));
    }

    let removed = runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(vec![
            sequence_log_record(unknown_removed, 3, true),
            sequence_log_record(unknown_removed, 4, true),
        ]),
    )?;
    assert_eq!(
        removed
            .reports
            .iter()
            .filter(
                |report| matches!(report.as_ref(), ReactiveReport::Reorg(reorg)
                if reorg.reason == evm_fork_cache::reactive::ReorgReason::RemovedLog
                    && reorg.dropped == Some(unknown_removed)
                    && reorg.dropped_blocks.is_empty())
            )
            .count(),
        1,
        "per-log removals coalesce only inside one atomic batch"
    );
    assert_eq!(runtime.metrics().reorgs_recovered, 3);
    assert_eq!(runtime.metrics().deep_reorgs, 3);
    assert!(matches!(
        runtime.health(),
        evm_fork_cache::reactive::CacheHealth::Unhealthy { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn unknown_parent_replacement_is_rejected_when_finalized_descent_is_unproven() -> Result<()> {
    let address = Address::repeat_byte(0xa9);
    let finalized = block(78, B256::repeat_byte(0x78), B256::repeat_byte(0x77));
    let old_parent = block(79, B256::repeat_byte(0x79), finalized.hash);
    let old = block(80, B256::repeat_byte(0x80), old_parent.hash);
    let replacement = block(80, B256::repeat_byte(0x81), B256::repeat_byte(0xe9));
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig {
        journal_depth: 2,
        ..ReactiveConfig::default()
    });

    for canonical in [finalized, old_parent, old] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"FinalizedDescent()")],
                    &canonical,
                    0,
                    0,
                    false,
                )),
                included_context(canonical, 0),
            ),
        )?;
    }
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Finalized(finalized)]),
    )?;

    let generation = cache.snapshot_generation();
    let error = runtime
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"FinalizedDescent()")],
                    &replacement,
                    0,
                    1,
                    false,
                )),
                included_context(replacement, 1),
            ),
        )
        .expect_err("unknown parent cannot prove descent from finalized state");

    assert!(matches!(error, ReactiveError::InvalidChainControl { .. }));
    assert_eq!(runtime.last_canonical_block(), Some(old));
    assert_eq!(runtime.finalized_head(), Some(&finalized));
    assert_eq!(cache.snapshot_generation(), generation);
    Ok(())
}

#[tokio::test]
async fn removed_and_reorged_records_cannot_drop_finalized_state() -> Result<()> {
    let address = Address::repeat_byte(0xa7);
    let parent = block(79, B256::repeat_byte(0x79), B256::repeat_byte(0x78));
    let finalized = block(80, B256::repeat_byte(0x80), parent.hash);
    let mut cache = setup_cache().await?;
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    for canonical in [&parent, &finalized] {
        runtime.ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    address,
                    vec![keccak256(b"Canonical()")],
                    canonical,
                    0,
                    0,
                    false,
                )),
                included_context(*canonical, 0),
            ),
        )?;
    }
    runtime.ingest_batch(
        &mut cache,
        ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([
                ChainControl::Safe(finalized),
                ChainControl::Finalized(finalized),
            ]),
    )?;

    let removed = batch(
        ReactiveInput::Log(rpc_log(
            address,
            vec![keccak256(b"Canonical()")],
            &finalized,
            0,
            0,
            true,
        )),
        reorged_context(finalized, 0),
    );
    assert!(matches!(
        runtime
            .ingest_batch(&mut cache, removed)
            .expect_err("removed log cannot cross finality"),
        ReactiveError::InvalidChainControl { .. }
    ));

    let reorged = batch(
        ReactiveInput::Log(rpc_log(
            address,
            vec![keccak256(b"Canonical()")],
            &finalized,
            0,
            0,
            false,
        )),
        reorged_context(finalized, 0),
    );
    assert!(matches!(
        runtime
            .ingest_batch(&mut cache, reorged)
            .expect_err("reorged status cannot cross finality"),
        ReactiveError::InvalidChainControl { .. }
    ));
    assert_eq!(runtime.last_canonical_block(), Some(finalized));
    assert_eq!(runtime.safe_head(), Some(&finalized));
    assert_eq!(runtime.finalized_head(), Some(&finalized));
    Ok(())
}

#[tokio::test]
async fn reactive_runtime_falls_back_to_purge_for_irreversible_dropped_effects() -> Result<()> {
    let address = Address::repeat_byte(0xa3);
    let slot = U256::from(9);
    let dropped = block(90, B256::repeat_byte(0x90), B256::repeat_byte(0x8f));
    let replacement = block(90, B256::repeat_byte(0x91), B256::repeat_byte(0x8f));
    let unrelated = Address::repeat_byte(0xf0);
    let mut cache = setup_cache().await?;
    install_mock_erc20(&mut cache, address);
    cache
        .db_mut()
        .insert_account_storage(address, slot, U256::from(99))?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(PurgeHandler { address }))?;
    runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"Purge()")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped, 0),
        ),
    )?;
    assert_eq!(cache.cached_storage_value(address, slot), Some(U256::ZERO));

    let report = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                unrelated,
                vec![keccak256(b"Replacement()")],
                &replacement,
                0,
                0,
                false,
            )),
            included_context(replacement, 0),
        ),
    )?;

    let reorg = report
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("replacement block emits a reorg report");
    assert_eq!(reorg.dropped_blocks, vec![dropped]);
    assert!(reorg.rollback_updates.is_empty());
    assert_eq!(
        reorg.purge_updates,
        vec![StateUpdate::purge(address, PurgeScope::AllStorage)]
    );

    Ok(())
}

#[tokio::test]
async fn reactive_runtime_cancels_hash_pinned_resyncs_for_dropped_blocks() -> Result<()> {
    let address = Address::repeat_byte(0xa4);
    let dropped = block(100, B256::repeat_byte(0xaa), B256::repeat_byte(0x99));
    let mut cache = setup_cache().await?;

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(ResyncOnlyHandler {
        address,
        slot: U256::from(1),
        block: ResyncBlock::Hash {
            number: dropped.number,
            hash: dropped.hash,
            require_canonical: true,
        },
    }))?;

    let first = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"NeedsRepair()")],
                &dropped,
                0,
                0,
                false,
            )),
            included_context(dropped, 0),
        ),
    )?;
    assert_eq!(first.resyncs.len(), 1);

    let second = runtime.ingest_batch(
        &mut cache,
        batch(
            ReactiveInput::Log(rpc_log(
                address,
                vec![keccak256(b"NeedsRepair()")],
                &dropped,
                0,
                0,
                true,
            )),
            reorged_context(dropped, 0),
        ),
    )?;
    let reorg = second
        .reports
        .iter()
        .find_map(|report| match report.as_ref() {
            ReactiveReport::Reorg(report) => Some(report),
            _ => None,
        })
        .expect("dropped block emits a reorg report");
    assert_eq!(reorg.canceled_resyncs.len(), 1);
    assert_eq!(
        reorg.canceled_resyncs[0].id,
        ResyncId::new("hash-pinned-repair")
    );

    Ok(())
}
