#![cfg(feature = "raw-flashblocks-json")]

use alloy_network::Ethereum;
#[cfg(feature = "reactive-ws")]
use alloy_primitives::U256;
use alloy_primitives::{Address, B256};
use alloy_provider::ProviderBuilder;
#[cfg(feature = "reactive-ws")]
use alloy_rpc_types_eth::Filter;
use alloy_transport::mock::Asserter;
use evm_fork_cache::reactive::{
    AlloySubscriber, BufferedRawJsonFlashblocksAdapter, FlashblockInvalidationReason,
    FlashblockUpdate, FlashblockUpdateChannelError, PreconfirmationMode, ProviderRef,
    RawJsonFlashblocksAdapter, RawJsonFlashblocksLimits, SubscriberConfig, SubscriberMode,
};
#[cfg(feature = "reactive-ws")]
use evm_fork_cache::reactive::{
    ChainStatus, EventSubscriber, LogInterest, ReactiveInput, ReactiveInterest,
};
use proptest::prelude::*;

const TX_ONE: B256 =
    alloy_primitives::b256!("5fe7f977e71dba2ea1a68e21057beebb9be2ac30c6410aa38d4f3fbe41dcffd2");
const TX_TWO: B256 =
    alloy_primitives::b256!("f2ee15ea639b73fa3db9b34a245bdfa015c260c598b211bf05a1ecc4b3e3b4f2");

fn index_zero() -> Vec<u8> {
    br#"{
        "payload_id":"0x1111111111111111",
        "index":0,
        "base":{
            "parent_hash":"0x6464646464646464646464646464646464646464646464646464646464646464",
            "block_number":"0x65",
            "timestamp":"0x6553f165",
            "gas_limit":"0x1c9c380",
            "base_fee_per_gas":"0x7",
            "fee_recipient":"0xcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcb",
            "prev_randao":"0x7777777777777777777777777777777777777777777777777777777777777777"
        },
        "diff":{
            "state_root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "block_hash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "transactions":["0x01"]
        },
        "metadata":{
            "block_number":101,
            "receipts":{
                "0x5fe7f977e71dba2ea1a68e21057beebb9be2ac30c6410aa38d4f3fbe41dcffd2":{
                    "type":"0x2",
                    "status":"0x1",
                    "cumulativeGasUsed":"0x5208",
                    "logs":[{
                        "address":"0x4242424242424242424242424242424242424242",
                        "topics":["0x4343434343434343434343434343434343434343434343434343434343434343"],
                        "data":"0x0102"
                    }]
                }
            }
        }
    }"#
    .to_vec()
}

fn index_one(receipt_transaction: B256) -> Vec<u8> {
    format!(
        r#"{{
            "payload_id":"0x1111111111111111",
            "index":1,
            "diff":{{
                "state_root":"0xabababababababababababababababababababababababababababababababab",
                "block_hash":"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "transactions":["0x02"]
            }},
            "metadata":{{
                "block_number":"0x65",
                "receipts":{{
                    "{receipt_transaction:#x}":{{
                        "logs":[
                            {{
                                "address":"0x4444444444444444444444444444444444444444",
                                "topics":[],
                                "data":"0x03"
                            }},
                            {{
                                "address":"0x4545454545454545454545454545454545454545",
                                "topics":[],
                                "data":"0x04"
                            }}
                        ]
                    }}
                }}
            }}
        }}"#
    )
    .into_bytes()
}

fn index_two(receipt_transaction: B256) -> Vec<u8> {
    format!(
        r#"{{
            "payload_id":"0x1111111111111111",
            "index":2,
            "diff":{{
                "state_root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "block_hash":"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "transactions":["0x03"]
            }},
            "metadata":{{
                "block_number":"0x65",
                "receipts":{{
                    "{receipt_transaction:#x}":{{
                        "logs":[]
                    }}
                }}
            }}
        }}"#
    )
    .into_bytes()
}

fn conflicting_index_one(receipt_transaction: B256) -> Vec<u8> {
    format!(
        r#"{{
            "payload_id":"0x1111111111111111",
            "index":1,
            "diff":{{
                "state_root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "block_hash":"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "transactions":["0x03"]
            }},
            "metadata":{{
                "block_number":"0x65",
                "receipts":{{
                    "{receipt_transaction:#x}":{{
                        "logs":[]
                    }}
                }}
            }}
        }}"#
    )
    .into_bytes()
}

fn alternate_index_zero() -> Vec<u8> {
    let third_transaction = alloy_primitives::keccak256([3_u8]);
    String::from_utf8(index_zero())
        .expect("index-zero fixture is UTF-8")
        .replace("\"transactions\":[\"0x01\"]", "\"transactions\":[\"0x03\"]")
        .replace(&format!("{TX_ONE:#x}"), &format!("{third_transaction:#x}"))
        .into_bytes()
}

fn alternate_index_one(receipt_transaction: B256) -> Vec<u8> {
    format!(
        r#"{{
            "payload_id":"0x1111111111111111",
            "index":1,
            "diff":{{
                "state_root":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "block_hash":"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "transactions":["0x04"]
            }},
            "metadata":{{
                "block_number":"0x65",
                "receipts":{{
                    "{receipt_transaction:#x}":{{
                        "logs":[]
                    }}
                }}
            }}
        }}"#
    )
    .into_bytes()
}

fn index_zero_for_block(block_number: u64) -> Vec<u8> {
    String::from_utf8(index_zero())
        .expect("index-zero fixture is UTF-8")
        .replace(
            "\"block_number\":\"0x65\"",
            &format!("\"block_number\":\"0x{block_number:x}\""),
        )
        .replace(
            "\"block_number\":101",
            &format!("\"block_number\":{block_number}"),
        )
        .into_bytes()
}

fn index_one_for_block(block_number: u64) -> Vec<u8> {
    String::from_utf8(index_one(TX_TWO))
        .expect("index-one fixture is UTF-8")
        .replace(
            "\"block_number\":\"0x65\"",
            &format!("\"block_number\":\"0x{block_number:x}\""),
        )
        .into_bytes()
}

fn adapter(endpoint: &str, generation: u64) -> RawJsonFlashblocksAdapter {
    RawJsonFlashblocksAdapter::new(ProviderRef::new(endpoint, generation))
}

#[tokio::test]
async fn bounded_update_sender_remains_external_to_subscriber_ownership() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let sender = subscriber
        .open_external_flashblock_update_channel(1)
        .expect("bounded update channel");
    let mut adapter = RawJsonFlashblocksAdapter::new(source.clone());
    let update = adapter
        .ingest_json(&index_zero())
        .expect("valid raw frame")
        .expect("standardized update");

    let _receipt = sender.try_send(update.clone()).expect("first queue slot");
    assert!(matches!(
        sender.try_send(update.clone()),
        Err(FlashblockUpdateChannelError::Full)
    ));

    let mut other = RawJsonFlashblocksAdapter::new(ProviderRef::new("other-endpoint", 1));
    let other_update = other
        .ingest_json(&index_zero())
        .expect("other raw frame")
        .expect("other standardized update");
    assert!(matches!(
        sender.try_send(other_update),
        Err(FlashblockUpdateChannelError::UnexpectedEndpoint)
    ));

    drop(subscriber);
    assert_eq!(
        sender.send(update).await,
        Err(FlashblockUpdateChannelError::Closed)
    );
}

#[test]
fn update_channel_configuration_is_single_and_nonzero() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source)
        .expect("configure external source");

    assert!(
        subscriber
            .open_external_flashblock_update_channel(0)
            .expect_err("zero capacity")
            .to_string()
            .contains("greater than zero")
    );
    subscriber
        .open_external_flashblock_update_channel(1)
        .expect("first channel");
    assert!(
        subscriber
            .open_external_flashblock_update_channel(1)
            .expect_err("second channel")
            .to_string()
            .contains("already opened")
    );
}

#[test]
fn direct_standardized_ingest_rejects_an_index_gap() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut adapter = RawJsonFlashblocksAdapter::new(source);

    let first = adapter
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    let _skipped = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("valid index one")
        .expect("index one snapshot");
    let third_transaction = alloy_primitives::keccak256([3_u8]);
    let gap = adapter
        .ingest_json(&index_two(third_transaction))
        .expect("valid index two")
        .expect("index two snapshot");

    subscriber
        .ingest_flashblock_update(first)
        .expect("index zero is accepted");
    assert!(
        subscriber
            .ingest_flashblock_update(gap)
            .expect_err("subscriber must reject a skipped index")
            .to_string()
            .contains("index")
    );
}

#[test]
fn direct_standardized_ingest_requires_index_zero_for_a_new_payload() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    let _ = adapter
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    let starts_late = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("valid index one")
        .expect("index one snapshot");

    assert!(
        subscriber
            .ingest_flashblock_update(starts_late)
            .expect_err("a new payload cannot start after index zero")
            .to_string()
            .contains("begin at index zero")
    );
}

#[test]
fn direct_standardized_ingest_rejects_an_index_regression() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    let first = adapter
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    let repeated_first = first.clone();
    let next = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("valid index one")
        .expect("index one snapshot");
    subscriber
        .ingest_flashblock_update(first)
        .expect("index zero is accepted");
    subscriber
        .ingest_flashblock_update(next)
        .expect("index one is accepted");

    assert!(
        subscriber
            .ingest_flashblock_update(repeated_first)
            .expect_err("an older index cannot replace a newer snapshot")
            .to_string()
            .contains("regressed")
    );
}

#[test]
fn direct_standardized_ingest_accepts_an_exact_duplicate_as_a_no_op() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    let first = adapter
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    subscriber
        .ingest_flashblock_update(first.clone())
        .expect("index zero is accepted");
    subscriber
        .ingest_flashblock_update(first)
        .expect("an exact duplicate is idempotent");
}

#[test]
fn direct_standardized_ingest_rejects_changed_content_at_the_same_index() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut first_source = RawJsonFlashblocksAdapter::new(source.clone());
    let first = first_source
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    let index_one = first_source
        .ingest_json(&index_one(TX_TWO))
        .expect("valid index one")
        .expect("index one snapshot");
    subscriber
        .ingest_flashblock_update(first)
        .expect("index zero is accepted");
    subscriber
        .ingest_flashblock_update(index_one)
        .expect("index one is accepted");

    let mut conflicting_source = RawJsonFlashblocksAdapter::new(source);
    let _ = conflicting_source
        .ingest_json(&index_zero())
        .expect("valid alternate index zero")
        .expect("alternate index zero snapshot");
    let third_transaction = alloy_primitives::keccak256([3_u8]);
    let conflict = conflicting_source
        .ingest_json(&conflicting_index_one(third_transaction))
        .expect("independently valid conflicting index one")
        .expect("conflicting index one snapshot");

    assert!(
        subscriber
            .ingest_flashblock_update(conflict)
            .expect_err("same-index content changes must fail closed")
            .to_string()
            .contains("same index")
    );
}

#[test]
fn direct_standardized_ingest_rejects_non_prefix_cumulative_membership() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut accepted_source = RawJsonFlashblocksAdapter::new(source.clone());
    let first = accepted_source
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    subscriber
        .ingest_flashblock_update(first)
        .expect("index zero is accepted");

    let mut divergent_source = RawJsonFlashblocksAdapter::new(source);
    let _ = divergent_source
        .ingest_json(&alternate_index_zero())
        .expect("valid divergent index zero")
        .expect("divergent index zero snapshot");
    let fourth_transaction = alloy_primitives::keccak256([4_u8]);
    let non_prefix = divergent_source
        .ingest_json(&alternate_index_one(fourth_transaction))
        .expect("independently valid divergent index one")
        .expect("divergent index one snapshot");

    assert!(
        subscriber
            .ingest_flashblock_update(non_prefix)
            .expect_err("cumulative membership must retain the prior prefix")
            .to_string()
            .contains("prefix")
    );
}

#[test]
fn direct_standardized_ingest_rejects_base_identity_changes_within_a_payload() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut accepted_source = RawJsonFlashblocksAdapter::new(source.clone());
    let first = accepted_source
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    subscriber
        .ingest_flashblock_update(first)
        .expect("index zero is accepted");

    let mut altered_source = RawJsonFlashblocksAdapter::new(source);
    let _ = altered_source
        .ingest_json(&index_zero_for_block(102))
        .expect("valid alternate base")
        .expect("alternate index zero snapshot");
    let altered_base = altered_source
        .ingest_json(&index_one_for_block(102))
        .expect("valid alternate index one")
        .expect("alternate index one snapshot");

    assert!(
        subscriber
            .ingest_flashblock_update(altered_base)
            .expect_err("base identity must stay stable within a payload")
            .to_string()
            .contains("base identity")
    );
}

#[test]
fn direct_standardized_ingest_rejects_delta_logs_from_prior_transactions() {
    let source = ProviderRef::new("raw-json", 7);
    let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let mut subscriber = AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    let first = adapter
        .ingest_json(&index_zero())
        .expect("valid index zero")
        .expect("index zero snapshot");
    subscriber
        .ingest_flashblock_update(first)
        .expect("index zero is accepted");

    let mut next = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("valid index one")
        .expect("index one snapshot");
    let FlashblockUpdate::Snapshot(snapshot) = &mut next else {
        panic!("expected snapshot")
    };
    snapshot.logs[0].transaction_hash = Some(TX_ONE);
    snapshot.logs[0].transaction_index = Some(0);
    snapshot.logs[0].log_index = Some(99);

    assert!(
        subscriber
            .ingest_flashblock_update(next)
            .expect_err("delta logs must come from newly appended transactions")
            .to_string()
            .contains("newly appended")
    );
}

#[test]
fn indexed_delta_becomes_a_standard_flashblock_update() {
    let provider = ProviderRef::new("raw-json", 7);
    let mut adapter = RawJsonFlashblocksAdapter::new(provider.clone());

    let update = adapter
        .ingest_json(&index_zero())
        .expect("valid raw Flashblock")
        .expect("first index publishes a batch");
    let FlashblockUpdate::Snapshot(batch) = update else {
        panic!("expected standardized snapshot")
    };

    assert_eq!(batch.flashblock.provider, provider);
    assert_eq!(batch.flashblock.payload_id, Some([0x11; 8].into()));
    assert_eq!(batch.flashblock.index, Some(0));
    assert_eq!(batch.flashblock.block_number, 101);
    assert_eq!(batch.flashblock.transaction_hashes, vec![TX_ONE]);
    assert_ne!(batch.flashblock.content_hash, B256::ZERO);
    assert_eq!(batch.logs.len(), 1);
    assert_eq!(batch.logs[0].address(), Address::repeat_byte(0x42));
    assert_eq!(batch.logs[0].transaction_hash, Some(TX_ONE));
    assert_eq!(batch.logs[0].transaction_index, Some(0));
    assert_eq!(batch.logs[0].log_index, Some(0));
    assert_eq!(
        batch.logs[0].block_hash,
        Some(batch.flashblock.content_hash)
    );
}

#[test]
fn converter_is_chain_neutral_and_carries_only_caller_provenance() {
    let mut first = adapter("chain-a-provider", 2);
    let mut second = adapter("chain-b-provider", 9);

    let FlashblockUpdate::Snapshot(first) = first
        .ingest_json(&index_zero())
        .expect("first provider frame")
        .expect("first provider snapshot")
    else {
        panic!("expected snapshot")
    };
    let FlashblockUpdate::Snapshot(second) = second
        .ingest_json(&index_zero())
        .expect("second provider frame")
        .expect("second provider snapshot")
    else {
        panic!("expected snapshot")
    };

    assert_eq!(
        first.flashblock.provider,
        ProviderRef::new("chain-a-provider", 2)
    );
    assert_eq!(
        second.flashblock.provider,
        ProviderRef::new("chain-b-provider", 9)
    );
    assert_ne!(
        first.flashblock.content_hash,
        second.flashblock.content_hash
    );
}

#[test]
fn rejected_delta_does_not_poison_a_corrected_retry() {
    let mut adapter = adapter("raw-json", 7);
    adapter
        .ingest_json(&index_zero())
        .expect("valid index zero");

    let error = adapter
        .ingest_json(&index_one(TX_ONE))
        .expect_err("receipt membership mismatch");
    assert!(error.to_string().contains("receipt-map membership"));

    let FlashblockUpdate::Snapshot(batch) = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("corrected retry remains admissible")
        .expect("corrected retry publishes")
    else {
        panic!("expected snapshot")
    };
    assert_eq!(batch.flashblock.transaction_hashes, vec![TX_ONE, TX_TWO]);
    assert_eq!(batch.logs.len(), 2);
    assert_eq!(batch.logs[0].transaction_index, Some(1));
    assert_eq!(batch.logs[0].log_index, Some(1));
    assert_eq!(batch.logs[1].log_index, Some(2));
}

#[test]
fn semantic_duplicate_is_idempotent_across_json_serializations() {
    let mut adapter = adapter("raw-json", 7);
    let frame = index_zero();
    adapter.ingest_json(&frame).expect("first delivery");
    let compact = serde_json::to_vec(
        &serde_json::from_slice::<serde_json::Value>(&frame).expect("fixture JSON"),
    )
    .expect("compact fixture");

    assert!(adapter.ingest_json(&compact).expect("duplicate").is_none());
}

#[test]
fn duplicate_receipt_map_keys_are_rejected_before_normalization() {
    let receipt_key = format!("{TX_ONE:#x}");
    let original_key = format!("\"{receipt_key}\":{{");
    let uppercase_key = format!("0x{}", receipt_key[2..].to_ascii_uppercase());

    for duplicate_key in [&receipt_key, &uppercase_key] {
        let repeated_key = format!("\"{receipt_key}\":{{\"logs\":[]}},\"{duplicate_key}\":{{");
        let frame = String::from_utf8(index_zero())
            .expect("index-zero fixture is UTF-8")
            .replace(&original_key, &repeated_key);
        let mut adapter = adapter("raw-json", 7);

        let error = adapter
            .ingest_json(frame.as_bytes())
            .expect_err("duplicate receipt key must be rejected");
        assert!(error.to_string().contains("duplicate receipt key"));
    }
}

#[test]
fn sequence_failures_emit_standard_invalidations() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("first delivery");
    let gap = String::from_utf8(index_one(TX_TWO))
        .expect("fixture UTF-8")
        .replace("\"index\":1", "\"index\":2")
        .into_bytes();

    let FlashblockUpdate::Invalidated(invalidation) = adapter
        .ingest_json(&gap)
        .expect("gap is an invalidation")
        .expect("gap is observable")
    else {
        panic!("expected invalidation")
    };
    assert_eq!(invalidation.provider, ProviderRef::new("raw-json", 7));
    assert_eq!(invalidation.reason, FlashblockInvalidationReason::IndexGap);
    assert!(
        adapter
            .ingest_json(&index_one(TX_TWO))
            .expect("ignored remainder")
            .is_none()
    );

    let replacement = String::from_utf8(index_zero())
        .expect("fixture UTF-8")
        .replace("0x1111111111111111", "0x2222222222222222")
        .into_bytes();
    assert!(matches!(
        adapter.ingest_json(&replacement).expect("next payload"),
        Some(FlashblockUpdate::Snapshot(_))
    ));
}

#[test]
fn one_missing_index_is_buffered_and_drained_in_order_inside_the_bound() {
    let mut adapter = BufferedRawJsonFlashblocksAdapter::new(
        ProviderRef::new("raw-json", 7),
        RawJsonFlashblocksLimits::default(),
        400,
    )
    .expect("reviewed 400ms gap bound");
    let first = adapter
        .ingest_json_at(&index_zero(), 1_000)
        .expect("index zero");
    assert_eq!(first.len(), 1);

    let buffered = adapter
        .ingest_json_at(&index_two(alloy_primitives::keccak256([3_u8])), 1_100)
        .expect("one missing index is held briefly");
    assert!(buffered.is_empty());
    assert_eq!(adapter.buffered_gap(), Some((1, 2, 1_500)));

    let drained = adapter
        .ingest_json_at(&index_one(TX_TWO), 1_499)
        .expect("late index inside the bound drains the sequence");
    let indices = drained
        .iter()
        .map(|update| match update {
            FlashblockUpdate::Snapshot(snapshot) => snapshot.flashblock.index,
            FlashblockUpdate::Invalidated(_) => None,
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(indices, vec![Some(1), Some(2)]);
    assert_eq!(adapter.buffered_gap(), None);
}

#[test]
fn timed_gap_drain_retains_each_frames_original_monotonic_arrival() {
    let mut adapter = BufferedRawJsonFlashblocksAdapter::new(
        ProviderRef::new("raw-json", 7),
        RawJsonFlashblocksLimits::default(),
        400,
    )
    .expect("reviewed 400ms gap bound");
    adapter
        .ingest_json_timed_at(&index_zero(), 1_000)
        .expect("index zero");
    assert!(
        adapter
            .ingest_json_timed_at(&index_two(alloy_primitives::keccak256([3_u8])), 1_100,)
            .expect("future frame is retained")
            .is_empty()
    );

    let drained = adapter
        .ingest_json_timed_at(&index_one(TX_TWO), 1_499)
        .expect("missing frame drains the retained future");
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].source_ingress_millis(), 1_499);
    assert_eq!(drained[1].source_ingress_millis(), 1_100);
    let FlashblockUpdate::Snapshot(first) = drained[0].update() else {
        panic!("missing index must normalize to a snapshot")
    };
    let FlashblockUpdate::Snapshot(second) = drained[1].update() else {
        panic!("retained future frame must normalize to a snapshot")
    };
    assert!(first.flashblock.same_base_identity(&second.flashblock));
}

#[test]
fn missing_index_timeout_revokes_only_the_speculative_payload() {
    let mut adapter = BufferedRawJsonFlashblocksAdapter::new(
        ProviderRef::new("raw-json", 7),
        RawJsonFlashblocksLimits::default(),
        400,
    )
    .expect("reviewed 400ms gap bound");
    adapter
        .ingest_json_at(&index_zero(), 1_000)
        .expect("index zero");
    adapter
        .ingest_json_at(&index_two(alloy_primitives::keccak256([3_u8])), 1_100)
        .expect("buffer index two");

    assert!(adapter.expire_gap_at(1_499).is_none());
    let FlashblockUpdate::Invalidated(invalidation) = adapter
        .expire_gap_at(1_500)
        .expect("expiry revokes the provisional payload")
    else {
        panic!("expected invalidation")
    };
    assert_eq!(invalidation.provider, ProviderRef::new("raw-json", 7));
    assert_eq!(invalidation.reason, FlashblockInvalidationReason::IndexGap);
    assert!(
        adapter
            .ingest_json_at(&index_one(TX_TWO), 1_501)
            .expect("expired payload remainder is ignored")
            .is_empty()
    );
}

#[test]
fn gap_buffer_configuration_is_bounded_to_the_reviewed_window() {
    for invalid in [0, 299, 501, u64::MAX] {
        assert!(
            BufferedRawJsonFlashblocksAdapter::new(
                ProviderRef::new("raw-json", 7),
                RawJsonFlashblocksLimits::default(),
                invalid,
            )
            .is_err()
        );
    }
    for valid in [300, 400, 500] {
        assert!(
            BufferedRawJsonFlashblocksAdapter::new(
                ProviderRef::new("raw-json", 7),
                RawJsonFlashblocksLimits::default(),
                valid,
            )
            .is_ok()
        );
    }
}

#[test]
fn conflicting_duplicate_while_buffered_invalidates_immediately() {
    let mut adapter = BufferedRawJsonFlashblocksAdapter::new(
        ProviderRef::new("raw-json", 7),
        RawJsonFlashblocksLimits::default(),
        400,
    )
    .expect("buffered adapter");
    adapter
        .ingest_json_at(&index_zero(), 1_000)
        .expect("index zero");
    let third_transaction = alloy_primitives::keccak256([3_u8]);
    adapter
        .ingest_json_at(&index_two(third_transaction), 1_100)
        .expect("buffer one future frame");
    let fourth_transaction = alloy_primitives::keccak256([4_u8]);
    let conflict = String::from_utf8(index_two(fourth_transaction))
        .expect("fixture UTF-8")
        .replace("\"transactions\":[\"0x03\"]", "\"transactions\":[\"0x04\"]")
        .into_bytes();

    let updates = adapter
        .ingest_json_at(&conflict, 1_101)
        .expect("valid same-index conflict becomes an invalidation");
    assert_eq!(updates.len(), 1);
    let FlashblockUpdate::Invalidated(invalidation) = &updates[0] else {
        panic!("expected conflict invalidation")
    };
    assert_eq!(
        invalidation.reason,
        FlashblockInvalidationReason::ConflictingDuplicate
    );
    assert_eq!(adapter.buffered_gap(), None);
}

#[test]
fn a_second_future_index_fails_closed_without_growing_the_buffer() {
    let mut adapter = BufferedRawJsonFlashblocksAdapter::new(
        ProviderRef::new("raw-json", 7),
        RawJsonFlashblocksLimits::default(),
        400,
    )
    .expect("buffered adapter");
    adapter
        .ingest_json_at(&index_zero(), 1_000)
        .expect("index zero");
    adapter
        .ingest_json_at(&index_two(alloy_primitives::keccak256([3_u8])), 1_100)
        .expect("buffer one future frame");
    let index_three = String::from_utf8(index_two(alloy_primitives::keccak256([3_u8])))
        .expect("fixture UTF-8")
        .replace("\"index\":2", "\"index\":3")
        .into_bytes();

    let updates = adapter
        .ingest_json_at(&index_three, 1_101)
        .expect("second future frame becomes an invalidation");
    let FlashblockUpdate::Invalidated(invalidation) = &updates[0] else {
        panic!("expected gap invalidation")
    };
    assert_eq!(invalidation.reason, FlashblockInvalidationReason::IndexGap);
    assert_eq!(adapter.buffered_gap(), None);
}

#[test]
fn reset_and_payload_replacement_clear_the_buffer() {
    let source = ProviderRef::new("raw-json", 7);
    let mut reset_adapter = BufferedRawJsonFlashblocksAdapter::new(
        source.clone(),
        RawJsonFlashblocksLimits::default(),
        400,
    )
    .expect("buffered adapter");
    reset_adapter
        .ingest_json_at(&index_zero(), 1_000)
        .expect("index zero");
    reset_adapter
        .ingest_json_at(&index_two(alloy_primitives::keccak256([3_u8])), 1_100)
        .expect("buffer index two");
    assert!(matches!(
        reset_adapter
            .reset(ProviderRef::new("raw-json", 8))
            .expect("new source generation"),
        Some(FlashblockUpdate::Invalidated(_))
    ));
    assert_eq!(reset_adapter.buffered_gap(), None);
    assert_eq!(reset_adapter.provider().generation, 8);

    let mut replacement_adapter =
        BufferedRawJsonFlashblocksAdapter::new(source, RawJsonFlashblocksLimits::default(), 400)
            .expect("buffered adapter");
    replacement_adapter
        .ingest_json_at(&index_zero(), 1_000)
        .expect("index zero");
    replacement_adapter
        .ingest_json_at(&index_two(alloy_primitives::keccak256([3_u8])), 1_100)
        .expect("buffer index two");
    let replacement = String::from_utf8(index_zero())
        .expect("fixture UTF-8")
        .replace("0x1111111111111111", "0x2222222222222222")
        .into_bytes();
    let updates = replacement_adapter
        .ingest_json_at(&replacement, 1_101)
        .expect("new index-zero payload replaces the old payload");
    assert!(matches!(
        updates.as_slice(),
        [FlashblockUpdate::Snapshot(_)]
    ));
    assert_eq!(replacement_adapter.buffered_gap(), None);
}

#[test]
fn malformed_and_resource_exhausting_future_frames_are_never_buffered() {
    let mut limits = RawJsonFlashblocksLimits::default();
    limits.max_transactions_per_payload = 1;
    let mut adapter =
        BufferedRawJsonFlashblocksAdapter::new(ProviderRef::new("raw-json", 7), limits, 400)
            .expect("buffered adapter");
    adapter
        .ingest_json_at(&index_zero(), 1_000)
        .expect("index zero consumes the transaction allowance");
    assert!(matches!(
        adapter.ingest_json_at(&index_two(alloy_primitives::keccak256([3_u8])), 1_100),
        Err(
            evm_fork_cache::reactive::RawJsonFlashblocksError::ResourceExhausted(
                "transaction count"
            )
        )
    ));
    assert_eq!(adapter.buffered_gap(), None);
    assert!(matches!(
        adapter.ingest_json_at(b"{", 1_101),
        Err(evm_fork_cache::reactive::RawJsonFlashblocksError::InvalidPayload(_))
    ));
    assert_eq!(adapter.buffered_gap(), None);
}

proptest! {
    #[test]
    fn only_one_exact_future_index_can_enter_the_reorder_buffer(index in 3_u64..64) {
        let mut adapter = BufferedRawJsonFlashblocksAdapter::new(
            ProviderRef::new("raw-json", 7),
            RawJsonFlashblocksLimits::default(),
            400,
        )
        .expect("buffered adapter");
        adapter.ingest_json_at(&index_zero(), 1_000).expect("index zero");
        let frame = String::from_utf8(index_two(alloy_primitives::keccak256([3_u8])))
            .expect("fixture UTF-8")
            .replace("\"index\":2", &format!("\"index\":{index}"))
            .into_bytes();

        let updates = adapter.ingest_json_at(&frame, 1_100).expect("valid distant index");
        prop_assert!(matches!(updates.as_slice(), [FlashblockUpdate::Invalidated(_)]));
        prop_assert_eq!(adapter.buffered_gap(), None);
    }
}

#[test]
fn joining_after_index_zero_invalidates_instead_of_inventing_a_base() {
    let mut adapter = adapter("raw-json", 7);
    let FlashblockUpdate::Invalidated(invalidation) = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("late join becomes an invalidation")
        .expect("late join is observable")
    else {
        panic!("expected invalidation")
    };
    assert_eq!(
        invalidation.reason,
        FlashblockInvalidationReason::MissingInitialIndex
    );
}

#[test]
fn every_delta_must_retain_the_index_zero_block_number() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("index zero");
    let wrong_block = String::from_utf8(index_one(TX_TWO))
        .expect("fixture UTF-8")
        .replace("\"block_number\":\"0x65\"", "\"block_number\":\"0x66\"")
        .into_bytes();
    assert!(
        adapter
            .ingest_json(&wrong_block)
            .expect_err("block drift")
            .to_string()
            .contains("block numbers disagree")
    );
    assert!(matches!(
        adapter
            .ingest_json(&index_one(TX_TWO))
            .expect("correct retry"),
        Some(FlashblockUpdate::Snapshot(_))
    ));
}

#[test]
fn caller_reset_revokes_the_active_generation_without_reconnecting() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("first delivery");

    let FlashblockUpdate::Invalidated(invalidation) = adapter
        .reset(ProviderRef::new("raw-json", 8))
        .expect("valid source transition")
        .expect("active payload is revoked")
    else {
        panic!("expected invalidation")
    };
    assert_eq!(invalidation.provider, ProviderRef::new("raw-json", 7));
    assert_eq!(
        invalidation.reason,
        FlashblockInvalidationReason::SourceReset
    );

    let FlashblockUpdate::Snapshot(replacement) = adapter
        .ingest_json(&index_zero())
        .expect("new generation frame")
        .expect("new generation snapshot")
    else {
        panic!("expected replacement snapshot")
    };
    assert_eq!(
        replacement.flashblock.provider,
        ProviderRef::new("raw-json", 8)
    );
}

#[test]
fn reset_rejects_endpoint_changes_and_non_increasing_generations() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("first delivery");

    for replacement in [
        ProviderRef::new("raw-json", 7),
        ProviderRef::new("raw-json", 6),
        ProviderRef::new("other-source", 8),
    ] {
        assert!(
            adapter
                .reset(replacement)
                .expect_err("ambiguous transition")
                .to_string()
                .contains("source transition")
        );
    }

    let FlashblockUpdate::Snapshot(next) = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("rejected reset preserves active state")
        .expect("next delta remains admissible")
    else {
        panic!("expected snapshot")
    };
    assert_eq!(next.flashblock.provider, ProviderRef::new("raw-json", 7));
    assert_eq!(next.flashblock.index, Some(1));
}

#[test]
fn zero_resource_bounds_are_rejected_at_construction() {
    let mut limits = RawJsonFlashblocksLimits::default();
    limits.max_frame_bytes = 0;
    let error = RawJsonFlashblocksAdapter::with_limits(ProviderRef::new("raw-json", 1), limits)
        .expect_err("zero frame limit must be rejected");
    assert!(error.to_string().contains("max_frame_bytes"));
}

#[test]
fn conflicting_duplicate_revokes_the_active_payload() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("first delivery");
    let conflicting = String::from_utf8(index_zero())
        .expect("fixture UTF-8")
        .replace("\"data\":\"0x0102\"", "\"data\":\"0x99\"")
        .into_bytes();

    let FlashblockUpdate::Invalidated(invalidation) = adapter
        .ingest_json(&conflicting)
        .expect("conflict becomes an invalidation")
        .expect("conflict is observable")
    else {
        panic!("expected invalidation")
    };
    assert_eq!(
        invalidation.reason,
        FlashblockInvalidationReason::ConflictingDuplicate
    );
    assert_eq!(invalidation.payload_id, [0x11_u8; 8]);
}

#[test]
fn a_new_payload_missing_index_zero_revokes_the_previous_payload() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("first delivery");
    let next_payload_gap = String::from_utf8(index_one(TX_TWO))
        .expect("fixture UTF-8")
        .replace("0x1111111111111111", "0x2222222222222222")
        .into_bytes();

    let FlashblockUpdate::Invalidated(invalidation) = adapter
        .ingest_json(&next_payload_gap)
        .expect("missing base becomes an invalidation")
        .expect("missing base is observable")
    else {
        panic!("expected invalidation")
    };
    assert_eq!(
        invalidation.reason,
        FlashblockInvalidationReason::MissingInitialIndex
    );
    assert_eq!(
        invalidation.payload_id, [0x11_u8; 8],
        "the subscriber can revoke the exact payload that was previously published"
    );
}

#[test]
fn rejected_replacement_frame_preserves_the_active_payload_for_explicit_reset() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("first delivery");
    let malformed_replacement = String::from_utf8(index_zero())
        .expect("fixture UTF-8")
        .replace("0x1111111111111111", "0x2222222222222222")
        .replace("\"block_number\":101", "\"block_number\":102")
        .into_bytes();

    assert!(adapter.ingest_json(&malformed_replacement).is_err());
    let FlashblockUpdate::Invalidated(invalidation) = adapter
        .reset(ProviderRef::new("raw-json", 8))
        .expect("valid source transition")
        .expect("the last published payload remains revocable")
    else {
        panic!("expected invalidation")
    };
    assert_eq!(invalidation.payload_id, [0x11_u8; 8]);
}

#[test]
fn duplicate_transaction_across_deltas_is_rejected_without_advancing_sequence() {
    let mut adapter = adapter("raw-json", 7);
    adapter.ingest_json(&index_zero()).expect("first delivery");
    let duplicate = String::from_utf8(index_one(TX_ONE))
        .expect("fixture UTF-8")
        .replace("\"transactions\":[\"0x02\"]", "\"transactions\":[\"0x01\"]")
        .into_bytes();
    assert!(
        adapter
            .ingest_json(&duplicate)
            .expect_err("duplicate cumulative member")
            .to_string()
            .contains("more than one indexed delta")
    );

    let FlashblockUpdate::Snapshot(corrected) = adapter
        .ingest_json(&index_one(TX_TWO))
        .expect("corrected index remains admissible")
        .expect("corrected index publishes")
    else {
        panic!("expected snapshot")
    };
    assert_eq!(corrected.flashblock.index, Some(1));
}

#[test]
fn duplicate_transaction_inside_one_delta_is_rejected_without_advancing_sequence() {
    let mut adapter = adapter("raw-json", 7);
    let duplicate = String::from_utf8(index_zero())
        .expect("fixture UTF-8")
        .replace(
            "\"transactions\":[\"0x01\"]",
            "\"transactions\":[\"0x01\",\"0x01\"]",
        )
        .into_bytes();

    assert!(
        adapter
            .ingest_json(&duplicate)
            .expect_err("duplicate transaction inside a delta")
            .to_string()
            .contains("duplicate hash")
    );
    assert!(matches!(
        adapter.ingest_json(&index_zero()).expect("corrected index"),
        Some(FlashblockUpdate::Snapshot(_))
    ));
}

#[test]
fn configured_frame_transaction_and_log_bounds_fail_closed() {
    let source = ProviderRef::new("raw-json", 1);
    let mut frame_limits = RawJsonFlashblocksLimits::default();
    frame_limits.max_frame_bytes = index_zero().len() - 1;
    let mut frame_limited = RawJsonFlashblocksAdapter::with_limits(source.clone(), frame_limits)
        .expect("valid frame limit");
    assert!(
        frame_limited
            .ingest_json(&index_zero())
            .expect_err("oversized frame")
            .to_string()
            .contains("byte limit")
    );

    let mut transaction_limits = RawJsonFlashblocksLimits::default();
    transaction_limits.max_transactions_per_payload = 1;
    let mut transaction_limited =
        RawJsonFlashblocksAdapter::with_limits(source.clone(), transaction_limits)
            .expect("valid transaction limit");
    transaction_limited
        .ingest_json(&index_zero())
        .expect("first transaction");
    assert!(
        transaction_limited
            .ingest_json(&index_one(TX_TWO))
            .expect_err("second cumulative transaction")
            .to_string()
            .contains("transaction count")
    );

    let mut log_limits = RawJsonFlashblocksLimits::default();
    log_limits.max_logs_per_payload = 1;
    let mut log_limited =
        RawJsonFlashblocksAdapter::with_limits(source, log_limits).expect("valid log limit");
    log_limited.ingest_json(&index_zero()).expect("first log");
    assert!(
        log_limited
            .ingest_json(&index_one(TX_TWO))
            .expect_err("second cumulative log")
            .to_string()
            .contains("log count")
    );
}

#[test]
fn receipt_logs_with_more_than_four_topics_are_rejected() {
    let mut adapter = adapter("raw-json", 7);
    let topics = format!(
        "\"topics\":[\"{0:#x}\",\"{1:#x}\",\"{2:#x}\",\"{3:#x}\",\"{4:#x}\"]",
        B256::repeat_byte(1),
        B256::repeat_byte(2),
        B256::repeat_byte(3),
        B256::repeat_byte(4),
        B256::repeat_byte(5),
    );
    let frame = String::from_utf8(index_zero())
        .expect("fixture UTF-8")
        .replace(
            "\"topics\":[\"0x4343434343434343434343434343434343434343434343434343434343434343\"]",
            &topics,
        )
        .into_bytes();
    assert!(
        adapter
            .ingest_json(&frame)
            .expect_err("too many topics")
            .to_string()
            .contains("more than four topics")
    );
}

proptest! {
    #[test]
    fn arbitrary_application_frames_never_panic_or_prevent_explicit_recovery(
        frame in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let mut adapter = adapter("raw-json", 1);
        let _ = adapter.ingest_json(&frame);
        let _ = adapter
            .reset(ProviderRef::new("raw-json", 2))
            .expect("valid source transition");
        prop_assert!(matches!(
            adapter.ingest_json(&index_zero()),
            Ok(Some(FlashblockUpdate::Snapshot(_)))
        ));
    }
}

#[tokio::test]
#[cfg(feature = "reactive-ws")]
async fn standardized_updates_enter_the_existing_preconfirmation_pipeline() {
    let asserter = Asserter::new();
    asserter.push_success(&U256::from(10));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let source = ProviderRef::new("raw-json", 7);
    let mut subscriber = evm_fork_cache::reactive::AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::PubSub,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Preferred,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    subscriber
        .register_interests(&[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(Address::repeat_byte(0x42))
                .event_signature(B256::repeat_byte(0x43)),
            local_matcher: None,
            route_key: None,
        })])
        .await
        .expect("register raw update interest");

    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    let update = adapter
        .ingest_json(&index_zero())
        .expect("decode raw frame")
        .expect("publish raw frame");
    subscriber
        .ingest_flashblock_update(update)
        .expect("ingest standardized update");

    let batch = subscriber
        .next_scoped_batch()
        .await
        .expect("subscriber remains healthy")
        .expect("preconfirmed batch");
    assert_eq!(batch.records().len(), 1);
    assert!(batch.records()[0].scope().is_preconfirmed());
    assert!(matches!(batch.records()[0].input, ReactiveInput::Log(_)));
    assert!(matches!(
        batch.records()[0].context.chain_status,
        ChainStatus::Preconfirmed { ref flashblock }
            if flashblock.provider == ProviderRef::new("raw-json", 7)
    ));
    assert_eq!(subscriber.flashblocks_rpc_metrics().total_requests(), 0);

    let reset = adapter
        .reset(ProviderRef::new("raw-json", 8))
        .expect("valid source transition")
        .expect("active source reset invalidates");
    subscriber
        .ingest_flashblock_update(reset)
        .expect("ingest source reset");
    let invalidation = subscriber
        .next_scoped_batch()
        .await
        .expect("subscriber remains healthy")
        .expect("invalidation batch");
    assert!(invalidation.preconfirmation_invalidated());
    assert!(invalidation.records().is_empty());

    let next = adapter
        .ingest_json(&index_zero())
        .expect("next provider generation frame")
        .expect("next provider generation publishes");
    subscriber
        .ingest_flashblock_update(next)
        .expect("ingest next provider generation");
    let next_batch = subscriber
        .next_scoped_batch()
        .await
        .expect("subscriber remains healthy")
        .expect("replacement batch");
    assert!(matches!(
        next_batch.records()[0].context.chain_status,
        ChainStatus::Preconfirmed { ref flashblock }
            if flashblock.provider == ProviderRef::new("raw-json", 8)
    ));

    let mut stale = RawJsonFlashblocksAdapter::new(ProviderRef::new("raw-json", 7));
    subscriber
        .ingest_flashblock_update(
            stale
                .ingest_json(&index_zero())
                .expect("stale frame decodes")
                .expect("stale snapshot"),
        )
        .expect("stale snapshot is ignored");
    let stale_invalidation = stale
        .reset(ProviderRef::new("raw-json", 9))
        .expect("valid stale-source transition")
        .expect("stale source invalidation");
    subscriber
        .ingest_flashblock_update(stale_invalidation)
        .expect("stale invalidation is ignored");

    let current_invalidation = adapter
        .reset(ProviderRef::new("raw-json", 9))
        .expect("valid current-source transition")
        .expect("current generation invalidation");
    subscriber
        .ingest_flashblock_update(current_invalidation)
        .expect("current invalidation is accepted");
    let current_invalidation = subscriber
        .next_scoped_batch()
        .await
        .expect("subscriber remains healthy")
        .expect("current invalidation batch");
    assert!(current_invalidation.preconfirmation_invalidated());
    assert!(current_invalidation.records().is_empty());
}

#[tokio::test]
#[cfg(feature = "reactive-ws")]
async fn subscriber_rejects_tampered_or_wrong_source_snapshots_without_queue_mutation() {
    let asserter = Asserter::new();
    asserter.push_success(&U256::from(10));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let source = ProviderRef::new("raw-json", 7);
    let mut subscriber = evm_fork_cache::reactive::AlloySubscriber::<_, Ethereum>::new(
        provider,
        SubscriberMode::PubSub,
        SubscriberConfig {
            preconfirmations: PreconfirmationMode::Required,
            ..SubscriberConfig::default()
        },
    );
    subscriber
        .configure_external_flashblock_updates(source.clone())
        .expect("configure external source");
    subscriber
        .register_interests(&[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(Address::repeat_byte(0x42))
                .event_signature(B256::repeat_byte(0x43)),
            local_matcher: None,
            route_key: None,
        })])
        .await
        .expect("register raw update interest");

    let mut adapter = RawJsonFlashblocksAdapter::new(source);
    let valid = adapter
        .ingest_json(&index_zero())
        .expect("decode raw frame")
        .expect("publish raw frame");
    let mut tampered = valid.clone();
    let FlashblockUpdate::Snapshot(snapshot) = &mut tampered else {
        panic!("expected snapshot")
    };
    snapshot.flashblock.content_hash = B256::repeat_byte(0xff);
    assert!(
        subscriber
            .ingest_flashblock_update(tampered)
            .expect_err("tampered commitment")
            .to_string()
            .contains("content commitment")
    );

    let wrong_source = RawJsonFlashblocksAdapter::new(ProviderRef::new("other-source", 7));
    let mut wrong_source = wrong_source;
    assert!(
        subscriber
            .ingest_flashblock_update(
                wrong_source
                    .ingest_json(&index_zero())
                    .expect("decode other source")
                    .expect("other source snapshot")
            )
            .expect_err("wrong source endpoint")
            .to_string()
            .contains("unexpected provider endpoint")
    );

    subscriber
        .ingest_flashblock_update(valid)
        .expect("valid snapshot remains admissible");
    let batch = subscriber
        .next_scoped_batch()
        .await
        .expect("subscriber remains healthy")
        .expect("only valid snapshot was queued");
    assert_eq!(batch.records().len(), 1);
    assert!(batch.records()[0].scope().is_preconfirmed());
}
