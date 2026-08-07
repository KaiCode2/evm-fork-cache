use std::time::Duration;

use alloy_network::Ethereum;
use alloy_primitives::{B256, Bytes, keccak256};
use alloy_provider::ProviderBuilder;
use alloy_transport::mock::Asserter;
use criterion::{BatchSize, Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use evm_fork_cache::reactive::{
    AlloySubscriber, PreconfirmationMode, ProviderRef, RawJsonFlashblocksAdapter,
    RawJsonFlashblocksLimits, SubscriberConfig, SubscriberMode,
};

const TX_TWO: B256 =
    alloy_primitives::b256!("f2ee15ea639b73fa3db9b34a245bdfa015c260c598b211bf05a1ecc4b3e3b4f2");

fn index_zero() -> &'static [u8] {
    br#"{
        "payload_id":"0x1111111111111111",
        "index":0,
        "base":{
            "parent_hash":"0x6464646464646464646464646464646464646464646464646464646464646464",
            "block_number":"0x65",
            "timestamp":"0x6553f165",
            "gas_limit":"0x1c9c380",
            "base_fee_per_gas":"0x7"
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
                    "logs":[{
                        "address":"0x4242424242424242424242424242424242424242",
                        "topics":["0x4343434343434343434343434343434343434343434343434343434343434343"],
                        "data":"0x0102"
                    }]
                }
            }
        }
    }"#
}

fn index_one() -> Vec<u8> {
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
                    "{TX_TWO:#x}":{{
                        "logs":[{{
                            "address":"0x4444444444444444444444444444444444444444",
                            "topics":[],
                            "data":"0x03"
                        }}]
                    }}
                }}
            }}
        }}"#,
    )
    .into_bytes()
}

fn adapter() -> RawJsonFlashblocksAdapter {
    RawJsonFlashblocksAdapter::new(ProviderRef::new("benchmark", 1))
}

fn scaled_frame(transaction_count: usize, logs_per_transaction: usize) -> Vec<u8> {
    scaled_frame_with_log_data(transaction_count, logs_per_transaction, "0x01020304")
}

fn scaled_frame_with_log_data(
    transaction_count: usize,
    logs_per_transaction: usize,
    log_data: &str,
) -> Vec<u8> {
    let mut transactions = Vec::with_capacity(transaction_count);
    let mut receipts = serde_json::Map::with_capacity(transaction_count);
    for transaction_index in 0..transaction_count {
        let raw = format!("0x02{:016x}", transaction_index + 1);
        let bytes = raw.parse::<Bytes>().expect("benchmark transaction bytes");
        let hash = keccak256(bytes);
        transactions.push(serde_json::Value::String(raw));
        let logs = (0..logs_per_transaction)
            .map(|log_index| {
                serde_json::json!({
                    "address": format!("0x{:040x}", transaction_index + 1),
                    "topics": [format!("0x{:064x}", log_index + 1)],
                    "data": log_data
                })
            })
            .collect::<Vec<_>>();
        receipts.insert(format!("{hash:#x}"), serde_json::json!({ "logs": logs }));
    }
    serde_json::to_vec(&serde_json::json!({
        "payload_id": "0x1111111111111111",
        "index": 0,
        "base": {
            "parent_hash": "0x6464646464646464646464646464646464646464646464646464646464646464",
            "block_number": "0x65",
            "timestamp": "0x6553f165",
            "gas_limit": "0x1c9c380",
            "base_fee_per_gas": "0x7"
        },
        "diff": {
            "state_root": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "block_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "transactions": transactions
        },
        "metadata": {
            "block_number": 101,
            "receipts": receipts
        }
    }))
    .expect("serialize scaled benchmark frame")
}

fn benchmark_raw_json_flashblocks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("raw_json_flashblocks");
    group.bench_function("index_zero_decode_normalize", |bencher| {
        bencher.iter_batched(
            adapter,
            |mut adapter| {
                std::hint::black_box(
                    adapter
                        .ingest_json(std::hint::black_box(index_zero()))
                        .expect("benchmark fixture"),
                )
            },
            BatchSize::SmallInput,
        );
    });

    let next = index_one();
    group.bench_function("next_delta_decode_normalize", |bencher| {
        bencher.iter_batched(
            || {
                let mut adapter = adapter();
                adapter
                    .ingest_json(index_zero())
                    .expect("benchmark base fixture");
                adapter
            },
            |mut adapter| {
                std::hint::black_box(
                    adapter
                        .ingest_json(std::hint::black_box(&next))
                        .expect("benchmark delta fixture"),
                )
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();

    let scaled = scaled_frame(250, 2);
    let mut scaled_group = criterion.benchmark_group("raw_json_flashblocks_scaled");
    scaled_group.throughput(Throughput::Bytes(
        u64::try_from(scaled.len()).expect("benchmark frame length fits u64"),
    ));
    scaled_group.bench_function("250_transactions_500_logs", |bencher| {
        bencher.iter_batched(
            adapter,
            |mut adapter| {
                std::hint::black_box(
                    adapter
                        .ingest_json(std::hint::black_box(&scaled))
                        .expect("scaled benchmark fixture"),
                )
            },
            BatchSize::SmallInput,
        );
    });
    scaled_group.finish();

    let qualified_application_limit = 4 * 1024 * 1024;
    let application_limit =
        scaled_frame_with_log_data(4_500, 2, &format!("0x{}", "01".repeat(128)));
    assert!(application_limit.len() <= qualified_application_limit);
    assert!(application_limit.len() >= 3 * 1024 * 1024);
    let mut application_group = criterion.benchmark_group("raw_json_flashblocks_application_limit");
    application_group.sample_size(20);
    application_group.sampling_mode(SamplingMode::Flat);
    application_group.warm_up_time(Duration::from_secs(2));
    application_group.measurement_time(Duration::from_secs(10));
    application_group.throughput(Throughput::Bytes(
        u64::try_from(application_limit.len()).expect("benchmark frame length fits u64"),
    ));
    application_group.bench_function(
        format!(
            "4500_transactions_9000_logs_{}_bytes",
            application_limit.len()
        ),
        |bencher| {
            bencher.iter_batched(
                adapter,
                |mut adapter| {
                    std::hint::black_box(
                        adapter
                            .ingest_json(std::hint::black_box(&application_limit))
                            .expect("application-limit benchmark fixture"),
                    )
                },
                BatchSize::LargeInput,
            );
        },
    );
    application_group.finish();

    // Exercise the parser close to its intentionally conservative library
    // ceiling without making every consumer pay that memory/latency budget.
    // Applications should qualify and configure smaller limits for their
    // observed provider profile.
    let near_limit = scaled_frame_with_log_data(17_000, 2, &format!("0x{}", "01".repeat(128)));
    let default_frame_limit = RawJsonFlashblocksLimits::default().max_frame_bytes;
    assert!(near_limit.len() <= default_frame_limit);
    assert!(near_limit.len() >= 12 * 1024 * 1024);
    let mut stress_group = criterion.benchmark_group("raw_json_flashblocks_near_limit");
    stress_group.sample_size(20);
    stress_group.sampling_mode(SamplingMode::Flat);
    stress_group.warm_up_time(Duration::from_secs(2));
    stress_group.measurement_time(Duration::from_secs(20));
    stress_group.throughput(Throughput::Bytes(
        u64::try_from(near_limit.len()).expect("benchmark frame length fits u64"),
    ));
    stress_group.bench_function(
        format!("17000_transactions_34000_logs_{}_bytes", near_limit.len()),
        |bencher| {
            bencher.iter_batched(
                adapter,
                |mut adapter| {
                    std::hint::black_box(
                        adapter
                            .ingest_json(std::hint::black_box(&near_limit))
                            .expect("near-limit benchmark fixture"),
                    )
                },
                BatchSize::LargeInput,
            );
        },
    );
    stress_group.finish();

    let handoff_frame = index_zero();
    let mut handoff_group = criterion.benchmark_group("raw_json_flashblocks_handoff");
    handoff_group.bench_function("bounded_try_send", |bencher| {
        bencher.iter_batched(
            || {
                let source = ProviderRef::new("benchmark", 1);
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
                    .expect("configure benchmark external source");
                let sender = subscriber
                    .open_external_flashblock_update_channel(1)
                    .expect("benchmark update channel");
                let mut adapter = RawJsonFlashblocksAdapter::new(source);
                let update = adapter
                    .ingest_json(handoff_frame)
                    .expect("handoff benchmark fixture")
                    .expect("handoff benchmark snapshot");
                (sender, subscriber, update)
            },
            |(sender, subscriber, update)| {
                let acknowledgement = sender.try_send(update).expect("bounded handoff");
                drop(std::hint::black_box(acknowledgement));
                std::hint::black_box(subscriber);
            },
            BatchSize::SmallInput,
        );
    });
    handoff_group.finish();
}

criterion_group!(benches, benchmark_raw_json_flashblocks);
criterion_main!(benches);
