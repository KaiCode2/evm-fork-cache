//! Subscribe to live mainnet AMM logs through `AlloySubscriber`.
//!
//! This example is intentionally RPC-gated. It exits successfully when
//! `RPC_URL` is unset, and it fails if a configured run window sees fewer logs
//! than requested. It also preflights the same filters against recent blocks so
//! a live quiet period can be distinguished from an incorrect AMM filter:
//!
//! ```sh
//! WS_RPC_URL=wss://example-mainnet-endpoint \
//! LIVE_AMM_SECONDS=90 \
//! LIVE_AMM_MIN_EVENTS=3 \
//! LIVE_AMM_PREFLIGHT_BLOCKS=50 \
//! cargo run --example reactive_alloy_amm_live_probe
//! ```
//!
//! Default builds use Alloy pubsub/WebSocket `subscribe_logs`. Compile with
//! `--no-default-features --features reactive,reactive-polling` and set
//! `LIVE_AMM_TRANSPORT=polling` to exercise the HTTP `watch_logs` fallback.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, address, keccak256};
#[cfg(feature = "reactive-ws")]
use alloy_provider::WsConnect;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::{Context, Result, bail};
use evm_fork_cache::reactive::{
    AlloySubscriber, ChainStatus, EventSubscriber, InputSource, LogInterest, ReactiveInput,
    ReactiveInterest, SubscriberConfig, SubscriberMode,
};
use tokio::time::timeout;

#[derive(Clone)]
struct AmmEventTarget {
    name: &'static str,
    address: Address,
    topic0: B256,
}

impl AmmEventTarget {
    fn filter(&self) -> Filter {
        Filter::new()
            .address(self.address)
            .event_signature(self.topic0)
    }
}

fn tracked_amm_events() -> Vec<AmmEventTarget> {
    vec![
        AmmEventTarget {
            name: "UniswapV2 WETH/USDC Swap",
            address: address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"),
            topic0: keccak256(b"Swap(address,uint256,uint256,uint256,uint256,address)"),
        },
        AmmEventTarget {
            name: "UniswapV2 WETH/USDC Sync",
            address: address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"),
            topic0: keccak256(b"Sync(uint112,uint112)"),
        },
        AmmEventTarget {
            name: "UniswapV3 USDC/WETH 0.05% Swap",
            address: address!("88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"),
            topic0: keccak256(b"Swap(address,address,int256,int256,uint160,uint128,int24)"),
        },
        AmmEventTarget {
            name: "Balancer V2 Vault Swap",
            address: address!("BA12222222228d8Ba445958a75a0704d566BF2C8"),
            topic0: keccak256(b"Swap(bytes32,address,address,uint256,uint256)"),
        },
    ]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if std::env::var("LIVE_AMM_TRANSPORT")
        .is_ok_and(|transport| transport.eq_ignore_ascii_case("polling"))
    {
        #[cfg(feature = "reactive-polling")]
        {
            return run_polling_probe().await;
        }

        #[cfg(not(feature = "reactive-polling"))]
        {
            bail!("LIVE_AMM_TRANSPORT=polling requires the reactive-polling feature");
        }
    }

    #[cfg(feature = "reactive-ws")]
    {
        return run_ws_probe().await;
    }

    #[cfg(all(not(feature = "reactive-ws"), feature = "reactive-polling"))]
    {
        return run_polling_probe().await;
    }

    #[cfg(not(any(feature = "reactive-ws", feature = "reactive-polling")))]
    {
        eprintln!("No AlloySubscriber transport feature is enabled for this example.");
        Ok(())
    }
}

#[cfg(feature = "reactive-ws")]
async fn run_ws_probe() -> Result<()> {
    let Ok(rpc_url) = std::env::var("WS_RPC_URL").or_else(|_| std::env::var("RPC_URL")) else {
        eprintln!("WS_RPC_URL not set - skipping reactive_alloy_amm_live_probe.");
        eprintln!(
            "  WS_RPC_URL=<ethereum websocket endpoint> cargo run --example reactive_alloy_amm_live_probe"
        );
        return Ok(());
    };

    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(rpc_url))
        .await
        .context("connect websocket provider")?;
    run_probe(
        provider,
        SubscriberMode::Auto,
        InputSource::Subscription,
        "Alloy subscribe_logs websocket/pubsub",
    )
    .await
}

#[cfg(feature = "reactive-polling")]
async fn run_polling_probe() -> Result<()> {
    let Ok(rpc_url) = std::env::var("RPC_URL") else {
        eprintln!("RPC_URL not set - skipping reactive_alloy_amm_live_probe polling mode.");
        eprintln!(
            "  LIVE_AMM_TRANSPORT=polling RPC_URL=<ethereum http endpoint> cargo run --no-default-features --features reactive,reactive-polling --example reactive_alloy_amm_live_probe"
        );
        return Ok(());
    };

    let provider = ProviderBuilder::new().connect_http(rpc_url.parse().context("valid RPC_URL")?);
    run_probe(
        provider,
        SubscriberMode::Polling,
        InputSource::Poll,
        "Alloy watch_logs HTTP polling",
    )
    .await
}

async fn run_probe<P>(
    provider: P,
    mode: SubscriberMode,
    expected_source: InputSource,
    transport_label: &'static str,
) -> Result<()>
where
    P: Provider + Send + Sync,
{
    let run_seconds = env_u64("LIVE_AMM_SECONDS", 90);
    let min_events = env_usize("LIVE_AMM_MIN_EVENTS", 3);
    let preflight_blocks = env_u64("LIVE_AMM_PREFLIGHT_BLOCKS", 50);
    let targets = tracked_amm_events();

    println!(
        "subscribing to {} AMM event filters for {}s; requiring at least {} log(s)",
        targets.len(),
        run_seconds,
        min_events
    );
    println!("transport: {transport_label}");

    if preflight_blocks > 0 {
        preflight_recent_logs(&provider, &targets, preflight_blocks).await?;
    }

    let mut subscriber = AlloySubscriber::new(
        provider,
        mode,
        SubscriberConfig {
            hydrate_pending_transactions: false,
            max_batch_size: 256,
            ..SubscriberConfig::default()
        },
    );

    let interests = targets
        .iter()
        .map(|target| {
            ReactiveInterest::Logs(LogInterest {
                provider_filter: target.filter(),
                local_matcher: None,
                route_key: None,
            })
        })
        .collect::<Vec<_>>();
    subscriber.register_interests(&interests)?;

    let started = Instant::now();
    let run_for = Duration::from_secs(run_seconds);
    let mut counts = BTreeMap::<&'static str, usize>::new();
    let mut total = 0usize;
    let mut removed = 0usize;

    while started.elapsed() < run_for && total < min_events {
        let remaining = run_for.saturating_sub(started.elapsed());
        let batch = match timeout(remaining, subscriber.next_batch()).await {
            Ok(Ok(Some(batch))) => batch,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => break,
        };

        println!("batch: {} record(s)", batch.records().len());
        for record in batch.records() {
            if record.context.source != expected_source {
                bail!(
                    "expected {:?} source, got {:?}",
                    expected_source,
                    record.context.source
                );
            }

            let ReactiveInput::Log(log) = &record.input else {
                bail!("expected only log inputs, got {:?}", record.input);
            };

            let Some(target) = identify_target(&targets, log) else {
                bail!(
                    "subscriber emitted an untracked log from {} topic0={:?}",
                    log.address(),
                    log.topics().first()
                );
            };

            match (&record.context.chain_status, log.removed) {
                (
                    ChainStatus::Included {
                        block,
                        confirmations,
                    },
                    false,
                ) => {
                    if Some(block.number) != log.block_number {
                        bail!(
                            "context block {} disagrees with log block {:?}",
                            block.number,
                            log.block_number
                        );
                    }
                    if *confirmations != 0 {
                        bail!("subscriber logs should currently report zero confirmations");
                    }
                }
                (ChainStatus::Reorged { dropped_from }, true) => {
                    if Some(dropped_from.number) != log.block_number {
                        bail!(
                            "reorg context block {} disagrees with removed log block {:?}",
                            dropped_from.number,
                            log.block_number
                        );
                    }
                    removed += 1;
                }
                (status, was_removed) => {
                    bail!(
                        "unexpected chain status {:?} for removed={}",
                        status,
                        was_removed
                    );
                }
            }

            *counts.entry(target.name).or_default() += 1;
            total += 1;

            println!(
                "  {:<34} block={} tx={} log_index={:?} removed={}",
                target.name,
                log.block_number
                    .map_or_else(|| "pending".to_owned(), |number| number.to_string()),
                log.transaction_hash
                    .map_or_else(|| "-".to_owned(), |hash| hash.to_string()),
                log.log_index,
                log.removed
            );
        }
    }

    println!(
        "summary: observed {} log(s), {} removed/reorged",
        total, removed
    );
    for target in &targets {
        println!(
            "  {:<34} {}",
            target.name,
            counts.get(target.name).copied().unwrap_or_default()
        );
    }

    if total < min_events {
        bail!(
            "observed {} log(s), below LIVE_AMM_MIN_EVENTS={}; increase LIVE_AMM_SECONDS or use a filter-capable RPC endpoint",
            total,
            min_events
        );
    }

    Ok(())
}

async fn preflight_recent_logs<P>(
    provider: &P,
    targets: &[AmmEventTarget],
    blocks: u64,
) -> Result<()>
where
    P: Provider,
{
    let latest = provider.get_block_number().await?;
    let from = latest.saturating_sub(blocks);
    let mut total = 0usize;

    println!(
        "preflight: scanning recent AMM logs over blocks {}..={} with the same filters",
        from, latest
    );
    for target in targets {
        let logs = provider
            .get_logs(&target.filter().from_block(from).to_block(latest))
            .await?;
        total += logs.len();
        println!("  {:<34} {}", target.name, logs.len());
    }

    if total == 0 {
        bail!(
            "preflight observed zero AMM logs over the last {} block(s); filters or endpoint are not suitable for this probe",
            blocks
        );
    }

    println!("preflight: observed {} recent log(s)", total);
    Ok(())
}

fn identify_target<'a>(targets: &'a [AmmEventTarget], log: &Log) -> Option<&'a AmmEventTarget> {
    let topic0 = log.topics().first().copied()?;
    targets
        .iter()
        .find(|target| target.address == log.address() && target.topic0 == topic0)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
