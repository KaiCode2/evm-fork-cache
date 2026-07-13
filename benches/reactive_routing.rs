//! Offline comparison of indexed and compatibility-fallback log routing.

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog};
use alloy_rpc_types_eth::{Filter, Log};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    HandlerError, HandlerId, HandlerOutcome, LogInterest, LogRouteIndex, LogRouteKey,
    ReactiveContext, ReactiveHandler, ReactiveInput, ReactiveInterest, ReactiveRegistry,
    RouteKeySpec, StateEffectQuality,
};

const HANDLER_COUNTS: [usize; 4] = [16, 64, 320, 4_096];

struct BenchHandler {
    id: HandlerId,
    emitter: Address,
    indexed: bool,
}

impl ReactiveHandler<Ethereum> for BenchHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(self.emitter),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn log_route_index(&self) -> Option<LogRouteIndex> {
        self.indexed
            .then(|| LogRouteIndex::single(LogRouteKey::Emitter(self.emitter)))
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect))
    }
}

fn address(index: usize) -> Address {
    let mut bytes = [0_u8; 20];
    bytes[12..].copy_from_slice(&(index as u64 + 1).to_be_bytes());
    Address::from(bytes)
}

fn registry(count: usize, indexed: bool) -> ReactiveRegistry<Ethereum> {
    let mut registry = ReactiveRegistry::new();
    for index in 0..count {
        registry
            .register_handler(Arc::new(BenchHandler {
                id: HandlerId::new(format!("handler-{index}")),
                emitter: address(index),
                indexed,
            }))
            .expect("unique handler");
    }
    registry
}

#[derive(Clone, Copy)]
enum ChurnMode {
    Fallback,
    IndexedDistinct,
    IndexedShared,
}

impl ChurnMode {
    fn name(self) -> &'static str {
        match self {
            Self::Fallback => "fallback",
            Self::IndexedDistinct => "indexed_distinct",
            Self::IndexedShared => "indexed_shared_key",
        }
    }

    fn indexed(self) -> bool {
        !matches!(self, Self::Fallback)
    }

    fn emitter(self, index: usize) -> Address {
        match self {
            Self::IndexedShared => Address::repeat_byte(0xee),
            Self::Fallback | Self::IndexedDistinct => address(index),
        }
    }
}

fn churn_registry(count: usize, mode: ChurnMode) -> ReactiveRegistry<Ethereum> {
    let mut registry = ReactiveRegistry::new();
    for index in 0..count {
        registry
            .register_handler(Arc::new(BenchHandler {
                id: HandlerId::new(format!("handler-{index}")),
                emitter: mode.emitter(index),
                indexed: mode.indexed(),
            }))
            .expect("unique handler");
    }
    registry
}

fn log(emitter: Address) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(emitter, vec![], Bytes::new()),
        block_hash: Some(B256::repeat_byte(0x01)),
        block_number: Some(1),
        ..Log::default()
    }
}

fn reactive_routing(c: &mut Criterion) {
    for (name, indexed, target_offset, expected_routes) in [
        ("indexed_emitter", true, 0, 1),
        ("indexed_miss", true, 10_000, 0),
        ("fallback_scan", false, 0, 1),
    ] {
        let mut group = c.benchmark_group(format!("reactive_routing/{name}"));
        for count in HANDLER_COUNTS {
            let registry = registry(count, indexed);
            let target = log(address(count - 1 + target_offset));
            assert_eq!(registry.route_log(&target).len(), expected_routes);
            group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
                b.iter(|| black_box(registry.route_log(black_box(&target))))
            });
        }
        group.finish();
    }

    for mode in [
        ChurnMode::Fallback,
        ChurnMode::IndexedDistinct,
        ChurnMode::IndexedShared,
    ] {
        let mut group = c.benchmark_group(format!("reactive_lifecycle/churn/{}", mode.name()));
        for count in HANDLER_COUNTS {
            let mut registry = churn_registry(count, mode);
            let target_id = HandlerId::new(format!("handler-{}", count - 1));
            let emitter = mode.emitter(count - 1);
            group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
                b.iter(|| {
                    let removed = registry
                        .unregister_handler(black_box(&target_id))
                        .expect("target remains active");
                    black_box(removed);
                    registry
                        .register_handler(Arc::new(BenchHandler {
                            id: target_id.clone(),
                            emitter,
                            indexed: mode.indexed(),
                        }))
                        .expect("re-register target");
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, reactive_routing);
criterion_main!(benches);
