//! Manager-authored acceptance tests for reactive routing and filter planning.
//!
//! These tests pin the public behavior for the provider filter consolidation
//! and routing-index phase. They should fail before the router/registry surface
//! exists and pass once filters are consolidated as safe supersets while local
//! routing remains exact.
#![cfg(feature = "reactive")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, keccak256};
use alloy_rpc_types_eth::{Filter, Log};
use anyhow::Result;

use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    HandlerError, HandlerId, HandlerOutcome, LogInterest, LogMatcher, LogRouteIndex, LogRouteKey,
    ReactiveContext, ReactiveEffect, ReactiveHandler, ReactiveInput, ReactiveInterest,
    ReactiveRegistry, RouteKey, RouteKeySpec, StateEffectQuality,
};

fn rpc_log(address: Address, topics: Vec<B256>) -> Log {
    Log {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::new()),
        block_hash: Some(B256::repeat_byte(0x10)),
        block_number: Some(10),
        block_timestamp: Some(1_700_000_010),
        transaction_hash: Some(B256::repeat_byte(0x20)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

#[derive(Clone)]
struct NoopHandler {
    id: HandlerId,
    interest: LogInterest,
}

impl NoopHandler {
    fn new(id: &'static str, interest: LogInterest) -> Self {
        Self {
            id: HandlerId::new(id),
            interest,
        }
    }
}

impl ReactiveHandler<Ethereum> for NoopHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(self.interest.clone())]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        _input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::Hook(evm_fork_cache::reactive::HookSignal {
                namespace: "test".into(),
                kind: self.id.as_str().to_owned().into(),
                labels: vec![],
                payload: None,
            })],
            quality: StateEffectQuality::NoStateEffect,
            tags: vec![],
        })
    }
}

struct TopicMatcher {
    index: usize,
    value: B256,
}

struct CountingTopicHandler {
    id: HandlerId,
    vault: Address,
    signature: B256,
    pool: B256,
    calls: Arc<AtomicUsize>,
}

struct CountingTopicMatcher {
    pool: B256,
    calls: Arc<AtomicUsize>,
}

impl LogMatcher for CountingTopicMatcher {
    fn matches(&self, log: &Log) -> bool {
        self.calls.fetch_add(1, Ordering::Relaxed);
        log.topics().get(1) == Some(&self.pool)
    }
}

impl ReactiveHandler<Ethereum> for CountingTopicHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(self.vault)
                .event_signature(self.signature),
            local_matcher: Some(Arc::new(CountingTopicMatcher {
                pool: self.pool,
                calls: self.calls.clone(),
            })),
            route_key: Some(RouteKeySpec::Topic { index: 1 }),
        })]
    }

    fn log_route_index(&self) -> Option<LogRouteIndex> {
        Some(LogRouteIndex::single(LogRouteKey::Topic {
            index: 1,
            value: self.pool,
        }))
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

#[test]
fn reactive_router_unions_topic_index_and_fallback_in_registration_order() -> Result<()> {
    let vault = Address::repeat_byte(0xcc);
    let swap_sig = keccak256(b"Swap(bytes32,address,int256)");
    let pool_a = B256::repeat_byte(0xa0);
    let pool_b = B256::repeat_byte(0xb0);
    let calls_a = Arc::new(AtomicUsize::new(0));
    let calls_b = Arc::new(AtomicUsize::new(0));
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry.register_handler(Arc::new(NoopHandler::new(
        "all-swaps",
        LogInterest {
            provider_filter: Filter::new().address(vault).event_signature(swap_sig),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        },
    )))?;
    registry.register_handler(Arc::new(CountingTopicHandler {
        id: HandlerId::new("pool-a"),
        vault,
        signature: swap_sig,
        pool: pool_a,
        calls: calls_a.clone(),
    }))?;
    registry.register_handler(Arc::new(CountingTopicHandler {
        id: HandlerId::new("pool-b"),
        vault,
        signature: swap_sig,
        pool: pool_b,
        calls: calls_b.clone(),
    }))?;
    registry.register_handler(Arc::new(NoopHandler::new(
        "audit",
        LogInterest {
            provider_filter: Filter::new().address(vault).event_signature(swap_sig),
            local_matcher: None,
            route_key: None,
        },
    )))?;

    let routes = registry.route_log(&rpc_log(vault, vec![swap_sig, pool_b]));
    let ids: Vec<_> = routes.into_iter().map(|route| route.handler_id).collect();

    assert_eq!(
        ids,
        vec![
            HandlerId::new("all-swaps"),
            HandlerId::new("pool-b"),
            HandlerId::new("audit")
        ]
    );
    assert_eq!(calls_a.load(Ordering::Relaxed), 0);
    assert_eq!(calls_b.load(Ordering::Relaxed), 1);
    Ok(())
}

impl LogMatcher for TopicMatcher {
    fn matches(&self, log: &Log) -> bool {
        log.topics().get(self.index) == Some(&self.value)
    }
}

#[test]
fn reactive_registry_consolidates_provider_filters_as_safe_superset() -> Result<()> {
    let token_a = Address::repeat_byte(0xa1);
    let token_b = Address::repeat_byte(0xb2);
    let sig_a = keccak256(b"TokenAEvent()");
    let sig_b = keccak256(b"TokenBEvent()");

    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry.register_handler(Arc::new(NoopHandler::new(
        "token-a",
        LogInterest {
            provider_filter: Filter::new().address(token_a).event_signature(sig_a),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        },
    )))?;
    registry.register_handler(Arc::new(NoopHandler::new(
        "token-b",
        LogInterest {
            provider_filter: Filter::new().address(token_b).event_signature(sig_b),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        },
    )))?;

    let filters = registry.log_subscription_filters();
    assert_eq!(filters.len(), 1, "compatible log interests should merge");
    let consolidated = &filters[0];

    let wanted_a = rpc_log(token_a, vec![sig_a]);
    let wanted_b = rpc_log(token_b, vec![sig_b]);
    let overfetched = rpc_log(token_a, vec![sig_b]);

    assert!(consolidated.rpc_matches(&wanted_a));
    assert!(consolidated.rpc_matches(&wanted_b));
    assert!(
        consolidated.rpc_matches(&overfetched),
        "merged filters may be a safe provider-side superset"
    );

    let route_a = registry.route_log(&wanted_a);
    let route_b = registry.route_log(&wanted_b);
    let overfetch_routes = registry.route_log(&overfetched);

    assert_eq!(route_a.len(), 1);
    assert_eq!(route_a[0].handler_id, HandlerId::new("token-a"));
    assert_eq!(route_a[0].route_key, Some(RouteKey::Address(token_a)));
    assert_eq!(route_b.len(), 1);
    assert_eq!(route_b[0].handler_id, HandlerId::new("token-b"));
    assert_eq!(route_b[0].route_key, Some(RouteKey::Address(token_b)));
    assert!(
        overfetch_routes.is_empty(),
        "local routing must remain exact after provider-side consolidation"
    );

    Ok(())
}

#[test]
fn reactive_router_routes_shared_emitters_by_route_key_in_registration_order() -> Result<()> {
    let vault = Address::repeat_byte(0xcc);
    let swap_sig = keccak256(b"Swap(bytes32,address,int256)");
    let pool_a = B256::repeat_byte(0xa0);
    let pool_b = B256::repeat_byte(0xb0);
    let pool_c = B256::repeat_byte(0xc0);

    let all_swaps = NoopHandler::new(
        "all-swaps",
        LogInterest {
            provider_filter: Filter::new().address(vault).event_signature(swap_sig),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        },
    );
    let pool_a_handler = NoopHandler::new(
        "pool-a",
        LogInterest {
            provider_filter: Filter::new().address(vault).event_signature(swap_sig),
            local_matcher: Some(Arc::new(TopicMatcher {
                index: 1,
                value: pool_a,
            })),
            route_key: Some(RouteKeySpec::Topic { index: 1 }),
        },
    );
    let pool_b_handler = NoopHandler::new(
        "pool-b",
        LogInterest {
            provider_filter: Filter::new().address(vault).event_signature(swap_sig),
            local_matcher: Some(Arc::new(TopicMatcher {
                index: 1,
                value: pool_b,
            })),
            route_key: Some(RouteKeySpec::Topic { index: 1 }),
        },
    );

    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry.register_handler(Arc::new(all_swaps))?;
    registry.register_handler(Arc::new(pool_a_handler))?;
    registry.register_handler(Arc::new(pool_b_handler))?;

    let filters = registry.log_subscription_filters();
    assert_eq!(
        filters.len(),
        1,
        "shared-emitter interests should share one provider filter"
    );
    assert!(filters[0].rpc_matches(&rpc_log(vault, vec![swap_sig, pool_a])));
    assert!(filters[0].rpc_matches(&rpc_log(vault, vec![swap_sig, pool_b])));

    let routes = registry.route_log(&rpc_log(vault, vec![swap_sig, pool_b]));
    let routed: Vec<_> = routes
        .iter()
        .map(|route| (route.handler_id.clone(), route.route_key.clone()))
        .collect();
    assert_eq!(
        routed,
        vec![
            (HandlerId::new("all-swaps"), Some(RouteKey::Address(vault))),
            (HandlerId::new("pool-b"), Some(RouteKey::Bytes32(pool_b))),
        ],
        "matching handlers must be returned in registration order with route keys"
    );

    let unrelated_pool_routes = registry.route_log(&rpc_log(vault, vec![swap_sig, pool_c]));
    assert_eq!(unrelated_pool_routes.len(), 1);
    assert_eq!(
        unrelated_pool_routes[0].handler_id,
        HandlerId::new("all-swaps"),
        "custom local matchers must exclude nonmatching shared-emitter handlers"
    );

    Ok(())
}
