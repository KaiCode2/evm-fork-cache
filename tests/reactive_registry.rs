//! Implementation-owned registry regression tests.
#![cfg(feature = "reactive")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use alloy_network::Ethereum;
use alloy_primitives::Address;
use alloy_rpc_types_eth::Filter;

use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    HandlerError, HandlerId, HandlerOutcome, LogInterest, LogRouteIndex, LogRouteKey,
    ReactiveContext, ReactiveHandler, ReactiveInput, ReactiveInterest, ReactiveRegistry,
    RegisterError, RouteKeySpec, StateEffectQuality,
};

struct NoopHandler {
    id: HandlerId,
    address: Address,
}

impl NoopHandler {
    fn new(id: impl Into<String>, address: Address) -> Self {
        Self {
            id: HandlerId::new(id),
            address,
        }
    }
}

impl ReactiveHandler<Ethereum> for NoopHandler {
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
        Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect))
    }
}

struct CountingAddressMatcher {
    address: Address,
    calls: Arc<AtomicUsize>,
}

impl evm_fork_cache::reactive::LogMatcher for CountingAddressMatcher {
    fn matches(&self, log: &alloy_rpc_types_eth::Log) -> bool {
        self.calls.fetch_add(1, Ordering::Relaxed);
        log.address() == self.address
    }
}

struct IndexedHandler {
    id: HandlerId,
    address: Address,
    matcher_calls: Arc<AtomicUsize>,
}

struct CountingDataMatcher {
    expected: Vec<u8>,
    calls: Arc<AtomicUsize>,
}

impl evm_fork_cache::reactive::LogMatcher for CountingDataMatcher {
    fn matches(&self, log: &alloy_rpc_types_eth::Log) -> bool {
        self.calls.fetch_add(1, Ordering::Relaxed);
        log.inner.data.data.as_ref().get(..self.expected.len()) == Some(self.expected.as_slice())
    }
}

struct IndexedDataHandler {
    id: HandlerId,
    expected: Vec<u8>,
    matcher_calls: Arc<AtomicUsize>,
}

impl ReactiveHandler<Ethereum> for IndexedDataHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new(),
            local_matcher: Some(Arc::new(CountingDataMatcher {
                expected: self.expected.clone(),
                calls: self.matcher_calls.clone(),
            })),
            route_key: Some(RouteKeySpec::DataSlice {
                offset: 0,
                len: self.expected.len(),
            }),
        })]
    }

    fn log_route_index(&self) -> Option<LogRouteIndex> {
        Some(LogRouteIndex::single(LogRouteKey::DataSlice {
            offset: 0,
            value: self.expected.clone(),
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

impl ReactiveHandler<Ethereum> for IndexedHandler {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new(),
            local_matcher: Some(Arc::new(CountingAddressMatcher {
                address: self.address,
                calls: self.matcher_calls.clone(),
            })),
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn log_route_index(&self) -> Option<LogRouteIndex> {
        Some(LogRouteIndex::single(LogRouteKey::Emitter(self.address)))
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
fn reactive_registry_exact_route_index_skips_unrelated_matchers() {
    let pool_a = Address::repeat_byte(0xa1);
    let pool_b = Address::repeat_byte(0xb2);
    let calls_a = Arc::new(AtomicUsize::new(0));
    let calls_b = Arc::new(AtomicUsize::new(0));
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry
        .register_handler(Arc::new(IndexedHandler {
            id: HandlerId::new("pool-a"),
            address: pool_a,
            matcher_calls: calls_a.clone(),
        }))
        .expect("register pool a");
    registry
        .register_handler(Arc::new(IndexedHandler {
            id: HandlerId::new("pool-b"),
            address: pool_b,
            matcher_calls: calls_b.clone(),
        }))
        .expect("register pool b");

    let routes = registry.route_log(&rpc_log(pool_b));

    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].handler_id, HandlerId::new("pool-b"));
    assert_eq!(calls_a.load(Ordering::Relaxed), 0);
    assert_eq!(calls_b.load(Ordering::Relaxed), 1);

    assert!(
        registry
            .route_log(&rpc_log(Address::repeat_byte(0xff)))
            .is_empty()
    );
    assert_eq!(calls_a.load(Ordering::Relaxed), 0);
    assert_eq!(calls_b.load(Ordering::Relaxed), 1);
}

#[test]
fn reactive_registry_reused_handler_id_does_not_retain_old_index_membership() {
    let old_address = Address::repeat_byte(0xa1);
    let replacement_address = Address::repeat_byte(0xb2);
    let old_calls = Arc::new(AtomicUsize::new(0));
    let replacement_calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry
        .register_handler(Arc::new(IndexedHandler {
            id: HandlerId::new("pool"),
            address: old_address,
            matcher_calls: old_calls.clone(),
        }))
        .expect("register old handler");
    assert_eq!(registry.route_log(&rpc_log(old_address)).len(), 1);

    registry
        .unregister_handler(&HandlerId::new("pool"))
        .expect("unregister old handler");
    registry
        .register_handler(Arc::new(IndexedHandler {
            id: HandlerId::new("pool"),
            address: replacement_address,
            matcher_calls: replacement_calls.clone(),
        }))
        .expect("register replacement handler");

    assert!(registry.route_log(&rpc_log(old_address)).is_empty());
    assert_eq!(registry.route_log(&rpc_log(replacement_address)).len(), 1);
    assert_eq!(old_calls.load(Ordering::Relaxed), 1);
    assert_eq!(replacement_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn reactive_registry_indexes_exact_data_slices() {
    let calls_a = Arc::new(AtomicUsize::new(0));
    let calls_b = Arc::new(AtomicUsize::new(0));
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry
        .register_handler(Arc::new(IndexedDataHandler {
            id: HandlerId::new("data-a"),
            expected: vec![0xaa, 0x01],
            matcher_calls: calls_a.clone(),
        }))
        .expect("register data a");
    registry
        .register_handler(Arc::new(IndexedDataHandler {
            id: HandlerId::new("data-b"),
            expected: vec![0xbb, 0x02],
            matcher_calls: calls_b.clone(),
        }))
        .expect("register data b");

    let routes = registry.route_log(&rpc_log_with_data(&[0xbb, 0x02, 0xff]));

    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].handler_id, HandlerId::new("data-b"));
    assert_eq!(calls_a.load(Ordering::Relaxed), 0);
    assert_eq!(calls_b.load(Ordering::Relaxed), 1);
}

#[test]
fn reactive_registry_repeated_churn_preserves_fallback_and_shared_index_order() {
    const HANDLERS: usize = 128;
    const CHURN_ROUNDS: usize = 1_000;

    let mut fallback = ReactiveRegistry::<Ethereum>::new();
    for index in 0..HANDLERS {
        fallback
            .register_handler(Arc::new(NoopHandler::new(
                format!("fallback-{index}"),
                Address::repeat_byte(index as u8),
            )))
            .expect("register fallback handler");
    }
    let fallback_id = HandlerId::new("fallback-64");
    for _ in 0..CHURN_ROUNDS {
        fallback
            .unregister_handler(&fallback_id)
            .expect("remove fallback owner");
        fallback
            .register_handler(Arc::new(NoopHandler::new(
                "fallback-64",
                Address::repeat_byte(64),
            )))
            .expect("re-register fallback owner");
    }
    assert_eq!(fallback.handler_ids().len(), HANDLERS);
    assert_eq!(fallback.handler_ids().last(), Some(&fallback_id));
    assert_eq!(
        fallback.route_log(&rpc_log(Address::repeat_byte(64))).len(),
        1
    );

    let shared_emitter = Address::repeat_byte(0xee);
    let mut shared = ReactiveRegistry::<Ethereum>::new();
    for index in 0..HANDLERS {
        shared
            .register_handler(Arc::new(IndexedHandler {
                id: HandlerId::new(format!("shared-{index}")),
                address: shared_emitter,
                matcher_calls: Arc::new(AtomicUsize::new(0)),
            }))
            .expect("register shared-key handler");
    }
    let shared_id = HandlerId::new("shared-64");
    for _ in 0..CHURN_ROUNDS {
        shared
            .unregister_handler(&shared_id)
            .expect("remove shared-key owner");
        shared
            .register_handler(Arc::new(IndexedHandler {
                id: shared_id.clone(),
                address: shared_emitter,
                matcher_calls: Arc::new(AtomicUsize::new(0)),
            }))
            .expect("re-register shared-key owner");
    }
    assert_eq!(shared.handler_ids().len(), HANDLERS);
    assert_eq!(shared.handler_ids().last(), Some(&shared_id));
    assert_eq!(shared.route_log(&rpc_log(shared_emitter)).len(), HANDLERS);
}

#[test]
fn reactive_registry_rejects_duplicate_handler_ids() {
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry
        .register_handler(Arc::new(NoopHandler::new(
            "duplicate",
            Address::repeat_byte(0x11),
        )))
        .expect("first registration should succeed");

    let err = registry
        .register_handler(Arc::new(NoopHandler::new(
            "duplicate",
            Address::repeat_byte(0x22),
        )))
        .expect_err("duplicate handler id must be rejected");

    assert!(matches!(
        err,
        RegisterError::DuplicateHandler(id) if id == HandlerId::new("duplicate")
    ));
}

#[test]
fn reactive_registry_unregisters_one_handler_without_rebuilding_others() {
    let pool_a = Address::repeat_byte(0xa1);
    let pool_b = Address::repeat_byte(0xb2);
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry
        .register_handler(Arc::new(NoopHandler::new("pool-a", pool_a)))
        .expect("register pool a");
    registry
        .register_handler(Arc::new(NoopHandler::new("pool-b", pool_b)))
        .expect("register pool b");

    assert!(registry.contains_handler(&HandlerId::new("pool-a")));
    assert!(registry.contains_handler(&HandlerId::new("pool-b")));
    assert_eq!(registry.interests().len(), 2);
    assert_eq!(
        registry
            .handler_interests(&HandlerId::new("pool-a"))
            .expect("pool a interests")
            .len(),
        1
    );

    let removed = registry
        .unregister_handler(&HandlerId::new("pool-a"))
        .expect("pool a should be removed");
    assert_eq!(removed.id(), HandlerId::new("pool-a"));

    assert!(!registry.contains_handler(&HandlerId::new("pool-a")));
    assert!(registry.contains_handler(&HandlerId::new("pool-b")));
    assert_eq!(registry.interests().len(), 1);
    assert!(registry.route_log(&rpc_log(pool_a)).is_empty());
    assert_eq!(registry.route_log(&rpc_log(pool_b)).len(), 1);

    registry
        .register_handler(Arc::new(NoopHandler::new("pool-a", pool_a)))
        .expect("unregistered id may be reused");
    assert!(registry.contains_handler(&HandlerId::new("pool-a")));
    assert_eq!(registry.interests().len(), 2);
}

fn rpc_log(address: Address) -> alloy_rpc_types_eth::Log {
    alloy_rpc_types_eth::Log {
        inner: alloy_primitives::Log::new_unchecked(
            address,
            vec![],
            alloy_primitives::Bytes::new(),
        ),
        block_hash: Some(alloy_primitives::B256::repeat_byte(0x10)),
        block_number: Some(10),
        block_timestamp: Some(1_700_000_010),
        transaction_hash: Some(alloy_primitives::B256::repeat_byte(0x20)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

fn rpc_log_with_data(data: &[u8]) -> alloy_rpc_types_eth::Log {
    alloy_rpc_types_eth::Log {
        inner: alloy_primitives::Log::new_unchecked(
            Address::ZERO,
            vec![],
            alloy_primitives::Bytes::copy_from_slice(data),
        ),
        block_hash: Some(alloy_primitives::B256::repeat_byte(0x10)),
        block_number: Some(10),
        block_timestamp: Some(1_700_000_010),
        transaction_hash: Some(alloy_primitives::B256::repeat_byte(0x20)),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

#[test]
fn reactive_registry_handler_ids_preserve_registration_order() {
    let mut registry = ReactiveRegistry::<Ethereum>::new();
    registry
        .register_handler(Arc::new(NoopHandler::new(
            "first",
            Address::repeat_byte(0x01),
        )))
        .expect("register first");
    registry
        .register_handler(Arc::new(NoopHandler::new(
            "second",
            Address::repeat_byte(0x02),
        )))
        .expect("register second");
    registry
        .register_handler(Arc::new(NoopHandler::new(
            "third",
            Address::repeat_byte(0x03),
        )))
        .expect("register third");

    assert_eq!(
        registry.handler_ids(),
        vec![
            HandlerId::new("first"),
            HandlerId::new("second"),
            HandlerId::new("third"),
        ]
    );

    // Removing the middle handler preserves the order of the rest.
    registry
        .unregister_handler(&HandlerId::new("second"))
        .expect("remove second");
    assert_eq!(
        registry.handler_ids(),
        vec![HandlerId::new("first"), HandlerId::new("third")]
    );
}
