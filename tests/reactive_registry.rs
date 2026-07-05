//! Implementation-owned registry regression tests.
#![cfg(feature = "reactive")]

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_primitives::Address;
use alloy_rpc_types_eth::Filter;

use evm_fork_cache::events::StateView;
use evm_fork_cache::reactive::{
    HandlerError, HandlerId, HandlerOutcome, LogInterest, ReactiveContext, ReactiveHandler,
    ReactiveInput, ReactiveInterest, ReactiveRegistry, RegisterError, RouteKeySpec,
    StateEffectQuality,
};

struct NoopHandler {
    id: HandlerId,
    address: Address,
}

impl NoopHandler {
    fn new(id: &'static str, address: Address) -> Self {
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
