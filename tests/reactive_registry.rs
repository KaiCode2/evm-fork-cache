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
