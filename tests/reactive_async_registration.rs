#![cfg(feature = "reactive")]

use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy_network::{Ethereum, Network};
use alloy_primitives::Address;
use alloy_rpc_types_eth::Filter;
use evm_fork_cache::ReactiveEngine;
use evm_fork_cache::reactive::{
    EventSubscriber, HandlerError, HandlerId, HandlerOutcome, InterestOwnerSubscriber, LogInterest,
    ReactiveConfig, ReactiveContext, ReactiveHandler, ReactiveInput, ReactiveInterest,
    RouteKeySpec, StateEffectQuality, SubscriberBackfill, SubscriberError, SubscriberNextBatch,
    SubscriberOperation,
};

struct DelayedSubscriber<N: Network = Ethereum> {
    owners: HashMap<HandlerId, Vec<ReactiveInterest<N>>>,
    registration_completed: bool,
    block_registration: bool,
    fail_removal: bool,
}

impl<N: Network> Default for DelayedSubscriber<N> {
    fn default() -> Self {
        Self {
            owners: HashMap::new(),
            registration_completed: false,
            block_registration: false,
            fail_removal: false,
        }
    }
}

impl<N> EventSubscriber<N> for DelayedSubscriber<N>
where
    N: Network + Send + 'static,
{
    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<N>],
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            if self.block_registration {
                std::future::pending::<()>().await;
            }
            tokio::task::yield_now().await;
            self.owners.clear();
            self.owners.insert(HandlerId::new("base"), interests);
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        Box::pin(async { Ok(None) })
    }
}

impl<N> InterestOwnerSubscriber<N> for DelayedSubscriber<N>
where
    N: Network + Send + 'static,
{
    fn replace_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if self.block_registration {
                std::future::pending::<()>().await;
            }
            tokio::task::yield_now().await;
            self.owners = owners.into_iter().collect();
            Ok(())
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if self.block_registration {
                std::future::pending::<()>().await;
            }
            tokio::task::yield_now().await;
            self.owners = owners.into_iter().collect();
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            if self.block_registration {
                std::future::pending::<()>().await;
            }
            tokio::task::yield_now().await;
            self.owners.insert(owner, interests);
            self.registration_completed = true;
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        _retained: evm_fork_cache::reactive::BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<N>>>> {
        let owner = owner.clone();
        Box::pin(async move {
            tokio::task::yield_now().await;
            if self.fail_removal {
                return Err(SubscriberError::InvalidConfig("forced removal failure"));
            }
            Ok(self.owners.remove(&owner))
        })
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<N>]> {
        self.owners.get(owner).map(Vec::as_slice)
    }
}

struct NoopHandler {
    id: HandlerId,
    address: Address,
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
        _state: &dyn evm_fork_cache::events::StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect))
    }
}

#[tokio::test]
async fn engine_registration_awaits_subscriber_completion() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        DelayedSubscriber::default(),
    );
    let id = HandlerId::new("pool-a");

    engine
        .register_handler(Arc::new(NoopHandler {
            id: id.clone(),
            address: Address::repeat_byte(0xa1),
        }))
        .await
        .expect("async subscriber registration should complete");

    assert!(engine.subscriber().registration_completed);
    assert!(engine.runtime().contains_handler(&id));
    assert!(engine.subscriber().owner_interests(&id).is_some());
}

#[tokio::test]
async fn engine_removal_failure_preserves_runtime_and_subscriber_owner() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        DelayedSubscriber::default(),
    );
    let id = HandlerId::new("pool-a");
    engine
        .register_handler(Arc::new(NoopHandler {
            id: id.clone(),
            address: Address::repeat_byte(0xa1),
        }))
        .await
        .expect("registration should complete");
    engine.subscriber_mut().fail_removal = true;

    let error = match engine.unregister_handler(&id).await {
        Ok(_) => panic!("subscriber removal failure should surface"),
        Err(error) => error,
    };

    assert!(matches!(error, SubscriberError::InvalidConfig(_)));
    assert!(engine.runtime().contains_handler(&id));
    assert!(engine.subscriber().owner_interests(&id).is_some());
}

#[tokio::test]
async fn cancelled_registration_does_not_commit_runtime_handler() {
    let subscriber = DelayedSubscriber::<Ethereum> {
        block_registration: true,
        ..Default::default()
    };
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        subscriber,
    );
    let id = HandlerId::new("pool-a");

    let mut registration = Box::pin(engine.register_handler(Arc::new(NoopHandler {
        id: id.clone(),
        address: Address::repeat_byte(0xa1),
    })));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), registration.as_mut())
            .await
            .is_err(),
        "test subscriber should keep registration pending"
    );
    drop(registration);

    assert!(!engine.runtime().contains_handler(&id));
    assert!(engine.subscriber().owner_interests(&id).is_none());
}

#[tokio::test]
async fn cancelled_removal_preserves_runtime_and_subscriber_owner() {
    let mut engine = ReactiveEngine::new(
        evm_fork_cache::ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        DelayedSubscriber::default(),
    );
    let id = HandlerId::new("pool-a");
    engine
        .register_handler(Arc::new(NoopHandler {
            id: id.clone(),
            address: Address::repeat_byte(0xa1),
        }))
        .await
        .expect("registration should complete");

    let mut removal = Box::pin(engine.unregister_handler(&id));
    assert!(
        futures::poll!(removal.as_mut()).is_pending(),
        "removal must pause at the subscriber commit boundary"
    );
    drop(removal);

    assert!(engine.runtime().contains_handler(&id));
    assert!(engine.subscriber().owner_interests(&id).is_some());
}

#[tokio::test]
async fn cancelled_exact_owner_replacement_preserves_previous_topology() {
    let stale = HandlerId::new("crash-stale");
    let mut subscriber = DelayedSubscriber::<Ethereum> {
        block_registration: true,
        ..Default::default()
    };
    subscriber.owners.insert(stale.clone(), Vec::new());
    let baseline = evm_fork_cache::reactive::BlockRef {
        number: 100,
        hash: alloy_primitives::B256::repeat_byte(100),
        parent_hash: Some(alloy_primitives::B256::repeat_byte(99)),
        timestamp: Some(1_700_000_100),
    };
    let backfill = SubscriberBackfill::after_canonical_block(baseline).expect("C + 1");

    let mut replacement = Box::pin(subscriber.replace_interest_owners_with_global_backfill(
        vec![(HandlerId::new("pool-a"), Vec::new())],
        backfill,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), replacement.as_mut())
            .await
            .is_err(),
        "test subscriber should keep replacement pending"
    );
    drop(replacement);

    assert_eq!(subscriber.owners.len(), 1);
    assert!(subscriber.owners.contains_key(&stale));
}

#[tokio::test]
async fn cancelled_fresh_owner_replacement_preserves_previous_topology() {
    let stale = HandlerId::new("crash-stale-fresh");
    let mut subscriber = DelayedSubscriber::<Ethereum> {
        block_registration: true,
        ..Default::default()
    };
    subscriber.owners.insert(stale.clone(), Vec::new());

    let mut replacement =
        Box::pin(subscriber.replace_interest_owners(vec![(HandlerId::new("pool-a"), Vec::new())]));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), replacement.as_mut())
            .await
            .is_err(),
        "test subscriber should keep replacement pending"
    );
    drop(replacement);

    assert_eq!(subscriber.owners.len(), 1);
    assert!(subscriber.owners.contains_key(&stale));
}
