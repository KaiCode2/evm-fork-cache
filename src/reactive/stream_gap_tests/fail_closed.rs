use super::*;
use alloy_json_rpc::{RequestPacket, ResponsePacket};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_transport::{TransportError, TransportErrorKind, TransportFut};
use std::sync::atomic::AtomicUsize;
use tower::Service;

#[derive(Clone)]
struct NoRequests(Arc<AtomicUsize>);

impl Service<RequestPacket> for NoRequests {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, packet: RequestPacket) -> Self::Future {
        let n = match packet {
            RequestPacket::Single(_) => 1,
            RequestPacket::Batch(items) => items.len(),
        };
        self.0.fetch_add(n, Ordering::SeqCst);
        Box::pin(async { Err(TransportErrorKind::custom_str("forbidden provider attempt")) })
    }
}

#[tokio::test]
async fn fail_closed_actual_notification_loss_revokes_coverage_before_any_provider_attempt() {
    for malformed in [false, true] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let provider =
            RootProvider::<Ethereum>::new(RpcClient::new(NoRequests(Arc::clone(&attempts)), true));
        let mut subscriber = AlloySubscriber::new(
            provider,
            SubscriberMode::PubSub,
            SubscriberConfig::default(),
        )
        .with_gap_policy(SubscriberGapPolicy::FailClosed);
        subscriber.chain_id = Some(1);
        subscriber.sources_dirty = false;
        subscriber.last_seen_log_blocks.insert(0, 100);
        subscriber
            .interests
            .push(ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new().address(Address::repeat_byte(0x77)),
                local_matcher: None,
                route_key: None,
            }));
        let block = BlockRef {
            number: 100,
            hash: B256::repeat_byte(1),
            parent_hash: None,
            timestamp: None,
        };
        subscriber.attested_log_coverage = Some(block);
        let (tx, subscription) = wired_subscription(if malformed { 8 } else { 1 });
        if malformed {
            tx.send(serde_json::value::RawValue::from_string("{}".to_owned()).unwrap())
                .unwrap();
        } else {
            tx.send(wire_log(101, 0)).unwrap();
            tx.send(wire_log(102, 0)).unwrap();
        }
        let mut streams = SubscriberStreams::new();
        streams.push(
            log_source(0),
            gap_stream(
                subscription,
                log_source(0),
                Arc::clone(&subscriber.gap_counters),
            ),
        );
        subscriber.state = AlloySubscriberState::Active(streams);
        let expected = if malformed {
            SubscriberStreamGap::Undecodable
        } else {
            SubscriberStreamGap::Lagged { skipped: 1 }
        };
        for _ in 0..2 {
            let error = subscriber.next_scoped_batch().await.unwrap_err();
            assert!(
                matches!(error, SubscriberError::CanonicalStreamGap { stream: "canonical_logs", gap } if gap == expected)
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 0);
            assert_eq!(subscriber.rpc_stats().total(), 0);
            assert_eq!(subscriber.stream_gap_stats().log_gaps_healed(), 0);
            assert!(subscriber.attested_log_coverage.is_none());
        }
        assert_eq!(subscriber.stream_gap_stats().total_gaps(), 1);
        // Policy changes cannot turn a lost generation back into a trusted one.
        subscriber = subscriber.with_gap_policy(SubscriberGapPolicy::Reconcile);
        assert!(matches!(
            subscriber.next_scoped_batch().await,
            Err(SubscriberError::CanonicalStreamGap { .. })
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        subscriber.reset_delivery_state();
        assert!(subscriber.check_resource_error().is_ok());
        drop(tx);
    }
}

#[tokio::test]
async fn fail_closed_header_loss_and_unanchored_logs_do_not_recover_implicitly() {
    for source in [log_source(0), SubscriberStreamSource::PubSubBlockHeaders] {
        let mut subscriber =
            mocked_subscriber(Asserter::new()).with_gap_policy(SubscriberGapPolicy::FailClosed);
        assert!(matches!(
            subscriber
                .recover_stream_gap(&source, SubscriberStreamGap::Undecodable)
                .await,
            Err(SubscriberError::CanonicalStreamGap { .. })
        ));
        assert_eq!(subscriber.rpc_stats().total(), 0);
        assert_eq!(subscriber.stream_gap_stats().log_gaps_healed(), 0);
    }
}

#[test]
fn defaults_and_planned_filter_view_do_not_broaden_subscription_semantics() {
    let mut subscriber = mocked_subscriber(Asserter::new());
    assert_eq!(subscriber.gap_policy(), SubscriberGapPolicy::Reconcile);
    for address in [Address::repeat_byte(1), Address::repeat_byte(2)] {
        subscriber
            .base_interests
            .push(ReactiveInterest::Logs(LogInterest {
                provider_filter: Filter::new()
                    .address(address)
                    .event_signature(B256::repeat_byte(3)),
                local_matcher: None,
                route_key: None,
            }));
    }
    let filters = subscriber.planned_log_subscription_filters();
    assert_eq!(filters.len(), 1);
    assert_eq!(filters[0].address.len(), 2);
    subscriber.config.max_log_addresses_per_subscription = 1;
    assert_eq!(subscriber.planned_log_subscription_filters().len(), 2);
    assert_eq!(subscriber.rpc_stats().total(), 0);
}
