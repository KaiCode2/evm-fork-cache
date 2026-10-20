//! Default consumers retain the pre-ingress record footprint.
use super::*;

#[test]
fn disabled_ingress_timing_preserves_record_layout() {
    type PreviousFields = (
        ReactiveInputRecord<Ethereum>,
        SubscriberInputScope,
        Option<FlashblockIngressTiming>,
    );
    assert_eq!(
        std::mem::size_of::<SubscriberInputRecord<Ethereum>>(),
        std::mem::size_of::<PreviousFields>()
    );
    assert_eq!(
        std::mem::align_of::<SubscriberInputRecord<Ethereum>>(),
        std::mem::align_of::<PreviousFields>()
    );
}
