//! Optional observational clocks supplied by an application-owned transport.
use alloy_primitives::B256;
use std::{sync::Arc, time::Instant};

/// One complete message observed before JSON-RPC decoding. Identity must name
/// the concrete receipt, not merely the payload or block. No execution authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportReceipt {
    identity: B256,
    received: Instant,
    decoded: Instant,
}
impl TransportReceipt {
    /// Reject inconsistent clocks instead of fabricating zero elapsed time.
    pub fn new(identity: B256, received: Instant, decoded: Instant) -> Option<Self> {
        (received <= decoded).then_some(Self {
            identity,
            received,
            decoded,
        })
    }
    /// Application-defined receipt identity, never a metric label.
    pub const fn identity(self) -> B256 {
        self.identity
    }
    /// Complete-message receive time, before envelope decoding.
    pub const fn received(self) -> Instant {
        self.received
    }
    /// JSON-RPC envelope decoded time, before pubsub delivery.
    pub const fn decoded(self) -> Instant {
        self.decoded
    }
}

/// Lookup for the exact raw payload consumed by a pubsub receiver. Implementations
/// must be bounded, nonblocking and observational. Missing, evicted or ambiguous
/// duplicate matches must return None; never pick an arbitrary earlier receipt.
/// The callback adds no transport read and must not mutate execution state.
///
/// A backend-neutral, one-shot in-memory observer. A real transport must maintain
/// bounded, unambiguous associations; this fixture accepts one known delivery only.
/// No exporter, metric labels, payload logging, or async runtime is required.
///
/// ```
/// use evm_fork_cache::reactive::{TransportReceipt, TransportReceiptLookup};
/// use alloy_primitives::B256;
/// use std::{sync::{Arc, Mutex}, time::Instant};
///
/// let received = Instant::now();
/// let receipt = TransportReceipt::new(B256::repeat_byte(1), received, Instant::now()).unwrap();
/// let slot = Mutex::new(Some(receipt));
/// let lookup: TransportReceiptLookup = Arc::new(move |raw| {
///     if raw != r#"{"fixture":true}"# { return None; }
///     slot.try_lock().ok()?.take()
/// });
/// let observed = lookup(r#"{"fixture":true}"#).unwrap();
/// tracing::debug!(receipt_id = %observed.identity(),
///     decode_ns = observed.decoded().duration_since(observed.received()).as_nanos(),
///     "transport receipt observed");
/// assert!(lookup(r#"{"fixture":true}"#).is_none());
/// ```
pub type TransportReceiptLookup = Arc<dyn Fn(&str) -> Option<TransportReceipt> + Send + Sync>;

/// Timing preserved with a canonical subscriber input through its bounded queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalIngressTiming {
    receipt: TransportReceipt,
    dequeued: Instant,
    decoded: Instant,
}
impl CanonicalIngressTiming {
    /// Bind source receipt to the actual dequeue and typed payload decode.
    pub fn new(receipt: TransportReceipt, dequeued: Instant, decoded: Instant) -> Option<Self> {
        (receipt.decoded <= dequeued && dequeued <= decoded).then_some(Self {
            receipt,
            dequeued,
            decoded,
        })
    }
    /// Concrete transport receipt.
    pub const fn receipt(self) -> TransportReceipt {
        self.receipt
    }
    /// Raw subscription notification dequeue time.
    pub const fn dequeued(self) -> Instant {
        self.dequeued
    }
    /// Typed domain payload decode completion time.
    pub const fn decoded(self) -> Instant {
        self.decoded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn queue_age_is_retained_and_invalid_clocks_remain_missing() {
        let received = Instant::now();
        let envelope = received + Duration::from_micros(5);
        let dequeue = envelope + Duration::from_millis(30);
        let decode = dequeue + Duration::from_micros(10);
        let receipt = TransportReceipt::new(B256::repeat_byte(1), received, envelope).unwrap();
        let timing = CanonicalIngressTiming::new(receipt, dequeue, decode).unwrap();
        assert_eq!(timing.receipt().received(), received);
        assert_eq!(
            timing.dequeued().duration_since(receipt.decoded()),
            Duration::from_millis(30)
        );
        assert!(TransportReceipt::new(B256::ZERO, envelope, received).is_none());
        assert!(CanonicalIngressTiming::new(receipt, received, decode).is_none());
    }
}
