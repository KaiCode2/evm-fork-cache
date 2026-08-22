//! Cooperative cancellation for a logical scope of EVM simulations.
//!
//! The signal itself is provider-free and is observed at EVM instruction
//! boundaries. It cannot interrupt a database callback or precompile that is
//! already executing; callers that require a wholly provider-free cancellation
//! path must construct their overlays without an external database.

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

const STARTED: u8 = 1;
const CANCELLED: u8 = 1 << 1;

/// A cloneable cancellation signal for one logical simulation scope.
///
/// Clones share one atomic state. The executing inspector marks the token as
/// started at its first instruction boundary, while an ingress owner may call
/// [`cancel`](Self::cancel) from another thread. One scope may contain several
/// related overlay calls, including multi-chain or access-list replay calls;
/// every clone and call observes the same cancellation decision. Cancellation
/// is monotonic: a token cannot be reset and must not be carried into a later,
/// independent simulation scope.
#[derive(Clone, Debug, Default)]
pub struct SimulationCancellationToken {
    state: Arc<AtomicU8>,
}

impl SimulationCancellationToken {
    /// Create a fresh, unstarted and uncancelled simulation scope.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation.
    ///
    /// This operation is idempotent and safe before, during, or after EVM
    /// execution. A running cancellable overlay observes it at an instruction
    /// boundary.
    pub fn cancel(&self) {
        self.state.fetch_or(CANCELLED, Ordering::AcqRel);
    }

    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) & CANCELLED != 0
    }

    /// Whether any cancellable EVM in this scope reached its first instruction
    /// boundary.
    pub fn has_started(&self) -> bool {
        self.state.load(Ordering::Acquire) & STARTED != 0
    }

    pub(crate) fn mark_started(&self) {
        self.state.fetch_or(STARTED, Ordering::Release);
    }
}
