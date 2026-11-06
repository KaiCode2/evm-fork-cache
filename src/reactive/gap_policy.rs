//! Opt-in failure instead of implicit provider repair after notification loss.

use super::{AlloySubscriber, Network, Provider, SubscriberError, SubscriberStreamGap};

/// Response to detected loss on a canonical log or header subscription.
///
/// This policy does not assert stream completeness or alter ordering. It also
/// does not disable polling, reconnect, verification, owner catch-up or other
/// explicit provider operations; consumers must select those independently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SubscriberGapPolicy {
    /// Preserve historical log-window reconciliation and header-gap reporting.
    #[default]
    Reconcile,
    /// Revoke canonical coverage and stop before issuing repair requests.
    /// The failure remains sticky until explicit full registration reset.
    FailClosed,
}

impl<P, N> AlloySubscriber<P, N>
where
    N: Network,
    P: Provider<N> + Send + Sync,
{
    /// Select loss handling without changing the compatible subscriber config.
    ///
    /// Changing policy never clears an existing terminal loss. Reconnect and
    /// cold lifecycle operations keep their independently configured behavior.
    #[must_use]
    pub fn with_gap_policy(mut self, policy: SubscriberGapPolicy) -> Self {
        self.gap_policy = policy;
        self
    }

    /// Configured canonical notification-loss policy.
    pub const fn gap_policy(&self) -> SubscriberGapPolicy {
        self.gap_policy
    }

    /// Exact planned provider-side filters after owner fan-in and address limits.
    ///
    /// This is a pure view, not proof that each source has delivered through any
    /// block. Consumers may combine source coverage with ordering evidence;
    /// multiple independent filters never share an inferred successor barrier.
    pub fn planned_log_subscription_filters(&self) -> Vec<alloy_rpc_types_eth::Filter> {
        self.log_stream_filters()
    }

    pub(super) fn require_gap_recovery(
        &mut self,
        stream: &'static str,
        gap: SubscriberStreamGap,
    ) -> Result<(), SubscriberError> {
        if self.gap_policy == SubscriberGapPolicy::FailClosed {
            self.terminal_gap.get_or_insert((stream, gap));
            self.reset_log_attestation();
            self.attested_log_coverage = None;
            self.pending_chain_controls
                .retain(|control| !matches!(control, super::ChainControl::LogCoverage(_)));
        }
        self.check_resource_error()
    }
}
