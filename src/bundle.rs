//! Multi-transaction **bundle simulation** over cumulative block state, plus
//! **coinbase / miner-payment accounting** (Phase 6 Track A+B).
//!
//! A *bundle* is an ordered sequence of `Call`-kind transactions applied to a
//! single overlay so that transaction `i` observes the committed writes of
//! transactions `0..i` — the minimal primitive an MEV searcher needs to value a
//! candidate set of transactions as a unit. It is intentionally *not* a block
//! builder: there is no mempool, ordering auction, or `Create`-kind support (see
//! the Phase 6 spec non-goals).
//!
//! The execution itself lives in [`EvmOverlay::simulate_bundle`] (and the
//! cache-side convenience [`EvmCache::simulate_bundle`]); this module owns the
//! public vocabulary those methods speak.
//!
//! # Coinbase accounting
//!
//! [`BundleResult::coinbase_payment`] is the block beneficiary's balance delta
//! across the bundle — the honest miner payment. Under EIP-1559 (London+, which
//! this engine runs by default) revm credits the beneficiary only the **priority
//! fee** (`(effective_gas_price − basefee) × gas_used`) and burns the base-fee
//! portion in-EVM, so the delta already excludes the base fee. It also captures
//! any **direct value transfers to the beneficiary** (an explicit coinbase tip).
//! So `coinbase_payment = Σ priority_feeᵢ × gas_usedᵢ + direct coinbase tips`,
//! over the transactions whose effects are kept. Set the base fee with
//! [`EvmCache::set_basefee`](crate::cache::EvmCache::set_basefee) to model a
//! non-zero base fee (a higher base fee lowers the priority fee, and thus the
//! payment, for a fixed `gas_price`). All arithmetic is saturating.
//!
//! [`EvmCache::simulate_bundle`]: crate::cache::EvmCache::simulate_bundle
//! [`EvmOverlay::simulate_bundle`]: crate::cache::EvmOverlay::simulate_bundle

use alloy_primitives::{Address, Bytes, Log, U256};
use revm::context::result::ExecutionResult;

use crate::cache::TxConfig;

/// One transaction in a bundle.
///
/// `Call`-kind only for Phase 6 (`Create`/`Create2` bundle transactions are a
/// documented follow-up). The [`tx`](BundleTx::tx) field reuses the existing
/// [`TxConfig`] vocabulary (`value` / `gas_limit` / `gas_price` / `nonce` /
/// `access_list`), so a bundle transaction can carry native value, be
/// gas-bounded, or pre-warm an EIP-2930 access list exactly like a single
/// [`call_raw_with_access_list_with`](crate::cache::EvmOverlay::call_raw_with_access_list_with)
/// call.
#[derive(Clone, Debug)]
pub struct BundleTx {
    /// Sender of the call (the `from` / caller address).
    pub from: Address,
    /// Call target (`Call`-kind only for Phase 6).
    pub to: Address,
    /// ABI-encoded calldata for the call.
    pub calldata: Bytes,
    /// Per-transaction environment overrides (value / gas / nonce / access list).
    pub tx: TxConfig,
}

impl BundleTx {
    /// A bundle transaction with a default [`TxConfig`] (zero value, default
    /// gas/nonce, no access list).
    pub fn new(from: Address, to: Address, calldata: Bytes) -> Self {
        Self {
            from,
            to,
            calldata,
            tx: TxConfig::default(),
        }
    }

    /// A bundle transaction carrying an explicit [`TxConfig`] — e.g. to send
    /// native `value` (a direct coinbase transfer), set a `gas_price`, or
    /// pre-warm an access list.
    pub fn with_config(from: Address, to: Address, calldata: Bytes, tx: TxConfig) -> Self {
        Self {
            from,
            to,
            calldata,
            tx,
        }
    }
}

/// What happens when a bundle transaction reverts (or halts).
///
/// Defaults to [`Atomic`](RevertPolicy::Atomic).
#[derive(Clone, Debug, Default)]
pub enum RevertPolicy {
    /// Any transaction revert/halt reverts the **whole** bundle to the outer
    /// checkpoint and sets [`BundleResult::succeeded`] to `false`. Execution
    /// stops at the failing transaction.
    #[default]
    Atomic,
    /// The listed transaction indices may revert without aborting the bundle:
    /// their state effects are rolled back **individually** (inner checkpoint
    /// revert) and later transactions still execute. A revert at an index *not*
    /// in the list behaves like [`Atomic`](RevertPolicy::Atomic).
    AllowReverts(Vec<usize>),
}

/// Options controlling a bundle simulation.
#[derive(Clone, Debug, Default)]
pub struct BundleOptions {
    /// Revert handling for individual transactions. Default
    /// [`Atomic`](RevertPolicy::Atomic).
    pub revert_policy: RevertPolicy,
    /// Whether the bundle's cumulative state is folded into the overlay's dirty
    /// layer (`true`) or reverted so the overlay is left unchanged (`false`).
    /// Default `false` (evaluate, don't persist).
    pub commit: bool,
}

/// Outcome of a single transaction executed within a bundle.
#[derive(Clone, Debug)]
pub struct TxOutcome {
    /// The raw revm [`ExecutionResult`] (`Success` / `Revert` / `Halt`).
    pub result: ExecutionResult,
    /// Gas consumed by this transaction.
    pub gas_used: u64,
    /// `true` if this transaction reverted or halted.
    pub reverted: bool,
    /// Logs emitted by this transaction (empty for a revert/halt).
    pub logs: Vec<Log>,
}

/// Result of a bundle simulation.
#[derive(Clone, Debug)]
pub struct BundleResult {
    /// One [`TxOutcome`] per executed transaction. Length equals `txs.len()`
    /// unless an [`Atomic`](RevertPolicy::Atomic) bundle aborted early, in which
    /// case it ends at the failing transaction.
    pub per_tx: Vec<TxOutcome>,
    /// Miner payment: the block beneficiary's balance delta across the kept
    /// transactions (priority fee + direct coinbase tips; the base fee is already
    /// excluded by revm — see the [module docs](self#coinbase-accounting)).
    /// Saturating; `0` for an [`Atomic`](RevertPolicy::Atomic) bundle that aborted.
    pub coinbase_payment: U256,
    /// Total gas used across the executed transactions.
    pub gas_used: u64,
    /// `false` iff an [`Atomic`](RevertPolicy::Atomic) bundle aborted on a
    /// revert/halt (or an `AllowReverts` bundle hit a revert at a non-whitelisted
    /// index).
    pub succeeded: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revert_policy_defaults_to_atomic() {
        assert!(matches!(RevertPolicy::default(), RevertPolicy::Atomic));
    }

    #[test]
    fn bundle_options_default_is_evaluate_only_atomic() {
        let opts = BundleOptions::default();
        assert!(matches!(opts.revert_policy, RevertPolicy::Atomic));
        assert!(!opts.commit, "default must not persist");
    }

    #[test]
    fn bundle_tx_new_uses_default_tx_config() {
        let from = Address::repeat_byte(0x01);
        let to = Address::repeat_byte(0x02);
        let tx = BundleTx::new(from, to, Bytes::from(vec![0xaa]));
        assert_eq!(tx.from, from);
        assert_eq!(tx.to, to);
        assert_eq!(tx.calldata, Bytes::from(vec![0xaa]));
        assert_eq!(tx.tx.value, U256::ZERO);
        assert!(tx.tx.gas_price.is_none());
        assert!(tx.tx.access_list.is_none());
    }

    #[test]
    fn bundle_tx_with_config_carries_value_and_gas_price() {
        let tx = BundleTx::with_config(
            Address::ZERO,
            Address::ZERO,
            Bytes::new(),
            TxConfig {
                value: U256::from(42u64),
                gas_price: Some(7),
                ..Default::default()
            },
        );
        assert_eq!(tx.tx.value, U256::from(42u64));
        assert_eq!(tx.tx.gas_price, Some(7));
    }
}
