//! Targeted state-mutation vocabulary and the structured diff it produces
//! (Pillar B.1 — the *writer half* of the event → state pipeline).
//!
//! This module defines the small, generic vocabulary a future event decoder
//! emits and [`EvmCache::apply_update`](crate::cache::EvmCache::apply_update)
//! consumes, plus the [`StateDiff`] that records what an apply actually changed.
//! It is pure data and logic on itself: it carries **no** protocol or event
//! knowledge and has no dependency on the cache or the `protocols` feature, so
//! it builds under `--no-default-features`.
//!
//! # The vocabulary
//!
//! A [`StateUpdate`] is one targeted mutation:
//!
//! - [`StateUpdate::Slot`] — set a single storage slot, authoritative across
//!   both cache layers.
//! - [`StateUpdate::Account`] — apply a partial [`AccountPatch`]
//!   (`balance`/`nonce`/`code`, each optional).
//! - [`StateUpdate::Purge`] — drop cached state at a [`PurgeScope`] so the next
//!   read re-fetches.
//!
//! # The dual-layer write-through policy
//!
//! [`apply_update`](crate::cache::EvmCache::apply_update) applies a `Slot` or
//! `Account` write-through with one consistent rule: the BlockchainDb backend
//! (layer 2) is written **always**; the CacheDB overlay (layer 1) is written
//! **only if an overlay account already exists** for the address. A new overlay
//! account is never materialized for a slot/account write — the read path falls
//! through to the backend for an absent overlay entry, so a backend-only write
//! is authoritative, and materializing an overlay entry would pollute layer 1
//! and could shadow later RPC reads. (This mirrors the established
//! [`inject_storage_batch_fresh`](crate::cache::EvmCache::inject_storage_batch_fresh)
//! semantics.)
//!
//! # The output
//!
//! Every apply returns a [`StateDiff`] of the changes it actually made: the
//! [`SlotChange`]s, [`AccountChange`]s, and [`PurgeRecord`]s. **Only real changes
//! are recorded** — re-applying a value the cache already holds yields an empty
//! diff, so idempotence is observable.
//!
//! # Relative updates / cold-aware read-modify-write
//!
//! Some callers learn only a *delta* (an ERC-20 `Transfer` log carries the
//! transferred `amount`, not the resulting balances), so the vocabulary also
//! supports *relative* updates: [`StateUpdate::SlotDelta`] reads the current slot
//! value, applies a saturating [`SlotDelta`] (`Add` clamps at `U256::MAX`, `Sub`
//! at `U256::ZERO`), and writes the result back through both layers. The general
//! closure form is
//! [`EvmCache::modify_slot`](crate::cache::EvmCache::modify_slot).
//!
//! A relative update is only valid against a value the cache *actually holds*. An
//! un-fetched ("cold") slot has no value, and applying a delta to it would compute
//! `0 ± amount`, write a wrong value, and (write-through) make it authoritative —
//! silently corrupting state. So relative application is **cold-aware**: a
//! `SlotDelta` on a cold slot is **not applied**; it is recorded in
//! [`StateDiff::skipped`] as a [`SkippedDelta`] so the caller can fetch+seed the
//! true value (the next read otherwise lazily fetches it). `modify_slot` hands its
//! closure an `Option<U256>` (`None` when cold) and lets the caller decide.
//!
//! The same relative, cold-aware rule extends to an account's **native balance**:
//! [`StateUpdate::BalanceDelta`] (and the closure form
//! [`EvmCache::modify_account_balance`](crate::cache::EvmCache::modify_account_balance))
//! read-modify-write `AccountInfo::balance`, preserving nonce/code. "Cold" here
//! means the account is absent from *both* layers (its balance is unknown); a
//! `BalanceDelta` on a cold account is **not applied** — it is surfaced in
//! [`StateDiff::skipped_balances`] as a [`SkippedBalanceDelta`]. This avoids
//! materializing a default account that would mask the real on-chain one.
//!
//! # Checking for skips
//!
//! Because a cold-skipped relative update produces **no** change, it is invisible
//! to the natural [`StateDiff::is_empty`] / [`StateDiff::len`] success check (those
//! are changes-only). A caller applying relative updates **must** therefore check
//! [`StateDiff::has_skipped`] (or inspect [`skipped`](StateDiff::skipped) /
//! [`skipped_balances`](StateDiff::skipped_balances)) — a cold target was dropped,
//! not applied, and a silently-dropped balance update can break conservation.
//! [`StateDiff::is_fully_applied`] and [`StateDiff::skipped_len`] are the
//! companions.
//!
//! # Warning — cold absolute `Account` patches
//!
//! A *partial* absolute [`StateUpdate::Account`] patch (e.g. balance-only) on an
//! address absent from **both** cache layers writes default nonce/code through the
//! shared backend as authoritative, pre-empting a real RPC fetch. Fetch+seed the
//! account first, or prefer [`StateUpdate::BalanceDelta`] for relative
//! native-balance tracking. See the warnings on
//! [`apply_update`](crate::cache::EvmCache::apply_update),
//! [`StateUpdate::Account`], and [`AccountPatch`].
//!
//! # Boundary — events are Phase 4
//!
//! This is the vocabulary a Phase 4 `EventDecoder` will *emit into*; Phase 3
//! does not decode events. Nothing here parses a `Log` or knows a protocol's
//! storage layout — that is the *reader half* of Pillar B and lands later.

use alloy_primitives::{Address, B256, Bytes, U256};

use crate::freshness::SlotChange;

/// A relative storage-slot mutation: read the current value, transform it, and
/// write it back.
///
/// Both directions **saturate** rather than wrap: `Add` clamps at `U256::MAX`
/// and `Sub` clamps at `U256::ZERO`. This is the delta a caller derives from an
/// event (e.g. an ERC-20 `Transfer` amount) without knowing the resulting
/// absolute value. It is applied by [`StateUpdate::SlotDelta`] (cold-aware — see
/// the module docs).
///
/// ```
/// use alloy_primitives::U256;
/// use evm_fork_cache::SlotDelta;
///
/// assert_eq!(SlotDelta::Add(U256::from(50)).apply(U256::from(100)), U256::from(150));
/// assert_eq!(SlotDelta::Sub(U256::from(50)).apply(U256::from(30)), U256::ZERO);
/// assert_eq!(SlotDelta::Add(U256::from(10)).apply(U256::MAX), U256::MAX);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SlotDelta {
    /// Add to the current value, saturating at `U256::MAX`.
    Add(U256),
    /// Subtract from the current value, saturating at `U256::ZERO`.
    Sub(U256),
}

impl SlotDelta {
    /// Apply the (saturating) delta to a current value.
    ///
    /// `Add` uses `saturating_add` (clamps at `U256::MAX`); `Sub` uses
    /// `saturating_sub` (clamps at `U256::ZERO`).
    pub fn apply(self, current: U256) -> U256 {
        match self {
            SlotDelta::Add(amount) => current.saturating_add(amount),
            SlotDelta::Sub(amount) => current.saturating_sub(amount),
        }
    }
}

/// A single targeted mutation to cached EVM state.
///
/// The vocabulary an event decoder (Phase 4) emits and
/// [`EvmCache::apply_update`](crate::cache::EvmCache::apply_update) consumes.
/// Generic: carries no protocol or event knowledge.
///
/// The enum is `#[non_exhaustive]`: new variants (e.g. a code-only convenience)
/// may be added pre-1.0 without a breaking change.
///
/// ```
/// use alloy_primitives::{Address, U256};
/// use evm_fork_cache::{AccountPatch, PurgeScope, StateUpdate};
///
/// let pool = Address::repeat_byte(0x01);
///
/// // A storage-slot write (authoritative across both cache layers).
/// let slot = StateUpdate::slot(pool, U256::from(0), U256::from(42));
///
/// // A balance-only account patch (nonce and code left untouched).
/// let bal = StateUpdate::balance(pool, U256::from(1_000));
/// assert_eq!(
///     bal,
///     StateUpdate::Account { address: pool, patch: AccountPatch::default().balance(U256::from(1_000)) },
/// );
///
/// // Drop just two storage slots so the next read re-fetches them.
/// let purge = StateUpdate::purge(pool, PurgeScope::Slots(vec![U256::from(0), U256::from(1)]));
/// # let _ = (slot, purge);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum StateUpdate {
    /// Set one storage slot to `value`, authoritative across both cache layers.
    Slot {
        /// Contract whose storage is written.
        address: Address,
        /// Storage slot key.
        slot: U256,
        /// New slot value.
        value: U256,
    },
    /// Apply a *relative* (saturating) mutation to one storage slot.
    ///
    /// Read-modify-write: the current value is read, the [`SlotDelta`] applied,
    /// and the result written back through both layers. **Cold-aware** — a delta
    /// on a slot absent from both layers is not applied; it is surfaced in
    /// [`StateDiff::skipped`] instead (see the module docs).
    SlotDelta {
        /// Contract whose storage is written.
        address: Address,
        /// Storage slot key.
        slot: U256,
        /// The relative, saturating mutation to apply to the current value.
        delta: SlotDelta,
    },
    /// Apply a *relative* (saturating) mutation to an account's **native balance**.
    ///
    /// Read-modify-write: the current `AccountInfo::balance` is read, the
    /// [`SlotDelta`] applied, and the result written back through both layers
    /// (nonce and code preserved). **Cold-aware** — "cold" here means the account
    /// is absent from *both* layers (its balance is unknown). A `BalanceDelta` on a
    /// cold account is not applied; it is surfaced in
    /// [`StateDiff::skipped_balances`] instead (so no default account is
    /// materialized to mask the real on-chain one — see the module docs).
    BalanceDelta {
        /// Account whose native balance is mutated.
        address: Address,
        /// The relative, saturating mutation to apply to the current balance.
        delta: SlotDelta,
    },
    /// Patch an account's balance/nonce/code (partial — see [`AccountPatch`]).
    ///
    /// # Warning
    ///
    /// A partial absolute patch (e.g. balance-only) on an address absent from
    /// **both** cache layers writes default nonce/code through the shared backend
    /// as authoritative, pre-empting a real RPC fetch. Fetch+seed the account
    /// first, or use [`StateUpdate::BalanceDelta`] for relative native-balance
    /// tracking.
    Account {
        /// Account to patch.
        address: Address,
        /// The partial mutation: each `Some` field overwrites, `None` leaves it.
        patch: AccountPatch,
    },
    /// Purge cached state for `address` at `scope`; the next read re-fetches.
    Purge {
        /// Account whose cached state is purged.
        address: Address,
        /// What part of the cached state to remove.
        scope: PurgeScope,
    },
}

impl StateUpdate {
    /// Construct a [`StateUpdate::Slot`] that sets `(address, slot)` to `value`.
    pub fn slot(address: Address, slot: U256, value: U256) -> Self {
        Self::Slot {
            address,
            slot,
            value,
        }
    }

    /// Construct a [`StateUpdate::SlotDelta`] that applies `delta` relative to the
    /// current value of `(address, slot)`.
    pub fn slot_delta(address: Address, slot: U256, delta: SlotDelta) -> Self {
        Self::SlotDelta {
            address,
            slot,
            delta,
        }
    }

    /// Construct a [`StateUpdate::BalanceDelta`] that applies `delta` relative to
    /// the account's current native balance.
    pub fn balance_delta(address: Address, delta: SlotDelta) -> Self {
        Self::BalanceDelta { address, delta }
    }

    /// Construct a [`StateUpdate::Account`] that patches only the balance.
    pub fn balance(address: Address, value: U256) -> Self {
        Self::Account {
            address,
            patch: AccountPatch::default().balance(value),
        }
    }

    /// Construct a [`StateUpdate::Account`] that patches only the nonce.
    pub fn nonce(address: Address, nonce: u64) -> Self {
        Self::Account {
            address,
            patch: AccountPatch::default().nonce(nonce),
        }
    }

    /// Construct a [`StateUpdate::Account`] that patches only the runtime code
    /// (the code hash is recomputed from `code` when applied).
    pub fn code(address: Address, code: Bytes) -> Self {
        Self::Account {
            address,
            patch: AccountPatch::default().code(code),
        }
    }

    /// Construct a [`StateUpdate::Account`] from a prebuilt [`AccountPatch`].
    pub fn account(address: Address, patch: AccountPatch) -> Self {
        Self::Account { address, patch }
    }

    /// Construct a [`StateUpdate::Purge`] for `address` at `scope`.
    pub fn purge(address: Address, scope: PurgeScope) -> Self {
        Self::Purge { address, scope }
    }
}

/// A partial account mutation: each `Some` field overwrites the cached value,
/// each `None` leaves it unchanged. Setting `code` recomputes the code hash;
/// `Some(empty bytes)` clears code to the empty-code hash.
///
/// Partial (rather than a full revm `AccountInfo`) because the Pillar B driver
/// is events, which usually carry *one* field (a `Transfer` changes a balance,
/// not nonce/code). This avoids forcing a caller to reconstruct a full
/// `AccountInfo` and keeps revm's type out of the public vocabulary.
///
/// The struct is `#[non_exhaustive]`: new fields may be added pre-1.0 without a
/// breaking change. Construct it via [`AccountPatch::default`] + the builders
/// ([`balance`](Self::balance) / [`nonce`](Self::nonce) / [`code`](Self::code)),
/// never a struct literal.
///
/// # Warning
///
/// Applying an absolute patch with [`StateUpdate::Account`] on an address absent
/// from **both** cache layers writes default values for the un-patched fields
/// (e.g. nonce `0`, empty code) through the shared backend as authoritative,
/// masking a later RPC fetch of the real on-chain account. Fetch+seed the account
/// first, or use [`StateUpdate::BalanceDelta`] for relative native-balance
/// tracking.
///
/// ```
/// use alloy_primitives::{Bytes, U256};
/// use evm_fork_cache::AccountPatch;
///
/// let patch = AccountPatch::default()
///     .balance(U256::from(42))
///     .nonce(7)
///     .code(Bytes::from_static(&[0x60, 0x00]));
/// assert_eq!(patch.balance, Some(U256::from(42)));
/// assert_eq!(patch.nonce, Some(7));
/// assert_eq!(AccountPatch::default().balance, None);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct AccountPatch {
    /// New balance, if set.
    pub balance: Option<U256>,
    /// New nonce, if set.
    pub nonce: Option<u64>,
    /// New runtime code, if set. Setting it recomputes the code hash; empty
    /// bytes clear the code to the empty-code hash.
    pub code: Option<Bytes>,
}

impl AccountPatch {
    /// Set the balance to overwrite (builder style).
    pub fn balance(mut self, balance: U256) -> Self {
        self.balance = Some(balance);
        self
    }

    /// Set the nonce to overwrite (builder style).
    pub fn nonce(mut self, nonce: u64) -> Self {
        self.nonce = Some(nonce);
        self
    }

    /// Set the runtime code to overwrite (builder style). The code hash is
    /// recomputed from these bytes when the patch is applied.
    pub fn code(mut self, code: Bytes) -> Self {
        self.code = Some(code);
        self
    }
}

/// What part of an address's cached state a purge removes.
///
/// The enum is `#[non_exhaustive]`: new scopes may be added pre-1.0 without a
/// breaking change.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum PurgeScope {
    /// Full account: `AccountInfo` (balance/nonce/code) **and** all storage.
    /// Equivalent to
    /// [`EvmCache::purge_account`](crate::cache::EvmCache::purge_account).
    Account,
    /// All storage slots; account info preserved. Equivalent to
    /// [`EvmCache::purge_pool_storage`](crate::cache::EvmCache::purge_pool_storage).
    AllStorage,
    /// Only the listed storage slots. Equivalent to
    /// [`EvmCache::purge_pool_slots`](crate::cache::EvmCache::purge_pool_slots).
    Slots(Vec<U256>),
}

/// What an `apply_*` call actually changed.
///
/// Returned by [`EvmCache::apply_update`](crate::cache::EvmCache::apply_update)
/// and [`apply_updates`](crate::cache::EvmCache::apply_updates). Only real
/// changes are recorded, so a no-op write yields a [`Default`] (empty) diff.
///
/// The struct is `#[non_exhaustive]`: it has grown fields pre-1.0
/// ([`skipped`](Self::skipped), [`skipped_balances`](Self::skipped_balances)) and
/// may grow more. Construct it via [`Default`] + field assignment, never an
/// exhaustive struct literal.
///
/// # Checking for skips
///
/// [`is_empty`](Self::is_empty) / [`len`](Self::len) are **changes-only**, so a
/// cold-skipped relative update ([`SlotDelta`](StateUpdate::SlotDelta) /
/// [`BalanceDelta`](StateUpdate::BalanceDelta)) is invisible to them. After
/// applying relative updates, check [`has_skipped`](Self::has_skipped) (or
/// inspect [`skipped`](Self::skipped) / [`skipped_balances`](Self::skipped_balances))
/// — a cold target was dropped, not applied.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct StateDiff {
    /// Storage slots whose value changed (`old != new`).
    pub slots: Vec<SlotChange>,
    /// Accounts whose balance/nonce/code-hash changed.
    pub accounts: Vec<AccountChange>,
    /// Purges performed, with what they removed.
    pub purged: Vec<PurgeRecord>,
    /// Relative slot updates ([`StateUpdate::SlotDelta`]) that were **not** applied
    /// because the target slot's current value was unknown (cold). This is
    /// informational metadata, not a change: it does **not** affect
    /// [`is_empty`](Self::is_empty) / [`len`](Self::len).
    pub skipped: Vec<SkippedDelta>,
    /// Relative balance updates ([`StateUpdate::BalanceDelta`]) that were **not**
    /// applied because the target account was absent from both layers (its balance
    /// was unknown). Like [`skipped`](Self::skipped) this is informational
    /// metadata, not a change.
    pub skipped_balances: Vec<SkippedBalanceDelta>,
}

impl StateDiff {
    /// Whether the diff recorded no change at all.
    ///
    /// Changes-only: counts `slots` + `accounts` + `purged`. A skipped relative
    /// update ([`skipped`](Self::skipped) / [`skipped_balances`](Self::skipped_balances))
    /// is informational metadata, not a change, so it does not affect this.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty() && self.accounts.is_empty() && self.purged.is_empty()
    }

    /// Total number of changed entries (slots + accounts + purges).
    ///
    /// Changes-only: skipped relative updates are not counted (a skip is not a
    /// change). See [`skipped_len`](Self::skipped_len) for the skip count.
    pub fn len(&self) -> usize {
        self.slots.len() + self.accounts.len() + self.purged.len()
    }

    /// Whether any relative update was skipped (slot **or** balance).
    ///
    /// `true` iff [`skipped`](Self::skipped) or
    /// [`skipped_balances`](Self::skipped_balances) is non-empty. A cold-skipped
    /// update produces no change, so it is invisible to
    /// [`is_empty`](Self::is_empty) — callers applying relative updates should
    /// check this to avoid silently dropping a balance update.
    pub fn has_skipped(&self) -> bool {
        !self.skipped.is_empty() || !self.skipped_balances.is_empty()
    }

    /// Total number of skipped relative updates (`skipped` + `skipped_balances`).
    pub fn skipped_len(&self) -> usize {
        self.skipped.len() + self.skipped_balances.len()
    }

    /// Whether every relative update in the apply was applied (none skipped).
    ///
    /// The inverse of [`has_skipped`](Self::has_skipped).
    pub fn is_fully_applied(&self) -> bool {
        !self.has_skipped()
    }

    /// Fold `other` into `self`, concatenating each category.
    ///
    /// Used by [`apply_updates`](crate::cache::EvmCache::apply_updates) to merge
    /// per-update diffs; the concatenation preserves order, so two writes to the
    /// same slot record their `old → new` history in sequence. The `skipped` and
    /// `skipped_balances` metadata are concatenated too.
    pub fn merge(&mut self, other: StateDiff) {
        self.slots.extend(other.slots);
        self.accounts.extend(other.accounts);
        self.purged.extend(other.purged);
        self.skipped.extend(other.skipped);
        self.skipped_balances.extend(other.skipped_balances);
    }
}

/// An account field delta. Each field is `Some((old, new))` only when it changed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AccountChange {
    /// Account whose fields changed.
    pub address: Address,
    /// Balance delta `(old, new)`, present only if the balance changed.
    pub balance: Option<(U256, U256)>,
    /// Nonce delta `(old, new)`, present only if the nonce changed.
    pub nonce: Option<(u64, u64)>,
    /// Code-hash delta `(old, new)`, present only if the code changed.
    pub code_hash: Option<(B256, B256)>,
}

/// Record of a purge: how much of each layer it removed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PurgeRecord {
    /// Account that was purged.
    pub address: Address,
    /// The scope that was applied.
    pub scope: PurgeScope,
    /// Storage slots removed from the BlockchainDb backend (layer 2).
    pub slots_removed: usize,
    /// Whether an `AccountInfo` was removed (only the [`PurgeScope::Account`] scope).
    pub account_removed: bool,
}

/// A relative update ([`StateUpdate::SlotDelta`]) that could not be applied
/// because the slot's current value is unknown (not cached in either layer).
///
/// A delta against a cold slot is skipped rather than applied (applying `0 ±
/// amount` would corrupt an unknown value and, write-through, make it
/// authoritative). It is surfaced here so the caller can fetch+seed the true
/// value and retry; otherwise the next read lazily fetches it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SkippedDelta {
    /// Contract whose storage slot the delta targeted.
    pub address: Address,
    /// Storage slot key that was cold.
    pub slot: U256,
    /// The delta that was not applied.
    pub delta: SlotDelta,
}

/// A relative balance update ([`StateUpdate::BalanceDelta`]) that could not be
/// applied because the account is absent from **both** cache layers (its native
/// balance is unknown).
///
/// A delta against a cold account is skipped rather than applied (applying it
/// against an assumed-zero balance would corrupt an unknown value, and
/// materializing a default account would mask the real on-chain one). It is
/// surfaced here so the caller can fetch+seed the account and retry.
///
/// Deliberately **not** `#[non_exhaustive]`: it is a stable, fully-determined leaf
/// record routinely constructed as a struct literal in equality assertions by the
/// test suite and downstream users testing against a returned diff.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SkippedBalanceDelta {
    /// Account whose native balance the delta targeted.
    pub address: Address,
    /// The delta that was not applied.
    pub delta: SlotDelta,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> Address {
        Address::repeat_byte(n)
    }

    #[test]
    fn account_patch_default_is_all_none() {
        let p = AccountPatch::default();
        assert_eq!(p.balance, None);
        assert_eq!(p.nonce, None);
        assert_eq!(p.code, None);
    }

    #[test]
    fn account_patch_builders_compose() {
        let p = AccountPatch::default()
            .balance(U256::from(42))
            .nonce(7)
            .code(Bytes::from_static(&[0x60, 0x00]));
        assert_eq!(p.balance, Some(U256::from(42)));
        assert_eq!(p.nonce, Some(7));
        assert_eq!(p.code, Some(Bytes::from_static(&[0x60, 0x00])));
    }

    #[test]
    fn state_update_constructors_produce_expected_variants() {
        let a = addr(0xaa);

        assert_eq!(
            StateUpdate::slot(a, U256::from(1), U256::from(2)),
            StateUpdate::Slot {
                address: a,
                slot: U256::from(1),
                value: U256::from(2),
            }
        );
        assert_eq!(
            StateUpdate::balance(a, U256::from(9)),
            StateUpdate::Account {
                address: a,
                patch: AccountPatch::default().balance(U256::from(9)),
            }
        );
        assert_eq!(
            StateUpdate::purge(a, PurgeScope::Account),
            StateUpdate::Purge {
                address: a,
                scope: PurgeScope::Account,
            }
        );
    }

    #[test]
    fn state_diff_default_is_empty() {
        let d = StateDiff::default();
        assert!(d.is_empty());
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn state_diff_merge_concatenates_and_counts() {
        let a = addr(0xbb);
        let mut left = StateDiff::default();
        left.slots.push(SlotChange {
            address: a,
            slot: U256::from(1),
            old: U256::ZERO,
            new: U256::from(5),
        });

        let mut right = StateDiff::default();
        right.accounts.push(AccountChange {
            address: a,
            balance: Some((U256::ZERO, U256::from(3))),
            nonce: None,
            code_hash: None,
        });
        right.purged.push(PurgeRecord {
            address: a,
            scope: PurgeScope::AllStorage,
            slots_removed: 2,
            account_removed: false,
        });

        left.merge(right);
        assert!(!left.is_empty());
        assert_eq!(left.len(), 3);
        assert_eq!(left.slots.len(), 1);
        assert_eq!(left.accounts.len(), 1);
        assert_eq!(left.purged.len(), 1);
        // Concatenation preserves the merged-in slot value.
        assert_eq!(left.slots[0].new, U256::from(5));
    }

    #[test]
    fn slot_delta_add_applies_saturating() {
        assert_eq!(
            SlotDelta::Add(U256::from(50)).apply(U256::from(100)),
            U256::from(150)
        );
        // Saturates at U256::MAX rather than wrapping.
        assert_eq!(
            SlotDelta::Add(U256::from(10)).apply(U256::MAX - U256::from(1)),
            U256::MAX
        );
        assert_eq!(SlotDelta::Add(U256::from(5)).apply(U256::MAX), U256::MAX);
    }

    #[test]
    fn slot_delta_sub_applies_saturating() {
        assert_eq!(
            SlotDelta::Sub(U256::from(30)).apply(U256::from(100)),
            U256::from(70)
        );
        // Saturates at zero rather than underflowing.
        assert_eq!(
            SlotDelta::Sub(U256::from(50)).apply(U256::from(30)),
            U256::ZERO
        );
        assert_eq!(SlotDelta::Sub(U256::from(1)).apply(U256::ZERO), U256::ZERO);
    }

    #[test]
    fn state_update_slot_delta_constructor() {
        let a = addr(0xcc);
        assert_eq!(
            StateUpdate::slot_delta(a, U256::from(1), SlotDelta::Add(U256::from(2))),
            StateUpdate::SlotDelta {
                address: a,
                slot: U256::from(1),
                delta: SlotDelta::Add(U256::from(2)),
            }
        );
    }

    #[test]
    fn state_diff_merge_extends_skipped_without_counting_it() {
        let a = addr(0xdd);
        let mut left = StateDiff::default();
        let mut right = StateDiff::default();
        right.skipped.push(SkippedDelta {
            address: a,
            slot: U256::from(1),
            delta: SlotDelta::Sub(U256::from(3)),
        });

        left.merge(right);
        assert_eq!(left.skipped.len(), 1);
        // A skip is metadata, not a change.
        assert!(left.is_empty());
        assert_eq!(left.len(), 0);
    }
}
